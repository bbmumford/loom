package rpc

import (
	"context"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestRegistryBasics(t *testing.T) {
	reg := NewRegistry()

	h := Handler{
		Namespace: "hstles",
		Domain:    "identity",
		Operation: "CreateUser",
		Func: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			return &wrapperspb.StringValue{Value: "created"}, nil
		},
		Request:  (*emptypb.Empty)(nil),
		Response: (*wrapperspb.StringValue)(nil),
		Scope:    ScopeTenant,
		Tags:     []string{"users", "write"},
	}

	if err := reg.Register(h); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// FQN
	if h.FQN() != "hstles.identity.CreateUser" {
		t.Errorf("FQN = %q, want hstles.identity.CreateUser", h.FQN())
	}
	if h.Role() != "hstles.identity" {
		t.Errorf("Role = %q, want hstles.identity", h.Role())
	}

	// Get
	got, ok := reg.Get("hstles.identity.CreateUser")
	if !ok {
		t.Fatal("Get: not found")
	}
	if got.Domain != "identity" {
		t.Errorf("Domain = %q", got.Domain)
	}

	// Count
	if reg.Count() != 1 {
		t.Errorf("Count = %d", reg.Count())
	}

	// Roles
	roles := reg.Roles()
	if len(roles) != 1 || roles[0] != "hstles.identity" {
		t.Errorf("Roles = %v", roles)
	}

	// Duplicate
	if err := reg.Register(h); err == nil {
		t.Error("expected duplicate error")
	}

	// ExtractRole
	if ExtractRole("hstles.identity.CreateUser") != "hstles.identity" {
		t.Error("ExtractRole failed")
	}
}

func TestDispatch(t *testing.T) {
	reg := NewRegistry()

	reg.Register(Handler{
		Namespace: "hstles",
		Domain:    "test",
		Operation: "Echo",
		Scope:     ScopeNone,
		Func: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			return req, nil // echo
		},
		Request:  (*emptypb.Empty)(nil),
		Response: (*emptypb.Empty)(nil),
	})

	payload, _ := proto.Marshal(&emptypb.Empty{})
	_, err := reg.Dispatch(context.Background(), "hstles.test.Echo", payload)
	if err != nil {
		t.Fatalf("Dispatch: %v", err)
	}

	// Not found
	_, err = reg.Dispatch(context.Background(), "hstles.test.Missing", nil)
	if err == nil {
		t.Error("expected not found error")
	}
}

func TestWrap(t *testing.T) {
	fn := Wrap(func(ctx context.Context, req *emptypb.Empty) (*wrapperspb.StringValue, error) {
		return &wrapperspb.StringValue{Value: "hello"}, nil
	})

	resp, err := fn(context.Background(), &emptypb.Empty{})
	if err != nil {
		t.Fatalf("Wrap: %v", err)
	}
	sv, ok := resp.(*wrapperspb.StringValue)
	if !ok {
		t.Fatal("wrong type")
	}
	if sv.Value != "hello" {
		t.Errorf("Value = %q", sv.Value)
	}
}

// TestMetadataFor exercises the canonical accessor a gateway uses to read a
// stored handler's metadata WITHOUT the executable Func — the same shape a
// domain Types() function returns. Func must be cleared on the returned value,
// and the Middleware/Tags slices must be defensive copies so mutating the
// result cannot rewrite registry state.
func TestMetadataFor(t *testing.T) {
	reg := NewRegistry()

	called := false
	src := Handler{
		Namespace: "hstles",
		Domain:    "identity",
		Operation: "CreateUser",
		Func: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			called = true
			return req, nil
		},
		Request:  (*emptypb.Empty)(nil),
		Response: (*wrapperspb.StringValue)(nil),
		Scope:    ScopeTenant,
		Tags:     []string{"users", "write"},
	}
	if err := reg.Register(src); err != nil {
		t.Fatalf("Register: %v", err)
	}

	got, ok := reg.MetadataFor("hstles.identity.CreateUser")
	if !ok {
		t.Fatal("MetadataFor: not found")
	}
	if got.Func != nil {
		t.Error("MetadataFor must strip Func")
	}
	if got.Scope != ScopeTenant {
		t.Errorf("Scope = %v, want ScopeTenant", got.Scope)
	}
	if got.Domain != "identity" || got.Operation != "CreateUser" {
		t.Errorf("identity drift: %+v", got)
	}

	// Mutating the returned Tags must not corrupt the registry copy.
	got.Tags = append(got.Tags, "mutated")
	stored, _ := reg.Get("hstles.identity.CreateUser")
	for _, tg := range stored.Tags {
		if tg == "mutated" {
			t.Error("MetadataFor returned Tags aliasing registry state — defensive-copy contract broken")
		}
	}

	// Missing FQN
	if _, ok := reg.MetadataFor("nope.nope.nope"); ok {
		t.Error("MetadataFor: expected miss")
	}
	if called {
		t.Error("Func must not be invoked by MetadataFor")
	}
}

// TestStripFuncs exercises the canonical helper a domain register.go uses
// to derive Types() from its single authoritative Register() handler list.
// Func must be cleared on every element; every other field preserved.
func TestStripFuncs(t *testing.T) {
	if StripFuncs(nil) != nil {
		t.Error("StripFuncs(nil) should return nil")
	}
	if StripFuncs([]Handler{}) != nil {
		t.Error("StripFuncs([]) should return nil")
	}

	src := []Handler{
		{
			Namespace: "hstles", Domain: "identity", Operation: "Op1",
			Func: func(ctx context.Context, req proto.Message) (proto.Message, error) {
				return req, nil
			},
			Request:  (*emptypb.Empty)(nil),
			Response: (*wrapperspb.StringValue)(nil),
			Scope:    ScopeTenant,
			Tags:     []string{"a", "b"},
			OpClass:  OpRealtime,
		},
		{
			Namespace: "hstles", Domain: "identity", Operation: "Op2",
			Scope: ScopeOrg,
		},
	}

	out := StripFuncs(src)
	if len(out) != 2 {
		t.Fatalf("len = %d, want 2", len(out))
	}
	for i, h := range out {
		if h.Func != nil {
			t.Errorf("out[%d].Func not stripped", i)
		}
	}
	if out[0].Scope != ScopeTenant || out[0].OpClass != OpRealtime {
		t.Errorf("Scope/OpClass not preserved: %+v", out[0])
	}
	// Source must still carry its Func — StripFuncs must not mutate input.
	if src[0].Func == nil {
		t.Error("StripFuncs mutated input slice's Func field")
	}
	// Mutating returned Tags must not corrupt source.
	out[0].Tags = append(out[0].Tags, "mutated")
	for _, tg := range src[0].Tags {
		if tg == "mutated" {
			t.Error("StripFuncs returned Tags aliasing source — defensive-copy contract broken")
		}
	}
}

// atomicH builds a minimal valid handler under one role for the atomicity tests.
func atomicH(op string) Handler {
	return Handler{
		Namespace: "hstles", Domain: "identity", Operation: op,
		Scope:    ScopeNone,
		Request:  (*emptypb.Empty)(nil),
		Response: (*emptypb.Empty)(nil),
	}
}

func withFunc(h Handler) Handler {
	h.Func = func(ctx context.Context, req proto.Message) (proto.Message, error) { return req, nil }
	return h
}

// TestRegisterAllAtomic_NoPartialPrefix is the hostile overlay for RegisterAll:
// a batch whose MIDDLE member collides with an already-registered handler must
// fail the whole call and leave the registry byte-unchanged — no earlier member
// may survive. A non-atomic loop-Register stores the first member before the
// middle fails, so this assertion kills that mutation.
func TestRegisterAllAtomic_NoPartialPrefix(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Register(atomicH("Existing")); err != nil {
		t.Fatalf("baseline: %v", err)
	}
	batch := []Handler{atomicH("First"), atomicH("Existing"), atomicH("Third")}
	if err := reg.RegisterAll(batch); err == nil {
		t.Fatal("RegisterAll with a colliding member must fail")
	}
	if reg.Count() != 1 {
		t.Errorf("partial prefix: Count = %d, want 1 (only the baseline)", reg.Count())
	}
	if _, ok := reg.Get("hstles.identity.First"); ok {
		t.Error("First was committed before the collision — partial prefix leaked")
	}
	if _, ok := reg.Get("hstles.identity.Third"); ok {
		t.Error("Third was committed despite the failed batch")
	}
}

// TestRegisterProxyAtomic_NoPartialPrefix mirrors the overlay for RegisterProxy
// (parity) and confirms Func is stripped on the success path.
func TestRegisterProxyAtomic_NoPartialPrefix(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Register(atomicH("Existing")); err != nil {
		t.Fatalf("baseline: %v", err)
	}
	batch := []Handler{withFunc(atomicH("PFirst")), atomicH("Existing"), atomicH("PThird")}
	if err := reg.RegisterProxy(batch); err == nil {
		t.Fatal("RegisterProxy with a colliding member must fail")
	}
	if reg.Count() != 1 {
		t.Errorf("RegisterProxy partial prefix: Count = %d, want 1", reg.Count())
	}
	if err := reg.RegisterProxy([]Handler{withFunc(atomicH("PA")), withFunc(atomicH("PB"))}); err != nil {
		t.Fatalf("clean RegisterProxy: %v", err)
	}
	if reg.Count() != 3 {
		t.Errorf("Count = %d, want 3", reg.Count())
	}
	pa, ok := reg.Get("hstles.identity.PA")
	if !ok || pa.Func != nil {
		t.Error("RegisterProxy must store the handler with Func stripped")
	}
}

// TestRegisterAllAtomic_WithinBatchDuplicate: two members with the same FQN in
// one batch reject atomically — neither is stored.
func TestRegisterAllAtomic_WithinBatchDuplicate(t *testing.T) {
	reg := NewRegistry()
	batch := []Handler{atomicH("Dup"), atomicH("Other"), atomicH("Dup")}
	if err := reg.RegisterAll(batch); err == nil {
		t.Fatal("within-batch duplicate must fail")
	}
	if reg.Count() != 0 {
		t.Errorf("within-batch dup left %d handlers; want 0 (atomic)", reg.Count())
	}
}

// TestRegisterAllAtomic_Success: a clean batch stores every member.
func TestRegisterAllAtomic_Success(t *testing.T) {
	reg := NewRegistry()
	if err := reg.RegisterAll([]Handler{atomicH("A"), atomicH("B"), atomicH("C")}); err != nil {
		t.Fatalf("clean RegisterAll: %v", err)
	}
	if reg.Count() != 3 {
		t.Errorf("Count = %d, want 3", reg.Count())
	}
}
