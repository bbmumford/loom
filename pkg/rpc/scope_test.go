/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package rpc

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/bbmumford/loom/pkg/rpc/scope"
)

// refPolicy reproduces the platform's historical presence semantics as a
// CONSUMER-side PresencePolicy so loom's EnforceScope can be exercised end to
// end. loom no longer contains this logic — it delegates every non-None,
// declared tag to whatever policy the deployment installs. The reference
// policy keys on the opaque axis names loom binds to the wire fields.
func refPolicy(platforms ...string) scope.PresencePolicyFunc {
	allow := make(map[string]struct{}, len(platforms))
	for _, p := range platforms {
		if p != "" {
			allow[p] = struct{}{}
		}
	}
	return func(declared scope.ScopeTag, id scope.AuthenticatedIdentity) (bool, string) {
		tenant := id.Scope(scope.AxisTransportPinned)
		switch declared {
		case scope.ScopeNone:
			return true, ""
		case scope.ScopePlatform:
			if tenant == "" {
				return false, "platform tenant required"
			}
			if _, ok := allow[tenant]; !ok {
				return false, "tenant not in platform allowlist"
			}
		case scope.ScopeTenant:
			if tenant == "" {
				return false, "tenant required"
			}
		case scope.ScopeOrg:
			if tenant == "" {
				return false, "tenant required"
			}
			if id.Scope(scope.AxisWithinTransport) == "" {
				return false, "organisation required"
			}
		case scope.ScopeUser:
			if tenant == "" {
				return false, "tenant required"
			}
			if id.Subject == "" {
				return false, "user required"
			}
		case scope.ScopeProfile:
			if tenant == "" {
				return false, "tenant required"
			}
		default:
			return false, "unknown scope"
		}
		return true, ""
	}
}

// idAxes builds an opaque identity from the three deployed axes, omitting any
// axis whose value is empty (an absent axis must stay absent).
func idAxes(tenant, org, user string) AuthenticatedScopeIdentity {
	id := AuthenticatedScopeIdentity{Subject: user}
	if tenant != "" {
		id.Scopes = append(id.Scopes, scope.ScopeValue{Name: scope.AxisTransportPinned, Value: tenant})
	}
	if org != "" {
		id.Scopes = append(id.Scopes, scope.ScopeValue{Name: scope.AxisWithinTransport, Value: org})
	}
	return id
}

// TestWithAuthenticatedScopeIdentity_FailsClosedOnInvalid confirms the rpc-layer
// stamper inherits the leaf validation: a duplicate-axis identity is not admitted
// and does not leave a poisoned prior snapshot readable.
func TestWithAuthenticatedScopeIdentity_FailsClosedOnInvalid(t *testing.T) {
	base := WithAuthenticatedScopeIdentity(context.Background(), idAxes("orbtr", "acme", "u1"))
	if AuthenticatedScopeIdentityFromContext(base).Scope(scope.AxisTransportPinned) != "orbtr" {
		t.Fatal("precondition: prior valid identity must be present")
	}
	dup := AuthenticatedScopeIdentity{Scopes: []scope.ScopeValue{
		{Name: scope.AxisTransportPinned, Value: "orbtr"},
		{Name: scope.AxisTransportPinned, Value: "evil"},
	}}
	got := AuthenticatedScopeIdentityFromContext(WithAuthenticatedScopeIdentity(base, dup))
	if got.Scope(scope.AxisTransportPinned) != "" {
		t.Fatalf("duplicate-axis identity was admitted through the rpc wrapper: %q", got.Scope(scope.AxisTransportPinned))
	}
}

func TestEnforceScope_None(t *testing.T) {
	h := &Handler{Namespace: "ns", Domain: "d", Operation: "Op", Scope: ScopeNone}
	// ScopeNone admits even with a nil policy — it is the handler's explicit
	// opt-out of enforcement.
	if err := EnforceScope(context.Background(), h, nil); err != nil {
		t.Fatalf("ScopeNone should always pass: %v", err)
	}
}

// TestEnforceScope_NilPolicyDeniesNonNone is a fail-CLOSED proof: without a
// consumer policy loom cannot judge presence, so any declared (non-None)
// handler is denied rather than silently admitted.
func TestEnforceScope_NilPolicyDeniesNonNone(t *testing.T) {
	for _, s := range []TenantScope{ScopeTenant, ScopeOrg, ScopeUser, ScopeProfile, ScopePlatform} {
		h := &Handler{Namespace: "ns", Domain: "d", Operation: "Op", Scope: s}
		ctx := WithAuthenticatedScopeIdentity(context.Background(), idAxes("orbtr", "acme", "u1"))
		if err := EnforceScope(ctx, h, nil); !errors.Is(err, ErrScopeDenied) {
			t.Fatalf("nil policy must deny scope %q, got %v", s, err)
		}
	}
}

// TestEnforceScope_UnsetDenies is a fail-CLOSED proof: an undeclared (zero
// value) scope is refused even when a permissive policy is installed.
func TestEnforceScope_UnsetDenies(t *testing.T) {
	h := &Handler{Namespace: "ns", Domain: "d", Operation: "Op", Scope: scope.ScopeUnset}
	allowAll := scope.PresencePolicyFunc(func(scope.ScopeTag, scope.AuthenticatedIdentity) (bool, string) {
		return true, ""
	})
	if err := EnforceScope(context.Background(), h, allowAll); !errors.Is(err, ErrScopeDenied) {
		t.Fatalf("ScopeUnset must deny even under a permissive policy, got %v", err)
	}
}

// TestEnforceScope_PolicyFalseDenies proves a policy that reports unsatisfied
// denies, and the reason is surfaced in the wrapped error.
func TestEnforceScope_PolicyFalseDenies(t *testing.T) {
	h := &Handler{Namespace: "ns", Domain: "d", Operation: "Op", Scope: ScopeTenant}
	deny := scope.PresencePolicyFunc(func(scope.ScopeTag, scope.AuthenticatedIdentity) (bool, string) {
		return false, "policy-said-no"
	})
	err := EnforceScope(context.Background(), h, deny)
	if !errors.Is(err, ErrScopeDenied) {
		t.Fatalf("policy returning false must deny, got %v", err)
	}
	if err == nil || !contains(err.Error(), "policy-said-no") {
		t.Fatalf("denial reason must be surfaced, got %v", err)
	}
}

// TestEnforceScope_OpaqueTagFailsClosed is the safety pair to loom's
// taxonomy-agnostic admission (IsDeclared admits any non-sentinel tag): a
// consumer-defined opaque axis loom does not bless must still fail CLOSED at
// enforcement. With a nil policy it is denied (no evaluator), and with a policy
// that does not recognise it the policy's own deny is surfaced — the widened
// admission never turns an unrecognised tag into a silent admit.
func TestEnforceScope_OpaqueTagFailsClosed(t *testing.T) {
	h := &Handler{Namespace: "ns", Domain: "d", Operation: "Op", Scope: scope.ScopeTag("partition")}
	ctx := WithAuthenticatedScopeIdentity(context.Background(), idAxes("orbtr", "acme", "u1"))
	// nil policy: loom cannot judge presence -> deny.
	if err := EnforceScope(ctx, h, nil); !errors.Is(err, ErrScopeDenied) {
		t.Fatalf("opaque tag under nil policy must deny, got %v", err)
	}
	// A policy that only recognises the blessed ladder denies the opaque tag.
	ladderOnly := scope.PresencePolicyFunc(func(declared scope.ScopeTag, _ scope.AuthenticatedIdentity) (bool, string) {
		if scope.IsDeclared(declared) && declared != scope.ScopeTag("partition") {
			return true, ""
		}
		return false, "unrecognised-axis"
	})
	err := EnforceScope(ctx, h, ladderOnly)
	if !errors.Is(err, ErrScopeDenied) || err == nil || !contains(err.Error(), "unrecognised-axis") {
		t.Fatalf("opaque tag must fail closed via policy deny, got %v", err)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// TestParseScopes verifies #K-32 wire decoding: space-joined → slice, with
// empty + stray-space robustness (strings.Fields drops empty tokens, so no
// phantom scope from a double space).
func TestParseScopes(t *testing.T) {
	cases := []struct {
		raw  string
		want []string
	}{
		{"", nil},
		{"storage:read", []string{"storage:read"}},
		{"storage:read assistant:write", []string{"storage:read", "assistant:write"}},
		{"  a   b  ", []string{"a", "b"}},
	}
	for _, c := range cases {
		got := ParseScopes(c.raw)
		if len(got) != len(c.want) {
			t.Errorf("ParseScopes(%q) = %v, want %v", c.raw, got, c.want)
			continue
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Errorf("ParseScopes(%q)[%d] = %q, want %q", c.raw, i, got[i], c.want[i])
			}
		}
	}
}

// TestScopesFromContext_RoundTrip verifies the receive-side accessor decodes
// scopes surfaced onto ctx via WithRPCContext.
func TestScopesFromContext_RoundTrip(t *testing.T) {
	ctx := WithRPCContext(context.Background(), map[string]string{"scopes": "a b c"})
	got := ScopesFromContext(ctx)
	if len(got) != 3 || got[0] != "a" || got[1] != "b" || got[2] != "c" {
		t.Errorf("ScopesFromContext = %v, want [a b c]", got)
	}
	if s := ScopesFromContext(context.Background()); s != nil {
		t.Errorf("ScopesFromContext on bare ctx = %v, want nil", s)
	}
}

// TestBridgeWireIdentity_NilSafe verifies the send-side bridge is a no-op
// when the rpc context map is absent (no panic, ctx unchanged), and does not
// panic on a populated map.
func TestBridgeWireIdentity_NilSafe(t *testing.T) {
	ctx := context.Background()
	if got := bridgeWireIdentity(ctx); got != ctx {
		t.Error("bridgeWireIdentity on bare ctx should return ctx unchanged")
	}
	ctx = WithRPCContext(context.Background(), map[string]string{"scopes": "x y", "userId": "u1"})
	_ = bridgeWireIdentity(ctx) // must not panic
}

func TestEnforceScope_Platform(t *testing.T) {
	h := &Handler{Namespace: "hstles", Domain: "identity", Operation: "Mint", Scope: ScopePlatform}
	policy := refPolicy("orbtr", "hstles")

	// Caller without tenant — denied
	if err := EnforceScope(context.Background(), h, policy); !errors.Is(err, ErrScopeDenied) {
		t.Errorf("expected ErrScopeDenied for missing tenant, got %v", err)
	}

	// Caller with non-platform tenant — denied
	ctx := WithAuthenticatedScopeIdentity(context.Background(), idAxes("stranger", "", ""))
	if err := EnforceScope(ctx, h, policy); !errors.Is(err, ErrScopeDenied) {
		t.Errorf("expected ErrScopeDenied for stranger tenant, got %v", err)
	}

	// Caller with platform tenant — pass
	ctx = WithAuthenticatedScopeIdentity(context.Background(), idAxes("orbtr", "", ""))
	if err := EnforceScope(ctx, h, policy); err != nil {
		t.Errorf("expected pass for platform tenant, got %v", err)
	}
}

func TestEnforceScope_Tenant(t *testing.T) {
	h := &Handler{Namespace: "ns", Domain: "d", Operation: "Op", Scope: ScopeTenant}
	policy := refPolicy()
	if err := EnforceScope(context.Background(), h, policy); !errors.Is(err, ErrScopeDenied) {
		t.Errorf("expected ErrScopeDenied for missing tenant, got %v", err)
	}
	ctx := WithAuthenticatedScopeIdentity(context.Background(), idAxes("orbtr", "", ""))
	if err := EnforceScope(ctx, h, policy); err != nil {
		t.Errorf("expected pass with tenant, got %v", err)
	}
}

func TestEnforceScope_Org(t *testing.T) {
	h := &Handler{Namespace: "ns", Domain: "d", Operation: "Op", Scope: ScopeOrg}
	policy := refPolicy()

	// Tenant only — denied for missing org
	ctx := WithAuthenticatedScopeIdentity(context.Background(), idAxes("orbtr", "", ""))
	if err := EnforceScope(ctx, h, policy); !errors.Is(err, ErrScopeDenied) {
		t.Errorf("expected ErrScopeDenied for missing org, got %v", err)
	}

	// Tenant + org — pass
	ctx = WithAuthenticatedScopeIdentity(context.Background(), idAxes("orbtr", "acme", ""))
	if err := EnforceScope(ctx, h, policy); err != nil {
		t.Errorf("expected pass with tenant+org, got %v", err)
	}

	// Mutable wire-map hints cannot manufacture either prerequisite: the policy
	// reads only the typed identity, never the rpc context map.
	ctx = WithRPCContext(context.Background(), map[string]string{
		"tenantId": "orbtr",
		"orgId":    "forged",
	})
	if err := EnforceScope(ctx, h, policy); !errors.Is(err, ErrScopeDenied) {
		t.Errorf("expected ErrScopeDenied for wire-only tenant+org hints, got %v", err)
	}

	// A conflicting wire hint cannot replace an independently authenticated
	// organisation. Presence enforcement consumes only the typed snapshot.
	ctx = WithRPCContext(ctx, map[string]string{"orgId": "evil"})
	ctx = WithAuthenticatedScopeIdentity(ctx, idAxes("orbtr", "acme", ""))
	if err := EnforceScope(ctx, h, policy); err != nil {
		t.Errorf("authenticated organisation must survive conflicting wire hint: %v", err)
	}
}

func TestEnforceScope_User(t *testing.T) {
	h := &Handler{Namespace: "ns", Domain: "d", Operation: "Op", Scope: ScopeUser}
	policy := refPolicy()

	ctx := WithAuthenticatedScopeIdentity(context.Background(), idAxes("orbtr", "", ""))
	if err := EnforceScope(ctx, h, policy); !errors.Is(err, ErrScopeDenied) {
		t.Errorf("expected ErrScopeDenied for missing user, got %v", err)
	}

	ctx = WithAuthenticatedScopeIdentity(context.Background(), idAxes("orbtr", "", "u1"))
	if err := EnforceScope(ctx, h, policy); err != nil {
		t.Errorf("expected pass with tenant+user, got %v", err)
	}
}

func TestEnforceScope_UnknownScope(t *testing.T) {
	// An unrecognised tag is not ScopeNone and not ScopeUnset, so it reaches
	// the policy, which reports it unsatisfied — denied.
	h := &Handler{Namespace: "ns", Domain: "d", Operation: "Op", Scope: "bogus"}
	if err := EnforceScope(context.Background(), h, refPolicy()); !errors.Is(err, ErrScopeDenied) {
		t.Errorf("expected ErrScopeDenied for unknown scope, got %v", err)
	}
}

func TestEnforceScope_NilHandler(t *testing.T) {
	if err := EnforceScope(context.Background(), nil, nil); !errors.Is(err, ErrScopeDenied) {
		t.Errorf("expected ErrScopeDenied for nil handler, got %v", err)
	}
}

func TestWithRPCContext_Merge(t *testing.T) {
	ctx := WithRPCContext(context.Background(), map[string]string{"a": "1", "b": "2"})
	ctx = WithRPCContext(ctx, map[string]string{"b": "B", "c": "3"})

	m := rpcContextMap(ctx)
	if m["a"] != "1" || m["b"] != "B" || m["c"] != "3" {
		t.Errorf("merge result wrong: %+v", m)
	}
}

func TestRegistryDispatch_UsesOnlyAuthenticatedScopeIdentity(t *testing.T) {
	var calls int
	h := Handler{
		Namespace: "test",
		Domain:    "scope",
		Operation: "OrgRead",
		Scope:     ScopeOrg,
		Request:   (*structpb.Struct)(nil),
		Response:  (*structpb.Struct)(nil),
		Func: func(_ context.Context, req proto.Message) (proto.Message, error) {
			calls++
			return req, nil
		},
	}
	registry := NewRegistry()
	registry.SetPresencePolicy(refPolicy())
	if err := registry.Register(h); err != nil {
		t.Fatalf("Register: %v", err)
	}

	hints := map[string]string{
		"tenantId":  "orbtr",
		"tenant_id": "evil",
		"orgId":     "forged",
		"org_id":    "also-forged",
		"userId":    "forged-user",
	}
	hintOnly := WithRPCContext(context.Background(), hints)
	hints["orgId"] = "mutated-after-context"
	if _, err := registry.Dispatch(hintOnly, h.FQN(), nil); !errors.Is(err, ErrScopeDenied) {
		t.Fatalf("map-only identity must deny with ErrScopeDenied, got %v", err)
	}
	if calls != 0 {
		t.Fatalf("handler ran on map-only identity: calls=%d", calls)
	}

	exact := WithAuthenticatedScopeIdentity(hintOnly, idAxes("orbtr", "org-1", "user-1"))
	hints["tenantId"] = "mutated-too"
	hints["orgId"] = "conflicting"
	if _, err := registry.Dispatch(exact, h.FQN(), nil); err != nil {
		t.Fatalf("exact authenticated axes must pass despite mutable hints: %v", err)
	}
	if calls != 1 {
		t.Fatalf("handler calls=%d, want 1", calls)
	}
}

// TestRegistryDispatch_NilPolicyFailsClosed proves a registry with no
// PresencePolicy denies a non-None handler rather than silently admitting it.
func TestRegistryDispatch_NilPolicyFailsClosed(t *testing.T) {
	var calls int
	h := Handler{
		Namespace: "test",
		Domain:    "scope",
		Operation: "TenantRead",
		Scope:     ScopeTenant,
		Request:   (*structpb.Struct)(nil),
		Response:  (*structpb.Struct)(nil),
		Func: func(_ context.Context, req proto.Message) (proto.Message, error) {
			calls++
			return req, nil
		},
	}
	registry := NewRegistry() // no SetPresencePolicy → nil policy
	if err := registry.Register(h); err != nil {
		t.Fatalf("Register: %v", err)
	}
	ctx := WithAuthenticatedScopeIdentity(context.Background(), idAxes("orbtr", "", ""))
	if _, err := registry.Dispatch(ctx, h.FQN(), nil); !errors.Is(err, ErrScopeDenied) {
		t.Fatalf("nil-policy registry must deny non-None handler, got %v", err)
	}
	if calls != 0 {
		t.Fatalf("handler ran under nil policy: calls=%d", calls)
	}
}

func TestExtractRole(t *testing.T) {
	cases := []struct{ in, out string }{
		{"hstles.identity.MintSession", "hstles.identity"},
		{"orbtr.device.List", "orbtr.device"},
		// malformed inputs all return "" — previous impl returned the whole
		// string for these.
		{"bogus", ""},
		{"bogus.x", ""}, // only one dot
		{".x.y", ""},    // empty namespace
		{"x.y.", ""},    // empty operation
		{"x..y", ""},    // empty domain
		{"", ""},        // empty
		{"....", ""},    // dots only
	}
	for _, c := range cases {
		if got := ExtractRole(c.in); got != c.out {
			t.Errorf("ExtractRole(%q) = %q, want %q", c.in, got, c.out)
		}
	}
}

func TestRegistry_RegisterCopiesSlices(t *testing.T) {
	reg := NewRegistry()
	tags := []string{"x"}
	if err := reg.Register(Handler{Namespace: "n", Domain: "d", Operation: "P", Scope: ScopeNone, Tags: tags}); err != nil {
		t.Fatalf("register: %v", err)
	}
	// Mutate caller's slice — registry must not see it
	tags[0] = "y"
	h, ok := reg.Get("n.d.P")
	if !ok {
		t.Fatal("missing")
	}
	if h.Tags[0] != "x" {
		t.Errorf("registry Tags aliased caller's slice: got %q", h.Tags[0])
	}
}
