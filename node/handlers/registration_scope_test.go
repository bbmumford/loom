/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package handlers

import (
	"context"
	"strings"
	"testing"

	"github.com/bbmumford/loom/ports"
)

type registrationScopeHandler struct {
	name  string
	scope Scope
}

func (h *registrationScopeHandler) Name() string               { return h.name }
func (h *registrationScopeHandler) Role() string               { return "test.scope" }
func (h *registrationScopeHandler) RequiresAuth() bool         { return false }
func (h *registrationScopeHandler) AllowedAuthTypes() []string { return nil }
func (h *registrationScopeHandler) Scopes() []string           { return nil }
func (h *registrationScopeHandler) Scope() Scope         { return h.scope }
func (h *registrationScopeHandler) AllowedPartitions() []string   { return nil }
func (h *registrationScopeHandler) ExecuteRPC(context.Context, *RPCRequest) (*RPCResponse, error) {
	return &RPCResponse{Success: true}, nil
}
func (h *registrationScopeHandler) ExecuteTask(context.Context, *Task) (*TaskResult, error) {
	return &TaskResult{Status: TaskStatusCompleted}, nil
}
func (h *registrationScopeHandler) HandleStream(context.Context, MessageStream) error {
	return nil
}

// TestHandlerRegistryRejectsSentinelTenantScopeBeforeMutation proves the legacy
// handlers registration path refuses ONLY the two non-declarable sentinels
// (TenantScopeUnset zero value, ScopeUnknown proto-decay marker) before
// mutating the handler map — matching the rpc.Registry path. A consumer-defined
// opaque axis admits (see TestHandlerRegistryAcceptsOpaqueConsumerAxis); loom
// no longer enumerates a fixed ladder at registration.
func TestHandlerRegistryRejectsSentinelTenantScopeBeforeMutation(t *testing.T) {
	registrars := []struct {
		name string
		call func(*HandlerRegistry, *registrationScopeHandler) error
	}{
		{name: "rpc", call: func(r *HandlerRegistry, h *registrationScopeHandler) error { return r.RegisterRPC(h) }},
		{name: "task", call: func(r *HandlerRegistry, h *registrationScopeHandler) error { return r.RegisterTask(h) }},
		{name: "stream", call: func(r *HandlerRegistry, h *registrationScopeHandler) error { return r.RegisterStream(h) }},
		{name: "meta", call: func(r *HandlerRegistry, h *registrationScopeHandler) error { return r.RegisterHandler(h) }},
	}
	scopes := []struct {
		name  string
		scope Scope
	}{
		{name: "zero"},
		{name: "unknown", scope: ScopeUnknown},
	}

	for _, registrar := range registrars {
		t.Run(registrar.name, func(t *testing.T) {
			for _, tc := range scopes {
				t.Run(tc.name, func(t *testing.T) {
					reg := NewHandlerRegistry()
					h := &registrationScopeHandler{name: registrar.name + "." + tc.name, scope: tc.scope}
					err := registrar.call(reg, h)
					if err == nil {
						t.Fatalf("registration accepted sentinel scope %q", tc.scope)
					}
					if !strings.Contains(err.Error(), "non-declarable scope") {
						t.Fatalf("registration error = %q, want non-declarable-scope diagnostic", err)
					}
					if _, ok := reg.GetMeta(h.name); ok {
						t.Fatal("rejected registration mutated handler map")
					}
				})
			}
		})
	}
}

// TestHandlerRegistryAcceptsOpaqueConsumerAxis proves the legacy handlers path
// admits a consumer-defined opaque scope tag loom does not bless — its meaning
// is the consumer PresencePolicy's to judge, and an unrecognised tag fails
// CLOSED at enforcement (proven in the rpc package).
func TestHandlerRegistryAcceptsOpaqueConsumerAxis(t *testing.T) {
	registrars := []struct {
		name string
		call func(*HandlerRegistry, *registrationScopeHandler) error
	}{
		{name: "rpc", call: func(r *HandlerRegistry, h *registrationScopeHandler) error { return r.RegisterRPC(h) }},
		{name: "task", call: func(r *HandlerRegistry, h *registrationScopeHandler) error { return r.RegisterTask(h) }},
		{name: "stream", call: func(r *HandlerRegistry, h *registrationScopeHandler) error { return r.RegisterStream(h) }},
		{name: "meta", call: func(r *HandlerRegistry, h *registrationScopeHandler) error { return r.RegisterHandler(h) }},
	}
	for _, registrar := range registrars {
		t.Run(registrar.name, func(t *testing.T) {
			reg := NewHandlerRegistry()
			h := &registrationScopeHandler{name: registrar.name + ".partition", scope: Scope("partition")}
			if err := registrar.call(reg, h); err != nil {
				t.Fatalf("registration rejected opaque consumer axis: %v", err)
			}
			if _, ok := reg.GetMeta(h.name); !ok {
				t.Fatal("admitted opaque-axis handler not present in handler map")
			}
		})
	}
}

func TestHandlerRegistryAcceptsExplicitTenantScopeNone(t *testing.T) {
	reg := NewHandlerRegistry()
	h := &registrationScopeHandler{name: "public.health", scope: ScopeNone}
	if err := reg.RegisterRPC(h); err != nil {
		t.Fatalf("RegisterRPC explicit ScopeNone: %v", err)
	}
	stored, ok := reg.GetMeta(h.name)
	if !ok || stored.Scope() != ScopeNone {
		t.Fatalf("stored explicit ScopeNone = (%+v, %v)", stored, ok)
	}
}

func TestBaseHandlerDoesNotSupplyAPermissiveDefault(t *testing.T) {
	reg := NewHandlerRegistry()
	h := NewBaseHandler(BaseHandlerConfig{Name: "base.unset", Role: "test.scope"})
	if err := reg.RegisterHandler(h); err == nil {
		t.Fatal("BaseHandler registered without an explicit tenant-scope declaration")
	}
}

func TestRegistrationScopeReservesButDoesNotExposeHandlersUntilPublication(t *testing.T) {
	reg := NewHandlerRegistry()
	scope, err := reg.OpenRegistrationScope(
		"owned-role",
		staticTaskAdmission{accepting: true},
	)
	if err != nil {
		t.Fatalf("OpenRegistrationScope: %v", err)
	}
	first := &roleRegistrationScopeHandler{
		registrationScopeHandler: &registrationScopeHandler{
			name:  "owned-role.pending",
			scope: ScopeNone,
		},
		role: "owned-role",
	}
	if _, err := reg.RegisterRPCScoped(scope, first); err != nil {
		t.Fatalf("RegisterRPCScoped: %v", err)
	}
	if _, ok := reg.Resolve(first.Name()); ok {
		t.Fatal("pending scoped handler was resolvable before publication")
	}
	if _, ok := reg.GetMeta(first.Name()); ok {
		t.Fatal("pending scoped handler was visible through GetMeta")
	}
	if got := reg.Count(); got != 0 {
		t.Fatalf("pending scoped handler changed public count to %d", got)
	}
	if got := reg.List(); len(got) != 0 {
		t.Fatalf("pending scoped handler changed public list to %v", got)
	}
	if err := reg.RegisterRPC(first); err == nil ||
		!strings.Contains(err.Error(), "reserved") {
		t.Fatalf("same-name registration during staging = %v, want reservation error", err)
	}

	if err := reg.PublishRegistrationScope(scope); err != nil {
		t.Fatalf("PublishRegistrationScope: %v", err)
	}
	if got, ok := reg.GetMeta(first.Name()); !ok || got != first {
		t.Fatalf("published handler = (%p, %v), want %p", got, ok, first)
	}
	if got := reg.Count(); got != 1 {
		t.Fatalf("published count = %d, want 1", got)
	}

	late := &roleRegistrationScopeHandler{
		registrationScopeHandler: &registrationScopeHandler{
			name:  "owned-role.late",
			scope: ScopeNone,
		},
		role: "owned-role",
	}
	if _, err := reg.RegisterRPCScoped(scope, late); err != nil {
		t.Fatalf("late RegisterRPCScoped: %v", err)
	}
	if got, ok := reg.GetMeta(late.Name()); !ok || got != late {
		t.Fatalf("late published handler = (%p, %v), want %p", got, ok, late)
	}
}

func TestRegistrationScopePublicationRacesWithLateRegistrationAtomically(t *testing.T) {
	for range 100 {
		reg := NewHandlerRegistry()
		scope, err := reg.OpenRegistrationScope(
			"owned-role",
			staticTaskAdmission{accepting: true},
		)
		if err != nil {
			t.Fatalf("OpenRegistrationScope: %v", err)
		}
		first := &roleRegistrationScopeHandler{
			registrationScopeHandler: &registrationScopeHandler{
				name:  "owned-role.initial",
				scope: ScopeNone,
			},
			role: "owned-role",
		}
		late := &roleRegistrationScopeHandler{
			registrationScopeHandler: &registrationScopeHandler{
				name:  "owned-role.concurrent",
				scope: ScopeNone,
			},
			role: "owned-role",
		}
		if _, err := reg.RegisterRPCScoped(scope, first); err != nil {
			t.Fatalf("RegisterRPCScoped(initial): %v", err)
		}

		start := make(chan struct{})
		results := make(chan error, 2)
		go func() {
			<-start
			results <- reg.PublishRegistrationScope(scope)
		}()
		go func() {
			<-start
			_, err := reg.RegisterRPCScoped(scope, late)
			results <- err
		}()
		close(start)
		for range 2 {
			if err := <-results; err != nil {
				t.Fatalf("concurrent publication/registration: %v", err)
			}
		}

		for _, want := range []*roleRegistrationScopeHandler{first, late} {
			got, ok := reg.GetMeta(want.Name())
			if !ok || got != want {
				t.Fatalf(
					"published handler %q = (%p, %v), want %p",
					want.Name(),
					got,
					ok,
					want,
				)
			}
		}
		if got := reg.Count(); got != 2 {
			t.Fatalf("published count = %d, want 2", got)
		}
	}
}

func TestClosingUnpublishedRegistrationScopeReleasesExactReservations(t *testing.T) {
	reg := NewHandlerRegistry()
	scope, err := reg.OpenRegistrationScope(
		"owned-role",
		staticTaskAdmission{accepting: true},
	)
	if err != nil {
		t.Fatalf("OpenRegistrationScope: %v", err)
	}
	pending := &roleRegistrationScopeHandler{
		registrationScopeHandler: &registrationScopeHandler{
			name:  "owned-role.aborted",
			scope: ScopeNone,
		},
		role: "owned-role",
	}
	if _, err := reg.RegisterRPCScoped(scope, pending); err != nil {
		t.Fatalf("RegisterRPCScoped: %v", err)
	}
	if _, ok := reg.CloseRegistrationScope(scope); !ok {
		t.Fatal("CloseRegistrationScope rejected pending scope")
	}
	if err := reg.RegisterRPC(pending); err != nil {
		t.Fatalf("pending reservation survived scope close: %v", err)
	}
}

func TestUnregisterExactPreservesSameNameReplacementAndRejectsForeignHandle(t *testing.T) {
	reg := NewHandlerRegistry()
	other := NewHandlerRegistry()
	h := &registrationScopeHandler{
		name:  "exact.identity",
		scope: ScopeNone,
	}
	firstScope, err := reg.OpenRegistrationScope(
		h.Role(),
		staticTaskAdmission{accepting: true},
	)
	if err != nil {
		t.Fatalf("OpenRegistrationScope: %v", err)
	}
	first, err := reg.RegisterHandlerScoped(firstScope, h)
	if err != nil {
		t.Fatalf("RegisterHandlerScoped(first): %v", err)
	}
	if other.UnregisterExact(first) {
		t.Fatal("foreign registry accepted an exact-registration handle")
	}
	if !reg.UnregisterExact(first) {
		t.Fatal("issuing registry rejected the exact-registration handle")
	}

	// Re-register the SAME handler object under the SAME name. Identity is the
	// immutable registration entry, not name or object address.
	if err := reg.RegisterHandler(h); err != nil {
		t.Fatalf("RegisterHandler(replacement): %v", err)
	}
	if reg.UnregisterExact(first) {
		t.Fatal("stale exact handle removed a same-name replacement")
	}
	if got, ok := reg.GetMeta(h.Name()); !ok || got != h {
		t.Fatalf("replacement = (%p, %v), want original object retained", got, ok)
	}
}

func TestRegistrationScopeRejectsWrongRoleAndSealedOrFabricatedCapability(t *testing.T) {
	reg := NewHandlerRegistry()
	registrationScope, err := reg.OpenRegistrationScope(
		"owned-role",
		staticTaskAdmission{accepting: true},
	)
	if err != nil {
		t.Fatalf("OpenRegistrationScope: %v", err)
	}
	wrongRole := &registrationScopeHandler{
		name:  "wrong.role",
		scope: ScopeNone,
	}
	if _, err := reg.RegisterHandlerScoped(registrationScope, wrongRole); err == nil ||
		!strings.Contains(err.Error(), "does not match") {
		t.Fatalf("wrong-role registration = %v, want role mismatch", err)
	}
	if _, ok := reg.GetMeta(wrongRole.Name()); ok {
		t.Fatal("wrong-role rejection mutated registry")
	}

	if _, ok := reg.SealRegistrationScope(registrationScope); !ok {
		t.Fatal("SealRegistrationScope rejected live scope")
	}
	rightRole := &registrationScopeHandler{
		name:  "sealed.scope",
		scope: ScopeNone,
	}
	// Override the fixture role through a small wrapper.
	scoped := &roleRegistrationScopeHandler{registrationScopeHandler: rightRole, role: "owned-role"}
	if _, err := reg.RegisterHandlerScoped(registrationScope, scoped); err == nil ||
		!strings.Contains(err.Error(), "closed") {
		t.Fatalf("sealed registration = %v, want closed-scope rejection", err)
	}
	if _, err := reg.RegisterHandlerScoped(ports.NewRegistrationScope(&struct{}{}), scoped); err == nil ||
		!strings.Contains(err.Error(), "invalid") {
		t.Fatalf("fabricated registration = %v, want invalid-scope rejection", err)
	}
}

func TestRegistrationScopeIdentitySeparatesConcurrentRolesAndRegistries(t *testing.T) {
	registry := NewHandlerRegistry()
	foreignRegistry := NewHandlerRegistry()
	admission := staticTaskAdmission{accepting: true}
	authScope, err := registry.OpenRegistrationScope("auth", admission)
	if err != nil {
		t.Fatalf("OpenRegistrationScope(auth): %v", err)
	}
	billingScope, err := registry.OpenRegistrationScope("billing", admission)
	if err != nil {
		t.Fatalf("OpenRegistrationScope(billing): %v", err)
	}
	foreignAuthScope, err := foreignRegistry.OpenRegistrationScope("auth", admission)
	if err != nil {
		t.Fatalf("foreign OpenRegistrationScope(auth): %v", err)
	}

	authToken := registrationScopeTokenOf(authScope)
	billingToken := registrationScopeTokenOf(billingScope)
	foreignToken := registrationScopeTokenOf(foreignAuthScope)
	if authToken == nil || billingToken == nil || foreignToken == nil {
		t.Fatal("issued scope did not retain a registry token")
	}
	if authToken == billingToken ||
		authToken == foreignToken ||
		billingToken == foreignToken {
		t.Fatalf(
			"scope identity collapsed: auth=%p billing=%p foreign=%p",
			authToken,
			billingToken,
			foreignToken,
		)
	}
	if authToken.issuer != registry ||
		billingToken.issuer != registry ||
		foreignToken.issuer != foreignRegistry ||
		authToken.id == billingToken.id {
		t.Fatalf(
			"scope issuance identity = auth(%p,%d) billing(%p,%d) foreign(%p,%d)",
			authToken.issuer,
			authToken.id,
			billingToken.issuer,
			billingToken.id,
			foreignToken.issuer,
			foreignToken.id,
		)
	}

	newHandler := func(name, role string) *roleRegistrationScopeHandler {
		return &roleRegistrationScopeHandler{
			registrationScopeHandler: &registrationScopeHandler{
				name:  name,
				scope: ScopeNone,
			},
			role: role,
		}
	}
	authHandle, err := registry.RegisterRPCScoped(
		authScope,
		newHandler("auth.first", "auth"),
	)
	if err != nil {
		t.Fatalf("RegisterRPCScoped(auth): %v", err)
	}
	billingHandle, err := registry.RegisterRPCScoped(
		billingScope,
		newHandler("billing.first", "billing"),
	)
	if err != nil {
		t.Fatalf("RegisterRPCScoped(billing): %v", err)
	}
	if _, err := registry.RegisterRPCScoped(
		foreignAuthScope,
		newHandler("auth.foreign", "auth"),
	); err == nil || !strings.Contains(err.Error(), "invalid") {
		t.Fatalf("foreign-registry scope = %v, want invalid-scope rejection", err)
	}

	authOwned, ok := registry.SealRegistrationScope(authScope)
	if !ok || len(authOwned) != 1 || authOwned[0] != authHandle {
		t.Fatalf("sealed auth ownership = (%+v, %v)", authOwned, ok)
	}
	billingLate, err := registry.RegisterRPCScoped(
		billingScope,
		newHandler("billing.late", "billing"),
	)
	if err != nil {
		t.Fatalf("billing registration after auth seal: %v", err)
	}
	if _, err := registry.RegisterRPCScoped(
		authScope,
		newHandler("auth.sealed", "auth"),
	); err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("sealed auth registration = %v, want closed-scope rejection", err)
	}

	if !registry.ReopenRegistrationScope(authScope) {
		t.Fatal("ReopenRegistrationScope(auth) = false")
	}
	authLate, err := registry.RegisterRPCScoped(
		authScope,
		newHandler("auth.reopened", "auth"),
	)
	if err != nil {
		t.Fatalf("RegisterRPCScoped(reopened auth): %v", err)
	}
	authOwned, ok = registry.CloseRegistrationScope(authScope)
	if !ok || len(authOwned) != 2 ||
		authOwned[0] != authHandle ||
		authOwned[1] != authLate {
		t.Fatalf("closed auth ownership = (%+v, %v)", authOwned, ok)
	}
	billingOwned, ok := registry.CloseRegistrationScope(billingScope)
	if !ok || len(billingOwned) != 2 ||
		billingOwned[0] != billingHandle ||
		billingOwned[1] != billingLate {
		t.Fatalf("closed billing ownership = (%+v, %v)", billingOwned, ok)
	}
	if _, ok := foreignRegistry.CloseRegistrationScope(foreignAuthScope); !ok {
		t.Fatal("foreign registry lost its own same-role scope")
	}
}

func TestRegistrationScopeIdentityExhaustionLatchesPermanently(t *testing.T) {
	registry := NewHandlerRegistry()
	registry.nextScopeID = ^uint64(0) - 1
	foreignRegistry := NewHandlerRegistry()
	admission := staticTaskAdmission{accepting: true}

	const callers = 32
	start := make(chan struct{})
	results := make(chan struct {
		scope ports.RegistrationScope
		err   error
	}, callers)
	for range callers {
		go func() {
			<-start
			scope, err := registry.OpenRegistrationScope("owned-role", admission)
			results <- struct {
				scope ports.RegistrationScope
				err   error
			}{scope: scope, err: err}
		}()
	}
	close(start)

	var finalScope ports.RegistrationScope
	successes := 0
	for range callers {
		result := <-results
		if result.err == nil {
			successes++
			finalScope = result.scope
			continue
		}
		if !strings.Contains(result.err.Error(), "identity exhausted") {
			t.Fatalf("exhaustion error = %q", result.err)
		}
	}
	if successes != 1 {
		t.Fatalf("successful final-boundary issuances = %d, want 1", successes)
	}
	finalToken := registrationScopeTokenOf(finalScope)
	if finalToken == nil || finalToken.id != ^uint64(0) {
		t.Fatalf("final scope token = %+v, want max uint64 identity", finalToken)
	}

	for attempt := range 10 {
		if _, err := registry.OpenRegistrationScope("owned-role", admission); err == nil ||
			!strings.Contains(err.Error(), "identity exhausted") {
			t.Fatalf("post-exhaustion attempt %d = %v, want permanent denial", attempt, err)
		}
	}
	if _, ok := registry.CloseRegistrationScope(finalScope); !ok {
		t.Fatal("final valid scope could not be closed after exhaustion")
	}
	if _, err := registry.OpenRegistrationScope("owned-role", admission); err == nil ||
		!strings.Contains(err.Error(), "identity exhausted") {
		t.Fatalf("scope teardown cleared exhaustion latch: %v", err)
	}

	foreignScope, err := foreignRegistry.OpenRegistrationScope("owned-role", admission)
	if err != nil {
		t.Fatalf("foreign registry inherited exhaustion: %v", err)
	}
	foreignToken := registrationScopeTokenOf(foreignScope)
	if foreignToken == nil || foreignToken.id != 1 {
		t.Fatalf("foreign registry first identity = %+v, want 1", foreignToken)
	}
}

type roleRegistrationScopeHandler struct {
	*registrationScopeHandler
	role string
}

func (h *roleRegistrationScopeHandler) Role() string { return h.role }
