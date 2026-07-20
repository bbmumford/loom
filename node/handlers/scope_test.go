/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package handlers

import (
	"context"
	"strings"
	"testing"
)

// stubMeta is a minimal HandlerMeta used solely to exercise validateTenantScope
// without pulling in a full registry or wrapper type. It carries only the
// fields the switch statement reads.
type stubMeta struct {
	name    string
	role    string
	scope   TenantScope
	allowed []string
}

func (s *stubMeta) Name() string               { return s.name }
func (s *stubMeta) Role() string               { return s.role }
func (s *stubMeta) RequiresAuth() bool         { return false }
func (s *stubMeta) AllowedAuthTypes() []string { return nil }
func (s *stubMeta) Scopes() []string           { return nil }
func (s *stubMeta) TenantScope() TenantScope   { return s.scope }
func (s *stubMeta) AllowedTenants() []string   { return s.allowed }

// ctxWithRPC builds a Go context carrying the RPC context map that
// GetTenantIDFromContext / getUserIDFromContext read from.
func ctxWithRPC(m map[string]interface{}) context.Context {
	return context.WithValue(context.Background(), "rpc_context", m)
}

// TestValidateTenantScope_Profile_MissingTenantFailsClosed asserts that the
// newly-added TenantScopeProfile arm rejects a request that is not carrying a
// tenant. Before finding rev-069 this scope value silently matched the empty
// default (fall-through) — profile-scoped handlers were effectively
// unenforced when declared via the handlers package.
func TestValidateTenantScope_Profile_MissingTenantFailsClosed(t *testing.T) {
	r := NewHandlerRegistry()
	h := &stubMeta{name: "billing.getProfile", scope: TenantScopeProfile}

	err := r.validateTenantScope(ctxWithRPC(nil), h)
	if err == nil {
		t.Fatalf("expected TenantScopeProfile without tenant to fail closed; got nil")
	}
	if !strings.Contains(err.Error(), "tenant") {
		t.Fatalf("expected error to mention tenant requirement; got %v", err)
	}
}

// TestValidateTenantScope_Profile_WithTenantPasses asserts the Profile arm
// admits a caller that has stamped a tenant. Deeper profile-membership
// enforcement is handled at the middleware tier (RequireScopesWithParam)
// per Quorum ADR-0012 — the dispatch tier only checks the tenant prerequisite.
func TestValidateTenantScope_Profile_WithTenantPasses(t *testing.T) {
	r := NewHandlerRegistry()
	h := &stubMeta{name: "billing.getProfile", scope: TenantScopeProfile}

	err := r.validateTenantScope(ctxWithRPC(map[string]interface{}{
		"tenant_id": "acme",
	}), h)
	if err != nil {
		t.Fatalf("expected TenantScopeProfile with tenant to pass; got %v", err)
	}
}

// TestValidateTenantScope_Unknown_FailsClosed asserts the explicit Unknown
// sentinel rejects every dispatch attempt. This mirrors rpc.EnforceScope's
// ScopeUnknown branch — a handler bound to this scope is structurally invalid
// and MUST NOT be dispatched.
func TestValidateTenantScope_Unknown_FailsClosed(t *testing.T) {
	r := NewHandlerRegistry()
	h := &stubMeta{name: "misconfigured.op", scope: TenantScopeUnknown}

	// Even with a fully-populated tenant/user context, the Unknown sentinel
	// must refuse — the caller identity is not the reason for refusal.
	err := r.validateTenantScope(ctxWithRPC(map[string]interface{}{
		"tenant_id": "acme",
		"userID":    "alice",
	}), h)
	if err == nil {
		t.Fatalf("expected TenantScopeUnknown to fail closed; got nil")
	}
	if !strings.Contains(err.Error(), "fail-closed") {
		t.Fatalf("expected fail-closed diagnostic in error; got %v", err)
	}
}

// TestValidateTenantScope_GarbageScope_FailsClosed asserts the default arm
// rejects any scope value that is not one of the recognised constants —
// preserving the isolation guarantee against typos and partial migrations
// from future scope tiers. Before finding rev-069 this fell through silently
// (fail-open), which meant a mis-typed scope value ("tennant") accepted any
// caller including anonymous ones.
func TestValidateTenantScope_GarbageScope_FailsClosed(t *testing.T) {
	r := NewHandlerRegistry()
	h := &stubMeta{name: "typo.op", scope: TenantScope("tennant")}

	err := r.validateTenantScope(ctxWithRPC(map[string]interface{}{
		"tenant_id": "acme",
	}), h)
	if err == nil {
		t.Fatalf("expected garbage scope value to fail closed; got nil")
	}
	if !strings.Contains(err.Error(), "unknown tenant scope") {
		t.Fatalf("expected 'unknown tenant scope' diagnostic; got %v", err)
	}
}

// TestValidateTenantScope_None_Passes documents that ScopeNone still opts
// the handler out of enforcement — this is the sole "anyone goes" tier and
// is used for liveness, /heartbeat, and OAuth init.
func TestValidateTenantScope_None_Passes(t *testing.T) {
	r := NewHandlerRegistry()
	h := &stubMeta{name: "public.heartbeat", scope: TenantScopeNone}

	if err := r.validateTenantScope(context.Background(), h); err != nil {
		t.Fatalf("expected TenantScopeNone to pass without context; got %v", err)
	}
}

// TestValidateTenantScope_LegacyScopes_StillEnforce guards against a
// regression where the switch refactor drops enforcement of an existing tier.
func TestValidateTenantScope_LegacyScopes_StillEnforce(t *testing.T) {
	cases := []struct {
		name  string
		scope TenantScope
		want  string // substring expected in error
	}{
		{name: "Tenant", scope: TenantScopeTenant, want: "tenant context"},
		{name: "Org", scope: TenantScopeOrg, want: "org context"},
		{name: "User", scope: TenantScopeUser, want: "tenant context"},
	}
	r := NewHandlerRegistry()
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := &stubMeta{name: "op." + c.name, scope: c.scope}
			err := r.validateTenantScope(ctxWithRPC(nil), h)
			if err == nil {
				t.Fatalf("expected %s scope without tenant to fail; got nil", c.name)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Fatalf("expected error to contain %q; got %v", c.want, err)
			}
		})
	}
}

// TestTenantScopeConstants_AgreeWithRPC pins the string values of the
// handlers.TenantScope* constants so they cannot drift from rpc.Scope*.
// If the two ever disagree, a value round-tripped between packages would
// silently degrade to ScopeNone-equivalent (empty string) or an unknown
// scope — both of which now fail closed, but the failure surface should
// be the constant contract, not a runtime dispatch.
func TestTenantScopeConstants_AgreeWithRPC(t *testing.T) {
	// These literal expectations are intentionally hard-coded rather than
	// imported from rpc — that would introduce a test-scope import of the
	// rpc package, which is imported by handlers' dependents. Any change
	// to rpc.Scope* string values must be reflected here.
	cases := []struct {
		name string
		got  TenantScope
		want string
	}{
		{"None", TenantScopeNone, ""},
		{"Platform", TenantScopePlatform, "platform"},
		{"Tenant", TenantScopeTenant, "tenant"},
		{"Org", TenantScopeOrg, "org"},
		{"User", TenantScopeUser, "user"},
		{"Profile", TenantScopeProfile, "profile"},
		{"Unknown", TenantScopeUnknown, "unknown"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if string(c.got) != c.want {
				t.Fatalf("TenantScope%s drift: got %q want %q — must match rpc.Scope%s", c.name, c.got, c.want, c.name)
			}
		})
	}
}
