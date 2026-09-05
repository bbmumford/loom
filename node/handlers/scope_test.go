/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package handlers

import (
	"context"
	"errors"
	"strings"
	"testing"

	rpcscope "github.com/bbmumford/loom/pkg/rpc/scope"
)

// stubMeta is a minimal HandlerMeta used solely to exercise validateScope
// without pulling in a full registry or wrapper type. It carries only the
// fields the switch statement reads.
type stubMeta struct {
	name    string
	role    string
	scope   Scope
	allowed []string
}

func (s *stubMeta) Name() string               { return s.name }
func (s *stubMeta) Role() string               { return s.role }
func (s *stubMeta) RequiresAuth() bool         { return false }
func (s *stubMeta) AllowedAuthTypes() []string { return nil }
func (s *stubMeta) Scopes() []string           { return nil }
func (s *stubMeta) Scope() Scope         { return s.scope }
func (s *stubMeta) AllowedPartitions() []string   { return s.allowed }

// scopeRefPolicy is a CONSUMER-side reference PresencePolicy reproducing the
// platform's historical presence semantics. loom no longer contains this
// logic — validateScope delegates every declared (non-None) tag to
// whatever policy the registry carries; a nil policy fails closed. The reason
// strings mirror the diagnostics these tests characterise.
func scopeRefPolicy(platforms ...string) rpcscope.PresencePolicyFunc {
	allow := make(map[string]struct{}, len(platforms))
	for _, p := range platforms {
		if p != "" {
			allow[p] = struct{}{}
		}
	}
	return func(declared Scope, id rpcscope.AuthenticatedIdentity) (bool, string) {
		tenant := id.Scope(rpcscope.AxisTransportPinned)
		switch declared {
		case ScopeNone:
			return true, ""
		case ScopePlatform:
			if tenant == "" {
				return false, "authenticated platform access required"
			}
			if _, ok := allow[tenant]; !ok {
				return false, "tenant not in platform allowlist"
			}
		case ScopeTenant:
			if tenant == "" {
				return false, "authenticated platform tenant context required"
			}
		case ScopeOrg:
			if tenant == "" {
				return false, "authenticated platform tenant context required"
			}
			if id.Scope(rpcscope.AxisWithinTransport) == "" {
				return false, "authenticated organisation context required"
			}
		case ScopeUser:
			if tenant == "" {
				return false, "authenticated platform tenant context required"
			}
			if id.Subject == "" {
				return false, "authenticated user context required"
			}
		case ScopeProfile:
			if tenant == "" {
				return false, "authenticated platform tenant context required"
			}
		default:
			return false, "unknown tenant scope (fail-closed sentinel)"
		}
		return true, ""
	}
}

// newScopedRegistry builds a registry with the reference policy installed so
// declared scopes evaluate under realistic semantics.
func newScopedRegistry(platforms ...string) *HandlerRegistry {
	r := NewHandlerRegistry()
	r.SetPresencePolicy(scopeRefPolicy(platforms...))
	return r
}

// scopeIdentity builds an opaque identity from the deployed axes, omitting any
// empty axis (an absent axis must stay absent).
func scopeIdentity(tenant, org, user string) rpcscope.AuthenticatedIdentity {
	id := rpcscope.AuthenticatedIdentity{Subject: user}
	if tenant != "" {
		id.Scopes = append(id.Scopes, rpcscope.ScopeValue{Name: rpcscope.AxisTransportPinned, Value: tenant})
	}
	if org != "" {
		id.Scopes = append(id.Scopes, rpcscope.ScopeValue{Name: rpcscope.AxisWithinTransport, Value: org})
	}
	return id
}

// ctxWithRPC builds a Go context carrying the RPC context map that
// GetPartitionIDFromContext / getUserIDFromContext read from.
func ctxWithRPC(m map[string]interface{}) context.Context {
	return context.WithValue(context.Background(), "rpc_context", m)
}

// TestValidateTenantScope_Profile_MissingTenantFailsClosed asserts that the
// newly-added ScopeProfile arm rejects a request that is not carrying a
// tenant. Before finding rev-069 this scope value silently matched the empty
// default (fall-through) — profile-scoped handlers were effectively
// unenforced when declared via the handlers package.
func TestValidateTenantScope_Profile_MissingTenantFailsClosed(t *testing.T) {
	r := newScopedRegistry()
	h := &stubMeta{name: "billing.getProfile", scope: ScopeProfile}

	err := r.validateScope(ctxWithRPC(nil), h)
	if err == nil {
		t.Fatalf("expected ScopeProfile without tenant to fail closed; got nil")
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
	r := newScopedRegistry()
	h := &stubMeta{name: "billing.getProfile", scope: ScopeProfile}

	ctx := rpcscope.WithAuthenticatedIdentity(context.Background(), scopeIdentity("acme", "", ""))
	err := r.validateScope(ctx, h)
	if err != nil {
		t.Fatalf("expected ScopeProfile with tenant to pass; got %v", err)
	}
}

// TestValidateTenantScope_Unknown_FailsClosed asserts the explicit Unknown
// sentinel rejects every dispatch attempt. This mirrors rpc.EnforceScope's
// ScopeUnknown branch — a handler bound to this scope is structurally invalid
// and MUST NOT be dispatched.
func TestValidateTenantScope_Unknown_FailsClosed(t *testing.T) {
	r := newScopedRegistry()
	h := &stubMeta{name: "misconfigured.op", scope: ScopeUnknown}

	// Even with a fully-populated tenant/user context, the Unknown sentinel
	// must refuse — the caller identity is not the reason for refusal.
	err := r.validateScope(ctxWithRPC(map[string]interface{}{
		"tenant_id": "acme",
		"userID":    "alice",
	}), h)
	if err == nil {
		t.Fatalf("expected ScopeUnknown to fail closed; got nil")
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
	r := newScopedRegistry()
	h := &stubMeta{name: "typo.op", scope: Scope("tennant")}

	err := r.validateScope(ctxWithRPC(map[string]interface{}{
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
	h := &stubMeta{name: "public.heartbeat", scope: ScopeNone}

	if err := r.validateScope(context.Background(), h); err != nil {
		t.Fatalf("expected ScopeNone to pass without context; got %v", err)
	}
}

func TestValidateTenantScope_PlatformUsesCanonicalFailClosedAllowlist(t *testing.T) {
	h := &stubMeta{name: "platform.op", scope: ScopePlatform}
	identity := func(tenantID string) context.Context {
		return rpcscope.WithAuthenticatedIdentity(
			context.Background(),
			scopeIdentity(tenantID, "", ""),
		)
	}
	tests := []struct {
		name    string
		set     []string
		tenant  string
		wantErr bool
	}{
		{name: "unset", tenant: "hstles", wantErr: true},
		{name: "empty", set: []string{}, tenant: "hstles", wantErr: true},
		{name: "foreign", set: []string{"hstles"}, tenant: "hstles-dev", wantErr: true},
		{name: "exact", set: []string{"hstles"}, tenant: "hstles"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			r := NewHandlerRegistry()
			// The platform allowlist now lives in the CONSUMER policy, not in
			// loom. An empty/nil allowlist admits nobody — fail closed.
			r.SetPresencePolicy(scopeRefPolicy(test.set...))
			err := r.validateScope(identity(test.tenant), h)
			if test.wantErr {
				if !errors.Is(err, rpcscope.ErrDenied) {
					t.Fatalf("expected shared fail-closed denial, got %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("expected exact platform allowlist member to pass: %v", err)
			}
		})
	}
}

// TestValidateTenantScope_LegacyScopes_StillEnforce guards against a
// regression where the switch refactor drops enforcement of an existing tier.
func TestValidateTenantScope_LegacyScopes_StillEnforce(t *testing.T) {
	cases := []struct {
		name  string
		scope Scope
		want  string // substring expected in error
	}{
		{name: "Tenant", scope: ScopeTenant, want: "tenant context"},
		{name: "Org", scope: ScopeOrg, want: "tenant context"},
		{name: "User", scope: ScopeUser, want: "tenant context"},
	}
	r := newScopedRegistry()
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := &stubMeta{name: "op." + c.name, scope: c.scope}
			err := r.validateScope(ctxWithRPC(nil), h)
			if err == nil {
				t.Fatalf("expected %s scope without tenant to fail; got nil", c.name)
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Fatalf("expected error to contain %q; got %v", c.want, err)
			}
			if !errors.Is(err, rpcscope.ErrDenied) {
				t.Fatalf("expected shared scope denial sentinel; got %v", err)
			}
		})
	}
}

// TestTenantScopeConstants_AgreeWithRPC pins the string values of the
// handlers.Scope* constants so they cannot drift from rpc.Scope*.
// If the two ever disagree, a value round-tripped between packages would
// silently degrade to an unset or unknown scope. Both fail closed, but the
// failure surface should be the constant contract, not runtime dispatch.
func TestTenantScopeConstants_AgreeWithRPC(t *testing.T) {
	// These literal expectations are intentionally hard-coded rather than
	// imported from rpc — that would introduce a test-scope import of the
	// rpc package, which is imported by handlers' dependents. Any change
	// to rpc.Scope* string values must be reflected here.
	cases := []struct {
		name string
		got  Scope
		want string
	}{
		{"None", ScopeNone, "none"},
		{"Platform", ScopePlatform, "platform"},
		{"Tenant", ScopeTenant, "tenant"},
		{"Org", ScopeOrg, "org"},
		{"User", ScopeUser, "user"},
		{"Profile", ScopeProfile, "profile"},
		{"Unknown", ScopeUnknown, "unknown"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if string(c.got) != c.want {
				t.Fatalf("Scope%s drift: got %q want %q — must match rpc.ScopeTag%s", c.name, c.got, c.want, c.name)
			}
		})
	}
}

// TestValidateTenantScope_TransportReconcile is a REGRESSION LOCK on the
// transport/authenticated-identity reconciliation. On a tenant-specific
// transport the aether session's tenant is stamped by WithTransportPartition; a
// later authenticated identity that disagrees is rejected. Mutable request
// maps are never one side of this comparison.
//
// This is the load-bearing behaviour the endpoint-template Config.Tenants fix
// (#R-697 Directive-1, "prove enforcement not config-presence") rests on, and
// it had no direct coverage. The switch arms above return only on failure, so
// the reconcile is reached on every scope whose context check passes; these
// cases drive a ScopeTenant handler (tenant present) straight into it.
func TestValidateTenantScope_TransportReconcile(t *testing.T) {
	const reqTenant = "acme"
	cases := []struct {
		name      string
		transport string // value stamped by WithTransportPartition (ignored if noStamp)
		noStamp   bool   // if true, stamp no transport tenant at all
		wantErr   string // "" => expect pass
	}{
		// Tenant-specific transport: authenticated tenant matches the envelope.
		{name: "match_passes", transport: "acme", wantErr: ""},
		// Tenant-specific transport: envelope lies about its tenant — REJECTED.
		{name: "mismatch_rejects", transport: "evil", wantErr: "transport/authenticated partition mismatch"},
		// Single-tenant fallback: reconcile skipped, envelope self-asserted.
		{name: "default_skips_selfassert", transport: "default", wantErr: ""},
		// Shared transport with no resolved ScopeID: skipped (empty == unset).
		{name: "empty_skips", transport: "", wantErr: ""},
		{name: "unstamped_skips", noStamp: true, wantErr: ""},
	}
	r := newScopedRegistry()
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := &stubMeta{name: "tenant.op", scope: ScopeTenant}
			ctx := context.Background()
			if !c.noStamp {
				ctx = WithTransportPartition(ctx, c.transport)
			}
			ctx = rpcscope.WithAuthenticatedIdentity(ctx, scopeIdentity(reqTenant, "", ""))
			err := r.validateScope(ctx, h)
			if c.wantErr == "" {
				if err != nil {
					t.Fatalf("%s: expected pass; got %v", c.name, err)
				}
				return
			}
			if err == nil {
				t.Fatalf("%s: expected error containing %q; got nil", c.name, c.wantErr)
			}
			if !strings.Contains(err.Error(), c.wantErr) {
				t.Fatalf("%s: expected error containing %q; got %v", c.name, c.wantErr, err)
			}
			if !errors.Is(err, rpcscope.ErrDenied) {
				t.Fatalf("%s: denial must wrap shared scope sentinel; got %v", c.name, err)
			}
		})
	}
}

func TestValidateTenantScope_OrganizationIgnoresMutableHints(t *testing.T) {
	r := newScopedRegistry()
	h := &stubMeta{name: "org.read", scope: ScopeOrg}

	for _, body := range []map[string]interface{}{
		{"tenant_id": "orbtr", "org_id": "forged"},
		{"tenantId": "orbtr", "orgId": "forged"},
		{"tenant_id": "orbtr", "organization": "forged"},
	} {
		ctx := WithTransportPartition(ctxWithRPC(body), "orbtr")
		err := r.validateScope(ctx, h)
		if err == nil || !strings.Contains(err.Error(), "organisation") {
			t.Fatalf("mutable org hint %#v must not satisfy ScopeOrg; got %v", body, err)
		}
	}

	ctx := WithTransportPartition(ctxWithRPC(map[string]interface{}{
		"tenant_id": "evil",
		"orgId":     "evil",
	}), "orbtr")
	ctx = rpcscope.WithAuthenticatedIdentity(ctx, scopeIdentity("orbtr", "acme", ""))
	if err := r.validateScope(ctx, h); err != nil {
		t.Fatalf("exact authenticated organisation must satisfy ScopeOrg despite body hints: %v", err)
	}
}
