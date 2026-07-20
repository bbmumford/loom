/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package rpc

import (
	"testing"

	"github.com/bbmumford/loom/node/handlers"
	"github.com/bbmumford/loom/pkg/rpc/scope"
)

// TestScopeAlias_HandlersEqualsRPC asserts that every rpc.Scope* constant
// is byte-equal to its handlers.TenantScope* counterpart. Finding rev-069
// collapsed the two enums to a single canonical declaration in rpc/scope —
// this test is a regression barrier that fails loudly if anyone
// re-introduces a divergent literal in either package.
//
// The test lives in rpc (not handlers) because rpc already imports handlers
// via adapter.go's registryShim; the reverse import is a cycle so a symmetric
// test in the handlers package is not possible.
func TestScopeAlias_HandlersEqualsRPC(t *testing.T) {
	cases := []struct {
		name string
		rpc  TenantScope
		hnd  handlers.TenantScope
	}{
		{"None", ScopeNone, handlers.TenantScopeNone},
		{"Platform", ScopePlatform, handlers.TenantScopePlatform},
		{"Tenant", ScopeTenant, handlers.TenantScopeTenant},
		{"Org", ScopeOrg, handlers.TenantScopeOrg},
		{"User", ScopeUser, handlers.TenantScopeUser},
		{"Profile", ScopeProfile, handlers.TenantScopeProfile},
		{"Unknown", ScopeUnknown, handlers.TenantScopeUnknown},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if string(c.rpc) != string(c.hnd) {
				t.Fatalf("Scope%s drift: rpc=%q handlers.TenantScope%s=%q — both must forward the same scope subpackage constant",
					c.name, c.rpc, c.name, c.hnd)
			}
		})
	}
}

// TestScopeAlias_TypeIdentity asserts that rpc.TenantScope, handlers.TenantScope,
// and scope.TenantScope are the SAME type identity — not merely compatible
// via structural string aliasing. If someone reverts one of them to a
// standalone `type TenantScope = string`, this test still passes (because
// all three are ultimately string), but the assignment-round-trip below
// exercises the intended type equivalence so any bespoke defined-type
// substitution (e.g. `type TenantScope string`) would break compilation
// rather than silently allow drift.
func TestScopeAlias_TypeIdentity(t *testing.T) {
	// Round-trip each canonical value through both aliases; if the
	// aliases diverge, the assignments fail to compile.
	var s TenantScope = handlers.TenantScopeProfile
	var h handlers.TenantScope = ScopeProfile
	var c scope.TenantScope = ScopeProfile
	if s != h || h != c || c != ScopeProfile {
		t.Fatalf("Profile scope failed identity round-trip: rpc=%q handlers=%q scope=%q", s, h, c)
	}
}

// TestScopeAlias_ProfileEnforceScopeParity guards the Profile-tier gap that
// finding rev-069 closed: a handler declared with rpc.ScopeProfile MUST
// receive the same enforcement whether it dispatches through EnforceScope
// (rpc) or validateTenantScope (handlers). Because the two implementations
// now consult the same constant, this test asserts they refuse the same
// missing-tenant input rather than one silently accepting.
func TestScopeAlias_ProfileEnforceScopeParity(t *testing.T) {
	// rpc.EnforceScope side: a ScopeProfile handler with an empty ctx
	// (no tenantId stamped) must be denied with ErrScopeDenied.
	h := &Handler{Namespace: "app", Domain: "billing", Operation: "GetProfile", Scope: ScopeProfile}
	err := EnforceScope(t.Context(), h, nil)
	if err == nil {
		t.Fatalf("EnforceScope(ScopeProfile, empty ctx) unexpectedly passed — Profile tier must require tenantId")
	}
	// handlers.validateTenantScope side is exercised in
	// mesh/node/handlers/scope_test.go:TestValidateTenantScope_Profile_MissingTenantFailsClosed.
	// Both together prove parity across the two dispatch surfaces.
}
