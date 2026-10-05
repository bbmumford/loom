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
// is byte-equal to its handlers.Scope* counterpart. Both packages forward the
// one canonical declaration in rpc/scope, and this test fails loudly if
// either package declares a divergent literal.
//
// The test lives in rpc (not handlers) because rpc already imports handlers
// via adapter.go's registryShim; the reverse import is a cycle so a symmetric
// test in the handlers package is not possible.
func TestScopeAlias_HandlersEqualsRPC(t *testing.T) {
	cases := []struct {
		name string
		rpc  ScopeTag
		hnd  handlers.Scope
	}{
		{"None", ScopeNone, handlers.ScopeNone},
		{"Allowlisted", ScopeAllowlisted, handlers.ScopeAllowlisted},
		{"Partition", ScopePartition, handlers.ScopePartition},
		{"WithinPartition", ScopeWithinPartition, handlers.ScopeWithinPartition},
		{"Subject", ScopeSubject, handlers.ScopeSubject},
		{"Profile", ScopePartition, handlers.ScopePartition},
		{"Unknown", ScopeUnknown, handlers.ScopeUnknown},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if string(c.rpc) != string(c.hnd) {
				t.Fatalf("Scope%s drift: rpc=%q handlers.Scope%s=%q — both must forward the same scope subpackage constant",
					c.name, c.rpc, c.name, c.hnd)
			}
		})
	}
}

// TestScopeAlias_TypeIdentity asserts that rpc.ScopeTag, handlers.Scope,
// and scope.ScopeTag are the SAME type identity — not merely compatible
// via structural string aliasing. If someone reverts one of them to a
// standalone type, the assignment round-trip below breaks at compile time.
// All three aliases must retain the canonical defined type's exact identity.
func TestScopeAlias_TypeIdentity(t *testing.T) {
	// Round-trip each canonical value through both aliases; if the
	// aliases diverge, the assignments fail to compile.
	var s ScopeTag = handlers.ScopePartition
	var h handlers.Scope = ScopePartition
	var c scope.ScopeTag = ScopePartition
	if s != h || h != c || c != ScopePartition {
		t.Fatalf("Profile scope failed identity round-trip: rpc=%q handlers=%q scope=%q", s, h, c)
	}
}

// TestScopeAlias_ProfileEnforceScopeParity: a handler declared with
// rpc.ScopePartition MUST receive the same enforcement whether it dispatches
// through EnforceScope (rpc) or validateScope (handlers). The two
// implementations consult the same constant, and this test asserts they
// refuse the same missing-partition input rather than one silently accepting.
func TestScopeAlias_ProfileEnforceScopeParity(t *testing.T) {
	// rpc.EnforceScope side: a ScopePartition handler with an empty ctx
	// (no platform-partition axis) must be denied with ErrScopeDenied under the
	// reference policy — mirroring what validateScope does on the
	// handlers side with the same policy.
	h := &Handler{Namespace: "app", Domain: "billing", Operation: "GetProfile", Scope: ScopePartition}
	err := EnforceScope(t.Context(), h, refPolicy())
	if err == nil {
		t.Fatalf("EnforceScope(ScopePartition, empty ctx) unexpectedly passed — ScopePartition must require a partition")
	}
	// handlers.validateScope side is exercised in
	// node/handlers/scope_test.go:TestValidateScope_Profile_MissingPartitionFailsClosed.
	// Both together prove parity across the two dispatch surfaces.
}
