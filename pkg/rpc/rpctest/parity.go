/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

// Package rpctest holds cross-domain testing helpers for the shared RPC
// registry. Keeping these out of package rpc means the production rpc
// package does not pull in the standard `testing` package as a transitive
// dependency of any endpoint binary.
//
// Import ONLY from _test.go files:
//
//	import "github.com/bbmumford/loom/pkg/rpc/rpctest"
package rpctest

import (
	"testing"

	"github.com/bbmumford/loom/pkg/rpc"
)

// AssertRegisterTypesParity is the shared parity assertion for the
// canonical Register/Types contract that every domain under
// library/domain/* declares (see rpc.StripFuncs for the canonical
// single-source-of-truth pattern).
//
// Purpose
// -------
// Register() is the authoritative server-side wiring (Func + Scope +
// middleware). Types() is the metadata-only mirror consumed by gateway
// endpoints to register proxy handlers via Registry.RegisterProxy (Func
// stripped). If Register and Types drift — a new op added to Register
// but not Types, or a Scope changed on one side only — gateways either
// fail to surface the op at all or surface it under a stale scope.
//
// Drift on the Scope field is purely cosmetic at runtime (the proxy
// path bypasses EnforceScope) but lies to anything reading the manifest:
// directory listings, scope-linters, security review tools. This helper
// fails loudly the moment a future edit forgets to update both lists in
// lockstep.
//
// The helper enforces four invariants:
//   - Every FQN Register() emits appears in Types() with matching Scope.
//   - Every FQN Types() emits appears in Register() (no strays).
//   - Types() emits no duplicate FQN entries.
//   - Register() leaves no Func=nil on any registered handler
//     (catches typeStubs/funcsByOp drift on the receiver-taking pattern).
//   - Types() never returns a non-nil Func (proxy-registration contract).
//
// Usage — no-receiver pattern (identity / anchor / storage / …):
//
//	func TestRegisterTypesParity(t *testing.T) {
//	    rpctest.AssertRegisterTypesParity(t, Register, Types)
//	}
//
// Usage — receiver-taking pattern (audit / device / plugins / …):
//
//	func TestRegisterTypesParity(t *testing.T) {
//	    var h *handlers.Handler // nil is safe — Wrap only binds the method
//	    rpctest.AssertRegisterTypesParity(t,
//	        func(reg *rpc.Registry) error { return Register(reg, h) },
//	        Types,
//	    )
//	}
//
// A nil *handlers.Handler is safe to pass because pointer-receiver method
// values (h.LogEvent, h.CreateUser, …) construct fine on a nil receiver;
// the registry stores the bound function without invoking it.
func AssertRegisterTypesParity(
	t *testing.T,
	register func(*rpc.Registry) error,
	types func() []rpc.Handler,
) {
	t.Helper()

	reg := rpc.NewRegistry()
	if err := register(reg); err != nil {
		t.Fatalf("Register: %v", err)
	}

	// Build FQN -> Scope map from the registered handlers. Also assert
	// each has a non-nil Func — a nil Func here means the caller wired
	// Register incorrectly (e.g. typeStubs / funcsByOp keys drifted).
	registered := make(map[string]rpc.TenantScope, reg.Count())
	for _, hh := range reg.All() {
		registered[hh.FQN()] = hh.Scope
		if hh.Func == nil {
			t.Errorf("Register left Func=nil on %q — typeStubs/funcsByOp drift", hh.FQN())
		}
	}

	// Build FQN -> Scope map from Types(). Reject duplicates and any
	// non-nil Func (proxy-registration contract requires Func-free).
	typesMap := make(map[string]rpc.TenantScope)
	for _, hh := range types() {
		fqn := hh.FQN()
		if _, dup := typesMap[fqn]; dup {
			t.Errorf("Types() contains duplicate FQN %q", fqn)
		}
		typesMap[fqn] = hh.Scope
		if hh.Func != nil {
			t.Errorf("Types() returned non-nil Func on %q", fqn)
		}
	}

	// Every Register entry must appear in Types with matching Scope.
	for fqn, regScope := range registered {
		typScope, ok := typesMap[fqn]
		if !ok {
			t.Errorf("Types() missing FQN %q (Register scope %v)", fqn, regScope)
			continue
		}
		if typScope != regScope {
			t.Errorf("Scope drift on %q: Register=%v Types=%v", fqn, regScope, typScope)
		}
	}
	// And no strays in Types that Register does not have.
	for fqn, typScope := range typesMap {
		if _, ok := registered[fqn]; !ok {
			t.Errorf("Types() has stray FQN %q (scope %v) not in Register", fqn, typScope)
		}
	}
	// Count parity catches duplicates the per-FQN checks miss.
	if len(registered) != len(typesMap) {
		t.Errorf("count mismatch: Register=%d Types=%d", len(registered), len(typesMap))
	}
}
