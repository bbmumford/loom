/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package rpc

import (
	"context"

	sharedDispatch "github.com/bbmumford/loom/pkg/dispatch"
)

// LocalDispatcher mirrors dispatch.LocalDispatcher — the rpc package
// keeps the type alias so existing callers (the runtime adapter at
// mesh/node/rpc_local_dispatch.go, and any out-of-tree consumers)
// don't need to change imports. The canonical implementation + global
// dispatcher state lives in the dispatch package so dispatch.CallJSON,
// dispatch.CallProto, rpc.Call, and rpc.CallRaw all consult the same
// installed instance.
//
// Audit critical fix #2 — before this consolidation each package had
// its own copy of the global, so the per-domain dispatch shims that
// go through dispatch.CallProto silently bypassed the local-handler
// short-circuit that rpc.Call had.
type LocalDispatcher = sharedDispatch.LocalDispatcher

// SetLocalDispatcher delegates to dispatch.SetLocalDispatcher so the
// single global is shared across both packages.
func SetLocalDispatcher(d LocalDispatcher) {
	sharedDispatch.SetLocalDispatcher(d)
}

// LocalDispatcherInstance delegates to dispatch.LocalDispatcherInstance.
func LocalDispatcherInstance() LocalDispatcher {
	return sharedDispatch.LocalDispatcherInstance()
}

// tryLocalDispatch is kept as a package-private wrapper that delegates
// to dispatch.TryLocalDispatch so call sites in this package read
// identically to the pre-consolidation version.
func tryLocalDispatch(ctx context.Context, name string, payload []byte) (out []byte, handled bool, err error) {
	return sharedDispatch.TryLocalDispatch(ctx, name, payload)
}
