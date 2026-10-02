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
// keeps the type alias so callers (the runtime adapter at
// node/rpc_local_dispatch.go, and any out-of-tree consumers) can import it
// from here. The canonical implementation + global dispatcher state lives in
// the dispatch package so dispatch.CallJSON, dispatch.CallProto, rpc.Call,
// and rpc.CallRaw all consult the same installed instance.
//
// One global, not one per package: a per-package copy would let a dispatch
// shim that goes through dispatch.CallProto silently skip the local-handler
// short-circuit rpc.Call takes.
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

// tryLocalDispatch is this package's name for dispatch.TryLocalDispatch,
// used by Call and CallRaw.
func tryLocalDispatch(ctx context.Context, name string, payload []byte) (out []byte, handled bool, err error) {
	return sharedDispatch.TryLocalDispatch(ctx, name, payload)
}
