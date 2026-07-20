/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package dispatch

import (
	"context"
	"sync"
)

// LocalDispatcher is the contract every caller — dispatch.CallJSON,
// dispatch.CallProto, rpc.Call, rpc.CallRaw — consults before going
// over the mesh. When this process hosts the handler, the request
// stays in-process: the network round-trip, the serialize-deserialize
// double pass, the role lookup, the route probe — all avoided.
//
// The HTTP `/rpc/` bridge already does this via `h.Func != nil`.
// Putting the interface here (vs. in the higher-level rpc package)
// closes the same loop for EVERY caller — every per-domain dispatch
// shim, every background job, every scheduler — not just the rpc.Call
// slice that the original Cascade C implementation reached.
// Audit critical fix #2.
//
// Implementations should:
//   - HasHandler: cheap lookup, no execution. MUST filter to handlers
//     that match the bytes-in/bytes-out contract DispatchBytes uses —
//     a registry that mixes RPC + Task + Stream handlers must report
//     true only for the RPC-shaped subset, otherwise DispatchBytes
//     will return ErrUnsupportedMode and the caller will get a
//     spurious failure that should have fallen through to the mesh.
//   - DispatchBytes: full local execution INCLUDING middleware +
//     tenant scope enforcement so a local call has the same security
//     semantics as a remote one.
//
// Set via SetLocalDispatcher at endpoint boot, BEFORE any caller fires.
type LocalDispatcher interface {
	HasHandler(name string) bool
	DispatchBytes(ctx context.Context, name string, payload []byte) ([]byte, error)
}

var (
	localDispatcher   LocalDispatcher
	localDispatcherMu sync.RWMutex
)

// SetLocalDispatcher installs the local-handler short-circuit. Pass
// nil to disable (every CallJSON/CallProto/rpc.Call goes over the
// mesh).
func SetLocalDispatcher(d LocalDispatcher) {
	localDispatcherMu.Lock()
	localDispatcher = d
	localDispatcherMu.Unlock()
}

// LocalDispatcherInstance returns the installed dispatcher (nil if
// unset). Exported so the rpc package + tests can share the same
// global without duplicating state.
func LocalDispatcherInstance() LocalDispatcher {
	localDispatcherMu.RLock()
	defer localDispatcherMu.RUnlock()
	return localDispatcher
}

// TryLocalDispatch returns (response bytes, true, nil) when the
// handler is hosted locally and dispatch succeeded; (nil, false, nil)
// when no local dispatcher or handler not local; (nil, true, err)
// when local dispatch errored. Callers use the bool to decide whether
// to fall through to mesh dispatch.
//
// Exported (vs. the original rpc.tryLocalDispatch which was package-
// private) so dispatch.CallJSON / CallProto and rpc.Call / CallRaw
// can share one impl rather than each defining a private copy.
func TryLocalDispatch(ctx context.Context, name string, payload []byte) (out []byte, handled bool, err error) {
	d := LocalDispatcherInstance()
	if d == nil {
		return nil, false, nil
	}
	if !d.HasHandler(name) {
		return nil, false, nil
	}
	out, err = d.DispatchBytes(ctx, name, payload)
	return out, true, err
}
