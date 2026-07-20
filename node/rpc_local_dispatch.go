/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"errors"
	"fmt"

	"github.com/bbmumford/loom/node/handlers"
	"github.com/bbmumford/loom/pkg/rpc"
)

// localDispatcherAdapter implements rpc.LocalDispatcher by delegating to
// the node's HandlerRegistry. It exists to break what would otherwise be
// a circular import — the `rpc` package can't depend on `mesh/node` /
// `handlers` (which depend on broader Library state). So the rpc package
// owns the interface; we own the adapter.
//
// Installed once at runtime startup via rpc.SetLocalDispatcher; from that
// point every rpc.Call / rpc.CallRaw consults the local registry FIRST
// and skips the mesh entirely when this process hosts the handler.
//
// Cascade C fix (L3 #1 + #2) from the 2026-05-23 routing review.
type localDispatcherAdapter struct {
	registry *handlers.HandlerRegistry
}

func (a *localDispatcherAdapter) HasHandler(name string) bool {
	if a == nil || a.registry == nil {
		return false
	}
	meta, ok := a.registry.GetMeta(name)
	if !ok {
		return false
	}
	// Audit critical fix #3: HandlerRegistry stores RPCHandler,
	// TaskHandler, and StreamHandler in the same map. DispatchBytes
	// can only execute RPCHandler — the registry's Dispatch switch
	// returns ErrUnsupportedMode for the other two. If HasHandler
	// reported true for a Task/Stream handler with the same name as a
	// remote-only RPC, TryLocalDispatch would return
	// (handled=true, err=ErrUnsupportedMode) and the caller would
	// surface a spurious failure that should have fallen through to
	// mesh dispatch. Filter to RPC-shaped handlers only.
	if _, isRPC := meta.(handlers.RPCHandler); !isRPC {
		return false
	}
	return true
}

// DispatchBytes wraps the payload in a handlers.RPCRequest and invokes
// the registry's Dispatch — which runs middleware (Before/After) and the
// tenant-scope check. Local calls thus have the same security semantics
// as remote ones.
func (a *localDispatcherAdapter) DispatchBytes(ctx context.Context, name string, payload []byte) ([]byte, error) {
	if a == nil || a.registry == nil {
		return nil, errors.New("localDispatcherAdapter: registry not set")
	}
	req := &handlers.RPCRequest{
		Handler: name,
		Payload: payload,
	}
	resp, err := a.registry.Dispatch(ctx, req)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, errors.New("localDispatcherAdapter: nil response from registry")
	}
	if !resp.Success {
		// Mirror the wire-format semantics: a handler that returned
		// Success=false should bubble up as an error to the caller.
		return nil, fmt.Errorf("%s", resp.Error)
	}
	return resp.Payload, nil
}

// installLocalDispatcher wires rpc.SetLocalDispatcher to this runtime's
// handler registry. Called from NewRuntime after the RPCServer is built.
// Safe to call multiple times — the last call wins; the rpc package only
// keeps one global dispatcher.
func installLocalDispatcher(reg *handlers.HandlerRegistry) {
	rpc.SetLocalDispatcher(&localDispatcherAdapter{registry: reg})
}
