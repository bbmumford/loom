/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"errors"

	"github.com/bbmumford/loom/node/handlers"
	"github.com/bbmumford/loom/pkg/rpc"
)

// localDispatcherAdapter implements rpc.LocalDispatcher by delegating to
// the node's HandlerRegistry. It exists to break what would otherwise be
// a circular import — the `rpc` package can't import `node`, because node
// imports rpc. So the rpc package owns the interface; node owns the adapter.
//
// Installed once at runtime startup via rpc.SetLocalDispatcher; from that
// point every rpc.Call / rpc.CallRaw consults the local registry FIRST
// and skips the mesh entirely when this process hosts the handler.
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
	// HandlerRegistry stores RPCHandler,
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
// partition-scope check. Local calls thus have the same security semantics
// as remote ones.
func (a *localDispatcherAdapter) DispatchBytes(ctx context.Context, name string, payload []byte) ([]byte, error) {
	if a == nil || a.registry == nil {
		return nil, errors.New("localDispatcherAdapter: registry not set")
	}
	req := &handlers.RPCRequest{
		Handler: name,
		Payload: payload,
	}
	// handlers.Outcome reads the result as the node's RPC server and
	// dispatch.LocalCaller read it: a failed response comes back as an error,
	// a refusal of the caller's authority as the refusal itself, so the HTTP
	// bridge proxying an operation this process hosts answers it 403
	// (pkg/rpc/http classifyMeshError) as it does one from another node, a
	// handler's conflict with its stored state likewise, which it answers 409,
	// and a result with no response as handlers.ErrNoResponse.
	return handlers.Outcome(a.registry.Dispatch(ctx, req))
}

// installLocalDispatcher wires rpc.SetLocalDispatcher to this runtime's
// handler registry. Called from NewRuntime after the RPCServer is built.
// Safe to call multiple times — the last call wins; the rpc package only
// keeps one global dispatcher.
func installLocalDispatcher(reg *handlers.HandlerRegistry) {
	rpc.SetLocalDispatcher(&localDispatcherAdapter{registry: reg})
}
