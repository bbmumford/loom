/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package dispatch

import (
	"context"
	"fmt"

	"github.com/bbmumford/loom/node/handlers"
	"github.com/bbmumford/loom/pkg/trace"
)

// LocalCaller implements Caller by dispatching directly to a HandlerRegistry
// (in-process, no mesh transport). Used by failover endpoints when VL1 mesh
// is unavailable, or for testing.
//
// The dispatch flow: proto bytes → HandlerRegistry.Dispatch → handler → proto bytes.
// This has one serialize/deserialize round-trip per call (same as HWPCaller but
// without network overhead).
type LocalCaller struct {
	registry *handlers.HandlerRegistry
}

// NewLocalCaller creates a Caller that dispatches directly to registered handlers.
// The registry must have domain handlers registered via the generated
// RegisterGenerated*Handlers functions.
func NewLocalCaller(registry *handlers.HandlerRegistry) *LocalCaller {
	return &LocalCaller{registry: registry}
}

// Call dispatches an RPC to a handler registered in the local registry.
// The role parameter is ignored (all handlers are local), but method must
// match a registered handler name (e.g., "auth.validate_session").
//
// A call naming an operation class the grade gate does not declare is refused
// before the handler runs, as HWPCaller.Call refuses it. A handler in this
// process has no route to grade, so no class's floor applies here, but a
// process serving through a LocalCaller during failover must refuse what the
// same process refuses across the mesh.
func (c *LocalCaller) Call(ctx context.Context, role, method string, payload []byte) ([]byte, error) {
	if _, err := classFloor(method, opClassFromContext(ctx)); err != nil {
		return nil, fmt.Errorf("local dispatch %s: %w", method, err)
	}
	req := &handlers.RPCRequest{
		Handler: method,
		Payload: payload,
		TraceID: trace.FromContext(ctx),
	}

	resp, err := c.registry.Dispatch(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("local dispatch %s: %w", method, err)
	}

	// A refusal of the caller's authority stays one (errors.Is matches
	// scope.ErrDenied), and a handler's conflict with its stored state stays
	// one (ErrConflict), as HWPCaller returns each for a handler on another
	// node.
	if !resp.Success {
		if resp.Denied != nil {
			return nil, fmt.Errorf("local dispatch %s: %w", method, resp.Denied)
		}
		if resp.Conflict != nil {
			return nil, fmt.Errorf("local dispatch %s: %w", method, resp.Conflict)
		}
		return nil, fmt.Errorf("local dispatch %s: %s", method, resp.Error)
	}

	return resp.Payload, nil
}

// Close is a no-op for LocalCaller (no connections to close).
func (c *LocalCaller) Close() {}
