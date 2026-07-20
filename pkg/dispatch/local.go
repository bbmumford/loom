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
func (c *LocalCaller) Call(ctx context.Context, role, method string, payload []byte) ([]byte, error) {
	req := &handlers.RPCRequest{
		Handler: method,
		Payload: payload,
		TraceID: trace.FromContext(ctx),
	}

	resp, err := c.registry.Dispatch(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("local dispatch %s: %w", method, err)
	}

	if !resp.Success {
		return nil, fmt.Errorf("local dispatch %s: %s", method, resp.Error)
	}

	return resp.Payload, nil
}

// Close is a no-op for LocalCaller (no connections to close).
func (c *LocalCaller) Close() {}
