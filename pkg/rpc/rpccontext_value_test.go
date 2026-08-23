/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package rpc

import (
	"context"
	"testing"
)

func TestRPCContextValue(t *testing.T) {
	// Absent context → miss.
	if v, ok := RPCContextValue(context.Background(), "orbtrSurfaceTicket"); ok || v != "" {
		t.Fatalf("no context must miss, got %q ok=%v", v, ok)
	}

	// Stamped value round-trips (the write side is the already-exported WithRPCContext).
	ctx := WithRPCContext(context.Background(), map[string]string{"orbtrSurfaceTicket": "TICKET-abc"})
	if v, ok := RPCContextValue(ctx, "orbtrSurfaceTicket"); !ok || v != "TICKET-abc" {
		t.Fatalf("stamped value must read back, got %q ok=%v", v, ok)
	}

	// A key that was not stamped misses even when the context carries others.
	if v, ok := RPCContextValue(ctx, "absent"); ok || v != "" {
		t.Fatalf("absent key must miss, got %q ok=%v", v, ok)
	}
}
