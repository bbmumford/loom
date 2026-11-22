/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"testing"
)

// CallerNode reads the peer a session-scoped entry point stamped, and nothing
// from a request that arrived over no peer session.
func TestCallerNodeReadsThePeerItsSessionStamped(t *testing.T) {
	if got := CallerNode(withCallerNode(context.Background(), "node-a")); got != "node-a" {
		t.Fatalf("CallerNode = %q, want the stamped peer node-a", got)
	}
	for name, ctx := range map[string]context.Context{
		"no stamp":    context.Background(),
		"empty stamp": withCallerNode(context.Background(), ""),
	} {
		if got := CallerNode(ctx); got != "" {
			t.Fatalf("%s: CallerNode = %q, want none", name, got)
		}
	}
}
