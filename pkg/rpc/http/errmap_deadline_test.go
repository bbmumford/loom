/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package rpchttp

import (
	"context"
	"fmt"
	"testing"

	"github.com/bbmumford/loom/pkg/dispatch"
)

// A deadline refusal answers 504, as a context deadline does: the request ran
// out of time on the way, at the hop that refused it or at the server it
// reached late, and neither the handler nor the gateway failed. Neither is
// retried here.
func TestADeadlineRefusalIsAGatewayTimeout(t *testing.T) {
	for name, err := range map[string]error{
		"the context deadline":      fmt.Errorf("dispatch svc/svc.Op: %w", context.DeadlineExceeded),
		"a hop that does not fit":   fmt.Errorf("dispatch svc/svc.Op: all 2 probes failed: %w", &dispatch.RouteDeadlineError{Handler: "svc.Op", Remaining: 3, Need: 9}),
		"a server reached too late": fmt.Errorf("dispatch svc/svc.Op: %w", &dispatch.RouteDeadlineError{Handler: "svc.Op", Remaining: -5}),
	} {
		status, _, retry := classifyMeshError(err)
		if status != 504 || retry {
			t.Errorf("%s: classifyMeshError = %d, retry %v; want 504 and no retry", name, status, retry)
		}
	}
}
