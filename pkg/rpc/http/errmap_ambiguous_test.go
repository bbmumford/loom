/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package rpchttp

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/bbmumford/loom/pkg/dispatch"
)

// An ambiguous effect answers 502 and is never marked retryable: the call left
// and may have run, so a retry could run it again. That holds whatever ended
// the wait, including a deadline or a closed session, whose own mappings would
// otherwise answer 504 or a retryable 502.
func TestAnAmbiguousEffectIsANonRetryableBadGateway(t *testing.T) {
	for name, cause := range map[string]error{
		"a deadline":                 context.DeadlineExceeded,
		"a session closed mid-call":  errors.New("session closed before send"),
		"a receive that broke":       errors.New("receive: connection reset"),
		"a relay that lost the call": errors.New("forward svc.Op failed (hop 1)"),
	} {
		err := fmt.Errorf("dispatch svc/svc.Op: %w", &dispatch.AmbiguousEffectError{Handler: "svc.Op", Cause: cause})
		status, _, retry := classifyMeshError(err)
		if status != 502 || retry {
			t.Errorf("%s: classifyMeshError = %d, retry %v; want 502 and no retry", name, status, retry)
		}
	}
}
