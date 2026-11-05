/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package rpchttp

import (
	"errors"
	"fmt"
	"testing"

	"github.com/bbmumford/loom/pkg/dispatch"
	"github.com/bbmumford/loom/pkg/rpc"
)

// A refusal of the caller's authority answers 403 and is not retried, whether
// a handler on another node made it (it is then that handler's answer as well,
// a dispatch.ErrHandlerError) or one in this process did; a handler's other
// failure keeps answering 400.
func TestARefusalOfTheCallerIsForbiddenAndNotRetried(t *testing.T) {
	for _, tc := range []struct {
		name   string
		err    error
		status int
	}{
		{"a far handler's refusal", fmt.Errorf("dispatch svc/svc.Op: %w", errors.Join(rpc.ErrScopeDenied, dispatch.ErrHandlerError)), 403},
		{"a local handler's refusal", fmt.Errorf("%w: host: the caller holds no grant", rpc.ErrScopeDenied), 403},
		{"another handler failure", fmt.Errorf("dispatch svc/svc.Op: %w", dispatch.NewHandlerError("not found")), 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, retryAfter, retry := classifyMeshError(tc.err)
			if status != tc.status || retryAfter != "" || retry {
				t.Errorf("classifyMeshError = %d, %q, retry %v; want %d, no hint, no retry",
					status, retryAfter, retry, tc.status)
			}
		})
	}
}
