/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package rpchttp

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	"github.com/bbmumford/loom/pkg/dispatch"
	"github.com/bbmumford/loom/pkg/rpc"
)

// A handler's conflict with its stored state answers 409 and is not retried,
// whether a handler on another node answered it (it is then that handler's
// answer as well, a dispatch.ErrHandlerError) or one in this process did; a
// conflict that also refuses the caller answers 403. A Func the bridge runs
// in its own process answers the same.
func TestAConflictAnswers409AndIsNotRetried(t *testing.T) {
	stale := fmt.Errorf("%w: host: role revision 3 is stale", rpc.ErrConflict)
	denial := fmt.Errorf("%w: host: the caller holds no grant", rpc.ErrScopeDenied)
	for _, tc := range []struct {
		name   string
		err    error
		status int
	}{
		{"a far handler's conflict", fmt.Errorf("dispatch svc/svc.Op: %w", errors.Join(rpc.ErrConflict, dispatch.ErrHandlerError)), http.StatusConflict},
		{"a local handler's conflict", stale, http.StatusConflict},
		{"a conflict that also refuses the caller", fmt.Errorf("%w: %w", denial, stale), http.StatusForbidden},
	} {
		t.Run(tc.name, func(t *testing.T) {
			status, retryAfter, retry := classifyMeshError(tc.err)
			if status != tc.status || retryAfter != "" || retry {
				t.Errorf("classifyMeshError = %d, %q, retry %v; want %d, no hint, no retry",
					status, retryAfter, retry, tc.status)
			}
			if got := classifyHandlerStatus(tc.err); got != tc.status {
				t.Errorf("classifyHandlerStatus = %d; want %d", got, tc.status)
			}
		})
	}
}
