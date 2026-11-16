/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package rpchttp

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/bbmumford/loom/pkg/rpc"
)

// A call that reached only nodes still starting answers 503 with Retry-After,
// whether the dispatcher returned the refusal (classifyMeshError) or a local
// handler's own downstream call did (handlerError), and the bridge writes the
// header with the status.
func TestAStartingRefusalIsAnswered503WithRetryAfter(t *testing.T) {
	proxied := fmt.Errorf("dispatch test.start/Op: all 2 probes failed: %w", fmt.Errorf("%w: test.start.Op", rpc.ErrStarting))
	status, retryAfter, eligible := classifyMeshError(proxied)
	if status != http.StatusServiceUnavailable || retryAfter != "1" || eligible {
		t.Errorf("classifyMeshError = %d, %q, %v; want 503, \"1\", false", status, retryAfter, eligible)
	}

	local := handlerError(fmt.Errorf("downstream: %w", rpc.ErrStarting), "test.start.Op")
	if local.status != http.StatusServiceUnavailable || local.retryAfter != "1" {
		t.Errorf("handlerError = %d, %q; want 503, \"1\"", local.status, local.retryAfter)
	}
	rec := httptest.NewRecorder()
	writeBridgeError(rec, local)
	if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") != "1" {
		t.Errorf("the bridge wrote %d with Retry-After %q, want 503 with \"1\"", rec.Code, rec.Header().Get("Retry-After"))
	}
}
