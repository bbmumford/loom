/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package rpchttp

import (
	"fmt"
	"testing"

	"github.com/bbmumford/loom/pkg/dispatch"
)

// Adverts that disagree answer 503 and are not retried: nothing left this node and
// a resend meets the same adverts.
func TestAdvertsThatDisagreeAreUnavailableAndNotRetried(t *testing.T) {
	err := fmt.Errorf("dispatch svc/svc.Op: %w", &dispatch.AnswerersDisagreeError{Handler: "svc.Op"})
	status, retryAfter, retry := classifyMeshError(err)
	if status != 503 || retryAfter != "" || retry {
		t.Errorf("classifyMeshError = %d, %q, retry %v; want 503, no hint, no retry", status, retryAfter, retry)
	}
}
