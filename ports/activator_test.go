/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package ports

import (
	"errors"
	"testing"
)

func TestRoleDeactivationDispositionIsExplicitAndPreservesCause(t *testing.T) {
	cause := errors.New("transaction rolled back")
	marked := RoleStillActiveAfterDeactivationError(cause)
	if !errors.Is(marked, cause) {
		t.Fatal("still-active disposition lost the original error")
	}
	if got := RoleDeactivationDispositionOf(marked); got != RoleDeactivationStillActive {
		t.Fatalf("marked disposition = %v, want still active", got)
	}
	if got := RoleDeactivationDispositionOf(cause); got != RoleDeactivationUnknown {
		t.Fatalf("ordinary error disposition = %v, want unknown", got)
	}
	if marked := RoleStillActiveAfterDeactivationError(nil); marked != nil {
		t.Fatalf("nil error became %v", marked)
	}
}
