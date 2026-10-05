/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package rpc

import "testing"

// A Handler that declares no OpClass carries OpStandard, the class node's
// forwarder applies to a request that names none. A zero value of OpBulk
// would put every undeclared handler on the floor that admits any path.
func TestAnUndeclaredHandlerOperationClassIsStandard(t *testing.T) {
	var h Handler
	if h.OpClass != OpStandard {
		t.Fatalf("an undeclared Handler.OpClass is %d, want OpStandard (%d)", h.OpClass, OpStandard)
	}
	seen := map[OperationClass]bool{}
	for _, oc := range []OperationClass{OpStandard, OpBulk, OpRealtime, OpCritical} {
		if seen[oc] {
			t.Fatalf("operation class value %d is declared twice", oc)
		}
		seen[oc] = true
	}
}
