/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package dispatch

import (
	"errors"
	"testing"
)

// The caller every GetCaller reader sends through cannot be swapped for one that
// skips the admission: once one is installed SetCaller takes one *HWPCaller and
// refuses any other caller and any replacement, and the first stays in force.
func TestTheCallerIsSealedOnceAnAdmissionIsInstalled(t *testing.T) {
	resetOutbound()
	t.Cleanup(resetOutbound)
	if err := SetCaller(NewLocalCaller(nil)); err != nil {
		t.Fatalf("SetCaller with no admission installed: %v", err)
	}
	resetOutbound()
	installGate(t)

	if err := SetCaller(NewLocalCaller(nil)); !errors.Is(err, ErrCallerSealed) {
		t.Fatalf("SetCaller(LocalCaller) = %v, want ErrCallerSealed", err)
	}
	first := NewHWPCaller(newAdmissionFinder())
	defer first.Close()
	if err := SetCaller(first); err != nil {
		t.Fatalf("SetCaller(HWPCaller): %v", err)
	}
	second := NewHWPCaller(newAdmissionFinder())
	defer second.Close()
	if err := SetCaller(second); !errors.Is(err, ErrCallerSealed) {
		t.Fatalf("a replacement SetCaller = %v, want ErrCallerSealed", err)
	}
	if GetCaller() != Caller(first) {
		t.Fatal("the replacement is in force")
	}
}
