/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package grade

import "testing"

// An OperationClass nobody set is Standard, whose floor is C. node's
// forwarder holds a request that names no class to that floor, so the zero
// value and the gate's default must be the same class.
func TestTheZeroOperationClassIsStandard(t *testing.T) {
	var oc OperationClass
	if oc != OpClassStandard {
		t.Fatalf("the zero OperationClass is %v, want %v", oc, OpClassStandard)
	}
	if got := oc.MinGrade(); got != GradeC {
		t.Fatalf("the zero OperationClass has floor %v, want %v", got, GradeC)
	}
}

// ParseOperationClass maps each hint dispatch.WithOpClass can carry to its
// class, the empty hint to Standard, and refuses every other name, so the
// forwarder never holds an unknown class to another class's floor.
func TestParseOperationClassRefusesAnUnknownName(t *testing.T) {
	for _, tc := range []struct {
		name  string
		want  OperationClass
		floor Grade
	}{
		{"", OpClassStandard, GradeC},
		{"standard", OpClassStandard, GradeC},
		{"bulk", OpClassBulk, GradeF},
		{"realtime", OpClassRealtime, GradeB},
		{"critical", OpClassCritical, GradeB},
	} {
		got, ok := ParseOperationClass(tc.name)
		if !ok || got != tc.want {
			t.Errorf("ParseOperationClass(%q) = %v, %v; want %v, true", tc.name, got, ok, tc.want)
		}
		if got.MinGrade() != tc.floor {
			t.Errorf("class %q has floor %v, want %v", tc.name, got.MinGrade(), tc.floor)
		}
	}
	for _, name := range []string{"turbo", "Bulk", " standard", "unknown"} {
		if _, ok := ParseOperationClass(name); ok {
			t.Errorf("ParseOperationClass(%q) accepted a name the gate does not declare", name)
		}
	}
}
