/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package effect

import "testing"

// The zero value, and any value outside the three classes, is Unsafe: an
// operation that declares nothing is held to one execution.
func TestTheZeroValueAndUnknownValuesAreNotRepeatable(t *testing.T) {
	var zero Class
	if zero != Unsafe || zero.Repeatable() {
		t.Fatalf("the zero Class is %v (repeatable %v); want Unsafe", zero, zero.Repeatable())
	}
	if Class(99).Repeatable() {
		t.Fatal("an undeclared value reads as Repeatable")
	}
	if !Idempotent.Repeatable() || !ReadOnly.Repeatable() {
		t.Fatal("Idempotent and ReadOnly must be Repeatable")
	}
}

// Least is the less repeatable of two classes, so one Unsafe or unknown
// holder makes the call Unsafe.
func TestLeastTakesTheLessRepeatableClass(t *testing.T) {
	for _, tc := range []struct{ a, b, want Class }{
		{ReadOnly, ReadOnly, ReadOnly},
		{ReadOnly, Idempotent, Idempotent},
		{Idempotent, ReadOnly, Idempotent},
		{Idempotent, Unsafe, Unsafe},
		{Unsafe, ReadOnly, Unsafe},
		{ReadOnly, Class(99), Unsafe},
	} {
		if got := Least(tc.a, tc.b); got != tc.want {
			t.Errorf("Least(%v, %v) = %v, want %v", tc.a, tc.b, got, tc.want)
		}
	}
}

// Parse reads what String writes, and an unknown name is Unsafe.
func TestParseReadsWhatStringWrites(t *testing.T) {
	for _, c := range []Class{Unsafe, Idempotent, ReadOnly} {
		if got, ok := Parse(c.String()); !ok || got != c {
			t.Errorf("Parse(%q) = (%v, %v), want (%v, true)", c.String(), got, ok, c)
		}
	}
	if got, ok := Parse("cached"); ok || got != Unsafe {
		t.Errorf("Parse of an unknown name = (%v, %v), want (Unsafe, false)", got, ok)
	}
}
