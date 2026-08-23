/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import "testing"

// COVERAGE of the resource-tier surface: calculateMaxRoles (:104),
// MeetsGatewayRequirements (:169), CanRunAdditionalRole (:175),
// GetResourceTier (:180), GetRecommendedRoles (:191), ParseResourceString
// (:213) — all at 0.0%.
//
// CENSUS, stated with its bound:
//
//	calculateMaxRoles         2 non-test callers (DetectSystemResources)
//	ParseResourceString       0 · MeetsGatewayRequirements 0 · CanRunAdditionalRole 0
//	GetResourceTier           0 · GetRecommendedRoles      0 · LogResources          0
//
// ⚠ SIX WITH ZERO NON-TEST CALLERS — and I am NOT calling them dead. They are
// EXPORTED methods on an EXPORTED type in a PUBLISHED module
// (github.com/bbmumford/loom, 22 go.mod consumers), so a
// source census of this repo cannot bound their callers. "Zero here" is the
// honest claim; "unused" is not one I can make.

// 🔴 THE HIGHEST-VALUE PROPERTY IN THIS FILE: calculateMaxRoles AND
// GetResourceTier INDEPENDENTLY ENCODE THE SAME THRESHOLDS.
//
//	calculateMaxRoles  <512MB || <1cpu → 1 · <1024MB || <2cpu → 2 · else 3
//	GetResourceTier    >=1024 && >=2 → "high" · >=512 && >=1 → "medium" · else "low"
//
// Two implementations of one policy, in one file, with no shared constant. They
// agree today. Nothing makes them agree tomorrow — change one boundary and a
// node reports tier "high" while being allowed 2 roles, and no test would have
// noticed. This pins the agreement itself rather than either side.
func TestTierAndMaxRolesAgreeAtEveryBoundary(t *testing.T) {
	tierRoles := map[string]int{"low": 1, "medium": 2, "high": 3}

	for _, tc := range []struct {
		mem int64
		cpu int
	}{
		{0, 0}, {256, 1}, {511, 1}, {511, 4}, // low
		{512, 1}, {512, 2}, {1023, 2}, {2048, 1}, // medium
		{1024, 2}, {4096, 8}, // high
	} {
		gotRoles := calculateMaxRoles(tc.mem, tc.cpu)
		r := &SystemResources{MemoryMB: tc.mem, CPUCores: tc.cpu}
		gotTier := r.GetResourceTier()

		if want, ok := tierRoles[gotTier]; !ok || gotRoles != want {
			t.Errorf("mem=%dMB cpu=%d: tier %q implies %d roles but calculateMaxRoles says %d — "+
				"the two thresholds have diverged. A node would report a tier its role budget "+
				"does not match, and nothing else in this file cross-checks them",
				tc.mem, tc.cpu, gotTier, want, gotRoles)
		}
	}
}

// 🔴 THE MAX_ROLES OVERRIDE IS UNBOUNDED, AND THAT IS THE WHOLE POINT OF PINNING
// IT. It is checked BEFORE any resource arithmetic, so a value of 1000 on a
// 256MB single-core box returns 1000. That may well be intended — an operator
// override that resources cannot veto — but it is the kind of thing that should
// fail a test deliberately rather than surprise someone.
func TestMaxRolesEnvOverridesResourceLimitsEntirely(t *testing.T) {
	t.Setenv("MAX_ROLES", "1000")

	if got := calculateMaxRoles(256, 1); got != 1000 {
		t.Fatalf("calculateMaxRoles(256MB, 1cpu) with MAX_ROLES=1000 = %d, want 1000 — "+
			"the override is no longer absolute", got)
	}
	// …and it wins on an ample box too, i.e. it is an override, not a ceiling.
	if got := calculateMaxRoles(8192, 16); got != 1000 {
		t.Fatalf("calculateMaxRoles(8GB, 16cpu) with MAX_ROLES=1000 = %d, want 1000", got)
	}
}

// A malformed or non-positive MAX_ROLES must fall through to the resource
// calculation rather than being honoured or zeroing the budget. Zero roles
// would mean a node that can never take any work.
func TestAnUnusableMaxRolesFallsThroughToResourceRules(t *testing.T) {
	for _, bad := range []string{"", "0", "-3", "lots", "3.5"} {
		t.Setenv("MAX_ROLES", bad)
		if got := calculateMaxRoles(2048, 4); got != 3 {
			t.Errorf("MAX_ROLES=%q gave %d roles on an ample box, want the resource-derived 3 — "+
				"an unusable override is being honoured", bad, got)
		}
	}
}

func TestResourceRulesAtTheirExactBoundaries(t *testing.T) {
	t.Setenv("MAX_ROLES", "") // ensure no override

	for _, tc := range []struct {
		mem  int64
		cpu  int
		want int
	}{
		{511, 8, 1},  // memory just under the floor dominates a large CPU count
		{512, 0, 1},  // cpu under the floor dominates ample memory
		{512, 1, 2},  // exactly at the low floor
		{1023, 4, 2}, // memory just under the high bar
		{4096, 1, 2}, // cpu just under the high bar
		{1024, 2, 3}, // exactly at the high bar
	} {
		if got := calculateMaxRoles(tc.mem, tc.cpu); got != tc.want {
			t.Errorf("calculateMaxRoles(%dMB, %dcpu) = %d, want %d", tc.mem, tc.cpu, got, tc.want)
		}
	}
}

// The gateway minimum is its own threshold and must not drift into the tier
// boundaries — it is checked independently before a node takes the role.
func TestGatewayRequirementsAreTheirOwnThreshold(t *testing.T) {
	for _, tc := range []struct {
		mem  int64
		cpu  int
		want bool
	}{
		{512, 1, true}, {2048, 8, true},
		{511, 1, false}, {512, 0, false}, {0, 0, false},
	} {
		r := &SystemResources{MemoryMB: tc.mem, CPUCores: tc.cpu}
		if got := r.MeetsGatewayRequirements(); got != tc.want {
			t.Errorf("MeetsGatewayRequirements(%dMB, %dcpu) = %v, want %v — a node would take "+
				"the gateway role on resources below its stated minimum", tc.mem, tc.cpu, got, tc.want)
		}
	}
}

// CanRunAdditionalRole is a strict "<", so a node already at MaxRoles must be
// refused. An off-by-one here over-commits every node in the mesh by one role.
func TestANodeAtItsRoleCeilingIsRefusedAnother(t *testing.T) {
	r := &SystemResources{MaxRoles: 2}

	if !r.CanRunAdditionalRole(0) || !r.CanRunAdditionalRole(1) {
		t.Error("a node below its ceiling was refused an additional role")
	}
	if r.CanRunAdditionalRole(2) {
		t.Error("a node AT its ceiling (2 of 2) was allowed another — every node in the mesh " +
			"over-commits by one role")
	}
	if r.CanRunAdditionalRole(99) {
		t.Error("a node far past its ceiling was allowed another")
	}
}

// GetRecommendedRoles leaves headroom but must never recommend zero — a node
// recommended zero roles contributes nothing and would never be selected.
func TestRecommendedRolesLeavesHeadroomButNeverReachesZero(t *testing.T) {
	for _, tc := range []struct{ max, want int }{
		{3, 2}, {2, 1}, {1, 1}, {0, 1}, {-5, 1},
	} {
		r := &SystemResources{MaxRoles: tc.max}
		if got := r.GetRecommendedRoles(); got != tc.want {
			t.Errorf("GetRecommendedRoles with MaxRoles=%d = %d, want %d — a zero or negative "+
				"recommendation makes the node uncontactable for work", tc.max, got, tc.want)
		}
	}
}

// 🔴 CHARACTERISATION, AND IT IS THIS SESSION'S SIGNATURE SHAPE ONE MORE TIME.
// ParseResourceString returns (0, unit) for anything it cannot parse — with NO
// error. A caller reading value==0 cannot distinguish "the string said 0MB"
// from "the string was garbage".
func TestParseResourceStringCannotSignalFailure(t *testing.T) {
	zeroValue, zeroUnit := ParseResourceString("0MB")
	junkValue, junkUnit := ParseResourceString("wat")

	if zeroValue != junkValue {
		t.Fatalf("a real zero (%d) and an unparseable string (%d) now differ — that is very "+
			"likely an improvement; update this test deliberately",
			zeroValue, junkValue)
	}
	if zeroValue != 0 {
		t.Fatalf("ParseResourceString(\"0MB\") value = %d, want 0", zeroValue)
	}
	if zeroUnit != "mb" || junkUnit != "wat" {
		t.Errorf("units: got %q and %q, want \"mb\" and \"wat\"", zeroUnit, junkUnit)
	}
}

func TestParseResourceStringSplitsAndLowercases(t *testing.T) {
	for _, tc := range []struct {
		in    string
		value int64
		unit  string
	}{
		{"512MB", 512, "mb"},
		{"  2cores  ", 2, "cores"},
		{"8GiB", 8, "gib"},
		{"1.9GB", 1, "gb"}, // float truncation, not rounding — 1.9 becomes 1
		{"", 0, ""},
		{"MB512", 0, "mb512"}, // leading unit: nothing numeric to parse
	} {
		v, u := ParseResourceString(tc.in)
		if v != tc.value || u != tc.unit {
			t.Errorf("ParseResourceString(%q) = (%d, %q), want (%d, %q)",
				tc.in, v, u, tc.value, tc.unit)
		}
	}
}

// calculateCapacity reads live host metrics, so its VALUE is not assertable —
// but its RANGE is, and the clamp is what protects every consumer of it.
func TestCapacityIsAlwaysWithinItsClampedRange(t *testing.T) {
	for i := 0; i < 5; i++ {
		c := calculateCapacity()
		if c < 0.0 || c > 1.0 {
			t.Fatalf("calculateCapacity() = %v, outside [0,1] — the clamp is not holding and "+
				"every bidding/load-balancing consumer receives an out-of-range weight", c)
		}
	}
}

// LogResources must not panic on a zero-valued struct — it is called on
// whatever DetectSystemResources returns, including degraded results.
func TestLogResourcesSurvivesAZeroValuedStruct(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("LogResources panicked on a zero-valued SystemResources: %v", r)
		}
	}()
	(&SystemResources{}).LogResources()
}
