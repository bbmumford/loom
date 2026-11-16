/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package capabilities

import (
	"errors"
	"fmt"
	"reflect"
	"testing"
)

// calculateMaxRoles and GetResourceTier each encode the same memory and CPU
// thresholds with no shared constant. SelectRoles reads MaxRoles for the role
// budget while a node reports its tier, so a boundary moved in one and not the
// other would let a node report a tier its role budget does not match.
func TestTierAndMaxRolesAgreeAtEveryBoundary(t *testing.T) {
	t.Setenv("MAX_ROLES", "")
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
			t.Errorf("mem=%dMB cpu=%d: tier %q implies %d roles but calculateMaxRoles says %d; "+
				"the two thresholds have diverged", tc.mem, tc.cpu, gotTier, want, gotRoles)
		}
	}
}

// MAX_ROLES is read before any resource arithmetic, so it is an operator
// override the host's resources cannot veto, on a small box and a large one.
func TestMaxRolesEnvOverridesResourceLimitsEntirely(t *testing.T) {
	t.Setenv("MAX_ROLES", "1000")

	if got := calculateMaxRoles(256, 1); got != 1000 {
		t.Fatalf("calculateMaxRoles(256MB, 1cpu) with MAX_ROLES=1000 = %d, want 1000: "+
			"the override is no longer absolute", got)
	}
	if got := calculateMaxRoles(8192, 16); got != 1000 {
		t.Fatalf("calculateMaxRoles(8GB, 16cpu) with MAX_ROLES=1000 = %d, want 1000", got)
	}
}

// A malformed or non-positive MAX_ROLES falls through to the resource rules: a
// zero budget would leave a node that never takes work.
func TestAnUnusableMaxRolesFallsThroughToResourceRules(t *testing.T) {
	for _, bad := range []string{"", "0", "-3", "lots", "3.5"} {
		t.Setenv("MAX_ROLES", bad)
		if got := calculateMaxRoles(2048, 4); got != 3 {
			t.Errorf("MAX_ROLES=%q gave %d roles on an ample box, want the resource-derived 3: "+
				"an unusable override is being honoured", bad, got)
		}
	}
}

func TestResourceRulesAtTheirExactBoundaries(t *testing.T) {
	t.Setenv("MAX_ROLES", "")

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

// The gateway minimum is its own threshold, checked before a node takes the
// role, and must not drift into the tier boundaries.
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
			t.Errorf("MeetsGatewayRequirements(%dMB, %dcpu) = %v, want %v: a node would take "+
				"the gateway role on resources below its stated minimum", tc.mem, tc.cpu, got, tc.want)
		}
	}
}

// CanRunAdditionalRole is a strict "<": a node already at MaxRoles is refused
// another, or every node over-commits by one role.
func TestANodeAtItsRoleCeilingIsRefusedAnother(t *testing.T) {
	r := &SystemResources{MaxRoles: 2}

	if !r.CanRunAdditionalRole(0) || !r.CanRunAdditionalRole(1) {
		t.Error("a node below its ceiling was refused an additional role")
	}
	if r.CanRunAdditionalRole(2) {
		t.Error("a node at its ceiling (2 of 2) was allowed another")
	}
	if r.CanRunAdditionalRole(99) {
		t.Error("a node far past its ceiling was allowed another")
	}
}

// GetRecommendedRoles leaves headroom but never recommends zero: a node
// recommended zero roles contributes nothing and is never selected.
func TestRecommendedRolesLeavesHeadroomButNeverReachesZero(t *testing.T) {
	for _, tc := range []struct{ max, want int }{
		{3, 2}, {2, 1}, {1, 1}, {0, 1}, {-5, 1},
	} {
		r := &SystemResources{MaxRoles: tc.max}
		if got := r.GetRecommendedRoles(); got != tc.want {
			t.Errorf("GetRecommendedRoles with MaxRoles=%d = %d, want %d", tc.max, got, tc.want)
		}
	}
}

// ParseResourceString returns (0, unit) for anything it cannot parse, with no
// error, so a caller reading value==0 cannot tell "0MB" from an unparseable
// string. This pins that shape; a change that separates the two fails here and
// updates the test on purpose.
func TestParseResourceStringCannotSignalFailure(t *testing.T) {
	zeroValue, zeroUnit := ParseResourceString("0MB")
	junkValue, junkUnit := ParseResourceString("wat")

	if zeroValue != junkValue {
		t.Fatalf("a real zero (%d) and an unparseable string (%d) now differ; update this test deliberately",
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
		{"1.9GB", 1, "gb"}, // float truncation, not rounding
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

// calculateCapacity reads live host metrics, so its value is not assertable,
// but its clamp to [0,1] is what every reader of SystemResources.Capacity
// relies on.
func TestCapacityIsAlwaysWithinItsClampedRange(t *testing.T) {
	for i := 0; i < 5; i++ {
		c := calculateCapacity()
		if c < 0.0 || c > 1.0 {
			t.Fatalf("calculateCapacity() = %v, outside [0,1]", c)
		}
	}
}

// LogResources is called on whatever DetectSystemResources returns, including
// a degraded result, so a zero-valued struct must not panic.
func TestLogResourcesSurvivesAZeroValuedStruct(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("LogResources panicked on a zero-valued SystemResources: %v", r)
		}
	}()
	(&SystemResources{}).LogResources()
}

// The load-time report names each gopsutil probe that fails with its error,
// one line each, and stays silent for a probe that works.
func TestDetectorDegradationIsReportedPerFailingProbe(t *testing.T) {
	oldMem, oldCPU := probeVirtualMemory, probeCPUCounts
	t.Cleanup(func() { probeVirtualMemory, probeCPUCounts = oldMem, oldCPU })

	memErr := errors.New("meminfo unreadable")
	cpuErr := errors.New("cpu count unreadable")
	const (
		memLine = "Warning: gopsutil memory detection unavailable, falling back to env vars/runtime: meminfo unreadable"
		cpuLine = "Warning: gopsutil CPU detection unavailable, falling back to env vars/runtime: cpu count unreadable"
	)

	for _, tc := range []struct {
		name     string
		mem, cpu error
		want     []string
	}{
		{"both fail", memErr, cpuErr, []string{memLine, cpuLine}},
		{"memory fails", memErr, nil, []string{memLine}},
		{"cpu fails", nil, cpuErr, []string{cpuLine}},
		{"both work", nil, nil, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			memResult, cpuResult := tc.mem, tc.cpu
			probeVirtualMemory = func() error { return memResult }
			probeCPUCounts = func() error { return cpuResult }

			var got []string
			reportDetectorDegradation(func(format string, args ...any) {
				got = append(got, fmt.Sprintf(format, args...))
			})
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("reported %q, want %q", got, tc.want)
			}
		})
	}
}
