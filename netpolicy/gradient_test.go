/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package netpolicy

import (
	"testing"
	"time"
)

func TestInterpolate_EndpointsExact(t *testing.T) {
	from := ProfileFor(LinkEthernet) // ReconcilePacing 0 (unlimited)
	to := ProfileFor(LinkCellular)   // ReconcilePacing 8 KB/s
	ramp := 30 * time.Second

	if got := InterpolateProfile(from, to, 0, ramp); got.ReconcilePacing != from.ReconcilePacing {
		t.Fatalf("elapsed 0 must yield `from` pacing (%d), got %d", from.ReconcilePacing, got.ReconcilePacing)
	}
	if got := InterpolateProfile(from, to, ramp, ramp); got.ReconcilePacing != to.ReconcilePacing {
		t.Fatalf("elapsed==ramp must yield `to` pacing (%d), got %d", to.ReconcilePacing, got.ReconcilePacing)
	}
	if got := InterpolateProfile(from, to, 2*ramp, ramp); got != to {
		t.Fatal("past the ramp must yield `to` exactly")
	}
	if got := InterpolateProfile(from, to, 10*time.Second, 0); got != to {
		t.Fatal("a non-positive ramp must yield `to` immediately")
	}
}

func TestInterpolate_BandwidthEasesUnlimitedToCapped(t *testing.T) {
	from := NetworkProfile{ReconcilePacing: 0}      // unlimited
	to := NetworkProfile{ReconcilePacing: 8 * 1024} // 8 KB/s
	ramp := 30 * time.Second

	// Halfway, the pacing should be well above the target (still easing down) but far below unlimited.
	mid := InterpolateProfile(from, to, 15*time.Second, ramp).ReconcilePacing
	if mid <= to.ReconcilePacing {
		t.Fatalf("halfway pacing should still be above the target while easing, got %d", mid)
	}
	if mid == 0 {
		t.Fatal("halfway pacing must not read as unlimited")
	}
	// Monotonic descent: later in the ramp is tighter than earlier.
	early := InterpolateProfile(from, to, 5*time.Second, ramp).ReconcilePacing
	late := InterpolateProfile(from, to, 25*time.Second, ramp).ReconcilePacing
	if !(early > late && late > to.ReconcilePacing) {
		t.Fatalf("pacing must descend monotonically toward the cap: early=%d late=%d target=%d", early, late, to.ReconcilePacing)
	}
}

func TestInterpolate_IntervalsFlipImmediately(t *testing.T) {
	from := ProfileFor(LinkEthernet)
	to := ProfileFor(LinkCellular)
	// Even one instant into the ramp, the cadence intervals are already the target's — no gradient.
	got := InterpolateProfile(from, to, time.Millisecond, 30*time.Second)
	if got.GossipBase != to.GossipBase || got.RumorRetryInitial != to.RumorRetryInitial || got.RumorFanout != to.RumorFanout {
		t.Fatalf("cadence/structure must flip immediately, got %+v", got)
	}
	// But bandwidth is still near the `from` (unlimited) end this early.
	if got.ReconcilePacing == to.ReconcilePacing {
		t.Fatal("bandwidth must NOT have flipped immediately — it ramps")
	}
}

func TestInterpolate_LooseningRampsUpToUnlimited(t *testing.T) {
	from := NetworkProfile{ReconcilePacing: 8 * 1024} // capped
	to := NetworkProfile{ReconcilePacing: 0}          // unlimited
	ramp := 30 * time.Second
	// Completed → unlimited.
	if got := InterpolateProfile(from, to, ramp, ramp).ReconcilePacing; got != 0 {
		t.Fatalf("loosening to unlimited must end at 0, got %d", got)
	}
	// Midway, pacing has climbed above the starting cap.
	if mid := InterpolateProfile(from, to, 15*time.Second, ramp).ReconcilePacing; mid <= from.ReconcilePacing {
		t.Fatalf("loosening should raise the cap while ramping, got %d", mid)
	}
}
