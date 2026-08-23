/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"testing"
	"time"
)

// COVERAGE of the upgrade walker's probe-cadence gate and consecutive-stall
// counter, all at 0.0%.
//
// CENSUSED FIRST, per symbol — all three are live:
//
//	minProbeIntervalForGrade  <- upgrade_walker.go:202 (the snapshot rate limit)
//	bumpConsecutiveStalls     <- upgrade_walker.go:404 (probeUpgrade)
//	clearConsecutiveStalls    <- upgrade_walker.go:423 (probeUpgrade, on success)
//
// 🔴 THESE TWO DECIDE HOW A DEGRADED TRANSPORT IS TREATED. The counter picks
// between a SHORT stall cooldown (re-probe soon, the path may be transiently
// lossy) and escalation to the LONG exponential dial-failure ladder (the path
// is empirically broken). Escalate too eagerly and a path that would have
// recovered is parked for up to 10 minutes; too reluctantly and a permanently
// broken path burns a handshake every couple of minutes forever — which is the
// failure the escalation was added to stop.

// ── The per-grade probe cadence ─────────────────────────────────────────────

// 🔑 GRADE A DELIBERATELY GETS THE GRADE-C CADENCE, and the comment explains
// why at length: bestActiveGrade() can report GradeA from a WebSocket/TLS
// session, or go stale after a noise-UDP path fell back without its
// transportConn being reaped. Freezing GradeA for 24h locked mis-graded peers
// onto WS/TLS with zero noise-UDP re-probes. Anyone "optimising" this by
// throttling GradeA re-introduces that bug.
func TestGradeAIsProbedAtTheGradeCCadenceOnPurpose(t *testing.T) {
	a := minProbeIntervalForGrade(GradeA)
	c := minProbeIntervalForGrade(GradeC)

	if a != c {
		t.Fatalf("GradeA interval = %v, GradeC = %v — they must match. A GradeA "+
			"best-grade does NOT imply the peer is on noise-UDP, so throttling "+
			"GradeA locks a mis-graded peer onto WS/TLS with no noise-UDP "+
			"re-probe ever", a, c)
	}
	if a != upgradeIntervalGradeC {
		t.Fatalf("GradeA interval = %v, want upgradeIntervalGradeC (%v)",
			a, upgradeIntervalGradeC)
	}
}

// Grade B is the one grade that IS throttled — its remaining upgrade headroom
// is small, so probing it at the C cadence would burn slots for little payoff.
func TestGradeBIsTheThrottledGrade(t *testing.T) {
	b := minProbeIntervalForGrade(GradeB)
	if b != upgradeIntervalGradeB {
		t.Fatalf("GradeB interval = %v, want %v", b, upgradeIntervalGradeB)
	}
	if b <= minProbeIntervalForGrade(GradeC) {
		t.Fatalf("GradeB (%v) is not slower than GradeC (%v) — the throttle that "+
			"stops the walker burning probe slots on nearly-optimal peers is "+
			"inverted or gone", b, minProbeIntervalForGrade(GradeC))
	}
}

// Low grades are probed aggressively because every successful upgrade has high
// payoff, and an unknown grade must fall back to the walk interval rather than
// to zero (which would probe on every tick).
func TestLowGradesAreAggressiveAndAnUnknownGradeFallsBackToTheWalkInterval(t *testing.T) {
	for _, g := range []Grade{GradeC, GradeF} {
		if got := minProbeIntervalForGrade(g); got != upgradeIntervalGradeC {
			t.Errorf("grade %v interval = %v, want %v", g, got, upgradeIntervalGradeC)
		}
	}

	// An out-of-range grade must not return 0 — a zero interval means every
	// tick qualifies and the walker probes the peer continuously.
	unknown := minProbeIntervalForGrade(Grade(99))
	if unknown <= 0 {
		t.Fatalf("an unknown grade returned %v — a non-positive interval makes "+
			"every walker tick qualify and the peer is probed continuously",
			unknown)
	}
	if unknown != upgradeWalkInterval {
		t.Fatalf("an unknown grade returned %v, want the walk interval (%v)",
			unknown, upgradeWalkInterval)
	}
}

// ── The consecutive-stall counter ───────────────────────────────────────────

// 🔴 THE COUNTER IS PER (PEER, PROTOCOL). Collapsing either dimension makes one
// peer's stalls escalate another's path, or a peer's WebSocket stalls escalate
// its noise-UDP probing.
func TestStallCountsAreScopedToPeerAndProtocolTogether(t *testing.T) {
	m := registerTestManager()

	if got := m.bumpConsecutiveStalls(testNodeIDB, ProtoNoiseUDP); got != 1 {
		t.Fatalf("first bump returned %d, want 1", got)
	}
	if got := m.bumpConsecutiveStalls(testNodeIDB, ProtoNoiseUDP); got != 2 {
		t.Fatalf("second bump returned %d, want 2 — the counter is not "+
			"accumulating, so a permanently broken path never escalates", got)
	}

	// A different protocol on the SAME peer starts fresh.
	if got := m.bumpConsecutiveStalls(testNodeIDB, ProtoWebSocket); got != 1 {
		t.Fatalf("a different protocol returned %d, want 1 — a peer's WebSocket "+
			"stalls are escalating its noise-UDP probing", got)
	}
	// A different peer on the SAME protocol starts fresh.
	if got := m.bumpConsecutiveStalls(testNodeIDA, ProtoNoiseUDP); got != 1 {
		t.Fatalf("a different peer returned %d, want 1 — one peer's stalls are "+
			"escalating another peer's path", got)
	}
	// And the original pair is untouched by either.
	if got := m.bumpConsecutiveStalls(testNodeIDB, ProtoNoiseUDP); got != 3 {
		t.Fatalf("the original pair returned %d, want 3 — the other bumps "+
			"disturbed its count", got)
	}
}

// 🔑 A RECOVERY MUST RESTORE THE FULL BUDGET. The doc is explicit: a transport
// that briefly degrades and recovers gets the whole
// upgradeStallEscalationThreshold again. Without the reset, a path that stalls
// once a day is escalated to the long cooldown after three days.
func TestASuccessfulProbeRestoresTheFullStallBudget(t *testing.T) {
	m := registerTestManager()

	for i := 1; i <= upgradeStallEscalationThreshold; i++ {
		if got := m.bumpConsecutiveStalls(testNodeIDB, ProtoNoiseUDP); got != i {
			t.Fatalf("bump %d returned %d", i, got)
		}
	}

	m.clearConsecutiveStalls(testNodeIDB, ProtoNoiseUDP)

	if got := m.bumpConsecutiveStalls(testNodeIDB, ProtoNoiseUDP); got != 1 {
		t.Fatalf("after a successful probe the count resumed at %d, want 1 — a "+
			"path that stalls once and recovers repeatedly is escalated to the "+
			"10-minute cooldown as though it were permanently broken", got)
	}
}

// Clearing is scoped too, and clearing an unknown pair must be a no-op rather
// than a panic — probeUpgrade calls it on every success, including the first.
func TestClearingIsScopedAndSafeForUnknownPairs(t *testing.T) {
	m := registerTestManager()

	m.clearConsecutiveStalls(testNodeIDB, ProtoNoiseUDP) // nothing recorded yet
	m.bumpConsecutiveStalls(testNodeIDB, ProtoNoiseUDP)
	m.bumpConsecutiveStalls(testNodeIDA, ProtoNoiseUDP)

	m.clearConsecutiveStalls(testNodeIDB, ProtoNoiseUDP)

	// B was cleared; A must not have been.
	if got := m.bumpConsecutiveStalls(testNodeIDA, ProtoNoiseUDP); got != 2 {
		t.Fatalf("clearing peer B reset peer A's count too (A now %d, want 2) — "+
			"one peer's recovery hands another peer a fresh budget it did not "+
			"earn", got)
	}
}

// The escalation threshold is a published constant that probeUpgrade compares
// against. It must stay small enough to be reached by a genuinely broken path
// within a few walker ticks — at the 30s walk interval, 3 stalls is ~90s.
func TestTheEscalationThresholdIsReachableWithinAFewWalkerTicks(t *testing.T) {
	if upgradeStallEscalationThreshold < 2 {
		t.Fatalf("threshold = %d — a single transient loss burst escalates a "+
			"healthy path to the long cooldown", upgradeStallEscalationThreshold)
	}
	worstCase := time.Duration(upgradeStallEscalationThreshold) * upgradeWalkInterval
	if worstCase > 10*time.Minute {
		t.Fatalf("reaching escalation takes %v at the %v walk interval — a "+
			"permanently broken path keeps burning a handshake per tick for that "+
			"long before the exponential ladder engages", worstCase,
			upgradeWalkInterval)
	}
}

// The map is lazily created, so the very first bump on a fresh manager must
// work rather than panicking on a nil map.
func TestTheStallMapIsCreatedLazily(t *testing.T) {
	m := &ConnectionManager{}
	if got := m.bumpConsecutiveStalls(testNodeIDB, ProtoNoiseUDP); got != 1 {
		t.Fatalf("first bump on a zero-value manager returned %d, want 1", got)
	}
	m2 := &ConnectionManager{}
	m2.clearConsecutiveStalls(testNodeIDB, ProtoNoiseUDP) // must not panic
}
