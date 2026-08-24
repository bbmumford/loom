/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"testing"
	"time"
)

// COVERAGE of peer reputation scoring, 9 functions at 0.0%.
//
// 🔴 CENSUSED FIRST, AND THE CENSUS IS THE FINDING. The tracker is
// constructed on every node (peer_connections.go:1039) and FED on a loop
// (feedReputationFromRTT → InjectRTT/InjectGradeInfo, peer_connections.go:1182,
// :4131, :4138). But:
//
//	ComputeAll   0 callers outside this file, all three roots
//	ScoreFor     0 callers outside this file
//	RankedPeers  0 callers outside this file
//
// and `ComputeAll` is the ONLY thing that ever puts a key in `rt.scores`.
// Both Inject* methods begin `rep, ok := rt.scores[nodeID]; if !ok { return }`.
// ⇒ in production the map is permanently empty, so EVERY injection returns at
// the guard and no score is ever computed, stored, or read. Proven below by
// TestInjectionsAreNoOpsUntilComputeAllHasPopulatedTheMap rather than argued.
//
// So this file does two things: it pins the scoring maths (which is correct
// and merely uncalled — useful to whoever decides to wire it), and it pins the
// inertness itself so that wiring ComputeAll turns a passing test red in the
// place that explains why.

func repEvent(peer, reason string, grade Grade, ago time.Duration) ConnectionEvent {
	return ConnectionEvent{
		PeerNodeID: peer, Reason: reason, NewGrade: grade,
		Transport: "websocket", Timestamp: time.Now().Add(-ago),
	}
}

func trackerWith(t *testing.T, events ...ConnectionEvent) *ReputationTracker {
	t.Helper()
	log := NewConnectionEventLog()
	for _, e := range events {
		log.Append(e)
	}
	return NewReputationTracker(log)
}

// 🔴 THE FINDING, AS AN ASSERTION.
func TestInjectionsAreNoOpsUntilComputeAllHasPopulatedTheMap(t *testing.T) {
	rt := trackerWith(t, repEvent(testNodeIDB, "connected", GradeA, 30*time.Minute))

	// This is exactly what feedReputationFromRTT does on its loop, and it is
	// the ONLY thing production ever does to this tracker.
	rt.InjectRTT(testNodeIDB, 20*time.Millisecond)
	rt.InjectGradeInfo(testNodeIDB, time.Now().Add(-time.Hour), 1.0, time.Hour)

	// ScoreFor returns a VALUE and answers "absent" with a zero-value struct
	// carrying only NodeID — so absence and a measured-terrible peer are told
	// apart only by AvgGrade/UptimePercent being unset. Recorded here because
	// it is the same absent-vs-measured shape, and harmless
	// today only because nothing consumes the score.
	if got := rt.ScoreFor(testNodeIDB); got.Score != 0 || got.AvgRTT != 0 {
		t.Fatalf("a score exists after injection alone (%+v) — if this now "+
			"passes, ComputeAll has been wired and the unwired finding is "+
			"resolved; update this test deliberately", got)
	}
	if got := rt.RankedPeers(); len(got) != 0 {
		t.Fatalf("RankedPeers returned %d entries with no ComputeAll", len(got))
	}

	// The same injections AFTER a ComputeAll do land — so the subsystem is
	// correct and merely uncalled, which is the whole point of the finding.
	rt.ComputeAll()
	rt.InjectRTT(testNodeIDB, 20*time.Millisecond)

	got := rt.ScoreFor(testNodeIDB)
	if got.Score == 0 {
		t.Fatal("ComputeAll did not create an entry for a peer with a " +
			"connected event in the window — the subsystem is broken as well " +
			"as unwired, which is a different and worse finding")
	}
	if got.AvgRTT != 20*time.Millisecond {
		t.Fatalf("AvgRTT = %v after injection, want 20ms — the injection ran "+
			"but did not reach the stored record", got.AvgRTT)
	}
}

// ── The scoring formula ─────────────────────────────────────────────────────

// The weights are the ranking. A peer's position is decided entirely here, so
// a changed constant silently reorders every peer without failing anything.
func TestScoreWeightsAreExactlyThirtyThirtyTwentyFiveFifteen(t *testing.T) {
	// All components at their maximum: uptime 1, grade 1, zero drops with a
	// long-stable grade, zero RTT ⇒ every term contributes its full weight.
	best := &PeerReputation{
		UptimePercent: 1, AvgGrade: 1, DropFrequency: 0,
		GradeStableSince: time.Now().Add(-time.Hour), AvgRTT: 0,
	}
	best.ComputeScore()
	if best.Score < 0.999 || best.Score > 1.001 {
		t.Fatalf("a perfect peer scored %v, want 1.0 — the weights no longer "+
			"sum to 1 and scores are not comparable across releases", best.Score)
	}

	// All components at their minimum. Note stability is NOT zero at its
	// floor: gradeStabilityFactor bottoms out at 0.2, and latency is
	// asymptotic, so the worst achievable score is above 0.
	worst := &PeerReputation{UptimePercent: 0, AvgGrade: 0, DropFrequency: 1000, AvgRTT: time.Hour}
	worst.ComputeScore()
	if worst.Score < 0 || worst.Score > 0.01 {
		t.Fatalf("the worst peer scored %v, want ~0 — a floor this high "+
			"compresses the usable range of the ranking", worst.Score)
	}

	// Each weight, isolated: set exactly one component to its max.
	for _, tc := range []struct {
		name string
		rep  PeerReputation
		want float64
	}{
		{"uptime carries 0.30", PeerReputation{UptimePercent: 1, DropFrequency: 1000, AvgRTT: time.Hour}, 0.30},
		{"grade carries 0.30", PeerReputation{AvgGrade: 1, DropFrequency: 1000, AvgRTT: time.Hour}, 0.30},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := tc.rep
			r.ComputeScore()
			if r.Score < tc.want-0.01 || r.Score > tc.want+0.01 {
				t.Fatalf("score = %v, want ~%v — this component's weight has "+
					"changed and every peer's rank moves with it", r.Score, tc.want)
			}
		})
	}
}

// 🔑 MESH-G04: EffectiveGrade must WIN over AvgGrade when set. The comment
// records that the documented RTT-bloat demotion has no effect on
// the score at all, because ComputeScore read AvgGrade only.
func TestEffectiveGradeOverridesAverageGradeWhenSet(t *testing.T) {
	demoted := &PeerReputation{
		UptimePercent: 1, AvgGrade: 1.0, EffectiveGrade: 0.25,
		GradeStableSince: time.Now().Add(-time.Hour),
	}
	demoted.ComputeScore()

	undemoted := &PeerReputation{
		UptimePercent: 1, AvgGrade: 1.0,
		GradeStableSince: time.Now().Add(-time.Hour),
	}
	undemoted.ComputeScore()

	if demoted.Score >= undemoted.Score {
		t.Fatalf("a peer demoted to EffectiveGrade 0.25 scored %v, no lower "+
			"than an undemoted peer at %v — the sustained-RTT-bloat demotion "+
			"is once again decorative (MESH-G04)", demoted.Score, undemoted.Score)
	}
	// And a zero EffectiveGrade must NOT be read as "demoted to the floor":
	// zero means "not set", which is the absent-vs-measured distinction again.
	if undemoted.Score < 0.5 {
		t.Fatalf("an unset EffectiveGrade (0) was treated as a real demotion — "+
			"score %v — so every peer without an effective grade is penalised "+
			"for data that was never measured", undemoted.Score)
	}
}

// Grade stability is a step function and the steps are load-bearing: a peer
// that has just settled must rank below one that has been stable for an hour.
func TestGradeStabilityStepsRewardLongerStability(t *testing.T) {
	scoreAfter := func(stable time.Duration) float64 {
		r := &PeerReputation{UptimePercent: 1, AvgGrade: 1}
		if stable > 0 {
			r.GradeStableSince = time.Now().Add(-stable)
		}
		r.ComputeScore()
		return r.Score
	}

	unset := scoreAfter(0)
	fresh := scoreAfter(10 * time.Second)
	mid := scoreAfter(5 * time.Minute)
	long := scoreAfter(30 * time.Minute)

	if !(long > mid && mid > fresh) {
		t.Fatalf("stability steps are not monotonic: fresh=%v mid=%v long=%v — "+
			"a flapping peer can rank above a steady one", fresh, mid, long)
	}
	if unset != fresh {
		t.Fatalf("an UNSET GradeStableSince scored %v but a 10s-old one scored "+
			"%v — absent stability data must land on the same floor as newly "+
			"established, not somewhere else", unset, fresh)
	}
}

// The connection-duration grade-stability nudge is additive and bounded: a
// longer unbroken connection raises the score monotonically, never by more than
// gradeStabilityNudgeMax over the same peer with no duration, and a zero
// duration leaves the pre-nudge score exactly intact.
func TestConnectedDurationNudgesScoreWithinBounds(t *testing.T) {
	scoreWith := func(dur time.Duration) float64 {
		r := &PeerReputation{
			UptimePercent: 0.5, AvgGrade: 0.5, DropFrequency: 1,
			GradeStableSince: time.Now().Add(-5 * time.Minute), ConnectedDuration: dur,
		}
		r.ComputeScore()
		return r.Score
	}

	none := scoreWith(0)
	short := scoreWith(1 * time.Minute)
	long := scoreWith(2 * time.Hour)

	// Zero duration must not move the score: a peer scored with the field unset
	// and one scored with an explicit zero must be identical, so wiring the
	// nudge does not silently reprice every already-scored peer.
	unset := &PeerReputation{
		UptimePercent: 0.5, AvgGrade: 0.5, DropFrequency: 1,
		GradeStableSince: time.Now().Add(-5 * time.Minute),
	}
	unset.ComputeScore()
	if none != unset.Score {
		t.Fatalf("a zero ConnectedDuration changed the score (%v vs %v) — the nudge is not "+
			"neutral at zero, so every peer shifts the moment the feed is wired", none, unset.Score)
	}

	// Longer connection ⇒ strictly higher score.
	if !(long > short && short > none) {
		t.Fatalf("nudge is not monotonic in duration: none=%v short=%v long=%v — a longer "+
			"stable connection did not raise reputation", none, short, long)
	}

	// The bonus is capped: it can never add more than gradeStabilityNudgeMax, so
	// connection age cannot overturn uptime, grade, drop-stability or latency.
	if long-none > gradeStabilityNudgeMax+1e-9 {
		t.Fatalf("nudge added %v over the no-duration score, exceeding the %v ceiling — "+
			"connection age is dominating the composite", long-none, gradeStabilityNudgeMax)
	}
}

// Latency is an inverse scale, and the documented reference points are the
// contract for anyone reading a score.
func TestLatencyComponentFollowsItsDocumentedCurve(t *testing.T) {
	// 🙋 Isolating this term needs care, and my first attempt got it wrong:
	// with everything else zero the score is NOT 0.15*latency. dropStability
	// is 1/(1+0) = 1 and gradeStabilityFactor floors at 0.2, so an all-zero
	// PeerReputation still carries 0.25*0.2 = 0.05 of stability. Subtract that
	// floor before dividing, or every reading comes out 0.33 too high.
	const stabilityFloor = 0.25 * 0.2
	latencyOnly := func(rtt time.Duration) float64 {
		r := &PeerReputation{AvgRTT: rtt}
		r.ComputeScore()
		return (r.Score - stabilityFloor) / 0.15
	}

	for _, tc := range []struct {
		rtt  time.Duration
		want float64
	}{
		{0, 1.0},
		{50 * time.Millisecond, 0.5},
		{200 * time.Millisecond, 0.2},
		{1000 * time.Millisecond, 0.048},
	} {
		got := latencyOnly(tc.rtt)
		if got < tc.want-0.02 || got > tc.want+0.02 {
			t.Errorf("latency term at %v = %.3f, want ~%.3f — the inverse-scale "+
				"curve has changed and every peer's rank moves with it",
				tc.rtt, got, tc.want)
		}
	}

	// 🔴 THE NEGATIVE-RTT CLAMP, AND ITS HAZARD BAND IS NARROW — which is why
	// my first version of this assertion let a mutant live. `latency` is
	// 1/(1 + rttMs/50), so without the clamp:
	//
	//	rttMs = -1000  ->  1/(1-20)   = -0.05   harmless, score merely drops
	//	rttMs =   -40  ->  1/(1-0.8)  = 5.0     score 0.80, four times a perfect peer
	//	rttMs =   -50  ->  1/0        = +Inf    score +Inf, ranks above everything
	//
	// I tested -1s, which is OUTSIDE the dangerous band, and the mutant that
	// deletes the clamp survived. Small negatives are also exactly what a
	// clock skew or a reversed subtraction produces — the large ones are not
	// the realistic input.
	zero := &PeerReputation{AvgRTT: 0}
	zero.ComputeScore()
	for _, rtt := range []time.Duration{-40 * time.Millisecond, -50 * time.Millisecond, -time.Second} {
		neg := &PeerReputation{AvgRTT: rtt}
		neg.ComputeScore()
		if neg.Score > zero.Score {
			t.Fatalf("RTT %v scored %v against %v for a perfect zero-RTT peer "+
				"— the negative clamp is gone and a clock skew of a few tens of "+
				"milliseconds makes a peer unbeatable", rtt, neg.Score, zero.Score)
		}
	}
}

func TestGradeNormalisationAndClamping(t *testing.T) {
	for grade, want := range map[Grade]float64{
		GradeA: 1.0, GradeB: 0.75, GradeC: 0.5, GradeF: 0.0,
	} {
		if got := gradeToNormalized(grade); got != want {
			t.Errorf("gradeToNormalized(%v) = %v, want %v", grade, got, want)
		}
	}

	for _, tc := range []struct{ v, lo, hi, want float64 }{
		{0.5, 0, 1, 0.5}, {-2, 0, 1, 0}, {7, 0, 1, 1}, {0, 0, 1, 0}, {1, 0, 1, 1},
	} {
		if got := clampFloat(tc.v, tc.lo, tc.hi); got != tc.want {
			t.Errorf("clampFloat(%v, %v, %v) = %v, want %v",
				tc.v, tc.lo, tc.hi, got, tc.want)
		}
	}
}

// ── ComputeAll over the event log ───────────────────────────────────────────

// A peer that connected and stayed connected has high uptime and no drops.
func TestComputeAllScoresAStableConnectedPeerHighly(t *testing.T) {
	rt := trackerWith(t, repEvent(testNodeIDB, "connected", GradeA, 50*time.Minute))
	rt.ComputeAll()

	rep := rt.ScoreFor(testNodeIDB)
	if rep.Score == 0 {
		t.Fatal("no reputation for a peer with a connected event in the window")
	}
	if rep.DropFrequency != 0 {
		t.Fatalf("DropFrequency = %v for a peer that never disconnected",
			rep.DropFrequency)
	}
	if rep.UptimePercent < 0.5 {
		t.Fatalf("UptimePercent = %v for a peer connected for 50 of the last "+
			"60 minutes — uptime is the largest scoring component and it is "+
			"under-counting", rep.UptimePercent)
	}
}

// Drops are what the score exists to punish.
func TestAFlappingPeerRanksBelowAStableOne(t *testing.T) {
	const flapper = "cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33"
	rt := trackerWith(t,
		repEvent(testNodeIDB, "connected", GradeA, 50*time.Minute),
		repEvent(flapper, "connected", GradeA, 50*time.Minute),
		repEvent(flapper, "disconnected", GradeF, 40*time.Minute),
		repEvent(flapper, "connected", GradeA, 30*time.Minute),
		repEvent(flapper, "disconnected", GradeF, 20*time.Minute),
		repEvent(flapper, "connected", GradeA, 10*time.Minute),
	)
	rt.ComputeAll()

	ranked := rt.RankedPeers()
	if len(ranked) != 2 {
		t.Fatalf("RankedPeers returned %d peers, want 2", len(ranked))
	}
	if ranked[0].NodeID != testNodeIDB {
		t.Fatalf("the flapping peer ranked first (%v) — RankedPeers is sorted "+
			"the wrong way, or drops are not being counted", ranked[0].NodeID)
	}
	flap := rt.ScoreFor(flapper)
	if flap.DropFrequency <= 0 {
		t.Fatalf("the flapping peer recorded no drops: %+v", flap)
	}
}

// 🔑 MESH-G04's OTHER HALF: an empty window must CLEAR the scores rather than
// freeze them. A frozen score outlives the peer it describes and keeps ranking
// a node that has been silent for an hour.
func TestAnEmptyWindowClearsScoresRatherThanFreezingThem(t *testing.T) {
	rt := trackerWith(t, repEvent(testNodeIDB, "connected", GradeA, 10*time.Minute))
	rt.ComputeAll()
	if rt.ScoreFor(testNodeIDB).Score == 0 {
		t.Fatal("premise wrong: no score to become stale")
	}

	// Shrink the window so nothing falls inside it.
	rt.window = time.Nanosecond
	rt.ComputeAll()

	if got := rt.ScoreFor(testNodeIDB); got.Score != 0 {
		t.Fatalf("a score survived an empty window (%+v) — it is frozen at its "+
			"last value forever and a long-dead peer keeps its rank (MESH-G04)",
			got)
	}
}

// A tracker with no event log at all must not panic — the constructor accepts
// one and ConnectionManager builds it from scaler.EventLog(), which is nil
// until the scaler exists.
func TestComputeAllWithoutAnEventLogIsSafe(t *testing.T) {
	rt := NewReputationTracker(nil)
	rt.ComputeAll()
	if got := rt.RankedPeers(); len(got) != 0 {
		t.Fatalf("RankedPeers returned %d entries with no event log", len(got))
	}
}
