/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"testing"
	"time"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/health"
	"github.com/ORBTR/aether/quality"
)

// Tests of the RTT-derived bounds the node runtime gives up on a peer by:
//
//	adaptiveRPCTimeout       the forwarded-RPC deadline (rpc_forward.go)
//	aether.KeepalivePlanFor  the keepalive's probe Interval and ProbeTimeout, which
//	                         AcceptMeshConnection's keepalive goroutine reads before
//	                         every wait and every probe
//
// A wrong bound here ends healthy sessions rather than failing a test elsewhere.
// Both share one shape: a nil Health() and an unconverged estimator are "no
// measurement yet" and get the longer bound, never the tight one (and
// adaptiveRPCTimeout takes a nil session the same way, for its callers without a
// session in hand).

// srttSession is an aether.Session whose Health() monitor can be driven to a
// chosen SRTT and sample count by feeding it real pong samples — the same
// path the estimator takes in production, rather than poking its internals.
type srttSession struct {
	probeSession
	health *health.Monitor
}

func (s *srttSession) Health() *health.Monitor { return s.health }

// sessionWithSamples returns a session whose RFC-6298 estimator has consumed
// n samples of the given RTT. n also drives SampleCount, which is what
// the keepalive plan's warmup gate reads.
func sessionWithSamples(t *testing.T, rtt time.Duration, n int) *srttSession {
	t.Helper()
	m := health.NewMonitor(0.125)
	for i := 0; i < n; i++ {
		seq := uint32(i + 1)
		m.RecordPingSent(seq)
		m.RecordPongRecv(seq, time.Now().Add(-rtt))
	}
	if n > 0 && m.SRTT() <= 0 {
		t.Fatalf("premise wrong: %d samples of %v left SRTT at %v, so every "+
			"assertion below would be testing the unconverged path instead",
			n, rtt, m.SRTT())
	}
	return &srttSession{health: m}
}

// nilHealthSession is a session that exists but whose Health() has not been
// installed — the real race window between session construction and
// SetHealthMonitor, which aether.KeepalivePlanFor reads as nothing measured.
type nilHealthSession struct{ probeSession }

func (s *nilHealthSession) Health() *health.Monitor { return nil }

// ── The shared safety property ──────────────────────────────────────────────

// Absence of a measurement never produces the tight bound, for every bound and
// every absence shape the bound's callers can hand it.
func TestUnmeasuredPathsGetTheSafeBoundNotTheTightOne(t *testing.T) {
	unconverged := sessionWithSamples(t, 0, 0) // monitor exists, no samples

	t.Run("adaptiveRPCTimeout", func(t *testing.T) {
		for name, s := range map[string]aether.Session{
			"nil session":  nil,
			"nil Health()": &nilHealthSession{},
			"zero SRTT":    unconverged,
		} {
			if got := adaptiveRPCTimeout(s); got != 5*time.Second {
				t.Errorf("%s → %v, want the 5s floor — an unmeasured path got a "+
					"deadline derived from an SRTT nobody measured", name, got)
			}
		}
	})

	// The keepalive goroutine always holds a session, so the plan is asked only
	// for a session whose Health() may still be nil or unconverged.
	t.Run("keepalive Interval", func(t *testing.T) {
		for name, s := range map[string]aether.Session{
			"nil Health()": &nilHealthSession{},
			"zero SRTT":    unconverged,
		} {
			if got := aether.KeepalivePlanFor(s).Interval; got != 5*time.Second {
				t.Errorf("%s → %v, want the 5s floor", name, got)
			}
		}
	})

	// The probe timeout is the sharpest case: a 1s wait on a session whose
	// estimator has not converged spends a strike on one slow answer, while the
	// session is new and may be the peer's only path. A nil Health() and an
	// estimator fed fewer than 8 answers get the 5s warmup floor.
	t.Run("keepalive ProbeTimeout warmup floor", func(t *testing.T) {
		for name, s := range map[string]aether.Session{
			"nil Health()":    &nilHealthSession{},
			"under 8 samples": sessionWithSamples(t, 10*time.Millisecond, 7),
		} {
			if got := aether.KeepalivePlanFor(s).ProbeTimeout; got != 5*time.Second {
				t.Errorf("%s → %v, want the 5s warmup floor — a new session now "+
					"spends a keepalive strike on one slow answer", name, got)
			}
		}
	})
}

// ── adaptiveRPCTimeout: clamp(20 × SRTT, 5s, 30s) ───────────────────────────

func TestRPCTimeoutScalesWithSRTTBetweenItsBounds(t *testing.T) {
	for _, tc := range []struct {
		name string
		srtt time.Duration
		want time.Duration
	}{
		// 20 × 5ms = 100ms, below the floor.
		{"a LAN path gets the floor", 5 * time.Millisecond, 5 * time.Second},
		// 20 × 500ms = 10s, inside the band — the whole point of the function.
		{"a cross-region path scales", 500 * time.Millisecond, 10 * time.Second},
		// 20 × 5s = 100s, above the ceiling.
		{"a satellite path is capped", 5 * time.Second, 30 * time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := sessionWithSamples(t, tc.srtt, 8)
			got := adaptiveRPCTimeout(s)
			// The estimator smooths, so compare against the formula applied to
			// the SRTT it actually reached rather than to the input RTT.
			want := 20 * s.health.SRTT()
			switch {
			case want < 5*time.Second:
				want = 5 * time.Second
			case want > 30*time.Second:
				want = 30 * time.Second
			}
			if got != want {
				t.Fatalf("adaptiveRPCTimeout = %v, want %v (SRTT %v) — a "+
					"forwarded RPC now gives up at the wrong time for this "+
					"path length", got, want, s.health.SRTT())
			}
			// The estimator smooths, so an SRTT of "500ms" lands a few
			// hundred nanoseconds off and 20x magnifies that. Compare with a
			// tolerance rather than exactly — the clamped cases are still
			// exact, because a clamp returns a constant.
			const tol = 50 * time.Millisecond
			if got < tc.want-tol || got > tc.want+tol {
				t.Fatalf("adaptiveRPCTimeout = %v, want ~%v — the clamp band "+
					"has moved", got, tc.want)
			}
		})
	}
}

// ── keepalive Interval: clamp(2 × SRTT, 5s, 30s) ────────────────────────────

func TestKeepaliveIntervalIsBoundedAtBothEnds(t *testing.T) {
	fast := sessionWithSamples(t, 5*time.Millisecond, 8)
	if got := aether.KeepalivePlanFor(fast).Interval; got != 5*time.Second {
		t.Fatalf("a 5ms path pings every %v, want the 5s floor", got)
	}

	slow := sessionWithSamples(t, 20*time.Second, 8)
	if got := aether.KeepalivePlanFor(slow).Interval; got != 30*time.Second {
		t.Fatalf("a 20s-RTT path pings every %v, want the 30s cap", got)
	}

	// Inside the band the cadence really does track SRTT — without this the
	// two clamps above would pass against a plan that always returns a
	// constant.
	mid := sessionWithSamples(t, 6*time.Second, 8)
	got := aether.KeepalivePlanFor(mid).Interval
	if got <= 5*time.Second || got >= 30*time.Second {
		t.Fatalf("a mid-band path returned %v, which is one of the clamps — "+
			"the interval is not tracking SRTT at all (SRTT %v)",
			got, mid.health.SRTT())
	}
	if got != 2*mid.health.SRTT() {
		t.Fatalf("interval = %v, want 2 × SRTT (%v)", got, 2*mid.health.SRTT())
	}
}

// ── keepalive ProbeTimeout: clamp(4 × RTO, floor, 15s) ──────────────────────

// Once the estimator has 8 samples the floor drops to 1s, and only then: samples
// 4 to 7 still carry the estimator's initial spread, and at the 5s cadence they
// arrive 20 to 35 s into a session, when a 1s wait would end a healthy new path.
func TestKeepaliveTimeoutFloorDropsOnlyAfterEightSamples(t *testing.T) {
	seven := sessionWithSamples(t, 10*time.Millisecond, 7)
	eight := sessionWithSamples(t, 10*time.Millisecond, 8)

	if got := aether.KeepalivePlanFor(seven).ProbeTimeout; got != 5*time.Second {
		t.Fatalf("7 samples → %v, want the 5s warmup floor — the gate has "+
			"moved back down and samples 4-7 are exposed again", got)
	}
	if got := aether.KeepalivePlanFor(eight).ProbeTimeout; got != time.Second {
		t.Fatalf("8 samples → %v, want the 1s steady floor — the estimator is "+
			"converged and the timeout should now reflect the measured path",
			got)
	}
}

// The plan's RTO is the health monitor's, which a test cannot set exactly through
// pong samples, so the band, the cap and a non-measurement are stated on
// quality.Keepalive, the function aether.KeepalivePlanFor hands the monitor's
// SRTT, RTO and sample count.
func TestKeepaliveTimeoutIsFourRTOClampedAtFifteenSeconds(t *testing.T) {
	probe := func(rto time.Duration) time.Duration {
		return quality.Keepalive(quality.KeepaliveInputs{SRTT: 10 * time.Millisecond, RTO: rto, Samples: 8}).ProbeTimeout
	}
	// 4 × 500ms = 2s, inside the band.
	if got := probe(500 * time.Millisecond); got != 2*time.Second {
		t.Fatalf("4 × 500ms RTO = %v, want 2s", got)
	}
	// 4 × 10s = 40s, capped: a pause of the peer's process of a second or so
	// must not spend a strike.
	if got := probe(10 * time.Second); got != 15*time.Second {
		t.Fatalf("a huge RTO produced %v, want the 15s cap", got)
	}
	// A negative or zero RTO is not a measurement; it must land on the floor,
	// not produce a tiny or negative timeout.
	for _, rto := range []time.Duration{0, -time.Second} {
		if got := probe(rto); got != time.Second {
			t.Fatalf("RTO %v → %v, want the 1s floor — a non-measurement "+
				"produced a timeout derived from it", rto, got)
		}
	}
}

// The RPC deadline and the keepalive cadence must not collapse into each other:
// they have different multipliers (20×, 2×), and one given the other's would be
// invisible to a test of either alone.
func TestTheRPCDeadlineAndTheKeepaliveCadenceRemainDistinct(t *testing.T) {
	s := sessionWithSamples(t, 400*time.Millisecond, 8)
	rpc := adaptiveRPCTimeout(s)
	interval := aether.KeepalivePlanFor(s).Interval

	if rpc == interval {
		t.Fatalf("adaptiveRPCTimeout and the keepalive Interval both "+
			"returned %v for the same session — one has been given the "+
			"other's multiplier, and the RPC deadline and the ping cadence "+
			"are not the same quantity", rpc)
	}
	if rpc <= interval {
		t.Fatalf("the RPC deadline (%v) is not longer than the keepalive "+
			"cadence (%v) — an RPC would time out before the path had a "+
			"chance to prove itself alive", rpc, interval)
	}
}
