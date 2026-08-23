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
)

// COVERAGE of the three RTT-derived timeout functions, all at 0.0%.
//
// CENSUSED FIRST and every one is live on a hot path:
//
//	adaptiveRPCTimeout        rpc_forward.go:360        the forwarded-RPC deadline
//	adaptiveKeepaliveInterval mesh_connection.go:604    keepalive ping cadence
//	adaptiveKeepaliveTimeout  mesh_connection.go:665    per-ping death timer
//
// 🔴 EVERY ONE OF THEM DECIDES WHEN TO GIVE UP ON A PEER, and each carries a
// long comment recording a production incident it was written to stop —
// noise-UDP session-death clusters, fly CPU-steal blackholes, cross-region
// jitter spikes chaining into the death policy. A regression here does not
// fail a test somewhere else; it kills healthy sessions in the field.
//
// 🔑 AND ALL THREE SHARE ONE SHAPE THAT IS THE POINT OF THIS FILE: a nil
// session, a nil Health(), and an unconverged estimator are ALL treated as
// "no measurement yet" and get the SAFE (longer) bound — never the tight one.
// That is the absent-vs-measured distinction decided
// correctly, three times, by whoever wrote these.

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
// adaptiveKeepaliveTimeout's warmup gate reads.
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
// SetHealthMonitor, called out by name in adaptiveKeepaliveTimeout's comment.
type nilHealthSession struct{ probeSession }

func (s *nilHealthSession) Health() *health.Monitor { return nil }

// ── The shared safety property ──────────────────────────────────────────────

// 🔴 THE ONE THAT MATTERS MOST: absence of a measurement must never produce
// the aggressive bound. All three functions, all three absence shapes.
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

	t.Run("adaptiveKeepaliveInterval", func(t *testing.T) {
		for name, s := range map[string]aether.Session{
			"nil session":  nil,
			"nil Health()": &nilHealthSession{},
			"zero SRTT":    unconverged,
		} {
			if got := adaptiveKeepaliveInterval(s); got != 5*time.Second {
				t.Errorf("%s → %v, want the 5s floor", name, got)
			}
		}
	})

	// 🔑 The keepalive DEATH timer is the sharpest case. Its comment records
	// that treating the nil-Health window as steady-state used a 1s timeout
	// and "killed every fresh noise-UDP session that hit any transient
	// jitter". A nil session, a nil Health(), and an under-converged
	// estimator must all get the 5s WARMUP floor, not the 1s steady floor.
	t.Run("adaptiveKeepaliveTimeout warmup floor", func(t *testing.T) {
		for name, s := range map[string]aether.Session{
			"nil session":     nil,
			"nil Health()":    &nilHealthSession{},
			"under 8 samples": sessionWithSamples(t, 10*time.Millisecond, 7),
		} {
			if got := adaptiveKeepaliveTimeout(s, 0); got != 5*time.Second {
				t.Errorf("%s → %v, want the 5s warmup floor — a fresh session "+
					"that hits one jitter spike now burns a keepalive strike, "+
					"which is the exact noise-UDP death cluster the warmup "+
					"floor was added to stop", name, got)
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

// ── adaptiveKeepaliveInterval: clamp(2 × SRTT, 5s, 30s) ─────────────────────

func TestKeepaliveIntervalIsBoundedAtBothEnds(t *testing.T) {
	fast := sessionWithSamples(t, 5*time.Millisecond, 8)
	if got := adaptiveKeepaliveInterval(fast); got != 5*time.Second {
		t.Fatalf("a 5ms path pings every %v, want the 5s floor — the floor "+
			"exists because fly's edge proxy closes idle connections and a "+
			"faster cadence buys nothing", got)
	}

	slow := sessionWithSamples(t, 20*time.Second, 8)
	if got := adaptiveKeepaliveInterval(slow); got != 30*time.Second {
		t.Fatalf("a 20s-RTT path pings every %v, want the 30s cap", got)
	}

	// Inside the band the cadence really does track SRTT — without this the
	// two clamps above would pass against a function that always returns a
	// constant.
	mid := sessionWithSamples(t, 6*time.Second, 8)
	got := adaptiveKeepaliveInterval(mid)
	if got <= 5*time.Second || got >= 30*time.Second {
		t.Fatalf("a mid-band path returned %v, which is one of the clamps — "+
			"the interval is not tracking SRTT at all (SRTT %v)",
			got, mid.health.SRTT())
	}
	if got != 2*mid.health.SRTT() {
		t.Fatalf("interval = %v, want 2 × SRTT (%v)", got, 2*mid.health.SRTT())
	}
}

// ── adaptiveKeepaliveTimeout: clamp(4 × RTO, floor, 15s) ────────────────────

// Once the estimator is converged the floor drops to 1s — but only then.
// The gate is SampleCount >= 8, and the comment records why 3 was too tight:
// a death cluster at ~34s, which is samples 4-7 at a 5s cadence.
func TestKeepaliveTimeoutFloorDropsOnlyAfterEightSamples(t *testing.T) {
	seven := sessionWithSamples(t, 10*time.Millisecond, 7)
	eight := sessionWithSamples(t, 10*time.Millisecond, 8)

	if got := adaptiveKeepaliveTimeout(seven, 0); got != 5*time.Second {
		t.Fatalf("7 samples → %v, want the 5s warmup floor — the gate has "+
			"moved back down and samples 4-7 are exposed again", got)
	}
	if got := adaptiveKeepaliveTimeout(eight, 0); got != time.Second {
		t.Fatalf("8 samples → %v, want the 1s steady floor — the estimator is "+
			"converged and the timeout should now reflect the measured path",
			got)
	}
}

func TestKeepaliveTimeoutIsFourRTOClampedAtFifteenSeconds(t *testing.T) {
	converged := sessionWithSamples(t, 10*time.Millisecond, 8)

	// 4 × 500ms = 2s, inside the band.
	if got := adaptiveKeepaliveTimeout(converged, 500*time.Millisecond); got != 2*time.Second {
		t.Fatalf("4 × 500ms RTO = %v, want 2s", got)
	}
	// 4 × 10s = 40s, capped.
	if got := adaptiveKeepaliveTimeout(converged, 10*time.Second); got != 15*time.Second {
		t.Fatalf("a huge RTO produced %v, want the 15s cap — the cap is what "+
			"stops a documented 400ms-1.2s fly CPU-steal window from consuming "+
			"a keepalive strike", got)
	}
	// A negative or zero RTO is not a measurement; it must land on the floor,
	// not produce a tiny or negative timeout.
	for _, rto := range []time.Duration{0, -time.Second} {
		if got := adaptiveKeepaliveTimeout(converged, rto); got != time.Second {
			t.Fatalf("RTO %v → %v, want the 1s floor — a non-measurement "+
				"produced a timeout derived from it", rto, got)
		}
	}
}

// The three functions must not collapse into each other: they have different
// multipliers (20×, 2×, 4×) and different caps (30s, 30s, 15s), and a copy
// -paste between them would be invisible to any single-function test.
func TestTheThreeTimeoutsRemainDistinct(t *testing.T) {
	s := sessionWithSamples(t, 400*time.Millisecond, 8)
	rpc := adaptiveRPCTimeout(s)
	interval := adaptiveKeepaliveInterval(s)

	if rpc == interval {
		t.Fatalf("adaptiveRPCTimeout and adaptiveKeepaliveInterval both "+
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
