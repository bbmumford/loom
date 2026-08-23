/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"sync"
	"testing"
	"time"
)

// COVERAGE of graceful connection draining, 11 functions at 0.0%.
//
// CENSUSED FIRST. This one is WIRED and it CLOSES LIVE CONNECTIONS:
// `NewDrainManager` is called from the ConnectionManager constructor
// (peer_connections.go:1019), so every node has one, and
// connection_scaling.go:475-488 drives `SelectForDrain` → `StartDrain` on
// every scale-down. A wrong answer here drops the wrong peer.
//
// 🛑 THREE OF THE ELEVEN ARE DEAD AND ARE NOT TESTED AS PRODUCTION BEHAVIOUR.
// Measured across all three roots, non-test callers:
//
//	DecrementInFlight  0   (the file's own MESH-C04 note says so)
//	IsDraining         0   ← the guard that would stop a re-dial mid-drain
//	DrainCount         0
//
// IsDraining and DrainCount are used BELOW only as observation helpers — the
// natural way to see what StartDrain did. Their coverage therefore comes from
// this file, not from any caller, so they are dead regardless.
// DecrementInFlight is left alone entirely: it mutates a counter nothing
// increments and nothing reads.

func drainManagerForTest(t *testing.T, grace time.Duration) (*DrainManager, chan [2]string) {
	t.Helper()
	closed := make(chan [2]string, 4)
	dm := NewDrainManager(func(peerNodeID, transport string, _ time.Time) {
		closed <- [2]string{peerNodeID, transport}
	})
	// monitorDrain uses min(dm.timeout, drainGracePeriod); shrinking the
	// timeout is the only lever a test has over the 5s constant.
	dm.timeout = grace
	return dm, closed
}

// ── Lifecycle ───────────────────────────────────────────────────────────────

// 🔴 THE CALLBACK IS THE WHOLE POINT: it is what actually closes the
// connection (peer_connections.go:4182). It must fire, once, with the pair it
// was given — closing the wrong transport on the right peer is as bad as
// closing the wrong peer.
func TestDrainCompletesAndClosesExactlyTheDrainedPair(t *testing.T) {
	dm, closed := drainManagerForTest(t, 20*time.Millisecond)

	dm.StartDrain(testNodeIDB, "websocket", "scale_down")
	if !dm.IsDraining(testNodeIDB, "websocket") {
		t.Fatal("StartDrain did not register the drain — the grace period is " +
			"not running and the connection will never be closed")
	}

	select {
	case got := <-closed:
		if got[0] != testNodeIDB || got[1] != "websocket" {
			t.Fatalf("close callback fired for %v, want (%s, websocket) — a "+
				"different connection was closed than the one drained",
				got, testNodeIDB)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the close callback never fired — a drained connection stays " +
			"open forever, holding a budget slot that scale-down is trying to " +
			"reclaim")
	}

	if dm.IsDraining(testNodeIDB, "websocket") {
		t.Fatal("the entry survived completion — a second drain of the same " +
			"pair would be swallowed as 'already draining' and never close")
	}
	if got := dm.DrainCount(); got != 0 {
		t.Fatalf("DrainCount = %d after completion, want 0", got)
	}
}

// A second StartDrain for the same pair must not start a second timer.
// Without this, N scale-down passes over the same connection queue N
// goroutines that each fire the close callback.
func TestRepeatedStartDrainIsIdempotentAndClosesOnce(t *testing.T) {
	dm, closed := drainManagerForTest(t, 20*time.Millisecond)

	for i := 0; i < 4; i++ {
		dm.StartDrain(testNodeIDB, "websocket", "scale_down")
	}
	if got := dm.DrainCount(); got != 1 {
		t.Fatalf("DrainCount = %d after 4 StartDrain calls for one pair, want 1",
			got)
	}

	<-closed // first completion
	time.Sleep(80 * time.Millisecond)
	select {
	case extra := <-closed:
		t.Fatalf("the close callback fired a SECOND time for %v — duplicate "+
			"drain goroutines are closing a connection that may already have "+
			"been re-established", extra)
	default:
	}
}

// Different transports to the same peer are independent drains: draining the
// WebSocket must not close the QUIC path.
func TestDrainsAreScopedToPeerAndTransportTogether(t *testing.T) {
	dm, _ := drainManagerForTest(t, time.Hour) // long grace: nothing completes

	dm.StartDrain(testNodeIDB, "websocket", "scale_down")

	if dm.IsDraining(testNodeIDB, "quic") {
		t.Fatal("draining the websocket path marked the QUIC path as draining " +
			"— a scale-down would close both transports to the peer")
	}
	if dm.IsDraining(testNodeIDA, "websocket") {
		t.Fatal("draining one peer marked another peer as draining")
	}

	dm.StartDrain(testNodeIDB, "quic", "grade_upgrade")
	if got := dm.DrainCount(); got != 2 {
		t.Fatalf("DrainCount = %d for two distinct transports of one peer, "+
			"want 2 — the key is collapsing them", got)
	}
}

// completeDrain on a key that is not draining must be a silent no-op. It is
// reachable: monitorDrain's timer fires unconditionally, so a drain that was
// already completed concurrently arrives here a second time.
func TestCompleteDrainOnAnUnknownKeyDoesNotInvokeTheCloseCallback(t *testing.T) {
	dm, closed := drainManagerForTest(t, time.Hour)

	dm.completeDrain(drainKey(testNodeIDB, "websocket"))

	select {
	case got := <-closed:
		t.Fatalf("the close callback fired for a pair that was never draining "+
			"(%v) — a stale timer would close a live connection", got)
	default:
	}
}

// A nil callback is legal and must not panic — the completion runs in a
// goroutine, so a panic there is unrecovered and takes the process down.
func TestDrainWithNoCloseCallbackDoesNotPanic(t *testing.T) {
	dm := NewDrainManager(nil)
	dm.timeout = 20 * time.Millisecond

	dm.StartDrain(testNodeIDB, "websocket", "budget_exceeded")

	deadline := time.Now().Add(2 * time.Second)
	for dm.DrainCount() > 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if dm.DrainCount() != 0 {
		t.Fatal("the drain never completed with a nil callback — either it " +
			"panicked in its goroutine or the entry leaked")
	}
}

// Concurrent drains of distinct pairs must all complete. StartDrain holds the
// mutex while spawning, and completeDrain takes it again from N goroutines.
func TestConcurrentDrainsAllComplete(t *testing.T) {
	dm, closed := drainManagerForTest(t, 20*time.Millisecond)

	const n = 8
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			dm.StartDrain(testNodeIDB, string(rune('a'+i)), "scale_down")
		}(i)
	}
	wg.Wait()

	seen := 0
	timeout := time.After(3 * time.Second)
	for seen < n {
		select {
		case <-closed:
			seen++
		case <-timeout:
			t.Fatalf("only %d of %d concurrent drains closed — connections are "+
				"stuck draining and their budget slots are never released",
				seen, n)
		}
	}
	if got := dm.DrainCount(); got != 0 {
		t.Fatalf("DrainCount = %d after all completions, want 0 — entries are "+
			"leaking, and a leaked entry blocks any future drain of that pair",
			got)
	}
}

// ── Selection ───────────────────────────────────────────────────────────────

func conn(priority ConnectionPriority, grade Grade, rtt time.Duration, age time.Duration) ConnectionInfo {
	return ConnectionInfo{
		PeerNodeID: testNodeIDB, Transport: "websocket", Priority: priority,
		Grade: grade, RTT: rtt, ConnectedAt: time.Now().Add(-age),
	}
}

func TestNothingIsDrainedWhenAlreadyAtOrBelowTarget(t *testing.T) {
	conns := []ConnectionInfo{conn(PriorityNormal, GradeA, time.Millisecond, time.Hour)}
	if got := SelectForDrain(conns, 1); got != nil {
		t.Fatalf("SelectForDrain returned %d connections when already at "+
			"target — a scale-down would close a connection it does not need "+
			"to", len(got))
	}
	if got := SelectForDrain(conns, 5); got != nil {
		t.Fatalf("SelectForDrain returned %d connections when BELOW target",
			len(got))
	}
}

// 🔴 CRITICAL CONNECTIONS ARE NEVER DRAINED. This is the one rule in the file
// whose violation is unrecoverable at the mesh layer: a critical path closed
// by a routine scale-down does not come back until the next dial cycle.
func TestCriticalConnectionsAreNeverSelectedEvenUnderPressure(t *testing.T) {
	conns := []ConnectionInfo{
		conn(PriorityCritical, GradeF, time.Second, time.Minute), // worst on every other axis
		conn(PriorityNormal, GradeA, time.Millisecond, time.Hour),
	}

	for _, target := range []int{0, 1} {
		for _, c := range SelectForDrain(conns, target) {
			if c.Priority >= PriorityCritical {
				t.Fatalf("a CRITICAL connection was selected for drain at "+
					"target %d — it is the worst connection by grade, latency "+
					"and age, and none of that may override criticality", target)
			}
		}
	}
}

// Criticals still consume slots against the target, so the drain count is
// computed from the non-critical pool. Getting this wrong drains too many.
func TestCriticalsCountTowardTheTargetWithoutBeingDrained(t *testing.T) {
	conns := []ConnectionInfo{
		conn(PriorityCritical, GradeA, time.Millisecond, time.Hour),
		conn(PriorityCritical, GradeA, time.Millisecond, time.Hour),
		conn(PriorityNormal, GradeA, time.Millisecond, time.Hour),
		conn(PriorityNormal, GradeA, time.Millisecond, time.Hour),
		conn(PriorityNormal, GradeA, time.Millisecond, time.Hour),
	}
	// 5 connections, target 3, of which 2 are critical ⇒ 1 drainable slot
	// remains, so exactly 2 of the 3 normals must drain.
	got := SelectForDrain(conns, 3)
	if len(got) != 2 {
		t.Fatalf("SelectForDrain selected %d, want 2 — with 2 criticals held "+
			"back, reaching a target of 3 means draining 2 of the 3 normal "+
			"connections", len(got))
	}
}

// When the criticals alone already exceed the target, every non-critical goes
// and nothing else can be done — the target is simply unreachable.
func TestWhenCriticalsExceedTheTargetAllNonCriticalsDrain(t *testing.T) {
	conns := []ConnectionInfo{
		conn(PriorityCritical, GradeA, time.Millisecond, time.Hour),
		conn(PriorityCritical, GradeA, time.Millisecond, time.Hour),
		conn(PriorityCritical, GradeA, time.Millisecond, time.Hour),
		conn(PriorityNormal, GradeA, time.Millisecond, time.Hour),
	}
	got := SelectForDrain(conns, 1)
	if len(got) != 1 || got[0].Priority >= PriorityCritical {
		t.Fatalf("selected %d connections (first priority %v) — with 3 "+
			"criticals and a target of 1, the single normal connection is the "+
			"only thing that may drain", len(got), got[0].Priority)
	}
}

// ── Drain ordering ──────────────────────────────────────────────────────────

// The ordering rules, each isolated so a failure names the rule that broke.
func TestDrainOrderPrefersLowerPriorityThenWorseGradeThenSlowerThenNewer(t *testing.T) {
	for _, tc := range []struct {
		name        string
		first, rest ConnectionInfo
		why         string
	}{
		{
			"lower priority drains first",
			conn(PriorityLow, GradeA, time.Millisecond, time.Hour),
			conn(PriorityHigh, GradeF, time.Second, time.Second),
			"priority is the primary key and must outrank grade, latency and age",
		},
		{
			"worse grade drains first at equal priority",
			conn(PriorityNormal, GradeF, time.Millisecond, time.Hour),
			conn(PriorityNormal, GradeA, time.Second, time.Second),
			"a worse transport is the cheapest connection to lose",
		},
		{
			"higher latency drains first at equal grade",
			conn(PriorityNormal, GradeB, time.Second, time.Hour),
			conn(PriorityNormal, GradeB, time.Millisecond, time.Second),
			"the slower path is the less useful one",
		},
		{
			"newer connection drains first at equal everything",
			conn(PriorityNormal, GradeB, time.Millisecond, time.Second),
			conn(PriorityNormal, GradeB, time.Millisecond, time.Hour),
			"long-lived connections are the stable ones and are preserved",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// Feed them in the WRONG order so a no-op sort fails the test.
			got := SelectForDrain([]ConnectionInfo{tc.rest, tc.first}, 1)
			if len(got) != 1 {
				t.Fatalf("selected %d, want 1", len(got))
			}
			if !got[0].ConnectedAt.Equal(tc.first.ConnectedAt) ||
				got[0].Priority != tc.first.Priority ||
				got[0].Grade != tc.first.Grade || got[0].RTT != tc.first.RTT {
				t.Fatalf("the wrong connection was chosen to drain: %s", tc.why)
			}
		})
	}
}

// ── Small surfaces ──────────────────────────────────────────────────────────

// The state names appear in logs and in the connection event log's
// DrainStarted/DrainComplete vocabulary, so an unknown value must be visibly
// unknown rather than silently rendering as "active".
func TestDrainStateNamesIncludeAnExplicitUnknown(t *testing.T) {
	for state, want := range map[DrainState]string{
		DrainActive:   "active",
		DrainStarted:  "draining",
		DrainComplete: "complete",
		DrainState(9): "unknown",
	} {
		if got := state.String(); got != want {
			t.Errorf("DrainState(%d).String() = %q, want %q", state, got, want)
		}
	}
}

// The key must separate the two components. Note the separator is not escaped:
// a peer ID containing "::" could in principle collide with another pair. Node
// IDs are hex and transports are a fixed label set, so it is unreachable
// today — recorded here rather than asserted as a defect.
func TestDrainKeyDistinguishesItsTwoComponents(t *testing.T) {
	if drainKey("a", "b") == drainKey("b", "a") {
		t.Fatal("drainKey is symmetric — peer/transport pairs collide and one " +
			"drain would cancel another")
	}
	if drainKey(testNodeIDB, "websocket") != drainKey(testNodeIDB, "websocket") {
		t.Fatal("drainKey is not deterministic")
	}
}
