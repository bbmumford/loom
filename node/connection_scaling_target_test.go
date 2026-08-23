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

// Covers the connection-scaling decision surface: TargetConnections, EmitEvent
// and AddEventSubscriber. Censused per symbol on call sites rather than comment
// mentions; all are live:
//
//	TargetConnections   3 <- connection_scaling.go:398 (Rebalance), multipath_dial.go:89 :198
//	EmitEvent           3 <- mesh_connection.go:250 :1192, peer_connections.go:4312
//	AddEventSubscriber  2 <- swarm_integration.go:582, peer_connections.go:1091
//	POSITIVE CONTROL, same pattern: .recordDialSuccess( -> 17
//
// 🔑 TargetConnections IS THE K IN EnsureK — it decides how many paths the mesh
// holds open to each peer. Too low and one drop disconnects a peer; too high and
// every node multiplies its footprint across the fleet. The CLAMP is what bounds
// that, so the clamp is what these tests pin.
//
// node/connection_scaling.go is not gofmt-clean, and reformatting it here would
// mix an unrelated whitespace diff into this file's history, so it is left as
// it is.

func scalerForTest() *ConnectionScaler {
	return NewConnectionScaler(&ConnectionManager{budget: DefaultConnectionBudget()}, nil)
}

// 🔴 THE CLAMP IS THE SAFETY PROPERTY. Every factor in the formula is a
// MULTIPLIER — latency, grade, failure and traffic all scale the target UP, and
// nothing in the product bounds it. A peer that is slow AND low-grade AND
// unreliable AND busy multiplies four factors together; only the clamp stops
// that becoming an unbounded connection count against one peer.
func TestTargetConnectionsIsAlwaysWithinTheBudgetClamp(t *testing.T) {
	s := scalerForTest()
	budget := s.connMgr.budget

	for _, tc := range []struct {
		name string
		peer *peerConn
	}{
		{"quiet local peer", &peerConn{nodeID: testNodeIDA}},
		{"very high RTT", &peerConn{nodeID: testNodeIDA, lastRTT: 5 * time.Second}},
		{"absurd RTT", &peerConn{nodeID: testNodeIDA, lastRTT: time.Hour}},
		{"cross-region", &peerConn{nodeID: testNodeIDA, crossRegion: true, lastRTT: time.Second}},
	} {
		got := s.TargetConnections(tc.peer, "syd")
		maxAllowed := budget.EffectiveMaxPerPeer(tc.peer.crossRegion)

		if got < budget.MinPerPeer {
			t.Errorf("%s: target %d < MinPerPeer %d — a peer can be given fewer "+
				"paths than the floor guarantees", tc.name, got, budget.MinPerPeer)
		}
		if got > maxAllowed {
			t.Errorf("%s: target %d > EffectiveMaxPerPeer %d — the multipliers are "+
				"escaping the ceiling, and every node in the fleet multiplies its "+
				"footprint against this peer", tc.name, got, maxAllowed)
		}
	}
}

// 🔑 A CRITICAL PEER GETS AT LEAST TWO PATHS, and the comment says exactly why:
// "If one connection drops, gossip continues on the other — no mesh disconnect."
// A floor of 1 on an anchor means a single drop partitions this node.
func TestACriticalPeerNeverDropsBelowTwoConnections(t *testing.T) {
	s := scalerForTest()
	peer := &peerConn{nodeID: testNodeIDA, priority: PriorityCritical}

	if got := s.TargetConnections(peer, "syd"); got < 2 {
		t.Fatalf("a PriorityCritical peer got target %d — anchors and bootstrap "+
			"peers need a second path so one drop does not partition this node "+
			"from the mesh", got)
	}
}

// A worse RTT must not REDUCE the target: the latency factor exists so a distant
// peer gets more redundancy, and an inverted sign would strip paths from exactly
// the peers most likely to lose one.
func TestAHigherRTTNeverReducesTheTarget(t *testing.T) {
	s := scalerForTest()

	near := s.TargetConnections(&peerConn{nodeID: testNodeIDA, lastRTT: time.Millisecond}, "syd")
	far := s.TargetConnections(&peerConn{nodeID: testNodeIDA, lastRTT: 500 * time.Millisecond}, "syd")

	if far < near {
		t.Fatalf("500ms peer got %d paths and a 1ms peer got %d — the latency "+
			"factor is inverted, so distant peers get LESS redundancy", far, near)
	}
}

// A manager with no quality tracker must not panic: failureFactor is guarded, and
// this is the ordinary shape on a node whose tracker was never constructed.
func TestTargetConnectionsToleratesAnAbsentQualityTracker(t *testing.T) {
	s := scalerForTest()
	if s.connMgr.qualityTracker != nil {
		t.Fatal("fixture wrong: this test is about the nil-tracker path")
	}

	if got := s.TargetConnections(&peerConn{nodeID: testNodeIDA}, "syd"); got < 1 {
		t.Fatalf("target %d with no quality tracker — a node whose tracker was "+
			"never built would hold no connections at all", got)
	}
}

// ── The event surface ───────────────────────────────────────────────────────

// A zero timestamp must be stamped. The event log is queried historically, so
// an unstamped event sorts as 1970 — absence taking the smallest value inside
// an ordering where the smallest value is also an extreme. The guard exists in
// EmitEvent; this pins it.
func TestAnUnstampedEventGetsATimestamp(t *testing.T) {
	var got ConnectionEvent
	s := NewConnectionScaler(&ConnectionManager{budget: DefaultConnectionBudget()},
		func(e ConnectionEvent) { got = e })

	s.EmitEvent(ConnectionEvent{PeerNodeID: testNodeIDA, Reason: "test"})

	if got.Timestamp.IsZero() {
		t.Fatal("an event emitted with no Timestamp reached the handler unstamped " +
			"— it sorts as 1970 in every historical query of the event log")
	}
}

// An explicitly-set timestamp must be preserved: a caller that stamps the moment
// a transport actually changed must not have it overwritten with emit time.
func TestAnExplicitTimestampIsNotOverwritten(t *testing.T) {
	want := time.Now().Add(-time.Hour)
	var got ConnectionEvent
	s := NewConnectionScaler(&ConnectionManager{budget: DefaultConnectionBudget()},
		func(e ConnectionEvent) { got = e })

	s.EmitEvent(ConnectionEvent{PeerNodeID: testNodeIDA, Timestamp: want})

	if !got.Timestamp.Equal(want) {
		t.Fatalf("timestamp = %v, want the caller's %v — the emit time replaced "+
			"the moment the event actually happened", got.Timestamp, want)
	}
}

// Every subscriber fires, and so does the primary handler. The doc is explicit
// that AddEventSubscriber "fires alongside the primary eventHandler" rather than
// replacing it — the Runtime relies on that to emit LAD records without
// displacing the default logger.
func TestEverySubscriberAndThePrimaryHandlerFire(t *testing.T) {
	var primary, a, b int
	s := NewConnectionScaler(&ConnectionManager{budget: DefaultConnectionBudget()},
		func(ConnectionEvent) { primary++ })
	s.AddEventSubscriber(func(ConnectionEvent) { a++ })
	s.AddEventSubscriber(func(ConnectionEvent) { b++ })

	s.EmitEvent(ConnectionEvent{PeerNodeID: testNodeIDA})

	if primary != 1 {
		t.Errorf("primary handler fired %d times, want 1 — registering a "+
			"subscriber has displaced the default handler", primary)
	}
	if a != 1 || b != 1 {
		t.Errorf("subscribers fired %d and %d, want 1 each — a later subscriber "+
			"is replacing an earlier one rather than being appended", a, b)
	}
}

// A nil subscriber must be refused at registration, not discovered at emit time
// — EmitEvent calls every subscriber unconditionally, so a stored nil panics on
// the next connection event, far from the code that registered it.
func TestANilSubscriberIsRefusedAtRegistration(t *testing.T) {
	s := scalerForTest()
	s.AddEventSubscriber(nil)

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("EmitEvent panicked after a nil subscriber was registered: %v "+
				"— the nil is stored and only discovered on the next connection "+
				"event, which is a crash on the mesh's hot path", r)
		}
	}()
	s.EmitEvent(ConnectionEvent{PeerNodeID: testNodeIDA})
}

// 🔑 RE-ENTRANCY: EmitEvent copies the subscriber slice under s.mu and releases
// the lock BEFORE calling handlers. That is load-bearing — a handler that
// registers another subscriber (or reads scaler state) would deadlock against a
// held lock, and it would deadlock inside a connection-state change.
func TestASubscriberMayRegisterAnotherWithoutDeadlocking(t *testing.T) {
	s := scalerForTest()

	var once sync.Once
	s.AddEventSubscriber(func(ConnectionEvent) {
		once.Do(func() { s.AddEventSubscriber(func(ConnectionEvent) {}) })
	})

	done := make(chan struct{})
	go func() {
		s.EmitEvent(ConnectionEvent{PeerNodeID: testNodeIDA})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("EmitEvent deadlocked when a subscriber registered another — the " +
			"subscriber slice is being iterated while s.mu is held, so any handler " +
			"that touches the scaler wedges a connection-state change")
	}
}

// ── Reaching the interior of the formula ────────────────────────────────────
//
// 🔴 THE TESTS ABOVE COULD NOT SEE THE FORMULA, AND MUTATION SAID SO. Three
// mutants survived — min clamp removed, critical floor removed, latency factor
// INVERTED — because every peer in that fixture returns the same number:
//
//	MEASURED: a peer with no transports and state != PeerConnected grades F
//	(weight 2.0), so the raw target is ceil(1.0 × 1.0 × 2.0 × 1.0 × 1.0) = 2,
//	which is exactly DefaultConnectionBudget().MaxPerPeer for a same-region peer.
//	Every input saturated at the ceiling, so the interior was invisible.
//
// 🔑 A CLAMPED OUTPUT HIDES THE FUNCTION UNDER IT. To exercise the formula the
// fixture must (a) give the peer a real grade — bestActiveGrade returns
// GradeForProtocol(protocol) once state == PeerConnected — and (b) widen the
// budget so the ceiling stops binding.

// gradeAPeer returns a peer whose bestActiveGrade is A (weight 1.0), so the
// grade penalty stops dominating and the other factors become observable.
func gradeAPeer(rtt time.Duration) *peerConn {
	return &peerConn{
		nodeID:   testNodeIDA,
		state:    PeerConnected,
		protocol: ProtoNoiseUDP,
		lastRTT:  rtt,
	}
}

func scalerWithBudget(b *ConnectionBudget) *ConnectionScaler {
	return NewConnectionScaler(&ConnectionManager{budget: b}, nil)
}

// With the ceiling out of the way, a higher RTT must yield MORE paths. This is
// the test that kills the inverted-latency mutant the first one could not.
func TestTheLatencyFactorActuallyRaisesTheTargetWhenTheCeilingIsNotBinding(t *testing.T) {
	b := DefaultConnectionBudget()
	b.MaxPerPeer = 10 // wide enough that the clamp does not flatten the result
	s := scalerWithBudget(b)

	near := s.TargetConnections(gradeAPeer(time.Millisecond), "syd")
	far := s.TargetConnections(gradeAPeer(500*time.Millisecond), "syd")

	if far <= near {
		t.Fatalf("1ms peer -> %d paths, 500ms peer -> %d — with the ceiling raised "+
			"the latency factor must produce strictly more redundancy for a "+
			"distant peer. Equal values mean the clamp is still hiding the "+
			"formula; a smaller value means the factor is inverted", near, far)
	}
}

// The MIN clamp is only reachable when the raw target falls below it: a GradeA
// peer at ~0 RTT computes 1, so a MinPerPeer above that exercises the floor.
func TestTheMinimumClampRaisesATargetThatWouldFallBelowIt(t *testing.T) {
	b := DefaultConnectionBudget()
	b.MinPerPeer, b.MaxPerPeer = 5, 10
	s := scalerWithBudget(b)

	if got := s.TargetConnections(gradeAPeer(0), "syd"); got < 5 {
		t.Fatalf("target %d for a peer whose raw target is 1, want >= MinPerPeer 5 "+
			"— the floor is not being applied, so a healthy low-latency peer is "+
			"held below the configured minimum redundancy", got)
	}
}

// 🔴 THE REAL GUARANTEE IS A UNIVERSAL K>=2 FLOOR, AND IT SUBSUMES THE
// CRITICAL-PEER FLOOR ENTIRELY.
//
// MEASURED: a GradeA 0-RTT peer computes a raw target of 1, and comes back as 2
// — not from the PriorityCritical branch but from the LAST statement in the
// function, `if result < 2 && result < maxPerPeer { result = 2 }`. Its comment
// gives the reason: every peer gets a standby path so a keepalive failure fails
// over via OnPrimaryFailure instead of triggering a full reconnect.
//
// ⇒ THE `PriorityCritical` MINIMUM-2 EARLIER IN THE FUNCTION CAN NO LONGER
// DETERMINE ANY OUTCOME — the universal floor already guarantees what it asks
// for. That is why the "critical floor removed" mutant SURVIVES: it is an
// EQUIVALENT mutant rather than a hole in these tests. The universal floor is
// what is pinned here, because it is the guard actually carrying the
// guarantee.
func TestEveryPeerGetsAStandbyPathUnlessTheBudgetForbidsIt(t *testing.T) {
	b := DefaultConnectionBudget()
	b.MinPerPeer, b.MaxPerPeer = 1, 10
	s := scalerWithBudget(b)

	if got := s.TargetConnections(gradeAPeer(0), "syd"); got < 2 {
		t.Fatalf("a healthy GradeA peer got %d paths, want >= 2 — the universal "+
			"standby floor is gone, so a single keepalive failure triggers a full "+
			"reconnect instead of an instant multipath failover", got)
	}

	// …and a budget of exactly 1 must still win: the floor is explicitly bounded
	// by maxPerPeer so a strict budget is not overridden.
	strict := DefaultConnectionBudget()
	strict.MinPerPeer, strict.MaxPerPeer = 1, 1
	if got := scalerWithBudget(strict).TargetConnections(gradeAPeer(0), "syd"); got != 1 {
		t.Fatalf("target %d with MaxPerPeer=1, want 1 — the standby floor is "+
			"overriding an explicit budget ceiling, so an operator cannot cap a "+
			"peer at a single connection", got)
	}
}
