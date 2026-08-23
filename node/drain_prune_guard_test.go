/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"testing"
	"time"
)

// pruneStalePeers keeps a peer whose drainState is anything other than
// DrainActive. DrainActive is the zero value and means "not draining", so that
// half of the guard only fires once something assigns DrainStarted — and until
// Rebalance did, no code path in the package ever wrote any value but the zero
// one. A peer could therefore be evicted while its drain was still inside the
// grace window, discarding the drainedAt re-dial suppression and the transport
// map along with it.

func prunablePeer(state PeerState, lastConnected time.Time) *peerConn {
	return &peerConn{
		nodeID:        "peer-a",
		state:         state,
		connCount:     0, // the other half of the guard must not mask the drain half
		lastConnected: lastConnected,
	}
}

// 🔴 A PEER MID-DRAIN SURVIVES THE PRUNE. Without this the drain completes
// against a peer entry that no longer exists.
func TestAPeerMidDrainIsNotPruned(t *testing.T) {
	m := &ConnectionManager{peers: map[string]*peerConn{}, budget: DefaultConnectionBudget()}
	p := prunablePeer(PeerDisconnected, time.Now().Add(-2*stalePeerTTL))
	p.drainState = DrainStarted
	m.peers["peer-a"] = p

	m.pruneStalePeers()

	if _, ok := m.peers["peer-a"]; !ok {
		t.Error("a peer whose drain is still in progress was pruned — the drain then " +
			"completes against a peer entry that is gone, losing the re-dial suppression " +
			"window and the transport map with it")
	}
}

// 🔬 THE CONTROL. A guard that preserved everything would satisfy the test
// above while disabling the sweep entirely, so a genuinely stale peer that is
// NOT draining must still be pruned.
func TestAStalePeerThatIsNotDrainingIsStillPruned(t *testing.T) {
	m := &ConnectionManager{peers: map[string]*peerConn{}, budget: DefaultConnectionBudget()}
	p := prunablePeer(PeerDisconnected, time.Now().Add(-2*stalePeerTTL))
	p.drainState = DrainActive // the zero value: not draining
	m.peers["peer-a"] = p

	m.pruneStalePeers()

	if _, ok := m.peers["peer-a"]; ok {
		t.Error("a long-disconnected peer with no drain in progress survived the sweep — " +
			"the peer map grows without bound")
	}
}

// The drain mark must be cleared when the drain completes, or a peer that
// drained once is preserved from every future sweep.
func TestCompletingADrainReturnsThePeerToTheSweep(t *testing.T) {
	m := &ConnectionManager{peers: map[string]*peerConn{}, budget: DefaultConnectionBudget()}
	p := prunablePeer(PeerDisconnected, time.Now().Add(-2*stalePeerTTL))
	p.drainState = DrainStarted
	p.transports = map[Protocol]*transportConn{}
	m.peers["peer-a"] = p

	// closeDrainedConnection is what the drain monitor calls on completion.
	m.closeDrainedConnection("peer-a", "quic", time.Now())

	m.mu.Lock()
	state := m.peers["peer-a"].drainState
	m.mu.Unlock()
	if state != DrainActive {
		t.Fatalf("drainState = %v after the drain completed, want DrainActive (not "+
			"draining) — a peer that drained once is preserved from every later sweep", state)
	}

	m.pruneStalePeers()
	if _, ok := m.peers["peer-a"]; ok {
		t.Error("the peer survived the sweep after its drain completed")
	}
}

// 🔬 THE TEST THAT COVERS THE WIRING, AND IT EXISTS BECAUSE A MUTANT SURVIVED.
// Every test above assigns drainState directly, so deleting the marking inside
// Rebalance changed nothing and they all stayed green — they prove the guard
// reads the field and prove nothing about anything writing it. Rebalance is the
// only writer of DrainStarted, so the property has to be entered through it.
//
// The fixture has to clear four gates before a candidate exists, and getting
// any of them wrong makes the assertion vacuous: the peer must be Connected,
// hold MORE TRANSPORTS than its target (SelectForDrain drains
// len(conns)-target, so one transport yields nothing however high connCount
// is), carry no Grade-A path (those are never drained), and carry something
// better than Grade C (a WebSocket+TLS pair with nothing better is preserved
// for cross-class failover).
func TestRebalanceMarksThePeerBeforeStartingItsDrain(t *testing.T) {
	m := &ConnectionManager{peers: map[string]*peerConn{}, budget: DefaultConnectionBudget()}
	m.scaler = NewConnectionScaler(m, nil)
	m.drainMgr = NewDrainManager(func(string, string, time.Time) {})
	t.Cleanup(m.drainMgr.Stop)

	p := &peerConn{
		nodeID:    "peer-a",
		state:     PeerConnected,
		protocol:  ProtoWebSocket,
		connCount: 3, // above any target the default budget permits
		transports: map[Protocol]*transportConn{
			ProtoWebSocket: {protocol: ProtoWebSocket, grade: GradeC, connectedAt: time.Now()},
			ProtoTLS:       {protocol: ProtoTLS, grade: GradeC, connectedAt: time.Now()},
			// Grade B: better than C, so the dual-Grade-C preservation does not
			// fire, and not Grade A, so the peer is still drainable.
			ProtoGRPC: {protocol: ProtoGRPC, grade: GradeB, connectedAt: time.Now()},
		},
	}
	m.peers["peer-a"] = p

	got := m.scaler.Rebalance()
	if len(got) == 0 {
		t.Fatal("Rebalance produced no drain candidate — the fixture is failing an " +
			"upstream gate, so this test cannot observe whether the peer is marked")
	}

	m.mu.Lock()
	state := p.drainState
	m.mu.Unlock()
	if state != DrainStarted {
		t.Errorf("drainState = %v after Rebalance selected this peer for drain, want "+
			"DrainStarted — nothing marks the peer, so pruneStalePeers cannot tell a "+
			"drain in progress from a peer that never drained and may evict it mid-drain",
			state)
	}
}
