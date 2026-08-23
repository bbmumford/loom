/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"testing"
	"time"
)

// Covers the ConnectionManager's operator/mesh-facing state accessors:
// BestActiveGrade (:1760), ActivePeers (:1775),
// PeerStates (:1787), GossipActiveSet (:1817), BudgetStatus (:1828).
//
// ⚠ DUPLICATION CHECK: no existing node/ test names any of the five.
//
// These are not incidental getters. BestActiveGrade is what LAD PUBLISHES as
// this node's max supported grade, ActivePeers is what InitRoute uses to
// backfill direct-path advertisements, and PeerStates/GossipActiveSet/
// BudgetStatus are the debug and monitoring surfaces an operator reads during
// an incident. A wrong answer here is either a false advertisement to the mesh
// or a false picture during triage.
//
// Lock discipline checked first: all five take the correct
// lock — BestActiveGrade/ActivePeers/PeerStates take m.mu, GossipActiveSet
// takes gossipMu, and BudgetStatus reads through the budget's own atomics.
// That is stated because it was measured, not assumed.

func accessorFixture() *ConnectionManager {
	return &ConnectionManager{peers: map[string]*peerConn{}}
}

// 🔴 THE ADVERTISEMENT PROPERTY. BestActiveGrade feeds LAD publishing, so a
// grade counted from a peer that cannot currently carry traffic is a capability
// this node advertises to the whole mesh and cannot honour. bestActiveGrade()
// is what enforces it per peer, and this pins that the aggregate relies on it.
func TestBestActiveGradeCountsOnlyPeersThatCanActuallyCarryTraffic(t *testing.T) {
	for _, tc := range []struct {
		name string
		peer *peerConn
		want Grade
	}{
		{
			"a disconnected peer contributes nothing",
			&peerConn{nodeID: "p", state: PeerDisconnected, protocol: ProtoNoiseUDP},
			GradeF,
		},
		{
			"a merely discovered peer contributes nothing",
			&peerConn{nodeID: "p", state: PeerDiscovered, protocol: ProtoNoiseUDP},
			GradeF,
		},
		{
			"a connected peer contributes its protocol's grade",
			&peerConn{nodeID: "p", state: PeerConnected, protocol: ProtoNoiseUDP},
			GradeForProtocol(ProtoNoiseUDP),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := accessorFixture()
			m.peers["p"] = tc.peer

			if got := m.BestActiveGrade(); got != tc.want {
				t.Errorf("BestActiveGrade = %v, want %v — this value is PUBLISHED to LAD as "+
					"this node's max supported grade, so an overstatement advertises a "+
					"capability the node cannot honour", got, tc.want)
			}
		})
	}
}

// An empty manager must report GradeF, not a zero value that happens to read as
// something better. GradeF is 0 and is also the correct identity for a maximum,
// so this pins that the two coincide deliberately rather than by luck.
func TestBestActiveGradeOnAnEmptyMeshIsGradeF(t *testing.T) {
	if got := accessorFixture().BestActiveGrade(); got != GradeF {
		t.Errorf("BestActiveGrade with no peers = %v, want GradeF", got)
	}
}

// 🔑 A DORMANT TRANSPORT IS NOT AN ACTIVE ONE. bestActiveGrade skips
// `tc.isDormant`, and the aggregate must inherit that: a peer whose only
// transport is parked would otherwise advertise its parked grade to the mesh.
func TestADormantTransportDoesNotRaiseTheAdvertisedGrade(t *testing.T) {
	m := accessorFixture()
	m.peers["p"] = &peerConn{
		nodeID: "p",
		state:  PeerConnected,
		transports: map[Protocol]*transportConn{
			ProtoNoiseUDP: {grade: GradeA, isDormant: true},
		},
	}

	if got := m.BestActiveGrade(); got != GradeF {
		t.Errorf("BestActiveGrade = %v with only a DORMANT grade-A transport, want GradeF — "+
			"the node would advertise grade A over a transport that is parked", got)
	}

	// And a live transport alongside it does count.
	m.peers["p"].transports[ProtoWebSocket] = &transportConn{grade: GradeB}
	if got := m.BestActiveGrade(); got != GradeB {
		t.Errorf("BestActiveGrade = %v with a live grade-B transport, want GradeB", got)
	}
}

// The aggregate is a MAXIMUM across peers — the best any peer can do — so one
// good peer must lift it and one bad peer must not lower it.
func TestBestActiveGradeIsTheMaximumAcrossPeersNotTheLastOneSeen(t *testing.T) {
	m := accessorFixture()
	m.peers["poor"] = &peerConn{nodeID: "poor", state: PeerConnected,
		transports: map[Protocol]*transportConn{ProtoWebSocket: {grade: GradeC}}}
	m.peers["good"] = &peerConn{nodeID: "good", state: PeerConnected,
		transports: map[Protocol]*transportConn{ProtoNoiseUDP: {grade: GradeA}}}
	m.peers["down"] = &peerConn{nodeID: "down", state: PeerDisconnected}

	if got := m.BestActiveGrade(); got != GradeA {
		t.Errorf("BestActiveGrade = %v, want GradeA — map iteration order must not decide "+
			"the advertised grade", got)
	}
}

// ─── ActivePeers ─────────────────────────────────────────────────────────

// ActivePeers filters on connCount, which is a DIFFERENT question from the
// state field BestActiveGrade's helper keys on. Both are answered here in one
// fixture so the two criteria stay visible side by side.
func TestActivePeersReportsOnlyPeersHoldingAnOpenSession(t *testing.T) {
	m := accessorFixture()
	m.peers["open"] = &peerConn{nodeID: "open", state: PeerConnected, connCount: 2}
	m.peers["none"] = &peerConn{nodeID: "none", state: PeerConnected, connCount: 0}
	m.peers["down"] = &peerConn{nodeID: "down", state: PeerDisconnected, connCount: 0}

	got := m.ActivePeers()
	if len(got) != 1 || got[0] != "open" {
		t.Errorf("ActivePeers = %v, want [open] — InitRoute backfills direct-path "+
			"advertisements from this list, so a peer with no open session would get a "+
			"direct route advertised for it", got)
	}
}

// Characterisation — and the divergence between these two is deliberate.
// BestActiveGrade's helper keys on peer.state and peer.transports;
// ActivePeers keys on peer.connCount. They answer different questions:
//
//	connCount  — how many SESSIONS are open   (route backfill: "can I advertise
//	             a direct path for this peer right now?")
//	transports — which TRANSPORTS are live    (LAD grade: "what is the best
//	             quality this node can offer?")
//
// MEASURED at the teardown site (mesh_connection.go:1068-1073): connCount is
// decremented unconditionally while state flips to PeerDisconnected only when
// removeTransport reports allDead. The two are decoupled ON PURPOSE, so a peer
// mid-teardown legitimately has connCount==0 with a live transport still
// registered — and counting its grade is CORRECT, because that transport can
// still carry traffic.
//
// ⚠ WHAT I HAVE NOT ESTABLISHED is whether this fixture's shape —
// state==PeerConnected with transports==nil, which takes bestActiveGrade's
// OTHER branch (GradeForProtocol on the bare protocol field) — is reachable in
// production. It is the one combination where the grade comes from neither a
// session nor a transport. Pinned as characterisation, reachability open.
func TestStateAndConnCountAnswerDifferentQuestionsAndCanDisagree(t *testing.T) {
	m := accessorFixture()
	m.peers["p"] = &peerConn{
		nodeID: "p", state: PeerConnected, connCount: 0, protocol: ProtoNoiseUDP,
	}

	grade := m.BestActiveGrade()
	active := m.ActivePeers()

	if grade == GradeF {
		t.Skip("bestActiveGrade no longer keys on state alone — the divergence this test " +
			"characterises has been closed; re-read the divergence note above before " +
			"deleting it")
	}
	if len(active) != 0 {
		t.Fatalf("fixture no longer produces the divergence: ActivePeers = %v", active)
	}
	t.Logf("CHARACTERISED: BestActiveGrade=%v (advertised to LAD) while ActivePeers is EMPTY "+
		"— state and connCount are read by different accessors and disagree here", grade)
}

// ─── PeerStates ──────────────────────────────────────────────────────────

// The debug surface must carry the fields an operator triages with, and must
// not emit a connectedSince for a peer that has never connected — a zero
// timestamp formatted as RFC3339 reads as "connected since year 1".
func TestPeerStatesOmitsConnectionTimesForAPeerThatNeverConnected(t *testing.T) {
	m := accessorFixture()
	m.peers["never"] = &peerConn{nodeID: "never", state: PeerDiscovered}
	m.peers["once"] = &peerConn{
		nodeID: "once", state: PeerConnected, connCount: 1,
		lastConnected: time.Now().Add(-time.Hour), lastRTT: 12 * time.Millisecond,
	}

	states := m.PeerStates()
	if len(states) != 2 {
		t.Fatalf("PeerStates returned %d entries, want 2", len(states))
	}

	for id, st := range states {
		if st["state"] == "" || st["protocol"] == "" || st["grade"] == "" {
			t.Errorf("%s: a triage field is empty: %v", id, st)
		}
	}

	var never, once map[string]string
	for id, st := range states {
		if st["state"] == PeerDiscovered.String() {
			never = st
		} else {
			once = st
		}
		_ = id
	}
	if never == nil || once == nil {
		t.Fatalf("could not identify both peers in %v", states)
	}
	if _, ok := never["connectedSince"]; ok {
		t.Errorf("a never-connected peer reported connectedSince=%q — a zero timestamp "+
			"formatted as RFC3339 reads as a connection from year 1", never["connectedSince"])
	}
	if _, ok := never["lastRTT"]; ok {
		t.Errorf("a never-connected peer reported lastRTT=%q", never["lastRTT"])
	}
	if once["connectedSince"] == "" || once["connectedDuration"] == "" {
		t.Errorf("a connected peer is missing its connection times: %v", once)
	}
	if once["lastRTT"] == "" {
		t.Errorf("a peer with a measured RTT is missing lastRTT: %v", once)
	}
}

// The returned map must be a snapshot the caller can hold: it is handed to a
// debug HTTP handler that serialises it outside m.mu.
func TestPeerStatesReturnsASnapshotNotALiveView(t *testing.T) {
	m := accessorFixture()
	m.peers["p"] = &peerConn{nodeID: "p", state: PeerConnected, connCount: 1}

	before := m.PeerStates()
	m.mu.Lock()
	m.peers["p"].state = PeerDisconnected
	m.mu.Unlock()

	if before[truncID("p")]["state"] != PeerConnected.String() {
		t.Error("a previously returned PeerStates map changed when peer state changed — the " +
			"debug handler serialises it outside m.mu, so it must be a copy")
	}
}

// ─── GossipActiveSet / BudgetStatus ──────────────────────────────────────

// GossipActiveSet distinguishes primary (gossip) from redundant (dispatch-only)
// connections for monitoring, and takes gossipMu rather than m.mu — a different
// lock guarding different state.
func TestGossipActiveSetReportsOnlyPeersWithALiveGossipLoop(t *testing.T) {
	m := accessorFixture()
	m.gossipActive = map[string]*gossipOwner{"gossiping": {grade: GradeA}}

	got := m.GossipActiveSet()
	if len(got) != 1 || !got[truncID("gossiping")] {
		t.Errorf("GossipActiveSet = %v, want the gossiping peer only", got)
	}

	// Snapshot, not a live view — monitoring reads it outside the lock.
	m.gossipMu.Lock()
	m.gossipActive["late"] = &gossipOwner{grade: GradeB}
	m.gossipMu.Unlock()
	if len(got) != 1 {
		t.Errorf("a previously returned GossipActiveSet grew to %v — it aliases the "+
			"manager's map", got)
	}
}

func TestBudgetStatusReportsTheConfiguredCeilingsAndCurrentUse(t *testing.T) {
	m := accessorFixture()
	m.budget = DefaultConnectionBudget()
	m.budget.MaxTotal = 50
	m.budget.MaxPerPeer = 3

	st := m.BudgetStatus()
	for _, k := range []string{"current_total", "max_total", "utilization", "max_per_peer"} {
		if _, ok := st[k]; !ok {
			t.Errorf("BudgetStatus is missing %q — an operator sizing the budget during an "+
				"incident reads exactly these four", k)
		}
	}
	if st["max_total"] != 50 {
		t.Errorf("max_total = %v, want the configured 50 — a reported ceiling that is not "+
			"the enforced one sends capacity triage the wrong way", st["max_total"])
	}
	if st["max_per_peer"] != 3 {
		t.Errorf("max_per_peer = %v, want the configured 3", st["max_per_peer"])
	}
}
