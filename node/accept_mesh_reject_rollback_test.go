/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"testing"
)

// COVERAGE of AcceptMeshConnection's REJECT path.
//
// AcceptMeshConnection is ~1,247 lines and was called not-a-bounded-slice at
// This is its bounded entry: when
// registerMeshSession refuses the session the function rolls back and
// `return nil`s at :210 — so the whole tail (multipath registration, ledger
// publish, scaler EmitEvent, gossip start) never runs. Prologue + rollback
// is a closed unit reachable with a small fixture.
//
// Every assertion below reproduces a DOCUMENTED INCIDENT rather than a
// property I invented. All three leak in the same direction: the connection
// table keeps something the peer does not have, and nothing errors.

// acceptTestManager() differs from registerTestManager() deliberately: the peers
// map must be non-nil because AcceptMeshConnection ASSIGNS into it (:111),
// which panics on a nil map. registerTestManager() leaves its maps nil on
// purpose, to exercise registerMeshSession's lazy init().
func acceptTestManager() *ConnectionManager {
	return &ConnectionManager{
		peers:        make(map[string]*peerConn),
		budget:       DefaultConnectionBudget(),
		walkerWakeCh: make(chan struct{}, 1),
		selfID:       testNodeIDA,
	}
}

// AUDIT H5: a same-grade rejection against an UNKNOWN peer must not leave a
// stub entry behind.
//
// The prologue creates the peerConn before it knows whether the session will
// be accepted. On rejection the rollback deletes it — otherwise the table
// keeps a ghost with connCount=0, protocol=<the rejected one> and
// state=PeerConnected, which the scaler and topology view both read as a
// connected peer.
func TestRejectedAcceptLeavesNoStubPeerEntry(t *testing.T) {
	ctx := context.Background()
	m := acceptTestManager()
	defer stopProving(t, m)

	// A live session already owns dispatch for this peer, so a second
	// same-grade dial from us is a duplicate and will be refused.
	if !m.registerMeshSession(ctx, testNodeIDB, wsSession(), true) {
		t.Fatal("premise wrong: the first session was refused")
	}
	m.mu.Lock()
	_, known := m.peers[testNodeIDB]
	m.mu.Unlock()
	if known {
		t.Fatal("premise wrong: the peer is already in the connection table, so " +
			"this test would exercise the peerExisted branch instead of H5")
	}
	budgetBefore := m.budget.CurrentTotal()

	err := m.AcceptMeshConnection(ctx, AcceptMeshConnectionOpts{
		NodeID:      testNodeIDB,
		Proto:       ProtoWebSocket,
		Session:     wsSession(),
		IsInitiator: true,
	})
	if err != nil {
		t.Fatalf("a rejected session returned an error: %v — rejection is a "+
			"normal outcome and must not surface as a failure", err)
	}

	m.mu.Lock()
	stub, exists := m.peers[testNodeIDB]
	m.mu.Unlock()
	if exists {
		t.Fatalf("AUDIT H5 REGRESSION: the peer created for a REJECTED session "+
			"was left in the table (connCount=%d protocol=%v state=%v "+
			"transports=%d) — the scaler and topology view will treat this ghost "+
			"as a connected peer",
			stub.connCount, stub.protocol, stub.state, len(stub.transports))
	}
	if got := m.budget.CurrentTotal(); got != budgetBefore {
		t.Fatalf("budget slot leaked on reject: currentTotal %d → %d. Each "+
			"rejection permanently consumes a slot, so MaxTotal is reached by "+
			"peers that were never connected", budgetBefore, got)
	}
}

// An EXISTING peer must be restored, not deleted — and specifically its
// protocol must go back to the ACTIVE one rather than staying on the
// rejected one, which the scaler, drain guards and topology display all read.
//
// previousProtocol is snapshotted BEFORE the create/adopt branch precisely so
// this restore has a real value to use.
func TestRejectedAcceptRestoresAnExistingPeersProtocolAndCounters(t *testing.T) {
	ctx := context.Background()
	m := acceptTestManager()
	defer stopProving(t, m)

	if !m.registerMeshSession(ctx, testNodeIDB, wsSession(), true) {
		t.Fatal("premise wrong: the first session was refused")
	}

	// A peer already known via LAD discovery / PEX, live on Noise-UDP.
	const priorConnCount = 5
	m.mu.Lock()
	m.peers[testNodeIDB] = &peerConn{
		nodeID:    testNodeIDB,
		protocol:  ProtoNoiseUDP,
		state:     PeerConnected,
		connCount: priorConnCount,
	}
	m.mu.Unlock()

	// The rejected session arrives on a DIFFERENT protocol, so a failure to
	// restore is visible as the wrong protocol rather than as no change.
	err := m.AcceptMeshConnection(ctx, AcceptMeshConnectionOpts{
		NodeID:      testNodeIDB,
		Proto:       ProtoWebSocket,
		Session:     wsSession(),
		IsInitiator: true,
	})
	if err != nil {
		t.Fatalf("a rejected session returned an error: %v", err)
	}

	m.mu.Lock()
	peer, exists := m.peers[testNodeIDB]
	m.mu.Unlock()
	if !exists {
		t.Fatal("a PRE-EXISTING peer was deleted by a rejected accept — the H5 " +
			"stub cleanup is firing on the peerExisted branch and is dropping " +
			"peers that have live transports of their own")
	}
	if peer.protocol != ProtoNoiseUDP {
		t.Fatalf("peer.protocol = %v after the rejection, want the pre-existing "+
			"%v — the scaler, drain guards and topology display now believe the "+
			"peer is on the protocol we just REFUSED",
			peer.protocol, ProtoNoiseUDP)
	}
	if peer.connCount != priorConnCount {
		t.Fatalf("connCount = %d, want %d. Each rejection leaks one unit; under "+
			"the v0.0.228-era WS flap this reached 40+ for stable peers and the "+
			"scaler then chased phantom connections it could never drain",
			peer.connCount, priorConnCount)
	}
	if _, leaked := peer.transports[ProtoWebSocket]; leaked {
		t.Fatal("the rejected session's transport was left in peer.transports — " +
			"diagnostics and scan-loop gates now see a path that does not exist")
	}
}

// MESH-C07: Release must be gated on whether Acquire actually succeeded.
//
// The old code ignored Acquire's return yet every teardown path still called
// Release, decrementing a slot that was never acquired. currentTotal drifted
// below the true count until MaxTotal stopped being enforced at all.
//
// 🛑 THE FIXTURE IS LOAD-BEARING AND AN EMPTY BUDGET WOULD MAKE THIS TEST
// VACUOUS. Release() is itself guarded by `if b.currentTotal > 0`, so with a
// budget at zero an unconditional Release is a no-op and the mutant survives
// for a reason unconnected to the property. The budget must therefore be
// SATURATED — full, not empty — so a wrongly-executed Release is observable
// as a freed slot.
func TestRejectedAcceptDoesNotReleaseABudgetSlotItNeverAcquired(t *testing.T) {
	ctx := context.Background()
	m := acceptTestManager()
	m.budget = &ConnectionBudget{
		MaxPerPeer: 2,
		MaxTotal:   1,
		priorities: make(map[string]ConnectionPriority),
	}
	defer stopProving(t, m)

	if !m.registerMeshSession(ctx, testNodeIDB, wsSession(), true) {
		t.Fatal("premise wrong: the first session was refused")
	}
	if !m.budget.Acquire() {
		t.Fatal("premise wrong: the sole slot could not be taken")
	}
	if m.budget.Acquire() {
		t.Fatal("premise wrong: the budget is not saturated, so the accept below " +
			"would succeed in acquiring and this test would prove nothing")
	}
	const saturated = 1
	if got := m.budget.CurrentTotal(); got != saturated {
		t.Fatalf("premise wrong: currentTotal = %d, want %d", got, saturated)
	}

	err := m.AcceptMeshConnection(ctx, AcceptMeshConnectionOpts{
		NodeID:      testNodeIDB,
		Proto:       ProtoWebSocket,
		Session:     wsSession(),
		IsInitiator: true,
	})
	if err != nil {
		t.Fatalf("a rejected session returned an error: %v", err)
	}

	if got := m.budget.CurrentTotal(); got != saturated {
		t.Fatalf("currentTotal = %d, want %d — the rollback released a slot the "+
			"accept never acquired. Repeated, this drives the counter below the "+
			"true connection count until MaxTotal stops being enforced and the "+
			"budget silently permits unbounded growth", got, saturated)
	}
}
