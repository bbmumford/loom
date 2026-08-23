/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import "testing"

// resetConnectingState checks the peer's STATE, not who established it, so any
// caller reaching it can clear PeerConnecting. Four call paths reach
// DialAndAcceptMesh and only connectPeer sets that state; the upgrade walker,
// hole puncher and multipath dialer all dial peers whose connecting state, if
// present, belongs to a concurrent connectPeer.

func connectingPeer(t *testing.T, state PeerState) *ConnectionManager {
	t.Helper()
	m := &ConnectionManager{peers: map[string]*peerConn{}, budget: DefaultConnectionBudget()}
	m.peers["peer-a"] = &peerConn{nodeID: "peer-a", state: state}
	return m
}

func stateOf(m *ConnectionManager, id string) PeerState {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.peers[id].state
}

// The owning path must still clear its own wedged state, or a failed dial
// leaves the peer in PeerConnecting forever and scanAndConnect never re-dials.
func TestTheOwningDialClearsItsOwnConnectingState(t *testing.T) {
	m := connectingPeer(t, PeerConnecting)

	m.resetConnectingState("peer-a")

	if got := stateOf(m, "peer-a"); got != PeerDisconnected {
		t.Errorf("state = %v after the owning dial failed, want PeerDisconnected — the "+
			"peer stays wedged in PeerConnecting and is never re-dialled", got)
	}
}

// 🔴 THE OWNERSHIP GATE. A borrowing path must not reach the reset at all. This
// asserts the gate at the only place it can be observed without a live dial:
// the constants that select it must be distinct, and only the connectPeer call
// site may carry the owning one.
func TestOnlyTheConnectPeerCallSiteOwnsTheConnectingState(t *testing.T) {
	if !bool(dialOwnsConnectingState) {
		t.Error("dialOwnsConnectingState is false, so the owning dial no longer clears " +
			"its own wedged state")
	}
	if bool(dialBorrowsConnectingState) {
		t.Error("dialBorrowsConnectingState is true — the walker, hole punch and " +
			"multipath dials would clear a connecting state they never set, cancelling " +
			"a concurrent connectPeer's live attempt")
	}
}

// A peer that is NOT connecting must be untouched whoever asks, so the guard
// stays the last line of defence even when ownership is granted.
func TestResetLeavesAPeerThatIsNotConnectingAlone(t *testing.T) {
	for _, st := range []PeerState{PeerConnected, PeerDisconnected, PeerDiscovered, PeerReconnecting} {
		m := connectingPeer(t, st)
		m.resetConnectingState("peer-a")
		if got := stateOf(m, "peer-a"); got != st {
			t.Errorf("a peer in %v was moved to %v by a reset it should have ignored", st, got)
		}
	}
}

// An unknown peer must be a harmless no-op: dials outlive peer eviction.
func TestResetForAnUnknownPeerIsHarmless(t *testing.T) {
	m := &ConnectionManager{peers: map[string]*peerConn{}, budget: DefaultConnectionBudget()}
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("resetConnectingState panicked for an unknown peer: %v", r)
		}
	}()
	m.resetConnectingState("gone")
}

// ⚠ WHAT THIS FILE DOES NOT COVER, RECORDED SO A GREEN RUN DOES NOT IMPLY IT.
// The `owns` gate lives on DialAndAcceptMesh's setup-failure branch, and every
// test here covers its ingredients instead: the two constants, and
// resetConnectingState's own behaviour. Deleting the gate clause leaves them
// all green.
//
// Reaching the branch needs SetupMeshSession to return an error, and it does
// not fail on the inputs a unit test can supply: a closed net.Pipe still
// constructs a WebSocket adapter session, so execution proceeds into
// AcceptMeshConnection and the peer state is then decided by session teardown
// rather than by the gate, so an assertion there passes for the wrong reason.
// A nil conn panics
// inside the adapter instead of erroring. Forcing a clean setup failure needs a
// transport that can be made to fail, which is an integration fixture.
//
// So the gate is covered by construction review and by W2/W3-style mutation of
// the constants, and its branch is not unit-covered. Stated rather than papered
// over.
