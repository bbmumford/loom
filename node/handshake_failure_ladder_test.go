/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import "testing"

// The dial sites credit recordDialSuccess as soon as dialWithProtocol returns a
// conn — connectPeer and dialAdditionalPath both do — which clears the
// per-(peer, protocol) dial cooldown. The mesh handshake then runs
// asynchronously in DialAndAcceptMesh, and its failure branch recorded nothing.
//
// A peer that accepts connections but never completes a session therefore had
// its ladder cleared on every attempt and was re-dialled at the base delay
// indefinitely: the escalating back-off could not escalate.

// ⚠ WHAT THESE TESTS COVER, AND WHAT THEY DO NOT. They exercise the ladder
// semantics the fix depends on — that repeated failures escalate to
// suppression, that success does not, and that the key is per-(peer, protocol).
// They call recordDialFailure directly and therefore say NOTHING about whether
// DialAndAcceptMesh calls it: deleting that call site leaves every test here
// green.
//
// That call site is on the same setup-failure branch as the connecting-state
// ownership gate, and it is unreachable from a unit test for the same reason
// recorded in connecting_state_ownership_test.go — a closed net.Pipe still
// constructs a session adapter, and a nil conn panics inside the adapter rather
// than returning an error. Both need an integration fixture with a transport
// that can be made to fail.
//
// 🔴 THE ASYMMETRY. Success at the transport layer clears the ladder; failure at
// the handshake layer must debit it, or the two layers disagree about whether
// the path works and the debit side never fires.
func TestAHandshakeFailureDebitsTheDialLadderThatTheTransportSuccessCleared(t *testing.T) {
	m := trackedManager()

	// The transport dial succeeded, so the ladder was cleared.
	m.recordDialSuccess(testNodeIDA, ProtoWebSocket)
	if m.dialIsSuppressed(testNodeIDA, ProtoWebSocket) {
		t.Fatal("fixture wrong: the path is suppressed before the handshake even ran, so " +
			"this test cannot observe a debit")
	}

	// Enough handshake failures to escalate past the suppression threshold.
	for i := 0; i < 8; i++ {
		m.recordDialFailure(testNodeIDA, ProtoWebSocket)
	}

	if !m.dialIsSuppressed(testNodeIDA, ProtoWebSocket) {
		t.Error("repeated handshake failures left the path unsuppressed — a peer that " +
			"accepts connections and never completes a session is re-dialled at the base " +
			"delay forever, because only the transport-layer success is recorded")
	}
}

// 🔬 THE CONTROL. A debit that fired regardless would suppress healthy paths.
// A path whose handshake succeeds must stay dialable.
func TestASuccessfulPathIsNotSuppressed(t *testing.T) {
	m := trackedManager()

	m.recordDialSuccess(testNodeIDA, ProtoWebSocket)

	if m.dialIsSuppressed(testNodeIDA, ProtoWebSocket) {
		t.Error("a path that dialled and handshook successfully is suppressed — the debit " +
			"is firing on the success path and healthy transports are being taken out")
	}
}

// The ladder is keyed by (peer, protocol), so a handshake failure on one
// protocol must not suppress a different one — the peer may be perfectly
// reachable over another transport.
func TestAHandshakeFailureDoesNotSuppressASiblingProtocol(t *testing.T) {
	m := trackedManager()

	for i := 0; i < 8; i++ {
		m.recordDialFailure(testNodeIDA, ProtoWebSocket)
	}

	if m.dialIsSuppressed(testNodeIDA, ProtoNoiseUDP) {
		t.Error("failing the WebSocket handshake suppressed noise-udp to the same peer — " +
			"one broken transport takes out a transport that never failed")
	}
}
