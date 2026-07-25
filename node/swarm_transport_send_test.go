/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"errors"
	"testing"

	"github.com/bbmumford/swarm"
)

// An unregistered peer must be reportable, not silently successful.
//
// This is the whole point of the change: the swarm layer's anti-entropy cannot
// repair a frame it believes was delivered, so "no peer" has to be an error the
// caller can see. A nil return here is the defect, not a tidy no-op.
func TestSend_UnregisteredPeerReportsErrorNotNil(t *testing.T) {
	tp := NewMeshSwarmTransport(swarm.NodeID("self"))
	defer tp.Stop()

	err := tp.Send(swarm.NodeID("never-registered"), []byte("frame"))
	if err == nil {
		t.Fatal("Send to an unregistered peer returned nil — an undeliverable frame reported as sent")
	}
	if !errors.Is(err, ErrPeerNotRegistered) {
		t.Fatalf("Send error = %v, want one matching ErrPeerNotRegistered via errors.Is", err)
	}
}

// The error has to survive errors.Is through the %w wrapping that carries the
// peer id, otherwise callers cannot distinguish "no such peer" from a genuine
// transport failure and are pushed back to ignoring every error.
func TestSend_UnregisteredPeerErrorIsMatchableAndIdentifiesPeer(t *testing.T) {
	tp := NewMeshSwarmTransport(swarm.NodeID("self"))
	defer tp.Stop()

	err := tp.Send(swarm.NodeID("peer-abcdef"), []byte("frame"))
	if !errors.Is(err, ErrPeerNotRegistered) {
		t.Fatalf("error = %v, want errors.Is(ErrPeerNotRegistered)", err)
	}
	// A bare sentinel would lose which peer failed; the wrap must keep it.
	if msg := err.Error(); msg == ErrPeerNotRegistered.Error() {
		t.Errorf("error is the bare sentinel %q — the failing peer is not identified", msg)
	}
}

// Broadcast previously returned nil unconditionally while discarding every
// error. With no peers there is nothing to report, so nil is correct here —
// this pins the empty case so the error-joining change cannot start inventing
// failures for an idle transport.
func TestBroadcast_NoPeersIsNotAnError(t *testing.T) {
	tp := NewMeshSwarmTransport(swarm.NodeID("self"))
	defer tp.Stop()

	if err := tp.Broadcast([]byte("frame")); err != nil {
		t.Fatalf("Broadcast with zero peers = %v, want nil", err)
	}
}

// A registered-but-streamless peer is the state the responder lands in when
// AcceptStreamByID(100) fails: present in the map, no stream. Broadcast must
// report it rather than skipping it, which is what `if p.stream != nil` did.
func TestBroadcast_StreamlessPeerIsReportedNotSkipped(t *testing.T) {
	tp := NewMeshSwarmTransport(swarm.NodeID("self"))
	defer tp.Stop()

	// Attach a peer with no stream, mirroring the failed-accept state.
	tp.mu.Lock()
	tp.peers[swarm.NodeID("streamless")] = &meshSwarmPeer{id: swarm.NodeID("streamless")}
	tp.mu.Unlock()

	err := tp.Broadcast([]byte("frame"))
	if err == nil {
		t.Fatal("Broadcast with a streamless peer returned nil — the undeliverable peer was skipped silently")
	}
	if !errors.Is(err, ErrPeerNotRegistered) {
		t.Fatalf("Broadcast error = %v, want errors.Is(ErrPeerNotRegistered)", err)
	}
}
