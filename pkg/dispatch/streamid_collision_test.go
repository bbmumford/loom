/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package dispatch

import (
	"context"
	"testing"
	"time"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/health"
)

// Characterisation of a reserved-ID collision in the dynamic stream-ID allocator.
//
// MEASURED: dynamicStreamIDFloor is 10 and nextDynamicStreamID returns
// `counter.Add(1) + (dynamicStreamIDFloor - 1)` — a monotonic per-session counter
// with NO skip list, NO reserved-ID exclusion and NO cap. So allocation N yields
// StreamID N+9, and allocation 91 yields exactly 100.
//
// 100 is the swarm transport's dedicated stream (loom/node/swarm_transport.go's
// swarmStreamConfig).
//
// AND THE DIRECTION OF THE FAULT IS THE OPPOSITE OF WHAT IT LOOKS LIKE. aether's
// own StreamConfig contract (session_stream.go, content-identical at v0.0.111 and
// v0.0.116) states: "Well-known IDs: 0=gossip, 1=RPC, 2=keepalive, 3=control.
// Application streams should use IDs >= 100."
//
//   - swarmStreamConfig picking 100 is NOT a stray literal parked outside a
//     reservation list. It is the one call site OBEYING that contract.
//   - THIS ALLOCATOR is the violator: a floor of 10 with an unbounded monotonic
//     counter mints 10, 11, 12 … through 99 and straight INTO the band aether
//     documents as application-owned.
//
// ⇒ SO A SKIP LIST FOR 100 WOULD BE THE WRONG FIX: it patches one victim while the
// allocator keeps minting 101, 102, 103 … every one of them inside the application
// band. The next application stream added at 101 or 110 collides identically and
// the skip list has to grow forever. A floor of 10 with no ceiling cannot coexist
// with a documented application band starting at 100 — the dynamic range needs a
// bound, not an exception list.
//
// This is also the same class as the H-Dispatch-StreamIDCollision precedent
// documented in streamid.go — that fix consolidated two competing counters onto one
// and never reconciled the range with aether's contract. That file records such
// collisions surfacing as "a phantom closed-stream error long after the bug", which
// is why this is worth a tripwire rather than a comment.
//
// ⚠ THIS TEST ASSERTS THE DEFECT, NOT THE DESIRED BEHAVIOUR. It passes today.
//
// 🛑 WHEN IT FAILS, THAT IS THE FIX LANDING — replace the assertion, do not delete
// the file: swap it for one proving no allocation ever enters the >= 100
// application band. Do NOT simply relax it, because the exposure is still
// unmeasured: StreamPool reuses streams and forgetSession resets the counter, so
// whether a live session reaches 91 allocations is unknown.

// collisionProbeSession is a minimal aether.Session used only as a map key for
// the per-session counter. No method is exercised.
type collisionProbeSession struct{}

func (s *collisionProbeSession) OpenStream(context.Context, aether.StreamConfig) (aether.Stream, error) {
	return nil, context.Canceled
}
func (s *collisionProbeSession) AcceptStream(context.Context) (aether.Stream, error) {
	return nil, context.Canceled
}
func (s *collisionProbeSession) AcceptStreamByID(context.Context, uint64) (aether.Stream, error) {
	return nil, context.Canceled
}
func (s *collisionProbeSession) LocalNodeID() aether.NodeID   { return "" }
func (s *collisionProbeSession) RemoteNodeID() aether.NodeID  { return "" }
func (s *collisionProbeSession) LocalPeerID() aether.PeerID   { return aether.PeerID{} }
func (s *collisionProbeSession) RemotePeerID() aether.PeerID  { return aether.PeerID{} }
func (s *collisionProbeSession) Capabilities() aether.Capabilities { return 0 }
func (s *collisionProbeSession) Ping(context.Context) (time.Duration, error) { return 0, nil }
func (s *collisionProbeSession) GoAway(context.Context, aether.GoAwayReason, string) error {
	return nil
}
func (s *collisionProbeSession) Close() error                       { return nil }
func (s *collisionProbeSession) IsClosed() bool                     { return false }
func (s *collisionProbeSession) Health() *health.Monitor            { return nil }
func (s *collisionProbeSession) SessionKey() []byte                 { return nil }
func (s *collisionProbeSession) ConnectionID() aether.ConnectionID  { return aether.ConnectionID{} }
func (s *collisionProbeSession) CongestionWindow() int64            { return 0 }
func (s *collisionProbeSession) Protocol() aether.Protocol          { return aether.ProtoNoise }
func (s *collisionProbeSession) Metrics() aether.SessionMetrics     { return aether.SessionMetrics{} }

// swarmStreamID is the swarm transport's dedicated stream, and
// applicationStreamFloor is the start of aether's documented application band.
// Both are duplicated here as literals ON PURPOSE: the point of this test is that
// no shared constant exists for either. If a single source of truth is introduced,
// these should be replaced by it — and that replacement is the real fix.
const (
	swarmStreamID          uint64 = 100
	applicationStreamFloor uint64 = 100
)

// TestDynamicStreamID_WalksOntoTheSwarmStream is the finding.
//
// It allocates until the counter reaches the swarm stream's ID and asserts it is
// reached exactly where the arithmetic says: allocation 91.
func TestDynamicStreamID_WalksOntoTheSwarmStream(t *testing.T) {
	sess := &collisionProbeSession{}
	defer forgetSession(sess)

	const wantAllocation = 91 // swarmStreamID - dynamicStreamIDFloor + 1

	var collidedAt int
	for i := 1; i <= wantAllocation+5; i++ {
		if got := nextDynamicStreamID(sess); got == swarmStreamID {
			collidedAt = i
			break
		}
	}

	if collidedAt == 0 {
		t.Fatalf("the allocator never returned %d in %d allocations. If a reserved-ID "+
			"skip was added, THIS IS THE FIX — replace this assertion with one proving "+
			"no allocation ever returns a reserved ID, rather than deleting the file",
			swarmStreamID, wantAllocation+5)
	}
	if collidedAt != wantAllocation {
		t.Errorf("allocator reached the swarm stream ID %d at allocation %d, expected %d "+
			"— the allocation arithmetic changed, so the reserved-band reasoning in this "+
			"file's header needs re-measuring", swarmStreamID, collidedAt, wantAllocation)
	}
}

// TestDynamicStreamID_StartsAboveTheDocumentedReservedBand is the POSITIVE CONTROL.
//
// Without it, the test above is consistent with "the allocator is broken in some
// unrelated way". This pins that the allocator IS doing what it documents for the
// band it knows about (0-9) — which is exactly why the collision at 100 is a gap
// in the reservation, not a bug in the counter.
func TestDynamicStreamID_StartsAboveTheDocumentedReservedBand(t *testing.T) {
	sess := &collisionProbeSession{}
	defer forgetSession(sess)

	first := nextDynamicStreamID(sess)
	if first != dynamicStreamIDFloor {
		t.Errorf("first allocation = %d, want %d (dynamicStreamIDFloor)", first, dynamicStreamIDFloor)
	}
	if first < 10 {
		t.Errorf("first allocation %d lands inside the documented 0-9 well-known band", first)
	}

	// And it must be strictly monotonic — the property that makes walking onto a
	// higher reserved ID inevitable rather than merely possible.
	prev := first
	for i := 0; i < 5; i++ {
		next := nextDynamicStreamID(sess)
		if next <= prev {
			t.Fatalf("allocator is not strictly increasing: %d followed by %d", prev, next)
		}
		prev = next
	}
}
