/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"context"
	"net"
	"slices"
	"testing"
	"time"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/adapter"
	"github.com/bbmumford/loom/core/streams"
)

// A mesh session's setup streams and the streams bound per peer pair reach their
// claimant's AcceptStreamByID even when it arms after aether.ByIDBacklogGrace while
// the AcceptStream loop AcceptMeshConnection runs is reading, because setupMeshSession
// lists them in the session's ClaimedStreams: the loop takes none of them.
func TestAMeshSessionHoldsItsClaimedStreamsForALateClaimant(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	rt := wireTestRuntime(t)
	other := wireTestRuntime(t)
	near, far := net.Pipe()
	peerOpts := aether.DefaultSessionOptions()
	peerOpts.WireVersions = meshWireVersions(nil)
	peer := adapter.NewTCPSession(far, other.identity.NodeID, rt.identity.NodeID, aether.ProtoTCP, peerOpts)
	defer peer.Close()
	sess, err := rt.SetupMeshSession(ctx, near, string(other.identity.NodeID), ProtoWebSocket, false)
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()

	taken := make(chan uint64, 16)
	go func() {
		for {
			st, err := sess.AcceptStream(ctx)
			if err != nil {
				return
			}
			taken <- st.StreamID()
		}
	}()

	claimed := []uint64{
		streams.StreamGossip, streams.StreamRPC, streams.StreamKeepalive,
		streams.StreamControl, streams.StreamReconcile,
		streams.StreamSwarm, streams.StreamMeshDB,
	}
	for _, id := range claimed {
		if _, err := peer.OpenStream(ctx, aether.StreamConfig{StreamID: id, Reliability: aether.ReliableOrdered}); err != nil {
			t.Fatalf("peer OpenStream %d: %v", id, err)
		}
	}
	time.Sleep(3 * aether.ByIDBacklogGrace)

	for _, id := range claimed {
		claimCtx, claimCancel := context.WithTimeout(ctx, 2*time.Second)
		st, err := sess.AcceptStreamByID(claimCtx, id)
		claimCancel()
		if err != nil || st == nil {
			t.Errorf("a claimant of stream %d arming after the backlog grace got %v", id, err)
		}
	}
	select {
	case id := <-taken:
		t.Fatalf("the AcceptStream loop took stream %d, which the session claims", id)
	default:
	}
}

// The runtime options' own claims are kept beside loom's, and an ID in both is listed
// once.
func TestMeshClaimedStreamsKeepsTheOptionsClaims(t *testing.T) {
	got := meshClaimedStreams([]uint64{777, streams.StreamSwarm})
	for _, id := range []uint64{777, streams.StreamGossip, streams.StreamReconcile, streams.StreamSwarm, streams.StreamMeshDB} {
		if !slices.Contains(got, id) {
			t.Fatalf("claims %v leave out %d", got, id)
		}
	}
	n := 0
	for _, id := range got {
		if id == streams.StreamSwarm {
			n++
		}
	}
	if n != 1 {
		t.Fatalf("claims %v list stream %d %d times", got, streams.StreamSwarm, n)
	}
}
