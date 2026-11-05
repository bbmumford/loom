/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/adapter"
	"github.com/ORBTR/aether/rpc/pb"
	"github.com/bbmumford/ledger/cache"
	"github.com/bbmumford/loom/core/directory/gossip"
	"github.com/bbmumford/loom/core/streams"
)

// meshReopenPeer runs AcceptMeshConnection on a real runtime over a real byte-stream
// session and returns the test's end of that session: the peer the runtime's gossip,
// bidi and setup streams talk to.
func meshReopenPeer(t *testing.T, runtimeInitiates bool) (*adapter.TCPSession, context.Context) {
	t.Helper()
	rt, _ := startWireTestRuntime(t)
	other := wireTestRuntime(t)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		c, _ := ln.Accept()
		accepted <- c
	}()
	near, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	far := <-accepted
	opts := aether.DefaultSessionOptions()
	opts.WireVersions = meshWireVersions(nil)
	session := adapter.NewTCPSession(near, rt.identity.NodeID, other.identity.NodeID, aether.ProtoTCP, opts)
	peer := adapter.NewTCPSession(far, other.identity.NodeID, rt.identity.NodeID, aether.ProtoTCP, opts)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(func() {
		cancel()
		_ = session.Close()
		_ = peer.Close()
	})
	go func() {
		_ = rt.connMgr.AcceptMeshConnection(ctx, AcceptMeshConnectionOpts{
			NodeID: string(other.identity.NodeID), Proto: ProtoWebSocket, Session: session, IsInitiator: runtimeInitiates,
		})
	}()
	return peer, ctx
}

func meshClaim(t *testing.T, ctx context.Context, s aether.Session, id uint64) aether.Stream {
	t.Helper()
	wait, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	st, err := s.AcceptStreamByID(wait, id)
	if err != nil {
		t.Fatalf("claiming stream %d: %v", id, err)
	}
	return st
}

// meshCallErr sends one bidi request on st and waits for its answer, which only the
// runtime's BidiRPC on the stream's new generation can write.
func meshCallErr(ctx context.Context, st aether.Stream) error {
	wire, err := pb.MarshalRequest(&pb.RPCRequest{Id: "probe", Handler: "no.Such"})
	if err != nil {
		return err
	}
	wait, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err := st.Send(wait, append([]byte{msgTypeRequest}, wire...)); err != nil {
		return err
	}
	for {
		data, err := st.Receive(wait)
		if err != nil {
			return err
		}
		if len(data) > 0 && data[0] == msgTypeResponse {
			return nil
		}
	}
}

func meshCall(t *testing.T, ctx context.Context, st aether.Stream) {
	t.Helper()
	if err := meshCallErr(ctx, st); err != nil {
		t.Fatalf("no answer on the stream: %v", err)
	}
}

// meshReopenAndUse opens cfg as the session's opener and runs use on it, opening
// again while the runtime refuses: an OPEN that comes before its consumer claims the
// ID, past aether's backlog grace, reaches the accept loop, whose refusal is the
// opener's signal to open again.
func meshReopenAndUse(t *testing.T, ctx context.Context, peer aether.Session, cfg aether.StreamConfig, use func(aether.Stream) error) {
	t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	for {
		st, err := peer.OpenStream(ctx, cfg)
		if err == nil {
			if err = use(st); err == nil {
				return
			}
			if !errors.Is(err, aether.ErrStreamRefused) {
				t.Fatalf("stream %d: %v", cfg.StreamID, err)
			}
		} else if !errors.Is(err, aether.ErrDuplicateStreamID) {
			t.Fatalf("OpenStream(%d): %v", cfg.StreamID, err)
		}
		if time.Now().After(deadline) {
			t.Fatalf("stream %d was never claimed again", cfg.StreamID)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// With the runtime as the session's opener, a typed reset of Stream 1 and of the
// gossip stream is followed by the runtime's own OPEN of each.
func TestAcceptMeshConnectionReopensItsStreamsAsOpener(t *testing.T) {
	peer, ctx := meshReopenPeer(t, true)
	setup := map[uint64]aether.Stream{}
	for _, id := range []uint64{streams.StreamGossip, streams.StreamRPC, streams.StreamKeepalive, streams.StreamControl, streams.StreamReconcile} {
		setup[id] = meshClaim(t, ctx, peer, id)
	}

	if err := setup[streams.StreamRPC].Reset(aether.ResetFlowCtrl); err != nil {
		t.Fatal(err)
	}
	meshCall(t, ctx, meshClaim(t, ctx, peer, streams.StreamRPC))

	if err := setup[streams.StreamGossip].Reset(aether.ResetFlowCtrl); err != nil {
		t.Fatal(err)
	}
	fresh := meshClaim(t, ctx, peer, streams.StreamGossip)
	if fresh == setup[streams.StreamGossip] {
		t.Fatal("the claim returned the ended stream")
	}
}

// With the runtime as the claimer, it claims each stream again: the peer's second
// OPEN reaches the runtime's BidiRPC and its gossip responder, which answer on it.
func TestAcceptMeshConnectionReopensItsStreamsAsClaimer(t *testing.T) {
	peer, ctx := meshReopenPeer(t, false)
	open := func(cfg aether.StreamConfig) aether.Stream {
		t.Helper()
		st, err := peer.OpenStream(ctx, cfg)
		if err != nil {
			t.Fatalf("OpenStream(%d): %v", cfg.StreamID, err)
		}
		return st
	}
	g := open(gossipStreamConfig())
	rpc := open(rpcStreamConfig())
	open(keepaliveStreamConfig())
	open(aether.StreamConfig{StreamID: streams.StreamControl, Reliability: aether.ReliableOrdered, Priority: 240})
	rc := open(reconcileStreamConfig())

	// A request crossing shows the runtime reached its service loops, so the resets
	// below reach running consumers.
	meshCall(t, ctx, rpc)
	if err := rpc.Reset(aether.ResetFlowCtrl); err != nil {
		t.Fatal(err)
	}
	meshReopenAndUse(t, ctx, peer, rpcStreamConfig(), func(st aether.Stream) error { return meshCallErr(ctx, st) })

	if err := g.Reset(aether.ResetFlowCtrl); err != nil {
		t.Fatal(err)
	}
	set, ok := any(peer).(aether.WireVersionsReporter)
	if !ok {
		t.Fatal("the session reports no wire versions")
	}
	p, err := gossip.NegotiatedProfile(set.WireVersions())
	if err != nil {
		t.Fatal(err)
	}
	meshReopenAndUse(t, ctx, peer, gossipStreamConfig(), func(st aether.Stream) error {
		wait, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		_, err := gossip.GossipOverConn(wait, adapter.NewStreamConn(st, wait), cache.NewDirectoryCache(), p, nil)
		return err
	})
	if !rc.IsOpen() {
		t.Fatal("the reconcile stream ended with the gossip stream")
	}
}
