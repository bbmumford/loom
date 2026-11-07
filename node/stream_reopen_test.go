/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"errors"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/adapter"
	"github.com/ORBTR/aether/rpc/pb"
	"github.com/bbmumford/loom/core/streams"
	"github.com/bbmumford/loom/pkg/dispatch"
	"github.com/bbmumford/swarm"
)

// reopenPair is two byte-stream sessions over a real TCP connection: A is vl1_aaa,
// the lower NodeID, B vl1_bbb.
func reopenPair(t *testing.T) (*adapter.TCPSession, *adapter.TCPSession) {
	t.Helper()
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
	a, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	b := <-accepted
	opts := aether.DefaultSessionOptions()
	sa := adapter.NewTCPSession(a, "vl1_aaa", "vl1_bbb", aether.ProtoTCP, opts)
	sb := adapter.NewTCPSession(b, "vl1_bbb", "vl1_aaa", aether.ProtoTCP, opts)
	t.Cleanup(func() {
		_ = sa.Close()
		_ = sb.Close()
	})
	return sa, sb
}

// reopenStreams opens cfg's ID on opener and claims it on claimer.
func reopenStreams(t *testing.T, opener, claimer aether.Session, cfg aether.StreamConfig) (aether.Stream, aether.Stream) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	claimed := make(chan aether.Stream, 1)
	go func() {
		st, _ := claimer.AcceptStreamByID(ctx, cfg.StreamID)
		claimed <- st
	}()
	st, err := opener.OpenStream(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	peer := <-claimed
	if peer == nil {
		t.Fatalf("stream %d was not claimed", cfg.StreamID)
	}
	return st, peer
}

// floodToOverrun sends from st until its Send names the peer's overrun, with the
// peer's consumer not yet reading.
func floodToOverrun(t *testing.T, st aether.Stream) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	msg := make([]byte, 64*1024)
	for range 200 {
		if err := st.Send(ctx, msg); err != nil {
			if !errors.Is(err, aether.ErrStreamOverrun) {
				t.Fatalf("Send err = %v, want overrun", err)
			}
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("the flood never overran the queue")
}

// bidiPair builds a bidi on each end of an overrun Stream 1. A opens, B claims.
func bidiPair(t *testing.T, sa, sb aether.Session, st, peer aether.Stream) (*BidiRPC, *BidiRPC) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	ra := &streamReopener{session: sa, opens: true, cfg: rpcStreamConfig()}
	rb := &streamReopener{session: sb, opens: false, cfg: rpcStreamConfig()}
	ba := newReopeningBidiRPC(st, "vl1_bbb", NewRPCServer(nil), "tcp", "s", "", func() (aether.Stream, error) { return ra.reopen(ctx) })
	bb := newReopeningBidiRPC(peer, "vl1_aaa", NewRPCServer(nil), "tcp", "s", "", func() (aether.Stream, error) { return rb.reopen(ctx) })
	return ba, bb
}

// callUntilAnswered calls until the bidi answers. Every failed attempt on the way
// must have been unsent, so a caller can take its dynamic stream.
func callUntilAnswered(t *testing.T, b *BidiRPC, name string) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		resp, err := b.Call(ctx, &pb.RPCRequest{Handler: "no.Such"})
		cancel()
		if err == nil && resp != nil && resp.Error != "bidi stream closed" {
			return
		}
		if err != nil && !dispatch.Unsent(err) && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("%s: a call during the reopen failed as sent: %v", name, err)
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("%s: Stream 1 carried no call within the deadline", name)
}

// A typed reset of Stream 1 is followed by its next generation on both ends, and a
// call each way crosses it: the session stays up, so the bidi does.
func TestBidiReopensStream1AfterATypedReset(t *testing.T) {
	sa, sb := reopenPair(t)
	st, peer := reopenStreams(t, sa, sb, rpcStreamConfig())
	floodToOverrun(t, st)
	ba, bb := bidiPair(t, sa, sb, st, peer)
	callUntilAnswered(t, ba, "opener to claimer")
	callUntilAnswered(t, bb, "claimer to opener")
	if !ba.IsAlive() || !bb.IsAlive() {
		t.Fatalf("alive: opener %v claimer %v", ba.IsAlive(), bb.IsAlive())
	}
}

// The overrun on the opener's own queue reopens just the same: the claimer's flood
// ends the stream, and the side that opens it is the side whose consumer read last.
func TestBidiReopensWhenTheOpenerSideOverran(t *testing.T) {
	sa, sb := reopenPair(t)
	st, peer := reopenStreams(t, sa, sb, rpcStreamConfig())
	if err := st.Send(context.Background(), []byte{msgTypeRequest, 1}); err != nil {
		t.Fatal(err)
	}
	floodToOverrun(t, peer)
	ba, bb := bidiPair(t, sa, sb, st, peer)
	callUntilAnswered(t, ba, "opener to claimer")
	callUntilAnswered(t, bb, "claimer to opener")
}

// An ordinary end of the stream ends the bidi: only a typed reset leaves a session
// worth reopening on, and nothing about a plain end says the peer wants another.
func TestBidiEndsOnAnOrdinaryStreamEnd(t *testing.T) {
	sa, sb := reopenPair(t)
	st, peer := reopenStreams(t, sa, sb, rpcStreamConfig())
	var reopened atomic.Bool
	b := newReopeningBidiRPC(st, "vl1_bbb", NewRPCServer(nil), "tcp", "s", "", func() (aether.Stream, error) {
		reopened.Store(true)
		return nil, io.EOF
	})
	if err := peer.Reset(aether.ResetCancel); err != nil {
		t.Fatal(err)
	}
	reopenWaitFor(t, "the bidi did not end on a plain RESET", func() bool { return !b.IsAlive() })
	if reopened.Load() {
		t.Fatal("the bidi reopened after an ordinary end")
	}
}

// A bidi with no reopen ends with its typed reset, as any ad-hoc bidi does.
func TestABidiWithNoReopenEndsOnATypedReset(t *testing.T) {
	sa, sb := reopenPair(t)
	st, peer := reopenStreams(t, sa, sb, rpcStreamConfig())
	floodToOverrun(t, st)
	b := NewBidiRPC(st, "vl1_bbb", NewRPCServer(nil), "tcp", "s")
	_ = peer
	reopenWaitFor(t, "the bidi did not end", func() bool { return !b.IsAlive() })
}

// A reopen that ends with the session ends the bidi: IsAlive goes false for
// CallViaBidi's eviction, and nothing keeps waiting.
func TestBidiEndsWhenItsReopenEnds(t *testing.T) {
	sa, sb := reopenPair(t)
	st, peer := reopenStreams(t, sa, sb, rpcStreamConfig())
	floodToOverrun(t, st)
	ba, _ := bidiPair(t, sa, sb, st, peer)
	_ = sa.Close()
	_ = sb.Close()
	reopenWaitFor(t, "the bidi outlived its session", func() bool { return !ba.IsAlive() })
}

// typedEndStream parks Receive until end is closed, then answers err.
type typedEndStream struct {
	bidiStream
	end chan struct{}
	err error
}

func (s *typedEndStream) Receive(ctx context.Context) ([]byte, error) {
	select {
	case <-s.end:
		return nil, s.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// A call pending when the stream resets is answered at once and stamped as an unknown
// outcome, and a call made during the reopen is unsent, so it can take a dynamic stream.
func TestPendingCallsFailAndNewCallsAreUnsentDuringAReopen(t *testing.T) {
	st := &typedEndStream{end: make(chan struct{}), err: aether.ErrStreamOverrun}
	release := make(chan struct{})
	defer close(release)
	b := newReopeningBidiRPC(st, "vl1_bbb", NewRPCServer(nil), "tcp", "s", "", func() (aether.Stream, error) {
		<-release
		return nil, io.EOF
	})
	answered := make(chan *pb.RPCResponse, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		resp, _ := b.Call(ctx, &pb.RPCRequest{Handler: "x"})
		answered <- resp
	}()
	reopenWaitFor(t, "the call did not reach the pending map", func() bool { return b.InflightCalls() == 1 })
	time.Sleep(50 * time.Millisecond)
	close(st.end)
	select {
	case resp := <-answered:
		if resp == nil || resp.Error != "bidi stream closed" {
			t.Fatalf("pending call answered %+v", resp)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the pending call was not answered")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := b.Call(ctx, &pb.RPCRequest{Handler: "x"})
	if !dispatch.Unsent(err) || !errors.Is(err, errBidiReopening) {
		t.Fatalf("a call during the reopen err = %v", err)
	}
	if !b.IsAlive() {
		t.Fatal("a bidi reopening reads as dead, and CallViaBidi would evict it")
	}
}

func reopenWaitFor(t *testing.T, msg string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal(msg)
}

// swarmPair attaches a swarm transport to each end of a stream that overran.
func swarmPair(t *testing.T) (ta, tb *MeshSwarmTransport, sa, sb *adapter.TCPSession, st aether.Stream, peer aether.Stream) {
	t.Helper()
	sa, sb = reopenPair(t)
	st, peer = reopenStreams(t, sa, sb, swarmStreamConfig())
	ta = NewMeshSwarmTransport(swarm.NodeID("vl1_aaa"))
	tb = NewMeshSwarmTransport(swarm.NodeID("vl1_bbb"))
	t.Cleanup(ta.Stop)
	t.Cleanup(tb.Stop)
	return ta, tb, sa, sb, st, peer
}

// A typed reset of the swarm stream takes the peer out while the stream is gone and
// brings it back on the next generation, with frames crossing it both ways. The reopen
// callback fires for the anti-entropy probe that repairs what the reset lost.
func TestSwarmTransportReopensAfterATypedReset(t *testing.T) {
	ta, tb, sa, sb, st, peer := swarmPair(t)
	var leftA, joinedA, reopenedA, framesAtB, framesAtA atomic.Int64
	ta.OnPeerLeave(func(swarm.NodeID) { leftA.Add(1) })
	ta.OnPeerJoin(func(swarm.NodeID) { joinedA.Add(1) })
	ta.onStreamReopened(func(swarm.NodeID) { reopenedA.Add(1) })
	ta.OnReceive(func(swarm.NodeID, []byte) { framesAtA.Add(1) })
	tb.OnReceive(func(_ swarm.NodeID, f []byte) {
		if len(f) == 1 {
			framesAtB.Add(1)
		}
	})
	floodToOverrun(t, st)
	ta.AcceptPeer("vl1_bbb", sa, st)
	tb.AcceptPeer("vl1_aaa", sb, peer)

	reopenWaitFor(t, "the transport never brought the peer back", func() bool {
		return reopenedA.Load() == 1 && len(ta.Peers()) == 1 && len(tb.Peers()) == 1
	})
	if leftA.Load() == 0 {
		t.Fatal("the peer stayed in the engine with no stream to carry a frame")
	}
	if joinedA.Load() < 2 {
		t.Fatalf("joined %d times, want the attach and the reopen", joinedA.Load())
	}
	reopenWaitFor(t, "a frame did not cross the new stream to the claimer", func() bool {
		_ = ta.Send("vl1_bbb", []byte("x"))
		return framesAtB.Load() > 0
	})
	reopenWaitFor(t, "a frame did not cross the new stream to the opener", func() bool {
		before := framesAtA.Load()
		_ = tb.Send("vl1_aaa", []byte("y"))
		return framesAtA.Load() > before
	})
}

// heldOpenSession holds OpenStream until release closes, so a reopen stays under way.
type heldOpenSession struct {
	aether.Session
	release chan struct{}
}

func (s *heldOpenSession) IsClosed() bool { return false }
func (s *heldOpenSession) OpenStream(ctx context.Context, cfg aether.StreamConfig) (aether.Stream, error) {
	select {
	case <-s.release:
		return &fakeStream{id: cfg.StreamID, frames: make(chan []byte, 1)}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// While the swarm stream is being reopened the peer is no peer: Peers leaves it out
// and Send and Broadcast answer ErrPeerNotRegistered. It comes back with the stream.
func TestASwarmPeerIsNotAPeerWhileItsStreamReopens(t *testing.T) {
	sess := &heldOpenSession{release: make(chan struct{})}
	tp := NewMeshSwarmTransport(swarm.NodeID("a"))
	defer tp.Stop()
	st := &typedEndStream{end: make(chan struct{}), err: aether.ErrStreamOverrun}
	tp.AcceptPeer("p", sess, st)
	if len(tp.Peers()) != 1 {
		t.Fatalf("peers = %v before the reset", tp.Peers())
	}
	close(st.end)
	reopenWaitFor(t, "the peer stayed listed with no stream", func() bool { return len(tp.Peers()) == 0 })
	if err := tp.Send("p", []byte("x")); !errors.Is(err, ErrPeerNotRegistered) {
		t.Fatalf("Send during the reopen err = %v", err)
	}
	if err := tp.Broadcast([]byte("x")); !errors.Is(err, ErrPeerNotRegistered) {
		t.Fatalf("Broadcast during the reopen err = %v", err)
	}
	close(sess.release)
	reopenWaitFor(t, "the peer did not come back with its stream", func() bool { return len(tp.Peers()) == 1 })
	if err := tp.Send("p", []byte("x")); err != nil {
		t.Fatalf("Send after the reopen err = %v", err)
	}
}

// A plain end of the swarm stream tears the peer down, with no reopen.
func TestSwarmTransportLeavesOnAnOrdinaryStreamEnd(t *testing.T) {
	ta, _, sa, _, st, peer := swarmPair(t)
	var reopened atomic.Int64
	ta.onStreamReopened(func(swarm.NodeID) { reopened.Add(1) })
	ta.AcceptPeer("vl1_bbb", sa, st)
	if err := peer.Reset(aether.ResetCancel); err != nil {
		t.Fatal(err)
	}
	reopenWaitFor(t, "the peer stayed after a plain RESET", func() bool { return len(ta.Peers()) == 0 })
	if err := ta.Send("vl1_bbb", []byte("x")); !errors.Is(err, ErrPeerNotRegistered) {
		t.Fatalf("Send err = %v", err)
	}
	// Past the opener's first wait, an entry still held would be a reopen under way.
	time.Sleep(2 * streamReopenMin)
	ta.mu.RLock()
	_, held := ta.peers["vl1_bbb"]
	ta.mu.RUnlock()
	if held || reopened.Load() != 0 {
		t.Fatalf("after an ordinary end: entry held %v, reopened %d", held, reopened.Load())
	}
}

// The opener's first OPEN can reach a peer whose consumer has not claimed the ID yet:
// the unclaimed-stream path answers RESET(ResetRefused), and the opener keeps opening
// until the claim is there.
func TestSwarmStreamOpensAgainWhenRefusedBeforeItsClaim(t *testing.T) {
	sa, sb := reopenPair(t)
	ta := NewMeshSwarmTransport(swarm.NodeID("vl1_aaa"))
	tb := NewMeshSwarmTransport(swarm.NodeID("vl1_bbb"))
	t.Cleanup(ta.Stop)
	t.Cleanup(tb.Stop)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	server := NewRPCServer(nil)
	go func() {
		for {
			st, err := sb.AcceptStream(ctx)
			if err != nil {
				return
			}
			go server.serveUnclaimedStream(ctx, st, "vl1_aaa")
		}
	}()
	var frames atomic.Int64
	tb.OnReceive(func(swarm.NodeID, []byte) { frames.Add(1) })
	if err := ta.RegisterPeer(ctx, "vl1_bbb", sa); err != nil {
		t.Fatal(err)
	}
	// Past aether's backlog grace, so the OPEN falls through to the accept loop.
	time.Sleep(300 * time.Millisecond)
	if err := tb.RegisterPeer(ctx, "vl1_aaa", sb); err != nil {
		t.Fatal(err)
	}
	reopenWaitFor(t, "no frame crossed once the claim was armed", func() bool {
		_ = ta.Send("vl1_bbb", []byte("x"))
		return frames.Load() > 0
	})
}

// A setup stream that reaches the accept loop is refused rather than served as
// one-way RPC, whose parse of its first frame would fail and whose opener would
// believe the stream was up.
func TestAnUnclaimedSetupStreamIsRefused(t *testing.T) {
	for _, id := range []uint64{streams.StreamGossip, streams.StreamRPC, streams.StreamKeepalive, streams.StreamControl, streams.StreamReconcile} {
		sa, sb := reopenPair(t)
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		st, err := sa.OpenStream(ctx, aether.StreamConfig{StreamID: id, Reliability: aether.ReliableOrdered})
		if err != nil {
			t.Fatal(err)
		}
		inbound, err := sb.AcceptStream(ctx)
		if err != nil {
			t.Fatal(err)
		}
		NewRPCServer(nil).serveUnclaimedStream(ctx, inbound, "vl1_aaa")
		if _, err := st.Receive(ctx); !errors.Is(err, aether.ErrStreamRefused) {
			t.Fatalf("stream %d: the opener read %v, want a refusal", id, err)
		}
		cancel()
	}
}

// Registering a peer again on the session it is attached on keeps the stream it has,
// where a second OpenStream of the ID would be refused.
func TestRegisterPeerTwiceOnOneSessionKeepsItsStream(t *testing.T) {
	sa, sb := reopenPair(t)
	ta := NewMeshSwarmTransport(swarm.NodeID("vl1_aaa"))
	t.Cleanup(ta.Stop)
	_ = sb
	ctx := context.Background()
	if err := ta.RegisterPeer(ctx, "vl1_bbb", sa); err != nil {
		t.Fatal(err)
	}
	ta.mu.RLock()
	first := ta.peers["vl1_bbb"].stream
	ta.mu.RUnlock()
	if err := ta.RegisterPeer(ctx, "vl1_bbb", sa); err != nil {
		t.Fatalf("second RegisterPeer: %v", err)
	}
	ta.mu.RLock()
	second := ta.peers["vl1_bbb"].stream
	ta.mu.RUnlock()
	if first != second {
		t.Fatal("the second RegisterPeer replaced the stream")
	}
}

// reopenEnded brings back the stream that ended and leaves the one still open.
func TestGossipStreamsReopenOnlyWhatEnded(t *testing.T) {
	sa, sb := reopenPair(t)
	g, gPeer := reopenStreams(t, sa, sb, gossipStreamConfig())
	rc, rcPeer := reopenStreams(t, sa, sb, reconcileStreamConfig())
	floodToOverrun(t, g)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for {
		if _, err := gPeer.Receive(ctx); err != nil {
			break
		}
	}
	opener := gossipStreams{
		gossip: g, reconcile: rc,
		gossipReopener:    streamReopener{session: sa, opens: true, cfg: gossipStreamConfig()},
		reconcileReopener: streamReopener{session: sa, opens: true, cfg: reconcileStreamConfig()},
	}
	claimer := gossipStreams{
		gossip: gPeer, reconcile: rcPeer,
		gossipReopener:    streamReopener{session: sb, opens: false, cfg: gossipStreamConfig()},
		reconcileReopener: streamReopener{session: sb, opens: false, cfg: reconcileStreamConfig()},
	}
	claimed := make(chan bool, 1)
	go func() { claimed <- claimer.reopenEnded(ctx, "vl1_aaa") }()
	if !opener.reopenEnded(ctx, "vl1_bbb") {
		t.Fatal("the opener's reopen ended")
	}
	if !<-claimed {
		t.Fatal("the claimer's reopen ended")
	}
	if opener.reconcile != rc || claimer.reconcile != rcPeer {
		t.Fatal("a stream still open was replaced")
	}
	if opener.gossip == g || claimer.gossip == gPeer {
		t.Fatal("the ended stream was not replaced")
	}
	if err := opener.gossip.Send(ctx, []byte("x")); err != nil {
		t.Fatal(err)
	}
	if got, err := claimer.gossip.Receive(ctx); err != nil || string(got) != "x" {
		t.Fatalf("Receive on the new generation: %q, %v", got, err)
	}
}

// The reopener's error ends a reopen at once when the session has gone or OpenStream
// answers something that will not clear; a refusal that clears is opened through.
type scriptedOpenSession struct {
	aether.Session
	errs   []error
	opened atomic.Int32
}

func (s *scriptedOpenSession) IsClosed() bool { return false }
func (s *scriptedOpenSession) OpenStream(context.Context, aether.StreamConfig) (aether.Stream, error) {
	n := int(s.opened.Add(1)) - 1
	if n < len(s.errs) {
		return nil, s.errs[n]
	}
	return &bidiStream{}, nil
}

func TestReopenOpensThroughARefusalThatClears(t *testing.T) {
	s := &scriptedOpenSession{errs: []error{aether.ErrDuplicateStreamID, aether.ErrStreamCapExhausted, aether.ErrStreamCreditRefused}}
	r := streamReopener{session: s, opens: true, cfg: rpcStreamConfig()}
	st, err := r.reopen(context.Background())
	if err != nil || st == nil {
		t.Fatalf("reopen = %v, %v", st, err)
	}
	if got := s.opened.Load(); got != 4 {
		t.Fatalf("%d opens, want 4", got)
	}
	if r.wait <= streamReopenMin {
		t.Fatalf("the wait stayed at %v across refusals", r.wait)
	}

	// A generation that lived past the healthy age starts the wait over; one
	// refused inside it keeps growing.
	grown := r.wait
	if _, err := r.reopen(context.Background()); err != nil {
		t.Fatal(err)
	}
	if r.wait <= grown {
		t.Fatalf("a reopen soon after the last left the wait at %v from %v", r.wait, grown)
	}
	r.opened = time.Now().Add(-streamReopenHealthy - time.Second)
	r.wait = 16 * streamReopenMin
	ctx, cancel := context.WithTimeout(context.Background(), 2*streamReopenMin)
	defer cancel()
	if _, err := r.reopen(ctx); err != nil {
		t.Fatalf("the reopen after a healthy generation waited past %v: %v", 2*streamReopenMin, err)
	}
}

func TestReopenStopsOnARefusalThatWillNotClear(t *testing.T) {
	final := errors.New("wire versions refused")
	s := &scriptedOpenSession{errs: []error{final}}
	r := streamReopener{session: s, opens: true, cfg: rpcStreamConfig()}
	if _, err := r.reopen(context.Background()); !errors.Is(err, final) {
		t.Fatalf("err = %v", err)
	}
	if got := s.opened.Load(); got != 1 {
		t.Fatalf("%d opens", got)
	}
}

// The keepalive stream runs no FEC: nothing is sent through it.
func TestKeepaliveStreamRunsNoFEC(t *testing.T) {
	if got := keepaliveStreamConfig().FECLevel; got != 0 {
		t.Fatalf("keepalive FEC level = %d", got)
	}
}

// aether_congestion_throttle_hits reads only Noise sessions: the probe in
// peer_connections.go asserts this interface, which a byte-stream session lacks.
func TestTheThrottleMetricReadsOnlyNoiseSessions(t *testing.T) {
	type throttled interface {
		Throttle() *aether.CongestionThrottle
	}
	var _ throttled = (*adapter.NoiseSession)(nil)
	sa, _ := reopenPair(t)
	if _, ok := any(sa).(throttled); ok {
		t.Fatal("a byte-stream session carries a throttle")
	}
}

// forwardSession counts how the forwarder's fallback uses its streams.
type forwardSession struct {
	aether.Session
	ids    []uint64
	resets atomic.Int32
}

func (s *forwardSession) RemoteNodeID() aether.NodeID { return "vl1_bbb" }
func (s *forwardSession) OpenStream(_ context.Context, cfg aether.StreamConfig) (aether.Stream, error) {
	s.ids = append(s.ids, cfg.StreamID)
	return &forwardStream{id: cfg.StreamID, s: s}, nil
}

type forwardStream struct {
	aether.Stream
	id uint64
	s  *forwardSession
	in bool
}

func (f *forwardStream) StreamID() uint64                   { return f.id }
func (f *forwardStream) IsOpen() bool                       { return !f.in }
func (f *forwardStream) Send(context.Context, []byte) error { return nil }
func (f *forwardStream) Close() error                       { panic("the fallback must reset") }
func (f *forwardStream) Reset(reason aether.ResetReason) error {
	f.s.resets.Add(1)
	f.in = true
	return nil
}
func (f *forwardStream) Receive(context.Context) ([]byte, error) {
	return pb.MarshalResponse(&pb.RPCResponse{Success: true})
}

// The forwarder's fallback draws from the session's dynamic ID table and resets its
// stream: its IDs stay in the dynamic band however many forwards run.
func TestTheForwardFallbackStaysInTheDynamicBand(t *testing.T) {
	sess := &forwardSession{}
	f := &runtimeForwarder{rt: &Runtime{}}
	const calls = 3 * int(streams.ApplicationBand)
	for range calls {
		if _, err := f.callOverMeshSession(context.Background(), sess, &pb.RPCRequest{Handler: "x"}); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range sess.ids {
		if id < streams.DynamicRPCBase || id >= streams.ApplicationBand {
			t.Fatalf("a forward opened stream %d, outside the dynamic band", id)
		}
	}
	if int(sess.resets.Load()) != calls {
		t.Fatalf("%d resets of %d forwards", sess.resets.Load(), calls)
	}
}

// failingSendStream fails Send and records the reset that follows.
type failingSendStream struct {
	bidiStream
	once   atomic.Bool
	resets atomic.Int32
}

func (s *failingSendStream) Send(context.Context, []byte) error { return errors.New("write failed") }
func (s *failingSendStream) Reset(aether.ResetReason) error     { s.resets.Add(1); return nil }
func (s *failingSendStream) Receive(context.Context) ([]byte, error) {
	if s.once.CompareAndSwap(false, true) {
		return pb.MarshalRequest(&pb.RPCRequest{Id: "1", Handler: "no.Such"})
	}
	return nil, io.EOF
}

// A dynamic-RPC server that cannot answer resets the stream on its way out: nothing
// reads it afterwards, and its unread messages would hold the opener's credit.
func TestServeMeshStreamResetsAStreamItCannotAnswer(t *testing.T) {
	st := &failingSendStream{}
	NewRPCServer(nil).ServeMeshStream(context.Background(), st, "vl1_aaa")
	if st.resets.Load() != 1 {
		t.Fatalf("resets = %d", st.resets.Load())
	}
}

// blockingSendStream's Send waits for its context, as a stalled session's does.
type blockingSendStream struct{ fakeStream }

func (s *blockingSendStream) Send(ctx context.Context, _ []byte) error {
	<-ctx.Done()
	return ctx.Err()
}

// One stalled peer holds a swarm Send for a bounded time, because the engine fans out
// with repeated Send.
func TestSwarmSendIsBoundedOnAStalledStream(t *testing.T) {
	if testing.Short() {
		t.Skip("waits out swarmSendTimeout")
	}
	tp := NewMeshSwarmTransport(swarm.NodeID("self"))
	defer tp.Stop()
	st := &blockingSendStream{fakeStream: fakeStream{id: 100, frames: make(chan []byte, 1)}}
	tp.AcceptPeer("p", nil, st)
	start := time.Now()
	err := tp.Send("p", []byte("x"))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Send err = %v", err)
	}
	if took := time.Since(start); took < swarmSendTimeout || took > swarmSendTimeout+10*time.Second {
		t.Fatalf("Send returned after %v, want about %v", took, swarmSendTimeout)
	}
}

// closingResetStream records Close and Reset, ending Receive at once.
type closingResetStream struct {
	fakeStream
	closes, resets atomic.Int32
}

func (s *closingResetStream) Close() error                   { s.closes.Add(1); return nil }
func (s *closingResetStream) Reset(aether.ResetReason) error { s.resets.Add(1); return nil }
func (s *closingResetStream) Receive(ctx context.Context) ([]byte, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// A swarm peer's teardown stops reading its stream, so it leaves through
// CloseAbandoned: closed, then reset once read off, the reset being what returns the
// peer's credit for unread messages.
func TestSwarmTeardownAbandonsTheStream(t *testing.T) {
	if testing.Short() {
		t.Skip("waits out the abandoned drain")
	}
	tp := NewMeshSwarmTransport(swarm.NodeID("self"))
	defer tp.Stop()
	st := &closingResetStream{fakeStream: fakeStream{id: 100}}
	tp.AcceptPeer("p", nil, st)
	tp.UnregisterPeer("p")
	reopenWaitFor(t, "the stream was not reset after its close", func() bool {
		return st.closes.Load() >= 1 && st.resets.Load() >= 1
	})
}

// A handler's stream leaves the same way when the handler returns.
func TestAHandlerStreamCloseAbandonsTheStream(t *testing.T) {
	st := &closingResetStream{fakeStream: fakeStream{id: 1000}}
	m := &meshMessageStream{ctx: context.Background(), stream: st}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	reopenWaitFor(t, "the stream was not reset after its close", func() bool {
		return st.closes.Load() == 1 && st.resets.Load() == 1
	})
}

// noiseRealmConn gives a NoiseSession over a plain conn the transport evidence the
// Noise transport's connection states: the chosen wire versions and a bound realm.
type noiseRealmConn struct {
	net.Conn
	chosen aether.NegotiatedWireVersions
}

func (c noiseRealmConn) WireVersions() aether.NegotiatedWireVersions { return c.chosen }
func (c noiseRealmConn) RealmID() (uint32, bool)                     { return 0, false }

// noisePair is two NoiseSessions over a real TCP connection: A is vl1_aaa, B vl1_bbb.
func noisePair(t *testing.T) (*adapter.NoiseSession, *adapter.NoiseSession) {
	t.Helper()
	chosen, err := aether.ChooseWireVersions(aether.AetherWireVersions(), aether.AetherWireVersions())
	if err != nil {
		t.Fatal(err)
	}
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
	a, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	b := <-accepted
	sa := adapter.NewNoiseSession(noiseRealmConn{a, chosen}, "vl1_aaa", "vl1_bbb", aether.DefaultSessionOptions())
	sb := adapter.NewNoiseSession(noiseRealmConn{b, chosen}, "vl1_bbb", "vl1_aaa", aether.DefaultSessionOptions())
	t.Cleanup(func() {
		_ = sa.Close()
		_ = sb.Close()
	})
	return sa, sb
}

// A typed reset of Stream 1 on a Noise session reads as on a byte-stream one: the
// RESET's reason reaches Receive as aether.ErrStreamOverrun, and the opener's bidi
// opens the next generation, whose request the bidi answers.
func TestBidiOnANoiseSessionReopensAsOpener(t *testing.T) {
	sa, sb := noisePair(t)
	st, peer := reopenStreams(t, sa, sb, rpcStreamConfig())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r := &streamReopener{session: sa, opens: true, cfg: rpcStreamConfig()}
	b := newReopeningBidiRPC(st, "vl1_bbb", NewRPCServer(nil), "noise-udp", "s", "", func() (aether.Stream, error) { return r.reopen(ctx) })
	if err := peer.Reset(aether.ResetFlowCtrl); err != nil {
		t.Fatal(err)
	}
	fresh := noiseClaim(t, sb, streams.StreamRPC)
	noiseAnswered(t, fresh)
	if !b.IsAlive() {
		t.Fatal("the bidi ended on a typed reset")
	}
}

// The claimer's bidi claims the opener's next OPEN. The opener reopens through its
// own streamReopener, as BidiRPC's readLoop does: the side that sent the RESET has
// the ID refused (aether.ErrDuplicateStreamID) until the peer acknowledges the RESET
// and the quiet time after it ends, and reopen waits that out.
func TestBidiOnANoiseSessionReopensAsClaimer(t *testing.T) {
	sa, sb := noisePair(t)
	st, peer := reopenStreams(t, sa, sb, rpcStreamConfig())
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	r := &streamReopener{session: sb, opens: false, cfg: rpcStreamConfig()}
	b := newReopeningBidiRPC(peer, "vl1_aaa", NewRPCServer(nil), "noise-udp", "s", "", func() (aether.Stream, error) { return r.reopen(ctx) })
	if err := st.Reset(aether.ResetFlowCtrl); err != nil {
		t.Fatal(err)
	}
	reopenWaitFor(t, "the claimer did not see the reset", func() bool { return b.currentStream() == nil })
	opener := &streamReopener{session: sa, opens: true, cfg: rpcStreamConfig()}
	fresh, err := opener.reopen(ctx)
	if err != nil {
		t.Fatal(err)
	}
	noiseAnswered(t, fresh)
}

// A Noise stream ended by a typed reset reads as not open, which is what makes
// gossipStreams.reopenEnded act on it.
func TestAReopenedGossipStreamOnANoiseSession(t *testing.T) {
	sa, sb := noisePair(t)
	st, peer := reopenStreams(t, sa, sb, gossipStreamConfig())
	if err := peer.Reset(aether.ResetFlowCtrl); err != nil {
		t.Fatal(err)
	}
	reopenWaitFor(t, "the opener's stream stayed open past the reset", func() bool { return !st.IsOpen() })
	if peer.IsOpen() {
		t.Fatal("the resetting side's stream stayed open")
	}
	g := gossipStreams{
		gossip: st, reconcile: &bidiStream{},
		gossipReopener: streamReopener{session: sa, opens: true, cfg: gossipStreamConfig()},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if !g.reopenEnded(ctx, "vl1_bbb") {
		t.Fatal("reopen ended")
	}
	if g.gossip == st || !g.gossip.IsOpen() {
		t.Fatal("no new generation")
	}
}

func noiseClaim(t *testing.T, s aether.Session, id uint64) aether.Stream {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	st, err := s.AcceptStreamByID(ctx, id)
	if err != nil {
		t.Fatalf("claiming %d: %v", id, err)
	}
	return st
}

func noiseAnswered(t *testing.T, st aether.Stream) {
	t.Helper()
	if err := meshCallErr(context.Background(), st); err != nil {
		t.Fatalf("the new generation answered no request: %v", err)
	}
}
