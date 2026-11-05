/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package gossip

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/adapter"
	"github.com/ORBTR/aether/flow"
	lad "github.com/bbmumford/ledger"
	"github.com/bbmumford/ledger/cache"
	"github.com/bbmumford/whisper"
	"google.golang.org/protobuf/proto"

	gossippb "github.com/bbmumford/loom/core/directory/gossip/pb"
)

// sessionPair is two byte-stream sessions over a real TCP connection.
func sessionPair(t *testing.T) (*adapter.TCPSession, *adapter.TCPSession) {
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

// streamPair opens id on sa, sends one byte so sb's accept returns, and reads it.
func streamPair(t *testing.T, sa, sb *adapter.TCPSession, id uint64) (aether.Stream, aether.Stream) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	claimed := make(chan aether.Stream, 1)
	go func() {
		st, _ := sb.AcceptStreamByID(ctx, id)
		claimed <- st
	}()
	st, err := sa.OpenStream(ctx, aether.StreamConfig{StreamID: id, Reliability: aether.ReliableOrdered, Priority: 64})
	if err != nil {
		t.Fatal(err)
	}
	peer := <-claimed
	if peer == nil {
		t.Fatalf("stream %d was not claimed", id)
	}
	return st, peer
}

// overrun floods st until its Send names the peer's overrun and returns.
func overrun(t *testing.T, st aether.Stream) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	msg := make([]byte, gossipChunkSize)
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

// drain reads st until its end and returns the end.
func drain(t *testing.T, st aether.Stream) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for {
		if _, err := st.Receive(ctx); err != nil {
			return err
		}
	}
}

type sendRecorder struct {
	aether.Stream
	failAfter int
	sends     []int
}

func (r *sendRecorder) Send(_ context.Context, b []byte) error {
	if r.failAfter > 0 && len(r.sends) == r.failAfter {
		return aether.ErrStreamOverrun
	}
	r.sends = append(r.sends, len(b))
	return nil
}

// A Write of a whole frame's body becomes Sends a session accepts: the byte-stream
// limit is the stream credit less the entry cost, the Noise one 255 fragments.
func TestAGossipConnWriteSplitsIntoBoundedSends(t *testing.T) {
	rec := &sendRecorder{}
	conn := newGossipConn(rec, context.Background())
	body := make([]byte, maxGossipPayload)
	n, err := conn.Write(body)
	if err != nil || n != len(body) {
		t.Fatalf("Write = %d, %v", n, err)
	}
	total := 0
	for _, s := range rec.sends {
		if s > gossipChunkSize {
			t.Fatalf("a Send of %d B, above %d", s, gossipChunkSize)
		}
		total += s
	}
	if total != len(body) {
		t.Fatalf("Sends carried %d of %d B", total, len(body))
	}
}

// A Send that fails part way reports the bytes the earlier ones carried.
func TestAGossipConnWriteReportsWhatLeft(t *testing.T) {
	rec := &sendRecorder{failAfter: 2}
	conn := newGossipConn(rec, context.Background())
	n, err := conn.Write(make([]byte, 5*gossipChunkSize))
	if !errors.Is(err, aether.ErrStreamOverrun) {
		t.Fatalf("err = %v", err)
	}
	if n != 2*gossipChunkSize {
		t.Fatalf("Write = %d, want %d", n, 2*gossipChunkSize)
	}
}

// Closing a gossip conn ends its stream on both ends, so the session's gossip loop
// opens a new one: a close alone leaves a half-closed stream that reads as open.
func TestClosingAGossipConnEndsItsStream(t *testing.T) {
	sa, sb := sessionPair(t)
	st, peer := streamPair(t, sa, sb, 0)
	conn := newGossipConn(st, context.Background())
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	if st.IsOpen() {
		t.Fatal("the closed conn's stream reads as open")
	}
	if !conn.streamEnded() {
		t.Fatal("streamEnded is false for an ended stream")
	}
	deadline := time.Now().Add(15 * time.Second)
	for peer.IsOpen() && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if peer.IsOpen() {
		t.Fatal("the peer's stream reads as open")
	}
}

// The largest frame fits the queue with its consumer yet to read a byte: the header
// and the body's Sends, each with the queue's entry cost, come in under the bound.
func TestTheLargestGossipFrameFitsAnUnreadQueue(t *testing.T) {
	const entryCost = 64
	chunks := maxGossipPayload / gossipChunkSize
	if maxGossipPayload%gossipChunkSize != 0 {
		chunks++
	}
	cost := int64(gossipHeaderSize+entryCost) + int64(maxGossipPayload) + int64(chunks)*entryCost
	if credit := flow.CurrentBounds().StreamCredit; cost > credit {
		t.Fatalf("a whole frame costs %d B of a queue bound at %d", cost, credit)
	}

	sa, sb := sessionPair(t)
	st, peer := streamPair(t, sa, sb, 0)
	conn := newGossipConn(st, context.Background())
	hdr := make([]byte, gossipHeaderSize)
	if _, err := conn.Write(hdr); err != nil {
		t.Fatal(err)
	}
	body := make([]byte, maxGossipPayload)
	if _, err := conn.Write(body); err != nil {
		t.Fatal(err)
	}
	// The consumer starts only once every Send has had time to arrive.
	time.Sleep(500 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	got := 0
	for got < len(hdr)+len(body) {
		b, err := peer.Receive(ctx)
		if err != nil {
			t.Fatalf("Receive after %d B: %v", got, err)
		}
		got += len(b)
	}
}

// marshalEnvelope keeps a frame inside the bound, and what it keeps decodes; records
// that do not fit stay out, and a set that does fit keeps every record.
func TestAnEnvelopeHoldsInsideTheFrameBound(t *testing.T) {
	p := testProfile(t)
	big := make([]byte, 16*1024)
	records := make([]lad.Record, 0, 200)
	for i := range 200 {
		records = append(records, lad.Record{
			Topic: lad.TopicMember, Partition: "t", NodeID: fmt.Sprintf("node-%d", i), Timestamp: seedTime, Body: big,
		})
	}
	payload, err := marshalEnvelope(p, &ExchangeMeta{}, records)
	if err != nil {
		t.Fatal(err)
	}
	if len(payload) > maxGossipPayload {
		t.Fatalf("envelope of %d B, past the bound %d", len(payload), maxGossipPayload)
	}
	var env gossippb.GossipEnvelope
	if err := proto.Unmarshal(payload, &env); err != nil {
		t.Fatal(err)
	}
	if len(env.Records) == 0 || len(env.Records) >= len(records) {
		t.Fatalf("%d of %d records kept", len(env.Records), len(records))
	}

	small, err := marshalEnvelope(p, &ExchangeMeta{}, records[:10])
	if err != nil {
		t.Fatal(err)
	}
	if err := proto.Unmarshal(small, &env); err != nil {
		t.Fatal(err)
	}
	if len(env.Records) != 10 {
		t.Fatalf("%d of 10 records kept in a set under the bound", len(env.Records))
	}
}

func TestIsFatalGossipErrorReadsTypedEnds(t *testing.T) {
	for _, err := range []error{
		aether.ErrStreamOverrun,
		fmt.Errorf("gossip: read header: %w", aether.ErrStreamRefused),
		fmt.Errorf("w: %w", aether.ErrSessionWriteFailed),
		aether.ErrSessionClosed,
		fmt.Errorf("%w: invalid magic", errGossipDesynchronised),
		net.ErrClosed,
	} {
		if !isFatalGossipError(err) {
			t.Errorf("isFatalGossipError(%v) = false", err)
		}
	}
	for _, err := range []error{
		nil,
		errInsufficientCredit,
		os.ErrDeadlineExceeded,
		flow.ErrCreditExhausted,
		context.DeadlineExceeded,
		adapter.ErrPayloadTooLarge,
	} {
		if isFatalGossipError(err) {
			t.Errorf("isFatalGossipError(%v) = true", err)
		}
	}
}

// countingReceive counts how often a stream's end is read.
type countingReceive struct {
	aether.Stream
	receives atomic.Int64
}

func (c *countingReceive) Receive(ctx context.Context) ([]byte, error) {
	c.receives.Add(1)
	return c.Stream.Receive(ctx)
}

// A responder whose gossip stream ended with a typed reset returns promptly, its
// reconcile engine with it, so the session's gossip loop can reopen the stream. It
// does not spin on the end, which every further read returns at once.
func TestAResponderLeavesAStreamATypedResetEnded(t *testing.T) {
	sa, sb := sessionPair(t)
	st, peer := streamPair(t, sa, sb, 0)
	rcOpen, rc := streamPair(t, sa, sb, 4)
	_ = rcOpen
	overrun(t, st)
	if err := drain(t, peer); !errors.Is(err, aether.ErrStreamOverrun) {
		t.Fatalf("the consumer's end = %v", err)
	}
	counted := &countingReceive{Stream: peer}
	done := make(chan struct{})
	go func() {
		defer close(done)
		RunGossipResponder(context.Background(), counted, rc, cache.NewDirectoryCache(), testProfile(t), "vl1_aaa", whisper.NewDeltaTracker(), nil)
	}()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatalf("the responder is still running after %d reads of the stream's end", counted.receives.Load())
	}
	if got := counted.receives.Load(); got > 10 {
		t.Fatalf("%d reads of an ended stream", got)
	}
}

// The reconcile stream ending ends the gossip engine too.
func TestAResponderLeavesWhenItsReconcileStreamEnds(t *testing.T) {
	sa, sb := sessionPair(t)
	gOpen, g := streamPair(t, sa, sb, 0)
	_ = gOpen
	rcOpen, rc := streamPair(t, sa, sb, 4)
	overrun(t, rcOpen)
	if err := drain(t, rc); !errors.Is(err, aether.ErrStreamOverrun) {
		t.Fatalf("the consumer's end = %v", err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		RunGossipResponder(context.Background(), g, rc, cache.NewDirectoryCache(), testProfile(t), "vl1_aaa", whisper.NewDeltaTracker(), nil)
	}()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("the responder is still running with its reconcile stream ended")
	}
}

// The initiator's loop leaves on its gossip stream's typed end, where a retry would
// fail every later exchange on it.
func TestAnInitiatorLeavesAStreamATypedResetEnded(t *testing.T) {
	sa, sb := sessionPair(t)
	st, peer := streamPair(t, sa, sb, 0)
	rcOpen, rcPeer := streamPair(t, sa, sb, 4)
	_ = rcPeer
	overrun(t, st)
	_ = drain(t, peer)
	done := make(chan struct{})
	go func() {
		defer close(done)
		RunGossipLoopWithPeer(context.Background(), st, rcOpen, cache.NewDirectoryCache(), testProfile(t), 10*time.Millisecond, "vl1_bbb", whisper.NewDeltaTracker(), nil)
	}()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("the initiator is still looping on an ended stream")
	}
}

// The initiator's loop leaves when its reconcile stream has ended, though G1 alone
// would keep it running.
func TestAnInitiatorLeavesWhenItsReconcileStreamEnds(t *testing.T) {
	sa, sb := sessionPair(t)
	st, peer := streamPair(t, sa, sb, 0)
	rcOpen, rcPeer := streamPair(t, sa, sb, 4)
	overrun(t, rcOpen)
	_ = drain(t, rcPeer)
	p := testProfile(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go runGossipResponderLoop(ctx, newGossipConn(peer, ctx), newGossipConn(&idleStream{Stream: peer}, ctx), cache.NewDirectoryCache(), p, nil, nil, whisper.NewDeltaTracker(), "vl1_aaa")
	done := make(chan struct{})
	go func() {
		defer close(done)
		RunGossipLoopWithPeer(ctx, st, rcOpen, cache.NewDirectoryCache(), p, 10*time.Millisecond, "vl1_bbb", whisper.NewDeltaTracker(), nil)
	}()
	select {
	case <-done:
	case <-time.After(15 * time.Second):
		t.Fatal("the initiator is still looping with its reconcile stream ended")
	}
}

// idleStream never delivers, for an engine that must stay parked.
type idleStream struct{ aether.Stream }

func (s *idleStream) Receive(ctx context.Context) ([]byte, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

// A reply that does not start with the magic is the stream out of alignment, typed so
// the loop ends the stream and it is opened again.
func TestGossipOverConnNamesAFrameOutOfAlignment(t *testing.T) {
	p := testProfile(t)
	near, far := net.Pipe()
	defer near.Close()
	defer far.Close()
	go func() {
		hdr := make([]byte, gossipHeaderSize)
		_, _ = readFull(far, hdr)
		n := binary.BigEndian.Uint32(hdr[2:6])
		_, _ = readFull(far, make([]byte, n))
		_, _ = far.Write([]byte{0xde, 0xad, 0, 0, 0, 1})
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := GossipOverConn(ctx, near, cache.NewDirectoryCache(), p, nil)
	if !errors.Is(err, errGossipDesynchronised) {
		t.Fatalf("err = %v, want errGossipDesynchronised", err)
	}
}

// The loop ends a stream out of alignment on its way out, because it reads garbage
// until it ends, and the session's gossip loop reopens only a stream that has ended.
func TestAnInitiatorEndsAStreamOutOfAlignment(t *testing.T) {
	sa, sb := sessionPair(t)
	st, peer := streamPair(t, sa, sb, 0)
	rcOpen, rcPeer := streamPair(t, sa, sb, 4)
	_ = rcPeer
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	go func() {
		for {
			if _, err := peer.Receive(ctx); err != nil {
				return
			}
			if err := peer.Send(ctx, []byte{0xde, 0xad, 0, 0, 0, 1}); err != nil {
				return
			}
		}
	}()
	done := make(chan struct{})
	go func() {
		defer close(done)
		RunGossipLoopWithPeer(ctx, st, rcOpen, cache.NewDirectoryCache(), testProfile(t), 10*time.Millisecond, "vl1_bbb", whisper.NewDeltaTracker(), nil)
	}()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("the initiator is still looping on a stream out of alignment")
	}
	if st.IsOpen() {
		t.Fatal("the loop left a stream out of alignment open")
	}
}

func readFull(c net.Conn, b []byte) (int, error) {
	got := 0
	for got < len(b) {
		n, err := c.Read(b[got:])
		got += n
		if err != nil {
			return got, err
		}
	}
	return got, nil
}
