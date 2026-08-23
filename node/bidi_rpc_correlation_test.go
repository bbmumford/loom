/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/rpc/pb"
)

// COVERAGE of BidiRPC request/response correlation, 8 functions at
// 0.0% — `Call`, `nextWireID`, `handleIncomingResponse`, `InflightCalls`,
// `NewBidiRPC` and the rest of bidi_rpc.go.
//
// 🙋 The cost, MEASURED rather than estimated (eight wrong estimates is
// enough): `NewBidiRPC(stream, remoteID, server, transport, scope)` needs an
// `aether.Stream` — a 10-method interface of which this path uses exactly one,
// `Send` — plus an `*RPCServer`, and `NewRPCServer(nil)` already works.
//
// 🔴 THIS IS THE CORRELATION LAYER, so the failure mode is not a crash: it is
// ONE CALLER RECEIVING ANOTHER CALLER'S RESPONSE. Two named incidents live in
// this file's comments — `M-BidiPending-IDCollision` (two forwarder probes
// sharing a caller-side id overwrote each other's pending channel) and
// `MESH-D01` (ids colliding across bidi instances in the shared dedup cache).
// Both are invariants nothing else asserts.
//
// 🙋 THREE OF MY OWN FIXTURE DEFECTS DIED HERE, ALL THE SAME MISTAKE: I wrote a
// SINGLE-THREADED fake for a CONCURRENT interface.
//   1. Receive returned immediately -> readLoop exited -> b.done closed ->
//      Call's three-arm select raced (would have shipped a flaky suite).
//   2. close(b.done) from a test -> "close of closed channel"; done is owned by
//      readLoop's defer.
//   3. Unsynchronised appends and an `int` counter in Send -> a data race that
//      -race reported and that had already lost a frame.
// The premise checks caught (1) and (3) as "only 1 frames sent" rather than as
// a false accusation against the correlation logic.

// bidiStream is an aether.Stream whose Send hands the frame to a hook, so a
// test can answer synchronously as a peer would — no readLoop, no goroutine,
// no timing.
type bidiStream struct {
	onSend  func(msg []byte) error
	sendErr error
	// sent is atomic because Send is called from EVERY caller goroutine. A
	// plain int here was a real data race that -race caught (my third fixture
	// defect in this slice — see the file header).
	sent atomic.Int64
	// 🙋 block keeps Receive parked. My first fake returned an error
	// immediately, so NewBidiRPC's readLoop exited at once and closed b.done —
	// which made Call's three-arm select race (Go picks randomly among ready
	// arms) and would have shipped a flaky suite. The fixture must keep the
	// bidi ALIVE unless a test is specifically about teardown.
	block chan struct{}
}

func (s *bidiStream) Send(_ context.Context, data []byte) error {
	s.sent.Add(1)
	if s.sendErr != nil {
		return s.sendErr
	}
	if s.onSend != nil {
		return s.onSend(data)
	}
	return nil
}

func (s *bidiStream) StreamID() uint64 { return 1 }

func (s *bidiStream) Receive(ctx context.Context) ([]byte, error) {
	select {
	case <-s.block:
	case <-ctx.Done():
	}
	return nil, context.Canceled
}
func (s *bidiStream) Close() error                   { return nil }
func (s *bidiStream) Reset(aether.ResetReason) error { return nil }
func (s *bidiStream) SetPriority(uint8, uint64)      {}
func (s *bidiStream) Config() aether.StreamConfig    { return aether.StreamConfig{} }
func (s *bidiStream) IsOpen() bool                   { return true }
func (s *bidiStream) Conn() net.Conn                 { return nil }

var _ aether.Stream = (*bidiStream)(nil)

// replyTo answers a request frame with a success response echoing the WIRE id,
// which is what a real server does (resp.Id = req.Id).
func replyTo(t *testing.T, b *BidiRPC, payload []byte) error {
	t.Helper()
	if len(payload) == 0 || payload[0] != msgTypeRequest {
		t.Fatalf("frame is not a request (first byte %v) — the test is answering "+
			"something other than the call under test", payload)
	}
	req, err := pb.UnmarshalRequest(payload[1:])
	if err != nil {
		t.Fatalf("the frame this bidi sent does not decode as an RPCRequest: %v", err)
	}
	respBytes, err := pb.MarshalResponse(&pb.RPCResponse{Id: req.Id, Success: true})
	if err != nil {
		t.Fatal(err)
	}
	b.handleIncomingResponse(respBytes)
	return nil
}

func newBidiForTest(t *testing.T, s *bidiStream) *BidiRPC {
	t.Helper()
	if s.block == nil {
		s.block = make(chan struct{})
	}
	b := NewBidiRPC(s, aether.NodeID(testNodeIDB), NewRPCServer(nil), "websocket", "same-origin")
	// Release readLoop at the end of the test so it exits and closes done
	// exactly once — b.done is owned by readLoop and closing it from a test
	// panics with "close of closed channel" (measured: my first version did).
	t.Cleanup(func() { close(s.block) })
	return b
}

// ── The wire-ID invariant ───────────────────────────────────────────────────

// 🔴 THE CALLER'S req.Id MUST COME BACK UNCHANGED, on every exit arm.
//
// Call stamps a bidi-local wire ID over req.Id for pending-map correlation and
// restores the original before returning — at FIVE separate sites (success,
// ctx.Done(), bidi closed, marshal error, send error). A missed restore leaks an
// internal id to a caller that logs and correlates by it.
func TestCallRestoresTheCallersRequestIDOnEveryExitPath(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		s := &bidiStream{}
		b := newBidiForTest(t, s)
		s.onSend = func(msg []byte) error { return replyTo(t, b, msg) }

		req := &pb.RPCRequest{Id: "caller-original", Handler: "orbtr.io.dhcp.ListLeases"}
		resp, err := b.Call(context.Background(), req)
		if err != nil {
			t.Fatalf("Call: %v", err)
		}
		if req.Id != "caller-original" {
			t.Fatalf("req.Id = %q after a successful Call, want the caller's own "+
				"id — the bidi-internal wire id leaked to the caller, which then "+
				"logs and correlates by an id no upstream hop has seen", req.Id)
		}
		if resp.Id != "caller-original" {
			t.Fatalf("resp.Id = %q, want the caller's id echoed back — the server "+
				"echoes our WIRE id and Call must swap it back", resp.Id)
		}
	})

	t.Run("send error", func(t *testing.T) {
		s := &bidiStream{sendErr: errors.New("transport down")}
		b := newBidiForTest(t, s)

		req := &pb.RPCRequest{Id: "caller-original"}
		if _, err := b.Call(context.Background(), req); err == nil {
			t.Fatal("Call succeeded despite a Send error")
		}
		if req.Id != "caller-original" {
			t.Fatalf("req.Id = %q after a send failure — the error path forgot to "+
				"restore it, and a retrying caller now sends a bidi-internal id",
				req.Id)
		}
	})

	t.Run("context deadline", func(t *testing.T) {
		s := &bidiStream{} // never answers
		b := newBidiForTest(t, s)

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		req := &pb.RPCRequest{Id: "caller-original"}
		_, err := b.Call(ctx, req)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("err = %v, want DeadlineExceeded", err)
		}
		if req.Id != "caller-original" {
			t.Fatalf("req.Id = %q after a timeout", req.Id)
		}
	})
}

// 🔴 MESH-D01: wire IDs must be unique ACROSS bidi instances, not just within
// one. The responder's dedup cache is shared, so two bidis that both start
// counting at 1 would collide there.
func TestWireIDsAreUniqueAcrossBidiInstances(t *testing.T) {
	a := newBidiForTest(t, &bidiStream{})
	c := newBidiForTest(t, &bidiStream{})

	seen := map[string]bool{}
	for i := 0; i < 32; i++ {
		for _, b := range []*BidiRPC{a, c} {
			id := b.nextWireID()
			if seen[id] {
				t.Fatalf("wire id %q was issued twice — two bidi instances share "+
					"an id namespace, so the responder's shared dedup cache "+
					"conflates their calls (MESH-D01)", id)
			}
			seen[id] = true
		}
	}

	// And within one bidi the ids must be monotonic-distinct, not random.
	first, second := a.nextWireID(), a.nextWireID()
	if first == second {
		t.Fatal("consecutive nextWireID calls returned the same id")
	}
	if !strings.HasPrefix(first, "b") || !strings.HasPrefix(second, "b") {
		t.Fatalf("wire ids %q/%q lost their per-instance namespace prefix",
			first, second)
	}
}

// ── Response correlation ────────────────────────────────────────────────────

// 🔴 THE ONE THAT MATTERS MOST: a response must resolve its OWN caller.
//
// Two concurrent calls, and the peer answers the SECOND one first. Each caller
// must get its own response — this is the M-BidiPending-IDCollision failure
// class, where correlation by the caller-supplied id let one probe consume
// another's answer.
func TestAResponseResolvesOnlyItsOwnPendingCall(t *testing.T) {
	s := &bidiStream{}
	b := newBidiForTest(t, s)

	// Capture both request frames without answering, then answer in reverse.
	//
	// 🙋 The mutex is not decoration: Send is called from TWO caller goroutines,
	// and my first version appended without one. The race lost an append, the
	// test reported "only 1 frames sent", and the premise check caught it — a
	// racy fixture would otherwise have produced an intermittent failure blaming
	// the correlation logic.
	var framesMu sync.Mutex
	var frames [][]byte
	s.onSend = func(msg []byte) error {
		cp := make([]byte, len(msg))
		copy(cp, msg)
		framesMu.Lock()
		frames = append(frames, cp)
		framesMu.Unlock()
		return nil
	}

	type result struct {
		resp *pb.RPCResponse
		err  error
	}
	results := make(chan result, 2)
	// Both callers deliberately share the SAME caller-side id — the exact
	// input that used to collide.
	for _, handler := range []string{"first.Handler", "second.Handler"} {
		go func(handler string) {
			r, err := b.Call(context.Background(),
				&pb.RPCRequest{Id: "shared-caller-id", Handler: handler})
			results <- result{r, err}
		}(handler)
	}

	nFrames := func() int {
		framesMu.Lock()
		defer framesMu.Unlock()
		return len(frames)
	}
	deadline := time.Now().Add(2 * time.Second)
	for nFrames() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if nFrames() != 2 {
		t.Fatalf("only %d frames sent — both calls must reach the wire before "+
			"the correlation below means anything", nFrames())
	}
	framesMu.Lock()
	captured := append([][]byte(nil), frames...)
	framesMu.Unlock()

	// Answer in REVERSE order, with distinct payloads so a swap is visible.
	for i := len(captured) - 1; i >= 0; i-- {
		req, err := pb.UnmarshalRequest(captured[i][1:])
		if err != nil {
			t.Fatal(err)
		}
		respBytes, err := pb.MarshalResponse(&pb.RPCResponse{
			Id: req.Id, Success: true, Payload: []byte(req.Handler),
		})
		if err != nil {
			t.Fatal(err)
		}
		b.handleIncomingResponse(respBytes)
	}

	got := map[string]bool{}
	for i := 0; i < 2; i++ {
		select {
		case r := <-results:
			if r.err != nil {
				t.Fatalf("Call returned %v — a pending caller was never resolved, "+
					"so a response was dropped or delivered to the wrong slot", r.err)
			}
			if r.resp.Id != "shared-caller-id" {
				t.Fatalf("resp.Id = %q, want the caller's id", r.resp.Id)
			}
			got[string(r.resp.Payload)] = true
		case <-time.After(2 * time.Second):
			t.Fatal("a Call never returned — its pending channel was overwritten " +
				"by the other call, which is exactly M-BidiPending-IDCollision")
		}
	}
	if !got["first.Handler"] || !got["second.Handler"] {
		t.Fatalf("the two callers did not each receive their own response: %v — "+
			"correlation is crossing callers", got)
	}
}

// An orphan response (no pending caller) must be dropped quietly: it happens
// whenever a caller has already timed out and gone away.
func TestAnOrphanResponseIsDroppedWithoutDisturbingOtherCallers(t *testing.T) {
	s := &bidiStream{}
	b := newBidiForTest(t, s)

	// A response for an id nobody is waiting on.
	orphan, err := pb.MarshalResponse(&pb.RPCResponse{Id: "nobody-is-waiting", Success: true})
	if err != nil {
		t.Fatal(err)
	}
	b.handleIncomingResponse(orphan) // must not panic

	// Malformed bytes must also be survivable — a peer can send anything.
	b.handleIncomingResponse([]byte{0xff, 0x00, 0x13, 0x37})

	// And a real call afterwards still works, so neither poisoned the bidi.
	s.onSend = func(msg []byte) error { return replyTo(t, b, msg) }
	if _, err := b.Call(context.Background(), &pb.RPCRequest{Id: "real"}); err != nil {
		t.Fatalf("a real Call failed after an orphan and a malformed response: "+
			"%v — one of them left the pending map or the bidi wedged", err)
	}
}

// ── Accounting and liveness ─────────────────────────────────────────────────

// InflightCalls feeds the bidirpc_inflight_gauge. It must return to zero however a call
// exits, or the gauge ratchets up and reads as a permanent backlog.
func TestInflightCallsReturnsToZeroOnEveryExitPath(t *testing.T) {
	s := &bidiStream{}
	b := newBidiForTest(t, s)
	if got := b.InflightCalls(); got != 0 {
		t.Fatalf("InflightCalls = %d before any call", got)
	}

	// Success path.
	s.onSend = func(msg []byte) error { return replyTo(t, b, msg) }
	if _, err := b.Call(context.Background(), &pb.RPCRequest{Id: "ok"}); err != nil {
		t.Fatal(err)
	}
	if got := b.InflightCalls(); got != 0 {
		t.Fatalf("InflightCalls = %d after a successful call, want 0 — the "+
			"gauge ratchets and every dashboard reads a permanent backlog", got)
	}

	// Timeout path.
	s.onSend = nil
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := b.Call(ctx, &pb.RPCRequest{Id: "timeout"}); err == nil {
		t.Fatal("expected a timeout")
	}
	if got := b.InflightCalls(); got != 0 {
		t.Fatalf("InflightCalls = %d after a TIMED-OUT call, want 0", got)
	}

	// Send-error path.
	s.sendErr = errors.New("down")
	if _, err := b.Call(context.Background(), &pb.RPCRequest{Id: "senderr"}); err == nil {
		t.Fatal("expected a send error")
	}
	if got := b.InflightCalls(); got != 0 {
		t.Fatalf("InflightCalls = %d after a send failure, want 0", got)
	}
}

// A timed-out call must not leave its pending entry behind: the map is keyed
// by wire id and grows for the life of the bidi.
func TestATimedOutCallRemovesItsPendingEntry(t *testing.T) {
	s := &bidiStream{}
	b := newBidiForTest(t, s)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := b.Call(ctx, &pb.RPCRequest{Id: "gone"}); err == nil {
		t.Fatal("expected a timeout")
	}

	b.mu.Lock()
	n := len(b.pending)
	b.mu.Unlock()
	if n != 0 {
		t.Fatalf("%d pending entries after a timed-out call, want 0 — the map "+
			"grows once per timeout for the life of the bidi", n)
	}
}

// IsAlive must flip when the read loop exits — driven through the REAL
// teardown (releasing Receive) rather than by closing b.done from the test:
// done is owned by readLoop's defer, and closing it here panics.
func TestIsAliveFlipsWhenTheReadLoopExits(t *testing.T) {
	s := &bidiStream{block: make(chan struct{})}
	b := NewBidiRPC(s, aether.NodeID(testNodeIDB), NewRPCServer(nil), "websocket", "same-origin")

	if !b.IsAlive() {
		t.Fatal("a fresh BidiRPC reports not alive")
	}

	close(s.block) // Receive returns an error -> readLoop exits -> done closes

	deadline := time.Now().Add(2 * time.Second)
	for b.IsAlive() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if b.IsAlive() {
		t.Fatal("IsAlive stayed true after the read loop exited — a caller keeps " +
			"dispatching onto a dead stream, and Call's b.done arm never fires")
	}

	// And a Call on a dead bidi must fail fast rather than hang to its deadline.
	_, err := b.Call(context.Background(), &pb.RPCRequest{Id: "after-close"})
	if err == nil {
		t.Fatal("Call succeeded on a closed bidi")
	}
	if !strings.Contains(err.Error(), "closed") {
		t.Fatalf("Call on a closed bidi returned %v — the caller cannot tell a "+
			"dead stream from a slow peer", err)
	}
}
