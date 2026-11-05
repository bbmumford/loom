/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package dispatch

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/adapter"
	"github.com/ORBTR/aether/flow"
	"github.com/ORBTR/aether/rpc/pb"
	"github.com/bbmumford/loom/pkg/rpc/effect"
)

// backoffErrs is every error a session's dynamic-stream opens back off on.
var backoffErrs = []error{
	aether.ErrStreamCapExhausted,
	aether.ErrStreamCreditRefused,
	aether.ErrStreamRefused,
	aether.ErrStreamOverrun,
}

// An open answered with backpressure holds Get and Fill for the window: neither
// reaches OpenStream, Get answers the typed error that armed it, and after the
// window the next open goes through.
func TestAnOpenRefusedWithBackpressureHoldsThePool(t *testing.T) {
	for _, typed := range backoffErrs {
		t.Run(typed.Error(), func(t *testing.T) {
			sess := &rpcMockSession{openErr: fmt.Errorf("OpenStream stream=10: %w", typed)}
			defer forgetSession(sess)
			pool := NewStreamPool(2)
			if _, err := pool.Get(context.Background(), sess); !errors.Is(err, typed) {
				t.Fatalf("first Get err = %v, want %v", err, typed)
			}
			if got := atomic.LoadInt32(&sess.openCount); got != 1 {
				t.Fatalf("first Get made %d opens, want 1", got)
			}
			_, err := pool.Get(context.Background(), sess)
			pool.Fill(context.Background(), sess)
			if !errors.Is(err, typed) {
				t.Fatalf("Get inside the window err = %v, want %v", err, typed)
			}
			if got := atomic.LoadInt32(&sess.openCount); got != 1 {
				t.Fatalf("Get and Fill inside the window reached OpenStream: %d opens", got)
			}
			time.Sleep(streamBackoff + 50*time.Millisecond)
			sess.openErr = nil
			sess.resp = &pb.RPCResponse{Success: true}
			st, err := pool.Get(context.Background(), sess)
			if err != nil || st == nil {
				t.Fatalf("Get after the window: %v, %v", st, err)
			}
		})
	}
}

// An open any other error refused arms nothing: the next open goes through.
func TestAnOrdinaryOpenFailureArmsNoBackoff(t *testing.T) {
	sess := &rpcMockSession{openErr: aether.ErrDuplicateStreamID}
	defer forgetSession(sess)
	pool := NewStreamPool(2)
	for range 3 {
		if _, err := pool.Get(context.Background(), sess); !errors.Is(err, aether.ErrDuplicateStreamID) {
			t.Fatalf("Get err = %v", err)
		}
	}
	if got := atomic.LoadInt32(&sess.openCount); got != 3 {
		t.Fatalf("%d opens, want 3: a duplicate ID is no reason to wait", got)
	}
}

// CapExhaustedHits counts the cap's refusals and no other.
func TestCapExhaustedHitsCountsOnlyTheCap(t *testing.T) {
	sess := &rpcMockSession{openErr: aether.ErrStreamCreditRefused}
	defer forgetSession(sess)
	pool := NewStreamPool(2)
	_, _ = pool.Get(context.Background(), sess)
	if got := pool.CapExhaustedHits(); got != 0 {
		t.Fatalf("a credit refusal counted as a cap hit: %d", got)
	}
	time.Sleep(streamBackoff + 50*time.Millisecond)
	sess.openErr = aether.ErrStreamCapExhausted
	_, _ = pool.Get(context.Background(), sess)
	if got := pool.CapExhaustedHits(); got != 1 {
		t.Fatalf("cap hits = %d, want 1", got)
	}
	// Inside the window nothing is refused, so nothing counts.
	_, _ = pool.Get(context.Background(), sess)
	pool.Fill(context.Background(), sess)
	if got := pool.CapExhaustedHits(); got != 1 {
		t.Fatalf("cap hits inside the backoff = %d, want 1", got)
	}
}

// typedEndSession opens streams whose Send or Receive answer a typed end, as a
// stream the peer reset after OpenStream returned does.
type typedEndSession struct {
	rpcMockSession
	sendErr, recvErr error
}

func (s *typedEndSession) OpenStream(_ context.Context, cfg aether.StreamConfig) (aether.Stream, error) {
	atomic.AddInt32(&s.openCount, 1)
	return &rpcMockStream{id: cfg.StreamID, sendErr: s.sendErr, recvErr: s.recvErr}, nil
}

// A RESET that reaches a call's Send or Receive holds the session's next open, on
// the dynamic path where nothing else would slow the next call. A refused stream
// reached no handler, and a Send that failed left nothing a handler would run, so
// even an operation that is not Repeatable reads as not sent. An overrun at Receive
// may have run it, so it stays ambiguous.
func TestAResetReachingACallHoldsTheNextOpen(t *testing.T) {
	for _, tc := range []struct {
		name             string
		sendErr, recvErr error
		wantUnsent       bool
	}{
		{"refused at Send", aether.ErrStreamRefused, nil, true},
		{"refused at Receive", nil, aether.ErrStreamRefused, true},
		{"overrun at Send", aether.ErrStreamOverrun, nil, true},
		{"overrun at Receive", nil, aether.ErrStreamOverrun, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sess := &typedEndSession{sendErr: tc.sendErr, recvErr: tc.recvErr}
			defer forgetSession(sess)
			caller := NewHWPCaller(&noRouteFinder{mockFinder{session: sess}})
			defer caller.Close()
			// Drop the pool's pre-opened entries, so the next open is observable.
			opens := func() int32 { return atomic.LoadInt32(&sess.openCount) }

			_, err := caller.Call(context.Background(), "svc", "svc.Op", nil)
			typed := tc.sendErr
			if typed == nil {
				typed = tc.recvErr
			}
			if !errors.Is(err, typed) {
				t.Fatalf("Call err = %v, want %v", err, typed)
			}
			if got := Unsent(err); got != tc.wantUnsent {
				t.Fatalf("Unsent(%v) = %v, want %v", err, got, tc.wantUnsent)
			}
			before := opens()
			_, err = openDynamicStreamOn(context.Background(), sess, aether.StreamConfig{})
			if !errors.Is(err, typed) {
				t.Fatalf("the open after the call answered %v, want the armed %v", err, typed)
			}
			if opens() != before {
				t.Fatal("the open after the call reached OpenStream inside the backoff")
			}
		})
	}
}

// The role-addressed paths arm the same backoff: the parallel probes of a Repeatable
// operation and the one route of any other. A refused stream on the single route
// reads as unsent, so the call may fail over.
func TestAResetReachingARoutedCallHoldsTheNextOpen(t *testing.T) {
	for _, class := range []effect.Class{effect.Idempotent, effect.Unsafe} {
		for _, typed := range []error{aether.ErrStreamRefused, aether.ErrStreamOverrun} {
			for _, phase := range []string{"send", "receive"} {
				t.Run(fmt.Sprintf("%v/%v/%s", class, typed, phase), func(t *testing.T) {
					sess := &typedEndSession{}
					if phase == "send" {
						sess.sendErr = typed
					} else {
						sess.recvErr = typed
					}
					defer forgetSession(sess)
					caller := NewHWPCaller(&mockFinder{session: sess, class: class})
					defer caller.Close()
					_, err := caller.Call(context.Background(), "svc", "svc.Op", nil)
					if err == nil {
						t.Fatal("Call succeeded")
					}
					if class == effect.Unsafe {
						want := phase == "send" || errors.Is(typed, aether.ErrStreamRefused)
						if got := Unsent(err); got != want {
							t.Fatalf("Unsent(%v) = %v, want %v", err, got, want)
						}
					}
					before := atomic.LoadInt32(&sess.openCount)
					_, err = openDynamicStreamOn(context.Background(), sess, aether.StreamConfig{})
					if !errors.Is(err, typed) {
						t.Fatalf("the open after the call answered %v, want the armed %v", err, typed)
					}
					if atomic.LoadInt32(&sess.openCount) != before {
						t.Fatal("the open after the call reached OpenStream inside the backoff")
					}
				})
			}
		}
	}
}

// Each error a Send answers before the message is on the stream reads as never left;
// a timeout may have handed the whole message on, so it does not.
func TestSendNeverLeftNamesTheErrorsThatLeaveNothing(t *testing.T) {
	for _, err := range []error{
		aether.ErrStreamRefused, aether.ErrStreamOverrun, flow.ErrCreditExhausted, adapter.ErrPayloadTooLarge,
		fmt.Errorf("stream 1: %w", aether.ErrStreamOverrun),
	} {
		if !SendNeverLeft(err) {
			t.Errorf("SendNeverLeft(%v) = false", err)
		}
	}
	for _, err := range []error{nil, context.DeadlineExceeded, errors.New("x"), aether.ErrSessionWriteFailed} {
		if SendNeverLeft(err) {
			t.Errorf("SendNeverLeft(%v) = true", err)
		}
	}
}

// resetCountingStream records how a stream was left.
type resetCountingStream struct {
	rpcMockStream
	resets, closes atomic.Int32
}

func (s *resetCountingStream) Reset(aether.ResetReason) error { s.resets.Add(1); return nil }
func (s *resetCountingStream) Close() error                   { s.closes.Add(1); return nil }

type handlerOpenSession struct {
	rpcMockSession
	errs   []error
	stream *resetCountingStream
	opened atomic.Int32
}

func (s *handlerOpenSession) OpenStream(_ context.Context, cfg aether.StreamConfig) (aether.Stream, error) {
	n := int(s.opened.Add(1)) - 1
	if n < len(s.errs) {
		return nil, s.errs[n]
	}
	s.stream.id = cfg.StreamID
	return s.stream, nil
}

// A handler stream tries the next ID only for an ID in use. Any other refusal stops
// at once, since no other ID would change it.
func TestOpenHandlerStreamRetriesOnlyADuplicateID(t *testing.T) {
	refused := &handlerOpenSession{errs: []error{aether.ErrStreamCreditRefused, aether.ErrStreamCreditRefused}}
	if _, err := OpenHandlerStream(context.Background(), refused, &pb.RPCRequest{Handler: "h"}); !errors.Is(err, aether.ErrStreamCreditRefused) {
		t.Fatalf("err = %v", err)
	}
	if got := refused.opened.Load(); got != 1 {
		t.Fatalf("%d opens on a credit refusal, want 1", got)
	}

	resp, _ := pb.MarshalResponse(&pb.RPCResponse{Success: true})
	dup := &handlerOpenSession{
		errs:   []error{aether.ErrDuplicateStreamID, aether.ErrDuplicateStreamID},
		stream: &resetCountingStream{rpcMockStream: rpcMockStream{respData: resp}},
	}
	if _, err := OpenHandlerStream(context.Background(), dup, &pb.RPCRequest{Handler: "h"}); err != nil {
		t.Fatalf("err = %v", err)
	}
	if got := dup.opened.Load(); got != 3 {
		t.Fatalf("%d opens across two duplicate IDs, want 3", got)
	}
}

// A handler stream that failed before it was handed out is reset, so what its far
// side sent does not sit unread holding that side's connection credit.
func TestAFailedHandlerStreamOpenResets(t *testing.T) {
	refusal, _ := pb.MarshalResponse(&pb.RPCResponse{Success: false, Error: "no"})
	for _, tc := range []struct {
		name   string
		stream rpcMockStream
	}{
		{"send failed", rpcMockStream{sendErr: errors.New("x")}},
		{"receive failed", rpcMockStream{recvErr: errors.New("x")}},
		{"unreadable answer", rpcMockStream{respData: []byte{0xff, 0xff, 0xff}}},
		{"handler refused", rpcMockStream{respData: refusal}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			st := &resetCountingStream{rpcMockStream: tc.stream}
			sess := &handlerOpenSession{stream: st}
			if _, err := OpenHandlerStream(context.Background(), sess, &pb.RPCRequest{Handler: "h"}); err == nil {
				t.Fatal("OpenHandlerStream succeeded")
			}
			if st.resets.Load() != 1 || st.closes.Load() != 0 {
				t.Fatalf("resets = %d closes = %d, want one reset and no close", st.resets.Load(), st.closes.Load())
			}
		})
	}
}

// endingStream answers Receive from its queue and then the end.
type endingStream struct {
	rpcMockStream
	queue          chan []byte
	resets, closes atomic.Int32
}

func (s *endingStream) Reset(aether.ResetReason) error { s.resets.Add(1); return nil }
func (s *endingStream) Close() error                   { s.closes.Add(1); return nil }
func (s *endingStream) Receive(ctx context.Context) ([]byte, error) {
	select {
	case b, ok := <-s.queue:
		if !ok {
			return nil, errors.New("end")
		}
		return b, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// CloseAbandoned closes at once, then reads off what the stream's queue holds and
// resets at its end, which is what returns the peer's credit for unread messages.
func TestCloseAbandonedReadsTheQueueOffAndResets(t *testing.T) {
	st := &endingStream{queue: make(chan []byte, 2)}
	st.queue <- []byte("unread")
	st.queue <- []byte("unread")
	close(st.queue)
	if err := CloseAbandoned(st); err != nil {
		t.Fatal(err)
	}
	if st.closes.Load() != 1 {
		t.Fatalf("closes = %d", st.closes.Load())
	}
	deadline := time.Now().Add(5 * time.Second)
	for st.resets.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if st.resets.Load() != 1 {
		t.Fatalf("resets = %d, want 1", st.resets.Load())
	}
	if len(st.queue) != 0 {
		t.Fatalf("%d messages left unread", len(st.queue))
	}
}
