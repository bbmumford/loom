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
	"github.com/ORBTR/aether/rpc/pb"
	"github.com/bbmumford/loom/pkg/rpc/effect"
)

// These tests cover how HWPCaller settles the circuit of an attempt, on the
// probed-route path and on callOnce: an attempt that ended because the caller's
// context ended, or because a sibling probe answered first, gives no verdict
// and only releases a half-open lease; an attempt the path failed counts; a
// response of any kind closes the circuit. Every attempt settles its own
// circuit, whether or not Call is still waiting for its result.

// stall blocks an attempt until the context it runs under ends. entered gets
// one token per attempt that has blocked, and left counts the attempts that
// have returned.
type stall struct {
	entered chan struct{}
	left    atomic.Int32
}

func newStall() *stall { return &stall{entered: make(chan struct{}, 64)} }

func (s *stall) wait(ctx context.Context) error {
	s.entered <- struct{}{}
	<-ctx.Done()
	s.left.Add(1)
	return ctx.Err()
}

// stalledSession opens streams that send at once and then stall receiving.
type stalledSession struct {
	rpcMockSession
	stall *stall
}

func (s *stalledSession) OpenStream(_ context.Context, cfg aether.StreamConfig) (aether.Stream, error) {
	return &stalledStream{rpcMockStream: rpcMockStream{id: cfg.StreamID}, stall: s.stall}, nil
}

type stalledStream struct {
	rpcMockStream
	stall *stall
}

func (st *stalledStream) Receive(ctx context.Context) ([]byte, error) {
	return nil, st.stall.wait(ctx)
}

// stalledBidiFinder has a bidi channel whose call stalls.
type stalledBidiFinder struct {
	mockFinder
	noRoutes bool
	stall    *stall
}

func (f *stalledBidiFinder) FindRoutes(ctx context.Context, role, handler string) []aether.ProbeRoute {
	if f.noRoutes {
		return nil
	}
	return f.mockFinder.FindRoutes(ctx, role, handler)
}

func (f *stalledBidiFinder) CallViaBidi(ctx context.Context, _ string, _ *pb.RPCRequest) (*pb.RPCResponse, bool, error) {
	return nil, true, f.stall.wait(ctx)
}

// stalledPaths are the four places an attempt can be waiting when its caller
// gives up. node is the node id the path keys its circuit by.
var stalledPaths = []struct {
	name  string
	node  string
	build func(*stall) (SessionFinder, aether.Session)
}{
	{"a probe waiting on its dynamic stream", "test-node", func(s *stall) (SessionFinder, aether.Session) {
		sess := &stalledSession{stall: s}
		return &mockFinder{session: sess}, sess
	}},
	{"a probe waiting on bidi", "test-node", func(s *stall) (SessionFinder, aether.Session) {
		sess := &rpcMockSession{}
		return &stalledBidiFinder{mockFinder: mockFinder{session: sess}, stall: s}, sess
	}},
	{"callOnce waiting on its dynamic stream", "remote-test", func(s *stall) (SessionFinder, aether.Session) {
		sess := &stalledSession{stall: s}
		return &noRouteFinder{mockFinder{session: sess}}, sess
	}},
	{"callOnce waiting on bidi", "remote-test", func(s *stall) (SessionFinder, aether.Session) {
		sess := &rpcMockSession{}
		return &stalledBidiFinder{mockFinder: mockFinder{session: sess}, noRoutes: true, stall: s}, sess
	}},
}

// openLongAgo trips cb and backdates the trip past its reset period, so the
// next Allow admits the half-open probe and the lease it takes lasts a full
// reset period.
func openLongAgo(cb *CircuitBreaker) {
	trip(cb)
	cb.mu.Lock()
	cb.openedAt = time.Now().Add(-2 * cb.resetTimeout)
	cb.mu.Unlock()
}

func circuitOf(cb *CircuitBreaker) (state CircuitState, leased bool, failures int) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state, cb.leased, cb.failures
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal(what)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

// cancelWhileStalled starts a call, waits until its attempt is stalled, ends
// the caller's context and waits for the call to return. It fails the test if
// the call returns without its attempt ever stalling, which is what a circuit
// that had counted earlier cancellations would cause.
func cancelWhileStalled(t *testing.T, caller *HWPCaller, s *stall) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := caller.Call(ctx, "svc", "svc.Op", nil)
		done <- err
	}()
	select {
	case <-s.entered:
	case err := <-done:
		t.Fatalf("the call returned %v before its attempt reached the path", err)
	case <-time.After(3 * time.Second):
		t.Fatal("the attempt never reached the path")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("a call whose caller gave up returned %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("the call did not return after its caller's context ended")
	}
}

// A half-open probe whose caller gives up releases its lease: the circuit is
// neither re-opened nor left leased, and the next call becomes the probe. A
// lease left held refuses every call on that capability and session until it
// expires, a reset period later.
func TestACallersCancellationReleasesTheHalfOpenLease(t *testing.T) {
	for _, path := range stalledPaths {
		t.Run(path.name, func(t *testing.T) {
			s := newStall()
			finder, sess := path.build(s)
			defer forgetSession(sess)
			caller := NewHWPCaller(finder)
			defer caller.Close()
			cb := caller.getBreaker("svc.Op", path.node, sess)
			openLongAgo(cb)

			cancelWhileStalled(t, caller, s)

			eventually(t, "the half-open lease is still held after the probe's caller gave up", func() bool {
				_, leased, _ := circuitOf(cb)
				return !leased
			})
			if state, _, _ := circuitOf(cb); state != CircuitHalfOpen {
				t.Fatalf("the circuit is %v after its probe's caller gave up; a cancellation is no "+
					"verdict and leaves it half-open", state)
			}
			if !cb.Allow() {
				t.Fatal("the next call was refused after the probe released its lease")
			}
		})
	}
}

// Calls whose callers give up are never counted: more of them than the trip
// threshold leave a closed circuit closed with no failure recorded.
func TestCancelledCallsNeverOpenAClosedCircuit(t *testing.T) {
	for _, path := range stalledPaths {
		t.Run(path.name, func(t *testing.T) {
			s := newStall()
			finder, sess := path.build(s)
			defer forgetSession(sess)
			caller := NewHWPCaller(finder)
			defer caller.Close()

			const calls = callerBreakerThreshold + 1
			for i := 0; i < calls; i++ {
				cancelWhileStalled(t, caller, s)
			}
			eventually(t, "a stalled attempt never returned", func() bool { return s.left.Load() == calls })
			// Each attempt settles its circuit right after it returns; this
			// leaves time for the last one.
			time.Sleep(50 * time.Millisecond)

			state, _, failures := circuitOf(caller.getBreaker("svc.Op", path.node, sess))
			if state != CircuitClosed || failures != 0 {
				t.Fatalf("the circuit is %v with %d failure(s) after %d cancelled calls; a caller's "+
					"cancellation is not the path's availability", state, failures, calls)
			}
		})
	}
}

// A probe that a sibling probe beat is cancelled by Call, not failed by its
// path: it releases a half-open lease and records nothing, although Call has
// already returned and no longer reads its result. Only a Repeatable operation
// is probed on two routes at once.
func TestAProbeASiblingBeatSettlesItsOwnCircuit(t *testing.T) {
	s := newStall()
	fast := &rpcMockSession{resp: &pb.RPCResponse{Success: true, Payload: []byte("fast")}}
	slow := &stalledSession{stall: s}
	defer forgetSession(fast)
	defer forgetSession(slow)
	caller := NewHWPCaller(&multiRouteFinder{class: effect.Idempotent, routes: []aether.ProbeRoute{
		{Session: fast, NodeID: "node-a", TargetNodeID: "target"},
		{Session: slow, NodeID: "node-b", TargetNodeID: "target"},
	}})
	defer caller.Close()
	cb := caller.getBreaker("svc.Op", "node-b", slow)
	openLongAgo(cb)

	out, err := caller.Call(context.Background(), "svc", "svc.Op", nil)
	if err != nil || string(out) != "fast" {
		t.Fatalf("Call returned (%q, %v); the first route answers", out, err)
	}
	eventually(t, "the beaten probe never returned", func() bool { return s.left.Load() == 1 })
	eventually(t, "the beaten probe left its half-open lease held", func() bool {
		_, leased, _ := circuitOf(cb)
		return !leased
	})
	if state, _, _ := circuitOf(cb); state != CircuitHalfOpen {
		t.Fatalf("the beaten probe's circuit is %v; being cancelled by a sibling is no verdict", state)
	}
}

// faultySession fails every attempt at one point of a dynamic-stream call.
// touched counts the attempts that reached it.
type faultySession struct {
	rpcMockSession
	fault   string // "open", "send", "receive" or "garbage"
	touched atomic.Int32
}

func (s *faultySession) OpenStream(_ context.Context, cfg aether.StreamConfig) (aether.Stream, error) {
	if s.fault == "open" {
		s.touched.Add(1)
		return nil, fmt.Errorf("conn dead")
	}
	return &faultyStream{rpcMockStream: rpcMockStream{id: cfg.StreamID}, sess: s}, nil
}

type faultyStream struct {
	rpcMockStream
	sess *faultySession
}

func (st *faultyStream) Send(context.Context, []byte) error {
	st.sess.touched.Add(1)
	if st.sess.fault == "send" {
		return fmt.Errorf("write: broken pipe")
	}
	return nil
}

func (st *faultyStream) Receive(context.Context) ([]byte, error) {
	if st.sess.fault == "receive" {
		return nil, fmt.Errorf("read: connection reset")
	}
	return []byte{0xff, 0xff, 0xff}, nil // not an RPCResponse
}

// On the path Call takes when the finder has no routes, a failed attempt is
// an availability failure wherever the dynamic stream failed it: the trip
// threshold of them opens the circuit, and the next call is refused by it
// without reaching the session.
func TestAvailabilityFailuresWithNoRoutesOpenTheCircuit(t *testing.T) {
	for _, fault := range []string{"open", "send", "receive", "garbage"} {
		t.Run("the stream fails at "+fault, func(t *testing.T) {
			sess := &faultySession{fault: fault}
			defer forgetSession(sess)
			caller := NewHWPCaller(&noRouteFinder{mockFinder{session: sess}})
			defer caller.Close()

			for i := 0; i < callerBreakerThreshold; i++ {
				if _, err := caller.Call(context.Background(), "svc", "svc.Op", nil); err == nil {
					t.Fatalf("premise wrong: call %d succeeded over a failing stream", i)
				}
			}
			cb := caller.getBreaker("svc.Op", "remote-test", sess)
			if state, _, failures := circuitOf(cb); state != CircuitOpen {
				t.Fatalf("the circuit is %v with %d failure(s) after %d failed attempts; the no-route "+
					"path never opens it", state, failures, callerBreakerThreshold)
			}
			before := sess.touched.Load()
			_, err := caller.Call(context.Background(), "svc", "svc.Op", nil)
			if !errors.Is(err, ErrCircuitOpen) || sess.touched.Load() != before {
				t.Fatalf("the next call returned %v and reached the session %d more time(s); an open "+
					"circuit refuses it first", err, sess.touched.Load()-before)
			}
		})
	}
}

// On the same path a response closes the circuit whatever it says: a handler's
// rejection is the handler's answer over a working path, so a half-open probe
// that gets one closes the circuit instead of leaving its lease to expire.
func TestAResponseWithNoRoutesClosesAHalfOpenCircuit(t *testing.T) {
	stamped := &pb.RPCResponse{Success: false, Error: "forward svc.Op failed (hop 2): refused"}
	StampRefusal(stamped, &RouteDeadlineError{Handler: "svc.Op", Remaining: time.Second, Need: 5 * time.Second})

	for _, tc := range []struct {
		name string
		resp *pb.RPCResponse
		want error // nil for a success
	}{
		{"a success", &pb.RPCResponse{Success: true, Payload: []byte("ran")}, nil},
		{"a handler rejection", &pb.RPCResponse{Success: false, Error: "not found"}, ErrHandlerError},
		{"a later hop's refusal", stamped, ErrRouteDeadline},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sess := &rpcMockSession{resp: tc.resp}
			defer forgetSession(sess)
			caller := NewHWPCaller(&noRouteFinder{mockFinder{session: sess}})
			defer caller.Close()
			cb := caller.getBreaker("svc.Op", "remote-test", sess)
			openLongAgo(cb)

			_, err := caller.Call(context.Background(), "svc", "svc.Op", nil)
			if (tc.want == nil) != (err == nil) || (tc.want != nil && !errors.Is(err, tc.want)) {
				t.Fatalf("Call returned %v, want %v", err, tc.want)
			}
			if state, leased, failures := circuitOf(cb); state != CircuitClosed || leased || failures != 0 {
				t.Fatalf("the circuit is %v (leased %v, %d failures) after the path answered; any "+
					"response closes it", state, leased, failures)
			}
		})
	}
}
