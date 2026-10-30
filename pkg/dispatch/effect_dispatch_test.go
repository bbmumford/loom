/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package dispatch

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/rpc/pb"
	"github.com/bbmumford/loom/pkg/rpc/effect"
)

// These tests hold HWPCaller to the effect class of the operation it calls. A
// call to an Unsafe operation is sent on one route at a time, and on the next
// only when the attempt before provably never left; a Repeatable operation is
// probed on every route at once.

// countingSession answers every request with resp and counts the requests its
// streams carried.
type countingSession struct {
	rpcMockSession
	sent atomic.Int32
}

func (s *countingSession) OpenStream(_ context.Context, cfg aether.StreamConfig) (aether.Stream, error) {
	respData, err := pb.MarshalResponse(s.resp)
	if err != nil {
		return nil, err
	}
	return &countingStream{rpcMockStream: rpcMockStream{id: cfg.StreamID, respData: respData}, sess: s}, nil
}

type countingStream struct {
	rpcMockStream
	sess *countingSession
}

func (st *countingStream) Send(context.Context, []byte) error {
	st.sess.sent.Add(1)
	return nil
}

func answering(payload string) *countingSession {
	return &countingSession{rpcMockSession: rpcMockSession{resp: &pb.RPCResponse{Success: true, Payload: []byte(payload)}}}
}

func threeRoutes(sessions ...aether.Session) []aether.ProbeRoute {
	routes := make([]aether.ProbeRoute, len(sessions))
	for i, s := range sessions {
		routes[i] = aether.ProbeRoute{Session: s, NodeID: string(rune('a'+i)) + "-node", TargetNodeID: "target"}
	}
	return routes
}

func totalSent(sessions ...*countingSession) int32 {
	var n int32
	for _, s := range sessions {
		n += s.sent.Load()
	}
	return n
}

// An Unsafe call over three healthy routes is sent exactly once.
func TestAnUnsafeCallOverThreeRoutesIsSentOnce(t *testing.T) {
	a, b, c := answering("a"), answering("b"), answering("c")
	for _, s := range []aether.Session{a, b, c} {
		defer forgetSession(s)
	}
	caller := NewHWPCaller(&multiRouteFinder{class: effect.Unsafe, routes: threeRoutes(a, b, c)})
	defer caller.Close()

	out, err := caller.Call(context.Background(), "svc", "svc.Op", nil)
	if err != nil || string(out) != "a" {
		t.Fatalf("Call returned (%q, %v); the first route answers", out, err)
	}
	// Leave a parallel probe the time it would need to send.
	time.Sleep(50 * time.Millisecond)
	if n := totalSent(a, b, c); n != 1 {
		t.Fatalf("an Unsafe call was sent %d times over three routes; a second send could run "+
			"it a second time", n)
	}
}

// A finder that reports no effect class leaves every operation Unsafe.
func TestAnUndeclaredOperationIsTreatedAsUnsafe(t *testing.T) {
	a, b := answering("a"), answering("b")
	defer forgetSession(a)
	defer forgetSession(b)
	caller := NewHWPCaller(&noEffectFinder{routes: threeRoutes(a, b)})
	defer caller.Close()

	if _, err := caller.Call(context.Background(), "svc", "svc.Op", nil); err != nil {
		t.Fatalf("Call: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	if n := totalSent(a, b); n != 1 {
		t.Fatalf("an operation with no declared class was sent %d times; undeclared is Unsafe", n)
	}
}

// noEffectFinder is a SessionFinder that does not implement EffectFinder.
type noEffectFinder struct{ routes []aether.ProbeRoute }

func (f *noEffectFinder) FindSession(context.Context, string, string) (aether.Session, error) {
	return f.routes[0].Session, nil
}
func (f *noEffectFinder) FindAnySession() (aether.Session, bool) { return nil, false }
func (f *noEffectFinder) CallViaBidi(context.Context, string, *pb.RPCRequest) (*pb.RPCResponse, bool, error) {
	return nil, false, nil
}
func (f *noEffectFinder) FindRoutes(context.Context, string, string) []aether.ProbeRoute {
	return f.routes
}
func (f *noEffectFinder) IsSupersededByUpgrade(aether.Session) bool { return false }

// A Repeatable call is still probed on every route.
func TestARepeatableCallIsProbedOnEveryRoute(t *testing.T) {
	for _, class := range []effect.Class{effect.Idempotent, effect.ReadOnly} {
		t.Run(class.String(), func(t *testing.T) {
			a, b, c := answering("a"), answering("b"), answering("c")
			for _, s := range []aether.Session{a, b, c} {
				defer forgetSession(s)
			}
			caller := NewHWPCaller(&multiRouteFinder{class: class, routes: threeRoutes(a, b, c)})
			defer caller.Close()

			if _, err := caller.Call(context.Background(), "svc", "svc.Op", nil); err != nil {
				t.Fatalf("Call: %v", err)
			}
			eventually(t, "a Repeatable call did not probe all three routes", func() bool {
				return a.sent.Load() == 1 && b.sent.Load() == 1 && c.sent.Load() == 1
			})
		})
	}
}

// An Unsafe call fails over to its next route when the first refused it before
// sending: a closed session, a stream that would not open, or an open
// circuit. The handler runs once, on the route that took it.
func TestAnUnsafeCallFailsOverOnlyBeforeItWasSent(t *testing.T) {
	for _, tc := range []struct {
		name  string
		first func(caller *HWPCaller) aether.Session
	}{
		{"closed session", func(*HWPCaller) aether.Session { return &rpcMockSession{closed: true} }},
		{"stream refused", func(*HWPCaller) aether.Session { return &faultySession{fault: "open"} }},
		{"open circuit", func(caller *HWPCaller) aether.Session {
			s := answering("never")
			cb := caller.getBreaker("svc.Op", "a-node", s)
			for i := 0; i < callerBreakerThreshold; i++ {
				if cb.Allow() {
					cb.RecordFailure()
				}
			}
			return s
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			second := answering("second")
			defer forgetSession(second)
			finder := &multiRouteFinder{class: effect.Unsafe}
			caller := NewHWPCaller(finder)
			defer caller.Close()
			first := tc.first(caller)
			defer forgetSession(first)
			finder.routes = threeRoutes(first, second)

			out, err := caller.Call(context.Background(), "svc", "svc.Op", nil)
			if err != nil || string(out) != "second" {
				t.Fatalf("Call returned (%q, %v); the first route never sent it, so the second "+
					"must", out, err)
			}
			if cs, ok := first.(*countingSession); ok && cs.sent.Load() != 0 {
				t.Fatalf("the refused route still sent the call %d time(s)", cs.sent.Load())
			}
			if n := second.sent.Load(); n != 1 {
				t.Fatalf("the second route sent the call %d times, want 1", n)
			}
		})
	}
}

// An Unsafe call whose send left and went unanswered ends with
// ErrAmbiguousEffect and is sent nowhere else.
func TestAnUnsafeCallThatLeftIsNeverSentAgain(t *testing.T) {
	for _, fault := range []string{"send", "receive", "garbage"} {
		t.Run("the stream fails at "+fault, func(t *testing.T) {
			first := &faultySession{fault: fault}
			second := answering("second")
			defer forgetSession(first)
			defer forgetSession(second)
			caller := NewHWPCaller(&multiRouteFinder{class: effect.Unsafe, routes: threeRoutes(first, second)})
			defer caller.Close()

			_, err := caller.Call(context.Background(), "svc", "svc.Op", nil)
			if !errors.Is(err, ErrAmbiguousEffect) {
				t.Fatalf("Call returned %v; a send that left unanswered is an ambiguous effect", err)
			}
			if n := second.sent.Load(); n != 0 {
				t.Fatalf("the call went on to the second route %d time(s) after the first send left; "+
					"the handler may now run twice", n)
			}
		})
	}
}

// bidiThenStreamFinder has a bidi channel that ends every call with err after
// counting it, and one route over session.
type bidiThenStreamFinder struct {
	mockFinder
	err      error
	noRoutes bool

	mu    sync.Mutex
	bidis int
}

func (f *bidiThenStreamFinder) CallViaBidi(context.Context, string, *pb.RPCRequest) (*pb.RPCResponse, bool, error) {
	f.mu.Lock()
	f.bidis++
	f.mu.Unlock()
	return nil, true, f.err
}

func (f *bidiThenStreamFinder) FindRoutes(ctx context.Context, role, handler string) []aether.ProbeRoute {
	if f.noRoutes {
		return nil
	}
	return f.mockFinder.FindRoutes(ctx, role, handler)
}

// A bidi attempt that left and timed out, or whose channel closed under it, is
// followed by a dynamic-stream send only for a Repeatable operation, on the
// routed path and on callOnce. An Unsafe one ends ambiguous with no second
// send. A bidi attempt that never left is followed by the dynamic stream
// whatever the class, because that is the call's first send.
func TestABidiAttemptThatLeftIsResentOnlyForARepeatableOperation(t *testing.T) {
	for _, path := range []struct {
		name     string
		noRoutes bool
	}{{"routed", false}, {"no routes", true}} {
		for _, tc := range []struct {
			name       string
			err        error
			class      effect.Class
			wantResend bool
		}{
			{"timed out, Unsafe", context.DeadlineExceeded, effect.Unsafe, false},
			{"channel closed, Unsafe", ErrChannelClosed, effect.Unsafe, false},
			{"timed out, Idempotent", context.DeadlineExceeded, effect.Idempotent, true},
			{"channel closed, Idempotent", ErrChannelClosed, effect.Idempotent, true},
			{"never sent, Unsafe", NotSent(errors.New("marshal")), effect.Unsafe, true},
		} {
			t.Run(path.name+"/"+tc.name, func(t *testing.T) {
				sess := answering("stream")
				defer forgetSession(sess)
				finder := &bidiThenStreamFinder{
					mockFinder: mockFinder{session: sess, class: tc.class},
					err:        tc.err,
					noRoutes:   path.noRoutes,
				}
				caller := NewHWPCaller(finder)
				defer caller.Close()

				out, err := caller.Call(context.Background(), "svc", "svc.Op", nil)
				if tc.wantResend {
					if err != nil || string(out) != "stream" || sess.sent.Load() != 1 {
						t.Fatalf("Call returned (%q, %v) with %d stream send(s); want the dynamic "+
							"stream to answer", out, err, sess.sent.Load())
					}
					return
				}
				if !errors.Is(err, ErrAmbiguousEffect) {
					t.Fatalf("Call returned %v; an Unsafe bidi attempt that left is ambiguous", err)
				}
				if n := sess.sent.Load(); n != 0 {
					t.Fatalf("the Unsafe call was sent again over a dynamic stream %d time(s)", n)
				}
			})
		}
	}
}

// relaySession answers every request with resp, as a relay node's RPCServer
// does when its own forward failed, and counts the requests it carried.
type relaySession struct{ countingSession }

// A route whose relay answered with a grade refusal never sent the call on, so
// an Unsafe call fails over; a route whose relay answered that its forward was
// ambiguous may have run it, so the call ends there.
func TestAnUnsafeCallFollowsTheRelaysVerdict(t *testing.T) {
	for _, tc := range []struct {
		name      string
		stamp     error
		wantOver  bool
		wantErrIs error
	}{
		{"grade refusal", &GradeTooLowError{Handler: "svc.Op"}, true, nil},
		{"ambiguous forward", &AmbiguousEffectError{Handler: "svc.Op", Cause: errors.New("lost")}, false, ErrAmbiguousEffect},
	} {
		t.Run(tc.name, func(t *testing.T) {
			failed := &pb.RPCResponse{Success: false, Error: "forward failed"}
			StampRefusal(failed, tc.stamp)
			relay := &relaySession{countingSession{rpcMockSession: rpcMockSession{resp: failed}}}
			second := answering("second")
			defer forgetSession(relay)
			defer forgetSession(second)
			caller := NewHWPCaller(&multiRouteFinder{class: effect.Unsafe, routes: threeRoutes(relay, second)})
			defer caller.Close()

			out, err := caller.Call(context.Background(), "svc", "svc.Op", nil)
			if tc.wantOver {
				if err != nil || string(out) != "second" || second.sent.Load() != 1 {
					t.Fatalf("Call returned (%q, %v); a refusal before sending fails over", out, err)
				}
				return
			}
			if !errors.Is(err, tc.wantErrIs) || second.sent.Load() != 0 {
				t.Fatalf("Call returned %v and the second route sent %d time(s); an ambiguous relay "+
					"forward ends the call", err, second.sent.Load())
			}
		})
	}
}
