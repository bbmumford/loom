/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package dispatch

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/rpc/pb"
	"github.com/bbmumford/loom/grade"
)

// These tests drive HWPCaller, the dispatcher behind rpc.Call, rpc.CallRaw and
// rpc.CallNode, through the grade gate it runs on the first hop of a call. A
// refusal is only proven if nothing was sent, so every refusal case asserts
// that no request reached a stream of the session below the floor.

// gradedSession is an rpcMockSession on a chosen transport, which is what
// grade.SessionGrade derives the grade from, and it counts the requests sent
// on its streams. aether.ProtoUnknown carries no capabilities and grades F.
type gradedSession struct {
	rpcMockSession
	proto aether.Protocol
	sends atomic.Int32
}

func (s *gradedSession) Protocol() aether.Protocol { return s.proto }

func (s *gradedSession) OpenStream(ctx context.Context, cfg aether.StreamConfig) (aether.Stream, error) {
	st, err := s.rpcMockSession.OpenStream(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return &countedStream{Stream: st, sends: &s.sends}, nil
}

type countedStream struct {
	aether.Stream
	sends *atomic.Int32
}

func (c *countedStream) Send(ctx context.Context, data []byte) error {
	c.sends.Add(1)
	return c.Stream.Send(ctx, data)
}

func newGradedSession(proto aether.Protocol, payload string) *gradedSession {
	return &gradedSession{
		proto:          proto,
		rpcMockSession: rpcMockSession{resp: &pb.RPCResponse{Success: true, Payload: []byte(payload)}},
	}
}

// callPaths are the two ways HWPCaller.Call reaches a session: the parallel
// probe over the finder's routes, and callOnce when the finder has none.
var callPaths = []struct {
	name   string
	finder func(aether.Session) SessionFinder
}{
	{"probed routes", func(s aether.Session) SessionFinder { return &mockFinder{session: s} }},
	{"no routes", func(s aether.Session) SessionFinder { return &noRouteFinder{mockFinder{session: s}} }},
}

// Each operation class, the default included, is held to its floor at the
// origin: Bulk F, Standard C (the class of a call that names none), Realtime
// and Critical B. Below the floor the call is refused with a
// *GradeTooLowError carrying the current and required grade and nothing is
// sent; at or above it the request is sent once.
func TestTheOriginHoldsEachOperationClassToItsFloor(t *testing.T) {
	for _, path := range callPaths {
		for _, tc := range []struct {
			name    string
			opClass string
			proto   aether.Protocol
			refused bool
		}{
			{"bulk over a grade F path", "bulk", aether.ProtoUnknown, false},
			{"the default class over a grade F path", "", aether.ProtoUnknown, true},
			{"the default class over a grade C path", "", aether.ProtoWebSocket, false},
			{"standard over a grade F path", "standard", aether.ProtoUnknown, true},
			{"standard over a grade C path", "standard", aether.ProtoWebSocket, false},
			{"realtime over a grade C path", "realtime", aether.ProtoWebSocket, true},
			{"realtime over a grade B path", "realtime", aether.ProtoQUIC, false},
			{"critical over a grade C path", "critical", aether.ProtoWebSocket, true},
			{"critical over a grade B path", "critical", aether.ProtoQUIC, false},
			{"critical over a grade A path", "critical", aether.ProtoNoise, false},
		} {
			t.Run(path.name+"/"+tc.name, func(t *testing.T) {
				sess := newGradedSession(tc.proto, "ran")
				defer forgetSession(sess)
				caller := NewHWPCaller(path.finder(sess))
				defer caller.Close()

				out, err := caller.Call(WithOpClass(context.Background(), tc.opClass), "svc", "svc.Op", nil)
				if !tc.refused {
					if err != nil || string(out) != "ran" {
						t.Fatalf("Call returned (%q, %v); a %v path meets this class's floor",
							out, err, grade.SessionGrade(sess))
					}
					if n := sess.sends.Load(); n != 1 {
						t.Fatalf("the session received %d request(s), want 1", n)
					}
					return
				}
				if !errors.Is(err, ErrGradeTooLow) {
					t.Fatalf("Call returned (%q, %v); a %v path is below this class's floor and "+
						"must be refused with ErrGradeTooLow", out, err, grade.SessionGrade(sess))
				}
				var gte *GradeTooLowError
				if !errors.As(err, &gte) {
					t.Fatalf("Call returned %T; the refusal must be a *GradeTooLowError", err)
				}
				if gte.UnknownClass || gte.Current != grade.SessionGrade(sess) || gte.Current >= gte.Required {
					t.Errorf("GradeTooLowError reports current %v, required %v, unknown %v; current "+
						"must be the path's %v and below required", gte.Current, gte.Required,
						gte.UnknownClass, grade.SessionGrade(sess))
				}
				if n := sess.sends.Load(); n != 0 {
					t.Errorf("the session received %d request(s) below the floor", n)
				}
			})
		}
	}
}

// A class the gate does not declare is refused before any path: the finder is
// not asked for a route, a session or a bidi send, and a cached result for the
// same call is not served in its place.
func TestTheOriginRefusesAnUndeclaredClassBeforeAnyPath(t *testing.T) {
	undeclared := WithOpClass(context.Background(), "turbo")
	assertUndeclared := func(t *testing.T, err error) {
		t.Helper()
		if !errors.Is(err, ErrGradeTooLow) {
			t.Fatalf("returned %v for an undeclared class; it must be refused with ErrGradeTooLow", err)
		}
		var gte *GradeTooLowError
		if !errors.As(err, &gte) || !gte.UnknownClass || gte.Class != "turbo" {
			t.Fatalf("returned %#v; the refusal must be a *GradeTooLowError naming the class and "+
				"marking it unknown", err)
		}
	}

	t.Run("Call", func(t *testing.T) {
		sess := newGradedSession(aether.ProtoNoise, "ran")
		defer forgetSession(sess)
		finder := &recordingFinder{mockFinder: mockFinder{session: sess}, bidiOK: true,
			bidiResp: &pb.RPCResponse{Success: true, Payload: []byte("ran")}}
		caller := NewHWPCaller(finder)
		defer caller.Close()
		caller.SetCacheableHandlers(map[string]time.Duration{"svc/svc.Op": time.Minute})
		caller.StoreCachedResult("svc/svc.Op", nil, []byte("cached"), time.Minute)

		out, err := caller.Call(undeclared, "svc", "svc.Op", nil)
		assertUndeclared(t, err)
		if out != nil {
			t.Errorf("Call returned payload %q with its refusal", out)
		}
		if finder.findRoutes != 0 || finder.findSessions != 0 || finder.bidiCalls != 0 || sess.sends.Load() != 0 {
			t.Errorf("the finder was asked for routes %d, sessions %d and bidi sends %d times, and "+
				"%d request(s) were sent; an undeclared class is refused before any path",
				finder.findRoutes, finder.findSessions, finder.bidiCalls, sess.sends.Load())
		}
	})

	t.Run("CallNode", func(t *testing.T) {
		finder := &recordingFinder{bidiOK: true, bidiResp: &pb.RPCResponse{Success: true}}
		caller := NewHWPCaller(finder)
		defer caller.Close()

		_, err := caller.CallNode(undeclared, "node-1", "svc", "svc.Op", nil)
		assertUndeclared(t, err)
		if finder.bidiCalls != 0 {
			t.Errorf("the request was handed to the finder %d time(s) for an undeclared class",
				finder.bidiCalls)
		}
	})
}

// A route below the floor is left out of the probe while another route meets
// it, and the routes the finder returned stay memoized whole: a later call of
// a lower class still finds the route this call's floor left out. The grade C
// route is the first one sortProbeRoutes orders, by node id, so a filter that
// compacted the memoized slice in place would write the grade B route over it.
func TestARouteBelowTheFloorIsNotProbedAndStaysInTheRouteCache(t *testing.T) {
	low := newGradedSession(aether.ProtoWebSocket, "low")
	high := newGradedSession(aether.ProtoQUIC, "high")
	defer forgetSession(low)
	defer forgetSession(high)
	caller := NewHWPCaller(&multiRouteFinder{routes: []aether.ProbeRoute{
		{Session: low, NodeID: "node-a", TargetNodeID: "target"},
		{Session: high, NodeID: "node-b", TargetNodeID: "target"},
	}})
	defer caller.Close()

	out, err := caller.Call(WithOpClass(context.Background(), "critical"), "svc", "svc.Op", nil)
	if err != nil || string(out) != "high" {
		t.Fatalf("a critical call returned (%q, %v); the grade B route meets its floor", out, err)
	}
	if n := low.sends.Load(); n != 0 {
		t.Fatalf("the grade C route received %d request(s) of a critical call", n)
	}

	// The grade B route goes away. A standard call inside the route cache's
	// lifetime must still reach the grade C route, which it can only do if
	// the critical call did not drop that route from the memoized set.
	high.closed = true
	out, err = caller.Call(context.Background(), "svc", "svc.Op", nil)
	if err != nil || string(out) != "low" {
		t.Fatalf("a standard call after the critical one returned (%q, %v); the grade C route "+
			"meets its floor and must still be in the cached route set", out, err)
	}
}

// refusingFinder is a finder whose bidi send refuses on grade, as node's
// ConnectionManager.CallViaBidi does for a session below the floor.
type refusingFinder struct {
	mockFinder
	noRoutes bool
	refused  atomic.Int32
}

func (f *refusingFinder) FindRoutes(ctx context.Context, role, handler string) []aether.ProbeRoute {
	if f.noRoutes {
		return nil
	}
	return f.mockFinder.FindRoutes(ctx, role, handler)
}

func (f *refusingFinder) CallViaBidi(_ context.Context, _ string, req *pb.RPCRequest) (*pb.RPCResponse, bool, error) {
	f.refused.Add(1)
	return nil, true, &GradeTooLowError{Handler: req.Handler, Class: req.Context["opClass"],
		Current: grade.GradeC, Required: grade.GradeB}
}

// A grade refusal from the finder's bidi send reaches the caller typed, is not
// retried over a dynamic stream, and is no availability failure: any number of
// them leave the path's circuit closed with nothing counted.
func TestAFindersGradeRefusalIsReturnedTypedAndCountsAgainstNoCircuit(t *testing.T) {
	for _, tc := range []struct {
		name     string
		noRoutes bool
		node     string // the node the circuit is keyed by on this path
	}{
		{"probed routes", false, "test-node"},
		{"no routes", true, "remote-test"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sess := newGradedSession(aether.ProtoQUIC, "ran")
			defer forgetSession(sess)
			finder := &refusingFinder{mockFinder: mockFinder{session: sess}, noRoutes: tc.noRoutes}
			caller := NewHWPCaller(finder)
			defer caller.Close()

			for i := 0; i < callerBreakerThreshold+1; i++ {
				_, err := caller.Call(WithOpClass(context.Background(), "critical"), "svc", "svc.Op", nil)
				var gte *GradeTooLowError
				if !errors.As(err, &gte) || gte.Current != grade.GradeC || gte.Required != grade.GradeB {
					t.Fatalf("call %d returned %v; the finder's refusal must reach the caller as "+
						"its *GradeTooLowError", i, err)
				}
			}
			if n := finder.refused.Load(); n != callerBreakerThreshold+1 {
				t.Fatalf("the finder refused %d of %d calls; a circuit stood in for it", n,
					callerBreakerThreshold+1)
			}
			if n := sess.sends.Load(); n != 0 {
				t.Errorf("%d request(s) went out on a dynamic stream after the finder refused the "+
					"route on grade", n)
			}
			cb := caller.getBreaker("svc.Op", tc.node, sess)
			cb.mu.Lock()
			state, failures := cb.state, cb.failures
			cb.mu.Unlock()
			if state != CircuitClosed || failures != 0 {
				t.Errorf("the circuit is %v with %d failure(s) after grade refusals alone; a "+
					"refusal is not an availability failure", state, failures)
			}
		})
	}

	t.Run("CallNode", func(t *testing.T) {
		finder := &refusingFinder{}
		caller := NewHWPCaller(finder)
		defer caller.Close()

		_, err := caller.CallNode(WithOpClass(context.Background(), "critical"), "node-1", "svc", "svc.Op", nil)
		var gte *GradeTooLowError
		if !errors.Is(err, ErrGradeTooLow) || !errors.As(err, &gte) || gte.Required != grade.GradeB {
			t.Fatalf("CallNode returned %v; the finder's refusal must reach the caller as its "+
				"*GradeTooLowError", err)
		}
		if errors.Is(err, ErrNoRouteToNode) || errors.Is(err, ErrHandlerError) {
			t.Errorf("CallNode reported a grade refusal as %v", err)
		}
	})
}
