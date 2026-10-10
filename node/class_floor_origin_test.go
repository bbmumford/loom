/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/rpc/pb"
	"google.golang.org/protobuf/proto"

	"github.com/bbmumford/loom/node/handlers"
	"github.com/bbmumford/loom/pkg/rpc"
)

// These tests cover the two places outside the forwarder where this package
// holds a request to its operation class: ConnectionManager.CallViaBidi, the
// send dispatch.HWPCaller makes for the first hop of a call, and
// RPCServer.handleRequest, which decides between running a request here and
// forwarding it.

// bidiManagerOn is bidiManager with the peer's session on a chosen transport,
// and a live bidi on that session whose stream answers every request with a
// success. sent counts the frames the bidi wrote.
func bidiManagerOn(t *testing.T, sess *probeSession) (m *ConnectionManager, sent func() int64) {
	t.Helper()
	m = registerTestManager()
	m.selfID = testNodeIDA
	m.rt = &Runtime{}
	sess.remote = aether.NodeID(testNodeIDB)

	stream := &bidiStream{}
	bidi := newBidiForTest(t, stream)
	stream.onSend = func(msg []byte) error { return replyTo(t, bidi, msg) }

	m.dispatchMu.Lock()
	m.meshSessions = map[string]aether.Session{testNodeIDB: sess}
	m.bidisBySession = map[aether.Session]*BidiRPC{sess: bidi}
	m.dispatchMu.Unlock()
	return m, stream.sent.Load
}

// CallViaBidi holds the session it sends on to the floor of the request's
// class. HWPCaller.CallNode reaches a named node through this method alone and
// never sees the session, so a refusal here is the only gate on that route's
// grade. A refusal is reported as handled, with the typed error and nothing
// written to the bidi; at or above the floor the request is sent once.
func TestCallViaBidiHoldsItsSessionToTheClassFloor(t *testing.T) {
	for _, tc := range []struct {
		name    string
		opClass string
		proto   aether.Protocol
		refused bool
		unknown bool
	}{
		{"the default class over a grade C session", "", aether.ProtoWebSocket, false, false},
		{"bulk over a grade C session", "bulk", aether.ProtoWebSocket, false, false},
		{"realtime over a grade C session", "realtime", aether.ProtoWebSocket, true, false},
		{"critical over a grade C session", "critical", aether.ProtoWebSocket, true, false},
		{"critical over a grade B session", "critical", aether.ProtoQUIC, false, false},
		{"critical over a grade A session", "critical", aether.ProtoNoise, false, false},
		{"an undeclared class over a grade A session", "turbo", aether.ProtoNoise, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sess := &probeSession{proto: tc.proto}
			m, sent := bidiManagerOn(t, sess)

			req := &pb.RPCRequest{Id: "req-1", Handler: "test.floor.Run", Context: map[string]string{}}
			if tc.opClass != "" {
				req.Context["opClass"] = tc.opClass
			}
			resp, ok, err := m.CallViaBidi(context.Background(), testNodeIDB, req)

			if !tc.refused {
				if !ok || err != nil || resp == nil || !resp.Success {
					t.Fatalf("CallViaBidi returned (%v, %v, %v); a %v session meets this class's floor",
						resp, ok, err, SessionGrade(sess))
				}
				if n := sent(); n != 1 {
					t.Fatalf("the bidi wrote %d frame(s), want 1", n)
				}
				return
			}
			if !ok {
				t.Fatalf("CallViaBidi reported no bidi (%v); a refusal reported that way sends "+
					"HWPCaller to its dynamic-stream arm over the same session", err)
			}
			if resp != nil || !errors.Is(err, rpc.ErrGradeTooLow) {
				t.Fatalf("CallViaBidi returned (%v, %v); a %v session is below this class's floor "+
					"and must be refused with rpc.ErrGradeTooLow", resp, err, SessionGrade(sess))
			}
			var gte *GradeTooLowError
			if !errors.As(err, &gte) || gte.UnknownClass != tc.unknown {
				t.Fatalf("CallViaBidi returned %#v; want a *GradeTooLowError with UnknownClass=%v",
					err, tc.unknown)
			}
			if !tc.unknown && (gte.Current != SessionGrade(sess) || gte.Current >= gte.Required) {
				t.Errorf("GradeTooLowError reports current %v, required %v; current must be the "+
					"session's %v and below required", gte.Current, gte.Required, SessionGrade(sess))
			}
			if n := sent(); n != 0 {
				t.Errorf("the bidi wrote %d frame(s) for a refused request", n)
			}
		})
	}
}

// countingForwarder is an RPCForwarder that records each request it is handed
// and answers with a fixed result.
type countingForwarder struct {
	calls atomic.Int32
	resp  *pb.RPCResponse
	err   error
}

func (f *countingForwarder) Forward(context.Context, *pb.RPCRequest) (*pb.RPCResponse, error) {
	f.calls.Add(1)
	return f.resp, f.err
}

// localRunServer builds an RPCServer that serves test.floor.Run locally and
// counts its runs.
func localRunServer(t *testing.T) (server *RPCServer, fqn string, runs *atomic.Int32) {
	t.Helper()
	runs = &atomic.Int32{}
	h := rpc.Handler{
		Namespace: "test", Domain: "floor", Operation: "Run",
		Func: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			runs.Add(1)
			return req, nil
		},
		Request:  (*localWebhookEvent)(nil),
		Response: (*localWebhookEvent)(nil),
		Scope:    rpc.ScopeNone,
	}
	source := rpc.NewRegistry()
	if err := source.Register(h); err != nil {
		t.Fatalf("Register: %v", err)
	}
	exported := handlers.NewHandlerRegistry()
	if err := source.ExportToHandlerRegistry(exported); err != nil {
		t.Fatalf("ExportToHandlerRegistry: %v", err)
	}
	return NewRPCServer(exported), h.FQN(), runs
}

func classRequest(id, handler, opClass string) *pb.RPCRequest {
	req := &pb.RPCRequest{Id: id, Handler: handler, Context: map[string]string{}}
	if opClass != "" {
		req.Context["opClass"] = opClass
	}
	return req
}

// A request naming a class the gate does not declare is refused by the server
// before it is run or forwarded, on each of the three ways handleRequest can
// dispatch it: a local run, a load-relief forward of a handler this node
// serves, and a forward of a handler it does not serve.
func TestTheServerRefusesAnUndeclaredClassBeforeRunningOrForwarding(t *testing.T) {
	t.Run("a handler served here", func(t *testing.T) {
		server, fqn, runs := localRunServer(t)

		resp := server.handleRequest(context.Background(), classRequest("u-1", fqn, "turbo"))
		if resp == nil || resp.Success {
			t.Fatalf("handleRequest returned %v; an undeclared class must be refused", resp)
		}
		if n := runs.Load(); n != 0 {
			t.Fatalf("the handler ran %d time(s) for a request naming an undeclared class", n)
		}

		// The same server runs a request that names a declared class, so the
		// refusal above is the class and not the fixture.
		if resp := server.handleRequest(context.Background(), classRequest("u-2", fqn, "standard")); !resp.Success || runs.Load() != 1 {
			t.Fatalf("premise wrong: a standard request was not served (%v, %d runs)", resp.Error, runs.Load())
		}
	})

	t.Run("a handler served here under load", func(t *testing.T) {
		server, fqn, runs := localRunServer(t)
		fwd := &countingForwarder{resp: &pb.RPCResponse{Success: true}}
		server.SetForwarder(fwd)
		server.SetLoadAdvisor(fakeAdvisor{health: 1.0, grade: GradeA})
		atomic.StoreInt32(&server.activeHandlers, 5000)
		if !server.shouldForwardWithContext(classRequest("u-3", fqn, "")) {
			t.Fatal("premise wrong: the server would not shed this request")
		}

		resp := server.handleRequest(context.Background(), classRequest("u-4", fqn, "turbo"))
		if resp == nil || resp.Success {
			t.Fatalf("handleRequest returned %v; an undeclared class must be refused", resp)
		}
		if runs.Load() != 0 || fwd.calls.Load() != 0 {
			t.Fatalf("the handler ran %d time(s) and the forwarder was asked %d time(s) for a "+
				"request naming an undeclared class", runs.Load(), fwd.calls.Load())
		}
	})

	t.Run("a handler served elsewhere", func(t *testing.T) {
		server := NewRPCServer(handlers.NewHandlerRegistry())
		fwd := &countingForwarder{resp: &pb.RPCResponse{Success: true}}
		server.SetForwarder(fwd)

		resp := server.handleRequest(context.Background(), classRequest("u-5", "test.floor.Run", "turbo"))
		if resp == nil || resp.Success {
			t.Fatalf("handleRequest returned %v; an undeclared class must be refused", resp)
		}
		if n := fwd.calls.Load(); n != 0 {
			t.Fatalf("the forwarder was asked %d time(s) for a request naming an undeclared class", n)
		}
	})
}

// A load-relief forward the forwarder refuses on grade leaves the request to
// run here. This node serves the handler and a local run leaves on no route,
// so no route floor applies to it; refusing instead would turn the node's own
// load shedding into a failed request it could have served.
func TestALoadReliefForwardRefusedOnGradeRunsLocally(t *testing.T) {
	server, fqn, runs := localRunServer(t)
	fwd := &countingForwarder{err: &GradeTooLowError{Handler: fqn, Class: "critical", Current: GradeC, Required: GradeB}}
	server.SetForwarder(fwd)
	server.SetLoadAdvisor(fakeAdvisor{health: 1.0, grade: GradeA})
	atomic.StoreInt32(&server.activeHandlers, 5000)

	resp := server.handleRequest(context.Background(), classRequest("l-1", fqn, "critical"))
	if n := fwd.calls.Load(); n != 1 {
		t.Fatalf("premise wrong: the forwarder was asked %d time(s), want 1", n)
	}
	if resp == nil || !resp.Success || runs.Load() != 1 {
		t.Fatalf("handleRequest returned %v with %d local run(s); a refused load-relief forward "+
			"must leave the request to run here once", resp, runs.Load())
	}
}
