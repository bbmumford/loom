/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/health"
	"github.com/ORBTR/aether/rpc/pb"

	"github.com/bbmumford/loom/pkg/rpc"
)

// These tests drive runtimeForwarder.Forward, the hop that relays a request
// this node does not serve, through the grade gate and the remaining-budget
// check it runs before every send. The next hop is a hopSession that records
// what reached it: the request it decoded and the deadline of the context the
// forwarder sent it under. A refusal is only proven if nothing was sent, so
// every refusal case also asserts the hop saw zero sends.

// hopSession is an aether.Session whose dynamic stream captures the forwarded
// request. Forward reaches it through callOverMeshSession's stream fallback,
// because the fixture registers no BidiRPC for it. proto picks the transport
// and so the grade grade.SessionGrade derives; aether.ProtoUnknown carries no
// capabilities and grades F.
type hopSession struct {
	remote aether.NodeID
	proto  aether.Protocol

	opens atomic.Int32

	mu           sync.Mutex
	got          []*pb.RPCRequest
	sendDeadline []time.Time // zero when the send context carried no deadline
}

func (s *hopSession) OpenStream(ctx context.Context, cfg aether.StreamConfig) (aether.Stream, error) {
	s.opens.Add(1)
	return &hopStream{sess: s, id: cfg.StreamID}, nil
}
func (s *hopSession) AcceptStream(context.Context) (aether.Stream, error) {
	return nil, context.Canceled
}
func (s *hopSession) AcceptStreamByID(context.Context, uint64) (aether.Stream, error) {
	return nil, context.Canceled
}
func (s *hopSession) LocalNodeID() aether.NodeID                                { return "self" }
func (s *hopSession) RemoteNodeID() aether.NodeID                               { return s.remote }
func (s *hopSession) LocalPeerID() aether.PeerID                                { return aether.PeerID{} }
func (s *hopSession) RemotePeerID() aether.PeerID                               { return aether.PeerID{} }
func (s *hopSession) Capabilities() aether.Capabilities                         { return 0 }
func (s *hopSession) Ping(context.Context) (time.Duration, error)               { return 0, nil }
func (s *hopSession) GoAway(context.Context, aether.GoAwayReason, string) error { return nil }
func (s *hopSession) Close() error                                              { return nil }
func (s *hopSession) IsClosed() bool                                            { return false }
func (s *hopSession) Health() *health.Monitor                                   { return nil }
func (s *hopSession) SessionKey() []byte                                        { return nil }
func (s *hopSession) ConnectionID() aether.ConnectionID                         { return aether.ConnectionID{} }
func (s *hopSession) CongestionWindow() int64                                   { return 0 }
func (s *hopSession) Protocol() aether.Protocol                                 { return s.proto }
func (s *hopSession) Metrics() aether.SessionMetrics                            { return aether.SessionMetrics{} }

func (s *hopSession) sends() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.got)
}

// hopStream decodes what the forwarder sends and answers it with a success,
// echoing the request id as the RPC server does.
type hopStream struct {
	sess *hopSession
	id   uint64

	mu   sync.Mutex
	last *pb.RPCRequest
}

func (st *hopStream) StreamID() uint64 { return st.id }
func (st *hopStream) Send(ctx context.Context, data []byte) error {
	req, err := pb.UnmarshalRequest(data)
	if err != nil {
		return err
	}
	dl, _ := ctx.Deadline()
	st.sess.mu.Lock()
	st.sess.got = append(st.sess.got, req)
	st.sess.sendDeadline = append(st.sess.sendDeadline, dl)
	st.sess.mu.Unlock()
	st.mu.Lock()
	st.last = req
	st.mu.Unlock()
	return nil
}
func (st *hopStream) Receive(context.Context) ([]byte, error) {
	st.mu.Lock()
	req := st.last
	st.mu.Unlock()
	if req == nil {
		return nil, errors.New("hopStream: receive before send")
	}
	return pb.MarshalResponse(&pb.RPCResponse{Id: req.Id, Success: true})
}
func (st *hopStream) Close() error                   { return nil }
func (st *hopStream) Reset(aether.ResetReason) error { return nil }
func (st *hopStream) SetPriority(uint8, uint64)      {}
func (st *hopStream) Config() aether.StreamConfig    { return aether.StreamConfig{StreamID: st.id} }
func (st *hopStream) IsOpen() bool                   { return true }
func (st *hopStream) Conn() net.Conn                 { return nil }

// forwarderWith builds a forwarder whose runtime holds exactly the given mesh
// sessions, keyed by remote node id, with no LAD cache, swarm or route engine,
// so the candidate set is the direct target session and nothing else.
func forwarderWith(sessions ...*hopSession) *runtimeForwarder {
	m := &ConnectionManager{
		peers:        map[string]*peerConn{},
		budget:       DefaultConnectionBudget(),
		meshSessions: map[string]aether.Session{},
	}
	m.scaler = NewConnectionScaler(m, nil)
	for _, s := range sessions {
		m.meshSessions[string(s.remote)] = s
	}
	rt := &Runtime{connMgr: m, identity: &NodeIdentity{NodeID: "self"}}
	m.rt = rt
	return &runtimeForwarder{rt: rt}
}

func forwardRequest(target, opClass string) *pb.RPCRequest {
	req := &pb.RPCRequest{
		Id:           "req-1",
		Handler:      "test.forward.Run",
		TargetNodeId: target,
		Hops:         1,
		Context:      map[string]string{},
	}
	if opClass != "" {
		req.Context["opClass"] = opClass
	}
	return req
}

// ── The remaining budget only shrinks ──────────────────────────────────────

// A caller with less time left than the hop needs is refused at the
// forwarder, typed, before anything is sent: adaptiveRPCTimeout gives a
// session with no RTT estimate its 5 s floor, and the caller here has 1 s.
// Sending anyway is what a budget extension did; sending inside the 1 s
// would hand the next hop a call it cannot finish.
func TestAForwardWithLessTimeThanTheHopNeedsFailsTypedBeforeSending(t *testing.T) {
	hop := &hopSession{remote: "target", proto: aether.ProtoNoise}
	f := forwarderWith(hop)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if need := adaptiveRPCTimeout(hop); need <= time.Second {
		t.Fatalf("fixture wrong: the hop needs %v, which a 1 s caller budget covers", need)
	}

	_, err := f.Forward(ctx, forwardRequest("target", ""))
	if !errors.Is(err, rpc.ErrRouteDeadline) {
		t.Fatalf("Forward returned %v; a caller budget below the hop's need must fail "+
			"with rpc.ErrRouteDeadline at the forwarder", err)
	}
	var rde *RouteDeadlineError
	if !errors.As(err, &rde) {
		t.Fatalf("Forward returned %T; the refusal must be a *RouteDeadlineError that "+
			"names the remaining budget and the hop's need", err)
	}
	if rde.Remaining >= rde.Need {
		t.Errorf("RouteDeadlineError reports remaining %v and need %v; a refusal is "+
			"only right when remaining is below need", rde.Remaining, rde.Need)
	}
	if n := hop.sends(); n != 0 {
		t.Errorf("the next hop received %d request(s); the refusal must come before "+
			"the send", n)
	}
}

// The forwarded request carries the caller's remaining time, not the budget
// the origin stamped. The RPC server at the next hop bounds its handler with
// TimeoutNs from the moment it receives the request, so a TimeoutNs copied
// unchanged from the origin restarts the clock at every hop. Both legs of
// Forward are checked: the RouteList fast path and the re-route probe.
func TestTheNextHopNeverReceivesMoreTimeThanTheCallerHasLeft(t *testing.T) {
	for _, leg := range []string{"route list", "re-route"} {
		t.Run(leg, func(t *testing.T) {
			hop := &hopSession{remote: "target", proto: aether.ProtoNoise}
			f := forwarderWith(hop)

			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			callerDeadline, _ := ctx.Deadline()

			req := forwardRequest("target", "")
			req.TimeoutNs = int64(30 * time.Second) // what the origin stamped
			if leg == "route list" {
				req.RouteList = []string{"target"}
			}

			if _, err := f.Forward(ctx, req); err != nil {
				t.Fatalf("Forward failed with %v; 8 s covers the hop's 5 s need", err)
			}
			if n := hop.sends(); n != 1 {
				t.Fatalf("the next hop received %d request(s), want 1", n)
			}
			hop.mu.Lock()
			got, sendDL := hop.got[0], hop.sendDeadline[0]
			hop.mu.Unlock()

			if budget := time.Duration(got.TimeoutNs); budget <= 0 || budget > 8*time.Second {
				t.Errorf("the next hop received TimeoutNs %v; it must be the caller's "+
					"remaining time, at most 8 s, never the origin's 30 s", budget)
			}
			if got.Deadline <= 0 || got.Deadline > callerDeadline.UnixNano() {
				t.Errorf("the next hop received Deadline %d; it must be no later than "+
					"the caller's %d", got.Deadline, callerDeadline.UnixNano())
			}
			if sendDL.IsZero() || sendDL.After(callerDeadline) {
				t.Errorf("the send ran under deadline %v; the caller's is %v, and a "+
					"forward never gains time", sendDL, callerDeadline)
			}
		})
	}
}

// ── The grade gate runs for every request ──────────────────────────────────

// Each operation class, the default included, is held to its floor: Bulk F,
// Standard C (the class of a request that names none), Realtime and Critical
// B. Below the floor the forwarder refuses with a *GradeTooLowError that
// carries the current and required grade and sends nothing; at or above it
// the request is forwarded once.
func TestEachOperationClassIsHeldToItsFloor(t *testing.T) {
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
		t.Run(tc.name, func(t *testing.T) {
			hop := &hopSession{remote: "target", proto: tc.proto}
			f := forwarderWith(hop)

			_, err := f.Forward(context.Background(), forwardRequest("target", tc.opClass))
			if !tc.refused {
				if err != nil {
					t.Fatalf("Forward refused with %v; a %v path meets this class's floor",
						err, SessionGrade(hop))
				}
				if n := hop.sends(); n != 1 {
					t.Fatalf("the next hop received %d request(s), want 1", n)
				}
				return
			}
			if !errors.Is(err, rpc.ErrGradeTooLow) {
				t.Fatalf("Forward returned %v; a %v path is below this class's floor and "+
					"must be refused with rpc.ErrGradeTooLow", err, SessionGrade(hop))
			}
			var gte *GradeTooLowError
			if !errors.As(err, &gte) {
				t.Fatalf("Forward returned %T; the refusal must be a *GradeTooLowError", err)
			}
			if gte.Current != SessionGrade(hop) || gte.Current >= gte.Required {
				t.Errorf("GradeTooLowError reports current %v, required %v; current must "+
					"be the path's %v and below required", gte.Current, gte.Required,
					SessionGrade(hop))
			}
			if n := hop.sends(); n != 0 {
				t.Errorf("the next hop received %d request(s) below the floor", n)
			}
		})
	}
}

// An operation class the gate does not know is refused, not mapped to another
// class: mapping it to Bulk let any misspelt or invented class through over
// every connected path.
func TestAnUnknownOperationClassIsRefusedTypedWithoutSending(t *testing.T) {
	hop := &hopSession{remote: "target", proto: aether.ProtoNoise}
	f := forwarderWith(hop)

	_, err := f.Forward(context.Background(), forwardRequest("target", "turbo"))
	if !errors.Is(err, rpc.ErrGradeTooLow) {
		t.Fatalf("Forward returned %v for an undeclared class; it must be refused with "+
			"rpc.ErrGradeTooLow", err)
	}
	var gte *GradeTooLowError
	if !errors.As(err, &gte) || !gte.UnknownClass || gte.Class != "turbo" {
		t.Fatalf("Forward returned %#v; the refusal must be a *GradeTooLowError naming "+
			"the class and marking it unknown", err)
	}
	if n := hop.sends(); n != 0 {
		t.Errorf("the next hop received %d request(s) for an undeclared class", n)
	}
}

// The RouteList fast path is gated as well. A hop the origin chose is still
// held to the class's floor; below it the request is not sent there, and with
// no other candidate the forwarder refuses typed.
func TestTheRouteListHopIsHeldToTheFloor(t *testing.T) {
	hop := &hopSession{remote: "target", proto: aether.ProtoWebSocket}
	f := forwarderWith(hop)

	req := forwardRequest("target", "critical")
	req.RouteList = []string{"target"}
	_, err := f.Forward(context.Background(), req)
	if !errors.Is(err, rpc.ErrGradeTooLow) {
		t.Fatalf("Forward returned %v; a grade C route-list hop is below the critical "+
			"floor and must be refused with rpc.ErrGradeTooLow", err)
	}
	if n := hop.sends(); n != 0 {
		t.Errorf("the route-list hop received %d request(s) below the floor", n)
	}
}
