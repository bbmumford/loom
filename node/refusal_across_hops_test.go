/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/rpc/pb"

	"github.com/bbmumford/loom/node/handlers"
	"github.com/bbmumford/loom/pkg/dispatch"
	"github.com/bbmumford/loom/pkg/rpc"
)

// These tests carry a call across one hop: dispatch.HWPCaller at the origin
// sends over a session whose far end is an RPCServer, and that server's
// forwarder refuses the next hop. The refusal has to come back to the origin's
// caller as the typed error the forwarder made, not as a handler rejection.

// servedSession is a mesh session whose far end is server: each stream hands
// the request it is sent to server.handleRequest and returns the response, as
// RPCServer.ServeMeshStream does for a real session.
type servedSession struct {
	hopSession
	server *RPCServer
}

func (s *servedSession) OpenStream(_ context.Context, cfg aether.StreamConfig) (aether.Stream, error) {
	return &servedStream{hopStream: hopStream{id: cfg.StreamID}, server: s.server}, nil
}

type servedStream struct {
	hopStream
	server *RPCServer
	resp   []byte
}

func (st *servedStream) Send(ctx context.Context, data []byte) error {
	req, err := pb.UnmarshalRequest(data)
	if err != nil {
		return err
	}
	st.resp, err = pb.MarshalResponse(st.server.handleRequest(ctx, req))
	return err
}

func (st *servedStream) Receive(context.Context) ([]byte, error) {
	if st.resp == nil {
		return nil, errors.New("servedStream: receive before send")
	}
	return st.resp, nil
}

// oneHopFinder is the origin's dispatch.SessionFinder: one route, straight
// to the far node over session, with no bidi channel.
type oneHopFinder struct{ session aether.Session }

func (f *oneHopFinder) FindSession(context.Context, string, string) (aether.Session, error) {
	return f.session, nil
}
func (f *oneHopFinder) FindAnySession() (aether.Session, bool) { return f.session, true }
func (f *oneHopFinder) CallViaBidi(context.Context, string, *pb.RPCRequest) (*pb.RPCResponse, bool, error) {
	return nil, false, nil
}
func (f *oneHopFinder) FindRoutes(context.Context, string, string) []aether.ProbeRoute {
	return []aether.ProbeRoute{{Session: f.session, NodeID: "far", TargetNodeID: "target"}}
}
func (f *oneHopFinder) IsSupersededByUpgrade(aether.Session) bool { return false }

// originOver builds the origin's caller over a grade B session to a far node
// that serves nothing itself and forwards through fwd.
func originOver(t *testing.T, fwd RPCForwarder) *dispatch.HWPCaller {
	t.Helper()
	far := NewRPCServer(handlers.NewHandlerRegistry())
	far.SetForwarder(fwd)
	caller := dispatch.NewHWPCaller(&oneHopFinder{session: &servedSession{
		hopSession: hopSession{remote: "far", proto: aether.ProtoQUIC},
		server:     far,
	}})
	t.Cleanup(caller.Close)
	return caller
}

// The far node's forwarder refuses the next hop on grade, and the origin's
// caller gets that refusal: rpc.ErrGradeTooLow, with the grade the far node
// had and the floor it missed, and not a handler error.
func TestAGradeRefusalOneHopOnReachesTheOriginTyped(t *testing.T) {
	next := &hopSession{remote: "target", proto: aether.ProtoWebSocket}
	caller := originOver(t, forwarderWith(next))

	ctx := dispatch.WithOpClass(context.Background(), "critical")
	_, err := caller.Call(ctx, "test.forward", "test.forward.Run", nil)
	if !errors.Is(err, rpc.ErrGradeTooLow) {
		t.Fatalf("the origin got %v; the far node refused on grade, which must reach the caller "+
			"as rpc.ErrGradeTooLow", err)
	}
	var gte *GradeTooLowError
	if !errors.As(err, &gte) || gte.Current != GradeC || gte.Required != GradeB || gte.UnknownClass {
		t.Fatalf("the origin got %#v; want the far node's refusal, current C and required B", err)
	}
	if errors.Is(err, dispatch.ErrHandlerError) {
		t.Errorf("the origin reports the refusal as a handler error: %v", err)
	}
	if n := next.sends(); n != 0 {
		t.Errorf("the hop below the floor received %d request(s)", n)
	}
}

// The far node's forwarder refuses the next hop because the caller's
// remaining time does not cover it, and the origin's caller gets that
// refusal: rpc.ErrRouteDeadline with the remaining time and the hop's need.
func TestADeadlineRefusalOneHopOnReachesTheOriginTyped(t *testing.T) {
	next := &hopSession{remote: "target", proto: aether.ProtoNoise}
	caller := originOver(t, forwarderWith(next))
	if need := adaptiveRPCTimeout(next); need <= time.Second {
		t.Fatalf("fixture wrong: the hop needs %v, which a 1 s caller budget covers", need)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, err := caller.Call(ctx, "test.forward", "test.forward.Run", nil)
	if !errors.Is(err, rpc.ErrRouteDeadline) {
		t.Fatalf("the origin got %v; the far node refused on the remaining budget, which must "+
			"reach the caller as rpc.ErrRouteDeadline", err)
	}
	var rde *RouteDeadlineError
	if !errors.As(err, &rde) || rde.Need <= 0 || rde.Remaining >= rde.Need {
		t.Fatalf("the origin got %#v; want the far node's refusal with remaining below need", err)
	}
	if errors.Is(err, dispatch.ErrHandlerError) {
		t.Errorf("the origin reports the refusal as a handler error: %v", err)
	}
	if n := next.sends(); n != 0 {
		t.Errorf("the hop the budget does not cover received %d request(s)", n)
	}
}

// classForgingForwarder stands for a hop that passes on a request whose class
// the next server does not declare: it hands the request to that server with
// the class rewritten, which no conforming origin would send.
type classForgingForwarder struct {
	next  *RPCServer
	class string
}

func (f *classForgingForwarder) Forward(ctx context.Context, req *pb.RPCRequest) (*pb.RPCResponse, error) {
	req.Context["opClass"] = f.class
	return f.next.handleRequest(ctx, req), nil
}

// A server's own refusal of an undeclared class is stamped like a
// forwarder's, so the origin's caller gets rpc.ErrGradeTooLow marked
// UnknownClass through the hop that relayed the response.
func TestAServersUndeclaredClassRefusalReachesTheOriginTyped(t *testing.T) {
	executor, _, runs := localRunServer(t)
	caller := originOver(t, &classForgingForwarder{next: executor, class: "turbo"})

	_, err := caller.Call(context.Background(), "test.floor", "test.floor.Run", nil)
	var gte *GradeTooLowError
	if !errors.Is(err, rpc.ErrGradeTooLow) || !errors.As(err, &gte) || !gte.UnknownClass || gte.Class != "turbo" {
		t.Fatalf("the origin got %v; want the executor's refusal of the undeclared class", err)
	}
	if errors.Is(err, dispatch.ErrHandlerError) {
		t.Errorf("the origin reports the refusal as a handler error: %v", err)
	}
	if n := runs.Load(); n != 0 {
		t.Errorf("the handler ran %d time(s) for an undeclared class", n)
	}
}

// A request whose absolute deadline has passed is refused by the forwarder's
// re-route with the typed deadline refusal, before a candidate is looked for
// and before anything is sent.
func TestAnExpiredRequestIsRefusedTypedBeforeReRoute(t *testing.T) {
	hop := &hopSession{remote: "target", proto: aether.ProtoNoise}
	f := forwarderWith(hop)

	req := forwardRequest("target", "")
	req.Deadline = time.Now().Add(-time.Second).UnixNano()
	_, err := f.Forward(context.Background(), req)
	if !errors.Is(err, rpc.ErrRouteDeadline) {
		t.Fatalf("Forward returned %v; a request past its deadline must be refused with "+
			"rpc.ErrRouteDeadline", err)
	}
	var rde *RouteDeadlineError
	if !errors.As(err, &rde) || rde.Remaining >= 0 || rde.Need != 0 {
		t.Fatalf("Forward returned %#v; want a *RouteDeadlineError with a negative remaining "+
			"time and no need, since no hop was chosen", err)
	}
	if n := hop.sends(); n != 0 {
		t.Errorf("the next hop received %d request(s) of an expired request", n)
	}
}
