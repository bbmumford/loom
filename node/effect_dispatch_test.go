/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/rpc/pb"
	swarmpb "github.com/bbmumford/swarm/proto/pb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/bbmumford/loom/node/handlers"
	"github.com/bbmumford/loom/pkg/dispatch"
	"github.com/bbmumford/loom/pkg/rpc"
	"github.com/bbmumford/loom/pkg/rpc/effect"
	rpcscope "github.com/bbmumford/loom/pkg/rpc/scope"
)

// These tests hold the receiving side to one execution per call: the dedup
// key joins every attempt of one call whichever peer or channel delivered it,
// concurrent arrivals run once, a relay never runs what it relays, and no node
// runs a call again after a send whose outcome is unknown, unless its handler
// is Repeatable.

// effectProbe counts executions, may hold each one until release is closed,
// and declares class (handlers.EffectDeclarer).
type effectProbe struct {
	name    string
	class   effect.Class
	release chan struct{}
	calls   int32
}

func (h *effectProbe) Name() string                { return h.name }
func (h *effectProbe) Role() string                { return "example.effect" }
func (h *effectProbe) RequiresAuth() bool          { return false }
func (h *effectProbe) AllowedAuthTypes() []string  { return nil }
func (h *effectProbe) Scopes() []string            { return nil }
func (h *effectProbe) Scope() handlers.Scope       { return handlers.ScopeNone }
func (h *effectProbe) AllowedPartitions() []string { return nil }
func (h *effectProbe) Effect() effect.Class        { return h.class }
func (h *effectProbe) ExecuteRPC(_ context.Context, req *handlers.RPCRequest) (*handlers.RPCResponse, error) {
	n := atomic.AddInt32(&h.calls, 1)
	if h.release != nil {
		<-h.release
	}
	return &handlers.RPCResponse{Success: true, Payload: []byte(fmt.Sprintf("run-%d", n))}, nil
}

const effectProbeFQN = "example.effect.Run"

func effectServer(t *testing.T, probe *effectProbe) *RPCServer {
	t.Helper()
	registry := handlers.NewHandlerRegistry()
	if err := registry.RegisterRPC(probe); err != nil {
		t.Fatalf("register: %v", err)
	}
	s := NewRPCServer(registry)
	s.SetAuthValidator(typedPrincipalValidator{})
	return s
}

// principalRequest is a sealed request for the probe from subject, pinned to
// the transport partition it is sent over.
func principalRequest(id, subject string) *pb.RPCRequest {
	return &pb.RPCRequest{
		Id:      id,
		Handler: effectProbeFQN,
		Context: map[string]string{dispatch.LogicalIDKey: id},
		Principal: &pb.AuthenticatedPrincipal{
			Subject:   subject,
			Axes:      []*pb.ScopeAxis{{Name: rpcscope.AxisTransportPinned, Value: "part-T"}},
			Partition: "part-A",
		},
	}
}

func overTransport(peer string) context.Context {
	return withCallerNode(handlers.WithTransportPartition(context.Background(), "part-T"), peer)
}

// Two principals that send the same request id each get their own run and
// their own response.
func TestTwoPrincipalsSharingARequestIDNeverShareAResponse(t *testing.T) {
	probe := &effectProbe{name: effectProbeFQN}
	s := effectServer(t, probe)

	a := s.handleRequest(overTransport("peer-a"), principalRequest("42", "subject-A"))
	b := s.handleRequest(overTransport("peer-a"), principalRequest("42", "subject-B"))
	if !a.Success || !b.Success {
		t.Fatalf("calls failed: %q / %q", a.Error, b.Error)
	}
	if got := atomic.LoadInt32(&probe.calls); got != 2 {
		t.Fatalf("the handler ran %d times for two principals sharing an id, want 2: one "+
			"principal was served the other's response", got)
	}
	if string(a.Payload) == string(b.Payload) {
		t.Fatalf("both principals received %q", a.Payload)
	}
}

// One principal's call reaching this node through two peers, as a direct
// route and a relayed route deliver it, runs once: the dedup key is the
// sealed principal, not the delivering peer.
func TestOneCallReachingThroughTwoPeersRunsOnce(t *testing.T) {
	probe := &effectProbe{name: effectProbeFQN}
	s := effectServer(t, probe)

	direct := s.handleRequest(overTransport("origin"), principalRequest("call-1", "subject-A"))
	relayed := s.handleRequest(overTransport("relay"), principalRequest("call-1", "subject-A"))
	if !direct.Success || !relayed.Success {
		t.Fatalf("calls failed: %q / %q", direct.Error, relayed.Error)
	}
	if got := atomic.LoadInt32(&probe.calls); got != 1 {
		t.Fatalf("the handler ran %d times for one call delivered by two peers, want 1", got)
	}
	if string(direct.Payload) != string(relayed.Payload) {
		t.Fatalf("the two deliveries got %q and %q; the second must get the first's answer",
			direct.Payload, relayed.Payload)
	}
}

// A bidi attempt arrives with its channel's wire id as Id and the logical id
// in Context; the dynamic-stream attempt that follows arrives with the logical
// id as Id. They are one call and run once.
func TestABidiAttemptAndItsDynamicStreamAttemptRunOnce(t *testing.T) {
	probe := &effectProbe{name: effectProbeFQN}
	s := effectServer(t, probe)
	ctx := withCallerNode(context.Background(), "peer-a")

	viaBidi := &pb.RPCRequest{Id: "wire-7", Handler: effectProbeFQN, Context: map[string]string{dispatch.LogicalIDKey: "call-1"}}
	viaStream := &pb.RPCRequest{Id: "call-1", Handler: effectProbeFQN, Context: map[string]string{dispatch.LogicalIDKey: "call-1"}}
	first := s.handleRequest(ctx, viaBidi)
	second := s.handleRequest(ctx, viaStream)
	if !first.Success || !second.Success {
		t.Fatalf("calls failed: %q / %q", first.Error, second.Error)
	}
	if got := atomic.LoadInt32(&probe.calls); got != 1 {
		t.Fatalf("the handler ran %d times for one call's bidi and dynamic-stream attempts, want 1", got)
	}
	if first.Id != "wire-7" || second.Id != "call-1" {
		t.Fatalf("responses carry ids %q and %q; each must echo the id its own request carried",
			first.Id, second.Id)
	}
}

// The same logical id with a different payload is a different request.
func TestOneRequestIDWithAnotherPayloadRunsAgain(t *testing.T) {
	probe := &effectProbe{name: effectProbeFQN}
	s := effectServer(t, probe)
	ctx := withCallerNode(context.Background(), "peer-a")

	for _, body := range []string{"first", "second"} {
		req := &pb.RPCRequest{Id: "42", Handler: effectProbeFQN, Payload: []byte(body)}
		if r := s.handleRequest(ctx, req); !r.Success {
			t.Fatalf("call failed: %s", r.Error)
		}
	}
	if got := atomic.LoadInt32(&probe.calls); got != 2 {
		t.Fatalf("the handler ran %d times for two bodies under one id, want 2: the second body "+
			"was answered with the first's response", got)
	}
}

// Arrivals of one call while its first run is in flight wait for that run and
// share its response; the handler runs once.
func TestConcurrentArrivalsOfOneCallRunOnce(t *testing.T) {
	probe := &effectProbe{name: effectProbeFQN, release: make(chan struct{})}
	s := effectServer(t, probe)
	ctx := withCallerNode(context.Background(), "peer-a")

	const arrivals = 5
	var wg sync.WaitGroup
	resps := make([]*pb.RPCResponse, arrivals)
	for i := 0; i < arrivals; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			resps[i] = s.handleRequest(ctx, &pb.RPCRequest{Id: fmt.Sprintf("attempt-%d", i), Handler: effectProbeFQN,
				Context: map[string]string{dispatch.LogicalIDKey: "call-1"}})
		}(i)
	}
	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadInt32(&probe.calls) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(50 * time.Millisecond) // let every arrival reach the coalescer
	close(probe.release)
	wg.Wait()

	if got := atomic.LoadInt32(&probe.calls); got != 1 {
		t.Fatalf("the handler ran %d times for %d concurrent arrivals of one call, want 1", got, arrivals)
	}
	for i, r := range resps {
		if !r.Success || string(r.Payload) != "run-1" {
			t.Fatalf("arrival %d got (%v, %q); every arrival shares the one run's response", i, r.Success, r.Payload)
		}
		if want := fmt.Sprintf("attempt-%d", i); r.Id != want {
			t.Fatalf("arrival %d's response carries id %q, want its own %q", i, r.Id, want)
		}
	}
}

// recordingForwarder answers every Forward with resp and err, counting them.
type recordingForwarder struct {
	resp  *pb.RPCResponse
	err   error
	calls atomic.Int32
}

func (f *recordingForwarder) Forward(context.Context, *pb.RPCRequest) (*pb.RPCResponse, error) {
	f.calls.Add(1)
	return f.resp, f.err
}

// A node that registers the handler but is not the request's target is a
// relay: it forwards and never runs the handler itself. With no way to forward
// it refuses, still without running it.
func TestARelayThatIsNotTheTargetForwardsAndNeverRuns(t *testing.T) {
	probe := &effectProbe{name: effectProbeFQN}
	s := effectServer(t, probe)
	s.SetNodeInfo("relay-node", "", "")
	req := func() *pb.RPCRequest {
		return &pb.RPCRequest{Id: "call-1", Handler: effectProbeFQN, TargetNodeId: "target-node", RouteList: []string{"target-node"}}
	}

	unforwarded := s.handleRequest(withCallerNode(context.Background(), "origin"), req())
	if unforwarded.Success {
		t.Fatal("a relay with no forwarder answered success")
	}

	fwd := &recordingForwarder{resp: &pb.RPCResponse{Success: true, Payload: []byte("from-target")}}
	s.SetForwarder(fwd)
	resp := s.handleRequest(withCallerNode(context.Background(), "origin"), req())
	if !resp.Success || string(resp.Payload) != "from-target" || fwd.calls.Load() != 1 {
		t.Fatalf("the relay answered (%v, %q) after %d forward(s); it must forward to the target",
			resp.Success, resp.Payload, fwd.calls.Load())
	}
	if got := atomic.LoadInt32(&probe.calls); got != 0 {
		t.Fatalf("the relay ran the handler %d time(s); the caller chose another node to run it", got)
	}
}

// busyAdvisor makes shouldForwardWithContext forward every request: healthy
// peers and a grade A path while this node is loaded.
type busyAdvisor struct{}

func (busyAdvisor) LocalLoad() int32                { return 60 }
func (busyAdvisor) PeerDispatchHealth() float64     { return 1 }
func (busyAdvisor) BestGradeToHandler(string) Grade { return GradeA }

// A load-relief forward that never left leaves the request to run here. One
// that left and went unanswered may have run on the peer, so only a
// Repeatable handler runs here after it.
func TestALoadReliefForwardThatLeftIsNotRunAgainHereForAnUnsafeHandler(t *testing.T) {
	for _, tc := range []struct {
		name    string
		class   effect.Class
		err     error
		wantRun bool
	}{
		{"unsent, Unsafe", effect.Unsafe, dispatch.NotSent(errors.New("no route")), true},
		{"unanswered, Unsafe", effect.Unsafe, errors.New("receive: connection reset"), false},
		{"unanswered, Idempotent", effect.Idempotent, errors.New("receive: connection reset"), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			probe := &effectProbe{name: effectProbeFQN, class: tc.class}
			s := effectServer(t, probe)
			s.SetLoadAdvisor(busyAdvisor{})
			s.SetForwarder(&recordingForwarder{err: tc.err})
			atomic.StoreInt32(&s.activeHandlers, 60)

			resp := s.handleRequest(withCallerNode(context.Background(), "origin"),
				&pb.RPCRequest{Id: "call-1", Handler: effectProbeFQN})
			ran := atomic.LoadInt32(&probe.calls) == 1
			if ran != tc.wantRun {
				t.Fatalf("the handler ran: %v, want %v (response %v %q)", ran, tc.wantRun, resp.Success, resp.Error)
			}
			if !tc.wantRun && resp.Metadata["refusal"] != "ambiguous" {
				t.Fatalf("the failed response carries metadata %v; an unanswered forward of an Unsafe "+
					"call reaches the origin stamped ambiguous", resp.Metadata)
			}
		})
	}
}

// lostSession takes every request and never answers it.
type lostSession struct{ hopSession }

func (s *lostSession) OpenStream(ctx context.Context, cfg aether.StreamConfig) (aether.Stream, error) {
	s.opens.Add(1)
	return &lostStream{hopStream: hopStream{sess: &s.hopSession, id: cfg.StreamID}}, nil
}

type lostStream struct{ hopStream }

func (st *lostStream) Receive(context.Context) ([]byte, error) {
	return nil, errors.New("read: connection reset")
}

// When the hop a request's route list names takes it and gives no answer, the
// forwarder re-routes it to the target only for a Repeatable handler; an
// Unsafe one ends ambiguous, never reaching the target a second way.
func TestTheForwarderDoesNotReRouteAnUnsafeCallAfterAnUnansweredHop(t *testing.T) {
	for _, tc := range []struct {
		name     string
		class    effect.Class
		wantSent int
	}{
		{"Unsafe", effect.Unsafe, 0},
		{"Idempotent", effect.Idempotent, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hop := &lostSession{hopSession{remote: "hop", proto: aether.ProtoQUIC}}
			target := &hopSession{remote: "target", proto: aether.ProtoQUIC}
			f := forwarderWith(&hop.hopSession, target)
			f.rt.connMgr.meshSessions["hop"] = hop
			registry := handlers.NewHandlerRegistry()
			if err := registry.RegisterRPC(&effectProbe{name: "test.forward.Run", class: tc.class}); err != nil {
				t.Fatal(err)
			}
			f.rt.rpcServer = NewRPCServer(registry)

			req := forwardRequest("target", "")
			req.RouteList = []string{"hop", "target"}
			_, err := f.Forward(context.Background(), req)

			if got := target.sends(); got != tc.wantSent {
				t.Fatalf("the target received %d request(s) after the routed hop went unanswered, "+
					"want %d", got, tc.wantSent)
			}
			if tc.class == effect.Unsafe && !errors.Is(err, dispatch.ErrAmbiguousEffect) {
				t.Fatalf("Forward returned %v; an Unsafe call whose hop went unanswered is ambiguous", err)
			}
		})
	}
}

// The effect classes a node registers ride its peer record beside the FQNs,
// and a caller's RoleTable reads them back. The least class among the
// holders wins, so a holder whose record carries no class makes the
// operation Unsafe.
func TestEffectClassesRideThePeerRecordToTheRoleTable(t *testing.T) {
	echo := func(ctx context.Context, req proto.Message) (proto.Message, error) { return req, nil }
	reg := rpc.NewRegistry()
	for _, h := range []rpc.Handler{
		{Operation: "Read", Effect: rpc.EffectReadOnly},
		{Operation: "Put", Effect: rpc.EffectIdempotent},
		{Operation: "Send"},
	} {
		h.Namespace, h.Domain, h.Func, h.Scope = "test", "publish", echo, rpc.ScopeNone
		h.Request, h.Response = (*wrapperspb.StringValue)(nil), (*wrapperspb.StringValue)(nil)
		if err := reg.Register(h); err != nil {
			t.Fatal(err)
		}
	}
	node := &capturingPublishNode{stubSwarmNode: &stubSwarmNode{}}
	NewPeerPublisher(node, nil, reg).publishOnce()
	var rec swarmpb.PeerRecord
	if err := proto.Unmarshal(node.body, &rec); err != nil {
		t.Fatal(err)
	}
	if rec.Capabilities == nil {
		rec.Capabilities = &swarmpb.Capabilities{}
	}
	rec.Capabilities.Roles = []string{"test.publish"}

	table, err := NewRoleTable(nil, "self")
	if err != nil {
		t.Fatal(err)
	}
	table.applyRecord(peerRecordToRoleRecord(&rec, "holder-1"), peerRecordToInfo(&rec, "holder-1"))
	for fqn, want := range map[string]effect.Class{
		"test.publish.Read": effect.ReadOnly,
		"test.publish.Put":  effect.Idempotent,
		"test.publish.Send": effect.Unsafe,
	} {
		if got, ok := table.HandlerEffect("test.publish", fqn); !ok || got != want {
			t.Errorf("HandlerEffect(%s) = (%v, %v), want (%v, true)", fqn, got, ok, want)
		}
	}
	if _, ok := table.HandlerEffect("test.publish", "test.publish.Absent"); ok {
		t.Error("HandlerEffect reports a class for an FQN no peer advertises")
	}

	bare := &swarmpb.PeerRecord{RpcHandlers: rec.RpcHandlers, Capabilities: &swarmpb.Capabilities{Roles: []string{"test.publish"}}}
	table.applyRecord(peerRecordToRoleRecord(bare, "holder-2"), peerRecordToInfo(bare, "holder-2"))
	if got, _ := table.HandlerEffect("test.publish", "test.publish.Read"); got != effect.Unsafe {
		t.Errorf("with a second holder advertising no class, HandlerEffect = %v, want Unsafe", got)
	}
}

// A bidi channel that closes after taking a request is reported as
// dispatch.ErrChannelClosed with ok=true, because the peer may have run the
// request; it is evicted all the same.
func TestABidiThatClosesDuringTheCallIsReportedClosedNotAbsent(t *testing.T) {
	m, sess := bidiManager(t)
	done := make(chan struct{})
	b := &BidiRPC{done: done, pending: map[string]chan *pb.RPCResponse{},
		stream: &closingStream{done: done}}
	m.dispatchMu.Lock()
	m.bidisBySession[sess] = b
	m.dispatchMu.Unlock()

	_, ok, err := m.CallViaBidi(context.Background(), testNodeIDB, &pb.RPCRequest{Id: "call-1", Handler: "test.Op"})
	if !ok || !errors.Is(err, dispatch.ErrChannelClosed) {
		t.Fatalf("CallViaBidi = (ok %v, %v); a channel that closed after taking the request is "+
			"ErrChannelClosed, not 'no bidi'", ok, err)
	}
	m.dispatchMu.RLock()
	_, still := m.bidisBySession[sess]
	m.dispatchMu.RUnlock()
	if still {
		t.Fatal("the closed bidi was left registered")
	}
}

// closingStream accepts one send and then closes its bidi's done channel, as
// readLoop does when the session ends.
type closingStream struct {
	hopStream
	done chan struct{}
	once sync.Once
}

func (st *closingStream) Send(context.Context, []byte) error {
	st.once.Do(func() { close(st.done) })
	return nil
}

// ── Two holders of a single-use code login ──────────────────────────────────

// codeLogin is a login operation over a single-use code store both holders
// share, as two machines of one endpoint share one database. The run that
// consumes the code mints a session and answers it after a delay; any later
// run finds the code spent and answers an empty success at once, the shape a
// refused login answers in.
type codeLogin struct {
	store *codeStore
	runs  int32
}

type codeStore struct {
	mu    sync.Mutex
	spent bool
	mints int32
}

func (h *codeLogin) Name() string                { return "example.authentication.LoginWithEmailCode" }
func (h *codeLogin) Role() string                { return "example.authentication" }
func (h *codeLogin) RequiresAuth() bool          { return false }
func (h *codeLogin) AllowedAuthTypes() []string  { return nil }
func (h *codeLogin) Scopes() []string            { return nil }
func (h *codeLogin) Scope() handlers.Scope       { return handlers.ScopeNone }
func (h *codeLogin) AllowedPartitions() []string { return nil }
func (h *codeLogin) ExecuteRPC(context.Context, *handlers.RPCRequest) (*handlers.RPCResponse, error) {
	atomic.AddInt32(&h.runs, 1)
	h.store.mu.Lock()
	first := !h.store.spent
	h.store.spent = true
	if first {
		h.store.mints++
	}
	h.store.mu.Unlock()
	if !first {
		return &handlers.RPCResponse{Success: true}, nil
	}
	time.Sleep(100 * time.Millisecond)
	return &handlers.RPCResponse{Success: true, Payload: []byte("session-1")}, nil
}

// serverForwarder forwards every request to one server, as the relay's
// forwarder reaches the target.
type serverForwarder struct{ to *RPCServer }

func (f serverForwarder) Forward(ctx context.Context, req *pb.RPCRequest) (*pb.RPCResponse, error) {
	return f.to.handleRequest(withCallerNode(ctx, "relay"), req), nil
}

// twoHolderFinder offers the origin a direct route to the target holder and a
// relayed route through the second holder, as FindRoutes does when both
// machines of an endpoint are connected.
type twoHolderFinder struct{ direct, relay aether.Session }

func (f *twoHolderFinder) FindSession(context.Context, string, string) (aether.Session, error) {
	return f.direct, nil
}
func (f *twoHolderFinder) FindAnySession() (aether.Session, bool) { return f.direct, true }
func (f *twoHolderFinder) CallViaBidi(context.Context, string, *pb.RPCRequest) (*pb.RPCResponse, bool, error) {
	return nil, false, nil
}
func (f *twoHolderFinder) FindRoutes(context.Context, string, string) []aether.ProbeRoute {
	return []aether.ProbeRoute{
		{Session: f.direct, NodeID: "holder-t", TargetNodeID: "holder-t"},
		{Session: f.relay, NodeID: "holder-r", TargetNodeID: "holder-t", RouteList: []string{"holder-t"}},
	}
}
func (f *twoHolderFinder) IsSupersededByUpgrade(aether.Session) bool { return false }

// With two holders of the login reachable, the call runs once and returns the
// session it minted. Probed on both routes, the second holder would find the
// code spent and its empty answer, returned first, would beat the real
// session.
func TestTwoHoldersOfASingleUseCodeLoginMintOneSessionAndReturnIt(t *testing.T) {
	store := &codeStore{}
	onT, onR := &codeLogin{store: store}, &codeLogin{store: store}
	holder := func(id string, h *codeLogin) *RPCServer {
		registry := handlers.NewHandlerRegistry()
		if err := registry.RegisterRPC(h); err != nil {
			t.Fatal(err)
		}
		s := NewRPCServer(registry)
		s.SetNodeInfo(id, "", "")
		return s
	}
	target, relay := holder("holder-t", onT), holder("holder-r", onR)
	relay.SetForwarder(serverForwarder{to: target})

	caller := dispatch.NewHWPCaller(&twoHolderFinder{
		direct: &servedSession{hopSession: hopSession{remote: "holder-t", proto: aether.ProtoQUIC}, server: target},
		relay:  &servedSession{hopSession: hopSession{remote: "holder-r", proto: aether.ProtoQUIC}, server: relay},
	})
	defer caller.Close()

	out, err := caller.Call(context.Background(), "example.authentication", "example.authentication.LoginWithEmailCode", nil)
	if err != nil {
		t.Fatalf("Call: %v", err)
	}
	if string(out) != "session-1" {
		t.Fatalf("the login answered %q; the empty answer of a second run beat the real session", out)
	}
	time.Sleep(150 * time.Millisecond) // a second probe, had one been sent, would have run by now
	if store.mints != 1 || atomic.LoadInt32(&onT.runs)+atomic.LoadInt32(&onR.runs) != 1 {
		t.Fatalf("the login ran %d time(s) on the target and %d on the second holder and minted %d "+
			"session(s); want one run", onT.runs, onR.runs, store.mints)
	}
}
