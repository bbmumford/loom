/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/rpc/pb"
	ladcache "github.com/bbmumford/ledger/cache"
	"github.com/bbmumford/loom/directory"
	"github.com/bbmumford/loom/pkg/dispatch"
	"github.com/bbmumford/loom/pkg/rpc"
	"github.com/bbmumford/loom/pkg/rpc/scope"
	"github.com/bbmumford/swarm"
	swarmpb "github.com/bbmumford/swarm/proto/pb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

const (
	advertRole  = "orbtr.audit"
	advertFQN   = "orbtr.audit.Write"
	advertNodeC = "cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33"
)

type advertOp struct {
	name string
	req  *scope.Requirement
	tag  rpc.ScopeTag
}

// publishedAdvert is the record body PeerPublisher really writes for a registry
// holding ops, with the role a real publisher takes from Runtime's role list.
func publishedAdvert(t *testing.T, ops ...advertOp) []byte {
	t.Helper()
	echo := func(ctx context.Context, r proto.Message) (proto.Message, error) { return r, nil }
	reg := rpc.NewRegistry()
	for _, op := range ops {
		tag := op.tag
		if tag == "" {
			tag = rpc.ScopeNone
		}
		if err := reg.Register(rpc.Handler{
			Namespace: "orbtr", Domain: "audit", Operation: op.name, Func: echo,
			Scope: tag, Requirement: op.req,
			Request: (*wrapperspb.StringValue)(nil), Response: (*wrapperspb.StringValue)(nil),
		}); err != nil {
			t.Fatal(err)
		}
	}
	n := &capturingPublishNode{stubSwarmNode: &stubSwarmNode{}}
	NewPeerPublisher(n, nil, reg).publishOnce()
	var rec swarmpb.PeerRecord
	if err := proto.Unmarshal(n.body, &rec); err != nil {
		t.Fatal(err)
	}
	if rec.Capabilities == nil {
		rec.Capabilities = &swarmpb.Capabilities{}
	}
	rec.Capabilities.Roles = []string{advertRole}
	body, err := proto.Marshal(&rec)
	if err != nil {
		t.Fatal(err)
	}
	return body
}

// advertManager builds a manager whose RoleTable ingested each node's record
// through the same path a fleet.peer record takes.
func advertManager(t *testing.T, records map[string][]byte) *ConnectionManager {
	t.Helper()
	table, err := NewRoleTable(nil, testNodeIDA)
	if err != nil {
		t.Fatal(err)
	}
	for id, body := range records {
		if err := table.onRecord(swarm.Record{NodeID: swarm.NodeID(id), Body: body}); err != nil {
			t.Fatal(err)
		}
	}
	m := registerTestManager()
	m.selfID = testNodeIDA
	m.peers = map[string]*peerConn{}
	m.rt = &Runtime{swarm: &SwarmIntegration{RoleTable: table}, cache: ladcache.NewDirectoryCache(), identity: &NodeIdentity{NodeID: testNodeIDA}}
	m.rt.connMgr = m
	return m
}

func capabilityOp(name, cap string) advertOp {
	req := scope.Capability(cap)
	return advertOp{name: name, req: &req}
}

// The requirement a node declares reaches a caller's RoleTable on its record.
func TestAnAdvertCarriesWhatTheOperationRequiresToTheRoleTable(t *testing.T) {
	nexus := scope.Nexus()
	body := publishedAdvert(t,
		capabilityOp("Write", "orbtr:audit.write"),
		advertOp{name: "Query", req: &nexus},
		advertOp{name: "Status"},
		advertOp{name: "Search", tag: rpc.ScopePartition},
	)
	m := advertManager(t, map[string][]byte{testNodeIDB: body})
	want := map[string]string{
		"orbtr.audit.Write":  "capability:orbtr:audit.write",
		"orbtr.audit.Query":  "nexus",
		"orbtr.audit.Status": "open",
		"orbtr.audit.Search": "scope:partition",
	}
	rec := m.rt.swarm.RoleTable.Lookup(advertRole, "")[0]
	if len(rec.Handlers) != len(want) {
		t.Fatalf("record lists %d handlers, want %d", len(rec.Handlers), len(want))
	}
	for _, h := range rec.Handlers {
		if !reflect.DeepEqual(h.RequiredScopes, []string{want[h.Name]}) {
			t.Errorf("%s requires %v, want %s", h.Name, h.RequiredScopes, want[h.Name])
		}
	}
}

// Two peers holding one capability both answer, and a call may reach either: the
// mesh carries load and survives a node going away by this.
func TestTwoPeersHoldingOneCapabilityBothAnswer(t *testing.T) {
	op := capabilityOp("Write", "orbtr:audit.write")
	m := advertManager(t, map[string][]byte{
		testNodeIDB: publishedAdvert(t, op),
		advertNodeC: publishedAdvert(t, op),
	})
	got, err := m.rt.swarm.RoleTable.Answerers(advertRole, advertFQN)
	if err != nil || len(got) != 2 {
		t.Fatalf("Answerers = (%v, %v), want both peers", got, err)
	}
	if err := m.CheckAnswerers(advertRole, advertFQN, ""); err != nil {
		t.Fatalf("CheckAnswerers = %v, want nil", err)
	}
	if err := m.CheckAnswerers(advertRole, advertFQN, advertNodeC); err != nil {
		t.Fatalf("CheckAnswerers naming a holder = %v, want nil", err)
	}
	if n := m.findNodeForRole(context.Background(), advertRole, advertFQN); n != testNodeIDB && n != advertNodeC {
		t.Fatalf("findNodeForRole = %q, want one of the holders", n)
	}
}

// A peer asking nothing beside a peer asking a capability is never picked, and the
// call never leaves: the HWPCaller the spine builds over this manager refuses it
// typed, naming both requirements, with no route offered.
func TestAPeerAskingLessThanAnotherIsRefusedBeforeTheCallLeaves(t *testing.T) {
	open := scope.Open()
	m := advertManager(t, map[string][]byte{
		testNodeIDB: publishedAdvert(t, capabilityOp("Write", "orbtr:audit.write")),
		advertNodeC: publishedAdvert(t, advertOp{name: "Write", req: &open}),
	})
	bind(m, testNodeIDB, noiseSession())
	bind(m, advertNodeC, noiseSession())

	if n := m.findNodeForRole(context.Background(), advertRole, advertFQN); n != "" {
		t.Fatalf("findNodeForRole = %q, want no node while the adverts disagree", n)
	}
	if routes := m.FindRoutes(context.Background(), advertRole, advertFQN); len(routes) != 0 {
		t.Fatalf("FindRoutes offered %d routes, want none", len(routes))
	}

	caller := dispatch.NewHWPCaller(m)
	defer caller.Close()
	_, err := caller.Call(context.Background(), advertRole, advertFQN, []byte("x"))
	if !errors.Is(err, rpc.ErrAnswerersDisagree) {
		t.Fatalf("Call = %v, want the typed refusal", err)
	}
	var typed *dispatch.AnswerersDisagreeError
	if !errors.As(err, &typed) || !reflect.DeepEqual(typed.Requirements, []string{
		"capability:orbtr:audit.write: " + truncID(testNodeIDB), "open: " + truncID(advertNodeC),
	}) {
		t.Fatalf("refusal = %+v, want each requirement with its node", typed)
	}
	if _, err := caller.CallNode(context.Background(), advertNodeC, advertRole, advertFQN, []byte("x")); !errors.Is(err, rpc.ErrAnswerersDisagree) {
		t.Fatalf("CallNode = %v, want the typed refusal: naming the node changes nothing", err)
	}
}

// A record naming no requirement holds nothing the other advert declares: it takes
// no call while a peer that names one is there, and a call naming it is refused.
func TestAPeerNamingNoRequirementDoesNotAnswerBesideOneThatDoes(t *testing.T) {
	declared := publishedAdvert(t, capabilityOp("Write", "orbtr:audit.write"))
	bare, err := proto.Marshal(&swarmpb.PeerRecord{
		RpcHandlers:  []string{advertFQN},
		Capabilities: &swarmpb.Capabilities{Roles: []string{advertRole}},
	})
	if err != nil {
		t.Fatal(err)
	}
	m := advertManager(t, map[string][]byte{testNodeIDB: declared, advertNodeC: bare})

	got, err := m.rt.swarm.RoleTable.Answerers(advertRole, advertFQN)
	if err != nil || len(got) != 1 || got[0].NodeID != testNodeIDB {
		t.Fatalf("Answerers = (%v, %v), want only the peer naming a requirement", got, err)
	}
	if n := m.findNodeForRole(context.Background(), advertRole, advertFQN); n != testNodeIDB {
		t.Fatalf("findNodeForRole = %q, want the peer naming a requirement", n)
	}
	if err := m.CheckAnswerers(advertRole, advertFQN, ""); err != nil {
		t.Fatalf("routed CheckAnswerers = %v, want nil: routing has an answerer", err)
	}
	if err := m.CheckAnswerers(advertRole, advertFQN, advertNodeC); !errors.Is(err, rpc.ErrAnswerersDisagree) {
		t.Fatalf("CheckAnswerers naming the bare peer = %v, want the refusal", err)
	}
	if err := m.CheckAnswerers(advertRole, advertFQN, testNodeIDB); err != nil {
		t.Fatalf("CheckAnswerers naming the holder = %v, want nil", err)
	}

	// Alone, it has nothing to be compared with and stands.
	m = advertManager(t, map[string][]byte{advertNodeC: bare})
	if n := m.findNodeForRole(context.Background(), advertRole, advertFQN); n != advertNodeC {
		t.Fatalf("findNodeForRole with one bare peer = %q, want it", n)
	}
}

// Two peers of one role with different handler sets: each operation reaches only
// the peer advertising it, and an operation neither advertises reaches neither.
func TestPeersOfOneRoleWithDifferentHandlerSetsEachTakeOnlyTheirOwn(t *testing.T) {
	m := advertManager(t, map[string][]byte{
		testNodeIDB: publishedAdvert(t, capabilityOp("Write", "orbtr:audit.write")),
		advertNodeC: publishedAdvert(t, capabilityOp("Query", "orbtr:audit.read")),
	})
	ctx := context.Background()
	if n := m.findNodeForRole(ctx, advertRole, "orbtr.audit.Write"); n != testNodeIDB {
		t.Errorf("Write routes to %q, want %q", n, testNodeIDB)
	}
	if n := m.findNodeForRole(ctx, advertRole, "orbtr.audit.Query"); n != advertNodeC {
		t.Errorf("Query routes to %q, want %q", n, advertNodeC)
	}
	if n := m.findNodeForRole(ctx, advertRole, "orbtr.audit.Purge"); n != "" {
		t.Errorf("Purge routes to %q, want no node", n)
	}
}

// A relay holds the same line as the origin: it takes nothing to either holder
// while their adverts disagree, and sends nothing, and the refusal is the typed
// one RPCServer stamps on its response.
func TestAForwardToDisagreeingAdvertsIsRefusedBeforeSending(t *testing.T) {
	open := scope.Open()
	table, err := NewRoleTable(nil, "self")
	if err != nil {
		t.Fatal(err)
	}
	for id, body := range map[string][]byte{
		"holder-1": publishedAdvert(t, capabilityOp("Write", "orbtr:audit.write")),
		"holder-2": publishedAdvert(t, advertOp{name: "Write", req: &open}),
	} {
		if err := table.onRecord(swarm.Record{NodeID: swarm.NodeID(id), Body: body}); err != nil {
			t.Fatal(err)
		}
	}
	hop := &hopSession{remote: "holder-1", proto: aether.ProtoNoise}
	other := &hopSession{remote: "holder-2", proto: aether.ProtoNoise}
	f := forwarderWith(hop, other)
	f.rt.swarm = &SwarmIntegration{RoleTable: table}

	resp, err := f.Forward(context.Background(), &pb.RPCRequest{Id: "r", Handler: advertFQN, Context: map[string]string{}})
	if !errors.Is(err, rpc.ErrAnswerersDisagree) || resp != nil {
		t.Fatalf("Forward = (%v, %v), want the typed refusal", resp, err)
	}
	if !dispatch.Unsent(err) {
		t.Fatalf("Unsent(%v) = false, want true", err)
	}
	if hop.sends()+other.sends() != 0 {
		t.Fatal("a refused forward sent to a holder")
	}
}

// The forwarder picks among answerers: a peer naming no requirement is not its
// target while a peer naming one is reachable.
func TestAForwardGoesToTheAnswererNotThePeerNamingNothing(t *testing.T) {
	bare, err := proto.Marshal(&swarmpb.PeerRecord{
		RpcHandlers:  []string{advertFQN},
		Capabilities: &swarmpb.Capabilities{Roles: []string{advertRole}},
	})
	if err != nil {
		t.Fatal(err)
	}
	table, err := NewRoleTable(nil, "self")
	if err != nil {
		t.Fatal(err)
	}
	for id, body := range map[string][]byte{
		"a-bare":   bare,
		"z-holder": publishedAdvert(t, capabilityOp("Write", "orbtr:audit.write")),
	} {
		if err := table.onRecord(swarm.Record{NodeID: swarm.NodeID(id), Body: body}); err != nil {
			t.Fatal(err)
		}
	}
	holder := &hopSession{remote: "z-holder", proto: aether.ProtoNoise}
	bareHop := &hopSession{remote: "a-bare", proto: aether.ProtoNoise}
	f := forwarderWith(holder, bareHop)
	f.rt.swarm = &SwarmIntegration{RoleTable: table}

	if _, err := f.Forward(context.Background(), &pb.RPCRequest{Id: "r", Handler: advertFQN, Context: map[string]string{}}); err != nil {
		t.Fatalf("Forward: %v", err)
	}
	if holder.sends() != 1 || bareHop.sends() != 0 {
		t.Fatalf("holder took %d, the peer naming nothing took %d; want 1 and 0", holder.sends(), bareHop.sends())
	}
}

// The live directory a running node hands out answers HandlersByName and
// HandlerFQNs from the records that node ingested, with the requirement: this is
// what the endpoint spine's fleet audit reads.
func TestALiveDirectoryAnswersForHandlersThisNodeIngested(t *testing.T) {
	rt := newPeerPublicationTestRuntime(t)
	rt.cache = ladcache.NewDirectoryCache()
	dir, err := directory.NewLADDirectory(rt.cache)
	if err != nil {
		t.Fatal(err)
	}
	rt.liveDirRaw, rt.liveDir = dir, dir

	echo := func(ctx context.Context, r proto.Message) (proto.Message, error) { return r, nil }
	reg := rpc.NewRegistry()
	req := scope.Capability("orbtr:audit.write")
	if err := reg.Register(rpc.Handler{
		Namespace: "orbtr", Domain: "audit", Operation: "Write", Func: echo, Scope: rpc.ScopeNone, Requirement: &req,
		Request: (*wrapperspb.StringValue)(nil), Response: (*wrapperspb.StringValue)(nil),
	}); err != nil {
		t.Fatal(err)
	}
	if err := rt.PublishRPCHandlersToLAD(context.Background(), reg); err != nil {
		t.Fatalf("PublishRPCHandlersToLAD: %v", err)
	}

	live := rt.LiveDirectory()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		got, err := live.HandlersByName(context.Background(), "", advertFQN)
		if err != nil {
			t.Fatal(err)
		}
		if len(got) == 1 {
			if string(got[0].NodeID) != string(rt.identity.NodeID) ||
				!reflect.DeepEqual(got[0].RequiredScopes, []string{"capability:orbtr:audit.write"}) {
				t.Fatalf("advert = %+v", got[0])
			}
			fqns, err := dir.HandlerFQNs(context.Background(), "")
			if err != nil || !reflect.DeepEqual(fqns, []string{advertFQN}) {
				t.Fatalf("HandlerFQNs = (%v, %v)", fqns, err)
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("HandlersByName never answered for a handler this node published and ingested")
}

// The role record a bridged fleet.peer record becomes gossips in every full LAD
// exchange, so it stays free of handlers: under the 1 MiB G1 payload bound a node's
// handlers would cost tens of KB per machine.
func TestABridgedRoleRecordCarriesNoHandlers(t *testing.T) {
	rt := bridgeTestRuntime(t)
	cache := ladcache.NewDirectoryCache()
	n := &stubSwarmNode{}
	bridgeReachFromSwarm(rt, n, cache)
	if err := n.sub(swarm.Record{NodeID: swarm.NodeID(testNodeIDB), Body: publishedAdvert(t, advertOp{name: "Write"}), HLC: 1 << 16}); err != nil {
		t.Fatal(err)
	}
	roles, err := cache.Roles(context.Background(), "", ladcache.RoleQuery{})
	if err != nil || len(roles) != 1 || len(roles[0].Handlers) != 0 {
		t.Fatalf("bridged role records = (%+v, %v), want one with no handlers", roles, err)
	}
}

// A node executing as many operations as the largest endpoint's does publishes its
// record and reaches its own RoleTable, which is the same size gate every receiver
// holds: over the cap it is absent from every table, and only its log says so.
func TestALargeEndpointsRecordIsPublished(t *testing.T) {
	echo := func(ctx context.Context, r proto.Message) (proto.Message, error) { return r, nil }
	reg := rpc.NewRegistry()
	const ops = 500
	for i := 0; i < ops; i++ {
		req := scope.Capability(fmt.Sprintf("orbtr.platform:domain%02d.read", i%25))
		if err := reg.Register(rpc.Handler{
			Namespace: "orbtr", Domain: "probeprobeprobeprobe", Operation: fmt.Sprintf("Operation%04d", i),
			Func: echo, Scope: rpc.ScopeNone, Requirement: &req, Effect: rpc.EffectReadOnly,
			Request: (*wrapperspb.StringValue)(nil), Response: (*wrapperspb.StringValue)(nil),
		}); err != nil {
			t.Fatal(err)
		}
	}
	rt := newPeerPublicationTestRuntime(t)
	if err := rt.PublishRPCHandlersToLAD(context.Background(), reg); err != nil {
		t.Fatalf("PublishRPCHandlersToLAD: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if rec, ok := rt.Swarm().RoleTable.All()[string(rt.identity.NodeID)]; ok && len(rec.Handlers) == ops {
			for _, h := range rec.Handlers {
				if len(h.RequiredScopes) != 1 {
					t.Fatalf("%s requires %v, want one requirement", h.Name, h.RequiredScopes)
				}
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("a node with %d operations never reached its own RoleTable", ops)
}

// The load ladder reads the best grade to the peers of a handler's role. A role is
// everything before an FQN's last dot, the split Forward and rpc.ExtractRole make,
// so a grade A peer of the role is what this reports for a namespaced operation.
func TestBestGradeToHandlerFindsThePeersOfTheOperationsRole(t *testing.T) {
	m := advertManager(t, nil)
	gradePeer(m, testNodeIDB, ProtoWebSocket, GradeA)
	cache := ladcache.NewDirectoryCache()
	rt := bridgeTestRuntime(t)
	n := &stubSwarmNode{}
	bridgeReachFromSwarm(rt, n, cache)
	if err := n.sub(swarm.Record{NodeID: swarm.NodeID(testNodeIDB), Body: publishedAdvert(t, advertOp{name: "Write"}), HLC: 1 << 16}); err != nil {
		t.Fatal(err)
	}
	dir, err := directory.NewLADDirectory(cache)
	if err != nil {
		t.Fatal(err)
	}
	m.rt.cache = cache
	m.rt.liveDir = dir
	if g := m.rt.BestGradeToHandler(advertFQN); g != GradeA {
		t.Fatalf("BestGradeToHandler(%s) = %v, want %v from the grade A peer of its role", advertFQN, g, GradeA)
	}
}
