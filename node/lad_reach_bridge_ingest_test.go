/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"testing"
	"time"

	lad "github.com/bbmumford/ledger"
	ladcache "github.com/bbmumford/ledger/cache"
	"github.com/bbmumford/swarm"
	swarmpb "github.com/bbmumford/swarm/proto/pb"
	"google.golang.org/protobuf/proto"
)

// END-TO-END COVERAGE of bridgeReachFromSwarm — the gossip-ingest path that
// turns a fleet.peer PeerRecord into the LAD reach/member/role records every
// directory read is served from.
//
// End-to-end rather than more unit tests, because reachAddrsFromPB skipping
// unmappable transports is provable at the unit level while the question a
// unit test structurally cannot answer is whether that holds along the path an
// actual gossip record travels. This drives a real
// PeerRecord through the real subscriber into a real DirectoryCache and reads
// the result back through the same query the dial path uses.

// stubSwarmNode implements swarm.Node, capturing the subscriber the bridge
// registers. Only Subscribe carries behaviour — the bridge calls nothing else,
// and a stub that pretended otherwise would be inventing a contract.
type stubSwarmNode struct {
	topic  swarm.Topic
	sub    swarm.Subscriber
	subErr error // when set, Subscribe fails and no subscriber is registered
}

func (s *stubSwarmNode) Subscribe(topic swarm.Topic, sub swarm.Subscriber) (swarm.Unsubscribe, error) {
	if s.subErr != nil {
		return nil, s.subErr
	}
	s.topic, s.sub = topic, sub
	return func() {}, nil
}

func (s *stubSwarmNode) Start(context.Context) error                              { return nil }
func (s *stubSwarmNode) Stop() error                                              { return nil }
func (s *stubSwarmNode) Publish(swarm.Topic, []byte) error                        { return nil }
func (s *stubSwarmNode) PublishKeyed(swarm.Topic, string, []byte) error           { return nil }
func (s *stubSwarmNode) PublishKeyedTombstone(swarm.Topic, string) error          { return nil }
func (s *stubSwarmNode) PublishTombstone(swarm.Topic) error                       { return nil }
func (s *stubSwarmNode) PublishObserverTombstone(swarm.Topic, swarm.NodeID) error { return nil }
func (s *stubSwarmNode) SetObserverRoleCheck(func(swarm.NodeID, []byte) bool)     {}
func (s *stubSwarmNode) SetRole(swarm.Role) error                                 { return nil }
func (s *stubSwarmNode) SelfRole() swarm.Role                                     { return swarm.Role(0) }
func (s *stubSwarmNode) Get(swarm.Topic, swarm.NodeID) (swarm.Record, bool) {
	return swarm.Record{}, false
}
func (s *stubSwarmNode) GetKeyed(swarm.Topic, swarm.NodeID, string) (swarm.Record, bool) {
	return swarm.Record{}, false
}
func (s *stubSwarmNode) NodeRecords(swarm.Topic, swarm.NodeID) []swarm.Record { return nil }
func (s *stubSwarmNode) TopicRecords(swarm.Topic) []swarm.Record              { return nil }
func (s *stubSwarmNode) Topics() []swarm.Topic                                { return nil }
func (s *stubSwarmNode) SetObserverQuorum(int, time.Duration)                 {}
func (s *stubSwarmNode) SetTenant(string) error                               { return nil }
func (s *stubSwarmNode) Pause() error                                         { return nil }
func (s *stubSwarmNode) Resume() error                                        { return nil }
func (s *stubSwarmNode) PerPeerConfig(swarm.NodeID) swarm.PerPeerConfig {
	return swarm.PerPeerConfig{}
}
func (s *stubSwarmNode) ContentTopic() swarm.ContentTopic { return nil }
func (s *stubSwarmNode) Tick(time.Time)                   {}
func (s *stubSwarmNode) ProbePeer(swarm.NodeID)           {}

var _ swarm.Node = (*stubSwarmNode)(nil)

func bridgeTestRuntime(t *testing.T) *Runtime {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	return &Runtime{
		ctx:      context.Background(),
		identity: &NodeIdentity{PublicKey: pub, PrivateKey: priv},
	}
}

// peerRecordBody marshals a PeerRecord exactly as the gossip producer does —
// the bridge proto.Unmarshals r.Body into a swarmpb.PeerRecord.
func peerRecordBody(t *testing.T, addrs []*swarmpb.Address) []byte {
	t.Helper()
	b, err := proto.Marshal(&swarmpb.PeerRecord{
		Addresses:    addrs,
		Capabilities: &swarmpb.Capabilities{Roles: []string{"auth"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// An address whose Transport is unset must not reach the stored reach record,
// asserted through the full ingest path rather than against reachAddrsFromPB
// alone.
//
// Address_UNKNOWN is the enum's ZERO VALUE, so this is the shape a
// partially-populated or older producer emits by default — not a corruption
// case. Before the fix it was stored with an empty Proto: undialable by every
// path, yet counted among the peer's addresses.
func TestBridgeDropsUnmappableAddressesBeforeTheyReachTheReachRecord(t *testing.T) {
	ctx := context.Background()
	rt := bridgeTestRuntime(t)
	cache := ladcache.NewDirectoryCache()
	node := &stubSwarmNode{}

	bridgeReachFromSwarm(rt, node, cache)
	if node.sub == nil {
		t.Fatal("the bridge registered no subscriber — nothing would ingest " +
			"fleet.peer at all")
	}
	if node.topic != swarm.Topic("fleet.peer") {
		t.Fatalf("subscribed to %q, want fleet.peer", node.topic)
	}

	body := peerRecordBody(t, []*swarmpb.Address{
		{Transport: swarmpb.Address_NOISE_UDP, Host: "fdaa:0:1234:a7b::2", Port: 41641, Scope: "private"},
		{Transport: swarmpb.Address_UNKNOWN, Host: "203.0.113.9", Port: 443, Scope: "public"},
		{Transport: swarmpb.Address_WEBSOCKET, Host: "devices.orbtr.io", Port: 443, Scope: "public"},
	})
	if err := node.sub(swarm.Record{NodeID: swarm.NodeID(testNodeIDA), Body: body, HLC: 100 << 16}); err != nil {
		t.Fatalf("subscriber returned an error: %v", err)
	}

	recs, err := cache.Reach(ctx, "", ladcache.ReachQuery{NodeID: testNodeIDA})
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 {
		t.Fatalf("got %d reach records, want 1 — the ingest path did not store "+
			"the peer at all, so nothing below is measured", len(recs))
	}

	got := recs[0].Addresses
	if len(got) != 2 {
		t.Fatalf("stored %d addresses, want 2 (udp + wss): %+v — the unmappable "+
			"entry survived ingest and is now in the signed reach record",
			len(got), got)
	}
	for _, a := range got {
		if a.Proto == "" {
			t.Fatalf("an address with an EMPTY Proto reached the stored record: "+
				"%+v — it is undialable by every path yet counts toward the "+
				"peer's address total", a)
		}
	}
	// Control: the mappable entries survived with their producer vocabulary
	// intact, or "store nothing" would satisfy the assertions above.
	if got[0].Proto != "udp" || got[1].Proto != "wss" {
		t.Fatalf("the surviving protos are %q/%q, want udp/wss — the wire "+
			"vocabulary directory's compatibility shim depends on has changed",
			got[0].Proto, got[1].Proto)
	}
}

// A record whose body is not a PeerRecord must be dropped without error and
// without storing anything. The subscriber runs on the swarm dispatcher, so
// returning an error or panicking here would affect unrelated records.
func TestBridgeIgnoresAnUnparseableBody(t *testing.T) {
	ctx := context.Background()
	rt := bridgeTestRuntime(t)
	cache := ladcache.NewDirectoryCache()
	node := &stubSwarmNode{}
	bridgeReachFromSwarm(rt, node, cache)

	err := node.sub(swarm.Record{
		NodeID: swarm.NodeID(testNodeIDA),
		Body:   []byte("this is not a protobuf"),
		HLC:    100 << 16,
	})
	if err != nil {
		t.Fatalf("an unparseable body returned an error (%v) — the swarm "+
			"dispatcher would treat one malformed peer record as a subscriber "+
			"failure", err)
	}

	recs, rerr := cache.Reach(ctx, "", ladcache.ReachQuery{NodeID: testNodeIDA})
	if rerr != nil {
		t.Fatal(rerr)
	}
	if len(recs) != 0 {
		t.Fatalf("an unparseable body still produced %d reach record(s)", len(recs))
	}
}

// The paired member record must carry the service name under the key the
// READER expects, pinned here at the producer: the bridge writes "serviceName"
// in camelCase, and a reader looking for "service_name" finds every member's
// service empty.
func TestBridgeWritesServiceNameInCamelCase(t *testing.T) {
	ctx := context.Background()
	rt := bridgeTestRuntime(t)
	cache := ladcache.NewDirectoryCache()
	node := &stubSwarmNode{}
	bridgeReachFromSwarm(rt, node, cache)

	pr := &swarmpb.PeerRecord{
		Addresses: []*swarmpb.Address{
			{Transport: swarmpb.Address_NOISE_UDP, Host: "fdaa:0:1234:a7b::2", Port: 41641, Scope: "private"},
		},
		Capabilities: &swarmpb.Capabilities{
			Roles: []string{"auth"},
			Tags:  []string{"service=svc-a", "region=syd"},
		},
	}
	body, err := proto.Marshal(pr)
	if err != nil {
		t.Fatal(err)
	}
	if err := node.sub(swarm.Record{NodeID: swarm.NodeID(testNodeIDA), Body: body, HLC: 100 << 16}); err != nil {
		t.Fatal(err)
	}

	members, err := cache.Members(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(members) == 0 {
		t.Fatal("the bridge stored no member record — the comparison below " +
			"would be vacuous")
	}

	var found *lad.MemberRecord
	for i := range members {
		if members[i].NodeID == testNodeIDA {
			found = &members[i]
		}
	}
	if found == nil {
		t.Fatalf("no member record for the ingested peer: %+v", members)
	}
	if _, ok := found.Attrs["serviceName"]; !ok {
		b, _ := json.Marshal(found.Attrs)
		t.Fatalf("the member record has no \"serviceName\" attr: %s — readers "+
			"key on camelCase, so every member's service name reads empty", b)
	}
}

// The departure path.
//
// When a peer departs, the bridge must forward the swarm tombstone as LAD
// tombstones for ALL THREE topics it writes on the live path: reach, member
// AND role. Getting this wrong does not error — a departed node simply
// lingers as dialable in every directory read.
//
// 🛑 THE ROLE TOMBSTONE IS NOT HYPOTHETICAL: the code's own MESH-F07 comment
// records that this path once drained reach+member only, so "a departed
// node's role record lingered until TTL/rebuild, and consumers (isAnchorNode,
// BestGradeToHandler) kept treating the dead node as an anchor." This pins
// all three so that regression cannot recur silently.
func TestBridgeDrainsAllThreeTopicsOnDeparture(t *testing.T) {
	ctx := context.Background()
	rt := bridgeTestRuntime(t)
	rt.partitionDetector = NewPartitionDetector(&rt.lamportClock)
	cache := ladcache.NewDirectoryCache()
	node := &stubSwarmNode{}
	bridgeReachFromSwarm(rt, node, cache)

	// --- the peer is live ---
	body := peerRecordBody(t, []*swarmpb.Address{
		{Transport: swarmpb.Address_NOISE_UDP, Host: "fdaa:0:1234:a7b::2", Port: 41641, Scope: "private"},
	})
	if err := node.sub(swarm.Record{NodeID: swarm.NodeID(testNodeIDA), Body: body, HLC: 100 << 16}); err != nil {
		t.Fatal(err)
	}

	// Premise: all three topics are populated, or the departure assertions
	// below are satisfied by a peer that was never there.
	reach, _ := cache.Reach(ctx, "", ladcache.ReachQuery{NodeID: testNodeIDA})
	members, _ := cache.Members(ctx, "")
	roles, _ := cache.Roles(ctx, "", ladcache.RoleQuery{Role: "auth"})
	if len(reach) == 0 || len(members) == 0 || len(roles) == 0 {
		t.Fatalf("premise wrong: live ingest left reach=%d members=%d roles=%d — "+
			"the departure check would be vacuous", len(reach), len(members), len(roles))
	}

	// --- the peer departs (HLC must ADVANCE or the tombstone loses to the
	// live record on ordering) ---
	if err := node.sub(swarm.Record{
		NodeID: swarm.NodeID(testNodeIDA), Tombstone: true, HLC: 200 << 16,
	}); err != nil {
		t.Fatalf("the departure record returned an error: %v", err)
	}

	reach2, _ := cache.Reach(ctx, "", ladcache.ReachQuery{NodeID: testNodeIDA})
	if len(reach2) != 0 {
		t.Errorf("reach survived departure (%d records) — the node is gone and "+
			"still dialable", len(reach2))
	}
	members2, _ := cache.Members(ctx, "")
	for _, m := range members2 {
		if m.NodeID == testNodeIDA {
			t.Errorf("member record survived departure — the departed node is " +
				"still counted as a mesh member")
		}
	}
	roles2, _ := cache.Roles(ctx, "", ladcache.RoleQuery{Role: "auth"})
	for _, r := range roles2 {
		if r.NodeID == testNodeIDA {
			t.Error("ROLE record survived departure — this is exactly MESH-F07: " +
				"isAnchorNode and BestGradeToHandler keep treating the dead node " +
				"as an anchor until TTL/rebuild")
		}
	}
}

// The departure path must not require a partition detector. Runtime wires one
// in Initialize, so any ingest arriving before that (or in a build that never
// sets it) hits a nil field on the same line.
func TestBridgeDepartureIsSafeWithoutAPartitionDetector(t *testing.T) {
	rt := bridgeTestRuntime(t)
	if rt.partitionDetector != nil {
		t.Fatal("premise wrong: this case needs the detector ABSENT")
	}
	cache := ladcache.NewDirectoryCache()
	node := &stubSwarmNode{}
	bridgeReachFromSwarm(rt, node, cache)

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("departure panicked with no partition detector: %v — this "+
				"runs on the swarm dispatcher and would take the node down", r)
		}
	}()
	if err := node.sub(swarm.Record{
		NodeID: swarm.NodeID(testNodeIDA), Tombstone: true, HLC: 200 << 16,
	}); err != nil {
		t.Fatalf("departure with no detector returned an error: %v", err)
	}
}

// The Capabilities.Tags parser.
//
// Read by eye, the uncovered lines here look like unreachable `json.Marshal`
// error guards. Parsing the coverage profile instead of the
// source showed TWO reachable branches hiding among them — the malformed-tag
// skip and the nat_class case. Read the profile, not the code.
//
// Tags are a FLAT []string of "key=value" (see role_table.go peerRecordToInfo),
// so a malformed entry is a wire-shaped input, not a hypothetical: anything
// that appends a bare word puts one there.
func TestBridgeParsesTagsAndSkipsMalformedOnes(t *testing.T) {
	ctx := context.Background()
	rt := bridgeTestRuntime(t)
	cache := ladcache.NewDirectoryCache()
	node := &stubSwarmNode{}
	bridgeReachFromSwarm(rt, node, cache)

	body, err := proto.Marshal(&swarmpb.PeerRecord{
		Addresses: []*swarmpb.Address{
			{Transport: swarmpb.Address_NOISE_UDP, Host: "fdaa:0:1234:a7b::2", Port: 41641, Scope: "private"},
		},
		Capabilities: &swarmpb.Capabilities{
			Roles: []string{"auth"},
			Tags: []string{
				"malformed-no-equals", // must be SKIPPED, not parsed as a key
				"nat_class=EndpointIndependent",
				"region=syd",
				"service=svc-a",
				"=emptykey", // degenerate but well-formed: key "", ignored by the switch
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := node.sub(swarm.Record{NodeID: swarm.NodeID(testNodeIDA), Body: body, HLC: 100 << 16}); err != nil {
		t.Fatalf("a record with a malformed tag returned an error: %v — one bad "+
			"tag would reject the whole peer record", err)
	}

	recs, rerr := cache.Reach(ctx, "", ladcache.ReachQuery{NodeID: testNodeIDA})
	if rerr != nil {
		t.Fatal(rerr)
	}
	if len(recs) != 1 {
		t.Fatalf("got %d reach records, want 1 — the malformed tag aborted ingest", len(recs))
	}
	// nat_class is the branch that was uncovered; it must reach the record.
	if recs[0].NATType != "EndpointIndependent" {
		t.Errorf("NATType = %q, want EndpointIndependent — the nat_class tag was "+
			"not parsed, so NAT traversal decisions lose their input and every "+
			"peer looks like an unknown class", recs[0].NATType)
	}
	if recs[0].Region != "syd" {
		t.Errorf("Region = %q, want syd", recs[0].Region)
	}
}

// If Subscribe fails the bridge must return without registering anything and
// without panicking — the caller has no error to check, so a nil deref here
// would surface far from its cause.
func TestBridgeSurvivesASubscribeFailure(t *testing.T) {
	rt := bridgeTestRuntime(t)
	cache := ladcache.NewDirectoryCache()
	node := &stubSwarmNode{subErr: errStubSubscribe}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("bridgeReachFromSwarm panicked when Subscribe failed: %v", r)
		}
	}()
	bridgeReachFromSwarm(rt, node, cache)

	if node.sub != nil {
		t.Fatal("a subscriber was registered despite Subscribe returning an error")
	}
}

var errStubSubscribe = errStub("subscribe refused")

type errStub string

func (e errStub) Error() string { return string(e) }
