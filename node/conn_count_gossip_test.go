/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"strings"
	"testing"
	"time"

	ladcache "github.com/bbmumford/ledger/cache"
	"github.com/bbmumford/swarm"
	swarmpb "github.com/bbmumford/swarm/proto/pb"
	"google.golang.org/protobuf/proto"
)

// capturingPublishNode records the bytes handed to swarm.Publish so a test can
// assert on what a peer would actually receive, rather than on what the
// publisher was asked to send.
type capturingPublishNode struct {
	*stubSwarmNode
	body []byte
}

func (c *capturingPublishNode) Publish(_ swarm.Topic, body []byte) error {
	c.body = append([]byte(nil), body...)
	return nil
}

// Covers the conn_count producer/consumer pair that makes hotspot damping
// reachable.
//
// The chain is: PeerPublisher stamps conn_count into Capabilities.Tags ->
// the fleet.peer ingest parses it into ConnectionMap -> ConnectionMap.IsHotspot
// -> ConnectionScaler.adjustForGlobalBalance reduces a target. Every link must
// hold, because ConnectionMap has exactly one writer and one reader: with the
// writer absent the map is empty, MeshAverage() answers 0, IsHotspot answers
// false for every peer, and the damping branch is unreachable however
// correctly it is written.
//
// These assert the two ENDS that carry the wiring — the tag the publisher
// actually emits, and the map entry the ingest actually installs — rather than
// the key-splitting in between, which would pass with neither side connected.

// connCountTagged marshals a PeerRecord carrying an explicit tag set, so the
// ingest test drives the real subscriber over real wire bytes.
func connCountTagged(t *testing.T, tags []string) []byte {
	t.Helper()
	b, err := proto.Marshal(&swarmpb.PeerRecord{
		Addresses: []*swarmpb.Address{
			{Transport: swarmpb.Address_NOISE_UDP, Host: "fdaa:0:1::2", Port: 41641, Scope: "private"},
		},
		Capabilities: &swarmpb.Capabilities{Roles: []string{"auth"}, Tags: tags},
	})
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// The PRODUCER, driven through publishOnce and read off the bytes actually
// handed to swarm.Publish. Building the tag string inside the test instead
// would pass with the producer deleted, which is the whole failure this pair
// of tests exists to prevent.
func TestPublishOnceStampsConnCountOntoTheWireBytes(t *testing.T) {
	node := &capturingPublishNode{stubSwarmNode: &stubSwarmNode{}}
	rt := bridgeTestRuntime(t)
	rt.connMgr = &ConnectionManager{peers: map[string]*peerConn{
		"a": {nodeID: "a", state: PeerConnected, connCount: 2},
		"b": {nodeID: "b", state: PeerConnected, connCount: 1},
		"c": {nodeID: "c", state: PeerConnected, connCount: 0},
	}}
	p := NewPeerPublisher(node, rt, nil)

	p.publishOnce()

	if node.body == nil {
		t.Fatal("publishOnce emitted nothing — no record reached swarm.Publish")
	}
	var rec swarmpb.PeerRecord
	if err := proto.Unmarshal(node.body, &rec); err != nil {
		t.Fatalf("published bytes do not decode as a PeerRecord: %v", err)
	}

	var got string
	for _, tag := range rec.GetCapabilities().GetTags() {
		if strings.HasPrefix(tag, connCountMetaKey+"=") {
			got = strings.TrimPrefix(tag, connCountMetaKey+"=")
		}
	}
	// Two peers hold an open session; the third has connCount 0. A count
	// taken from len(peers) would publish 3 and overstate this node's load
	// to every receiver.
	if got != "2" {
		t.Fatalf("published conn_count=%q, want \"2\" — the value must be peers "+
			"holding an OPEN SESSION, not every peer in the map. An absent tag "+
			"means the publisher is not writing it at all, which leaves every "+
			"receiver's ConnectionMap empty", got)
	}
}

// The CONSUMER, driven through the real fleet.peer subscriber the bridge
// registers. This is the half that was missing entirely: ConnectionMap.Update
// had no non-test caller, so the map was permanently empty on every node.
func TestFleetPeerIngestFeedsConnCountIntoTheConnectionMap(t *testing.T) {
	rt := bridgeTestRuntime(t)
	rt.connMgr = &ConnectionManager{peers: map[string]*peerConn{}}
	rt.connMgr.connectionMap = NewConnectionMap()
	cache := ladcache.NewDirectoryCache()
	node := &stubSwarmNode{}

	bridgeReachFromSwarm(rt, node, cache)
	if node.sub == nil {
		t.Fatal("the bridge registered no subscriber — nothing ingests fleet.peer")
	}

	if err := node.sub(swarm.Record{
		NodeID: swarm.NodeID(testNodeIDA),
		Body:   connCountTagged(t, []string{"service=auth", connCountMetaKey + "=7", "region=syd"}),
		HLC:    1,
	}); err != nil {
		t.Fatalf("subscriber returned %v", err)
	}

	got, ok := rt.connMgr.connectionMap.ConnectionCount(testNodeIDA)
	if !ok || got != 7 {
		t.Fatalf("ConnectionCount = (%d, %v), want (7, true) — a record carrying "+
			"conn_count reached the bridge and nothing landed in ConnectionMap, "+
			"so IsHotspot answers false for every peer and the damping branch "+
			"stays unreachable", got, ok)
	}
}

// A malformed or negative count must be DROPPED by the ingest, not stored.
// ConnectionMap averages every entry it holds, so one bad value moves
// MeshAverage and therefore the 2x hotspot threshold for every peer in the
// mesh — a peer could suppress damping fleet-wide by publishing garbage.
func TestTheIngestDropsAMalformedConnCount(t *testing.T) {
	for _, raw := range []string{"", "abc", "-4", "1e3", " 5"} {
		rt := bridgeTestRuntime(t)
		rt.connMgr = &ConnectionManager{peers: map[string]*peerConn{}}
		rt.connMgr.connectionMap = NewConnectionMap()
		node := &stubSwarmNode{}
		bridgeReachFromSwarm(rt, node, ladcache.NewDirectoryCache())

		if err := node.sub(swarm.Record{
			NodeID: swarm.NodeID(testNodeIDA),
			Body:   connCountTagged(t, []string{connCountMetaKey + "=" + raw}),
			HLC:    1,
		}); err != nil {
			t.Fatalf("subscriber returned %v", err)
		}

		if _, ok := rt.connMgr.connectionMap.ConnectionCount(testNodeIDA); ok {
			t.Errorf("conn_count=%q was stored — a peer can move every other "+
				"peer's hotspot threshold by publishing one bad value", raw)
		}
	}
}

// The property the whole chain exists for: a peer carrying more than twice the
// mesh average is damped, and the damping stops at MinPerPeer.
func TestAPopulatedConnectionMapMakesTheDampingBranchFire(t *testing.T) {
	m := &ConnectionManager{budget: DefaultConnectionBudget()}
	s := NewConnectionScaler(m, nil)
	s.connectionMap = NewConnectionMap()

	// Three ordinary peers and one hotspot. The average across all four is
	// (2+2+2+40)/4 = 11.5, so 40 clears 2x and 2 does not.
	s.connectionMap.Update("p1", 2, 0)
	s.connectionMap.Update("p2", 2, 0)
	s.connectionMap.Update("p3", 2, 0)
	s.connectionMap.Update(testNodeIDA, 40, 0)

	raw := m.budget.MinPerPeer + 2
	if got := s.adjustForGlobalBalance(testNodeIDA, raw); got != raw-1 {
		t.Fatalf("adjustForGlobalBalance(hotspot) = %d, want %d — with the map "+
			"populated the hotspot must be damped by one", got, raw-1)
	}
	if got := s.adjustForGlobalBalance("p1", raw); got != raw {
		t.Fatalf("adjustForGlobalBalance(ordinary peer) = %d, want %d — damping "+
			"a peer at the mesh average steers traffic off healthy nodes", got, raw)
	}

	// The floor holds: damping must never push a peer below MinPerPeer, or a
	// busy node loses its last path and the peer is disconnected outright.
	if got := s.adjustForGlobalBalance(testNodeIDA, m.budget.MinPerPeer); got != m.budget.MinPerPeer {
		t.Fatalf("damping drove the target to %d, below MinPerPeer %d",
			got, m.budget.MinPerPeer)
	}
}

// The negative control for the whole feature: with NO writer the map is empty,
// and an empty map must leave every target untouched. This is the state the
// code was in — it passes both before and after the producer is wired, and it
// exists so that a future change which silently drops the producer shows up as
// the damping tests failing rather than this one.
func TestAnEmptyConnectionMapDampsNothing(t *testing.T) {
	m := &ConnectionManager{budget: DefaultConnectionBudget()}
	s := NewConnectionScaler(m, nil)
	s.connectionMap = NewConnectionMap()

	if avg := s.connectionMap.MeshAverage(); avg != 0 {
		t.Fatalf("MeshAverage on an empty map = %v, want 0", avg)
	}
	raw := m.budget.MinPerPeer + 3
	if got := s.adjustForGlobalBalance(testNodeIDA, raw); got != raw {
		t.Fatalf("adjustForGlobalBalance = %d on an empty map, want %d unchanged", got, raw)
	}
}

// Entries age out. A departed peer stops republishing, so its count must stop
// counting toward MeshAverage rather than pinning the average at its last
// value forever and suppressing damping for everyone still present.
func TestAStaleConnCountStopsCountingTowardTheMeshAverage(t *testing.T) {
	cm := NewConnectionMap()
	cm.Update(testNodeIDA, 40, 0)
	if cm.MeshAverage() == 0 {
		t.Fatal("premise wrong: a fresh entry did not reach MeshAverage")
	}

	cm.mu.Lock()
	e := cm.entries[testNodeIDA]
	e.ReportedAt = time.Now().Add(-2 * cm.maxAge)
	cm.entries[testNodeIDA] = e
	cm.mu.Unlock()

	if avg := cm.MeshAverage(); avg != 0 {
		t.Fatalf("MeshAverage = %v with only a stale entry, want 0 — a departed "+
			"node's last count would hold the threshold up for every live peer", avg)
	}
	if cm.IsHotspot(testNodeIDA) {
		t.Error("a stale entry still reports as a hotspot")
	}
}
