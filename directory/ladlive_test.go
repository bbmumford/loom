/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package directory

import (
	"context"
	"crypto/ed25519"
	crand "crypto/rand"
	"encoding/json"
	"errors"
	"net"
	"strconv"
	"testing"
	"time"

	lad "github.com/bbmumford/ledger"
	ladcache "github.com/bbmumford/ledger/cache"

	"github.com/bbmumford/loom/ports"
)

const testTenant = "hstles"

// testTenantID is the port-level form of testTenant.
const testTenantID = ports.Tenant(testTenant)

func applyLAD(t *testing.T, c *ladcache.DirectoryCache, topic lad.Topic, nodeID string, body any) {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	rec := lad.Record{
		Topic:     topic,
		TenantID:  testTenant,
		NodeID:    nodeID,
		Body:      b,
		Timestamp: time.Now(),
	}
	if err := c.Apply(rec); err != nil {
		t.Fatalf("apply %s: %v", topic, err)
	}
}

// ladSideOfTheSameNode publishes, through LAD's three typed records, the same
// facts peerRecord publishes as one signed swarm fleet.peer record.
func ladSideOfTheSameNode(t *testing.T, c *ladcache.DirectoryCache, id ports.NodeID, roles []string, service string) {
	t.Helper()
	applyLAD(t, c, lad.TopicMember, string(id), lad.MemberRecord{
		TenantID:  testTenant,
		NodeID:    string(id),
		CreatedAt: time.Now(),
		// Keys copied from the producer, node/lad_reach_bridge.go:156-158 —
		// NOT from what this adapter happens to read. Writing the fixture to
		// match the reader is how the snake_case bug survived its own test.
		Attrs: map[string]string{"serviceName": service, "region": "syd"},
	})
	applyLAD(t, c, lad.TopicRole, string(id), lad.RoleRecord{
		TenantID: testTenant,
		NodeID:   string(id),
		Roles:    roles,
		Handlers: []lad.HandlerMetadata{{Name: "hstles." + service + ".ping"}},
		Updated:  time.Now(),
	})
	applyLAD(t, c, lad.TopicReach, string(id), lad.ReachRecord{
		TenantID: testTenant,
		NodeID:   string(id),
		Region:   "syd",
		Addresses: []lad.ReachAddress{
			{Host: "203.0.113.7", Port: 443, Proto: "ws"},
			{Host: "fdaa::7", Port: 41641, Proto: "udp"},
		},
		UpdatedAt: time.Now(),
		ExpiresAt: time.Now().Add(time.Hour),
	})
}

func newLADDirectory(t *testing.T) (*LADDirectory, *ladcache.DirectoryCache) {
	t.Helper()
	c := ladcache.NewDirectoryCache()
	d, err := NewLADDirectory(c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })
	return d, c
}

// THE MEASUREMENT THE SHADOW PHASE EXISTS FOR: two INDEPENDENT
// implementations, given the same facts, must agree on the typed projections.
// Until this test existed, TestShadowParity compared a SwarmDirectory against
// another SwarmDirectory — proving the comparator's mechanics and nothing
// about the cutover, because a defect shared by both sides is invisible when
// both sides are the same code.
func TestLADAndSwarmAgreeOnTheTypedProjectionsOfTheSameNode(t *testing.T) {
	ctx := context.Background()
	idA, pubA, _ := testIdentity(t)
	roles := []string{"anchor", "auth"}

	swarmSide := newTestDirectory(t, nil)
	if err := swarmSide.Ingest(ctx, peerRecord(t, idA, pubA, 100<<16, roles, "auth-svc")); err != nil {
		t.Fatal(err)
	}
	ladSide, _ := newLADDirectory(t)
	ladSideOfTheSameNode(t, ladSide.cache, idA, roles, "auth-svc")

	rep, err := CompareDirectories(ctx, swarmSide, ladSide, testTenantID, roles)
	if err != nil {
		t.Fatal(err)
	}
	if !rep.InParity() {
		t.Fatalf("independent implementations disagree on the same facts:\n  mismatches: %v\n  degraded:   %v",
			rep.Mismatches, rep.Degraded)
	}
	// Controls: a parity verdict over zero comparisons proves nothing.
	if rep.ComparedMembers == 0 || rep.ComparedRoles == 0 || rep.ComparedReach == 0 || rep.ComparedHandlers == 0 {
		t.Fatalf("vacuous parity — compared members=%d roles=%d reach=%d handlers=%d",
			rep.ComparedMembers, rep.ComparedRoles, rep.ComparedReach, rep.ComparedHandlers)
	}
}

// A divergence between the two implementations must SURFACE. Without this the
// test above could pass because the comparator cannot see anything at all.
func TestLADSwarmDivergenceIsReported(t *testing.T) {
	ctx := context.Background()
	idA, pubA, _ := testIdentity(t)

	swarmSide := newTestDirectory(t, nil)
	if err := swarmSide.Ingest(ctx, peerRecord(t, idA, pubA, 100<<16, []string{"auth"}, "auth-svc")); err != nil {
		t.Fatal(err)
	}
	ladSide, _ := newLADDirectory(t)
	// Same node, DIFFERENT role set.
	ladSideOfTheSameNode(t, ladSide.cache, idA, []string{"billing"}, "auth-svc")

	rep, err := CompareDirectories(ctx, swarmSide, ladSide, testTenantID, []string{"auth", "billing"})
	if err != nil {
		t.Fatal(err)
	}
	if rep.InParity() {
		t.Fatal("a genuine role divergence between the two sides was not reported")
	}
}

// Cross-model fingerprinting is a category error, and the comparator must say
// so rather than return false. LAD carries a node as three typed records where
// swarm carries one signed record, so identical FACTS give different RECORDS.
// A plain false here is worse than an error: during shadow mode it presents as
// a permanent divergence, and both available responses — hunt a phantom, or
// weaken the comparator until it passes — damage the cutover.
func TestCrossModelFingerprintIsRefusedRatherThanReportedAsDivergence(t *testing.T) {
	ctx := context.Background()
	idA, pubA, _ := testIdentity(t)

	swarmSide := newTestDirectory(t, nil)
	if err := swarmSide.Ingest(ctx, peerRecord(t, idA, pubA, 100<<16, []string{"auth"}, "auth-svc")); err != nil {
		t.Fatal(err)
	}
	ladSide, _ := newLADDirectory(t)
	ladSideOfTheSameNode(t, ladSide.cache, idA, []string{"auth"}, "auth-svc")

	if RecordModelOf(swarmSide) == RecordModelOf(ladSide) {
		t.Fatal("the two sides declare the same record model — the premise of this test is gone")
	}
	_, err := CompareFingerprints(ctx, swarmSide, ladSide)
	if !errors.Is(err, ErrRecordModelMismatch) {
		t.Fatalf("cross-model fingerprint comparison returned err=%v, want ErrRecordModelMismatch", err)
	}

	// Control: same model still compares normally.
	other := newTestDirectory(t, nil)
	if err := other.Ingest(ctx, peerRecord(t, idA, pubA, 100<<16, []string{"auth"}, "auth-svc")); err != nil {
		t.Fatal(err)
	}
	ok, err := CompareFingerprints(ctx, swarmSide, other)
	if err != nil || !ok {
		t.Fatalf("same-model comparison broke: ok=%v err=%v", ok, err)
	}
}

// LAD's SetGossipLivenessOverride has no expiry; the port's does. The TTL is
// the adapter's responsibility, and dropping it fails OPEN — an overridden
// node is exempt from liveness eviction forever. Measured through the cache's
// real eviction path, not through the adapter's own bookkeeping.
func TestLivenessOverrideExpiresAndEvictionResumes(t *testing.T) {
	d, c := newLADDirectory(t)
	id := "node-override"

	// A member last seen well past the gossip cutoff: evictable on sight.
	applyLAD(t, c, lad.TopicMember, id, lad.MemberRecord{
		TenantID:  testTenant,
		NodeID:    id,
		CreatedAt: time.Now().Add(-30 * time.Minute),
	})

	d.OverrideLiveness(ports.NodeID(id), true, 150)
	if n := c.EvictExpired(); n != 0 {
		t.Fatalf("an overridden node was evicted while its override was live (%d removed)", n)
	}

	// Control: the node really is otherwise evictable, so the 0 above was the
	// override doing its job and not an inert sweep.
	deadline := time.Now().Add(3 * time.Second)
	evicted := 0
	for time.Now().Before(deadline) {
		if evicted = c.EvictExpired(); evicted > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if evicted == 0 {
		t.Fatal("the override never expired — the node is permanently exempt " +
			"from liveness eviction, which is how dropping ttlMs fails OPEN")
	}
}

// ttlMs <= 0 clears the override immediately, per the port contract.
func TestZeroTTLClearsTheOverride(t *testing.T) {
	d, c := newLADDirectory(t)
	id := "node-clear"
	applyLAD(t, c, lad.TopicMember, id, lad.MemberRecord{
		TenantID:  testTenant,
		NodeID:    id,
		CreatedAt: time.Now().Add(-30 * time.Minute),
	})

	d.OverrideLiveness(ports.NodeID(id), true, 60_000)
	if n := c.EvictExpired(); n != 0 {
		t.Fatalf("override not applied (%d evicted)", n)
	}
	d.OverrideLiveness(ports.NodeID(id), true, 0)
	if n := c.EvictExpired(); n == 0 {
		t.Fatal("ttlMs=0 did not clear the override")
	}
}

// One observer holds one observation PER OBSERVED PEER. LAD indexes latency by
// observer, so the projection must give each observation its own slot — the
// composite key. Collapsed to one slot per observer, a node's whole latency
// view is whichever record happened to land last.
func TestLatencyProjectionKeepsOneSlotPerObservedPeer(t *testing.T) {
	d, c := newLADDirectory(t)
	ctx := context.Background()
	const from = "observer-1"

	for _, to := range []string{"peer-a", "peer-b", "peer-c"} {
		applyLAD(t, c, lad.TopicLatency, from, lad.LatencyRecord{
			FromNode:   from,
			ToNode:     to,
			RTTMs:      12,
			MeasuredAt: time.Now(),
			ExpiresAt:  time.Now().Add(time.Hour),
		})
	}

	recs, err := d.RecordsByTopic(ctx, LatencyTopic(ports.NodeID(from)))
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 3 {
		t.Fatalf("one observer's latency records project to %d slots, want 3 — "+
			"observations are overwriting each other", len(recs))
	}
	seen := map[string]bool{}
	for _, r := range recs {
		if r.Key == "" {
			t.Fatal("a latency observation projected with an empty composite key")
		}
		seen[r.Key] = true
	}
	if len(seen) != 3 {
		t.Fatalf("latency slots share keys: %v", seen)
	}
}

// The port forbids query-then-subscribe. A record applied AFTER the live
// subscriber is registered but BEFORE/while history is read must still be
// delivered exactly once — never dropped into the handoff gap, never doubled.
func TestSubscribeDeliversHistoryAndLiveExactlyOnce(t *testing.T) {
	d, c := newLADDirectory(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	applyLAD(t, c, lad.TopicMember, "node-history", lad.MemberRecord{
		TenantID: testTenant, NodeID: "node-history", CreatedAt: time.Now(),
	})

	ch, err := d.Subscribe(ctx, []ports.Topic{FleetPeerTopic}, 0)
	if err != nil {
		t.Fatal(err)
	}

	applyLAD(t, c, lad.TopicMember, "node-live", lad.MemberRecord{
		TenantID: testTenant, NodeID: "node-live", CreatedAt: time.Now(),
	})

	got := map[ports.NodeID]int{}
	deadline := time.After(3 * time.Second)
	for len(got) < 2 {
		select {
		case r, ok := <-ch:
			if !ok {
				t.Fatalf("stream closed early, saw %v", got)
			}
			got[r.NodeID]++
		case <-deadline:
			t.Fatalf("history→live handoff lost a record: saw %v, want both "+
				"node-history and node-live", got)
		}
	}
	for id, n := range got {
		if n != 1 {
			t.Fatalf("record for %s delivered %d times — the overlap is not deduped", id, n)
		}
	}
}

// The fleet's private addresses are IPv6 (Fly 6PN, fdaa::/48). A reach entry
// is a DIAL STRING, so an IPv6 literal must be bracketed — "fdaa::7:41641"
// parses as neither host nor port. Both projections must agree on the form,
// and the correct form is net.JoinHostPort's.
func TestReachAddressesAreDialableForIPv6(t *testing.T) {
	ctx := context.Background()
	idA, pubA, _ := testIdentity(t)

	d := newTestDirectory(t, nil)
	if err := d.Ingest(ctx, peerRecord(t, idA, pubA, 100<<16, []string{"auth"}, "auth-svc")); err != nil {
		t.Fatal(err)
	}
	addrs, err := d.Reach(ctx, testTenantID, idA)
	if err != nil {
		t.Fatal(err)
	}
	if len(addrs) == 0 {
		t.Fatal("no reach addresses — the assertion below would be vacuous")
	}
	for _, a := range addrs {
		host, port, err := net.SplitHostPort(a.Address)
		if err != nil {
			t.Fatalf("reach address %q is not a dial string: %v", a.Address, err)
		}
		if host == "" || port == "" {
			t.Fatalf("reach address %q split to host=%q port=%q", a.Address, host, port)
		}
	}
}

// LAD declares `keyops` and `quorum` as topic constants, but its cache stores
// NEITHER: applyCore's default branch is "ignore other topics for directory
// view", and ChangesSinceHLC has no branch for them. The adapter therefore
// cannot see those records — and must SAY SO, because the alternative is
// answering "no key-rotation records exist" to a security consumer when the
// truth is "I cannot see them".
//
// This is the failure direction that keeps recurring: a silent empty reads as
// a measured absence.
func TestUnprojectedTopicsErrorRatherThanReturningEmpty(t *testing.T) {
	d, _ := newLADDirectory(t)
	ctx := context.Background()

	for _, topic := range []ports.Topic{"keyops", "quorum"} {
		recs, err := d.RecordsByTopic(ctx, topic)
		if err == nil {
			t.Fatalf("RecordsByTopic(%q) returned %d records and nil error — "+
				"an unseeable topic reported as empty", topic, len(recs))
		}
		if !errors.Is(err, ErrTopicNotProjected) {
			t.Fatalf("RecordsByTopic(%q) error = %v, want ErrTopicNotProjected", topic, err)
		}
	}

	// Control: a topic LAD DOES project answers normally, so the error above
	// is about projection coverage and not a blanket failure.
	if _, err := d.RecordsByTopic(ctx, FleetPeerTopic); err != nil {
		t.Fatalf("a projected topic errored: %v", err)
	}
}

// CHARACTERISATION — the measured limit of ports.Record as an envelope for
// LAD-signed records, and a hard prerequisite for stage 2's "signed
// anchor-snapshot input".
//
// §0.5.4 requires that "owner-signed provenance survives gossip, persistence,
// snapshot, recovery, and PROJECTION". For swarm-native records it does:
// swarm's signable bytes are fields ports.Record carries. For LAD records it
// does NOT. lad.signatureContent covers Topic, NodeID, TenantID, Body,
// Timestamp, LamportClock, Seq, HLC, ExpiresAt(UnixNano), Tombstone,
// DeletedAt, TombstoneReason, AuthorPubKey, BlobCID — and ports.Record carries
// no TenantID, no Seq, no Timestamp, and only MILLISECOND expiry.
//
// So anchor.Generator.Generate([]lad.Record) cannot be fed from a
// ports.DirectorySnapshot: every record's signature would fail to verify. This
// test pins WHICH fields are missing, so the fix is a checklist rather than a
// rediscovery — and so that if ports.Record later gains them, the test says so
// by failing at the step that no longer matters.
func TestLADSignedRecordDoesNotSurviveThePortEnvelope(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(crand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	orig := lad.Record{
		Topic: lad.TopicMember, TenantID: testTenant, NodeID: "node-1", Seq: 42,
		Body: []byte(`{"x":1}`), Timestamp: time.Now(), LamportClock: 7,
		HLCTimestamp: 99 << 16, ExpiresAt: time.Now().Add(time.Hour),
	}
	lad.SignRecord(&orig, priv)
	if !lad.VerifyRecord(orig) {
		t.Fatal("control failed: a freshly signed record does not verify — " +
			"the rest of this test would be meaningless")
	}

	p := r2p(orig)
	// Reconstruct with everything the port envelope actually carries, and give
	// it the original topic for free (the projection rewrites it to fleet.peer,
	// which is itself signature-covered).
	back := lad.Record{
		Topic: lad.TopicMember, NodeID: string(p.NodeID), Body: p.Body,
		LamportClock: p.Lamport, HLCTimestamp: uint64(p.HLC), Tombstone: p.Tombstone,
		AuthorPubKey: p.AuthorPubKey, Signature: p.Signature, BlobCID: p.BlobCID,
	}
	if p.ExpiresAtUnixMs != 0 {
		back.ExpiresAt = time.UnixMilli(p.ExpiresAtUnixMs)
	}
	if lad.VerifyRecord(back) {
		t.Fatal("a LAD-signed record now survives the port envelope — ports.Record " +
			"has gained the missing signature-covered fields; update this test and " +
			"the §0.5.4 note it documents")
	}

	// Restore exactly the fields ports.Record cannot express. If verification
	// returns only after ALL FOUR, these are the complete set of losses.
	back.TenantID = orig.TenantID
	back.Seq = orig.Seq
	back.Timestamp = orig.Timestamp
	back.ExpiresAt = orig.ExpiresAt // nanosecond precision, not UnixMilli
	if !lad.VerifyRecord(back) {
		t.Fatal("restoring TenantID+Seq+Timestamp+ExpiresAt(ns) did NOT restore " +
			"verification — the loss set is larger than these four and the " +
			"anchor-snapshot prerequisite is worse than documented")
	}
}

// PINNED MEASUREMENT (#M-515): the LAD read path carries NO owner signature,
// and its Body is a re-marshal rather than the received bytes.
//
// This is not a defect in the adapter — it is a property of ladcache v0.0.20's
// only bulk read, ChangesSinceHLC, which rebuilds records from the typed store
// and never populates Signature/AuthorPubKey. It is pinned here because the
// opposite is easy to assume: ports.Record documents Body/Signature as the
// owner's verbatim bytes, this adapter copies them verbatim, and both
// statements are true while the guarantee still fails.
//
// The trap worth keeping: the re-marshalled body had the SAME LENGTH as the
// owner's wire bytes (282) and different content. A length assertion passes.
func TestLADReadPathCarriesNoOwnerSignature(t *testing.T) {
	d, c := newLADDirectory(t)
	ctx := context.Background()

	rr := lad.ReachRecord{
		TenantID: testTenant, NodeID: "node-1", Seq: 5,
		Addresses: []lad.ReachAddress{{Host: "1.2.3.4", Port: 443, Proto: "ws"}},
		UpdatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour),
	}
	wire, err := json.Marshal(rr)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Apply(lad.Record{
		Topic: lad.TopicReach, TenantID: testTenant, NodeID: "node-1", Seq: 5,
		Body: wire, Timestamp: time.Now(),
		Signature: []byte("OWNER-SIGNATURE-BYTES"), AuthorPubKey: []byte("OWNER-PUBKEY"),
	}); err != nil {
		t.Fatal(err)
	}

	recs, err := d.RecordsByTopic(ctx, FleetPeerTopic)
	if err != nil {
		t.Fatal(err)
	}
	var reach *ports.Record
	for i := range recs {
		if recs[i].Key == ladKeyReach {
			reach = &recs[i]
			break
		}
	}
	if reach == nil {
		t.Fatal("no reach slot projected — the assertions below would be vacuous")
	}

	if len(reach.Signature) != 0 || len(reach.AuthorPubKey) != 0 {
		t.Fatalf("the LAD read path now carries an owner signature "+
			"(sig=%d pubkey=%d bytes) — ladcache must have gained signature "+
			"propagation; update ladToPortRecord's doc and the §0.5.4 note, and "+
			"re-evaluate whether the anchor can be fed from this adapter",
			len(reach.Signature), len(reach.AuthorPubKey))
	}
	if string(reach.Body) == string(wire) {
		t.Fatal("the LAD read path now returns the owner's verbatim body — " +
			"ChangesSinceHLC must have stopped re-marshalling; update the docs " +
			"that depend on this measurement")
	}

	// The trap, asserted so it is not lost: same length, different bytes.
	if len(reach.Body) != len(wire) {
		t.Logf("NOTE: re-marshalled body length %d differs from wire %d; the "+
			"same-length case that defeats a length check was the measured one",
			len(reach.Body), len(wire))
	}

	// Control: the cache DOES retain the owner's bytes — on a different API.
	// This is why GetLastReachBody cannot be replaced by RecordsByTopic.
	if got := c.GetLastReachBody(testTenant, "node-1"); string(got) != string(wire) {
		t.Fatalf("control failed: GetLastReachBody no longer returns the received "+
			"bytes (%d vs %d) — the claim that it is the only verbatim route is stale",
			len(got), len(wire))
	}
}

// The raw tier exists so consumers matching the PRODUCER's vocabulary do not
// have to bypass the port (#R-1464 ④). Its whole value is that RawProtocol can
// DIFFER from Protocol — if they were always equal the tier would be dead
// weight, and this asserts the difference on the case that motivated it.
func TestRawReachTierPreservesTheProducersVocabulary(t *testing.T) {
	d, c := newLADDirectory(t)
	ctx := context.Background()
	idA, _, _ := testIdentity(t)
	ladSideOfTheSameNode(t, c, idA, []string{"auth"}, "auth-svc")

	addrs, err := d.Reach(ctx, testTenantID, idA)
	if err != nil {
		t.Fatal(err)
	}
	if len(addrs) == 0 {
		t.Fatal("no reach addresses — the assertions below would be vacuous")
	}

	var sawNoiseUDP bool
	for _, a := range addrs {
		if a.Host == "" || a.Port == 0 {
			t.Fatalf("raw Host/Port not populated for %+v — a consumer needing "+
				"them apart would still have to re-split Address", a)
		}
		if net.JoinHostPort(a.Host, strconv.Itoa(a.Port)) != a.Address {
			t.Fatalf("raw Host/Port disagree with Address: %q/%d vs %q",
				a.Host, a.Port, a.Address)
		}
		if a.Protocol == "noise-udp" {
			sawNoiseUDP = true
			// THE POINT: the fixture publishes "udp" (the reach layer's own
			// name). forwarder.go filters on exactly that string.
			if a.RawProtocol != "udp" {
				t.Fatalf("RawProtocol = %q, want the producer's %q — a consumer "+
					"filtering on the reach layer's name would match nothing",
					a.RawProtocol, "udp")
			}
			if a.RawProtocol == a.Protocol {
				t.Fatal("RawProtocol equals Protocol on the renamed transport — " +
					"the raw tier is not preserving anything")
			}
		}
	}
	if !sawNoiseUDP {
		t.Fatal("no noise-udp address in the fixture — the rename case, which is " +
			"the only reason this tier exists, went untested")
	}
}

// Attrs carries the producer's open metadata so consumers reading keys the
// typed projection does not model (http_port, and either service-name
// spelling) can stay inside the port.
func TestMemberAttrsCarryTheProducersOpenMetadata(t *testing.T) {
	d, c := newLADDirectory(t)
	ctx := context.Background()
	applyLAD(t, c, lad.TopicMember, "node-attrs", lad.MemberRecord{
		TenantID: testTenant, NodeID: "node-attrs", CreatedAt: time.Now(),
		Attrs: map[string]string{"serviceName": "svc", "http_port": "8080"},
	})

	m, ok, err := d.Member(ctx, testTenantID, ports.NodeID("node-attrs"))
	if err != nil || !ok {
		t.Fatalf("member not found: ok=%v err=%v", ok, err)
	}
	if m.Attrs["http_port"] != "8080" {
		t.Fatalf("Attrs[http_port] = %q, want 8080 — a consumer reading an "+
			"unmodelled key still cannot use the port", m.Attrs["http_port"])
	}
	// The typed field stays primary and populated; the map is additive.
	if m.ServiceName != "svc" {
		t.Fatalf("ServiceName = %q — the typed projection regressed", m.ServiceName)
	}
}
