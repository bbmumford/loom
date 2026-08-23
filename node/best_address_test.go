/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/ORBTR/aether/quality"

	lad "github.com/bbmumford/ledger"
	ladcache "github.com/bbmumford/ledger/cache"

	"github.com/bbmumford/loom/directory"
)

// Characterises bestAddress's safety-relevant invariant.
//
// bestAddress selects the dial target. The invariant pinned here is
// the one whose violation is both silent and dangerous: a CROSS-ORIGIN peer
// must never be handed a private-IP candidate. Tier 0 is gated on
// `!peerCrossOrigin`; if a migration flips or drops that gate, this node would
// attempt a private dial to a foreign org — routable to the wrong machine on a
// shared ULA range, and failing silently rather than erroring.
//
// Scope is deliberately narrow: no AddressTracker, no swarm AddressTable
// (m.rt is nil, so that branch is unreachable here). Those paths deserve their
// own coverage and are NOT claimed by this file.
// A realistic manager: production never builds a ConnectionManager with a nil
// Runtime, and bestAddress reaches peerServiceHostname, which dereferences it.
// A fixture omitting rt panics inside peerServiceHostname, which is a defect in
// the fixture rather than in the code under test.
// Real NodeIDs are hex(ed25519 pubkey) = 64 chars. Production truncates them
// for logging (nodeID[:12]); a short test ID panics there. The fixture must
// look like the wire, not like a readable label.
const (
	testNodeIDA = "aa11aa11aa11aa11aa11aa11aa11aa11aa11aa11aa11aa11aa11aa11aa11aa11"
	testNodeIDB = "bb22bb22bb22bb22bb22bb22bb22bb22bb22bb22bb22bb22bb22bb22bb22bb22"
)

func newDialTestManager() *ConnectionManager {
	return &ConnectionManager{rt: &Runtime{ctx: context.Background()}}
}

func TestBestAddressWithholdsPrivateCandidatesFromCrossOriginPeers(t *testing.T) {
	m := newDialTestManager()

	privateOnly := []lad.ReachAddress{
		{Host: "fdaa:0:1234:a7b::2", Port: 41641, Proto: "udp", Scope: "private"},
	}

	sameOrigin := &peerConn{nodeID: testNodeIDA, addresses: privateOnly, crossOrigin: false}
	crossOrigin := &peerConn{nodeID: testNodeIDB, addresses: privateOnly, crossOrigin: true}

	got := m.bestAddress(sameOrigin, ProtoNoiseUDP)
	if got == "" {
		t.Fatal("control: a SAME-origin peer with a 6PN private address got no " +
			"candidate — the positive case is broken, so the negative below " +
			"would prove nothing")
	}

	if bad := m.bestAddress(crossOrigin, ProtoNoiseUDP); bad != "" {
		t.Fatalf("a CROSS-ORIGIN peer was handed the private candidate %q — this "+
			"node would attempt a private dial into a foreign org, which on a "+
			"shared ULA range can reach the wrong machine and fails silently", bad)
	}
}

// An empty address set must yield no candidate rather than a garbage one.
func TestBestAddressEmptyYieldsNoCandidate(t *testing.T) {
	m := newDialTestManager()
	p := &peerConn{nodeID: testNodeIDA}
	if got := m.bestAddress(p, ProtoNoiseUDP); got != "" {
		t.Fatalf("a peer with no addresses produced candidate %q", got)
	}
}

// The AddressTracker scoring path — the largest remaining branch of
// bestAddress, and the one a type migration is most likely to disturb because
// it keys on the ADDRESS STRING that the flatten produces.
//
// The tracker needs no elaborate setup: quality.NewAddressTracker() is a
// zero-arg constructor and RecordSuccess/RecordFailure are the whole fixture.
func trackerManager() (*ConnectionManager, *quality.AddressTracker) {
	tr := quality.NewAddressTracker()
	m := &ConnectionManager{
		rt:             &Runtime{ctx: context.Background()},
		addressTracker: tr,
	}
	return m, tr
}

// The dead-address check applies whatever the candidate count is.
//
// Placing it behind `if len(candidates) == 1 { return ... }` re-dials a peer
// whose ONLY address is known-dead every cycle for the full 30-minute cooldown.
// An early return may skip only work that genuinely cannot be done, and the
// tracker is present on that path, so a guard on candidate COUNT is unrelated
// to the work being skipped.
//
// Safe because the cooldown EXPIRES: measured AddressDeadCooldown = 30m,
// cleared immediately by any success, so returning "" suppresses dialling
// temporarily rather than stranding the peer.
func TestBestAddressSkipsDeadPathsAtAnyCandidateCount(t *testing.T) {
	m, tr := trackerManager()
	m.setOurOrigin("0:1234")

	const addr = "[fdaa:0:1234:a7b::2]:41641"
	peer := &peerConn{
		nodeID: testNodeIDA,
		addresses: []lad.ReachAddress{
			{Host: "fdaa:0:1234:a7b::2", Port: 41641, Proto: "udp", Scope: "private"},
		},
	}

	// Control: with no history the candidate IS selected.
	if got := m.bestAddress(peer, ProtoNoiseUDP); got != addr {
		t.Fatalf("control: fresh candidate = %q, want %q — the positive case is "+
			"broken so the negative below would prove nothing", got, addr)
	}

	// Kill it: consecutive failures past the tracker's dead threshold.
	for i := 0; i < 12; i++ {
		tr.RecordFailure(testNodeIDA, ProtoNoiseUDP.String(), addr)
	}
	if s, ok := tr.Score(testNodeIDA, ProtoNoiseUDP.String(), addr); !ok || !s.IsDead() {
		t.Fatalf("premise wrong: the tracker does not consider the path dead "+
			"(ok=%v score=%+v) — raise the failure count", ok, s)
	}

	// A SINGLE dead candidate now yields no address — the fix.
	if got := m.bestAddress(peer, ProtoNoiseUDP); got != "" {
		t.Fatalf("single dead candidate returned %q, want \"\" — the dead-path "+
			"skip is behind a candidate-count guard again, and the dialer will "+
			"retry a known-broken endpoint for the whole 30m cooldown", got)
	}

	// And it must still hold for multiple candidates.
	peer2 := &peerConn{
		nodeID: testNodeIDA,
		addresses: []lad.ReachAddress{
			{Host: "fdaa:0:1234:a7b::2", Port: 41641, Proto: "udp", Scope: "private"},
			{Host: "fdaa:0:1234:ccc::3", Port: 41641, Proto: "udp", Scope: "private"},
		},
	}
	const addr2 = "[fdaa:0:1234:ccc::3]:41641"
	for i := 0; i < 12; i++ {
		tr.RecordFailure(testNodeIDA, ProtoNoiseUDP.String(), addr2)
	}
	if got := m.bestAddress(peer2, ProtoNoiseUDP); got != "" {
		t.Fatalf("with TWO dead candidates bestAddress = %q, want \"\"", got)
	}
}

// With two live candidates the tracker's success history must decide, not
// slice order — that is the whole reason the scoring path exists.
func TestBestAddressPrefersTheHealthierPath(t *testing.T) {
	m, tr := trackerManager()
	m.setOurOrigin("0:1234")

	const good = "[fdaa:0:1234:aaa::1]:41641"
	const bad = "[fdaa:0:1234:bbb::2]:41641"
	peer := &peerConn{
		nodeID: testNodeIDA,
		addresses: []lad.ReachAddress{
			// bad is FIRST in slice order, so tier order alone would pick it.
			{Host: "fdaa:0:1234:bbb::2", Port: 41641, Proto: "udp", Scope: "private"},
			{Host: "fdaa:0:1234:aaa::1", Port: 41641, Proto: "udp", Scope: "private"},
		},
	}

	tr.RecordSuccess(testNodeIDA, ProtoNoiseUDP.String(), good, 10*time.Millisecond)
	tr.RecordSuccess(testNodeIDA, ProtoNoiseUDP.String(), good, 10*time.Millisecond)
	tr.RecordFailure(testNodeIDA, ProtoNoiseUDP.String(), bad)

	if got := m.bestAddress(peer, ProtoNoiseUDP); got != good {
		t.Fatalf("bestAddress = %q, want the healthier %q — history is not "+
			"overriding slice order, so a known-bad path keeps being dialled", got, good)
	}
}

// The swarm AddressTable FRESHNESS branch. AddressTable.Get reads only its
// byNode map and this test lives in package node, so the fixture is a struct
// literal.
//
// The behaviour it pins is a deliberate authority rule with a documented
// reason: peerAddresses is a per-tick snapshot taken in scanAndConnect, so an
// AddressTable record arriving between ticks (a peer machine restarting and
// re-publishing its 6PN endpoint) is invisible to it until the next scan. When
// the table holds ANY noise-udp candidate for the peer, it is treated as
// authoritative and the snapshot's 6PN entries are IGNORED — otherwise the
// dialer targets a cached pre-restart address.
func tableManager(cands map[string][]DialCandidate) *ConnectionManager {
	return &ConnectionManager{
		rt: &Runtime{
			ctx:   context.Background(),
			swarm: &SwarmIntegration{AddressTable: &AddressTable{byNode: cands}},
		},
	}
}

func TestBestAddressPrefersAddressTableOverTheStaleSnapshot(t *testing.T) {
	// Hex-valid groups: net.ParseIP rejects mnemonic labels like "new"/"old",
	// and an unparseable host is silently SKIPPED by the Tier-0 filter — so a
	// readable fixture tests nothing. Fourth instance of that today.
	const staleHost = "fdaa:0:1234:0ad::1"
	const freshHost = "fdaa:0:1234:9ef::9"

	m := tableManager(map[string][]DialCandidate{
		testNodeIDA: {{
			NodeID: testNodeIDA, Transport: "noise-udp",
			Host: freshHost, Port: 41641, Scope: "private",
		}},
	})
	m.setOurOrigin("0:1234")

	peer := &peerConn{
		nodeID: testNodeIDA,
		// The per-tick snapshot still holds the PRE-RESTART address.
		addresses: []lad.ReachAddress{
			{Host: staleHost, Port: 41641, Proto: "udp", Scope: "private"},
		},
	}

	got := m.bestAddress(peer, ProtoNoiseUDP)
	if got == "["+staleHost+"]:41641" {
		t.Fatalf("bestAddress returned the STALE snapshot address %q while the "+
			"AddressTable held a fresher 6PN endpoint — a peer that restarted "+
			"would be dialled at its pre-restart address until the next scan tick", got)
	}
	if got != "["+freshHost+"]:41641" {
		t.Fatalf("bestAddress = %q, want the AddressTable's fresh %q", got, "["+freshHost+"]:41641")
	}
}

// And the documented fallback: with NO noise-udp entry in the table, the
// snapshot is used. Without this the branch above could be "the table always
// wins", which would break peers the swarm fabric has not yet seen.
func TestBestAddressFallsBackToSnapshotWhenTableHasNoUDPEntry(t *testing.T) {
	const snapHost = "fdaa:0:1234:5ab::1"

	m := tableManager(map[string][]DialCandidate{
		// A websocket-only entry must NOT suppress the snapshot's 6PN address.
		testNodeIDA: {{
			NodeID: testNodeIDA, Transport: "websocket",
			Host: "203.0.113.9", Port: 443, Scope: "public",
		}},
	})
	m.setOurOrigin("0:1234")

	peer := &peerConn{
		nodeID: testNodeIDA,
		addresses: []lad.ReachAddress{
			{Host: snapHost, Port: 41641, Proto: "udp", Scope: "private"},
		},
	}

	if got := m.bestAddress(peer, ProtoNoiseUDP); got != "["+snapHost+"]:41641" {
		t.Fatalf("bestAddress = %q, want the snapshot's %q — a websocket-only "+
			"AddressTable entry suppressed the peer's known 6PN address",
			got, "["+snapHost+"]:41641")
	}
}

// 🛑 THE TIER-3 ASYMMETRY REPRODUCES A NAMED PRODUCTION INCIDENT IF INVERTED.
//
// Cross-org noise-UDP falls back to the peer's service hostname on the
// configured VL1 UDP port. Same-org dials deliberately DO NOT, even though the
// hostname is equally available — because anycast UDP cannot guarantee
// per-packet machine affinity: the handshake succeeds, then later datagrams
// 5-tuple-hash to a different machine with no session state and the session
// dies. The code comment records this as the observed v0.0.388 symptom
// ("walker probes succeeded, then the upgraded session died inside a single
// ping window").
//
// So this is not a style preference: making the fallback symmetric
// reintroduces a diagnosed outage. Pinned in both directions.
func hostnameManager(t *testing.T, crossOrigin bool) *ConnectionManager {
	t.Helper()
	c := ladcache.NewDirectoryCache()
	mb, err := json.Marshal(lad.MemberRecord{
		TenantID: "", NodeID: testNodeIDA, CreatedAt: time.Now(),
		// Producer key + a value the hostname check accepts (needs a dot).
		Attrs: map[string]string{"serviceName": "node.orbtr.io"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Apply(lad.Record{
		Topic: lad.TopicMember, TenantID: "", NodeID: testNodeIDA,
		Body: mb, Timestamp: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	ld, err := directory.NewLADDirectory(c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ld.Close() })

	rt := &Runtime{ctx: context.Background(), cache: c, liveDir: ld, liveDirRaw: ld}
	rt.cfg.VL1.UDPPort = 41641
	return &ConnectionManager{rt: rt}
}

func TestTier3HostnameFallbackIsCrossOrgOnly(t *testing.T) {
	// No addresses at all, so the hostname fallback is the ONLY possible
	// candidate — this isolates Tier 3 from every other tier.
	t.Run("cross-org GETS the hostname fallback", func(t *testing.T) {
		m := hostnameManager(t, true)
		peer := &peerConn{nodeID: testNodeIDA, crossOrigin: true}
		got := m.bestAddress(peer, ProtoNoiseUDP)
		if got != "node.orbtr.io:41641" {
			t.Fatalf("cross-org bestAddress = %q, want the service-hostname "+
				"fallback %q — cross-org noise-UDP has no dialable address "+
				"until the AddressTable merge without it", got, "node.orbtr.io:41641")
		}
	})

	t.Run("same-org must NOT take it (anycast affinity)", func(t *testing.T) {
		m := hostnameManager(t, false)
		peer := &peerConn{nodeID: testNodeIDA, crossOrigin: false}
		if got := m.bestAddress(peer, ProtoNoiseUDP); got != "" {
			t.Fatalf("same-org bestAddress = %q — the anycast hostname fallback "+
				"fired for a SAME-ORG peer. That reintroduces the v0.0.388 "+
				"symptom: handshake succeeds, later datagrams hash to a machine "+
				"with no session state, session dies inside a ping window", got)
		}
	})
}

// Tier ordering is a property of the ADDRESS, so it applies unconditionally.
//
// Public UDP is tiered IPv4=1 (preferred) / IPv6=2, "because some platforms do
// not proxy UDP on IPv6". The tier sort used to live inside the tracker-only
// branch, so with no tracker the append order decided and IPv4 preference did
// not exist — on what is a SUPPORTED path (mesh_services.go notes callers
// "keep working with addressTracker == nil").
//
// Both paths must now agree, which is what makes this test worth keeping.
func TestPublicUDPTierOrderAppliesWithOrWithoutATracker(t *testing.T) {
	m := &ConnectionManager{rt: &Runtime{ctx: context.Background()}}
	peer := &peerConn{
		nodeID:      testNodeIDA,
		crossOrigin: true, // keep Tier 0 out of the way
		addresses: []lad.ReachAddress{
			// IPv6 listed FIRST, so slice order alone would pick it.
			{Host: "2001:db8::7", Port: 41641, Proto: "udp", Scope: "public"},
			{Host: "203.0.113.7", Port: 41641, Proto: "udp", Scope: "public"},
		},
	}
	// No tracker: tier must still win over slice order.
	if got := m.bestAddress(peer, ProtoNoiseUDP); got != "203.0.113.7:41641" {
		t.Fatalf("tracker-less bestAddress = %q, want the IPv4 %q — tier ordering "+
			"has been re-coupled to the tracker, and IPv4 preference silently "+
			"stops existing wherever no tracker is wired", got, "203.0.113.7:41641")
	}

	// And with a tracker present the answer is identical — the two paths agree.
	m2 := &ConnectionManager{
		rt:             &Runtime{ctx: context.Background()},
		addressTracker: quality.NewAddressTracker(),
	}
	if got := m2.bestAddress(peer, ProtoNoiseUDP); got != "203.0.113.7:41641" {
		t.Fatalf("with a tracker present bestAddress = %q, want the IPv4 %q — "+
			"tier preference is not applied even on the sorted path",
			got, "203.0.113.7:41641")
	}
}

// The WS/TLS arm returns a DIFFERENT ADDRESS SHAPE from the UDP arm, and that
// difference is load-bearing.
//
// 🛑 UDP tiers return "host:port" (net.JoinHostPort). The WS Tier-0 path
// returns a BARE HOSTNAME — the code comment says "caller prepends wss://".
// A migration that harmonises address shape (the obvious tidy when the type
// changes) would hand the WS dialer "wss://host:port" instead of "wss://host",
// or a bracketed IPv6 where a TLS SNI hostname is expected.
//
// Pinned because it is exactly the kind of "consistency" change a type
// migration invites, and nothing else in the suite asserts the shape.
func TestWebSocketTierReturnsBareHostnameNotHostPort(t *testing.T) {
	m := &ConnectionManager{rt: &Runtime{ctx: context.Background()}}
	peer := &peerConn{
		nodeID:      testNodeIDA,
		crossOrigin: true, // keep the same-org 6PN WS path out of the way
		addresses: []lad.ReachAddress{
			{Host: "devices.orbtr.io", Port: 443, Proto: "wss", Scope: "public"},
		},
	}

	got := m.bestAddress(peer, ProtoWebSocket)
	if got != "devices.orbtr.io" {
		t.Fatalf("WS Tier-0 = %q, want the BARE hostname %q. If this now returns "+
			"host:port, the caller's wss:// prefix produces a malformed URL — the "+
			"WS arm's shape differs from the UDP arm's by design", got, "devices.orbtr.io")
	}
	if strings.Contains(got, ":") {
		t.Fatalf("WS Tier-0 returned %q — it contains a port separator, so the "+
			"address shape has been harmonised with the UDP arm", got)
	}
}

// The TLS bootstrap host short-circuits every tier: it is the address that is
// known to work before any reach record exists, and it must not be displaced
// by candidate scoring.
func TestTLSBootstrapHostShortCircuitsTierSelection(t *testing.T) {
	m := &ConnectionManager{rt: &Runtime{ctx: context.Background()}}
	peer := &peerConn{
		nodeID:        testNodeIDA,
		crossOrigin:   true,
		bootstrapHost: "node.hstles.com",
		// A competing wss record that must NOT win for ProtoTLS.
		addresses: []lad.ReachAddress{
			{Host: "devices.orbtr.io", Port: 443, Proto: "wss", Scope: "public"},
		},
	}

	if got := m.bestAddress(peer, ProtoTLS); got != "node.hstles.com" {
		t.Fatalf("ProtoTLS = %q, want the bootstrap host %q — the TLS path must "+
			"use the address known to work before reach records exist", got, "node.hstles.com")
	}

	// CONTRAST: the same peer on ProtoWebSocket does NOT take the bootstrap
	// short-circuit, so the wss record wins. This proves the short-circuit is
	// protocol-scoped rather than unconditional.
	if got := m.bestAddress(peer, ProtoWebSocket); got != "devices.orbtr.io" {
		t.Fatalf("ProtoWebSocket = %q, want the wss record %q — the TLS bootstrap "+
			"short-circuit is firing for the wrong protocol", got, "devices.orbtr.io")
	}
}
