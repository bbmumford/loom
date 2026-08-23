/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"encoding/json"
	ladcache "github.com/bbmumford/ledger/cache"
	"testing"
	"time"

	lad "github.com/bbmumford/ledger"
)

// CHARACTERISATION of the same-org/cross-org classification, written BEFORE
// the peer.addresses type migration touches it.
//
// 🛑 WHY THIS EXISTS AND WHY NOW. `isCrossOrigin`, `isCrossOriginLocked`,
// `bestAddress`, `extractOriginPrefix` and `isCrossRegion` were ALL at 0.0%
// statement coverage — measured with `go tool cover`, not inferred from a grep
// (a grep for test references returns 0 even for functions that ARE exercised
// indirectly, which is how `parseSixPNUDPAddr` reads).
//
// That matters because this classification decides whether a peer is eligible
// for a direct noise-UDP dial. Getting it wrong does not error: it silently
// routes everything through relay, or worse, treats a foreign org as same-org.
// A type migration through untested code of that shape is how the silent
// failures this work keeps finding get made — so the behaviour gets pinned
// first, and the migration must then preserve exactly these answers.
//
// These tests assert CURRENT behaviour, including the conservative defaults.
// They are not a judgement that every answer below is desirable.

func newOriginTestManager(t *testing.T, ourOrigin string) *ConnectionManager {
	t.Helper()
	m := &ConnectionManager{}
	if ourOrigin != "" {
		m.setOurOrigin(ourOrigin)
	}
	return m
}

func TestExtractOriginPrefixCharacterisation(t *testing.T) {
	cases := []struct {
		name, ip, want string
	}{
		{"fly 6PN full address", "fdaa:0:1234:a7b:c8d:2:0", "0:1234"},
		{"fly 6PN short", "fdaa:1:2:3", "1:2"},
		{"non-fly ULA (tailscale) yields nothing", "fd7a:115c:a1e0::1", ""},
		{"ipv4 yields nothing", "10.0.0.5", ""},
		{"public ipv6 yields nothing", "2001:db8::1", ""},
		{"empty yields nothing", "", ""},
		{"fdaa prefix but too few segments", "fdaa:1", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := extractOriginPrefix(tc.ip); got != tc.want {
				t.Fatalf("extractOriginPrefix(%q) = %q, want %q", tc.ip, got, tc.want)
			}
		})
	}
}

func TestIsCrossOriginCharacterisation(t *testing.T) {
	const ourOrigin = "0:1234"
	priv := func(host string) lad.ReachAddress {
		return lad.ReachAddress{Host: host, Port: 41641, Proto: "udp", Scope: "private"}
	}
	pub := func(host string) lad.ReachAddress {
		return lad.ReachAddress{Host: host, Port: 443, Proto: "ws", Scope: "public"}
	}

	cases := []struct {
		name      string
		ourOrigin string
		addrs     []lad.ReachAddress
		want      bool
		why       string
	}{
		{
			name: "same 6PN org is same-origin", ourOrigin: ourOrigin,
			addrs: []lad.ReachAddress{priv("fdaa:0:1234:a7b::2")}, want: false,
			why: "the only recognised private entry matches our org",
		},
		{
			name: "different 6PN org is cross-origin", ourOrigin: ourOrigin,
			addrs: []lad.ReachAddress{priv("fdaa:0:9999:a7b::2")}, want: true,
			why: "org prefix differs",
		},
		{
			name: "unknown OUR origin is conservatively cross", ourOrigin: "",
			addrs: []lad.ReachAddress{priv("fdaa:0:1234:a7b::2")}, want: true,
			why: "we cannot classify without knowing our own org — fails safe",
		},
		{
			name: "no private entries at all is conservatively cross", ourOrigin: ourOrigin,
			addrs: []lad.ReachAddress{pub("203.0.113.7")}, want: true,
			why: "no usable private entry",
		},
		{
			name: "empty address list is conservatively cross", ourOrigin: ourOrigin,
			addrs: nil, want: true,
			why: "nothing to classify on",
		},
		{
			name:      "non-6PN private entry does NOT drive the decision",
			ourOrigin: ourOrigin,
			// A Docker bridge / Tailscale address marked private must be
			// skipped, not treated as a foreign org.
			addrs: []lad.ReachAddress{priv("fd7a:115c::1"), priv("fdaa:0:1234:a7b::2")},
			want:  false,
			why:   "the unrecognised private entry is skipped; the 6PN one matches",
		},
		{
			name:      "a non-6PN private entry ALONE is still cross",
			ourOrigin: ourOrigin,
			addrs:     []lad.ReachAddress{priv("fd7a:115c::1")},
			want:      true,
			why:       "skipped entries leave sawPrivate false → conservative",
		},
		{
			name:      "ANY foreign 6PN org wins over a matching one",
			ourOrigin: ourOrigin,
			addrs:     []lad.ReachAddress{priv("fdaa:0:1234:a7b::2"), priv("fdaa:0:9999:a7b::2")},
			want:      true,
			why:       "the first foreign org returns true immediately",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := newOriginTestManager(t, tc.ourOrigin)
			if got := m.isCrossOrigin(tc.addrs); got != tc.want {
				t.Fatalf("isCrossOrigin = %v, want %v — %s", got, tc.want, tc.why)
			}
		})
	}
}

// The classification must be CONSERVATIVE in both unknown directions: an
// unknown local origin and an unknown peer address both mean "cross", because
// treating a foreign org as same-org would authorise a direct dial that should
// not happen. This asserts the direction of the default explicitly, so a
// migration that flips it fails loudly rather than widening direct-dial
// eligibility in silence.
func TestIsCrossOriginDefaultsFailSafe(t *testing.T) {
	m := newOriginTestManager(t, "")
	if !m.isCrossOrigin(nil) {
		t.Fatal("unknown local origin + no addresses classified as SAME-origin — " +
			"the default must fail toward cross, or direct dialling is authorised " +
			"on no evidence")
	}

	m2 := newOriginTestManager(t, "0:1234")
	if !m2.isCrossOrigin([]lad.ReachAddress{{Host: "", Scope: "private"}}) {
		t.Fatal("an empty private host classified as SAME-origin")
	}
}

// 🔑 THE ASYMMETRY IS DELIBERATE AND MUST SURVIVE ANY TIDYING REFACTOR.
//
// isCrossOrigin defaults UNKNOWN -> CROSS (see TestIsCrossOriginDefaultsFailSafe).
// isCrossRegion defaults UNKNOWN -> SAME.
//
// They look inconsistent and are not: cross-ORIGIN gates whether a private
// dial is attempted, so the safe default is "cross" (attempt nothing private).
// cross-REGION only biases routing preference, so the safe default is "same"
// (do not penalise a peer we cannot place). A refactor that harmonises them
// in either direction breaks one of the two — this pins both so it fails
// loudly rather than degrading dialling or routing in silence.
func TestCrossRegionAndCrossOriginDefaultInOppositeDirections(t *testing.T) {
	t.Run("isCrossRegion: unknown self region -> SAME", func(t *testing.T) {
		m := &ConnectionManager{}
		if m.isCrossRegion("syd") {
			t.Fatal("unknown self region classified as CROSS-region")
		}
	})
	t.Run("isCrossRegion: unknown peer region -> SAME", func(t *testing.T) {
		m := &ConnectionManager{selfRegion: "iad"}
		if m.isCrossRegion("") {
			t.Fatal("unknown peer region classified as CROSS-region")
		}
	})
	t.Run("isCrossRegion: both known and differing -> CROSS", func(t *testing.T) {
		m := &ConnectionManager{selfRegion: "iad"}
		if !m.isCrossRegion("syd") {
			t.Fatal("iad vs syd classified as same-region")
		}
	})
	t.Run("isCrossRegion: both known and equal -> SAME", func(t *testing.T) {
		m := &ConnectionManager{selfRegion: "iad"}
		if m.isCrossRegion("iad") {
			t.Fatal("iad vs iad classified as cross-region")
		}
	})
	// The counterpart default, asserted HERE too so the contrast is visible
	// in one place rather than inferred across two tests.
	t.Run("isCrossOrigin: unknown -> CROSS (the opposite default)", func(t *testing.T) {
		m := &ConnectionManager{}
		if !m.isCrossOrigin(nil) {
			t.Fatal("isCrossOrigin's unknown default is no longer CROSS — the " +
				"asymmetry with isCrossRegion has been harmonised away, and " +
				"private dialling is now authorised on no evidence")
		}
	})
}

// isCrossOriginLocked is the lock-held lookup used on the connection path. Its
// fallback when a peer has no known addresses is CROSS — the same fail-safe
// direction as isCrossOrigin, and for a documented reason: a misclassified
// same-org peer pays a 34-byte preamble its receiver ignores, while a
// misclassified cross-org peer's noise dials fail on every retry.
func TestIsCrossOriginLockedFallsBackToCross(t *testing.T) {
	priv6PN := []lad.ReachAddress{
		{Host: "fdaa:0:1234:a7b::2", Port: 41641, Proto: "udp", Scope: "private"},
	}

	t.Run("unknown peer -> CROSS", func(t *testing.T) {
		m := &ConnectionManager{peers: map[string]*peerConn{}}
		m.setOurOrigin("0:1234")
		if !m.isCrossOriginLocked("never-seen") {
			t.Fatal("an unknown peer classified as same-origin")
		}
	})
	t.Run("known peer with NO addresses -> CROSS", func(t *testing.T) {
		m := &ConnectionManager{peers: map[string]*peerConn{"p": {nodeID: "p"}}}
		m.setOurOrigin("0:1234")
		if !m.isCrossOriginLocked("p") {
			t.Fatal("a peer with no known addresses classified as same-origin")
		}
	})
	t.Run("known peer WITH same-org addresses -> delegates to isCrossOrigin", func(t *testing.T) {
		m := &ConnectionManager{peers: map[string]*peerConn{"p": {nodeID: "p", addresses: priv6PN}}}
		m.setOurOrigin("0:1234")
		if m.isCrossOriginLocked("p") {
			t.Fatal("a peer with a matching 6PN address classified as cross-origin " +
				"— the delegation to isCrossOrigin is not happening")
		}
	})
}

// 🔒 computeCrossOriginForNode — "Audit critical fix #4", called by
// AcceptMeshConnection BEFORE it takes m.mu.
//
// 🛑 ITS THREE PROPERTIES, IN THE ORDER THE CODE TRIES THEM:
//  1. an existing peer's addresses are the fast signal — no LAD lookup at all;
//  2. otherwise a LAD reach lookup, deliberately WITH m.mu DROPPED (the audit
//     fix: "holding m.mu across that I/O would serialize the entire
//     connection table");
//  3. and when nothing is known it returns TRUE — CROSS-origin.
//
// 🔑 (3) is the fail-safe direction and it matters: cross-origin means "do not
// attempt a private dial", so an unknown peer is treated as foreign rather
// than assumed local. Inverting it would authorise private dialling on no
// evidence — the same default TestIsCrossOriginDefaultsFailSafe pins one
// level down.

func TestComputeCrossOriginUsesExistingPeerAddressesFirst(t *testing.T) {
	m := &ConnectionManager{peers: map[string]*peerConn{}}
	m.setOurOrigin("0:1234")

	// A peer we already know, holding a SAME-org 6PN address, and NO runtime
	// at all — so a LAD lookup would nil-deref if the fast path did not win.
	m.peers[testNodeIDA] = &peerConn{nodeID: testNodeIDA, addresses: []lad.ReachAddress{
		{Host: "fdaa:0:1234:a7b::2", Port: 41641, Proto: "udp", Scope: "private"},
	}}
	if m.rt != nil {
		t.Fatal("premise wrong: this case needs NO runtime, so only the fast path can answer")
	}

	if m.computeCrossOriginForNode(testNodeIDA) {
		t.Fatal("a peer whose known address is in OUR org was classified " +
			"cross-origin — private dialling is suppressed for a same-org peer")
	}

	// Same fixture, FOREIGN org: the fast path must still answer, oppositely.
	m.peers[testNodeIDB] = &peerConn{nodeID: testNodeIDB, addresses: []lad.ReachAddress{
		{Host: "fdaa:0:9999:a7b::2", Port: 41641, Proto: "udp", Scope: "private"},
	}}
	if !m.computeCrossOriginForNode(testNodeIDB) {
		t.Fatal("a peer in a FOREIGN org was classified same-origin — a private " +
			"dial would be attempted across orgs")
	}
}

// 🛡️ THE FAIL-SAFE DEFAULT: nothing known anywhere ⇒ CROSS.
func TestComputeCrossOriginDefaultsToCrossWhenNothingIsKnown(t *testing.T) {
	// No peer entry, no runtime, no cache — the state during a cold accept.
	m := &ConnectionManager{peers: map[string]*peerConn{}}
	m.setOurOrigin("0:1234")
	if !m.computeCrossOriginForNode(testNodeIDA) {
		t.Fatal("an entirely unknown peer was classified SAME-origin — " +
			"AcceptMeshConnection would authorise a private dial on no evidence")
	}

	// A peer entry with NO addresses is equally uninformative.
	m.peers[testNodeIDA] = &peerConn{nodeID: testNodeIDA}
	if !m.computeCrossOriginForNode(testNodeIDA) {
		t.Fatal("a known peer with no addresses was classified SAME-origin")
	}
}

// The LAD fallback: with no peer addresses, the reach cache answers — and the
// classification must follow the record it finds.
func TestComputeCrossOriginFallsBackToTheReachCache(t *testing.T) {
	c := ladcache.NewDirectoryCache()
	m := &ConnectionManager{
		peers: map[string]*peerConn{},
		rt:    &Runtime{ctx: context.Background(), cache: c},
	}
	m.setOurOrigin("0:1234")

	body, err := json.Marshal(lad.ReachRecord{
		NodeID:    testNodeIDA,
		Addresses: []lad.ReachAddress{{Host: "fdaa:0:1234:a7b::2", Port: 41641, Proto: "udp", Scope: "private"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Apply(lad.Record{
		Topic: lad.TopicReach, NodeID: testNodeIDA, Body: body, Timestamp: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}

	// PREMISE: no peer entry, so only the LAD path can answer.
	if _, ok := m.peers[testNodeIDA]; ok {
		t.Fatal("premise wrong: a peer entry would take the fast path")
	}
	if m.computeCrossOriginForNode(testNodeIDA) {
		t.Fatal("a peer whose REACH RECORD is in our org was classified " +
			"cross-origin — the LAD fallback is not being consulted, so every " +
			"cold accept suppresses private dialling until a peer entry exists")
	}
}
