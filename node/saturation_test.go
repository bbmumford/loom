/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"context"
	"testing"
	"time"
)

func TestParseSaturationTags(t *testing.T) {
	cases := []struct {
		name     string
		tags     []string
		wantSat  bool
		wantTill int64
	}{
		{"both set", []string{"saturated=1", "backoff_until=1700000000"}, true, 1700000000},
		{"with other tags", []string{"service=node", "saturated=1", "region=iad", "backoff_until=42"}, true, 42},
		{"not saturated", []string{"saturated=0", "backoff_until=42"}, false, 42},
		{"absent", []string{"service=node", "region=iad"}, false, 0},
		{"malformed deadline does not panic", []string{"saturated=1", "backoff_until=notanumber"}, true, 0},
		{"empty", nil, false, 0},
		{"bare tag ignored", []string{"saturated", "saturated=1"}, true, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sat, till := parseSaturationTags(c.tags)
			if sat != c.wantSat || till != c.wantTill {
				t.Fatalf("parseSaturationTags(%v) = (%v,%d), want (%v,%d)", c.tags, sat, till, c.wantSat, c.wantTill)
			}
		})
	}
}

func TestSaturationCap(t *testing.T) {
	cases := []struct {
		name      string
		target    int
		connCount int
		backoff   bool
		want      int
	}{
		{"no backoff leaves target", 5, 2, false, 5},
		{"backoff caps growth to connCount", 6, 4, true, 4},
		{"backoff below connCount unchanged", 3, 4, true, 3},
		{"backoff never drops below standby floor", 4, 1, true, 2},
		{"backoff floor when connCount zero", 5, 0, true, 2},
		{"backoff target already at floor", 2, 1, true, 2},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := saturationCap(c.target, c.connCount, c.backoff); got != c.want {
				t.Fatalf("saturationCap(%d,%d,%v) = %d, want %d", c.target, c.connCount, c.backoff, got, c.want)
			}
		})
	}
}

// livePeer is a connected peer with one active grade C transport and no
// better-grade address, whose saturated tag and deadline this node read at the
// given instant. crossOrigin varies its address origin so a case can show the
// backoff decision does not read it.
func livePeer(crossOrigin bool, until int64, read time.Time) *peerConn {
	p := &peerConn{
		state:       PeerConnected,
		crossOrigin: crossOrigin,
		transports:  map[Protocol]*transportConn{ProtoWebSocket: {protocol: ProtoWebSocket, grade: GradeC}},
	}
	p.readBackoffHint(true, until, read)
	return p
}

// TestPeerWantsBackoff holds the advisory rules: a backoff_until that is
// missing, zero, malformed (parsed to 0), negative, expired or further ahead
// than the publisher's TTL plus the admitted skew is no hint; a live hint
// delays extra paths to a peer that already has a live one, whatever the
// peer's address origin or grade; a peer with no live path is never backed
// off, because the dial is its only path, and a dormant transport is not a
// live path; a deadline this node holds no reading instant for is no hint,
// because the bound on a hint's effect runs from that instant; and a run of
// restamped deadlines is backed off until it has lasted
// saturationBackoffMaxEffect.
func TestPeerWantsBackoff(t *testing.T) {
	m := &ConnectionManager{}
	now := time.Unix(1_000_000, 0)
	fresh := now.Add(saturationBackoffTTL / 2).Unix()
	beyond := now.Add(saturationBackoffTTL + saturationBackoffSkew + time.Minute).Unix()
	noPath := &peerConn{crossOrigin: true}
	noPath.readBackoffHint(true, fresh, now)
	dormantOnly := livePeer(true, fresh, now)
	dormantOnly.transports[ProtoWebSocket].isDormant = true
	// restamping is a peer whose first hint this node read at the given
	// instant and that has published a new deadline every 20 seconds since,
	// so its run of hints is unbroken up to now.
	restamping := func(since time.Time) *peerConn {
		p := livePeer(true, since.Add(saturationBackoffTTL).Unix(), since)
		for at := since.Add(20 * time.Second); !at.After(now); at = at.Add(20 * time.Second) {
			p.readBackoffHint(true, at.Add(saturationBackoffTTL).Unix(), at)
		}
		p.readBackoffHint(true, fresh, now)
		return p
	}
	unread := &peerConn{
		state:            PeerConnected,
		peerSaturated:    true,
		peerBackoffUntil: fresh,
		transports:       map[Protocol]*transportConn{ProtoWebSocket: {protocol: ProtoWebSocket, grade: GradeC}},
	}

	for _, tc := range []struct {
		name string
		p    *peerConn
		want bool
	}{
		{"nil peer", nil, false},
		{"unsaturated peer", &peerConn{}, false},
		{"a fresh deadline on a peer with a live path", livePeer(true, fresh, now), true},
		{"a same-origin grade C peer is not exempt", livePeer(false, fresh, now), true},
		{"a missing or malformed deadline is no hint", livePeer(true, 0, now), false},
		{"a negative deadline is no hint", livePeer(true, -5, now), false},
		{"an expired deadline is no hint", livePeer(true, now.Unix()-1, now), false},
		{"a deadline past the TTL and skew is no hint", livePeer(true, beyond, now), false},
		{"a peer with no live path is never backed off", noPath, false},
		{"a connected peer whose only transport is dormant is never backed off", dormantOnly, false},
		{"a deadline with no reading instant is no hint", unread, false},
		{"a run of restamped deadlines one second short of the bound is backed off",
			restamping(now.Add(time.Second - saturationBackoffMaxEffect)), true},
		{"a run of restamped deadlines that has lasted the bound is not backed off",
			restamping(now.Add(-saturationBackoffMaxEffect)), false},
	} {
		if got := m.peerWantsBackoff(tc.p, now); got != tc.want {
			t.Errorf("%s: peerWantsBackoff = %v, want %v", tc.name, got, tc.want)
		}
	}
}

// countPeersAdvertisingBackoff counts by the same rule peerWantsBackoff reads
// the deadline by: a saturated tag with no live deadline is not a peer asking
// for backoff.
func TestTheBackoffMetricCountsOnlyLiveHints(t *testing.T) {
	now := time.Now()
	m := &ConnectionManager{peers: map[string]*peerConn{
		"live":    livePeer(true, now.Add(saturationBackoffTTL/2).Unix(), now),
		"zero":    livePeer(true, 0, now),
		"expired": livePeer(true, now.Add(-time.Minute).Unix(), now),
		"future":  livePeer(true, now.Add(time.Hour).Unix(), now),
	}}
	if got := m.countPeersAdvertisingBackoff(); got != 1 {
		t.Fatalf("countPeersAdvertisingBackoff = %d, want 1 (only the live deadline)", got)
	}
}

// A peer that restamps its deadline before the last one lapses is backed off
// for saturationBackoffMaxEffect from the first hint this node read, and no
// longer. The peer here is saturated for three times that long and publishes a
// new deadline every 20 seconds, so its hint never lapses. While the bound
// runs peerWantsBackoff holds its extra paths back and the upgrade walker
// emits no probe for it; from the bound on the walker probes it again,
// although the peer is still advertising.
func TestAPeerThatKeepsRestampingItsDeadlineIsBackedOffForABoundedTime(t *testing.T) {
	start := time.Now()
	m := walkerManager(t)
	p := activePeer(start, ProtoWebSocket)
	m.peers[p.nodeID] = p

	for elapsed := time.Duration(0); elapsed <= 3*saturationBackoffMaxEffect; elapsed += 20 * time.Second {
		now := start.Add(elapsed)
		m.mu.Lock()
		p.readBackoffHint(true, now.Add(saturationBackoffTTL).Unix(), now)
		backedOff := m.peerWantsBackoff(p, now)
		m.mu.Unlock()
		// The walker spaces its own probes per peer; clearing that spacing
		// leaves the backoff as the only gate that varies across this loop.
		m.walkerProbeMu.Lock()
		m.walkerProbeAt = nil
		m.walkerProbeMu.Unlock()
		probes := len(m.snapshotUpgradeCandidates(now))

		if within := elapsed < saturationBackoffMaxEffect; within {
			if !backedOff || probes != 0 {
				t.Fatalf("%v into the run: backed off = %v with %d upgrade probes, want the peer backed off and no probe",
					elapsed, backedOff, probes)
			}
		} else if backedOff || probes == 0 {
			t.Fatalf("%v into the run, past the %v bound: backed off = %v with %d upgrade probes, "+
				"want the peer no longer backed off and the walker probing it",
				elapsed, saturationBackoffMaxEffect, backedOff, probes)
		}
	}
}

// A run of hints ends when the peer stops advertising saturation or lets its
// deadline lapse, and the next live hint starts a new run with a new bound. A
// peer whose earlier run reached the bound is therefore backed off again the
// next time it becomes saturated.
func TestABackoffRunThatEndsGivesTheNextHintANewBound(t *testing.T) {
	m := &ConnectionManager{}
	start := time.Unix(1_000_000, 0)
	deadline := func(at time.Time) int64 { return at.Add(saturationBackoffTTL).Unix() }

	// pastTheBound is a peer whose run of restamped hints has outlasted the
	// bound, with the instant of its last reading.
	pastTheBound := func(t *testing.T) (*peerConn, time.Time) {
		t.Helper()
		p := livePeer(false, deadline(start), start)
		now := start
		for now.Sub(start) < saturationBackoffMaxEffect {
			now = now.Add(20 * time.Second)
			p.readBackoffHint(true, deadline(now), now)
		}
		if m.peerWantsBackoff(p, now) {
			t.Fatalf("premise wrong: %v into an unbroken run the peer is still backed off", now.Sub(start))
		}
		return p, now
	}

	t.Run("the peer stops advertising and saturates again", func(t *testing.T) {
		p, now := pastTheBound(t)
		now = now.Add(20 * time.Second)
		p.readBackoffHint(false, 0, now)
		now = now.Add(20 * time.Second)
		p.readBackoffHint(true, deadline(now), now)
		if !m.peerWantsBackoff(p, now) {
			t.Fatal("a peer that cleared its saturated tag and set it again was not backed off")
		}
	})

	t.Run("the peer clears its tag while its last deadline is still ahead", func(t *testing.T) {
		p, now := pastTheBound(t)
		stillAhead := deadline(now)
		now = now.Add(20 * time.Second)
		p.readBackoffHint(false, stillAhead, now)
		now = now.Add(20 * time.Second)
		p.readBackoffHint(true, deadline(now), now)
		if !m.peerWantsBackoff(p, now) {
			t.Fatal("a peer that cleared its saturated tag and set it again was not backed off")
		}
	})

	t.Run("a reading finds the deadline lapsed", func(t *testing.T) {
		p, now := pastTheBound(t)
		lapsed := deadline(now)
		now = time.Unix(lapsed, 0).Add(20 * time.Second)
		p.readBackoffHint(true, lapsed, now)
		now = now.Add(20 * time.Second)
		p.readBackoffHint(true, deadline(now), now)
		if !m.peerWantsBackoff(p, now) {
			t.Fatal("a peer whose deadline lapsed and that then published a new one was not backed off")
		}
	})

	t.Run("the deadline lapses between two readings", func(t *testing.T) {
		p, now := pastTheBound(t)
		now = time.Unix(deadline(now), 0).Add(20 * time.Second)
		p.readBackoffHint(true, deadline(now), now)
		if !m.peerWantsBackoff(p, now) {
			t.Fatal("a new deadline read after the last one had lapsed was not backed off")
		}
	})
}

// scanAndConnect reads a peer's saturation from its swarm dial candidates
// through readBackoffHint, both for a peer the connection table already holds
// and for one the scan adds, so each hint has the reading instant
// peerWantsBackoff bounds it from. The manager's ID sorts after both peers, so
// dialOwned leaves the added peer to dial first and the scan opens no
// connection.
func TestScanAndConnectReadsAPeersBackoffHint(t *testing.T) {
	const held, added = testNodeIDA, testNodeIDB
	until := time.Now().Add(saturationBackoffTTL / 2).Unix()
	candidates := func(nodeID string) []DialCandidate {
		return []DialCandidate{{
			NodeID: nodeID, Transport: "websocket", Host: "peer.example.com", Port: 443, Scope: "public",
			Saturated: true, BackoffUntil: until,
		}}
	}
	m := &ConnectionManager{
		selfID: "cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33",
		peers: map[string]*peerConn{held: {
			nodeID:         held,
			state:          PeerConnected,
			outboundDialed: true,
			transports:     map[Protocol]*transportConn{ProtoWebSocket: {protocol: ProtoWebSocket, grade: GradeC}},
		}},
		rt: &Runtime{swarm: &SwarmIntegration{AddressTable: &AddressTable{byNode: map[string][]DialCandidate{
			held:  candidates(held),
			added: candidates(added),
		}}}},
	}

	m.scanAndConnect(context.Background())

	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	if !m.peerWantsBackoff(m.peers[held], now) {
		t.Error("a held peer with a live path whose candidates carry a live hint was not backed off after the scan")
	}
	p := m.peers[added]
	if p == nil {
		t.Fatal("the scan did not add the peer its candidates name")
	}
	if p.state != PeerDiscovered {
		t.Fatalf("premise wrong: the added peer is %v, so the scan dialled it", p.state)
	}
	// The added peer has no path yet, which alone keeps it from being backed
	// off; with a path it is backed off only if the scan read its hint.
	p.state = PeerConnected
	if !m.peerWantsBackoff(p, now) {
		t.Error("a peer the scan added, whose candidates carry a live hint, was not backed off once it had a live path")
	}
}

// TestLocalSaturationHysteresis drives the latch across the band: no tag below
// 0.90, stays latched between 0.90 and 0.75, clears at <= 0.75, and the
// deadline is a flat now+90s (re-stamped, not doubled).
func TestLocalSaturationHysteresis(t *testing.T) {
	b := &ConnectionBudget{MaxTotal: 10}
	rt := &Runtime{connMgr: &ConnectionManager{budget: b}}

	b.currentTotal = 7 // 0.70 — below high-water
	if sat, _ := rt.localSaturation(); sat {
		t.Fatal("0.70 utilization must not latch saturated")
	}
	b.currentTotal = 9 // 0.90 — enter
	sat, until := rt.localSaturation()
	if !sat || until == 0 {
		t.Fatalf("0.90 must latch saturated with a deadline; got sat=%v until=%d", sat, until)
	}
	if d := until - time.Now().Unix(); d < 80 || d > 100 {
		t.Fatalf("backoff deadline should be a flat ~90s out, got %ds", d)
	}
	first := until
	until2 := mustSat(t, rt)
	if until2-first > 5 {
		t.Fatalf("deadline must re-stamp flat (~same), not double: %d -> %d", first, until2)
	}
	b.currentTotal = 8 // 0.80 — hysteresis band, stays latched
	if sat, _ := rt.localSaturation(); !sat {
		t.Fatal("0.80 in the hysteresis band must stay latched saturated")
	}
	b.currentTotal = 7 // 0.70 <= low-water — clear
	if sat, _ := rt.localSaturation(); sat {
		t.Fatal("0.70 <= low-water must clear saturated")
	}
	if sat, _ := (&Runtime{}).localSaturation(); sat {
		t.Fatal("nil connMgr must report not saturated")
	}
}

func mustSat(t *testing.T, rt *Runtime) int64 {
	t.Helper()
	sat, until := rt.localSaturation()
	if !sat {
		t.Fatal("expected still saturated")
	}
	return until
}
