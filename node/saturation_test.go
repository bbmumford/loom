/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
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
// better-grade address, saturated with the given deadline. crossOrigin varies
// its address origin so a case can show the backoff decision does not read it.
func livePeer(crossOrigin bool, until int64) *peerConn {
	return &peerConn{
		state:            PeerConnected,
		crossOrigin:      crossOrigin,
		peerSaturated:    true,
		peerBackoffUntil: until,
		transports:       map[Protocol]*transportConn{ProtoWebSocket: {protocol: ProtoWebSocket, grade: GradeC}},
	}
}

// TestPeerWantsBackoff holds the advisory rules: a backoff_until that is
// missing, zero, malformed (parsed to 0), negative, expired or further ahead
// than the publisher's TTL plus the admitted skew is no hint; a live hint
// delays extra paths to a peer that already has a live one, whatever the
// peer's address origin or grade; and a peer with no live path is never
// backed off, because the dial is its only path.
func TestPeerWantsBackoff(t *testing.T) {
	m := &ConnectionManager{}
	now := time.Unix(1_000_000, 0)
	fresh := now.Add(saturationBackoffTTL / 2).Unix()
	beyond := now.Add(saturationBackoffTTL + saturationBackoffSkew + time.Minute).Unix()

	for _, tc := range []struct {
		name string
		p    *peerConn
		want bool
	}{
		{"nil peer", nil, false},
		{"unsaturated peer", &peerConn{}, false},
		{"a fresh deadline on a peer with a live path", livePeer(true, fresh), true},
		{"a same-origin grade C peer is not exempt", livePeer(false, fresh), true},
		{"a missing or malformed deadline is no hint", livePeer(true, 0), false},
		{"a negative deadline is no hint", livePeer(true, -5), false},
		{"an expired deadline is no hint", livePeer(true, now.Unix()-1), false},
		{"a deadline past the TTL and skew is no hint", livePeer(true, beyond), false},
		{"a peer with no live path is never backed off",
			&peerConn{peerSaturated: true, crossOrigin: true, peerBackoffUntil: fresh}, false},
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
		"live":    livePeer(true, now.Add(saturationBackoffTTL/2).Unix()),
		"zero":    livePeer(true, 0),
		"expired": livePeer(true, now.Add(-time.Minute).Unix()),
		"future":  livePeer(true, now.Add(time.Hour).Unix()),
	}}
	if got := m.countPeersAdvertisingBackoff(); got != 1 {
		t.Fatalf("countPeersAdvertisingBackoff = %d, want 1 (only the live deadline)", got)
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
