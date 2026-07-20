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

// TestPeerWantsBackoff covers the load-bearing safety property: a same-org peer
// with no strictly-better path is NEVER backed off, so its only viable Tier-0
// 6PN route can't be starved by the saturation bit. A bare peerConn has no
// addresses/transports, so peerHasBetterGradeAddress is false and
// bestActiveGrade is below A — exactly the "only viable path" shape.
func TestPeerWantsBackoff(t *testing.T) {
	m := &ConnectionManager{}
	now := time.Unix(1000, 0)

	if m.peerWantsBackoff(nil, now) {
		t.Fatal("nil peer must not want backoff")
	}
	if m.peerWantsBackoff(&peerConn{}, now) {
		t.Fatal("unsaturated peer must not want backoff")
	}
	if !m.peerWantsBackoff(&peerConn{peerSaturated: true, crossOrigin: true, peerBackoffUntil: 2000}, now) {
		t.Fatal("cross-org saturated peer with a fresh TTL should want backoff")
	}
	if m.peerWantsBackoff(&peerConn{peerSaturated: true, crossOrigin: true, peerBackoffUntil: 500}, now) {
		t.Fatal("an elapsed backoff TTL must be treated as cleared")
	}
	if m.peerWantsBackoff(&peerConn{peerSaturated: true, crossOrigin: false, peerBackoffUntil: 2000}, now) {
		t.Fatal("same-org peer with no better path must NOT be backed off (6PN starvation guard)")
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
