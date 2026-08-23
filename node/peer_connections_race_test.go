/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"sync"
	"testing"
)

// Regression test for a confirmed data race.
//
// Before the fix, `go test -race` reported two races inside
// isSameRegionSameOrigin — peer.crossOrigin (peer_connections.go:2233) and
// peer.peerRegion (:2236) — reached through pickPath, against writes that
// scanAndConnect/SeedBootstrapPeer/reach_resync perform under m.mu.
//
// ⚠ THIS TEST IS ONLY MEANINGFUL UNDER `-race`. Without it the assertions still
// pass and prove nothing: a data race has no deterministic observable, so the
// detector IS the oracle here. That is stated rather than assumed because a
// green run of this file without -race is not evidence of anything.
//
// The shape mirrors production exactly: scanAndConnect writes these fields
// under m.mu while dialing peers in parallel goroutines, and connectPeer →
// pickPath runs with NO lock held (both connectPeer call sites Unlock first).
func TestPickPathReadsPeerFieldsUnderTheLockThatGuardsTheirWriters(t *testing.T) {
	m := &ConnectionManager{selfRegion: "syd"}
	peer := &peerConn{nodeID: "peer-1", peerRegion: "syd"}

	const iterations = 500
	var wg sync.WaitGroup
	wg.Add(2)

	// WRITER — the scanAndConnect/reach_resync pattern: both fields written
	// while holding m.mu.
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			m.mu.Lock()
			peer.crossOrigin = i%2 == 0
			if i%3 == 0 {
				peer.peerRegion = "syd"
			} else {
				peer.peerRegion = "fra"
			}
			m.mu.Unlock()
		}
	}()

	// READER — the connectPeer pattern: pickPath called with no lock held.
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			got := m.pickPath(peer)
			if len(got) == 0 {
				// Not a race assertion — a sanity floor. An empty protocol set
				// would mean the dial loop has nothing to try and the peer is
				// silently unreachable.
				t.Errorf("pickPath returned no protocols")
				return
			}
		}
	}()

	wg.Wait()
}

// The same predicate is read from AcceptMeshConnection with m.mu ALREADY HELD,
// so it must use the *Locked variant: sync.Mutex is not reentrant and the
// locking wrapper would self-deadlock the accept path.
//
// This test would hang rather than fail if the wrong variant were used, so it
// is the deadlock canary for that call site — a timeout is the signal.
func TestTheLockedVariantIsCallableWithTheMutexHeld(t *testing.T) {
	m := &ConnectionManager{selfRegion: "syd"}
	peer := &peerConn{nodeID: "peer-1", peerRegion: "syd"}

	m.mu.Lock()
	got := m.isSameRegionSameOriginLocked(peer)
	m.mu.Unlock()

	if !got {
		t.Error("same region, same origin reported as false")
	}
}

// The two variants must agree — they are one predicate expressed twice, and
// nothing in the code forces the wrapper to keep delegating to the core.
// Same shape as a threshold agreement or an append/remove inverse pair.
func TestBothVariantsAgreeOnEveryCombination(t *testing.T) {
	for _, tc := range []struct {
		name                    string
		selfRegion, peerRegion  string
		crossOrigin, wantSameRO bool
	}{
		{"same region, same origin", "syd", "syd", false, true},
		{"same region, CROSS origin", "syd", "syd", true, false},
		{"different region", "syd", "fra", false, false},
		{"peer region unknown", "syd", "", false, false},
		{"self region unknown", "", "syd", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := &ConnectionManager{selfRegion: tc.selfRegion}
			peer := &peerConn{nodeID: "p", peerRegion: tc.peerRegion, crossOrigin: tc.crossOrigin}

			unlocked := m.isSameRegionSameOrigin(peer)
			m.mu.Lock()
			locked := m.isSameRegionSameOriginLocked(peer)
			m.mu.Unlock()

			if unlocked != locked {
				t.Errorf("isSameRegionSameOrigin=%v but isSameRegionSameOriginLocked=%v — the "+
					"wrapper no longer delegates to the core, so two call sites now disagree "+
					"about the same peer", unlocked, locked)
			}
			if unlocked != tc.wantSameRO {
				t.Errorf("got %v, want %v", unlocked, tc.wantSameRO)
			}

			// And the decision must reach pickPath: same-region same-origin
			// pairs get ONLY noise-UDP, per the hard rule.
			path := m.pickPath(peer)
			onlyUDP := len(path) == 1 && path[0] == ProtoNoiseUDP
			if onlyUDP != tc.wantSameRO {
				t.Errorf("pickPath returned %v (onlyUDP=%v) but sameRegionSameOrigin=%v — a "+
					"same-region same-origin pair falling to the full cascade is the WS/edge-"+
					"reaping regression the rule exists to prevent", path, onlyUDP, tc.wantSameRO)
			}
		})
	}
}

// A nil peer must not panic and must not be treated as same-region: the dial
// loop would collapse to noise-UDP only, with no fallback, for a peer it knows
// nothing about.
func TestANilPeerIsNotSameRegionSameOrigin(t *testing.T) {
	m := &ConnectionManager{selfRegion: "syd"}

	if m.isSameRegionSameOrigin(nil) {
		t.Error("a nil peer was reported same-region same-origin")
	}
	if got := m.pickPath(nil); len(got) <= 1 {
		t.Errorf("pickPath(nil) = %v — a nil peer must get the full cascade, not the "+
			"noise-UDP-only hard rule", got)
	}
}
