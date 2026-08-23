/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"testing"

	lad "github.com/bbmumford/ledger"
)

// CHARACTERISATION of resyncStalePeerAddresses — the second file the
// peer.addresses migration crosses (12 of its 53 uses), measured at 0.0%
// coverage before this.
//
// 🛑 IT IS THE FUNCTION bestAddress's OWN MESH-B02 COMMENT NAMES as the one
// that REASSIGNS peer.addresses under m.mu while bestAddress reads it
// lock-free. So it is precisely the lock-discipline the type change disturbs.
//
// 🔴 AND IT DEDUPES USING THE STRUCT AS A MAP KEY:
//
//	seen := make(map[lad.ReachAddress]struct{}, len(peer.addresses))
//
// Struct-keyed maps compare ALL fields. ports.ReachAddress has MORE fields
// than lad.ReachAddress, which carries RawProtocol/Host/Port, so
// migrating this type silently changes what counts as a duplicate: two
// addresses equal today could differ in a new field and both be appended.
// That is a migration hazard no compiler catches, pinned here first.

func resyncManager(t *testing.T, cands map[string][]DialCandidate) *ConnectionManager {
	t.Helper()
	return &ConnectionManager{
		peers: map[string]*peerConn{},
		rt: &Runtime{
			ctx:   context.Background(),
			swarm: &SwarmIntegration{AddressTable: &AddressTable{byNode: cands}},
		},
	}
}

func TestResyncMergesUDPCandidatesIntoStalePeers(t *testing.T) {
	m := resyncManager(t, map[string][]DialCandidate{
		testNodeIDA: {{
			NodeID: testNodeIDA, Transport: "noise-udp",
			Host: "fdaa:0:1234:a7b::2", Port: 41641, Scope: "private",
		}},
	})
	// Connected, but holding only a WS address — the stale condition.
	m.peers[testNodeIDA] = &peerConn{
		nodeID: testNodeIDA, state: PeerConnected,
		addresses: []lad.ReachAddress{
			{Host: "203.0.113.7", Port: 443, Proto: "ws", Scope: "public"},
		},
	}

	m.resyncStalePeerAddresses(context.Background())

	m.mu.Lock()
	got := append([]lad.ReachAddress(nil), m.peers[testNodeIDA].addresses...)
	m.mu.Unlock()

	var hasUDP bool
	for _, a := range got {
		if a.Proto == "udp" {
			hasUDP = true
		}
	}
	if !hasUDP {
		t.Fatalf("stale peer did not gain a UDP candidate: %+v — the noise-UDP "+
			"upgrade path stays blocked for this peer", got)
	}
	if len(got) != 2 {
		t.Fatalf("addresses = %d, want 2 (original WS + merged UDP): %+v", len(got), got)
	}
}

// A peer that already has a UDP entry is NOT stale and must be left alone —
// otherwise every resync tick rewrites live dial state for healthy peers.
func TestResyncLeavesPeersThatAlreadyHaveUDP(t *testing.T) {
	m := resyncManager(t, map[string][]DialCandidate{
		testNodeIDA: {{
			NodeID: testNodeIDA, Transport: "noise-udp",
			Host: "fdaa:0:1234:9ef::9", Port: 41641, Scope: "private",
		}},
	})
	original := []lad.ReachAddress{
		{Host: "fdaa:0:1234:a7b::2", Port: 41641, Proto: "udp", Scope: "private"},
	}
	m.peers[testNodeIDA] = &peerConn{
		nodeID: testNodeIDA, state: PeerConnected,
		addresses: append([]lad.ReachAddress(nil), original...),
	}

	m.resyncStalePeerAddresses(context.Background())

	m.mu.Lock()
	got := m.peers[testNodeIDA].addresses
	m.mu.Unlock()
	if len(got) != len(original) {
		t.Fatalf("a peer that already had UDP was modified: %d -> %d (%+v) — "+
			"the staleness gate is not discriminating", len(original), len(got), got)
	}
}

// 🔴 THE DEDUP CONTRACT, EXERCISED FOR REAL — and getting here took a second
// attempt worth recording.
//
// My first version gave the peer a "udp" address identical to the table's
// candidate. That makes the peer NOT STALE, so the function returns before
// dedup ever runs: the test asserted the staleness gate while claiming to
// assert dedup, and MUTATING THE DEDUP OUT LEFT IT GREEN.
//
// The reachable shape: the peer is stale (no "udp" proto) AND already holds an
// address the table will re-derive. "websocket" maps to "wss", not "udp", so a
// peer holding that wss entry stays stale while the wss addition is a genuine
// duplicate.
func TestResyncDedupesIdenticalAddresses(t *testing.T) {
	const wssHost = "devices.orbtr.io"
	m := resyncManager(t, map[string][]DialCandidate{
		testNodeIDA: {
			{NodeID: testNodeIDA, Transport: "websocket", Host: wssHost, Port: 443, Scope: "public"},
			{NodeID: testNodeIDA, Transport: "noise-udp", Host: "fdaa:0:1234:a7b::2", Port: 41641, Scope: "private"},
		},
	})
	// Stale (no "udp" proto) AND already holding the exact wss address the
	// table will produce.
	m.peers[testNodeIDA] = &peerConn{
		nodeID: testNodeIDA, state: PeerConnected,
		addresses: []lad.ReachAddress{
			{Host: wssHost, Port: 443, Proto: "wss", Scope: "public"},
		},
	}

	m.resyncStalePeerAddresses(context.Background())

	m.mu.Lock()
	got := append([]lad.ReachAddress(nil), m.peers[testNodeIDA].addresses...)
	m.mu.Unlock()

	wssCount := 0
	for _, a := range got {
		if a.Proto == "wss" && a.Host == wssHost {
			wssCount++
		}
	}
	if wssCount != 1 {
		t.Fatalf("the wss address appears %d times, want 1 — dedup did not fire, "+
			"and peer.addresses grows without bound every resync tick: %+v",
			wssCount, got)
	}
	// Control: the genuinely-new udp candidate WAS added, so dedup is
	// suppressing duplicates rather than suppressing everything.
	hasUDP := false
	for _, a := range got {
		if a.Proto == "udp" {
			hasUDP = true
		}
	}
	if !hasUDP {
		t.Fatalf("the new UDP candidate was not merged: %+v — dedup is rejecting "+
			"non-duplicates too", got)
	}
}

// Fail-closed guards: no runtime, no swarm, no table, cancelled context.
func TestResyncFailsClosedWithoutItsDependencies(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("resync panicked instead of returning: %v", r)
		}
	}()

	(&ConnectionManager{rt: nil}).resyncStalePeerAddresses(context.Background())
	(&ConnectionManager{rt: &Runtime{ctx: context.Background()}}).
		resyncStalePeerAddresses(context.Background())

	// 🛑 THE CANCELLED-CONTEXT CASE NEEDS A TABLE THAT WOULD OTHERWISE
	// MERGE, and the first version of this did not have one.
	//
	// It used resyncManager(t, nil) — an EMPTY AddressTable. With no
	// candidates for any node, Get returns nothing and the merge is
	// skipped whatever the context says, so "0 addresses" was satisfied
	// for a reason unrelated to cancellation. MEASURED: deleting the
	// `if ctx.Err() != nil` guard from resyncStalePeerAddresses left
	// this test GREEN, so the guard was pinned by nothing at all.
	//
	// That guard is load-bearing for a specific race: runReachResyncWalker
	// selects over ctx.Done() and the ticker, and when BOTH are ready Go
	// chooses pseudo-randomly — so the tick arm can win on a cancelled
	// context and call this function anyway. The guard is what makes that
	// harmless, and the walker is its only production caller.
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	m := resyncManager(t, map[string][]DialCandidate{
		testNodeIDA: {{
			NodeID: testNodeIDA, Transport: "noise-udp",
			Host: "fdaa:0:1234:a7b::2", Port: 41641, Scope: "private",
		}},
	})
	m.peers[testNodeIDA] = &peerConn{nodeID: testNodeIDA, state: PeerConnected}

	// Premise: this fixture DOES merge on a live context, or the
	// assertion below is vacuous again for a new reason.
	probe := resyncManager(t, map[string][]DialCandidate{
		testNodeIDA: {{
			NodeID: testNodeIDA, Transport: "noise-udp",
			Host: "fdaa:0:1234:a7b::2", Port: 41641, Scope: "private",
		}},
	})
	probe.peers[testNodeIDA] = &peerConn{nodeID: testNodeIDA, state: PeerConnected}
	probe.resyncStalePeerAddresses(context.Background())
	if len(probe.peers[testNodeIDA].addresses) == 0 {
		t.Fatal("premise wrong: this fixture merges nothing even on a LIVE " +
			"context, so the cancelled-context assertion below proves nothing")
	}

	m.resyncStalePeerAddresses(cancelled)
	if n := len(m.peers[testNodeIDA].addresses); n != 0 {
		t.Fatalf("a cancelled context still mutated peer state (%d addresses) — "+
			"the ctx.Err() guard is gone, and the walker's select race can now "+
			"merge addresses into peers after shutdown", n)
	}
}
