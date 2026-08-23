/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"testing"
	"time"

	lad "github.com/bbmumford/ledger"
)

// COVERAGE of the proactive-upgrade walker's GATES: tryProactiveUpgrades
// (:119) and snapshotUpgradeCandidates (:149), both at 0.0%.
//
// CENSUSED FIRST: tryProactiveUpgrades <- peer_connections.go:1241 :1252 (the
// walker tick); snapshotUpgradeCandidates <- :128; probeUpgrade <- :131 (spawned
// as a goroutine, so it is NOT covered here — it dials).
//
// 🔑 THE GATES ARE THE WHOLE POINT. This walker exists to upgrade a peer onto a
// better transport, and every gate exists to stop it doing that too eagerly:
// probing an unstable peer churns the connection it is trying to improve, and
// probing a peer that is already GradeA spends a dial slot for no headroom.
//
// node/upgrade_walker.go is not gofmt-clean, and reformatting it here would mix
// run `gofmt -w` on it — this slice adds a test file only.

// walkerManager returns a manager that OWNS the dial for testNodeIDB
// (dialOwned: selfID < peer.nodeID) so the ownership gate is not what is being
// measured in the tests below.
func walkerManager(t *testing.T) *ConnectionManager {
	t.Helper()
	m := registerTestManager()
	m.selfID = testNodeIDA
	m.peers = map[string]*peerConn{}
	// bestAddress -> peerServiceHostname dereferences m.rt.cache, and
	// NewConnectionManager always sets rt on a live node, so a nil rt here is a
	// fixture gap rather than a production shape. A bare Runtime (cache nil)
	// gives the "no dialable address" outcome these gate tests need.
	m.rt = &Runtime{}
	return m
}

// stablePeer returns a peer that passes every gate except the one under test:
// connected, stable well past upgradeStabilityWindow, low-grade so there is
// upgrade headroom, AND carrying a resolvable public hostname.
//
// 🔑 THE ADDRESS IS WHAT MAKES THE GATE TESTS MEAN ANYTHING, and my first
// version omitted it. Without a resolvable address bestAddress returns "" for
// EVERY peer, so no candidate is ever produced and the upstream gates become
// invisible: mutants that deleted the stability window and the connected-state
// check both SURVIVED. A terminal gate that rejects everything hides every gate
// above it — the same shape as a clamped scaler output.
func stablePeer(now time.Time) *peerConn {
	return &peerConn{
		nodeID:          testNodeIDB,
		state:           PeerConnected,
		lastConnectedAt: now.Add(-10 * time.Minute),
		protocol:        ProtoWebSocket, // GradeC -> real headroom to upgrade
		addresses: []lad.ReachAddress{
			// Tier 1: a public, non-udp hostname resolves for WS/gRPC/noise
			// upgrades without needing a populated DirectoryCache.
			{Proto: "tcp", Host: "peer.example.com", Port: 443, Scope: "public"},
		},
	}
}

// 🔴 A CANCELLED CONTEXT MUST STOP THE TICK BEFORE IT COUNTS. The walker runs on
// a 30s ticker for the life of the node; on shutdown the context is cancelled
// and the tick must be a no-op — including the counter, or shutdown inflates the
// walker telemetry every operator later reads.
func TestACancelledContextStopsTheWalkerBeforeItCountsATick(t *testing.T) {
	m := walkerManager(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	before := m.walkerTicks.Load()
	m.tryProactiveUpgrades(ctx)

	if got := m.walkerTicks.Load(); got != before {
		t.Fatalf("walkerTicks moved %d -> %d on a cancelled context — a shutting-"+
			"down node keeps counting ticks it never performed", before, got)
	}
}

// A live context DOES count the tick, so the test above cannot pass against a
// walker that simply never counts anything.
func TestALiveContextCountsTheTick(t *testing.T) {
	m := walkerManager(t)

	before := m.walkerTicks.Load()
	m.tryProactiveUpgrades(context.Background())

	if got := m.walkerTicks.Load(); got != before+1 {
		t.Fatalf("walkerTicks = %d, want %d — the tick counter never advances, so "+
			"the cancelled-context test above proves nothing", got, before+1)
	}
}

// 🔴 A PEER THAT JUST CONNECTED IS NOT A CANDIDATE. upgradeStabilityWindow (60s)
// exists because probing a fresh connection churns the very session the walker
// is trying to improve — and a just-connected peer is exactly the one most
// likely to still be settling.
func TestAFreshlyConnectedPeerIsNotAnUpgradeCandidate(t *testing.T) {
	now := time.Now()
	m := walkerManager(t)
	p := stablePeer(now)
	p.lastConnectedAt = now.Add(-time.Second) // well inside the stability window
	m.peers[p.nodeID] = p

	if got := m.snapshotUpgradeCandidates(now); len(got) != 0 {
		t.Fatalf("a peer connected 1s ago produced %d candidates — the %v "+
			"stability window is not being applied, so the walker probes "+
			"connections that are still settling", len(got), upgradeStabilityWindow)
	}
}

// A peer that is not Connected is never a candidate: dialing an upgrade for a
// disconnected peer races the reconnect path that owns it.
func TestAPeerThatIsNotConnectedIsNeverAnUpgradeCandidate(t *testing.T) {
	now := time.Now()
	m := walkerManager(t)

	for _, state := range []PeerState{PeerDisconnected, PeerConnecting} {
		p := stablePeer(now)
		p.state = state
		m.peers = map[string]*peerConn{p.nodeID: p}

		if got := m.snapshotUpgradeCandidates(now); len(got) != 0 {
			t.Errorf("a peer in state %v produced %d candidates — the walker is "+
				"racing the reconnect path that owns this peer", state, len(got))
		}
	}
}

// A nil entry in the peers map must be skipped rather than dereferenced. The map
// is written by several paths in peer_connections.go and the walker reads it on
// every tick, so a nil here is a crash on a timer.
func TestANilPeerEntryDoesNotCrashTheWalker(t *testing.T) {
	now := time.Now()
	m := walkerManager(t)
	m.peers["nil-entry"] = nil

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("snapshotUpgradeCandidates panicked on a nil peer entry: %v — "+
				"this runs on a 30s timer, so it is a crash loop", r)
		}
	}()
	if got := m.snapshotUpgradeCandidates(now); len(got) != 0 {
		t.Fatalf("a nil peer produced %d candidates", len(got))
	}
}

// 🔑 THE RATE LIMITER MUST NOT BURN ITS BUDGET ON A NO-OP SNAPSHOT. The code's
// own comment records this as a fixed defect: stamping at the FILTER step meant
// a peer gated out downstream (most commonly bestAddress() == "" for a same-org
// peer whose AddressTable merge had not landed) was rate-limited for the full
// interval even though no probe was ever queued — and by the time the limiter
// released, the address had merged but the walker would not re-check.
//
// The peer here produces NO candidate (no address resolves), so its rate-limit
// slot must remain unstamped and the next tick must re-evaluate it.
func TestAPeerGatedOutDownstreamDoesNotBurnItsRateLimitBudget(t *testing.T) {
	now := time.Now()
	m := walkerManager(t)
	// This test needs a peer that passes every UPSTREAM gate and is rejected at
	// the ADDRESS step — so, deliberately, no addresses.
	p := stablePeer(now)
	p.addresses = nil
	m.peers[p.nodeID] = p

	if got := m.snapshotUpgradeCandidates(now); len(got) != 0 {
		t.Fatalf("premise wrong: a peer with no addresses was expected to produce "+
			"no candidate, got %d", len(got))
	}

	m.walkerProbeMu.Lock()
	_, stamped := m.walkerProbeAt[p.nodeID]
	m.walkerProbeMu.Unlock()

	if stamped {
		t.Fatal("a peer that produced NO candidate had its rate-limit slot " +
			"stamped — it is now locked out for the full grade interval despite " +
			"never being probed, which is the exact defect upgrade_walker.go's " +
			"own comment records as fixed")
	}
}
