/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"testing"
	"time"
)

// snapshotUpgradeCandidates emits a probe for every higher-grade protocol a
// peer is NOT already carrying. Deriving its "already active" set by
// round-tripping the multipath manager's aether protocols through
// unmapProtocol, and that round trip cannot produce ProtoTLS: mapProtocol folds
// ProtoTLS onto aether.ProtoWebSocket when the path is registered, while
// unmapProtocol only yields ProtoTLS from aether.ProtoTCP, which mapProtocol
// never emits. peer.transports is keyed by node protocol and records what the
// peer actually holds, so it is what the walker compares against.
//
// 🔑 THESE REUSE walkerManager/stablePeer FROM upgrade_walker_gates_test.go.
// A hand-rolled peer without a resolvable address makes bestAddress return ""
// for every protocol, so zero candidates are produced and every assertion here
// passes vacuously — including the ones that are supposed to fail.

// activePeer is stablePeer plus a set of live node-layer transports.
func activePeer(now time.Time, live ...Protocol) *peerConn {
	p := stablePeer(now)
	p.transports = map[Protocol]*transportConn{}
	for _, proto := range live {
		p.transports[proto] = &transportConn{protocol: proto, connectedAt: now.Add(-time.Minute)}
	}
	return p
}

func walkerTargets(t *testing.T, live ...Protocol) map[Protocol]bool {
	t.Helper()
	now := time.Now()
	m := walkerManager(t)
	m.peers[testNodeIDB] = activePeer(now, live...)

	got := m.snapshotUpgradeCandidates(now)
	if len(got) == 0 {
		t.Fatal("no upgrade candidates at all — the fixture is failing an upstream gate, " +
			"so every assertion in this file would pass without exercising the active set")
	}
	out := map[Protocol]bool{}
	for _, c := range got {
		out[c.target] = true
	}
	return out
}

// 🔴 A LIVE TLS PATH MUST COUNT AS ACTIVE. Before the set came from
// peer.transports, a peer carrying TLS was probed for TLS again on every walk —
// a lateral same-protocol dial that registerMeshSession can only discard.
func TestALiveTLSPathIsNotProbedAgain(t *testing.T) {
	if walkerTargets(t, ProtoTLS)[ProtoTLS] {
		t.Error("the walker emitted a TLS probe for a peer already carrying a live TLS " +
			"path — ProtoTLS cannot appear in an aether-derived active set, so this " +
			"repeats on every walk for the life of the connection")
	}
}

// 🔬 THE CONTROL. An active set that swallowed everything would satisfy the
// test above while disabling the walker, so a protocol the peer LACKS must
// still be probed.
func TestAProtocolThePeerLacksIsStillProbed(t *testing.T) {
	got := walkerTargets(t, ProtoTLS)
	if !got[ProtoNoiseUDP] && !got[ProtoQUIC] && !got[ProtoGRPC] {
		t.Error("no probe was emitted for any protocol the peer lacks — the walker has " +
			"stopped proposing upgrades, so a peer stuck on a low-grade transport never " +
			"improves")
	}
}

// Every live transport is excluded, not just TLS: the change is about where the
// set comes from, so a multi-transport peer is what exercises it.
func TestEveryLiveTransportIsExcludedFromTheProbeSet(t *testing.T) {
	got := walkerTargets(t, ProtoTLS, ProtoWebSocket)
	for _, proto := range []Protocol{ProtoTLS, ProtoWebSocket} {
		if got[proto] {
			t.Errorf("%s was probed although the peer already carries it", proto)
		}
	}
}
