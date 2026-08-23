/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"testing"
	"time"
)

// closeDrainedConnection resolves the transport to cancel from
// peer.transports[proto], a map keyed by protocol alone. A drain runs for a
// grace window before its callback fires, and a reconnect inside that window
// installs a replacement under the same key — so the callback can reach a
// transport the scaler never selected. connectedAt is the only field that
// distinguishes the two instances.

func drainRaceFixture(t *testing.T, connectedAt time.Time) (*ConnectionManager, *transportConn, *bool) {
	t.Helper()
	cancelled := false
	tc := &transportConn{
		protocol:    ProtoQUIC,
		connectedAt: connectedAt,
		cancelFunc:  func() { cancelled = true },
	}
	m := &ConnectionManager{
		peers:  map[string]*peerConn{},
		budget: DefaultConnectionBudget(),
	}
	m.peers["peer-a"] = &peerConn{
		nodeID:     "peer-a",
		state:      PeerConnected,
		protocol:   ProtoQUIC,
		transports: map[Protocol]*transportConn{ProtoQUIC: tc},
	}
	return m, tc, &cancelled
}

// 🔴 THE RACE. The transport in the map was established AFTER the drain began,
// so it is a replacement and must survive. Cancelling it tears down a healthy
// connection and books it as a voluntary scale-down.
func TestADrainDoesNotCancelATransportThatReplacedItsTarget(t *testing.T) {
	drainStarted := time.Now()
	m, tc, cancelled := drainRaceFixture(t, drainStarted.Add(time.Second))

	m.closeDrainedConnection("peer-a", "quic", drainStarted)

	if *cancelled {
		t.Error("the replacement transport was cancelled — it was established after the " +
			"drain began, so the scaler never selected it, and the peer loses a healthy " +
			"connection that is then accounted as a voluntary scale-down")
	}
	if tc.draining {
		t.Error("the replacement was marked draining, so its eventual teardown will skip " +
			"the failure counters that should record it")
	}
	if _, stamped := m.peers["peer-a"].drainedAt[ProtoQUIC]; stamped {
		t.Error("a re-dial suppression window was written for a transport that was never " +
			"drained, so scanAndConnect will not re-dial this protocol for 5 minutes")
	}
}

// 🔬 THE OTHER HALF, AND THE REASON THE TEST ABOVE IS NOT ENOUGH ON ITS OWN. A
// guard that refused every cancel would satisfy it while disabling drain
// entirely. The transport that WAS the drain's target must still be closed.
func TestADrainStillCancelsTheTransportItWasStartedFor(t *testing.T) {
	drainStarted := time.Now()
	m, tc, cancelled := drainRaceFixture(t, drainStarted.Add(-time.Second))

	m.closeDrainedConnection("peer-a", "quic", drainStarted)

	if !*cancelled {
		t.Error("the drain's own transport was not cancelled — scale-down no longer " +
			"closes anything, so the connection budget is never reclaimed")
	}
	if !tc.draining {
		t.Error("the drained transport was not marked draining, so its cleanup counts a " +
			"voluntary scale-down as a chronic path failure")
	}
	if _, stamped := m.peers["peer-a"].drainedAt[ProtoQUIC]; !stamped {
		t.Error("no re-dial suppression was recorded, so scanAndConnect immediately " +
			"redials the connection the scaler just drained")
	}
}

// A drain for a peer that has since been evicted must be a no-op, not a panic:
// the grace window easily outlives a peer eviction.
func TestADrainForAnEvictedPeerIsHarmless(t *testing.T) {
	m := &ConnectionManager{peers: map[string]*peerConn{}, budget: DefaultConnectionBudget()}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("closing a drain for an unknown peer panicked: %v", r)
		}
	}()

	m.closeDrainedConnection("gone", "quic", time.Now())
}
