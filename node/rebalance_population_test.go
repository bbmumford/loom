/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"testing"
	"time"
)

// Rebalance decides scale-down and SelectForDrain decides what to drain, and
// they used to test different populations: the gate compared peer.connCount to
// target while SelectForDrain applies its own len(connections) <= targetCount
// test over connectionInfoPerTransport, which enumerates non-dormant entries in
// peer.transports. connCount is written by the accept/close accounting; the
// transports map is what a drain can act on. Both are now read from the
// drainable population.

func rebalancePeer(t *testing.T, connCount int, live ...Protocol) *ConnectionManager {
	t.Helper()
	m := &ConnectionManager{peers: map[string]*peerConn{}, budget: DefaultConnectionBudget()}
	m.rt = &Runtime{}
	m.scaler = NewConnectionScaler(m, nil)
	p := &peerConn{
		nodeID:          "peer-a",
		state:           PeerConnected,
		connCount:       connCount,
		protocol:        ProtoWebSocket,
		lastConnectedAt: time.Now().Add(-time.Hour),
		transports:      map[Protocol]*transportConn{},
	}
	for _, proto := range live {
		p.transports[proto] = &transportConn{
			protocol:    proto,
			grade:       GradeForProtocol(proto),
			connectedAt: time.Now().Add(-time.Hour),
		}
	}
	m.peers["peer-a"] = p
	return m
}

// 🔴 THE CAPABILITY LOSS. connCount at or below target while MORE transports are
// live meant the gate never opened, so genuine excess was never drained and the
// per-peer budget was quietly exceeded. Three live Grade-C transports against a
// budget that wants fewer must produce drain candidates.
func TestExcessLiveTransportsAreDrainedEvenWhenConnCountLooksInBudget(t *testing.T) {
	m := rebalancePeer(t, 1, ProtoWebSocket, ProtoTLS, ProtoGRPC) // connCount lies low
	got := m.scaler.Rebalance()

	if len(got) == 0 {
		t.Error("no drain candidates for a peer holding 3 live transports while connCount " +
			"reported 1 — the scale-down gate read a counter the drain cannot act on, so " +
			"the peer keeps more connections than the budget allows and nothing ever " +
			"reclaims them")
	}
}

// 🔬 THE CONTROL, and the reason the test above is not enough on its own. A gate
// that drained unconditionally would satisfy it while destroying every peer's
// connectivity. A peer inside its budget must produce no candidates.
func TestAPeerWithinItsBudgetIsNotDrained(t *testing.T) {
	m := rebalancePeer(t, 1, ProtoWebSocket)
	if got := m.scaler.Rebalance(); len(got) != 0 {
		t.Errorf("%d drain candidates for a peer with a single transport — the scaler is "+
			"draining peers that are not over budget", len(got))
	}
}

// A peer holding ANY Grade-A transport is skipped ENTIRELY, not merely spared
// that one transport. The guard predates this change and must survive it.
//
// 🔬 ASSERTING ONLY "the Grade-A one was not drained" IS TOO WEAK, and an
// earlier version of this test made that mistake: shouldDrainFirst already
// sorts lower grades first, so with a single unit of excess the Grade-A path is
// last in line and survives whether or not the guard exists. Neutering the
// guard left that assertion green. What the guard uniquely does is spare the
// peer's OTHER transports too, so the discriminating assertion is that a peer
// with a Grade-A path yields no candidates at all.
func TestAPeerWithAnyGradeATransportIsSkippedEntirely(t *testing.T) {
	m := rebalancePeer(t, 5, ProtoNoiseUDP, ProtoWebSocket, ProtoTLS, ProtoGRPC)

	got := m.scaler.Rebalance()

	if len(got) != 0 {
		t.Errorf("%d drain candidates (%v) for a peer holding a Grade-A noise-udp path — "+
			"the scaler preserves such a peer's connections regardless of budget, because "+
			"the fastest path is worth more than the budget it exceeds", len(got), got)
	}
}

// Dormant transports are excluded from the drainable population by
// connectionInfoPerTransport, so they must not make a peer look over budget.
func TestDormantTransportsDoNotTriggerAScaleDown(t *testing.T) {
	m := rebalancePeer(t, 1, ProtoWebSocket)
	m.peers["peer-a"].transports[ProtoTLS] = &transportConn{
		protocol: ProtoTLS, grade: GradeC, connectedAt: time.Now(), isDormant: true,
	}
	m.peers["peer-a"].transports[ProtoGRPC] = &transportConn{
		protocol: ProtoGRPC, grade: GradeB, connectedAt: time.Now(), isDormant: true,
	}

	if got := m.scaler.Rebalance(); len(got) != 0 {
		t.Errorf("%d drain candidates for a peer whose extra transports are all dormant — "+
			"dormant entries are routing flags, not connections the drain can reclaim", len(got))
	}
}
