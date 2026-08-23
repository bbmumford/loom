/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/ORBTR/aether/rpc/pb"
)

// — WIRING THE ONLY WRITER OF PER-PEER RPC TRAFFIC.
//
// ConnectionManager.RecordRPC had ZERO non-test callers across loom, ORBTR and
// HSTLES while its own doc claimed "Called by RPC dispatch" (the coding contract). Measured
// with positive controls on the identical command shape: .Rebalance( = 1,
// .SetPriority( = 1, .RecordPathFailure( = 2, against .RecordRPC( = 0.
//
// Two subsystems were starved by that one missing call, and BOTH degraded
// silently to a constant rather than failing:
//
//	updatePriorities ladder      three of five arms unreachable -> every
//	                             non-anchor peer stuck at PriorityIdle
//	scaler TrafficWeight         1 + ln(1+0/10) = 1.0 exactly, forever
//
// These tests prove the wiring DELIVERS — walking from the call site to each
// consumer's observable output, not merely asserting the field moved. A
// counter that increments and reaches no consumer is the same defect one layer
// down ("a value SET is not a value DELIVERED").

func trafficFixture(t *testing.T, peerIDs ...string) *ConnectionManager {
	t.Helper()
	m := &ConnectionManager{
		peers:  map[string]*peerConn{},
		budget: DefaultConnectionBudget(),
	}
	m.scaler = NewConnectionScaler(m, nil)
	for _, id := range peerIDs {
		m.peers[id] = connectedPeer(id, 20*time.Millisecond)
	}
	return m
}

// 🔴🔴 THE TEST THAT ACTUALLY COVERS THE FIX, AND IT EXISTS BECAUSE A MUTANT
// SURVIVED. Every other test in this file calls m.RecordRPC directly.
// Deleting the new call site in callOverMeshSession therefore changed NOTHING
// and the whole file stayed green — proving the fan-out works while proving
// nothing about the wiring, which is the entire fix.
//
// That is the same defect this lane has filed all session (a mechanism verified
// in isolation and assumed to be driven), committed here while fixing an
// instance of it. The only cure is a test that enters through the production
// path.
//
// The call is expected to FAIL: probeSession.OpenStream returns an error and no
// BidiRPC is registered. That is deliberate — it also pins the documented
// choice to record OFFERED load before dispatch, so a peer whose calls are
// failing still counts as carrying traffic. Crediting only successes would
// shrink the connection target of exactly the peer that is struggling.
func TestCallOverMeshSessionCreditsThePeerEvenWhenTheCallFails(t *testing.T) {
	m := trafficFixture(t, "busy")
	rt := &Runtime{connMgr: m, identity: &NodeIdentity{NodeID: "self"}}
	m.rt = rt
	f := &runtimeForwarder{rt: rt}
	sess := &probeSession{local: "self", remote: "busy"}

	_, err := f.callOverMeshSession(context.Background(), sess, &pb.RPCRequest{
		Id: "req-1", Handler: "orbtr.ai.auth.Login",
	})
	if err == nil {
		t.Fatal("fixture wrong: the stub session cannot complete an RPC, so this call " +
			"was expected to fail — a nil error means the test is exercising some other path")
	}

	m.mu.Lock()
	count := m.peers["busy"].rpcsLastMinute
	m.mu.Unlock()
	if count != 1 {
		t.Errorf("rpcsLastMinute = %d after one forwarded RPC, want 1 — "+
			"callOverMeshSession is not recording peer traffic, so both the priority "+
			"ladder and the scaler's TrafficWeight are back to the constants they were "+
			"pinned to before this", count)
	}

	m.scaler.mu.Lock()
	_, counted := m.scaler.rpcCounters["busy"]
	m.scaler.mu.Unlock()
	if !counted {
		t.Error("the scaler was not credited through the production call path")
	}
}

// 🔴 CONSUMER 1: THE PRIORITY LADDER. Before the wiring every non-anchor peer
// landed on PriorityIdle regardless of traffic. This walks RecordRPC ->
// peer.rpcsLastMinute -> updatePriorities -> budget, and asserts the ladder now
// separates a busy peer from an idle one.
//
// 🔬 ANTI-CORRELATED BY CONSTRUCTION: two peers on the same manager, one with
// traffic and one without. A ladder still pinned to Idle gives them the SAME
// priority, which is exactly the pre-wiring behaviour — a single-peer fixture
// could not tell the two apart.
func TestRecordedTrafficReachesThePriorityLadder(t *testing.T) {
	m := trafficFixture(t, "busy", "quiet")
	m.rt = &Runtime{liveDir: &countingDir{}} // no anchors, so the ladder decides

	for i := 0; i < 11; i++ { // clears the >10 High threshold
		m.RecordRPC("busy")
	}

	m.updatePriorities()

	busy := m.budget.GetPriority("busy")
	quiet := m.budget.GetPriority("quiet")

	if busy == quiet {
		t.Fatalf("busy and quiet peers both landed on %v — the ladder cannot distinguish "+
			"them, which is the pre-wiring behaviour where rpcsLastMinute was always 0 "+
			"and every non-anchor peer sat at PriorityIdle", busy)
	}
	if busy != PriorityHigh {
		t.Errorf("busy peer priority = %v, want %v after 11 recorded RPCs", busy, PriorityHigh)
	}
	if quiet != PriorityIdle {
		t.Errorf("quiet peer priority = %v, want %v", quiet, PriorityIdle)
	}
}

// The middle rungs must be reachable too — proving the wiring restored the
// whole ladder rather than only its top. Before the fix all three of these
// were unreachable in production.
func TestTheMiddleRungsAreReachableOnceTrafficIsRecorded(t *testing.T) {
	for _, tc := range []struct {
		name  string
		calls int
		want  ConnectionPriority
	}{
		{"one RPC reaches Normal", 1, PriorityNormal},
		{"ten RPCs is still Normal", 10, PriorityNormal},
		{"eleven RPCs reaches High", 11, PriorityHigh},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := trafficFixture(t, "peer-a")
			m.rt = &Runtime{liveDir: &countingDir{}}
			for i := 0; i < tc.calls; i++ {
				m.RecordRPC("peer-a")
			}

			m.updatePriorities()

			if got := m.budget.GetPriority("peer-a"); got != tc.want {
				t.Errorf("after %d RPCs priority = %v, want %v", tc.calls, got, tc.want)
			}
		})
	}
}

// 🔴 CONSUMER 2: THE SCALER'S TrafficWeight. The second, independent consumer.
// A wiring that fed only the priority ladder would leave the scaling formula's
// fourth factor an identity multiply — so this asserts the OTHER counter moved.
//
// The assertion is on trafficWeight's own value rather than on the final
// connection target, because the target is clamped by MaxPerPeer (=2 by
// default) and the clamp masks the factor for same-region peers. Asserting the
// target alone would pass whether or not the weight was restored.
func TestRecordedTrafficReachesTheScalerTrafficWeight(t *testing.T) {
	m := trafficFixture(t, "busy")

	for i := 0; i < 20; i++ {
		m.RecordRPC("busy")
	}

	m.scaler.mu.Lock()
	rc, ok := m.scaler.rpcCounters["busy"]
	m.scaler.mu.Unlock()
	if !ok {
		t.Fatal("the scaler has no rpcCounter for a peer that just served 20 RPCs — " +
			"rpcCounters is still the permanently-empty map it was before the wiring, " +
			"so TrafficWeight stays 1.0 and the fourth factor of the sizing formula " +
			"remains inert")
	}

	perMin := float64(rc.rpcsLastMinute())
	if perMin <= 0 {
		t.Fatalf("rpcsLastMinute = %v after 20 recorded RPCs, want > 0", perMin)
	}
	if want := 1 + math.Log(1+perMin/10); want <= 1.0 {
		t.Fatalf("fixture wrong: %v RPCs/min still yields trafficWeight %v — pick a "+
			"larger count so the factor is distinguishable from the inert 1.0", perMin, want)
	}
}

// An RPC to a peer the manager does not track must not fabricate state on
// either consumer. Crediting an unknown peer would create scaler counters for
// nodes that have no connection to size.
func TestTrafficForAnUntrackedPeerCreditsNeitherConsumer(t *testing.T) {
	m := trafficFixture(t, "known")

	m.RecordRPC("stranger")

	if _, ok := m.peers["stranger"]; ok {
		t.Error("recording an RPC manufactured a peer entry")
	}
	m.scaler.mu.Lock()
	_, counted := m.scaler.rpcCounters["stranger"]
	m.scaler.mu.Unlock()
	if counted {
		t.Error("the scaler now holds an rpcCounter for a peer the manager does not " +
			"track — the sizing formula has an entry for a connection that does not exist")
	}
}

// resetRPCCounters gives the ladder its sliding window. It must clear the
// per-minute count WITHOUT clearing lastRPCAt, or a peer that went quiet drops
// straight from High to Idle instead of resting on Low for five minutes.
//
// 🔬 THE lastRPCAt HALF IS THE DISCRIMINATING ONE. Asserting only that the
// count reset would pass even if the reset wiped both fields — and wiping both
// removes the Low rung from the ladder again, which is precisely the rung the
// wiring just restored.
func TestTheSlidingWindowResetKeepsTheLastRPCTimestamp(t *testing.T) {
	m := trafficFixture(t, "peer-a")
	m.rt = &Runtime{liveDir: &countingDir{}}
	for i := 0; i < 11; i++ {
		m.RecordRPC("peer-a")
	}

	m.resetRPCCounters()

	m.mu.Lock()
	count, last := m.peers["peer-a"].rpcsLastMinute, m.peers["peer-a"].lastRPCAt
	m.mu.Unlock()

	if count != 0 {
		t.Errorf("rpcsLastMinute = %d after the window reset, want 0", count)
	}
	if last.IsZero() {
		t.Fatal("the window reset also cleared lastRPCAt — a peer that goes quiet now " +
			"falls straight to PriorityIdle instead of resting on PriorityLow, which " +
			"removes the very rung this wiring restored")
	}

	m.updatePriorities()

	if got := m.budget.GetPriority("peer-a"); got != PriorityLow {
		t.Errorf("a recently-busy peer settled at %v after the window reset, want %v",
			got, PriorityLow)
	}
}
