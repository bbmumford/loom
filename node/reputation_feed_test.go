/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"testing"
	"time"

	"github.com/bbmumford/loom/ports"
)

// COVERAGE of feedReputationFromRTT (peer_connections.go:4291) and
// updatePriorities (:4180), both at 0.0%. Both run every scan-loop tick
// (:1240, :1247), so unlike the takeover engine and the circuit breaker these
// two DO execute in a deployment.
//
// 🔑 WHY feedReputationFromRTT IS WORTH A TEST OF ITS OWN. It is one of the
// few places in this lane's territory where "absent is not a measurement" is
// already decided CORRECTLY: a connected peer whose lastRTT is still 0 is
// skipped rather than injected. Injecting the zero would set AvgRTT = 0, and
// ComputeScore's latency component reads lower RTT as better — so a peer that
// has never been measured would score as the fastest peer in the fleet and win
// selection over every peer with a real measurement.
//
// This lane has now filed that same shape five times as a DEFECT (rung
// ordering, empty signatures, the path-score RTT, resolveChainPosition, the
// dial ranking). Here it is done right, untested, one edit from being undone.
// Pinning a correct decision is worth as much as finding a wrong one.

// repFixture wires a manager with a reputation tracker holding a score entry
// for each named peer — Inject* returns early for unknown nodes, so without
// the entry every assertion below would pass vacuously.
func repFixture(t *testing.T, nodeIDs ...string) (*ConnectionManager, *ReputationTracker) {
	t.Helper()
	tr := NewReputationTracker(nil)
	for _, id := range nodeIDs {
		tr.scores[id] = &PeerReputation{NodeID: id}
	}
	return &ConnectionManager{peers: map[string]*peerConn{}, reputationTracker: tr}, tr
}

func connectedPeer(id string, rtt time.Duration) *peerConn {
	return &peerConn{
		nodeID:        id,
		state:         PeerConnected,
		protocol:      ProtoQUIC,
		lastRTT:       rtt,
		lastConnected: time.Now().Add(-time.Hour),
	}
}

// 🔴 THE PROPERTY THAT MATTERS. An unmeasured RTT must not be injected as
// zero. The fixture is anti-correlated on purpose: one peer HAS a measurement
// and one has not, so a blanket inject and a correct skip produce different
// results. With only the unmeasured peer, "AvgRTT == 0" is indistinguishable
// from "nothing was injected".
func TestAnUnmeasuredRTTIsNotInjectedAsZero(t *testing.T) {
	m, tr := repFixture(t, "measured", "unmeasured")
	m.peers["measured"] = connectedPeer("measured", 42*time.Millisecond)
	m.peers["unmeasured"] = connectedPeer("unmeasured", 0) // never measured

	m.feedReputationFromRTT()

	if got := tr.scores["measured"].AvgRTT; got != 42*time.Millisecond {
		t.Errorf("measured peer AvgRTT = %v, want 42ms — the real measurement did not "+
			"reach the tracker, so this test cannot tell a skip from a no-op", got)
	}
	if got := tr.scores["unmeasured"].AvgRTT; got != 0 {
		t.Errorf("unmeasured peer AvgRTT = %v, want it left alone", got)
	}
	// The discriminating check: a zero injected as a MEASUREMENT would have
	// computed a score, because InjectRTT calls ComputeScore.
	if got := tr.scores["unmeasured"].Score; got != 0 {
		t.Errorf("the unmeasured peer has a computed Score of %v — a zero RTT was "+
			"injected as though it were a measurement, and ComputeScore reads lower "+
			"RTT as better, so a peer nobody has ever measured now outranks every peer "+
			"with a real one", got)
	}
}

// A peer that is not connected has no current network conditions to report.
// Feeding a stale RTT from a dropped peer would keep a dead peer's reputation
// warm and preferred.
func TestADisconnectedPeerIsNotFedIntoReputation(t *testing.T) {
	m, tr := repFixture(t, "gone")
	p := connectedPeer("gone", 10*time.Millisecond)
	p.state = PeerDisconnected
	m.peers["gone"] = p

	m.feedReputationFromRTT()

	if got := tr.scores["gone"].AvgRTT; got != 0 {
		t.Errorf("a disconnected peer's RTT (%v) was fed into reputation — a dropped "+
			"peer keeps a warm score and stays preferred by selection", got)
	}
}

// The grade must arrive normalised to 0-1 against GradeA, which is what
// ComputeScore's grade component expects. An un-normalised grade would blow
// past the component's 0-1 range and dominate the composite score.
func TestTheInjectedGradeIsNormalisedAgainstGradeA(t *testing.T) {
	m, tr := repFixture(t, "peer-a")
	m.peers["peer-a"] = connectedPeer("peer-a", 5*time.Millisecond)

	m.feedReputationFromRTT()

	got := tr.scores["peer-a"].EffectiveGrade
	if got < 0 || got > 1 {
		t.Errorf("EffectiveGrade = %v, want it normalised into [0,1] — ComputeScore "+
			"treats this component as 0-1, so an out-of-range value dominates the "+
			"composite and makes the other three components irrelevant", got)
	}
	if want := float64(GradeForProtocol(ProtoQUIC)) / float64(GradeA); got != want {
		t.Errorf("EffectiveGrade = %v, want %v (the peer's own best active grade over "+
			"GradeA)", got, want)
	}
}

// A manager with no reputation tracker must be a no-op, not a panic — the
// tracker is constructed alongside the scaler and a manager built without one
// still runs the scan loop that calls this.
func TestFeedReputationToleratesAnAbsentTracker(t *testing.T) {
	m := &ConnectionManager{peers: map[string]*peerConn{
		"peer-a": connectedPeer("peer-a", time.Millisecond),
	}}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("feedReputationFromRTT panicked with no tracker: %v — the scan loop "+
				"calls this every tick, so the whole loop dies", r)
		}
	}()

	m.feedReputationFromRTT()
}

// ─── updatePriorities ────────────────────────────────────────────────────

// The full priority ladder in one table. Each rung decides whether a peer is
// eligible for drain when the connection budget is under pressure, so a peer
// landing one rung too low is dropped while a busier one survives.
//
// 🔴🔴 READ THIS BEFORE TRUSTING THE COVERAGE NUMBER ON updatePriorities.
// THREE OF THE FIVE ARMS BELOW ARE UNREACHABLE IN PRODUCTION. This test sets
// peer.rpcsLastMinute and peer.lastRPCAt directly, so every arm is exercised
// here — but MEASURED (, positive controls .Rebalance(=1, .SetPriority(=1,
// .RecordPathFailure(=2 against a result of 0):
//
//	peer.rpcsLastMinute / peer.lastRPCAt are written at exactly ONE non-test
//	site, peer_connections.go:4321-4322, inside ConnectionManager.RecordRPC —
//	and RecordRPC has ZERO non-test callers in loom, ORBTR or HSTLES, despite
//	its doc at :4313 claiming "Called by RPC dispatch when traffic flows
//	through a peer connection" (the coding contract: a declared operation is not evidence
//	of a writer).
//
// So in a deployment both fields are permanently zero, and the ladder collapses:
//
//	anchor                        -> PriorityCritical   REACHABLE
//	rpcsLastMinute > 10           -> PriorityHigh       UNREACHABLE
//	rpcsLastMinute > 0            -> PriorityNormal     UNREACHABLE
//	==0 && !lastRPCAt.IsZero()    -> PriorityLow        UNREACHABLE (always zero)
//	default                       -> PriorityIdle       REACHABLE
//
// Every non-anchor peer is PriorityIdle forever, so drain selection treats a
// peer carrying all the traffic exactly like an idle one — which is the whole
// thing the ladder exists to prevent.
//
// ⚠ THIS TEST IS STILL CORRECT AND STILL WORTH KEEPING: it pins the ladder's
// logic so that wiring RecordRPC is a one-line change with the semantics already
// nailed down. But a green test here plus "updatePriorities 91.7% covered" reads
// as "peer prioritisation works", and it does not. Per the coding contract the honest
// statement is REGISTERED, not REACHABLE — and the fix (call RecordRPC from RPC
// dispatch) is a wiring decision for @R/DESIGN, not this lane's to make.
func TestThePriorityLadderPlacesEachPeerOnItsOwnRung(t *testing.T) {
	for _, tc := range []struct {
		name     string
		rpcs     int
		lastRPC  time.Time
		wantPrio ConnectionPriority
	}{
		{"busy peer", 11, time.Now(), PriorityHigh},
		{"exactly at the busy threshold is NOT high", 10, time.Now(), PriorityNormal},
		{"lightly used", 1, time.Now(), PriorityNormal},
		{"idle but recently used", 0, time.Now().Add(-time.Minute), PriorityLow},
		{"idle and long unused", 0, time.Now().Add(-10 * time.Minute), PriorityIdle},
		{"never used at all", 0, time.Time{}, PriorityIdle},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := &ConnectionManager{peers: map[string]*peerConn{}, budget: DefaultConnectionBudget()}
			p := connectedPeer("peer-a", time.Millisecond)
			p.rpcsLastMinute, p.lastRPCAt = tc.rpcs, tc.lastRPC
			m.peers["peer-a"] = p

			m.updatePriorities()

			if got := m.peers["peer-a"].priority; got != tc.wantPrio {
				t.Errorf("priority = %v, want %v — a peer on the wrong rung is drained "+
					"ahead of a less useful one when the budget tightens", got, tc.wantPrio)
			}
		})
	}
}

// 🔴 THE PRIORITY MUST REACH THE BUDGET, not just the peer struct. The budget
// is what external drain-selection queries read (connection_budget.go:128), so
// a priority computed correctly and never published is a priority that does
// not exist — the "a value SET is not a value DELIVERED" shape.
func TestTheComputedPriorityIsPublishedToTheConnectionBudget(t *testing.T) {
	m := &ConnectionManager{peers: map[string]*peerConn{}, budget: DefaultConnectionBudget()}
	p := connectedPeer("peer-a", time.Millisecond)
	p.rpcsLastMinute, p.lastRPCAt = 50, time.Now()
	m.peers["peer-a"] = p

	m.updatePriorities()

	if got := m.budget.GetPriority("peer-a"); got != PriorityHigh {
		t.Errorf("budget priority = %v, want %v — updatePriorities computed a priority "+
			"and never published it, so every external drain query sees the default",
			got, PriorityHigh)
	}
}

// countingDir counts Member lookups and answers "not found". The embedded
// interface is nil and never reached — only the override is used — so no real
// directory has to be constructed to observe the call count.
type countingDir struct {
	ports.LiveDirectory
	calls int
}

func (d *countingDir) Member(context.Context, ports.Tenant, ports.NodeID) (ports.Member, bool, error) {
	d.calls++
	return ports.Member{}, false, nil
}

// 🔬 THIS TEST EXISTS TO SEPARATE TWO GUARDS THAT HIDE EACH OTHER.
// updatePriorities filters disconnected peers TWICE — once when snapshotting
// (:4191) and again when applying (:4213). Mutation showed the pair is
// mutually redundant: removing EITHER alone is invisible, and only removing
// BOTH turns a test red. So "a disconnected peer gets no priority" cannot tell
// the two apart, and neither guard is individually covered.
//
// The snapshot guard has one effect the apply guard cannot have: it runs
// BEFORE the anchor lookup, so it suppresses a directory read per disconnected
// peer. That read is deliberately done with m.mu released (see the comment at
// :4182 about cache.Roles acquiring its own lock), and it is the expensive part
// of the pass. Counting the lookups is the only observation that distinguishes
// them.
//
// Fourth time this session that two guards agreeing everywhere let one hide
// behind the other — after mutants I3, E3 and U2.
func TestADisconnectedPeerCostsNoDirectoryLookup(t *testing.T) {
	dir := &countingDir{}
	m := &ConnectionManager{
		peers:  map[string]*peerConn{},
		budget: DefaultConnectionBudget(),
		rt:     &Runtime{liveDir: dir},
	}
	live := connectedPeer("live", time.Millisecond)
	gone := connectedPeer("gone", time.Millisecond)
	gone.state = PeerDisconnected
	m.peers["live"], m.peers["gone"] = live, gone

	m.updatePriorities()

	if dir.calls != 1 {
		t.Errorf("%d directory lookups for 1 connected + 1 disconnected peer, want 1 — "+
			"disconnected peers are being carried into the anchor-lookup phase, which "+
			"runs a directory read per peer with m.mu released", dir.calls)
	}
}

// Disconnected peers must be skipped entirely: they are not candidates for
// prioritisation and a stale entry would keep influencing drain decisions.
//
// ⚠ This asserts the PAIR of guards, not either one — see
// TestADisconnectedPeerCostsNoDirectoryLookup for why that distinction matters
// and which test covers the snapshot guard on its own.
func TestDisconnectedPeersAreSkippedByUpdatePriorities(t *testing.T) {
	m := &ConnectionManager{peers: map[string]*peerConn{}, budget: DefaultConnectionBudget()}
	p := connectedPeer("gone", time.Millisecond)
	p.state, p.rpcsLastMinute = PeerDisconnected, 50
	m.peers["gone"] = p

	m.updatePriorities()

	// 🔬 READ THE MAP, NOT GetPriority. GetPriority returns PriorityIdle for an
	// unknown node (connection_budget.go:139), which is the same value an
	// explicitly-idle peer gets — so it cannot distinguish "never published"
	// from "published as idle", and this assertion would hold either way. The
	// absent-vs-zero collision this lane keeps filing, in the assertion itself.
	if _, published := m.budget.priorities["gone"]; published {
		t.Error("a disconnected peer was published to the budget — it stays in drain " +
			"accounting after the connection is gone")
	}
}
