/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"testing"
	"time"
)

// Covers the PartitionDetector.
//
// Detect's DIVERGENCE branch measures a peer's clock against the mesh's
// leading edge. Both operands must come from the same domain: peers report a
// swarm HLC (packed unix-MILLISECONDS << 16, ≈1.17e17), so comparing one
// against this node's Lamport tick counter puts every peer over the
// threshold of 50 permanently. The tests below pin the shared domain.
//
// The silence and janitor paths key off SeenAt rather than the clock, so they
// are independent of that comparison.

func detectorFixture() *PartitionDetector {
	return NewPartitionDetector(&LamportClock{})
}

// observedAt plants a peer with an explicit SeenAt. Constructing the state
// directly is the only way to test time-based branches without sleeping for
// minutes, and peerClockState is in-package.
func observedAt(pd *PartitionDetector, nodeID string, clock uint64, seenAt time.Time) {
	pd.mu.Lock()
	defer pd.mu.Unlock()
	pd.peerClocks[nodeID] = &peerClockState{Clock: clock, SeenAt: seenAt, NodeID: nodeID}
}

// "Only advance — never regress (stale gossip)". Gossip re-delivers older
// records routinely, so without this a peer's tracked clock would sawtooth
// with whatever arrived last.
func TestObservePeerAdvancesButNeverRegresses(t *testing.T) {
	pd := detectorFixture()

	pd.ObservePeer(testNodeIDB, 100)
	if got := pd.PeerCount(); got != 1 {
		t.Fatalf("PeerCount = %d after the first observation, want 1", got)
	}

	pd.ObservePeer(testNodeIDB, 150)
	pd.mu.RLock()
	advanced := pd.peerClocks[testNodeIDB].Clock
	pd.mu.RUnlock()
	if advanced != 150 {
		t.Fatalf("clock = %d after a HIGHER observation, want 150 — the "+
			"detector ignores forward progress", advanced)
	}

	pd.ObservePeer(testNodeIDB, 20) // stale re-delivery
	pd.mu.RLock()
	after := pd.peerClocks[testNodeIDB].Clock
	pd.mu.RUnlock()
	if after != 150 {
		t.Fatalf("clock = %d after a STALE observation, want it held at 150 — a "+
			"re-delivered old record drags the peer's tracked clock backwards, "+
			"which is exactly what gossip does all day", after)
	}

	// A second peer must be tracked separately.
	pd.ObservePeer(testNodeIDA, 5)
	if got := pd.PeerCount(); got != 2 {
		t.Fatalf("PeerCount = %d with two distinct peers, want 2", got)
	}
}

// SeenAt is LIVENESS, Clock is ORDERING, and ObservePeer must move SeenAt on
// every observation.
//
// Moving SeenAt only when the clock ADVANCES means a peer that is alive and
// gossiping but has published nothing new stops refreshing its liveness: it
// ages into SilentPeers after SilenceTimeout and is evicted after
// SilentEvictTimeout while still connected. An idle peer is not a partitioned
// one.
func TestObservePeerRefreshesLivenessEvenWhenTheClockDoesNotAdvance(t *testing.T) {
	pd := detectorFixture()
	const clock = 100

	// A peer last heard from long enough ago to be reported silent.
	observedAt(pd, testNodeIDB, clock, time.Now().Add(-pd.SilenceTimeout-time.Minute))
	if st := pd.Detect(); len(st.SilentPeers) != 1 {
		t.Fatalf("premise wrong: the planted peer is not silent (SilentPeers=%v) "+
			"so the refresh below would have nothing to fix", st.SilentPeers)
	}

	// It gossips, but has published nothing new — same clock value.
	pd.ObservePeer(testNodeIDB, clock)

	st := pd.Detect()
	if len(st.SilentPeers) != 0 {
		t.Fatalf("SilentPeers = %v after the peer was observed — a live peer "+
			"that simply has no new records to publish is still reported as "+
			"partitioned, and after SilentEvictTimeout it is dropped from the "+
			"table while connected", st.SilentPeers)
	}

	// And the ordering half must be unchanged: a non-advancing observation
	// must not move the clock.
	pd.mu.RLock()
	got := pd.peerClocks[testNodeIDB].Clock
	pd.mu.RUnlock()
	if got != clock {
		t.Fatalf("clock = %d after a same-value observation, want %d — the "+
			"liveness fix moved the ordering value too", got, clock)
	}
}

func TestRemovePeerStopsTracking(t *testing.T) {
	pd := detectorFixture()
	pd.ObservePeer(testNodeIDB, 10)
	if pd.PeerCount() != 1 {
		t.Fatal("premise wrong: the peer was never tracked")
	}

	pd.RemovePeer(testNodeIDB)

	if got := pd.PeerCount(); got != 0 {
		t.Fatalf("PeerCount = %d after RemovePeer, want 0 — a peer that left "+
			"gracefully is still watched and will be reported as silent", got)
	}
	// Removing an unknown peer must not panic or corrupt the map.
	pd.RemovePeer("never-seen")
	if got := pd.PeerCount(); got != 0 {
		t.Fatalf("PeerCount = %d after removing an unknown peer, want 0", got)
	}
}

// A peer quiet past SilenceTimeout is reported — this is the detector's one
// working signal, so it is worth pinning precisely.
func TestDetectReportsPeersSilentPastTheTimeout(t *testing.T) {
	pd := detectorFixture()

	// Control: a freshly-seen peer at the local clock value is NOT reported.
	// This deliberately avoids the divergence question — local is 0 and so is
	// the peer, so delta is 0 whatever domain the values are in.
	observedAt(pd, testNodeIDA, 0, time.Now())
	if st := pd.Detect(); st.Partitioned {
		t.Fatalf("premise wrong: a fresh, non-diverged peer was already "+
			"reported as partitioned (silent=%v diverged=%d) — the assertions "+
			"below could not distinguish anything",
			st.SilentPeers, len(st.DivergedPeers))
	}

	observedAt(pd, testNodeIDB, 0, time.Now().Add(-pd.SilenceTimeout-time.Second))

	st := pd.Detect()

	if !st.Partitioned {
		t.Fatal("a peer silent past SilenceTimeout did not set Partitioned")
	}
	if len(st.SilentPeers) != 1 || st.SilentPeers[0] != testNodeIDB {
		t.Fatalf("SilentPeers = %v, want exactly [%s]", st.SilentPeers, testNodeIDB)
	}
	if pd.PeerCount() != 2 {
		t.Fatalf("PeerCount = %d — a merely SILENT peer must still be tracked; "+
			"only the hard evict timeout drops it", pd.PeerCount())
	}
}

// MESH-G02's janitor. Without it, a peer that leaves ungracefully (so
// RemovePeer never fires) is reported as a SilentPeer forever — a permanent
// false Partitioned=true — and peerClocks grows without bound.
func TestDetectEvictsPeersSilentPastTheHardTimeout(t *testing.T) {
	pd := detectorFixture()
	observedAt(pd, testNodeIDB, 0, time.Now().Add(-pd.SilentEvictTimeout-time.Second))
	if pd.PeerCount() != 1 {
		t.Fatal("premise wrong: the peer was not planted")
	}

	st := pd.Detect()

	if pd.PeerCount() != 0 {
		t.Fatalf("PeerCount = %d — a long-departed peer was not evicted, so "+
			"peerClocks grows for the life of the process", pd.PeerCount())
	}
	if len(st.SilentPeers) != 0 {
		t.Fatalf("SilentPeers = %v — an EVICTED peer was also reported silent. "+
			"It is gone, not partitioned, and reporting it keeps Partitioned "+
			"true forever, which is the false positive MESH-G02 removed",
			st.SilentPeers)
	}
	if st.Partitioned {
		t.Fatal("Partitioned was set by a peer that had already departed")
	}
}

// The two timeouts are ordered: eviction is checked first and wins, so a peer
// past BOTH thresholds is dropped rather than reported. Getting this backwards
// reintroduces the permanent-false-positive that MESH-G02 fixed.
func TestEvictionTakesPrecedenceOverSilenceReporting(t *testing.T) {
	pd := detectorFixture()
	if pd.SilentEvictTimeout <= pd.SilenceTimeout {
		t.Fatalf("premise wrong: evict (%v) must be longer than silence (%v) "+
			"for this ordering to be testable", pd.SilentEvictTimeout, pd.SilenceTimeout)
	}

	// Past BOTH thresholds.
	observedAt(pd, testNodeIDB, 0, time.Now().Add(-pd.SilentEvictTimeout-time.Minute))
	// Past silence only.
	observedAt(pd, testNodeIDA, 0, time.Now().Add(-pd.SilenceTimeout-time.Second))

	st := pd.Detect()

	if len(st.SilentPeers) != 1 || st.SilentPeers[0] != testNodeIDA {
		t.Fatalf("SilentPeers = %v, want exactly [%s] — the peer past the hard "+
			"evict timeout should have been dropped silently, not reported",
			st.SilentPeers, testNodeIDA)
	}
	if got := pd.PeerCount(); got != 1 {
		t.Fatalf("PeerCount = %d, want 1 — exactly the evicted peer should be "+
			"gone and the merely-silent one kept", got)
	}
}

// Divergence is measured against the mesh's LEADING EDGE — the highest clock
// among live peers — which puts both operands in one domain by construction
// and is what gives DivergenceThreshold meaning. Comparing peer HLCs against
// this node's Lamport tick counter instead spans two domains, and every peer
// then exceeds the threshold permanently.
func TestDivergenceIsMeasuredAgainstTheMeshLeadingEdgeNotTheLocalCounter(t *testing.T) {
	lc := &LamportClock{}
	pd := NewPartitionDetector(lc)
	// A local counter deliberately in a different magnitude from the peer
	// clocks — the exact condition that used to make everything diverge.
	for i := 0; i < 5; i++ {
		lc.Tick()
	}

	const edge = 1_000_000
	now := time.Now()
	observedAt(pd, testNodeIDA, edge, now)    // leading edge
	observedAt(pd, testNodeIDB, edge-10, now) // in step
	const laggard = "cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33"
	observedAt(pd, laggard, edge-(pd.DivergenceThreshold+1_000), now) // genuinely behind

	if pd.DivergenceThreshold >= 1_000 {
		t.Fatalf("premise wrong: threshold %d is too large for this fixture's "+
			"spread, so the laggard may not clear it", pd.DivergenceThreshold)
	}

	st := pd.Detect()

	if st.ReferenceClock != edge {
		t.Fatalf("ReferenceClock = %d, want the leading edge %d", st.ReferenceClock, edge)
	}
	if st.LocalClock != lc.Current() {
		t.Fatalf("LocalClock = %d, want this node's own counter %d — the field "+
			"must keep meaning what its name says even though divergence is no "+
			"longer measured from it", st.LocalClock, lc.Current())
	}
	if len(st.DivergedPeers) != 1 {
		t.Fatalf("DivergedPeers = %d, want exactly the one laggard. If this is "+
			"3, divergence is being measured against the local tick counter "+
			"again and every peer trips the threshold", len(st.DivergedPeers))
	}
	dp := st.DivergedPeers[0]
	if dp.NodeID != laggard {
		t.Fatalf("the reported diverged peer is %s, want the laggard", dp.NodeID)
	}
	// The log line must be self-checking: delta == ref - peer.
	if dp.Delta != dp.ReferenceClock-dp.PeerClock {
		t.Fatalf("delta %d != ref %d - peer %d — a logged divergence cannot be "+
			"reproduced from the numbers in its own line",
			dp.Delta, dp.ReferenceClock, dp.PeerClock)
	}
	if !st.Partitioned {
		t.Fatal("a peer past the divergence threshold did not set Partitioned")
	}
}

// The bound on the leading edge: peers that have been EVICTED must not set
// it. A departed node's last-known
// clock is exactly the stale value that would drag the reference backwards
// and make every live peer look ahead of the mesh.
func TestEvictedPeersDoNotSetTheReferenceClock(t *testing.T) {
	pd := detectorFixture()
	const fresh = 5_000
	observedAt(pd, testNodeIDA, fresh, time.Now())
	// A departed peer whose last-known clock is far AHEAD — if it counted,
	// it would become the leading edge and brand the live peer a laggard.
	observedAt(pd, testNodeIDB, fresh+1_000_000, time.Now().Add(-pd.SilentEvictTimeout-time.Minute))

	st := pd.Detect()

	if st.ReferenceClock != fresh {
		t.Fatalf("ReferenceClock = %d, want %d — an evicted peer's stale clock "+
			"set the mesh's leading edge", st.ReferenceClock, fresh)
	}
	if len(st.DivergedPeers) != 0 {
		t.Fatalf("DivergedPeers = %v — the only live peer was branded diverged "+
			"against a departed node's clock", st.DivergedPeers)
	}
}

// The local clock is reported verbatim so an operator reading a partition log
// can compare it against the peer values in the same line.
func TestDetectReportsTheLocalClockItObserved(t *testing.T) {
	lc := &LamportClock{}
	pd := NewPartitionDetector(lc)

	lc.Tick()
	lc.Tick()
	want := lc.Current()
	if want != 2 {
		t.Fatalf("premise wrong: two ticks produced %d, want 2", want)
	}

	if got := pd.Detect().LocalClock; got != want {
		t.Fatalf("LocalClock = %d, want %d — the status reports a clock value "+
			"other than the one it compared against, so every logged delta is "+
			"unreproducible", got, want)
	}
}
