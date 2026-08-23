/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"sync"
	"sync/atomic"
	"time"
)

// ═══════════════════════════════════════════════════════════════════════════
// Split-Brain Detection
//
// Provides Lamport timestamps for causal ordering of LAD records and a
// PartitionDetector that monitors clock divergence across peers to detect
// when the mesh has split into isolated clusters.
// ═══════════════════════════════════════════════════════════════════════════

// LamportClock is a lock-free logical clock for causal ordering.
// Each local event increments the clock. When merging with a remote clock,
// the local clock advances to max(local, remote) + 1.
type LamportClock struct {
	counter uint64 // accessed atomically
}

// Tick increments the clock and returns the new value.
// Called on every local publish event.
func (lc *LamportClock) Tick() uint64 {
	return atomic.AddUint64(&lc.counter, 1)
}

// Merge advances the clock to max(local, remote) + 1.
//
// 🛑 NOTHING CALLS THIS, AND NOTHING SHOULD. The merging clock for this estate
// is swarm.HLC.Observe (swarm/hlc.go): it already advances past observed remote
// stamps and is wired on the gossip ingest paths. Calling Merge as well would
// run a second merging clock beside that one.
//
// Kept rather than deleted because it is exported on a published module, so an
// in-tree caller count cannot bound external users.
//
// 🛑 Note what this does NOT fix: because the clock only ever ticks and is
// in-memory, it returns to 0 on restart. That matters because the LAD merge
// rule keys on this stamp — see the LamportClock field's consumers.
func (lc *LamportClock) Merge(remote uint64) uint64 {
	for {
		local := atomic.LoadUint64(&lc.counter)
		next := local + 1
		if remote >= local {
			next = remote + 1
		}
		if atomic.CompareAndSwapUint64(&lc.counter, local, next) {
			return next
		}
	}
}

// Current returns the current clock value without incrementing.
func (lc *LamportClock) Current() uint64 {
	return atomic.LoadUint64(&lc.counter)
}

// ─── Partition Detection ─────────────────────────────────────────────────

// peerClockState tracks the last-known Lamport clock value from a peer.
type peerClockState struct {
	Clock  uint64    // last reported Lamport clock
	SeenAt time.Time // when we last received this value
	NodeID string
}

// PartitionDetector monitors Lamport clock divergence across peers.
// When clusters become isolated during a network partition, their clocks
// diverge. After the partition heals, the detector identifies peers whose
// clocks have drifted significantly, indicating a split-brain event.
type PartitionDetector struct {
	mu    sync.RWMutex
	local *LamportClock

	// peerClocks tracks the last-known clock value for each peer
	peerClocks map[string]*peerClockState

	// DivergenceThreshold is the maximum allowed clock difference before
	// a partition is suspected. Default: 50 ticks.
	DivergenceThreshold uint64

	// SilenceTimeout is how long a peer can be silent before it is
	// considered potentially partitioned. Default: 2 minutes.
	SilenceTimeout time.Duration

	// SilentEvictTimeout is how long a peer can be silent before it is dropped
	// entirely (assumed departed, not partitioned). Without this a peer that
	// leaves and never returns is reported as a SilentPeer forever, which pins
	// Partitioned=true, and peerClocks would
	// grow unbounded. Default: 30 minutes.
	SilentEvictTimeout time.Duration
}

// NewPartitionDetector creates a detector that watches for split-brain events.
func NewPartitionDetector(local *LamportClock) *PartitionDetector {
	return &PartitionDetector{
		local:               local,
		peerClocks:          make(map[string]*peerClockState),
		DivergenceThreshold: 50,
		SilenceTimeout:      2 * time.Minute,
		SilentEvictTimeout:  30 * time.Minute,
	}
}

// ObservePeer records a peer's latest Lamport clock value, typically
// received during a gossip exchange.
func (pd *PartitionDetector) ObservePeer(nodeID string, clock uint64) {
	pd.mu.Lock()
	defer pd.mu.Unlock()

	state, ok := pd.peerClocks[nodeID]
	if !ok {
		pd.peerClocks[nodeID] = &peerClockState{
			Clock:  clock,
			SeenAt: time.Now(),
			NodeID: nodeID,
		}
		return
	}

	// SeenAt is LIVENESS and updates on every observation; Clock is ORDERING
	// and only advances. Tying SeenAt to a clock advance would stop refreshing
	// liveness for a peer that is gossiping but has published nothing new: it
	// ages into SilentPeers after SilenceTimeout and is evicted after
	// SilentEvictTimeout while still connected.
	state.SeenAt = time.Now()
	// Only advance — never regress (stale gossip re-delivers older records).
	if clock > state.Clock {
		state.Clock = clock
	}
}

// RemovePeer stops tracking a peer (e.g., after graceful leave).
func (pd *PartitionDetector) RemovePeer(nodeID string) {
	pd.mu.Lock()
	defer pd.mu.Unlock()
	delete(pd.peerClocks, nodeID)
}

// PartitionStatus describes the detected state of the mesh.
type PartitionStatus struct {
	// Partitioned is true if at least one peer shows signs of split-brain.
	Partitioned bool

	// DivergedPeers lists peers whose clocks have diverged beyond the threshold.
	DivergedPeers []DivergedPeer

	// SilentPeers lists peers that have not reported in within SilenceTimeout.
	SilentPeers []string

	// LocalClock is this node's own Lamport counter. Reported for operator
	// context ONLY — divergence is NOT measured against it (see
	// ReferenceClock). The two are different clock domains: peers report
	// hybrid logical clocks and this counter is a local tick count.
	LocalClock uint64

	// ReferenceClock is the highest peer clock currently observed — the
	// mesh's leading edge — and is what Delta is measured against.
	//
	// 🔴 BOTH OPERANDS OF Delta MUST COME FROM ObservePeer. ReferenceClock is
	// max(state.Clock) over live peers and Delta is ReferenceClock - state.Clock,
	// so the subtraction stays inside one clock domain by construction.
	// Comparing a peer HLC against LocalClock instead mixes a packed unix-ms
	// stamp with a tick counter, and every peer then exceeds DivergenceThreshold.
	//
	// 🛑 BOUND, so this does not overclaim: a detector referenced to SELF can
	// never notice that SELF is the partitioned side, and referencing the peer
	// leading edge fixes that only for the multi-peer case. A FULLY ISOLATED
	// node observes nobody, so its leading edge is its own stale view — total
	// isolation is undetectable here.
	ReferenceClock uint64
}

// DivergedPeer describes a peer whose clock lags the mesh's leading edge.
type DivergedPeer struct {
	NodeID    string
	PeerClock uint64

	// LocalClock is this node's own Lamport counter, for context only.
	// Delta is NOT derived from it — see ReferenceClock.
	LocalClock uint64

	// ReferenceClock is the leading edge Delta was measured against, so a
	// logged divergence is reproducible from the numbers in its own line.
	ReferenceClock uint64

	Delta    uint64 // ReferenceClock - PeerClock
	LastSeen time.Time
}

// Detect checks for split-brain indicators.
// Returns a PartitionStatus summarising the current state.
func (pd *PartitionDetector) Detect() PartitionStatus {
	pd.mu.Lock() // MESH-G02: write lock so we can evict long-departed peers inline
	defer pd.mu.Unlock()

	localClock := pd.local.Current()
	now := time.Now()

	status := PartitionStatus{
		LocalClock: localClock,
	}

	// PASS 1 — evict the departed, collect the silent, and establish the
	// reference clock from the peers that remain. The leading edge must be
	// computed BEFORE any divergence check, and evicted peers
	// must not contribute to it: a long-departed node's last-known clock is
	// exactly the stale value that would drag the reference backwards and
	// make every live peer look ahead of the mesh.
	live := make([]*peerClockState, 0, len(pd.peerClocks))
	for nodeID, state := range pd.peerClocks {
		silence := now.Sub(state.SeenAt)
		// A peer silent past the hard evict timeout is treated as
		// departed (dropped), not partitioned — otherwise a peer that leaves and
		// never returns is reported as SilentPeer forever and the map grows
		// without bound (the RemovePeer path may not fire for ungraceful exits).
		if silence > pd.SilentEvictTimeout {
			delete(pd.peerClocks, nodeID)
			continue
		}
		// Check for silence (peer not heard from recently)
		if silence > pd.SilenceTimeout {
			status.SilentPeers = append(status.SilentPeers, nodeID)
			status.Partitioned = true
			continue
		}
		if state.Clock > status.ReferenceClock {
			status.ReferenceClock = state.Clock
		}
		live = append(live, state)
	}

	// PASS 2 — divergence, measured against the leading edge rather than
	// against this node's own counter. Because ReferenceClock is the maximum
	// over `live`, every delta here is non-negative by construction and means
	// "how far this peer LAGS the mesh".
	for _, state := range live {
		delta := status.ReferenceClock - state.Clock
		if delta > pd.DivergenceThreshold {
			status.DivergedPeers = append(status.DivergedPeers, DivergedPeer{
				NodeID:         state.NodeID,
				PeerClock:      state.Clock,
				LocalClock:     localClock,
				ReferenceClock: status.ReferenceClock,
				Delta:          delta,
				LastSeen:       state.SeenAt,
			})
			status.Partitioned = true
		}
	}

	return status
}

// PeerCount returns the number of tracked peers.
func (pd *PartitionDetector) PeerCount() int {
	pd.mu.RLock()
	defer pd.mu.RUnlock()
	return len(pd.peerClocks)
}

// LogStatus logs the current partition status if a partition is detected.
func (pd *PartitionDetector) LogStatus() {
	status := pd.Detect()
	if !status.Partitioned {
		return
	}

	if len(status.DivergedPeers) > 0 {
		for _, dp := range status.DivergedPeers {
			// ref= is the operand delta is measured against; local= is this
			// node's own counter, carried for context and deliberately NOT
			// part of the arithmetic. Printing both keeps the
			// line self-checking: delta == ref - peer.
			dbgNodeHealth.Printf("Split-brain clock lag: peer=%s delta=%d (ref=%d peer=%d local=%d last_seen=%s)",
				dp.NodeID, dp.Delta, dp.ReferenceClock, dp.PeerClock, dp.LocalClock,
				dp.LastSeen.Format(time.RFC3339))
		}
	}
	if len(status.SilentPeers) > 0 {
		dbgNodeHealth.Printf("Split-brain silent peers (>%s): %v", pd.SilenceTimeout, status.SilentPeers)
	}
}
