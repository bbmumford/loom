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
// Called when receiving a record from a peer during gossip.
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
	Clock    uint64    // last reported Lamport clock
	SeenAt   time.Time // when we last received this value
	NodeID   string
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
	// entirely (assumed departed, not partitioned). MESH-G02: without this a
	// peer that leaves and never returns would be reported as a SilentPeer
	// forever (permanent false-positive Partitioned=true) and peerClocks would
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

	// Only advance — never regress (stale gossip)
	if clock > state.Clock {
		state.Clock = clock
		state.SeenAt = time.Now()
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

	// LocalClock is the current local Lamport clock value.
	LocalClock uint64
}

// DivergedPeer describes a peer with a significantly different clock.
type DivergedPeer struct {
	NodeID     string
	PeerClock  uint64
	LocalClock uint64
	Delta      uint64
	LastSeen   time.Time
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

	for nodeID, state := range pd.peerClocks {
		silence := now.Sub(state.SeenAt)
		// MESH-G02: a peer silent past the hard evict timeout is treated as
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

		// Check clock divergence
		var delta uint64
		if localClock > state.Clock {
			delta = localClock - state.Clock
		} else {
			delta = state.Clock - localClock
		}

		if delta > pd.DivergenceThreshold {
			status.DivergedPeers = append(status.DivergedPeers, DivergedPeer{
				NodeID:     nodeID,
				PeerClock:  state.Clock,
				LocalClock: localClock,
				Delta:      delta,
				LastSeen:   state.SeenAt,
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
			dbgNodeHealth.Printf("Split-brain clock divergence: peer=%s delta=%d (local=%d peer=%d last_seen=%s)",
				dp.NodeID, dp.Delta, dp.LocalClock, dp.PeerClock, dp.LastSeen.Format(time.RFC3339))
		}
	}
	if len(status.SilentPeers) > 0 {
		dbgNodeHealth.Printf("Split-brain silent peers (>%s): %v", pd.SilenceTimeout, status.SilentPeers)
	}
}

// ResolveConflict determines which of two records should win during merge.
// Higher LamportClock wins. If clocks are equal (concurrent records from
// isolated clusters), the later UpdatedAt timestamp is the tiebreaker.
// Returns true if rec1 wins, false if rec2 wins.
func ResolveConflict(lc1, lc2 uint64, ts1, ts2 time.Time) bool {
	if lc1 != lc2 {
		return lc1 > lc2
	}
	// Concurrent records — tiebreak by timestamp
	return !ts1.Before(ts2)
}
