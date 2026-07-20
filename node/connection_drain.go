/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"log"
	"sync"
	"time"
)

// DrainState represents the lifecycle of a draining connection.
type DrainState int

const (
	DrainActive   DrainState = iota // connection is active (not draining)
	DrainStarted                    // drain initiated, finishing in-flight
	DrainComplete                   // all in-flight complete, ready to close
)

func (s DrainState) String() string {
	switch s {
	case DrainActive:
		return "active"
	case DrainStarted:
		return "draining"
	case DrainComplete:
		return "complete"
	default:
		return "unknown"
	}
}

// DrainEntry tracks a single connection being drained.
type DrainEntry struct {
	PeerNodeID  string
	Transport   string
	State       DrainState
	StartedAt   time.Time
	Reason      string // "scale_down", "grade_upgrade", "budget_exceeded"
	InFlightOps int    // MESH-C04: reserved for future per-op quiescence tracking — currently inert (never incremented); monitorDrain uses drainGracePeriod instead
}

// DrainManager coordinates graceful connection draining.
// When ConnectionManager decides to reduce connections to a peer,
// it hands the connection to DrainManager instead of abruptly closing it.
type DrainManager struct {
	mu       sync.Mutex
	entries  map[string]*DrainEntry             // key: peerNodeID+transport
	timeout  time.Duration                      // max time to wait for in-flight to finish
	onClosed func(peerNodeID, transport string) // callback when drain completes
}

// DrainTimeout is the maximum time to wait for in-flight operations during drain.
const DrainTimeout = 30 * time.Second

// drainGracePeriod is how long a drained connection stays open before the force
// close, giving in-flight gossip exchanges and RPCs a window to finish. MESH-C04:
// DrainEntry.InFlightOps was never incremented anywhere (DecrementInFlight has
// zero callers), so the old 1s poll saw 0 on its first tick and force-closed
// almost immediately — aborting in-flight work and never engaging DrainTimeout.
// Per-op in-flight accounting is not wired through the gossip/RPC paths, so this
// is honestly a best-effort fixed grace window, not true quiescence tracking.
const drainGracePeriod = 5 * time.Second

// NewDrainManager creates a drain manager.
func NewDrainManager(onClosed func(peerNodeID, transport string)) *DrainManager {
	return &DrainManager{
		entries:  make(map[string]*DrainEntry),
		timeout:  DrainTimeout,
		onClosed: onClosed,
	}
}

// drainKey generates a unique key for a peer+transport pair.
func drainKey(peerNodeID, transport string) string {
	return peerNodeID + "::" + transport
}

// StartDrain initiates graceful draining of a connection.
// The connection stops accepting new gossip exchanges and waits for in-flight
// operations to complete (up to DrainTimeout).
func (dm *DrainManager) StartDrain(peerNodeID, transport, reason string) {
	dm.mu.Lock()
	defer dm.mu.Unlock()

	key := drainKey(peerNodeID, transport)
	if _, exists := dm.entries[key]; exists {
		return // already draining
	}

	entry := &DrainEntry{
		PeerNodeID: peerNodeID,
		Transport:  transport,
		State:      DrainStarted,
		StartedAt:  time.Now(),
		Reason:     reason,
	}
	dm.entries[key] = entry

	log.Printf("[DRAIN] Started draining %s connection to %s (reason: %s)",
		transport, truncID(peerNodeID), reason)

	// Monitor the drain in a goroutine
	go dm.monitorDrain(key, entry)
}

// monitorDrain keeps the connection open for a best-effort grace period so
// in-flight gossip/RPC can finish, then force-closes it. MESH-C04: see
// drainGracePeriod — the previous in-flight poll was inert and closed in ~1s.
func (dm *DrainManager) monitorDrain(key string, entry *DrainEntry) {
	grace := dm.timeout
	if drainGracePeriod < grace {
		grace = drainGracePeriod
	}
	timer := time.NewTimer(grace)
	defer timer.Stop()

	<-timer.C
	// If a concurrent completeDrain already fired, this is a no-op.
	log.Printf("[DRAIN] Grace period (%v) elapsed for %s to %s, closing",
		grace, entry.Transport, truncID(entry.PeerNodeID))
	dm.completeDrain(key)
}

// completeDrain marks a drain as complete and triggers the close callback.
func (dm *DrainManager) completeDrain(key string) {
	dm.mu.Lock()
	entry, exists := dm.entries[key]
	if !exists {
		dm.mu.Unlock()
		return
	}
	entry.State = DrainComplete
	delete(dm.entries, key)
	dm.mu.Unlock()

	if dm.onClosed != nil {
		dm.onClosed(entry.PeerNodeID, entry.Transport)
	}
}

// DecrementInFlight decreases the in-flight count for a draining connection.
// MESH-C04: reserved for future per-op quiescence tracking. It has no callers
// today, and monitorDrain uses a fixed drainGracePeriod rather than polling
// InFlightOps, so this is currently inert.
func (dm *DrainManager) DecrementInFlight(peerNodeID, transport string) {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	key := drainKey(peerNodeID, transport)
	if entry, exists := dm.entries[key]; exists {
		entry.InFlightOps--
	}
}

// IsDraining returns true if the connection is currently being drained.
func (dm *DrainManager) IsDraining(peerNodeID, transport string) bool {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	_, exists := dm.entries[drainKey(peerNodeID, transport)]
	return exists
}

// DrainCount returns the number of connections currently being drained.
func (dm *DrainManager) DrainCount() int {
	dm.mu.Lock()
	defer dm.mu.Unlock()
	return len(dm.entries)
}

// SelectForDrain picks which connections to drain when scaling down.
// Uses the existing ConnectionInfo type from connection_reporter.go.
// Returns connections ordered by drain priority (drain first = index 0):
//  1. Critical connections are never drained (filtered out)
//  2. Lower priority drains first (Idle before Low before Normal before High)
//  3. Lowest grade transport within same priority (worst quality drained first)
//  4. Highest latency within same grade
//  5. Most recently established (preserve longer-lived stable connections)
func SelectForDrain(connections []ConnectionInfo, targetCount int) []ConnectionInfo {
	if len(connections) <= targetCount {
		return nil // nothing to drain
	}

	// Filter out Critical connections — they never drain
	drainable := make([]ConnectionInfo, 0, len(connections))
	for _, c := range connections {
		if c.Priority >= PriorityCritical {
			continue
		}
		drainable = append(drainable, c)
	}

	criticalCount := len(connections) - len(drainable)
	adjustedTarget := targetCount - criticalCount
	if adjustedTarget < 0 {
		adjustedTarget = 0
	}
	if len(drainable) <= adjustedTarget {
		return nil // nothing to drain after protecting critical
	}

	// Sort by drain priority using insertion sort (stable)
	sorted := make([]ConnectionInfo, len(drainable))
	copy(sorted, drainable)

	for i := 1; i < len(sorted); i++ {
		for j := i; j > 0; j-- {
			if shouldDrainFirst(sorted[j], sorted[j-1]) {
				sorted[j], sorted[j-1] = sorted[j-1], sorted[j]
			}
		}
	}

	// Return the excess connections (first N in drain-priority order)
	excess := len(drainable) - adjustedTarget
	return sorted[:excess]
}

// shouldDrainFirst returns true if a should be drained before b.
// Priority is the primary sort key so lower-priority (idle/low) tenant
// connections are drained before higher-priority ones when scale-down
// pressure hits.
func shouldDrainFirst(a, b ConnectionInfo) bool {
	// Lower priority drains first (Idle < Low < Normal < High)
	if a.Priority != b.Priority {
		return a.Priority < b.Priority
	}
	// Lower grade (worse transport) drains first
	if a.Grade != b.Grade {
		return a.Grade < b.Grade
	}
	// Higher latency drains first within same grade
	if a.RTT != b.RTT {
		return a.RTT > b.RTT
	}
	// More recently established drains first (preserve old stable connections)
	return a.ConnectedAt.After(b.ConnectedAt)
}
