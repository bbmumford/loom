/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"sync"
	"time"
)

// connMapMaxEntries caps the connection map size to prevent unbounded growth
// from gossip-propagated data.
const connMapMaxEntries = 200

// ConnectionMapEntry tracks a single node's connection count as reported via gossip.
type ConnectionMapEntry struct {
	NodeID      string
	Connections int       // inbound + outbound connection count
	Capacity    int       // max connections this node can accept
	ReportedAt  time.Time // when the peer reported this count
}

// ConnectionMap aggregates connection counts propagated via gossip metadata.
// Each gossip exchange includes the sender's connection count. Over multiple
// rounds, every node builds an approximate view of the mesh-wide distribution.
type ConnectionMap struct {
	mu      sync.RWMutex
	entries map[string]ConnectionMapEntry
	maxAge  time.Duration // entries older than this are evicted
}

// NewConnectionMap creates a connection map with default staleness settings.
func NewConnectionMap() *ConnectionMap {
	return &ConnectionMap{
		entries: make(map[string]ConnectionMapEntry),
		maxAge:  2 * time.Minute, // 4x gossip interval
	}
}

// Update records a connection count from a peer's gossip metadata.
func (cm *ConnectionMap) Update(nodeID string, connections, capacity int) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.entries[nodeID] = ConnectionMapEntry{
		NodeID:      nodeID,
		Connections: connections,
		Capacity:    capacity,
		ReportedAt:  time.Now(),
	}
	cm.pruneIfNeeded()
}

// BatchUpdate processes a full connection map from gossip metadata.
func (cm *ConnectionMap) BatchUpdate(counts map[string]int) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	for nodeID, count := range counts {
		existing, ok := cm.entries[nodeID]
		if ok {
			existing.Connections = count
			existing.ReportedAt = time.Now()
			cm.entries[nodeID] = existing
		} else {
			cm.entries[nodeID] = ConnectionMapEntry{
				NodeID:      nodeID,
				Connections: count,
				ReportedAt:  time.Now(),
			}
		}
	}
	cm.pruneIfNeeded()
}

// ConnectionCount returns the last known connection count for a peer.
// Returns 0, false if no data available.
func (cm *ConnectionMap) ConnectionCount(nodeID string) (int, bool) {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	entry, ok := cm.entries[nodeID]
	if !ok || time.Since(entry.ReportedAt) > cm.maxAge {
		return 0, false
	}
	return entry.Connections, true
}

// MeshAverage returns the average connection count across all non-stale entries.
func (cm *ConnectionMap) MeshAverage() float64 {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	now := time.Now()
	var total, count int
	for _, entry := range cm.entries {
		if now.Sub(entry.ReportedAt) <= cm.maxAge {
			total += entry.Connections
			count++
		}
	}
	if count == 0 {
		return 0
	}
	return float64(total) / float64(count)
}

// IsHotspot returns true if the peer has significantly more connections than average.
// Threshold: >2x the mesh-wide mean.
func (cm *ConnectionMap) IsHotspot(nodeID string) bool {
	conns, ok := cm.ConnectionCount(nodeID)
	if !ok {
		return false
	}
	avg := cm.MeshAverage()
	if avg == 0 {
		return false
	}
	return float64(conns) > 2.0*avg
}

// EvictStale removes entries older than maxAge.
func (cm *ConnectionMap) EvictStale() int {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	now := time.Now()
	evicted := 0
	for nodeID, entry := range cm.entries {
		if now.Sub(entry.ReportedAt) > cm.maxAge {
			delete(cm.entries, nodeID)
			evicted++
		}
	}
	return evicted
}

// pruneIfNeeded evicts stale entries and enforces the connMapMaxEntries cap.
// Must be called with mu held (write lock).
func (cm *ConnectionMap) pruneIfNeeded() {
	now := time.Now()
	// First pass: evict stale entries
	for nodeID, entry := range cm.entries {
		if now.Sub(entry.ReportedAt) > cm.maxAge {
			delete(cm.entries, nodeID)
		}
	}
	// Hard cap: evict oldest entries by ReportedAt
	if len(cm.entries) > connMapMaxEntries {
		evictCount := len(cm.entries) - connMapMaxEntries
		for i := 0; i < evictCount; i++ {
			var oldestID string
			var oldestTime time.Time
			for id, entry := range cm.entries {
				if oldestID == "" || entry.ReportedAt.Before(oldestTime) {
					oldestID = id
					oldestTime = entry.ReportedAt
				}
			}
			delete(cm.entries, oldestID)
		}
		dbgPeers.Printf("CONN-MAP pruned to %d entries (cap=%d, evicted=%d)",
			len(cm.entries), connMapMaxEntries, evictCount)
	}
}

// Snapshot returns the current connection counts for inclusion in gossip metadata.
func (cm *ConnectionMap) Snapshot() map[string]int {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	now := time.Now()
	snapshot := make(map[string]int, len(cm.entries))
	for nodeID, entry := range cm.entries {
		if now.Sub(entry.ReportedAt) <= cm.maxAge {
			snapshot[nodeID] = entry.Connections
		}
	}
	return snapshot
}
