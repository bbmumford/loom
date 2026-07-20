/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"sync"
)

// ConnectionPriority levels based on RPC activity.
// Higher values mean the connection is more important and should be protected from draining.
type ConnectionPriority int

const (
	PriorityIdle     ConnectionPriority = 0 // gossip only, no recent RPC
	PriorityLow      ConnectionPriority = 1 // <1 RPC/min
	PriorityNormal   ConnectionPriority = 2 // 1-10 RPCs/min
	PriorityHigh     ConnectionPriority = 3 // >10 RPCs/min
	PriorityCritical ConnectionPriority = 4 // anchor node, auth dispatch — never drain
)

func (p ConnectionPriority) String() string {
	switch p {
	case PriorityIdle:
		return "idle"
	case PriorityLow:
		return "low"
	case PriorityNormal:
		return "normal"
	case PriorityHigh:
		return "high"
	case PriorityCritical:
		return "critical"
	default:
		return "unknown"
	}
}

// ConnectionBudget controls the maximum number of connections a node can maintain.
// Prevents unbounded connection growth as the mesh scales.
type ConnectionBudget struct {
	mu               sync.RWMutex
	MaxPerPeer       int // max connections to a single peer (default: 3)
	MaxTotal         int // max total connections across all peers (default: 50)
	MinPerPeer       int // minimum connections to maintain per peer (default: 1)
	PreferredPerPeer int // target connections in normal conditions (default: 1)
	CrossRegionBonus int // extra connections for cross-region peers (default: 1)
	currentTotal     int // current total active connections
	priorities       map[string]ConnectionPriority // nodeID → priority level
}

// DefaultConnectionBudget returns sensible defaults for typical mesh deployments.
func DefaultConnectionBudget() *ConnectionBudget {
	return &ConnectionBudget{
		MaxPerPeer:       2,
		MaxTotal:         50,
		MinPerPeer:       1,
		PreferredPerPeer: 1,
		CrossRegionBonus: 1,
		priorities:       make(map[string]ConnectionPriority),
	}
}

// CanConnect returns true if the budget allows a new connection to the given peer.
// peerConns is the current number of connections to this specific peer.
func (b *ConnectionBudget) CanConnect(peerConns int) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.currentTotal >= b.MaxTotal {
		return false
	}
	if peerConns >= b.MaxPerPeer {
		return false
	}
	return true
}

// Acquire increments the connection counter. Returns false if over budget.
func (b *ConnectionBudget) Acquire() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.currentTotal >= b.MaxTotal {
		return false
	}
	b.currentTotal++
	return true
}

// Release decrements the connection counter.
func (b *ConnectionBudget) Release() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.currentTotal > 0 {
		b.currentTotal--
	}
}

// CurrentTotal returns the current total active connections.
func (b *ConnectionBudget) CurrentTotal() int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.currentTotal
}

// Utilization returns the fraction of budget used (0.0 to 1.0).
func (b *ConnectionBudget) Utilization() float64 {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.MaxTotal == 0 {
		return 1.0
	}
	return float64(b.currentTotal) / float64(b.MaxTotal)
}

// EffectiveMaxPerPeer returns MaxPerPeer + CrossRegionBonus for cross-region peers.
func (b *ConnectionBudget) EffectiveMaxPerPeer(crossRegion bool) int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	max := b.MaxPerPeer
	if crossRegion {
		max += b.CrossRegionBonus
	}
	return max
}

// SetPriority sets the connection priority for a given node.
// Thread-safe. Used by ConnectionManager.updatePriorities to reflect
// current RPC activity and anchor status.
func (b *ConnectionBudget) SetPriority(nodeID string, priority ConnectionPriority) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.priorities == nil {
		b.priorities = make(map[string]ConnectionPriority)
	}
	b.priorities[nodeID] = priority
}

// GetPriority returns the connection priority for a given node.
// Returns PriorityIdle if no priority has been set.
func (b *ConnectionBudget) GetPriority(nodeID string) ConnectionPriority {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.priorities == nil {
		return PriorityIdle
	}
	return b.priorities[nodeID]
}
