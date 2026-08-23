/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import "time"

// dedupRejectEvictTTL bounds how long a dedup-reject entry lingers.
// Far past any cooldown window, so eviction never drops a live-cooldown entry —
// it only reclaims entries for peers that were reject-looped but never
// successfully registered (the sole path that calls clearDedupRejectAllForPeer)
// or churned-out node IDs that never return.
const dedupRejectEvictTTL = 10 * time.Minute

// dedupRejectKey returns the composite map key for the dedupRejectAt
// table. Keyed by (nodeID, protoTag) so a reject on one protocol doesn't
// suppress dial attempts on another: keyed by nodeID alone, a WS
// dedup-reject blanket-blocks noise-UDP upgrade dials for the full
// 3-minute cooldown.
//
// protoTag is a stable per-protocol string. Callers passing aether
// sessions can use `session.Protocol().String()`; callers using
// node.Protocol can use `mapProtocol(proto).String()` or a hand-written
// label. The composite-key approach (vs map[string]map[Protocol]) keeps
// the lock and iteration patterns identical to the original nodeID-only
// implementation.
func dedupRejectKey(nodeID, protoTag string) string {
	return nodeID + "|" + protoTag
}

// recordDedupRejectLocked stamps a reject for (nodeID, protoTag).
// Caller holds m.dispatchMu.
func (m *ConnectionManager) recordDedupRejectLocked(nodeID, protoTag string) {
	if m.dedupRejectAt == nil {
		m.dedupRejectAt = make(map[string]time.Time)
	}
	now := time.Now()
	// Opportunistically evict entries older than the TTL so the map
	// can't grow without bound across a churning fleet.
	for k, t := range m.dedupRejectAt {
		if now.Sub(t) > dedupRejectEvictTTL {
			delete(m.dedupRejectAt, k)
		}
	}
	m.dedupRejectAt[dedupRejectKey(nodeID, protoTag)] = now
}

// dedupRejectAtFor returns the most recent reject timestamp for
// (nodeID, protoTag). Caller holds m.dispatchMu (or accepts a torn
// read). Zero Time when not recorded.
func (m *ConnectionManager) dedupRejectAtFor(nodeID, protoTag string) time.Time {
	if m.dedupRejectAt == nil {
		return time.Time{}
	}
	return m.dedupRejectAt[dedupRejectKey(nodeID, protoTag)]
}

// clearDedupRejectAllForPeer removes every dedup-reject entry for a
// given peer. Called when a session successfully registers, which makes any
// rejected duplicate dials for it stale information.
// Caller holds m.dispatchMu.
func (m *ConnectionManager) clearDedupRejectAllForPeer(nodeID string) {
	if m.dedupRejectAt == nil {
		return
	}
	prefix := nodeID + "|"
	for k := range m.dedupRejectAt {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			delete(m.dedupRejectAt, k)
		}
	}
}
