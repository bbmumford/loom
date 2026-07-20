/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/ORBTR/aether"
)

// TopologyRouter provides topology-aware RPC session routing.
// It queries ConnectionReporter for direct peer connections and prefers
// Grade A (Noise UDP) or Grade B (QUIC) paths over TLS bootstrap fallback,
// reducing RPC latency from ~5-10ms to ~1ms for direct connections.
type TopologyRouter struct {
	reporter ConnectionReporter
	connMgr  *ConnectionManager
	identity *NodeIdentity

	mu    sync.RWMutex
	stats routerStats
}

// routerStats tracks routing decisions for observability.
type routerStats struct {
	DirectHits   uint64 // routed via direct A/B connection
	Fallbacks    uint64 // fell through to default dispatch
	GradeMisses  uint64 // connection exists but grade too low
	LookupErrors uint64 // ConnectionReporter returned no connection
}

// TopologyRouterConfig holds configuration for the topology router.
type TopologyRouterConfig struct {
	// MinGrade is the minimum connection grade to use for direct routing.
	// Connections below this grade fall back to default dispatch.
	// Default: GradeB (QUIC) — meaning only A and B connections are used.
	MinGrade Grade
}

// DefaultTopologyRouterConfig returns the default router configuration.
func DefaultTopologyRouterConfig() TopologyRouterConfig {
	return TopologyRouterConfig{
		MinGrade: GradeB,
	}
}

// NewTopologyRouter creates a topology-aware RPC router.
// reporter provides read-only connection state; peerMgr provides dial capability
// for establishing new sessions over direct transports.
func NewTopologyRouter(reporter ConnectionReporter, peerMgr *ConnectionManager, identity *NodeIdentity) *TopologyRouter {
	return &TopologyRouter{
		reporter: reporter,
		connMgr:  peerMgr,
		identity: identity,
	}
}

// RouteToNode returns a aether.Connection for reaching the target node via
// the best available path. If a Grade A or B direct connection exists
// (per ConnectionReporter), a session is established over that direct
// aether. Otherwise returns nil, allowing callers to fall back to
// their default dispatch path (e.g., TLS bootstrap via SessionPool).
//
// The caller is responsible for closing the returned session.
func (tr *TopologyRouter) RouteToNode(ctx context.Context, nodeID string) (aether.Connection, error) {
	if tr.reporter == nil {
		tr.recordFallback()
		return nil, nil
	}

	conn, ok := tr.reporter.ConnectionTo(nodeID)
	if !ok {
		tr.recordLookupError()
		return nil, nil
	}

	// Only route via direct connection if grade is A or B.
	if conn.Grade < GradeB {
		tr.recordGradeMiss(nodeID, conn.Grade)
		return nil, nil
	}

	// Determine protocol from connection grade and dial a fresh RPC session.
	proto := ProtocolForGrade(conn.Grade)
	if tr.connMgr == nil {
		tr.recordFallback()
		return nil, nil
	}

	session, err := tr.dialDirect(ctx, nodeID, proto)
	if err != nil {
		log.Printf("[TOPOLOGY] direct dial to %s via %s failed: %v (falling back)", shortID(nodeID), proto, err)
		tr.recordFallback()
		return nil, nil // graceful fallback — don't propagate dial errors
	}

	tr.recordDirectHit(nodeID, conn.Grade, conn.RTT)
	return session, nil
}

// GetExistingSession implements the dispatch.ExistingSessionProvider pattern.
// Checks for a direct Grade A/B connection and returns a session if available.
// Returns nil if no suitable direct connection exists (caller falls back).
//
// MESH-G05: despite the "existing session" name, this DIALS A FRESH transport
// session on every call (RouteToNode → dialDirect → connMgr.dialWithProtocol) —
// it does NOT return a pooled/reused session from the ConnectionManager's
// session table. The CALLER MUST Close the returned Connection; treating it like
// a cached session leaks one transport connection (and defeats the connection
// budget) per call. There is no in-repo caller today; a future one that expects
// reuse should instead consult GetMeshSession, or this should be rewired to
// return an established session. The name is retained only because it satisfies
// the dispatch.ExistingSessionProvider interface.
func (tr *TopologyRouter) GetExistingSession(nodeID string) aether.Connection {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	session, _ := tr.RouteToNode(ctx, nodeID)
	return session
}

// HasDirectRoute returns true if a Grade A or B direct connection exists
// to the target node, without establishing a session.
func (tr *TopologyRouter) HasDirectRoute(nodeID string) bool {
	if tr.reporter == nil {
		return false
	}
	conn, ok := tr.reporter.ConnectionTo(nodeID)
	return ok && conn.Grade >= GradeB
}

// Stats returns a snapshot of routing statistics.
func (tr *TopologyRouter) Stats() routerStats {
	tr.mu.RLock()
	defer tr.mu.RUnlock()
	return tr.stats
}

// dialDirect establishes a transport session to the target node using the
// specified protocol via ConnectionManager's transport infrastructure.
func (tr *TopologyRouter) dialDirect(ctx context.Context, nodeID string, proto Protocol) (aether.Connection, error) {
	tr.connMgr.mu.Lock()
	peer, ok := tr.connMgr.peers[nodeID]
	if !ok {
		tr.connMgr.mu.Unlock()
		return nil, fmt.Errorf("peer %s not found in ConnectionManager", shortID(nodeID))
	}
	// Copy what we need under lock, then release. MESH-G06: the scalar fields are
	// a stable snapshot, but a bare `*peer` also aliases the shared addresses
	// backing array (and the transports/drainedAt maps) that resync/scan mutate
	// under m.mu. Deep-copy the addresses slice — the only shared store the dial
	// path (bestAddress) reads — into a fresh backing array so the dial can't
	// race an in-place mutation.
	peerCopy := *peer
	peerCopy.addresses = append(peer.addresses[:0:0], peer.addresses...)
	tr.connMgr.mu.Unlock()

	return tr.connMgr.dialWithProtocol(ctx, &peerCopy, proto)
}

// --- stats helpers ---

func (tr *TopologyRouter) recordDirectHit(nodeID string, grade Grade, rtt time.Duration) {
	tr.mu.Lock()
	tr.stats.DirectHits++
	tr.mu.Unlock()
	dbgForward.Printf("Routed to %s via direct %s connection (RTT: %v)", shortID(nodeID), grade, rtt)
}

func (tr *TopologyRouter) recordFallback() {
	tr.mu.Lock()
	tr.stats.Fallbacks++
	tr.mu.Unlock()
}

func (tr *TopologyRouter) recordGradeMiss(nodeID string, grade Grade) {
	tr.mu.Lock()
	tr.stats.GradeMisses++
	tr.mu.Unlock()
	dbgForward.Printf("%s has grade %s (below B) — falling back to default dispatch", shortID(nodeID), grade)
}

func (tr *TopologyRouter) recordLookupError() {
	tr.mu.Lock()
	tr.stats.LookupErrors++
	tr.mu.Unlock()
}

// shortID returns the first 12 characters of a node ID for logging.
func shortID(nodeID string) string {
	return TruncateNodeID(nodeID, 12)
}
