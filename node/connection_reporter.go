/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"time"
)

// ConnectionPurpose describes the capability of a connection.
type ConnectionPurpose string

const (
	PurposeGossip    ConnectionPurpose = "gossip"           // LAD gossip stream
	PurposeGossipDispatch ConnectionPurpose = "gossip+dispatch" // Aether: gossip + RPC on same session
)

// ConnectionInfo describes a single active connection to a peer.
type ConnectionInfo struct {
	PeerNodeID  string             // full node ID of the remote peer
	Grade       Grade              // current connection quality grade (A-F)
	Transport   string             // transport protocol label ("noise-udp", "websocket", etc.)
	RTT         time.Duration      // most recent network RTT measurement
	Region      string             // peer's region (from LAD reach record)
	CrossRegion bool               // true if peer is in a different region than self
	ConnCount   int                // number of active connections to this peer
	ConnectedAt time.Time          // when the connection was first established
	State       PeerState          // peer lifecycle state
	Priority    ConnectionPriority // tenant-aware connection priority (drives drain order and rebalance)
	Purpose     ConnectionPurpose  // "gossip" or "gossip+dispatch"
}

// ConnectionReporter provides read-only access to mesh connection state.
// Implementations must be safe for concurrent use.
type ConnectionReporter interface {
	// ActiveConnections returns all connections in the Connected state.
	ActiveConnections() []ConnectionInfo

	// ConnectionTo returns the connection info for a specific peer, or false
	// if no active connection exists.
	ConnectionTo(peerNodeID string) (ConnectionInfo, bool)

	// ConnectedPeerCount returns the number of peers in the Connected state.
	ConnectedPeerCount() int

	// SelfRegion returns the region of the local node.
	SelfRegion() string

	// SelfNodeID returns the node ID of the local node.
	SelfNodeID() string
}

// peerConnectionReporter wraps ConnectionManager to satisfy ConnectionReporter.
type peerConnectionReporter struct {
	mgr *ConnectionManager
}

// NewConnectionReporter wraps a ConnectionManager as a ConnectionReporter.
func NewConnectionReporter(mgr *ConnectionManager) ConnectionReporter {
	return &peerConnectionReporter{mgr: mgr}
}

// liveSessionView captures the per-peer transport/grade/RTT view from
// the live aether session, sourced from the same MeshSessionMetrics the
// mesh-debug endpoint surfaces. Reported separately from peerConn.protocol/
// peerConn.lastRTT because those collapsed fields lag the active session
// when a peer holds multiple concurrent transports: noise-udp may be
// registered in p.protocol while the actual data plane has already
// failed over to ws, and the topology view must reflect the data plane,
// not the most recent install. Populated under dispatchMu and consumed
// under m.mu; snapshotting first avoids holding both locks at once.
type liveSessionView struct {
	transport Protocol
	rtt       time.Duration
	present   bool
}

// snapshotLiveSessions captures the active aether session view for every
// open meshSession entry. Caller must NOT hold m.mu — dispatchMu is
// acquired here and the canonical lock-release-then-take-m.mu pattern
// (see zombie-sweep in peer_connections.go) avoids the reverse ordering
// that elsewhere acquires m.mu first then dispatchMu.
func (r *peerConnectionReporter) snapshotLiveSessions() map[string]liveSessionView {
	r.mgr.dispatchMu.RLock()
	defer r.mgr.dispatchMu.RUnlock()
	if r.mgr.meshSessions == nil {
		return nil
	}
	out := make(map[string]liveSessionView, len(r.mgr.meshSessions))
	for nodeID, sess := range r.mgr.meshSessions {
		if sess == nil || sess.IsClosed() {
			continue
		}
		sm := sess.Metrics()
		out[nodeID] = liveSessionView{
			transport: unmapProtocol(sess.Protocol()),
			rtt:       sm.RTT,
			present:   true,
		}
	}
	return out
}

func (r *peerConnectionReporter) ActiveConnections() []ConnectionInfo {
	live := r.snapshotLiveSessions()

	r.mgr.mu.Lock()
	defer r.mgr.mu.Unlock()

	result := make([]ConnectionInfo, 0, len(r.mgr.peers)*2)

	for _, p := range r.mgr.peers {
		if p.state != PeerConnected {
			continue
		}
		purpose := PurposeGossip
		if p.isMuxed {
			purpose = PurposeGossipDispatch
		}
		// Prefer the live aether session's transport/grade/RTT over the
		// collapsed peer-level fields. peerConn.protocol/lastRTT reflect
		// the most recent install, which can disagree with the active
		// data plane when multiple transports are held concurrently
		// (e.g., noise-udp registered while ws actually carries traffic).
		transport := p.protocol
		rtt := p.lastRTT
		if lv, ok := live[p.nodeID]; ok && lv.present {
			transport = lv.transport
			rtt = lv.rtt
		}
		result = append(result, ConnectionInfo{
			PeerNodeID:  p.nodeID,
			Grade:       GradeForProtocol(transport),
			Transport:   transport.String(),
			RTT:         rtt,
			Region:      p.peerRegion,
			CrossRegion: p.crossRegion,
			ConnCount:   p.connCount,
			ConnectedAt: p.lastConnected,
			State:       p.state,
			Priority:    p.priority,
			Purpose:     purpose,
		})
	}

	return result
}

func (r *peerConnectionReporter) ConnectionTo(peerNodeID string) (ConnectionInfo, bool) {
	// Single-peer lookup: snapshot just the one live session under
	// dispatchMu, then read peer state under m.mu. Same lock-ordering
	// rationale as ActiveConnections.
	r.mgr.dispatchMu.RLock()
	var lv liveSessionView
	if r.mgr.meshSessions != nil {
		if sess, ok := r.mgr.meshSessions[peerNodeID]; ok && sess != nil && !sess.IsClosed() {
			sm := sess.Metrics()
			lv = liveSessionView{
				transport: unmapProtocol(sess.Protocol()),
				rtt:       sm.RTT,
				present:   true,
			}
		}
	}
	r.mgr.dispatchMu.RUnlock()

	r.mgr.mu.Lock()
	defer r.mgr.mu.Unlock()

	p, ok := r.mgr.peers[peerNodeID]
	if !ok || p.state != PeerConnected {
		return ConnectionInfo{}, false
	}
	purpose := PurposeGossip
	if p.isMuxed {
		purpose = PurposeGossipDispatch
	}
	transport := p.protocol
	rtt := p.lastRTT
	if lv.present {
		transport = lv.transport
		rtt = lv.rtt
	}
	return ConnectionInfo{
		PeerNodeID:  p.nodeID,
		Grade:       GradeForProtocol(transport),
		Transport:   transport.String(),
		RTT:         rtt,
		Region:      p.peerRegion,
		CrossRegion: p.crossRegion,
		ConnCount:   p.connCount,
		ConnectedAt: p.lastConnected,
		State:       p.state,
		Priority:    p.priority,
		Purpose:     purpose,
	}, true
}

func (r *peerConnectionReporter) ConnectedPeerCount() int {
	r.mgr.mu.Lock()
	defer r.mgr.mu.Unlock()
	count := 0
	for _, p := range r.mgr.peers {
		if p.state == PeerConnected {
			count++
		}
	}
	return count
}

func (r *peerConnectionReporter) SelfRegion() string {
	return r.mgr.selfRegion
}

func (r *peerConnectionReporter) SelfNodeID() string {
	return r.mgr.selfID
}

// NilConnectionReporter is a no-op implementation for nodes without ConnectionManager.
type NilConnectionReporter struct{}

func (NilConnectionReporter) ActiveConnections() []ConnectionInfo          { return nil }
func (NilConnectionReporter) ConnectionTo(string) (ConnectionInfo, bool)   { return ConnectionInfo{}, false }
func (NilConnectionReporter) ConnectedPeerCount() int                      { return 0 }
func (NilConnectionReporter) SelfRegion() string                           { return "" }
func (NilConnectionReporter) SelfNodeID() string                           { return "" }

// Compile-time interface checks.
var _ ConnectionReporter = (*peerConnectionReporter)(nil)
var _ ConnectionReporter = NilConnectionReporter{}
