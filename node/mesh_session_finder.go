/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"context"
	"fmt"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/rpc/pb"
	"github.com/bbmumford/loom/pkg/dispatch"
)

// FindSession resolves a role + handler to an Aether session.
// Implements dispatch.SessionFinder interface.
//
// Path selection:
// 1. Direct session to the target node (LAD role query → GetMeshSession)
// 2. Any available session for mesh forwarding (GetAnyMeshSession)
// 3. Error — no session available
func (m *ConnectionManager) FindSession(ctx context.Context, role, handler string) (aether.Session, error) {
	if m == nil {
		return nil, fmt.Errorf("mesh not initialized")
	}
	// Find target node via LAD role query
	nodeID := m.findNodeForRole(ctx, role, handler)

	// Path 1: Direct session to target node
	if nodeID != "" {
		if session, ok := m.GetMeshSession(nodeID); ok {
			return session, nil
		}
	}

	// Path 2: Any session for forwarding (peer will forward to target)
	if session, ok := m.GetAnyMeshSession(); ok {
		return session, nil
	}

	// Path 3: No session available
	if nodeID != "" {
		return nil, fmt.Errorf("no Aether session to %s (role=%s, handler=%s)", truncID(nodeID), role, handler)
	}
	return nil, fmt.Errorf("no nodes found for role %s (handler=%s)", role, handler)
}

// FindAnySession returns any available Aether session for mesh forwarding.
// Implements dispatch.SessionFinder interface.
func (m *ConnectionManager) FindAnySession() (aether.Session, bool) {
	if m == nil {
		return nil, false
	}
	return m.GetAnyMeshSession()
}

// IsSupersededByUpgrade reports whether the given session has been
// replaced by a higher-grade session for the same peer. Used by the
// HWPCaller's evictIdle to drop cached sessions whose peer now has a
// better path available — without this, cold-start cross-region WS
// sessions stick in the cache for the full 5-minute idle TTL even
// after a same-region noise-UDP session arrives 30 seconds later.
//
// Implements dispatch.SessionFinder.IsSupersededByUpgrade.
func (m *ConnectionManager) IsSupersededByUpgrade(session aether.Session) bool {
	if m == nil || session == nil || session.IsClosed() {
		return false
	}
	nodeID := string(session.RemoteNodeID())
	if nodeID == "" {
		return false
	}
	current, ok := m.GetMeshSession(nodeID)
	if !ok || current == nil || current == session {
		return false
	}
	// Current is a different session for the same peer. If it has a
	// strictly higher grade, the cached one is superseded.
	return SessionGrade(current) > SessionGrade(session)
}

// CallViaBidi sends an RPC via BidiRPC (Stream 1) if available for the node.
// Returns (nil, false, nil) if no BidiRPC channel exists OR the existing
// one is dead — both cases signal the dispatch layer to use its dynamic-
// stream fallback instead of treating the bidi error as a routing failure.
//
// Race window addressed: when a session closes, BidiRPC.readLoop exits and
// closes b.done BEFORE the cleanup path's unregisterMeshSession runs and
// evicts the bidi entry. A GetBidiRPC call inside that window returns the
// still-registered-but-dead bidi; without IsAlive() / post-call recheck,
// the dispatch loop would surface "bidi stream closed" as the probe error
// and skip the dynamic-stream fallback, so every probe of a caller fails
// with "bidi stream closed" whenever WS sessions to cross-origin anchors
// churn. So a bidi found dead before the call is treated as "no bidi" and the
// stale entry is evicted, and the next call goes straight to the
// dynamic-stream path until a fresh bidi is registered.
//
// A bidi that dies DURING the call is evicted the same way, but the request
// may already have reached the peer. Unless the call's error says it never
// left (dispatch.Unsent), the answer is (nil, true, err) with err unwrapping to
// dispatch.ErrChannelClosed: HWPCaller resends it over a dynamic stream only for
// an effect.Class.Repeatable operation, and CallNode returns it.
//
// Implements dispatch.SessionFinder.CallViaBidi.
func (m *ConnectionManager) CallViaBidi(ctx context.Context, nodeID string, req *pb.RPCRequest) (*pb.RPCResponse, bool, error) {
	if m == nil {
		return nil, false, nil
	}
	// Resolve the active session first so we can scope dead-bidi
	// eviction precisely to that session (sibling sessions for the
	// same peer keep their own bidis).
	sess, sessOK := m.GetMeshSession(nodeID)
	if !sessOK || sess == nil {
		return nil, false, nil
	}
	// The bidi registered on that same session, not a second lookup by node:
	// the gate below grades sess, so the request must leave on sess.
	bidi, ok := m.bidiForSession(sess)
	if !ok {
		return nil, false, nil
	}
	if !bidi.IsAlive() {
		// Pre-call dead-bidi detection: evict the dead bidi from its
		// session's slot so subsequent dispatch rounds bypass it.
		m.evictDeadBidiForSession(sess, bidi)
		return nil, false, nil
	}
	// The grade gate on the session this request would leave on. HWPCaller
	// gates the sessions it holds itself; it never holds this one, and for
	// CallNode this is the only place the route's grade is known. A refusal is
	// reported as handled (ok=true) so the caller returns it instead of
	// trying its dynamic-stream arm.
	if err := dispatch.CheckRouteGrade(req.Handler, req.Context["opClass"], sess); err != nil {
		return nil, true, err
	}
	resp, err := bidi.Call(ctx, req)
	if err != nil && !bidi.IsAlive() {
		// Bidi died DURING the call (stream closed under us).
		m.evictDeadBidiForSession(sess, bidi)
		if dispatch.Unsent(err) {
			return nil, false, nil
		}
		return nil, true, fmt.Errorf("%w: %w", dispatch.ErrChannelClosed, err)
	}
	return resp, true, err
}

// bidiForSession returns the BidiRPC registered on exactly this session.
// CallViaBidi uses it so the session it grades is the one the call leaves on;
// GetBidiRPC resolves the node's session again and may pick a sibling.
func (m *ConnectionManager) bidiForSession(sess aether.Session) (*BidiRPC, bool) {
	m.dispatchMu.RLock()
	defer m.dispatchMu.RUnlock()
	bidi, ok := m.bidisBySession[sess]
	return bidi, ok
}

// evictDeadBidiForSession removes the bidi entry for a specific session
// iff it still points to the supplied dead bidi. Identity-matched
// delete protects against a concurrent register having installed a
// fresh, healthy bidi for the same session between the dead detection
// and this eviction.
func (m *ConnectionManager) evictDeadBidiForSession(sess aether.Session, dead *BidiRPC) {
	m.dispatchMu.Lock()
	defer m.dispatchMu.Unlock()
	if m.bidisBySession == nil {
		return
	}
	if cur, ok := m.bidisBySession[sess]; ok && cur == dead {
		delete(m.bidisBySession, sess)
	}
}

// FindRoutes computes up to 3 routes to a handler's role.
// Returns routes sorted by quality (direct first, then relayed).
// Implements dispatch.SessionFinder.FindRoutes.
func (m *ConnectionManager) FindRoutes(ctx context.Context, role, handler string) []aether.ProbeRoute {
	if m == nil {
		return nil
	}
	targetNodeID := m.findNodeForRole(ctx, role, handler)
	if targetNodeID == "" {
		return nil
	}

	selfID := string(m.selfID)
	var routes []aether.ProbeRoute

	// Route 1: Direct session to target (if available)
	if session, ok := m.GetMeshSession(targetNodeID); ok && !session.IsClosed() {
		routes = append(routes, aether.ProbeRoute{
			Session:      session,
			NodeID:       targetNodeID,
			TargetNodeID: targetNodeID,
			RouteList:    nil, // direct — no intermediate hops
		})
	}

	// Route 2+3: Via relay nodes that have latency to the target
	if m.rt.cache != nil {
		allLatency := m.rt.cache.Latency("")
		type relayCandidate struct {
			nodeID string
			rttMs  int64
		}
		var relays []relayCandidate
		for _, lat := range allLatency {
			var relay string
			if lat.ToNode == targetNodeID && lat.FromNode != selfID && lat.FromNode != targetNodeID {
				relay = lat.FromNode
			} else if lat.FromNode == targetNodeID && lat.ToNode != selfID && lat.ToNode != targetNodeID {
				relay = lat.ToNode
			}
			if relay == "" {
				continue
			}
			// Must have a live session to the relay
			if session, ok := m.GetMeshSession(relay); ok && !session.IsClosed() {
				relays = append(relays, relayCandidate{nodeID: relay, rttMs: lat.RTTMs})
			}
		}

		// Deduplicate and pick best 2 relays (lowest RTT)
		seen := map[string]bool{}
		for _, r := range relays {
			if seen[r.nodeID] {
				continue
			}
			seen[r.nodeID] = true
			session, _ := m.GetMeshSession(r.nodeID)
			routes = append(routes, aether.ProbeRoute{
				Session:      session,
				NodeID:       r.nodeID,
				TargetNodeID: targetNodeID,
				RouteList:    []string{targetNodeID},
			})
			if len(routes) >= 3 {
				break
			}
		}
	}

	// If no routes yet, try any session as blind relay
	if len(routes) == 0 {
		m.dispatchMu.RLock()
		for id, session := range m.meshSessions {
			if id != selfID && !session.IsClosed() {
				routes = append(routes, aether.ProbeRoute{
					Session:      session,
					NodeID:       id,
					TargetNodeID: targetNodeID,
					RouteList:    []string{targetNodeID},
				})
				if len(routes) >= 2 {
					break
				}
			}
		}
		m.dispatchMu.RUnlock()
	}

	// Sort routes by composite score, not
	// positional order. Without this, a high-RTT cross-region direct
	// path would be returned first; parallel probes in HWPCaller.Call
	// would then let a low-RTT same-region relay beat the direct path,
	// cancel the direct stream mid-flight, and train LAD freshness
	// toward the relay. Score = (first-hop RTT or grade synthetic) +
	// 100ms per intermediate hop, so a healthy direct path wins as
	// long as its RTT is comparable to the relay's combined legs.
	if len(routes) > 1 && m.rt != nil && m.rt.cache != nil {
		rttByPeer := make(map[string]int64, len(routes))
		for _, lat := range m.rt.cache.Latency("") {
			if lat.FromNode != selfID || lat.RTTMs <= 0 {
				continue
			}
			if prev, ok := rttByPeer[lat.ToNode]; !ok || lat.RTTMs < prev {
				rttByPeer[lat.ToNode] = lat.RTTMs
			}
		}
		sortProbeRoutesByScore(routes, rttByPeer)
	}

	return routes
}

// findNodeForRole returns the NodeID of a peer that serves the given role
// and, when handler is non-empty, advertises that handler. When multiple
// peers qualify, picks the one with the lowest measured latency to the
// local node (load-aware routing); ties break on grade.
//
// Sourced from the swarm RoleTable; returns "" when the swarm subsystem
// isn't initialized yet, no peer advertises the role, or no peer of the
// role advertises the handler. A peer's record lists every handler it
// executes (PeerPublisher), so a peer of the role whose record lacks the
// handler refuses it as not found when targeted (RPCServer), and peers
// sharing a role serve different handlers of it. On "" FindRoutes offers no
// route and FindSession offers a forwarding session, so the request leaves
// with no target and a receiving node's forwarder resolves it.
func (m *ConnectionManager) findNodeForRole(ctx context.Context, role, handler string) string {
	if m.rt.swarm == nil || m.rt.swarm.RoleTable == nil {
		return ""
	}

	nodes := m.rt.swarm.RoleTable.Lookup(role, handler)
	if len(nodes) == 0 {
		return ""
	}
	if len(nodes) == 1 {
		return nodes[0].NodeID
	}
	_ = ctx

	// Pick the node with lowest latency from us
	selfID := string(m.selfID)
	bestNode := nodes[0].NodeID
	bestRTT := m.rt.cache.LatencyBetween(selfID, nodes[0].NodeID)
	bestGrade := m.rt.peerGrade(nodes[0].NodeID)

	for _, node := range nodes[1:] {
		rtt := m.rt.cache.LatencyBetween(selfID, node.NodeID)
		switch {
		case rtt > 0 && bestRTT == 0:
			// New candidate has measured latency; previous best did
			// not. Prefer the measured one.
			bestRTT = rtt
			bestNode = node.NodeID
			bestGrade = m.rt.peerGrade(node.NodeID)
		case rtt > 0 && rtt < bestRTT:
			bestRTT = rtt
			bestNode = node.NodeID
			bestGrade = m.rt.peerGrade(node.NodeID)
		case rtt == 0 && bestRTT == 0:
			// Neither node has a latency record. Rather than
			// falling through to bestNode=nodes[0] alphabetically,
			// tiebreak on the locally-measured connection grade
			// to that peer. A peer we're already connected to at
			// Grade A beats an unknown one even without latency data.
			if g := m.rt.peerGrade(node.NodeID); g > bestGrade {
				bestGrade = g
				bestNode = node.NodeID
			}
		}
	}

	return bestNode
}
