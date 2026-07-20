/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"math"
	"sync"
	"time"

)

// Grade is the connection quality tier from the transport package.
// Type alias so all node-package consumers use Grade transparently.

// Re-export grade constants so existing node-package code compiles unchanged.
const (
)

// ProtocolForGrade maps a connection grade back to the node Protocol used to dial.
// Inverse of GradeForProtocol. Used by TopologyRouter to determine which
// protocol to dial when routing via a direct Grade A/B connection.
func ProtocolForGrade(g Grade) Protocol {
	switch g {
	case GradeA:
		return ProtoNoiseUDP
	case GradeB:
		return ProtoQUIC
	default:
		return ProtoWebSocket // shouldn't be reached for A/B routing
	}
}

// GradeForProtocol maps a node transport protocol to its connection grade.
func GradeForProtocol(p Protocol) Grade {
	switch p {
	case ProtoNoiseUDP:
		return GradeA
	case ProtoQUIC:
		return GradeB
	case ProtoWebSocket:
		return GradeC
	case ProtoTLS:
		return GradeC // TLS bootstrap: same transport quality as WebSocket (both TCP-based)
	case ProtoGRPC:
		return GradeB // HTTP/2: native reliability, flow control, congestion, mux, encryption
	default:
		return GradeF
	}
}

// ConnectionEvent represents a change in connection state or grade.
type ConnectionEvent struct {
	PeerNodeID string
	OldGrade   Grade
	NewGrade   Grade
	Transport  string
	Reason     string    // "connected", "upgraded", "downgraded", "disconnected"
	Timestamp  time.Time
}

// ConnectionEventHandler is called when connection state changes.
type ConnectionEventHandler func(event ConnectionEvent)

// ConnectionScaler computes target connections per peer and manages rebalancing.
type ConnectionScaler struct {
	mu            sync.Mutex
	connMgr       *ConnectionManager
	eventHandler  ConnectionEventHandler
	subscribers   []ConnectionEventHandler // fan-out to additional listeners
	eventLog      *ConnectionEventLog
	rpcCounters   map[string]*rpcCounter // peerNodeID -> recent RPC tracking
	connectionMap *ConnectionMap         // gossip-propagated global connection counts (used to detect hotspots)
}

// AddEventSubscriber registers an additional handler that fires alongside
// the primary eventHandler. Used by the Runtime to emit LAD latency/member
// records on state changes without replacing the default logger handler.
// Not ordered — don't rely on fire order between subscribers.
func (s *ConnectionScaler) AddEventSubscriber(h ConnectionEventHandler) {
	if h == nil {
		return
	}
	s.mu.Lock()
	s.subscribers = append(s.subscribers, h)
	s.mu.Unlock()
}

// rpcCounter tracks RPC calls in a rolling window.
type rpcCounter struct {
	mu    sync.Mutex
	calls []time.Time
}

// rpcsLastMinute returns RPCs in the last minute.
func (r *rpcCounter) rpcsLastMinute() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	cutoff := time.Now().Add(-1 * time.Minute)
	count := 0
	for _, t := range r.calls {
		if t.After(cutoff) {
			count++
		}
	}
	return count
}

// record adds an RPC call timestamp.
func (r *rpcCounter) record() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, time.Now())
	// Prune old entries
	cutoff := time.Now().Add(-1 * time.Minute)
	for len(r.calls) > 0 && r.calls[0].Before(cutoff) {
		r.calls = r.calls[1:]
	}
}

// NewConnectionScaler creates a scaler bound to a ConnectionManager.
func NewConnectionScaler(connMgr *ConnectionManager, handler ConnectionEventHandler) *ConnectionScaler {
	return &ConnectionScaler{
		connMgr:      connMgr,
		eventHandler: handler,
		eventLog:     NewConnectionEventLog(),
		rpcCounters:  make(map[string]*rpcCounter),
	}
}

// pruneCountersFor removes the per-peer rpc counter entries for any
// peer where the predicate returns true. Paired with the
// ConnectionManager.pruneStalePeers() sweep so the scaler's satellite
// map shrinks alongside the main peers map. Drop counters live in the
// shared quality tracker which has its own ForgetPeer hook.
func (s *ConnectionScaler) pruneCountersFor(shouldPrune func(peerNodeID string) bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id := range s.rpcCounters {
		if shouldPrune(id) {
			delete(s.rpcCounters, id)
		}
	}
}

// TargetConnections computes the ideal number of connections for a peer
// using the formula from the redesign doc:
//
//	TargetConns = clamp(
//	    ceil(BaseConns * LatencyFactor * GradePenalty * FailureFactor * TrafficWeight),
//	    MinPerPeer,
//	    MaxPerPeer
//	)
//
// peerRegion must be supplied by the caller. It is passed in rather than
// read from peer.peerRegion because Rebalance snapshots peer state under
// m.mu (avoiding a data race with the writers in peer_connections.go);
// non-Rebalance callers hold m.mu directly and can pass peer.peerRegion.
func (s *ConnectionScaler) TargetConnections(peer *peerConn, peerRegion string) int {
	// LatencyFactor: 1 + ln(1 + RTTms/50)
	// Range: 1ms -> 1.0, 50ms -> 1.7, 150ms -> 2.2, 500ms -> 3.0
	rttMs := float64(0)
	if peer.lastRTT > 0 {
		rttMs = float64(peer.lastRTT.Milliseconds())
	}
	latencyFactor := 1.0 + math.Log(1.0+rttMs/50.0)

	// GradePenalty: A=1.0, B=1.2, C=1.5, F=2.0.
	//
	// Audit M10: use peer.bestActiveGrade() instead of
	// GradeForProtocol(peer.protocol). The single peer.protocol field
	// only reflects the last-accepted transport — for a peer with
	// Noise-UDP + WS both active, it can be either depending on which
	// accept ran most recently. The grade penalty wants "what's the
	// best path I have", not "what's the last one I touched".
	grade := peer.bestActiveGrade()
	gradePenalty := grade.Weight()

	// FailureFactor scales the target up when a peer is unreliable so
	// extra connections absorb future drops without leaving the peer
	// disconnected. Sourced from the shared quality tracker's
	// reliability EMA across all transports for this peer; no separate
	// per-peer drop bookkeeping. Mapping: a perfectly-reliable peer
	// (reliability=1.0) gives factor 1.0; a peer whose recent close
	// stream is mostly errors (reliability=0.0) gives factor 3.0.
	failureFactor := 1.0
	if s.connMgr.qualityTracker != nil {
		unreliability := 1.0 - s.connMgr.qualityTracker.PeerReliability(peer.nodeID)
		if unreliability < 0 {
			unreliability = 0
		}
		failureFactor = math.Min(1.0+unreliability*2.0, 3.0)
	}

	// TrafficWeight: 1 + ln(1 + RPCsPerMin/10)
	rpcsPerMin := float64(0)
	s.mu.Lock()
	if rc, ok := s.rpcCounters[peer.nodeID]; ok {
		rpcsPerMin = float64(rc.rpcsLastMinute())
	}
	s.mu.Unlock()
	trafficWeight := 1.0 + math.Log(1.0+rpcsPerMin/10.0)

	target := math.Ceil(1.0 * latencyFactor * gradePenalty * failureFactor * trafficWeight)

	// Role affinity computed up front so the per-peer max can absorb the
	// bonus without being silently clamped away. A peer carrying a role
	// we care about (especially same-region + unique-capability, the
	// +3 case) is exactly the peer whose maxPerPeer ceiling should rise
	// to let the extra paths land. The default MaxPerPeer=2 against a
	// +3 bonus otherwise collapses to "no change vs a peer with no
	// roles" — the bonus is computed, telemetry-recorded, then clipped
	// at the existing ceiling. EffectiveMaxPerPeer is the per-peer
	// budget (MaxPerPeer + cross-region bonus); the maxTotal cap on
	// ConnectionBudget still bounds the fleet-wide footprint.
	affinity := s.connMgr.peerRoleAffinity(peer.nodeID, peerRegion)
	s.connMgr.roleAffinityStat.recordRebalanceObservation(affinity)

	maxPerPeer := s.connMgr.budget.EffectiveMaxPerPeer(peer.crossRegion) + affinity.total()/2
	minPerPeer := s.connMgr.budget.MinPerPeer

	// Critical peers (anchors/bootstrap) get minimum 2 connections for redundancy.
	// If one connection drops, gossip continues on the other — no mesh disconnect.
	if peer.priority == PriorityCritical && minPerPeer < 2 {
		minPerPeer = 2
	}

	result := int(target)
	if result < minPerPeer {
		result = minPerPeer
	}
	if result > maxPerPeer {
		result = maxPerPeer
	}

	// Apply the additive affinity bonus on top of the base target. The
	// integer-division half of affinity.total() already lifted the
	// per-peer ceiling above; the full bonus stacks here so a +3 peer
	// (same-region + unique-capability) lands at base+3 capped by the
	// raised maxPerPeer. Captured here BEFORE adjustForGlobalBalance
	// so hotspot reduction can still trim affinity-boosted targets —
	// affinity is a preference, not an override of the global balancer.
	if bonus := affinity.total(); bonus > 0 {
		result += bonus
		if result > maxPerPeer {
			result = maxPerPeer
		}
	}

	// Adjust for global balance — reduce target if the peer is a hotspot
	// according to the gossip-propagated connection map.
	result = s.adjustForGlobalBalance(peer.nodeID, result)

	// Universal K>=2 floor for forever-sessions design: every peer gets
	// at least one standby path so the keepalive failure of any single
	// path can fail over instantly via the multipath manager's
	// OnPrimaryFailure instead of triggering a full reconnect. The
	// dispatch registry swap + bidiRPC eviction in the mesh_connection
	// cleanup path means the logical session survives transport
	// instance death whenever a standby exists. Previously this floor
	// was gated on (bestActiveGrade < GradeA && peerHasBetterGradeAddress)
	// which only fired for low-grade peers with a known upgrade
	// candidate — leaving same-grade-only peers and already-Grade-A
	// peers at K=1, no standby, and a single transient packet-loss
	// burst away from a teardown. Bounded by maxPerPeer so a strict
	// budget still wins.
	if result < 2 && result < maxPerPeer {
		result = 2
	}

	return result
}

// saturationCap limits a computed target when the peer asked us to back off: it
// won't grow beyond what we already hold, but never drops below the K>=2 standby
// floor (a saturated peer keeps its failover path). Pure so callers apply it
// under whatever lock state they already hold; the backoff decision is computed
// by the caller (which knows whether it holds m.mu).
func saturationCap(target, connCount int, backoff bool) int {
	if !backoff {
		return target
	}
	limit := connCount
	if limit < 2 {
		limit = 2
	}
	if target > limit {
		return limit
	}
	return target
}

// adjustForGlobalBalance reduces the target connection count if the peer is a
// hotspot according to the gossip-propagated connection map. If the peer has
// significantly more connections than the mesh average (>2x), the target is
// reduced by 1 to steer new connections toward less-loaded nodes.
func (s *ConnectionScaler) adjustForGlobalBalance(peerNodeID string, rawTarget int) int {
	if s.connectionMap == nil {
		return rawTarget
	}
	if s.connectionMap.IsHotspot(peerNodeID) && rawTarget > s.connMgr.budget.MinPerPeer {
		conns, _ := s.connectionMap.ConnectionCount(peerNodeID)
		dbgScaler.Printf("Reducing target for hotspot %s (%d conns, mesh avg %.1f)",
			truncID(peerNodeID), conns, s.connectionMap.MeshAverage())
		return max(rawTarget-1, s.connMgr.budget.MinPerPeer)
	}
	return rawTarget
}

// RecordRPC records an RPC call to a peer (for TrafficWeight).
func (s *ConnectionScaler) RecordRPC(peerNodeID string) {
	s.mu.Lock()
	rc, ok := s.rpcCounters[peerNodeID]
	if !ok {
		rc = &rpcCounter{}
		s.rpcCounters[peerNodeID] = rc
	}
	s.mu.Unlock()
	rc.record()
}

// EmitEvent fires a connection event to the registered handler and appends
// it to the event log for historical queries. Fans out to any additional
// subscribers registered via AddEventSubscriber.
func (s *ConnectionScaler) EmitEvent(event ConnectionEvent) {
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}
	if s.eventLog != nil {
		s.eventLog.Append(event)
	}
	if s.eventHandler != nil {
		s.eventHandler(event)
	}
	s.mu.Lock()
	subs := make([]ConnectionEventHandler, len(s.subscribers))
	copy(subs, s.subscribers)
	s.mu.Unlock()
	for _, h := range subs {
		h(event)
	}
	dbgScaler.Printf("%s: %s grade=%s->%s transport=%s",
		truncID(event.PeerNodeID), event.Reason, event.OldGrade, event.NewGrade, event.Transport)
}

// EventLog returns the connection event log for historical queries.
func (s *ConnectionScaler) EventLog() *ConnectionEventLog {
	return s.eventLog
}

// Rebalance evaluates all peers and adjusts connection counts.
// Called periodically from ConnectionManager.Start().
// Returns a list of peers that should be drained (current > target).
func (s *ConnectionScaler) Rebalance() []DrainCandidate {
	// Bump rebalance tick on the role-affinity telemetry so operators
	// can compare per-tier hit counts against the total number of
	// affinity scoring opportunities.
	s.connMgr.roleAffinityStat.rebalanceTicks.Add(1)
	// Snapshot peer pointers and their connCount + peerRegion while holding
	// m.mu to avoid data races on fields the writers in peer_connections.go
	// mutate without coordination from the scaler. connCount is written by
	// startGossip/closeDrainedConnection; peerRegion by scanAndConnect's
	// PeerRecord absorbers. TargetConnections receives peerRegion explicitly
	// so the sameRegion role-affinity tier reads the snapshotted value.
	type peerSnap struct {
		peer         *peerConn
		connCount    int
		peerRegion   string
		wantsBackoff bool
	}
	now := time.Now()
	s.connMgr.mu.Lock()
	snaps := make([]peerSnap, 0, len(s.connMgr.peers))
	for _, p := range s.connMgr.peers {
		if p.state == PeerConnected {
			snaps = append(snaps, peerSnap{
				peer:         p,
				connCount:    p.connCount,
				peerRegion:   p.peerRegion,
				wantsBackoff: s.connMgr.peerWantsBackoff(p, now),
			})
		}
	}
	s.connMgr.mu.Unlock()

	var candidates []DrainCandidate

	for _, snap := range snaps {
		peer := snap.peer
		current := snap.connCount
		// MESH-C01: TargetConnections reads peer.bestActiveGrade(), which
		// iterates peer.transports. Rebalance runs after releasing m.mu, so a
		// concurrent addTransport/removeTransport made this a `fatal error:
		// concurrent map read and map write` that crashes the whole node. Hold
		// m.mu across the call — the multipath_dial.go callers already do.
		s.connMgr.mu.Lock()
		target := s.TargetConnections(peer, snap.peerRegion)
		s.connMgr.mu.Unlock()
		// Saturation back-off: a peer asking us to stop opening paths is not
		// grown beyond what we already hold, but keeps its K>=2 standby.
		target = saturationCap(target, current, snap.wantsBackoff)

		if current < target {
			dbgScaler.Printf("%s: scaling up %d -> %d connections", truncID(peer.nodeID), current, target)
			// Additional connections will be attempted on next scan cycle.
			// ConnectionManager.scanAndConnect handles the actual dial.
		} else if current > target {
			// Never drain when this peer has ANY Grade-A transport
			// active — they're the fastest path and should be
			// preserved even if the scaler thinks we're over budget.
			//
			// L1 #5 fix: previously this used GradeForProtocol(peer.protocol),
			// but peer.protocol is a single field that holds the
			// last-accepted protocol. A rejected duplicate accept
			// could have flipped it to a lower grade temporarily
			// (the rollback for Cascade A.1 closes that hole, but
			// peer.protocol is still a one-transport-of-many view).
			// bestActiveGrade() reflects the truth across all active
			// transports.
			// MESH-C01: guard the transports-map read (see above).
			s.connMgr.mu.Lock()
			isGradeA := peer.bestActiveGrade() == GradeA
			s.connMgr.mu.Unlock()
			if isGradeA {
				continue
			}

			// Preserve cross-class redundancy within Grade C: when the peer
			// has BOTH a WebSocket transport AND a TLS-bootstrap transport
			// active, keep both. They fail under different conditions (WS
			// is sensitive to HTTP/2 proxy idle timeouts and frame-level
			// resets; TLS bootstrap runs over a hijacked HTTPS socket with
			// its own keepalive) so they genuinely provide failover, not
			// redundancy. Draining either one produces a 60-second loop:
			// connectToPeer re-walks protocolOrder, the dropped class
			// comes back up, the scaler sees current > target, drains
			// the sibling. Observed on cross-region Fly pairs where the
			// grade-A UDP path is unavailable.
			// L1 #9 gate: only preserve dual Grade-C when there is
			// NO better grade active. With noise-UDP up, the WS+TLS
			// duo is redundant; the scaler should be allowed to drain
			// the WS/TLS sibling instead of preserving both.
			s.connMgr.mu.Lock()
			distinctClasses := peerDistinctGradeCTransportClasses(peer)
			haveBetterThanC := peer.bestActiveGrade() > GradeC
			s.connMgr.mu.Unlock()
			if !haveBetterThanC && distinctClasses >= 2 && current <= distinctClasses {
				dbgScaler.Printf("%s: %d grade-C transport classes active; preserving for failover",
					truncID(peer.nodeID), distinctClasses)
				continue
			}

			dbgScaler.Printf("%s: scaling down %d -> %d connections", truncID(peer.nodeID), current, target)

			// Build one ConnectionInfo per active transport. The
			// original code synthesised N copies of a single
			// peer-level ConnectionInfo carrying peer.protocol — the
			// last-accepted transport — which collapsed all multipath
			// drain decisions to the most recent upgrade. With a fresh
			// noise-UDP path that ALWAYS won the "newest" tiebreak in
			// shouldDrainFirst, the scaler ended up draining the very
			// path the upgrade walker had just installed. Sourcing per-
			// transport ConnectionInfo from peer.transports lets
			// shouldDrainFirst sort by real per-class grade and age,
			// so the lower-grade WS gets drained and the noise-UDP path
			// is preserved.
			s.connMgr.mu.Lock()
			conns := s.connMgr.connectionInfoPerTransport(peer)
			s.connMgr.mu.Unlock()
			if len(conns) == 0 {
				continue
			}

			toDrain := SelectForDrain(conns, target)
			for _, c := range toDrain {
				candidates = append(candidates, DrainCandidate{
					PeerNodeID: c.PeerNodeID,
					Transport:  c.Transport,
				})
			}
		}
	}

	// Trigger draining for selected connections
	if len(candidates) > 0 && s.connMgr.drainMgr != nil {
		for _, c := range candidates {
			s.connMgr.drainMgr.StartDrain(c.PeerNodeID, c.Transport, "scale_down")
		}
	}

	return candidates
}

// DrainCandidate identifies a connection selected for draining.
type DrainCandidate struct {
	PeerNodeID string
	Transport  string
}
