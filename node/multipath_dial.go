/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/multipath"
	"github.com/ORBTR/aether/quality"
)

// installEnsureKDialFn wires the per-peer multipath manager's EnsureK
// reconnect loop with a DialFunc that adds an additional path to the
// peer using a protocol not currently represented in the manager.
//
// scanAndConnect still handles the initial dial when a peer is first
// discovered (one path per peer). EnsureK then keeps K total paths
// alive by dialing whichever transport from protocolOrder is missing
// and not currently in dial cooldown. K comes from
// ConnectionScaler.TargetConnections — same latency × grade × failure
// × traffic-weight × hotspot formula, just with a new consumer.
//
// Cooldown and dial-failure suppression are inherited from the shared
// dialWithProtocol primitive that connectPeer uses, so the historical
// safeguards are not duplicated. refreshEnsureKTargets re-applies K
// every 45 s so changes in RTT / drops / traffic flow into the loop
// without waiting for a new addMultipathSession event.
//
// The dial callback runs on the manager's own goroutine (one per peer)
// so it doesn't need to coordinate with scanAndConnect's parallel
// dialer beyond the existing per-protocol cooldown — that already
// serialises outbound attempts to a given (peer, protocol) tuple.
func (m *ConnectionManager) installEnsureKDialFn(mgr *multipath.Manager, nodeID string) {
	// Initial K target. refreshEnsureKTargets handles steady-state
	// updates; this just primes the loop.
	target := 0
	m.mu.Lock()
	peer, ok := m.peers[nodeID]
	if ok && m.scaler != nil {
		target = m.scaler.TargetConnections(peer, peer.peerRegion)
		target = saturationCap(target, peer.connCount, m.peerWantsBackoff(peer, time.Now()))
	}
	m.mu.Unlock()

	if target < 1 {
		target = 1
	}

	cfg := multipath.QualityConfig{
		Tracker:     m.qualityTracker,
		LocalRegion: m.selfRegion,
		PeerID:      nodeID,
		Weights:     defaultQualityWeights(),
	}
	m.attachDialFn(mgr, nodeID, &cfg)
	// L4 #8 fix: pass the runtime context (cancelled on shutdown) so
	// EnsureK's internal goroutine exits cleanly. Previously
	// context.Background() was used, leaving goroutines outliving the
	// ConnectionManager teardown — risk of use-after-free on the half-
	// torn-down map state, plus a benign goroutine leak.
	ctx := m.rt.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	if err := mgr.EnsureK(ctx, cfg, target); err != nil {
		log.Printf("[multipath-dial] %s: EnsureK setup failed: %v", truncID(nodeID), err)
	}
}

// pathProtos returns the set of protocols currently registered with the
// manager — across active, standby, AND dead states (but excluding
// closed sessions). Used by the EnsureK dial callback to avoid
// re-dialing a protocol that already has a session — instead it picks
// the next protocol in protocolOrder that's missing.
//
// L4 #12 fix: the previous implementation used AllSessions() which
// excludes PathDead. A transiently dead path's proto was missing from
// the "already represented" set, so EnsureK would repeatedly re-dial
// the same broken transport — racing the still-extant Dead-state Path
// on the other side and producing fresh dedup races. Using
// AllProtocolsRepresented lets EnsureK skip that protocol and pick
// the next one instead. The dead path itself will be resurrected by
// the manager's probe loop (Cascade A.3) when its session-level Ping
// confirms recovery.
func pathProtos(mgr *multipath.Manager) map[Protocol]struct{} {
	out := make(map[Protocol]struct{})
	for _, proto := range mgr.AllProtocolsRepresented() {
		out[unmapProtocol(proto)] = struct{}{}
	}
	return out
}

// unmapProtocol is the inverse of mapProtocol — converts an aether
// transport protocol back to the Library's Protocol type. Both are
// numeric enums but defined in separate packages so the cast isn't
// implicit.
func unmapProtocol(p aether.Protocol) Protocol {
	switch p {
	case aether.ProtoNoise:
		return ProtoNoiseUDP
	case aether.ProtoQUIC:
		return ProtoQUIC
	case aether.ProtoWebSocket:
		return ProtoWebSocket
	case aether.ProtoGRPC:
		return ProtoGRPC
	case aether.ProtoTCP:
		return ProtoTLS
	default:
		return ProtoNoiseUDP
	}
}

// refreshEnsureKTargets walks the multipath managers and re-applies
// scaler.TargetConnections so RTT spikes, drop accumulation, hotspot
// adjustments, and traffic-weight changes propagate into each per-peer
// reconnect loop. Without it, K is set once per session register and
// stuck — a peer whose conditions degrade mid-session would never see
// K reduced, and one whose traffic grows wouldn't get more redundancy
// until reconnect.
//
// Cheap: O(peers), no I/O, just per-peer lock acquisitions and an
// EnsureK call that atomically swaps the target without disturbing
// the dial loop. Called from the main ticker at 45 s cadence — fast
// enough to track live conditions, slow enough not to thrash on noise.
func (m *ConnectionManager) refreshEnsureKTargets() {
	if m.qualityTracker == nil {
		return
	}

	// Snapshot the manager set under multipathMu to avoid holding it
	// while iterating (EnsureK acquires manager-internal locks). We
	// only need the keys — the managers themselves are stable
	// pointers for as long as a peer has an entry.
	m.multipathMu.Lock()
	keys := make([]string, 0, len(m.multipathManagers))
	mgrs := make([]*multipath.Manager, 0, len(m.multipathManagers))
	for k, mgr := range m.multipathManagers {
		keys = append(keys, k)
		mgrs = append(mgrs, mgr)
	}
	m.multipathMu.Unlock()

	for i, nodeID := range keys {
		m.mu.Lock()
		peer, ok := m.peers[nodeID]
		var target int
		if ok && m.scaler != nil {
			target = m.scaler.TargetConnections(peer, peer.peerRegion)
			target = saturationCap(target, peer.connCount, m.peerWantsBackoff(peer, time.Now()))
		}
		m.mu.Unlock()
		if target < 1 {
			target = 1
		}

		cfg := multipath.QualityConfig{
			Tracker:     m.qualityTracker,
			LocalRegion: m.selfRegion,
			PeerID:      nodeID,
			Weights:     defaultQualityWeights(),
			DialFunc:    nil, // unchanged — EnsureK only updates DialFunc when non-nil
		}
		// Re-attach the dial fn so callers calling EnsureK with
		// DialFunc=nil don't accidentally clobber the existing one.
		// installEnsureKDialFn captures `nodeID` and `m` in its
		// closure; reusing it here keeps the dial logic single-
		// sourced.
		m.attachDialFn(mgrs[i], nodeID, &cfg)

		ctx := m.rt.ctx
		if ctx == nil {
			ctx = context.Background()
		}
		_ = mgrs[i].EnsureK(ctx, cfg, target)
	}
}

// attachDialFn populates cfg.DialFunc with the same dial closure
// installEnsureKDialFn uses. Extracted so refreshEnsureKTargets can
// re-apply the closure without a full installEnsureKDialFn call (which
// would also recompute K from scratch — already done by the caller).
func (m *ConnectionManager) attachDialFn(mgr *multipath.Manager, nodeID string, cfg *multipath.QualityConfig) {
	cfg.DialFunc = func(ctx context.Context) (aether.Session, aether.Protocol, string, error) {
		return m.dialAdditionalPath(ctx, mgr, nodeID)
	}
}

// dialAdditionalPath is the body of installEnsureKDialFn's dialFn,
// extracted so refreshEnsureKTargets can rebuild a config carrying the
// same closure without duplicating the logic. Returns the same
// (session, proto, region, err) tuple multipath.QualityConfig.DialFunc
// expects.
func (m *ConnectionManager) dialAdditionalPath(ctx context.Context, mgr *multipath.Manager, nodeID string) (aether.Session, aether.Protocol, string, error) {
	m.mu.Lock()
	peer, ok := m.peers[nodeID]
	if !ok {
		m.mu.Unlock()
		return nil, 0, "", nil
	}
	// Deterministic dial ownership governs the multipath dialer too: only
	// the lower-nodeID side of a pair dials extra paths; the higher-nodeID
	// side accepts them. connectPeer/scanAndConnect is already
	// ownership-gated (dialOwned) — but EnsureK was not, so the higher
	// side's multipath dialer kept racing the lower side's dials into
	// cross-initiator session pairs. registerMeshSession then has to
	// tie-break each pair, and the loser is closed — its peer-end sees the
	// close, redials, and the race repeats. Whenever a (cross-region) WS
	// session drops, that loop re-ignites fleet churn. Gating EnsureK here
	// closes the last unowned dial path: the higher-nodeID side now purely
	// accepts, the lower side dials every path, so no cross-initiator pair
	// is ever created. The grace fallback inside dialOwned still lets the
	// higher side take over if the owner is genuinely gone.
	if !m.dialOwned(peer, time.Now()) {
		m.mu.Unlock()
		return nil, 0, "", nil
	}
	activeProtos := pathProtos(mgr)
	// Snapshot dispatch grade so the proto-selection loop can require
	// strict upgrades from the start (L1 #7 fix). Previously the loop
	// picked the first missing+unsuppressed proto then bailed AFTER
	// the dialWithProtocol call if the chosen proto wasn't a grade
	// upgrade — a wasted iteration. Now we filter in the loop.
	m.dispatchMu.RLock()
	dispSnap, hasDispSnap := m.meshSessions[nodeID]
	m.dispatchMu.RUnlock()
	var dispGrade Grade
	if hasDispSnap && dispSnap != nil && !dispSnap.IsClosed() {
		dispGrade = SessionGrade(dispSnap)
	}
	var chosen Protocol
	var found bool
	peerNodeID := peer.nodeID
	for _, proto := range protocolOrder {
		if _, alreadyActive := activeProtos[proto]; alreadyActive {
			continue
		}
		// tryDial consumes a probe slot — call only at the actual
		// dial gate so probe allowances aren't wasted on observability
		// peeks. Audit M12.
		if !m.tryDial(peerNodeID, proto) {
			continue
		}
		// Strict-upgrade requirement: skip protos that don't beat the
		// current dispatch grade. If no dispatch session exists yet,
		// dispGrade is zero (GradeF) and every transport qualifies.
		if hasDispSnap && GradeForProtocol(proto) <= dispGrade {
			continue
		}
		chosen = proto
		found = true
		break
	}
	peerRegion := peer.peerRegion
	m.mu.Unlock()

	if !found {
		return nil, 0, "", nil
	}

	// Fix A — only dial a path that UPGRADES the peer's grade. The
	// multipath manager wants K paths, but registerMeshSession enforces
	// ONE dispatch session per peer and dedups same-grade duplicates by
	// closing one — and that close cascades to the peer (readLoop EOF →
	// both sides redial), which is the fleet churn loop. A lateral
	// dial (e.g. WS→TLS, both Grade C) only feeds that loop. So if a
	// healthy dispatch session already exists and `chosen` is not a
	// strictly better grade, skip the dial. Genuine grade diversity
	// (adding Noise-UDP/A beside a WS/C primary) is still allowed —
	// that's the multipath value; same-grade duplication is not.
	m.dispatchMu.RLock()
	disp, hasDisp := m.meshSessions[nodeID]
	// L3 #11: consult dedup-reject ONLY for the proto we're about to
	// dial. The previous nodeID-keyed lookup blanket-blocked all
	// protocols for 3 minutes when ANY proto had a recent reject.
	dedupRejectedAt := m.dedupRejectAtFor(nodeID, mapProtocol(chosen).String())
	m.dispatchMu.RUnlock()

	// Dedup-reject back-off — the non-churning half of the fix. If
	// registerMeshSession recently rejected a session for this peer as a
	// duplicate, the peer demonstrably already has a healthy session.
	// Re-dialing now just rebuilds the same duplicate, which is rejected
	// and closed again — the reject→close→redial loop that is fleet
	// churn. The grade gate below catches the steady-state case, but it
	// races a session that is mid-establishment (meshSessions briefly
	// empty at check time). This timestamp closes that race: any reject,
	// from any path, suppresses the next dedupRejectCooldown of EnsureK
	// dials so the loop cannot self-sustain.
	if !dedupRejectedAt.IsZero() && time.Since(dedupRejectedAt) < dedupRejectCooldown {
		dbgPeers.Printf("%s: EnsureK skip %s — dedup-reject cooldown (%.0fs left)",
			truncID(nodeID), chosen, (dedupRejectCooldown - time.Since(dedupRejectedAt)).Seconds())
		return nil, 0, "", nil
	}

	if hasDisp && disp != nil && !disp.IsClosed() {
		if GradeForProtocol(chosen) <= SessionGrade(disp) {
			dbgPeers.Printf("%s: EnsureK skip %s — not a grade upgrade over dispatch grade %d",
				truncID(nodeID), chosen, SessionGrade(disp))
			return nil, 0, "", nil
		}
	}

	dialedConn, err := m.dialWithProtocol(ctx, peer, chosen)
	if err != nil {
		m.recordDialFailure(peerNodeID, chosen)
		// A direct noise-UDP dial failed but the peer has a working
		// (lower-grade) dispatch session we can signal over — attempt a
		// coordinated hole-punch before giving up on noise-UDP. On
		// success the punched session is registered asynchronously, the
		// same as a normal dial; the EnsureK loop sees the new path on
		// its next pass. On failure the peer keeps its existing path.
		if chosen == ProtoNoiseUDP && hasDisp && !m.rt.cfg.VL1.HolePunchDisabled {
			if m.attemptHolePunch(ctx, peerNodeID, peerRegion) {
				return nil, 0, peerRegion, nil
			}
		}
		return nil, 0, "", fmt.Errorf("dial %s: %w", chosen, err)
	}

	m.recordDialSuccess(peerNodeID, chosen)
	m.mu.Lock()
	peer.outboundDialed = true
	peer.reconnectDelay = baseCooldown
	bootstrapHost := peer.bootstrapHost
	m.mu.Unlock()

	baseConn, ok := dialedConn.(*aether.BaseConnection)
	if !ok || baseConn == nil {
		return nil, 0, "", fmt.Errorf("dialWithProtocol returned unexpected conn type")
	}
	serviceName := m.peerServiceName(nodeID)
	// MESH-E01: bind the session lifecycle to the long-lived runtime ctx, NOT
	// the DialFunc's `ctx`. `ctx` here is runEnsure's per-dial dialCtx, which is
	// cancelled the instant this function returns — so the async
	// DialAndAcceptMesh (which derives connCtx from it and opens every stream on
	// it) got a dead context after registerMeshSession had already demoted the
	// working primary, producing a ~15s install-then-teardown churn loop. The
	// connectPeer/probeUpgrade dial sites already pass the long-lived ctx.
	go m.rt.DialAndAcceptMesh(m.rt.ctx, baseConn.Conn, nodeID, peerRegion, chosen, bootstrapHost, serviceName, "")
	log.Printf("[multipath-dial] %s: EnsureK dialed %s alive=%d (target watch)",
		truncID(nodeID), chosen, mgr.PathCount())
	return nil, 0, peerRegion, nil
}

// defaultQualityWeights is the central knob for score-component
// weighting. Wraps quality.DefaultWeights so callers in this package
// don't need to import the quality package directly and so a
// configuration-driven override can later be added in one place.
func defaultQualityWeights() quality.Weights {
	return quality.DefaultWeights
}

// adaptiveKeepaliveInterval computes the keepalive ping cadence for a
// session from its measured SRTT: clamp(2 × SRTT, 5s, 30s).
//
// 2 × SRTT gives roughly one round-trip plus jitter between probes.
// The 5 s floor handles fly's edge-proxy idle close (a transport-level
// concern also covered separately by WSConn.pingLoop) and ensures a
// brand-new session doesn't drift more than ~5 s without a probe
// before SRTT converges. The 30 s ceiling keeps healthy stable paths
// from being polled more aggressively than they need to be — a 5 ms
// RTT path pings every 5 s, a stable 1 s RTT path every 2 s capped at
// 5, a hypothetical 15 s satellite path every 30 s.
//
// SRTT 0 (nil session or estimator hasn't converged) returns the 5 s
// floor — same as new sessions before AgeGrace lifts.
func adaptiveKeepaliveInterval(s aether.Session) time.Duration {
	const (
		floor = 5 * time.Second
		cap_  = 30 * time.Second
	)
	if s == nil {
		return floor
	}
	// Health() returns nil before SetHealthMonitor lands on
	// freshly-installed sessions; treat as estimator-unconverged.
	h := s.Health()
	if h == nil {
		return floor
	}
	srtt := h.SRTT()
	if srtt <= 0 {
		return floor
	}
	interval := 2 * srtt
	if interval < floor {
		return floor
	}
	if interval > cap_ {
		return cap_
	}
	return interval
}

// dialKeyFor returns the tracker key for a (peer, transport) tuple.
// Centralised so every caller uses the exact same string format.
func dialKeyFor(peerID string, proto Protocol) string {
	return quality.Key(peerID, mapProtocol(proto).String())
}

// dialIsSuppressed reports whether the (peer, transport) pair is in
// dial cooldown right now. PURE PREDICATE — for observability,
// dashboards, and dual-checks. Sites that are about to actually
// attempt a dial must call tryDial instead, which consumes an
// opportunistic-probe slot. Audit M12.
func (m *ConnectionManager) dialIsSuppressed(peerID string, proto Protocol) bool {
	if m.qualityTracker == nil {
		return false
	}
	suppressed, _ := m.qualityTracker.IsDialSuppressed(dialKeyFor(peerID, proto))
	return suppressed
}

// tryDial returns true if the dialer should be allowed to attempt a
// dial RIGHT NOW for (peerID, proto). Stamps the opportunistic-probe
// timestamp when allowing through during a cooldown (L1 #6) — call
// this exactly once at the start of a dial attempt; otherwise the
// probe slot is wasted on a non-dialing check. Companion to
// dialIsSuppressed (which is the pure predicate form).
func (m *ConnectionManager) tryDial(peerID string, proto Protocol) bool {
	if m.qualityTracker == nil {
		return true
	}
	return m.qualityTracker.TryDialThrough(dialKeyFor(peerID, proto))
}

// recordDialFailure increments the dial failure counter and sets a
// cooldown for the (peer, transport) pair. The tracker handles the
// cooldown growth schedule (30s → 1m → 2m → 4m → ... → 10m cap).
func (m *ConnectionManager) recordDialFailure(peerID string, proto Protocol) {
	if m.qualityTracker == nil {
		return
	}
	m.qualityTracker.RecordDialFailure(dialKeyFor(peerID, proto))
}

// stallCooldown is the post-stall back-off applied to a (peer, transport)
// pair after the aether session-stall detector trips. Short and flat:
// the first few stalls may represent transient asymmetric loss that
// clears in seconds-to-minutes, so the right response is "let the
// working transport carry traffic for a brief window, then re-probe".
// After stallTransientThreshold consecutive stalls on the same protocol
// without an intervening recovery, mesh_connection.go switches to the
// exponential dial-failure schedule (RecordDialFailure) — at that point
// the path is empirically permanently broken (cross-region 6PN UDP with
// loss high enough that the noise reliability layer can never drain its
// window) and continuing to flat-60s-retry just burns a handshake every
// two minutes.
const stallCooldown = 60 * time.Second

// stallTransientThreshold is the per-(peer, protocol) stuck-count above
// which we stop treating stalls as transient and escalate to the
// exponential dial-failure cooldown ladder. Three matches stickyStuck
// Threshold on the sticky-bad-path mechanism and is the smallest value
// that gives confidence the path is genuinely broken rather than a
// one-off loss burst.
const stallTransientThreshold = 3

// recordStallCooldown applies the stall back-off without bumping the
// dial-failure counter. Lets the path re-probe at scanAndConnect's
// 20 s tick once the cooldown expires, so a path that recovers comes
// back into rotation quickly. RecordCooldown preserves any longer
// in-flight cooldown so this never shortens a real failure cooldown.
func (m *ConnectionManager) recordStallCooldown(peerID string, proto Protocol) {
	if m.qualityTracker == nil {
		return
	}
	m.qualityTracker.RecordCooldown(dialKeyFor(peerID, proto), stallCooldown)
}

// recordDialSuccess clears the failure counter and cooldown for the
// (peer, transport) pair after a successful handshake.
func (m *ConnectionManager) recordDialSuccess(peerID string, proto Protocol) {
	if m.qualityTracker == nil {
		return
	}
	m.qualityTracker.RecordDialSuccess(dialKeyFor(peerID, proto))
}

// adaptiveRPCTimeout returns a default RPC deadline derived from the
// session's measured RTT: clamp(20 × SRTT, 5s, 30s).
//
// 20 × SRTT covers a typical RPC round-trip plus retries; the 5 s
// floor protects paths whose RTT estimator hasn't converged yet, the
// 30 s ceiling caps high-RTT paths. Scales the deadline with the
// physical distance the call has to travel so a 250 ms cross-region
// path gets a more generous budget than a 5 ms LAN path.
//
// Callers without a session in hand (e.g. handler-metadata publishing
// where the timeout is advertised, not used) can pass nil to get the
// floor (5 s).
func adaptiveRPCTimeout(s aether.Session) time.Duration {
	const (
		floor = 5 * time.Second
		cap_  = 30 * time.Second
	)
	if s == nil {
		return floor
	}
	// Health() returns nil before SetHealthMonitor lands on
	// freshly-installed sessions; treat as estimator-unconverged.
	h := s.Health()
	if h == nil {
		return floor
	}
	srtt := h.SRTT()
	if srtt <= 0 {
		return floor
	}
	timeout := 20 * srtt
	if timeout < floor {
		return floor
	}
	if timeout > cap_ {
		return cap_
	}
	return timeout
}

// adaptiveKeepaliveTimeout returns the per-ping wait before declaring
// a single keepalive ping failed. Replaces Grade.KeepaliveTimeout-
// FromRTO() with a simpler RFC-6298-style formula:
//
//	timeout = clamp(4 × RTO, floor, 15 s)
//
// 4× RTO gives the path one full retransmit-window worth of slack
// before declaring a probe dead — an honest path under transient loss
// completes within that, a genuinely-broken path doesn't. The 15 s cap
// gives jittery cross-region paths plenty of headroom before a single
// ping is declared lost — a 1–2 s transient spike on a 250 ms-RTT path
// with high jitter previously turned into a "ping failed" event, which
// chained into the death policy. Documented CPU-steal events on
// shared-cpu fly machines can blackhole inbound packet processing for
// 400 ms–1.2 s windows; the 15 s cap means a single such window will
// not consume a strike. Paired with the path-aware death policy in
// mesh_connection.go (maxKeepaliveRetries varies by PathCount), the
// worst-case death budget on a no-standby cross-region path is
// interval + 5 × 15 s + 4 × steadyFloor = 5–30 s + 79 s = ~85–110 s —
// well below the 5 min idle timeout, still above typical jitter spike
// duration.
//
// During the SRTT warmup window — before the RFC 6298 estimator on
// session.Health() has consumed at least 3 correlated pongs — RTO is
// dominated by the InitialRTO seed and any first-packet jitter, not the
// path's steady-state behaviour. A 1 s floor in that window is too
// tight: a single transient loss on a freshly-installed noise-UDP
// session burns the retry budget at exactly the moment the path is
// still proving itself and no standby exists. Use a 5 s warmup floor
// while SampleCount < 3, then drop to the steady-state 1 s floor once
// the estimator is converged and the timeout reflects measured path
// behaviour.
//
// Nil Session OR nil Health is treated as warmup (5 s floor), matching
// adaptiveKeepaliveInterval / adaptiveRPCTimeout. A session whose
// Health() has not yet been assigned is by definition uninitialised —
// the keepalive loop in mesh_connection.go reads Health() once per
// retry, and there is a tiny race window between session construction
// and SetHealthMonitor. Treating that window as steady-state previously
// used a 1 s timeout that killed every fresh noise-UDP session that
// hit any transient jitter — exactly the failure mode the warmup
// floor was added to prevent.
func adaptiveKeepaliveTimeout(s aether.Session, rto time.Duration) time.Duration {
	const (
		steadyFloor = 1 * time.Second
		warmupFloor = 5 * time.Second
		cap_        = 15 * time.Second
		// warmupSamples is the number of correlated pongs the RFC 6298
		// estimator must consume before the 1 s steady-state floor is
		// trusted. The previous threshold of 3 was too tight: by sample
		// 3 RTO is fresh but still noisy; a single 1 s timeout on
		// sample 4-7 can fire a ping failure on a healthy path purely
		// because the estimator hasn't damped out its initial spread.
		// History showed a noise-UDP session-death cluster at ~34 s,
		// which is sample 4-7 territory at the default 5 s keepalive
		// interval; raising the gate to 8 keeps the 5 s warmup floor
		// in effect through the full convergence window.
		warmupSamples = 8
	)
	floor := steadyFloor
	if s == nil {
		floor = warmupFloor
	} else if h := s.Health(); h == nil || h.SampleCount() < warmupSamples {
		floor = warmupFloor
	}
	if rto <= 0 {
		return floor
	}
	timeout := 4 * rto
	if timeout < floor {
		return floor
	}
	if timeout > cap_ {
		return cap_
	}
	return timeout
}
