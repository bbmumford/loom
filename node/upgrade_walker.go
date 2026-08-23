/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"log"
	"time"

	"github.com/ORBTR/aether"
)

// upgradeWalkInterval is how often the proactive transport-grade upgrade
// walker runs. Mirrors the deleted upgradeIntervalGradeC from the
// pre-c7ef4bb tryUpgrades cadence: fast enough that a peer stuck on a
// suboptimal transport recovers within ~one tick of the underlying
// condition clearing, slow enough not to thrash dial bookkeeping or
// burn probe slots.
const upgradeWalkInterval = 30 * time.Second

// upgradeStabilityWindow is the minimum time a connection must have been
// live before a proactive upgrade probe is allowed. Mirrors the 60s
// stability gate the deleted tryUpgrades enforced. Prevents probes from
// firing on flapping connections that are about to be torn down anyway —
// the gate clears once a connection has proven stable.
const upgradeStabilityWindow = 60 * time.Second

// upgradeStallEscalationThreshold caps how many consecutive stall-cooldown
// failures the walker tolerates on a given (peer, target-proto) before
// escalating to recordDialFailure's long quality-tracker cooldown.
// Without this gate a truly-unreachable transport (e.g. cross-org
// noise-UDP today, before the anycast UDP gap clears) would burn one
// tryDial probe slot every walker tick forever.
//
// 3 attempts × 30s interval = 90s of fast retry before escalation —
// long enough to absorb genuine transient failures (deploy restart,
// brief packet loss, region not yet classified) but short enough that
// permanently-broken paths don't keep consuming probe slots.
const upgradeStallEscalationThreshold = 3

// minProbeIntervalForGrade returns the minimum time between upgrade
// probes for a peer at the given grade. Lower-grade peers are probed
// aggressively because every successful upgrade has high payoff; peers
// already at GradeA or GradeB get throttled so the walker doesn't burn
// probe slots on peers whose remaining upgrade headroom is small.
// Mirrors the cadence the deleted tryUpgrades enforced before c7ef4bb.
//
// The walker still ticks every upgradeWalkInterval (30s); this gate is
// a per-(peer, current-best-grade) rate limit applied during snapshot.
// A GradeC peer whose last probe was 45s ago qualifies on the next tick;
// a GradeB peer whose last probe was 45s ago waits until 5 min have
// elapsed since the previous probe.
func minProbeIntervalForGrade(g Grade) time.Duration {
	switch g {
	case GradeA:
		// A GradeA best-grade does NOT imply the peer is on noise-UDP.
		// bestActiveGrade() can read GradeA from a WebSocket/TLS session,
		// or go stale after a noise-UDP path fell back to WS/TLS without
		// its transportConn being reaped — yet noise-UDP is still the
		// preferred transport per protocolOrder. Freezing GradeA for 24h
		// here undid the equal-grade noise-UDP probe the strictly-less-than
		// gate below (see the "WebSocket Grade A must still get a noise-UDP
		// probe" comment) was added to allow, locking mis-graded peers onto
		// WS/TLS with zero noise-UDP re-probes. Probe at the GradeC cadence
		// instead; the downstream active-protocol gate (AllProtocolsRepresented)
		// plus registerMeshSession arbitration suppress a redundant probe when
		// noise-UDP is GENUINELY the live path, so a peer truly already on
		// noise-UDP is not stormed.
		return upgradeIntervalGradeC
	case GradeB:
		return upgradeIntervalGradeB
	case GradeC, GradeF:
		return upgradeIntervalGradeC
	default:
		return upgradeWalkInterval
	}
}

// upgradeCandidate is a single (peer, target-protocol) pair captured
// under m.mu, dispatched outside the lock. Snapshotting avoids holding
// the lock across the bestAddress / dialWithProtocol call sites.
type upgradeCandidate struct {
	peerNodeID    string
	peerRegion    string
	bootstrapHost string
	target        Protocol
}

// tryProactiveUpgrades walks every connected peer once per tick. For
// each peer it probes every strictly-greater-grade non-active protocol
// for which a dialable address can be resolved. First successful upgrade
// promotes the peer; concurrent probes for the same peer are arbitrated
// by registerMeshSession + the K-floor in the multipath manager so the
// keep-set stays bounded.
//
// The walker is intentionally:
//
//   - Origin-agnostic. Same-Fly-org AND cross-org peers are walked. When
//     the cross-org anycast-UDP delivery gap clears (Fly fix OR per-machine
//     IP workaround), cross-org peers auto-upgrade to noise-UDP with no
//     code change — bestAddress already includes the noise-UDP candidate
//     for cross-org via Tier-1 (public IPv4) or Tier-3 (peerServiceHostname);
//     the walker's only requirement is that bestAddress returns non-empty.
//
//   - Grade-fan-out, not single-best. A peer on WS (Grade C) gets a probe
//     fired for noise-UDP (A), QUIC (B), AND gRPC (B) in the same tick.
//     First success wins at registerMeshSession; the multipath manager's
//     K-floor caps total simultaneous paths so extra successes don't
//     trigger churn.
//
//   - Stall-cooldown on failure, not the long quality-tracker cooldown.
//     A transient probe failure (region not yet classified, listener
//     warming up, momentary packet loss) must not suppress retries for
//     minutes — the next ticker window must probe again. Long cooldowns
//     are reserved for actual dial-pipeline failures from connectPeer.
func (m *ConnectionManager) tryProactiveUpgrades(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	m.walkerTicks.Add(1)
	// Reap closed pending-session tags before snapshotting so the map
	// stays bounded when a probe's dialed session never reaches register.
	m.drainStaleWalkerPendingSessions()
	now := time.Now()
	candidates := m.snapshotUpgradeCandidates(now)
	m.walkerCandidates.Add(uint64(len(candidates)))
	for _, c := range candidates {
		go m.probeUpgrade(ctx, c)
	}
}

// snapshotUpgradeCandidates iterates m.peers under the read lock and
// produces (peer, target-proto) pairs that satisfy every upgrade gate:
// the peer is connected, stable for >= upgradeStabilityWindow, the local
// node owns the dial (dialOwned), the target protocol is not currently
// active in the multipath manager, the target's grade is strictly better
// than the peer's best-active grade, and bestAddress can resolve a
// dialable address for the target.
//
// snapshotUpgradeCandidates must not call peerServiceHostname / bestAddress
// under m.mu — both acquire cache.mu, which a parallel cache writer might
// hold while waiting for m.mu. The walker resolves bestAddress AFTER
// releasing the peers-map lock; transient inconsistency between snapshot
// and probe time is tolerable because the probe re-resolves on the dial
// path anyway.
func (m *ConnectionManager) snapshotUpgradeCandidates(now time.Time) []upgradeCandidate {
	type peerSnapshot struct {
		nodeID        string
		region        string
		bootstrapHost string
		bestGrade     Grade
		multipathMgr  interface{ AllProtocolsRepresented() []aether.Protocol }
	}

	m.mu.Lock()
	snapshots := make([]peerSnapshot, 0, len(m.peers))
	for _, p := range m.peers {
		if p == nil || p.state != PeerConnected {
			continue
		}
		if p.lastConnectedAt.IsZero() || now.Sub(p.lastConnectedAt) < upgradeStabilityWindow {
			continue
		}
		if !m.dialOwned(p, now) {
			continue
		}
		snapshots = append(snapshots, peerSnapshot{
			nodeID:        p.nodeID,
			region:        p.peerRegion,
			bootstrapHost: p.bootstrapHost,
			bestGrade:     p.bestActiveGrade(),
		})
	}
	m.mu.Unlock()

	// Per-peer grade-adaptive rate limit. GradeA peers are skipped
	// outright (no upgrade headroom); GradeB peers are probed every
	// upgradeIntervalGradeB (5 min) at most; GradeC/D/F peers are
	// probed every upgradeIntervalGradeC (1 min) at most. The walker
	// ticks every upgradeWalkInterval (30s); this gate is a per-peer
	// minimum spacing applied on top of the tick cadence.
	//
	// Filter only — stamping happens AFTER the per-protocol gates
	// below, once a candidate has actually been produced. Stamping
	// at the filter step burned the rate-limit budget on no-op
	// snapshots whenever every protocol was gated out downstream —
	// most commonly when bestAddress() returned "" for a same-org
	// peer whose swarm AddressTable merge hadn't landed yet. The
	// peer was rate-limited for the full upgradeIntervalGradeC even
	// though no probe was ever queued; by the time the limiter
	// released the address had merged, but the walker still wouldn't
	// re-check until the limiter clock expired.
	m.walkerProbeMu.Lock()
	if m.walkerProbeAt == nil {
		m.walkerProbeAt = make(map[string]time.Time)
	}
	kept := snapshots[:0]
	for _, s := range snapshots {
		minInterval := minProbeIntervalForGrade(s.bestGrade)
		if last, ok := m.walkerProbeAt[s.nodeID]; ok && now.Sub(last) < minInterval {
			continue
		}
		kept = append(kept, s)
	}
	m.walkerProbeMu.Unlock()
	snapshots = kept

	// Resolve the multipath manager for each snapshot under multipathMu so
	// snapshotUpgradeCandidates doesn't double-lock m.mu while reading the
	// multipath map. AllProtocolsRepresented is the manager's own
	// concurrency boundary; safe to call without holding multipathMu after
	// the pointer snapshot.
	m.multipathMu.Lock()
	for i := range snapshots {
		if mgr, ok := m.multipathManagers[snapshots[i].nodeID]; ok {
			snapshots[i].multipathMgr = mgr
		}
	}
	m.multipathMu.Unlock()

	out := make([]upgradeCandidate, 0)
	for _, s := range snapshots {
		var active map[Protocol]struct{}
		if s.multipathMgr != nil {
			active = make(map[Protocol]struct{})
			for _, proto := range s.multipathMgr.AllProtocolsRepresented() {
				active[unmapProtocol(proto)] = struct{}{}
			}
		} else {
			// No multipath manager — fall back to a single-protocol active set
			// derived from the peer's recorded protocol. Without the multipath
			// manager the peer's K-floor isn't being enforced anyway; the
			// walker still spawns probes so a missing-manager state self-heals.
			active = map[Protocol]struct{}{}
		}
		// Need peer-conn fields again to compute the per-protocol gates;
		// pull a fresh read under m.mu. Cheap (one map lookup per peer).
		m.mu.Lock()
		peer, present := m.peers[s.nodeID]
		var live []Protocol
		if present && peer != nil {
			live = make([]Protocol, 0, len(peer.transports))
			for proto := range peer.transports {
				live = append(live, proto)
			}
		}
		m.mu.Unlock()
		if !present || peer == nil {
			continue
		}

		// peer.transports is keyed by NODE protocol and is the authoritative
		// record of what this peer currently has. The set above was derived by
		// round-tripping aether protocols through unmapProtocol, which cannot
		// return ProtoTLS: mapProtocol folds ProtoTLS onto aether.ProtoWebSocket
		// when the path is registered, and unmapProtocol only produces ProtoTLS
		// from aether.ProtoTCP, which mapProtocol never emits. A peer with a
		// live TLS path therefore never had ProtoTLS in the active set, so the
		// walker re-probed a protocol it was already carrying on every pass.
		for _, proto := range live {
			active[proto] = struct{}{}
		}
		for _, proto := range protocolOrder {
			if _, on := active[proto]; on {
				continue
			}
			// Strictly-less-than: a protocol whose grade is BELOW the
			// peer's current best grade is a downgrade and must be
			// skipped. Equal grades are NOT skipped — a peer that
			// established WebSocket Grade A must still get a noise-UDP
			// probe, because noise-UDP at Grade A is the preferred
			// transport per protocolOrder and the only path that
			// unlocks the per-frame UDP advantages (no head-of-line,
			// native NAT traversal). The previous <= gate locked
			// every same-org WS-A pair into WebSocket forever because
			// the equal-grade noise-UDP probe was suppressed.
			if GradeForProtocol(proto) < s.bestGrade {
				continue
			}
			// Skip transports the scaler just drained as redundant.
			// scanAndConnect already honours this gate; without
			// mirroring it here the walker probes a fresh dial 30 s
			// after the scaler drained the same class, starting the
			// drain→redial loop the cooldown was designed to break.
			// Calling under m.mu lock — drainedAt is the peerConn-
			// scoped map written by closeDrainedConnection.
			m.mu.Lock()
			peer2, present2 := m.peers[s.nodeID]
			cooledDown := false
			wantsBackoff := false
			if present2 && peer2 != nil {
				if peer2.drainedAt != nil {
					if drainedAt, ok := peer2.drainedAt[proto]; ok && now.Sub(drainedAt) < drainRedialCooldown {
						cooledDown = true
					}
				}
				wantsBackoff = m.peerWantsBackoff(peer2, now)
			}
			m.mu.Unlock()
			if cooledDown {
				continue
			}
			// Honour a peer's saturation back-off: stop opening fresh upgrade
			// paths to it. peerWantsBackoff's same-org guard means same-org
			// probes (the noise-UDP 6PN path the walker exists to find) are
			// never suppressed — only cross-org extra-path probes back off.
			if wantsBackoff {
				continue
			}
			// Address resolution is the final gate — bestAddress is the
			// authoritative answer for "is there any way to reach this
			// peer over this transport". If it returns "" we don't even
			// burn a tryDial probe slot on the candidate. Calling
			// bestAddress here happens outside m.mu deliberately.
			if m.bestAddress(peer, proto) == "" {
				continue
			}
			out = append(out, upgradeCandidate{
				peerNodeID:    s.nodeID,
				peerRegion:    s.region,
				bootstrapHost: s.bootstrapHost,
				target:        proto,
			})
		}
	}

	// Stamp rate-limit timestamps only for peers that produced at
	// least one candidate. The stamp now means "we actually queued
	// a probe", matching the semantic in minProbeIntervalForGrade
	// ("at most one probe per N seconds"). Peers whose per-protocol
	// gates all skipped (most often bestAddress() == "" during a
	// pending AddressTable merge) leave their walkerProbeAt entry
	// unchanged so the next 30 s tick re-checks the same gates and
	// picks them up immediately when the merge lands.
	if len(out) > 0 {
		stamped := make(map[string]struct{}, len(out))
		for _, c := range out {
			stamped[c.peerNodeID] = struct{}{}
		}
		m.walkerProbeMu.Lock()
		for nodeID := range stamped {
			m.walkerProbeAt[nodeID] = now
		}
		m.walkerProbeMu.Unlock()
	}
	return out
}

// probeUpgrade runs in its own goroutine per (peer, proto) candidate. It
// re-validates the peer is still a valid upgrade target (state could have
// changed between snapshot and probe), consumes a tryDial probe slot,
// dials the target protocol, and on success hands off to DialAndAcceptMesh
// for negotiate + register. The success path is identical to connectPeer's
// happy path so registerMeshSession + the multipath manager arbitrate the
// keep-set the same way they do for fresh dials.
//
// On failure we use recordStallCooldown rather than recordDialFailure: a
// transient probe failure must not suppress retries for minutes when the
// underlying issue (region not yet classified, listener warming up,
// momentary packet loss) clears in seconds. After
// upgradeStallEscalationThreshold consecutive stall failures the probe
// is demoted to recordDialFailure's long quality cooldown so a truly-
// unreachable transport (cross-org noise-UDP before the anycast UDP
// gap clears, etc.) stops burning probe slots every tick. Success
// clears the consecutive-stall counter so a recovered path is given
// the same short-retry treatment again.
func (m *ConnectionManager) probeUpgrade(ctx context.Context, c upgradeCandidate) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[upgrade-walker] %s: probe %s panic recovered: %v",
				truncID(c.peerNodeID), c.target, r)
		}
	}()

	if ctx.Err() != nil {
		return
	}

	// Raw probe attempt counter — bumped before any gate so per-stage
	// attrition (started → succeeded → proving → proving_succeeded) is
	// computable from the published stats tuple.
	m.walkerProbesStarted.Add(1)

	// Re-validate peer still exists and the upgrade is still wanted.
	// State can change between snapshot and probe (peer disconnect,
	// stronger transport already arrived, etc.).
	m.mu.Lock()
	peer, ok := m.peers[c.peerNodeID]
	if !ok || peer == nil || peer.state != PeerConnected {
		m.mu.Unlock()
		return
	}
	if GradeForProtocol(c.target) < peer.bestActiveGrade() {
		// Another path already upgraded this peer between snapshot and
		// probe. Strictly-less-than mirrors the snapshot-side gate above:
		// we only abandon the probe if the live state is GREATER than
		// the target grade. Equal grades let the probe proceed — the
		// target may be more preferred per protocolOrder (e.g. a
		// noise-UDP probe still races against a same-grade WebSocket
		// that established between snapshot and dial).
		m.mu.Unlock()
		m.walkerProbesSkippedRace.Add(1)
		return
	}
	m.mu.Unlock()

	// Probe-slot budgeting: skip if the per-(peer,transport) tracker is
	// cooled down. tryDial consumes a slot only when it returns true.
	if !m.tryDial(c.peerNodeID, c.target) {
		m.walkerProbesSkippedSlot.Add(1)
		return
	}

	session, err := m.dialWithProtocol(ctx, peer, c.target)
	if err != nil {
		// Consecutive-stall escalation. The walker keeps re-attempting on
		// stall cooldown semantics, but only up to a bounded threshold
		// per (peer, proto) — past that we fall back to the long quality
		// cooldown so unreachable paths stop burning probe slots.
		stalls := m.bumpConsecutiveStalls(c.peerNodeID, c.target)
		if stalls >= upgradeStallEscalationThreshold {
			m.recordDialFailure(c.peerNodeID, c.target)
			m.walkerProbesEscalated.Add(1)
			dbgPeers.Printf("%s: upgrade probe %s escalated to quality cooldown after %d consecutive stalls: %v",
				truncID(c.peerNodeID), c.target, stalls, err)
			return
		}
		m.recordStallCooldown(c.peerNodeID, c.target)
		m.walkerProbesStallCooled.Add(1)
		dbgPeers.Printf("%s: upgrade probe %s failed (stall %d/%d): %v",
			truncID(c.peerNodeID), c.target, stalls, upgradeStallEscalationThreshold, err)
		return
	}

	// Success — clear failure state for this (peer, transport) AND the
	// walker's consecutive-stall counter so a recovered path is given
	// the same short-retry treatment again the next time it degrades.
	m.recordDialSuccess(c.peerNodeID, c.target)
	m.clearConsecutiveStalls(c.peerNodeID, c.target)
	bs, ok := session.(*aether.BaseConnection)
	if !ok || bs == nil {
		log.Printf("[upgrade-walker] %s: probe %s returned non-BaseConnection (%T) — closing",
			truncID(c.peerNodeID), c.target, session)
		if session != nil {
			session.Close()
		}
		return
	}
	conn := bs.Conn

	svcName := m.peerServiceName(c.peerNodeID)
	// walkerProbesSucceeded keeps its original semantics: the probe's
	// Noise handshake completed and we're about to hand the session to
	// the same negotiate+register path connectPeer uses. It does NOT
	// imply the upgrade survived the 60s proving window — for that, see
	// walkerProbesProving / walkerProbesProvingSucceeded / walkerProbesProvingFailed.
	// They're populated by registerMeshSession's upgrade-promote branch
	// and the proving timer / unregister-revert paths, using the
	// peerNodeID tag installed by markWalkerPendingSession below.
	m.walkerProbesSucceeded.Add(1)
	// Tag the peer so registerMeshSession's upgrade-promote branch can
	// attribute the resulting provingSession to the walker (proving /
	// provingSucceeded / provingFailed counters). The tag must be
	// installed BEFORE the handoff goroutine so register can't run
	// against an empty map. Keyed by peerNodeID — see field comment
	// on walkerPendingSessions for the shared-key rationale.
	m.markWalkerPendingSession(c.peerNodeID)
	log.Printf("[upgrade-walker] %s: probe upgraded to %s, launching Aether session",
		truncID(c.peerNodeID), c.target)
	// Hand off to the same negotiate+register path used by connectPeer.
	// registerMeshSession arbitrates dedup vs the existing lower-grade
	// session; multipath_dial's K-floor caps simultaneous paths.
	go m.rt.DialAndAcceptMesh(ctx, conn, c.peerNodeID, c.peerRegion, c.target, c.bootstrapHost, svcName, "", dialBorrowsConnectingState)
}

// markWalkerPendingSession records that a probe-dial for the given
// peer is about to be handed off to DialAndAcceptMesh, so
// registerMeshSession's upgrade-promote branch can attribute the
// resulting provingSession to the walker. Keyed by peerNodeID because
// the session pointer the walker holds (*aether.BaseConnection from
// dialWithProtocol) and the session pointer register receives (created
// inside SetupMeshSession by adapter.NewSessionForProtocol) are
// different objects; the peer nodeID is the only identity shared
// between the two call sites. Some adapters (gRPC, Relay) also return
// nil from NetConn() so the raw net.Conn can't bridge them either.
//
// The stored timestamp powers drainStaleWalkerPendingSessions's TTL
// reaper: entries whose register call never lands (handoff goroutine
// dropped the session before AcceptMeshConnection ran, peer rejected
// the new session, etc.) age out after walkerPendingTTL so the map
// stays bounded.
func (m *ConnectionManager) markWalkerPendingSession(peerID string) {
	if peerID == "" {
		return
	}
	m.walkerPendingMu.Lock()
	if m.walkerPendingSessions == nil {
		m.walkerPendingSessions = make(map[string]time.Time)
	}
	m.walkerPendingSessions[peerID] = time.Now()
	m.walkerPendingMu.Unlock()
}

// consumeWalkerPendingSession returns true and removes the entry if
// the given peer was tagged by markWalkerPendingSession. Called from
// registerMeshSession's upgrade-promote branch under m.dispatchMu;
// the lock acquired here is the separate walkerPendingMu so no nested
// lock-order risk vs dispatchMu.
func (m *ConnectionManager) consumeWalkerPendingSession(peerID string) bool {
	if peerID == "" {
		return false
	}
	m.walkerPendingMu.Lock()
	defer m.walkerPendingMu.Unlock()
	if m.walkerPendingSessions == nil {
		return false
	}
	if _, ok := m.walkerPendingSessions[peerID]; ok {
		delete(m.walkerPendingSessions, peerID)
		return true
	}
	return false
}

// walkerPendingTTL bounds how long a walker-pending tag survives
// before drainStaleWalkerPendingSessions reaps it. Sized at two walker
// intervals so a tag that misses one register call still survives the
// next tick's drain — long enough to absorb a slow SetupMeshSession or
// a delayed AcceptMeshConnection handoff, short enough that a probe
// whose session never reached register doesn't leak the slot forever.
const walkerPendingTTL = 2 * upgradeWalkInterval

// drainStaleWalkerPendingSessions reaps walkerPendingSessions entries
// older than walkerPendingTTL. Called once per walker tick from
// tryProactiveUpgrades so the map can't grow without bound when
// DialAndAcceptMesh drops a session before registerMeshSession runs
// (panic recovered in the handoff goroutine, peer rejected at
// AcceptMeshConnection, etc.). Without a session pointer we can't
// observe IsClosed() — TTL is the substitute liveness signal.
func (m *ConnectionManager) drainStaleWalkerPendingSessions() {
	m.walkerPendingMu.Lock()
	defer m.walkerPendingMu.Unlock()
	if m.walkerPendingSessions == nil {
		return
	}
	cutoff := time.Now().Add(-walkerPendingTTL)
	for peerID, markedAt := range m.walkerPendingSessions {
		if markedAt.Before(cutoff) {
			delete(m.walkerPendingSessions, peerID)
		}
	}
}

// walkerStallKey uniquely identifies a (peer, target-protocol) pair in
// the consecutive-stall map. Composite key keeps the per-(peer,proto)
// counter isolated so a flapping protocol on peer A doesn't escalate a
// healthy probe on peer B.
type walkerStallKey struct {
	peerID string
	proto  Protocol
}

// bumpConsecutiveStalls increments the walker's consecutive-stall
// counter for (peer, proto) and returns the post-increment value. Used
// by probeUpgrade to decide whether to keep stall-cooling (short retry)
// or escalate to the long quality cooldown.
func (m *ConnectionManager) bumpConsecutiveStalls(peerID string, proto Protocol) int {
	m.walkerStallMu.Lock()
	defer m.walkerStallMu.Unlock()
	if m.walkerStalls == nil {
		m.walkerStalls = make(map[walkerStallKey]int)
	}
	key := walkerStallKey{peerID: peerID, proto: proto}
	m.walkerStalls[key]++
	return m.walkerStalls[key]
}

// clearConsecutiveStalls resets the walker's stall counter for
// (peer, proto). Called on a successful probe so a transport that
// briefly degrades and recovers gets the full upgradeStallEscalationThreshold
// budget again before being demoted to the long cooldown.
func (m *ConnectionManager) clearConsecutiveStalls(peerID string, proto Protocol) {
	m.walkerStallMu.Lock()
	defer m.walkerStallMu.Unlock()
	if m.walkerStalls == nil {
		return
	}
	delete(m.walkerStalls, walkerStallKey{peerID: peerID, proto: proto})
}

// WalkerStats returns the walker's telemetry counters as a snapshot
// tuple. Mirrors CrossOrgPreambleStats / CrossOrgGateStats style so
// runtime.MeshMetrics can surface them as snake_case keys without
// touching the underlying atomics from outside this package.
//
// Returned fields (in order):
//
//   - ticks: number of tryProactiveUpgrades runs.
//   - candidates: total (peer, target-proto) pairs queued across all ticks.
//   - started: probes that entered probeUpgrade (raw attempt counter).
//   - succeeded: probes whose Noise handshake completed and handed off
//     to DialAndAcceptMesh. NOT a measure of upgrade durability — a
//     handshake-honest probe that dies inside the 60s proving window
//     still counts here. Pair with proving / provingSucceeded /
//     provingFailed for the honest "upgrade actually stuck" signal.
//   - proving: probes whose handoff installed a proving-window entry.
//     succeeded - proving = handoffs that never started a proving cycle
//     (same-grade reject, dedup race, transport-equal short-circuit).
//   - provingSucceeded: probes whose new session survived the 60s
//     proving window AND was still the dispatch entry when the timer
//     fired. The honest upgrade-actually-stuck count.
//   - provingFailed: probes whose new session died during proving and
//     triggered the revert-to-old branch in unregisterMeshSession.
//   - stallCooled: probe dial failed and was put on the short stall
//     cooldown for the next walker tick.
//   - escalated: probe hit upgradeStallEscalationThreshold consecutive
//     stalls and was demoted to recordDialFailure's long cooldown.
//   - skippedRace: between snapshot and probe, another path upgraded
//     the peer above the target grade; probe aborted before dialing.
//   - skippedSlot: tryDial returned false because the per-(peer,proto)
//     slot was in cooldown.
func (m *ConnectionManager) WalkerStats() (ticks, candidates, started, succeeded, proving, provingSucceeded, provingFailed, stallCooled, escalated, skippedRace, skippedSlot uint64) {
	return m.walkerTicks.Load(),
		m.walkerCandidates.Load(),
		m.walkerProbesStarted.Load(),
		m.walkerProbesSucceeded.Load(),
		m.walkerProbesProving.Load(),
		m.walkerProbesProvingSucceeded.Load(),
		m.walkerProbesProvingFailed.Load(),
		m.walkerProbesStallCooled.Load(),
		m.walkerProbesEscalated.Load(),
		m.walkerProbesSkippedRace.Load(),
		m.walkerProbesSkippedSlot.Load()
}

// WalkerWakeStats returns the wake-channel coalescing counters.
// delivered = SignalWalkerWake calls that enqueued a wake; coalesced =
// calls that found the channel already full and dropped (the queued wake
// covers this signal too, by walker semantics — the walker re-snapshots
// every connected peer every run).
func (m *ConnectionManager) WalkerWakeStats() (delivered, coalesced uint64) {
	return m.walkerWakeDelivered.Load(), m.walkerWakeCoalesced.Load()
}

// SignalWalkerWake nudges the proactive upgrade walker to run its next
// pass as soon as the Start loop's select wakes. Non-blocking: if a wake
// is already queued the call returns immediately and the second signal
// is coalesced — the walker re-snapshots every connected peer per run
// so one queued wake is sufficient to observe N records that landed in
// the burst.
//
// The expected call site is AddressTable.onRecord when a fresh
// PeerRecord adds a Tier-0 (Scope="private" + Transport=NOISE_UDP, i.e.
// Fly 6PN ULA) address for a peer the local node may already hold a
// lower-grade session for. Without this signal the walker waits up to
// upgradeWalkInterval (30s) to discover the new address; with it the
// upgrade probe fires on the very next Start-loop iteration.
//
// Safe to call from any goroutine and before the walker is running
// (the buffered channel absorbs the first signal until Start drains it).
func (m *ConnectionManager) SignalWalkerWake() {
	if m == nil || m.walkerWakeCh == nil {
		return
	}
	select {
	case m.walkerWakeCh <- struct{}{}:
		m.walkerWakeDelivered.Add(1)
	default:
		// Channel full — a wake is already queued. The walker re-snapshots
		// every connected peer on each run, so the queued wake covers
		// this signal too. Increment the coalesce counter so the no-op
		// is observable.
		m.walkerWakeCoalesced.Add(1)
	}
}
