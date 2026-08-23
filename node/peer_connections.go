/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"github.com/bbmumford/loom/ports"
	"hash/fnv"
	"log"
	"net"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	aether "github.com/ORBTR/aether"
	"github.com/ORBTR/aether/adapter"
	grpctransport "github.com/ORBTR/aether/grpc"
	"github.com/ORBTR/aether/multipath"
	"github.com/ORBTR/aether/noise"
	"github.com/ORBTR/aether/quality"
	quictransport "github.com/ORBTR/aether/quic"
	"github.com/ORBTR/aether/resume"
	wstransport "github.com/ORBTR/aether/websocket"
	lad "github.com/bbmumford/ledger"
	ladcache "github.com/bbmumford/ledger/cache"
	"github.com/bbmumford/swarm"
)

// Protocol identifies a transport protocol for peer connections.
type Protocol int

const (
	ProtoNoiseUDP Protocol = iota
	ProtoQUIC
	ProtoWebSocket
	ProtoGRPC
	ProtoTLS // TLS bootstrap (VL1 hijacked HTTPS) — permanent fallback
)

func (p Protocol) String() string {
	switch p {
	case ProtoNoiseUDP:
		return "noise-udp"
	case ProtoQUIC:
		return "quic"
	case ProtoWebSocket:
		return "websocket"
	case ProtoGRPC:
		return "grpc"
	case ProtoTLS:
		return "tls"
	default:
		return "unknown"
	}
}

// installableMeshConn reports whether a SUCCESSFULLY-DIALED session is one the
// mesh can actually install, returning the underlying connection when it is.
//
// The reason this check exists at all: QuicTransport.Dial
// returns a *QuicSession, not a *BaseConnection, so on a noise-UDP→QUIC fallback
// (same UDP address) a bare type assertion panicked; the defer-recover then
// marked the peer Disconnected but never Closed the session, leaking the QUIC
// conn and its goroutines after dial-success had already been recorded.
//
// 🔑 EXTRACTED AS A PURE FUNCTION, and the honest limit is worth
// stating: this pins the LOGIC, not the CALL SITE. `connectPeer` is at 0.0%
// coverage and cannot be reached by a unit test — the three transports are
// concrete pointer types with no seam to substitute a fake through.
// The call-site gap is RECORDED, not closed; (b), the injectable-dial seam the
// sibling path already uses at multipath_dial.go:233, is sequenced behind
// identifying peer_connections.go's other writer.
// ⚠ NO EXPLICIT `session == nil` GUARD, and that is deliberate: my first draft
// had one and mutation proved it inert — a nil interface fails the type
// assertion, so `!ok` already covers it. `bs == nil` is NOT redundant, though:
// it catches a non-nil interface holding a nil *BaseConnection, which the
// assertion accepts.
func installableMeshConn(session aether.Connection) (*aether.BaseConnection, bool) {
	bs, ok := session.(*aether.BaseConnection)
	if !ok || bs == nil {
		return nil, false
	}
	return bs, true
}

// ParseProtocolStrict converts a transport type string to a Protocol enum and
// reports whether the string was RECOGNISED. It is the single source of the
// label set — ParseProtocol wraps it — so callers that must distinguish "this
// is noise-udp" from "I could not parse this" have one list to trust.
//
// Added because ParseProtocol's lossy default made those two cases
// indistinguishable, and the ranking site in rpc_forward.go needs to tell them
// apart on peer-supplied input.
func ParseProtocolStrict(s string) (Protocol, bool) {
	switch s {
	case "noise-udp", "vl1":
		return ProtoNoiseUDP, true
	case "quic":
		return ProtoQUIC, true
	case "websocket", "ws", "wss":
		return ProtoWebSocket, true
	case "grpc":
		return ProtoGRPC, true
	// "gossip-tls" is a DOCUMENTED value of lad.LatencyRecord.Transport
	// (ledger/types.go:261) that was missing from this switch, so it fell to the
	// default and graded A — the best grade — where its own transport class
	// grades C..
	case "tls", "gossip-tls":
		return ProtoTLS, true
	default:
		return ProtoNoiseUDP, false
	}
}

// ParseProtocol converts a transport type string to a Protocol enum.
// Returns ProtoNoiseUDP for unrecognized strings (safe default for VL1 listeners).
//
// ⚠ THE DEFAULT IS LOSSY AND FIVE CALLERS DEPEND ON IT. ProtoNoiseUDP is the
// zero value of Protocol and grades A, so an unrecognised or absent transport
// grades BEST here. That is deliberate for VL1 listeners and was deliberately
// NOT changed — flipping it is a package-wide grading-policy change.
// Callers that rank PEER-SUPPLIED transport strings must use ParseProtocolStrict
// and decide for themselves what an unparseable label deserves.
func ParseProtocol(s string) Protocol {
	p, _ := ParseProtocolStrict(s)
	return p
}

// protocolOrder is the failover hierarchy — fastest first, TLS as final fallback.
// gRPC (Grade B, HTTP/2 native) is tried before WebSocket (Grade C, no native mux/flow control).
// All protocols are tried in order. TLS uses bootstrap host addresses (not reach records).
var protocolOrder = []Protocol{ProtoNoiseUDP, ProtoQUIC, ProtoGRPC, ProtoWebSocket, ProtoTLS}

// PeerState tracks the connection lifecycle of a peer.
type PeerState int

const (
	PeerDiscovered   PeerState = iota // known from LAD, no connection
	PeerConnecting                    // trying to establish connection
	PeerConnected                     // gossip running
	PeerReconnecting                  // previous connection failed, trying again
	PeerDisconnected                  // all protocols failed, waiting for backoff
)

func (s PeerState) String() string {
	switch s {
	case PeerDiscovered:
		return "discovered"
	case PeerConnecting:
		return "connecting"
	case PeerConnected:
		return "connected"
	case PeerReconnecting:
		return "reconnecting"
	case PeerDisconnected:
		return "disconnected"
	default:
		return "unknown"
	}
}

// transportConn tracks a single transport connection to a peer.
// A peer can have multiple simultaneous transports (e.g., noise-udp + TLS bootstrap).
type transportConn struct {
	protocol      Protocol
	grade         Grade
	conn          net.Conn
	isMuxed       bool               // Aether sessions are always muxed
	cancelFunc    context.CancelFunc // cancels connection goroutine for this transport
	connectedAt   time.Time
	successCount  int    // consecutive gossip successes on this transport
	isDormant     bool   // lower-priority transport: all streams alive, routing flag only
	draining      bool   // Set by closeDrainedConnection so cleanup skips chronic/path-failure accounting (voluntary scale-down, not a failure)
	bootstrapHost string // TLS only: host for re-dial if conn dies
}

// peerConn tracks a single peer's transport state.
// Multi-transport: a peer can have simultaneous connections via different protocols.
type peerConn struct {
	nodeID       string
	addresses    []lad.ReachAddress
	state        PeerState
	protocol     Protocol // best active transport protocol
	successCount int      // consecutive successful gossip exchanges on current protocol
	// Per-(peer, transport) dial cooldown lives on the
	// quality.Tracker; query via ConnectionManager.dialIsSuppressed /
	// recordDialFailure / recordDialSuccess.
	crossOrigin bool
	crossRegion bool   // peer is in a different region
	peerRegion  string // peer's region from reach record
	// peerSaturated / peerBackoffUntil mirror the peer's advertised
	// connection-budget pressure (the saturated=1 / backoff_until tags),
	// refreshed from the swarm DialCandidate in scanAndConnect. The scaler and
	// upgrade walker read them under m.mu via peerWantsBackoff rather than a
	// cache round-trip.
	peerSaturated    bool
	peerBackoffUntil int64
	// discoveredAt is the wall-clock time the entry was first added to the
	// peers map. Distinct from lastConnected (which only updates on a
	// successful Accept) so pruneStalePeers can age out PEX/LAD-discovered
	// peers that are dialed, fail every protocol, and end up
	// PeerDisconnected with lastConnected.IsZero() — pre-v0.0.217 those
	// entries were retained forever because the prune predicate skipped
	// any peer with zero lastConnected.
	discoveredAt   time.Time
	lastConnected  time.Time
	reconnectDelay time.Duration
	connCount      int                // current number of active connections to this peer
	lastRTT        time.Duration      // most recent NetworkRTT for this peer
	initRTT        time.Duration      // dial/handshake RTT (set once at connection, never overwritten)
	lastExchange   time.Time          // timestamp of last successful gossip exchange
	drainState     DrainState         // drain lifecycle state (graceful shutdown tracker)
	priority       ConnectionPriority // tenant-aware connection priority (drives drain order and rebalance)
	rpcsLastMinute int                // RPC count in the last minute — rolling traffic weight
	lastRPCAt      time.Time          // timestamp of most recent RPC on this connection
	isMuxed        bool               // Aether sessions are always muxed
	outboundDialed bool               // true if ConnectionManager dialed this peer (not just incoming)
	bootstrapHost  string             // TLS bootstrap host for re-dial (e.g., "node.hstles.com")

	// dialEligibleSince stamps when this peer first became eligible for an
	// outbound dial while THIS node is the higher-nodeID (deferring) side of
	// the pair. Deterministic dial ownership (see dialOwned) has the higher
	// nodeID wait dialOwnershipGrace for the lower nodeID to dial inbound
	// before dialing anyway. Zero whenever the peer is connected or when we
	// own the dial; set lazily on the first deferred scan tick.
	dialEligibleSince time.Time

	// Multi-transport: all active transports to this peer
	transports map[Protocol]*transportConn // nil until first multi-transport connection

	// Connection stability tracking (grade-agnostic)
	chronicFailCount int           // consecutive connections lasting < chronicThreshold
	lastConnLifetime time.Duration // how long the most recent connection lasted
	lastConnectedAt  time.Time     // when current connection was established (for lifetime calc)
	// Per-(peer, transport) dial-failure counters live on the
	// quality.Tracker entry, alongside the cooldown state.

	// Per-protocol stuck-session escalation. When a session closes with
	// aether.ErrSessionStuck the connection manager has historically marked
	// the protocol failed for a fixed cooldown (cooldownFor's static base
	// doubled per other failed protocol). On a peer where one specific
	// protocol — usually noise-udp on a high-loss cross-region anycast
	// path — repeatedly transitions ESTABLISHED → STUCK → COOLDOWN →
	// ESTABLISHED → STUCK we'd waste a full handshake every cycle for an
	// effectively-permanently-broken path. stuckCount tracks how many
	// times the protocol has been declared stuck without an intervening
	// stable run; cooldownFor uses it to escalate the cooldown
	// geometrically (30 s → 5 min → 30 min → 1 h …). stuckCount resets to
	// zero once the peer accumulates stuckRecoverySuccesses successful
	// gossip exchanges on that same protocol — an actually-stable run.
	stuckCount        map[Protocol]int // protocol → consecutive stuck-fail count
	stuckSuccessSince map[Protocol]int // protocol → successCount at the last stuck event (for recovery detection)

	// Sticky-bad-path detection now lives entirely on the
	// quality.Tracker. Stuck-kill events from the aether stall
	// detector flow through ConnectionManager.recordDialFailure(),
	// which increments the tracker's per-(peer, transport)
	// dial-failure count and sets a growing-schedule cooldown.
	// Sustained successful gossip via noteStuckRecovery clears
	// the same state.

	// bestEverGrade is the highest transport grade ever achieved with
	// this peer. Used by topology surfaces to render "what was the
	// best path we ever saw to this node". The current transport's
	// grade is just GradeForProtocol(p.protocol) — there is no
	// congestion-driven demotion field; stalled paths are reflected
	// in the quality tracker's score rather than by mutating grade.
	bestEverGrade Grade

	// Per-transport drain cooldown — wall-clock time at which each transport
	// class was voluntarily drained (scale_down reason). scanAndConnect and
	// tryUpgrades consult this before dialing: a class drained as redundant
	// with a better active path must not be re-dialed until the cooldown
	// elapses, otherwise the scaler drains the fresh connection on the next
	// rebalance and a 60s redial/drain churn loop forms.
	drainedAt map[Protocol]time.Time
}

// gossipOwner records who currently holds the per-peer gossip-initiator
// dedup slot. grade is the grade of the underlying transport the
// owner is running gossip on; cancel terminates the owner's gossip
// context (a context.WithCancel-derived child of connCtx), causing
// RunGossipLoopWithPeer to return without affecting the surrounding
// session. A new initiator with a strictly higher grade calls cancel
// to preempt the current owner; the preempted owner's outer
// initiator loop then sees the slot has been reassigned (or is empty)
// and waits/retries normally.
type gossipOwner struct {
	grade  Grade
	cancel context.CancelFunc
}

// evictGossipOwnerLocked clears the per-peer gossip dedup slot and
// cancels the existing owner's gossip context, if any. Used by the
// session-replace and upgrade-promote paths in registerMeshSession
// where the new (typically higher-grade) session needs to grab the
// gossip role immediately rather than waiting for the existing
// owner's natural exit. reason is logged for trace correlation
// against [DISPATCH-TRACE] register-exit lines.
//
// Caller must NOT hold m.gossipMu; this function takes it. Callers
// may hold m.dispatchMu — we never acquire dispatchMu here so no
// lock-order conflict.
func (m *ConnectionManager) evictGossipOwnerLocked(nodeID, reason string) {
	m.gossipMu.Lock()
	owner, ok := m.gossipActive[nodeID]
	if ok && owner != nil {
		owner.cancel()
	}
	delete(m.gossipActive, nodeID)
	m.gossipMu.Unlock()
	if ok && owner != nil {
		dbgAether.Printf("%s: evicted gossip owner (grade=%s, reason=%s)",
			truncID(nodeID), owner.grade, reason)
	}
}

// promoteGrade tracks the highest grade ever observed for this peer.
// Called whenever a transport becomes active so the topology surface
// can render "what was the best path we ever saw to this node" even
// after the path itself has moved or torn down. Idempotent and
// monotonic — only writes when newGrade strictly beats the stored
// peak. Caller must hold ConnectionManager.mu.
//
// The current transport's grade is always GradeForProtocol(p.protocol);
// there is no congestion-driven demotion field. Stalled paths are
// reflected in the quality tracker's score, not by mutating peer.
func (p *peerConn) promoteGrade(newGrade Grade, _ time.Time) {
	if newGrade.BetterThan(p.bestEverGrade) {
		p.bestEverGrade = newGrade
	}
}

// bestActiveGrade returns the highest grade among active (non-dormant) transports.
// Returns GradeF if no active transports exist.
func (p *peerConn) bestActiveGrade() Grade {
	if p.transports == nil {
		if p.state == PeerConnected {
			return GradeForProtocol(p.protocol)
		}
		return GradeF
	}
	best := GradeF
	for _, tc := range p.transports {
		if tc != nil && !tc.isDormant && tc.grade > best {
			best = tc.grade
		}
	}
	return best
}

// peerHasBetterGradeAddress reports whether peer.addresses includes at
// least one address that could be dialed at a grade higher than C AND
// isn't already known-dead in the AddressTracker. The scanAndConnect
// gate uses this to avoid dialing out when the only available addresses
// are WS/TLS — any outbound dial would just produce a second Grade-C
// transport and feed the WS↔TLS drain loop.
//
// The previous implementation only checked Proto; an
// address that AddressTracker had marked dead still counted as
// "available", which caused dialWithProtocol to return "no suitable
// address" → recorded as failure → cooldown grew. Now we check
// addressTracker.Score per candidate and require at least one
// non-dead address.
//
// Caller must hold ConnectionManager.mu.
func (m *ConnectionManager) peerHasBetterGradeAddress(p *peerConn) bool {
	if p == nil {
		return false
	}
	for _, a := range p.addresses {
		var transport string
		switch a.Proto {
		case "udp":
			transport = "noise-udp"
		case "grpc":
			transport = "grpc"
		default:
			continue
		}
		// AddressTracker absence = unknown, treat as alive (give it
		// a chance). Only skip when explicitly known-dead.
		if m.addressTracker != nil {
			candAddr := net.JoinHostPort(a.Host, fmt.Sprintf("%d", a.Port))
			if s, ok := m.addressTracker.Score(p.nodeID, transport, candAddr); ok && s.IsDead() {
				continue
			}
		}
		return true
	}
	return false
}

// peerWantsBackoff reports whether a peer's advertised saturation should make us
// stop opening fresh paths to it. It returns false once the backoff TTL has
// elapsed (treating a lingering tag as cleared), and — the load-bearing guard —
// false for a same-org peer with no strictly-better path, so the single viable
// Tier-0 6PN route is never starved by the bit (bestAddress already withholds
// the Tier-3 anycast fallback for same-org peers, so suppressing the 6PN dial
// would strand them). Caller must hold ConnectionManager.mu.
func (m *ConnectionManager) peerWantsBackoff(p *peerConn, now time.Time) bool {
	if p == nil || !p.peerSaturated {
		return false
	}
	if p.peerBackoffUntil != 0 && now.Unix() >= p.peerBackoffUntil {
		return false
	}
	if !p.crossOrigin && !m.peerHasBetterGradeAddress(p) && p.bestActiveGrade() < GradeA {
		return false
	}
	return true
}

// countPeersAdvertisingBackoff returns how many peers are currently asking us to
// back off (saturated with an unelapsed TTL). Surfaced as the
// peers_advertising_backoff metric. Takes ConnectionManager.mu itself.
func (m *ConnectionManager) countPeersAdvertisingBackoff() int {
	now := time.Now().Unix()
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, p := range m.peers {
		if p != nil && p.peerSaturated && (p.peerBackoffUntil == 0 || now < p.peerBackoffUntil) {
			n++
		}
	}
	return n
}

// peerDistinctGradeCTransportClasses counts how many distinct Grade-C
// transport classes the peer has active. WebSocket and TLS bootstrap
// are both Grade C but fail under different conditions, so keeping both
// provides real failover (not wasteful redundancy). The scaler consults
// this before draining — two Grade-C classes that both carry traffic
// are preserved rather than reduced to one. Caller must hold
// ConnectionManager.mu.
func peerDistinctGradeCTransportClasses(p *peerConn) int {
	if p == nil || p.transports == nil {
		return 0
	}
	seen := make(map[Protocol]bool, 2)
	for proto, tc := range p.transports {
		if tc == nil || tc.isDormant {
			continue
		}
		if tc.grade != GradeC {
			continue
		}
		// Collapse WS and TLS into their distinct Protocol keys — the
		// map key IS the class we care about.
		seen[proto] = true
	}
	return len(seen)
}

// hasActiveTransport returns true if any non-dormant transport exists.
func (p *peerConn) hasActiveTransport() bool {
	if p.transports == nil {
		return p.state == PeerConnected
	}
	for _, tc := range p.transports {
		if tc != nil && !tc.isDormant {
			return true
		}
	}
	return false
}

// addTransport associates a transportConn with the peer under its
// protocol key. Pre-v0.0.218 the map was strict single-slot per
// protocol: a second active session of the same protocol arriving
// while the first was alive returned early without recording the
// new transport. The drift case that exposed: session A registers
// (transports[noise-udp] = A, multipath = [A]); upgrade flow
// produces session B on the same protocol; addTransport keeps A,
// multipath now has [A, B]; A subsequently closes; removeTransport
// for noise-udp wipes the slot; multipath still has B but
// transports has nothing for noise-udp — diagnostics + scan-loop
// gates that read peer.transports stop seeing the active path.
//
// v0.0.218: always install the freshest transportConn. The pair
// removeTransport(proto, tc) is now session-aware so a concurrent
// older-session close can't wipe a newer entry — see below.
func (p *peerConn) addTransport(tc *transportConn) {
	if p.transports == nil {
		p.transports = make(map[Protocol]*transportConn)
	}
	prev, replacing := p.transports[tc.protocol]
	p.transports[tc.protocol] = tc
	// Observed-good signal: an accepted transport proves the peer is
	// reachable right now, so collapse any escalated reconnect backoff
	// back to the boot default. Without this reset, a peer that flapped
	// up to maxCooldown earlier keeps that long backoff even after a
	// fresh transport lands, so the next transient drop waits minutes
	// before re-dial. Mirrors the success-path reset in connectPeer's
	// post-dial branch and the same reset in registerMeshSession.
	p.reconnectDelay = baseCooldown
	// [TRANSPORT-CHURN] high-signal log to diagnose WS/UDP imbalance.
	// Fires whenever a peer's transport slot is filled or replaced.
	action := "add"
	prevGrade := "-"
	if replacing && prev != nil {
		action = "replace"
		prevGrade = prev.grade.String()
	}
	totalProtos := make([]string, 0, len(p.transports))
	for proto := range p.transports {
		totalProtos = append(totalProtos, proto.String())
	}
	log.Printf("[TRANSPORT-CHURN] %s peer=%s proto=%s grade=%s prevGrade=%s active=%v",
		action, truncID(p.nodeID), tc.protocol, tc.grade, prevGrade, totalProtos)
}

// removeTransport removes a transport entry for the given protocol
// IF the current map entry references the supplied transportConn.
// Pass nil for tc to force-delete by protocol (legacy behaviour);
// any caller that still does should be migrated to the
// session-aware form so a stale-session cleanup goroutine can't
// race a fresher session into orphan-state. Returns true if the
// peer has no remaining active (non-dormant) transports after the
// removal attempt.
func (p *peerConn) removeTransport(proto Protocol, tc *transportConn) bool {
	removed := false
	if p.transports != nil {
		if tc == nil {
			if _, ok := p.transports[proto]; ok {
				delete(p.transports, proto)
				removed = true
			}
		} else if existing, ok := p.transports[proto]; ok && existing == tc {
			delete(p.transports, proto)
			removed = true
		}
		// else: a different (newer) transportConn now owns the slot;
		// leave it alone so dispatch-registry consumers keep seeing
		// the live path.
	}
	allDead := !p.hasActiveTransport()
	if removed {
		// [TRANSPORT-CHURN] paired with addTransport — diagnoses WS/UDP imbalance.
		remaining := make([]string, 0, len(p.transports))
		for prot := range p.transports {
			remaining = append(remaining, prot.String())
		}
		var lifetime time.Duration
		if tc != nil {
			lifetime = time.Since(tc.connectedAt)
		}
		log.Printf("[TRANSPORT-CHURN] remove peer=%s proto=%s lifetime=%v remaining=%v allDead=%v",
			truncID(p.nodeID), proto, lifetime, remaining, allDead)
	}
	return allDead
}

// getDormantTransport returns a dormant transport (for reactivation) or nil.
func (p *peerConn) getDormantTransport() *transportConn {
	if p.transports == nil {
		return nil
	}
	for _, tc := range p.transports {
		if tc != nil && tc.isDormant {
			return tc
		}
	}
	return nil
}

const (
	baseCooldown          = 10 * time.Second
	maxCooldown           = 2 * time.Minute
	chronicThreshold      = 60 * time.Second // connections dying < this are "chronic"
	chronicLimit          = 3                // N chronic failures = extended cooldown
	suppressThreshold     = 10               // consecutive dial failures → long suppression
	suppressCooldown      = 10 * time.Minute // suppression duration
	upgradeIntervalGradeB = 5 * time.Minute  // B-grade: try A every 5 min
	upgradeIntervalGradeC = 1 * time.Minute  // C/D-grade: try upgrade every 1 min
	scanInterval          = 20 * time.Second

	// maxInitialScanJitter bounds the per-node deterministic delay before the
	// first connect scan (see ConnectionManager.Start). Sized at ~one scan
	// interval so a fleet of nodes spreads across a full window — with a
	// handful of nodes that is a couple of seconds apart, far more than the
	// sub-second a session needs to establish, which is enough to break the
	// all-pairs simultaneous-dial race without materially delaying convergence.
	maxInitialScanJitter = 18 * time.Second

	// dialOwnershipGrace bounds how long the higher-nodeID peer in a pair
	// waits for the lower-nodeID peer to dial it before dialing anyway.
	// One scan interval plus a margin gives the owning side a clean head
	// start: it dials on tick T, the handshake completes within seconds,
	// and the inbound session moves the peer to PeerConnected before the
	// deferring side's next tick at T+scanInterval. Past the grace the
	// deferring side dials regardless, so a dead or unreachable owner can
	// never strand the pair.
	dialOwnershipGrace = scanInterval + 5*time.Second

	// dedupRejectCooldown is how long the EnsureK multipath dialer holds
	// off re-dialing a peer after registerMeshSession rejected one of its
	// sessions as a duplicate. A dedup reject is proof the peer already
	// has a healthy session, so an immediate re-dial just rebuilds the
	// same duplicate and re-triggers the reject — a self-sustaining
	// reject→close→redial churn loop. Three minutes is long enough to
	// break the loop and let the dispatch session settle, short enough
	// that a genuine later need for path diversity (a better transport
	// becoming reachable) is only briefly delayed. The window refreshes
	// on every reject, so a peer stuck producing duplicates stays backed
	// off rather than churning.
	dedupRejectCooldown = 3 * time.Minute

	// Cooldown before a voluntarily-drained transport class is redialed.
	// Five minutes is long enough to prevent tick-rate churn (scanAndConnect
	// runs every 20s) but short enough that a genuine need to reconnect
	// (e.g., the preferred transport died) isn't permanently suppressed —
	// the dial-failure tracker still escalates cooldown if the peer
	// actually becomes unreachable.
	drainRedialCooldown = 5 * time.Minute
)

// ConnectionManager manages per-peer transport connections with failover.
type ConnectionManager struct {
	mu         sync.Mutex
	peers      map[string]*peerConn
	rt         *Runtime
	selfID     string
	selfRegion string // our region from config
	// ourOrigin holds our network origin prefix (e.g. "a:ba33" from a Fly
	// 6PN ULA fdaa:a:ba33:…). Stored via atomic.Pointer so a background
	// re-detection goroutine can update it after construction without
	// taking a lock on the hot path of isCrossOrigin().
	//
	// On Fly the initial discoverPrivateIP() call during construction can
	// race the platform's hostname/interface population, returning empty.
	// If that happens we kick off a retry loop (detectOurOriginLoop) that
	// updates this pointer once the platform settles. Until it's set,
	// isCrossOrigin() conservatively returns true so cross-org dials emit
	// the noise-UDP preamble — same-org dials pay 34 bytes of overhead
	// but the forwarder hairpin keeps working.
	ourOriginAtomic   atomic.Pointer[string]
	budget            *ConnectionBudget
	scaler            *ConnectionScaler
	connectionMap     *ConnectionMap
	drainMgr          *DrainManager
	peerStore         *FilePeerStore     // persisted peer list for cold-restart warm dials
	reputationTracker *ReputationTracker // rolling per-peer uptime/grade/drop score
	quicTr            *quictransport.QuicTransport
	wsTr              *wstransport.WebsocketTransport
	grpcTr            *grpctransport.GrpcTransport

	// Aether session tracking — maps nodeID → Aether session for native dispatch.
	// Used by AetherCaller (SessionFinder) for role-based RPC routing.
	dispatchMu   sync.RWMutex
	meshSessions map[string]aether.Session
	// sessionLifecycleStripes serialize admission against peer-wide zombie
	// cleanup for the same nodeID. A bounded stripe table avoids an
	// ever-growing lock map while retaining per-peer concurrency in the
	// common case; unrelated peers that hash to the same stripe merely
	// serialize their short admission/cleanup transactions.
	sessionLifecycleStripes [64]sync.Mutex

	// sessionHook fires after a session is installed/removed in meshSessions.
	// Set via SetSessionHook. Used by the swarm transport adapter to track
	// peer lifecycle without poking into ConnectionManager internals.
	sessionHookMu sync.RWMutex
	sessionHook   func(nodeID string, session aether.Session, joined bool)
	// meshSessionInitiators tracks whether WE initiated each registered
	// session (true = our outbound, false = inbound we accepted). The
	// dispatch tie-break for simultaneous-dial duplicates uses this to
	// compute the initiator's nodeID and converge with the peer on a
	// single winning session deterministically. Same key set as
	// meshSessions; entries are added/removed in lockstep under
	// dispatchMu.
	meshSessionInitiators map[string]bool
	// sessionInitiators is the session-pointer-keyed source-of-truth for
	// per-session initiator info. Updated whenever a session is
	// registered (alongside meshSessionInitiators). Survives the
	// "promoted standby loses its initiator" failure mode — the
	// multipath failover path looks up the new primary's initiator here
	// and rewrites meshSessionInitiators[nodeID] so subsequent dedup
	// tie-breaks compute symmetrically with the peer (was the
	// H-Mesh-Failover-Initiators finding). Entries are removed when the
	// session is fully torn down (unregisterMeshSession or zombie reap).
	sessionInitiators map[aether.Session]bool

	// stopOnce + stoppedCh implement deterministic shutdown. Stop() is
	// idempotent (stopOnce); the main Start() loop closes stoppedCh on
	// exit so a Stop caller can block until teardown completes. Without
	// this, shutdown relied solely on ctx cancellation with no ordering
	// guarantee for multipath managers / per-session goroutines —
	// reviewers couldn't reason about the relative order of teardown
	// versus concurrent dial-completion callbacks.
	stopOnce  sync.Once
	stoppedCh chan struct{}
	// sessionRegisteredAt tracks when each meshSessions entry was
	// installed. Used by the same-initiator dedup branch to
	// distinguish a mature session (peer redial = asymmetric half-
	// close, replace) from a fresh session (peer redial = likely a
	// peer-side dial-loop bug, probe-and-reject). Same key set as
	// meshSessions; updated whenever a session is installed or
	// replaced.
	sessionRegisteredAt map[string]time.Time
	// dedupRejectAt records when registerMeshSession last rejected a
	// duplicate session for a peer (tie-break loser, same-initiator
	// duplicate, or cannot-coexist). The EnsureK multipath dialer
	// consults this via inDedupCooldown: a peer whose duplicate dial
	// was just rejected demonstrably already has a healthy session, so
	// re-dialing within dedupRejectCooldown only re-creates the same
	// duplicate and re-triggers the reject — the reject→close→redial
	// loop that IS fleet churn. Stamping here turns every dedup reject
	// into a back-off signal so the loop cannot self-sustain. Guarded
	// by dispatchMu alongside meshSessions.
	//
	// Keyed by (nodeID, protocol) — composite string
	// "nodeID|proto" — so a WS reject doesn't block a noise-UDP
	// upgrade dial. Before this, the field was keyed by nodeID alone,
	// meaning any reject on any protocol suppressed EnsureK's path-
	// building across ALL protocols for the full 3-minute cooldown.
	// Helpers dedupRejectKey/dedupRejectAnyForPeer encapsulate the
	// composite-key access pattern.
	dedupRejectAt map[string]time.Time

	// Bidirectional RPC channels on Stream 1, keyed by SESSION pointer
	// (not nodeID). Each session has its own stream-1 BidiRPC; under
	// multipath, several sessions can coexist for a single peer and
	// each carries its own bidi. Keying by session means a session's
	// death only invalidates that session's bidi — sibling sessions'
	// bidis remain reachable.
	//
	// Why session-keyed instead of nodeID-keyed:
	//
	// A nodeID-keyed map stores only the LATEST registered bidi per peer,
	// because each AcceptMeshConnection overwrites the entry. A multipath
	// standby's registration then clobbers the primary's bidi entry, and
	// when the OLDER path dies the failover cleanup deletes the entry —
	// but that entry is the
	// SURVIVING (newer) session's bidi. Dispatch then fell through to
	// dynamic-stream every call, adding 1 RTT per RPC (≈100 ms on
	// cross-region WAN). Observable as a uniform latency floor shift
	// after any failover event on the fleet.
	//
	// Session-keyed eliminates that race entirely: each session's bidi
	// lives independently of the others, GetBidiRPC routes through the
	// active session pointer from GetMeshSession, and a session's
	// cleanup path drops just its own entry.
	bidisBySession map[aether.Session]*BidiRPC

	// Per-address dial scoring keyed by (peer, transport, address).
	// Drives bestAddress when more than one candidate IP/hostname is
	// known for a transport, demotes addresses that consistently fail
	// to connect, and ages out unused entries via PruneOlderThan.
	addressTracker *quality.AddressTracker

	// Aether multipath managers — per-peer active/standby path management.
	multipathMu       sync.Mutex
	multipathManagers map[string]*multipath.Manager

	// Quality tracker — single shared instance feeds all per-peer
	// multipath managers and the dial-cooldown helpers. Holds the
	// longer-window EMA state (baseline RTT, throughput, consecutive
	// failures, reliability, stability, dial-failure count, dial
	// cooldown expiry) keyed by (peer, transport) so the relevant
	// history survives session teardown and reconnect cycles.
	qualityTracker *quality.Tracker

	// Aether 0-RTT resume token store — caches session tokens for fast reconnection.
	resumeStore resume.Store

	// Proving sessions — when upgrading transport grade, the old session is
	// kept alive for 60s as a fallback. If the new session dies during
	// proving, the old session is restored as primary so a failed upgrade
	// never leaves the peer unreachable.
	proving map[string]*provingSession

	// Gossip dedup — tracks which peers have an active gossip
	// initiator loop AND the grade of the underlying transport that
	// owns it. Separate from meshSessions because sessions are
	// registered before gossip starts. Pre-v0.0.220 the value was a
	// bare bool: whichever session connected first won the dedup
	// lock and held it until its own session closed, even when a
	// later higher-grade session was installed as the dispatch
	// primary. The deferred-migration pattern that produced was
	// the WS-pinning behaviour observed in production: WSS
	// bootstrap connects, gossip pins to WS, noise-udp upgrade
	// arrives but its initiator goroutine waits forever on the
	// dedup lock, the noise-udp session has no traffic flowing
	// through it, and aether's stall detector eventually closes
	// the noise-udp session — bouncing the peer back to WS
	// permanently.
	//
	// v0.0.220: stores a *gossipOwner per peer with the owner's
	// grade and a preemption channel. A new initiator with a
	// strictly higher grade can preempt the existing owner by
	// cancelling its derived gossip context, forcing
	// RunGossipLoopWithPeer to return; the outer initiator loop
	// then sees the dedup slot is free (or that ours is now the
	// owner) and takes over without disturbing the lower-grade
	// session itself, which continues to carry RPC, keepalive,
	// and control traffic.
	gossipMu     sync.Mutex
	gossipActive map[string]*gossipOwner

	// multipath_failover_total counts every time the ConnectionManager
	// promotes a standby path after a primary session failure. Incremented at
	// the OnPrimaryFailure call site in mesh_connection.go; a single multipath
	// takeover bumps this once regardless of how many paths exist. Monotonically
	// increasing — compare successive snapshots to get the per-interval rate.
	// Useful for distinguishing "path flap" (rate > 0 continuously) from "one-
	// time failover" (counter advances once then plateaus).
	multipathFailoverTotal atomic.Uint64

	// Cross-org noise-UDP forwarder dial telemetry. Counts every
	// dialWithProtocol call that sets MetadataKeyCrossOrgPreamble (i.e.
	// the dialer chose to engage the public-IPv4 anycast hairpin path
	// because peer.crossOrigin && proto==NoiseUDP && address is public).
	// Pair this with runtime.go's receive-side forwarder_drops_*
	// counters to localize why cross-Fly-org pairs stay on websocket:
	//   - Attempted near 0 + ws-pinned peers ⇒ dial selection isn't
	//     even trying the forwarder path (AddressTracker sticky-
	//     suppression on noise-UDP, or pickPath excluding it).
	//   - Attempted growing + Succeeded near 0 + forwarder_drops_* near 0
	//     ⇒ dial is firing but packets never reach the receiver
	//     (NAT/firewall/anycast misroute between Fly orgs).
	//   - Attempted growing + Succeeded near 0 + forwarder_drops_*
	//     growing ⇒ packets arrive but classifier or hairpin lookup
	//     drops them (e.g. target_unknown means LAD reach cache miss).
	// Surfaced via Runtime.MeshMetrics on /api/monitoring/mesh-debug.
	crossOrgPreambleDialsAttempted atomic.Uint64
	crossOrgPreambleDialsSucceeded atomic.Uint64
	crossOrgPreambleDialsFailed    atomic.Uint64

	// Cross-org dial funnel gates. Pinpoint which upstream stage drops
	// cross-org noise-UDP dials before they reach the preamble code.
	// Diagnostic decision tree (v0.0.350+v0.0.351 left preamble counters
	// stuck at 0, narrowing the gate to one of these stages):
	//   PickPathNoiseUDPIncluded == 0 → pickPath excludes NoiseUDP for
	//     cross-org peers (unlikely per scout but kept as the truth check)
	//   PickPathNoiseUDPIncluded > 0 && BestAddrPublicUDPCount == 0 →
	//     peers publish no public-scope UDP addresses (advertisement gap)
	//   BestAddrCandidatesEmpty == DialNoSuitableAddr > 0 → candidate
	//     pool empty after filter; same root as above, surfaced from the
	//     bestAddress branch
	//   BestAddrAllDead > 0 → AddressTracker sticky-suppression killed
	//     the candidate (30-min DeadUntil cooldown on 3+ failures)
	//   BestAddrNonAnycastSelected > 0 && PreambleDialsAttempted == 0 →
	//     peers advertise direct (non-anycast) public IPs; preamble
	//     correctly skipped, hairpin not needed
	crossOrgPickPathNoiseUDPIncluded   atomic.Uint64
	crossOrgBestAddrPublicUDPCount     atomic.Uint64
	crossOrgBestAddrCandidatesEmpty    atomic.Uint64
	crossOrgBestAddrAllDead            atomic.Uint64
	crossOrgBestAddrNonAnycastSelected atomic.Uint64
	crossOrgDialNoSuitableAddr         atomic.Uint64

	// Proactive transport-grade upgrade walker telemetry. Each tick of
	// tryProactiveUpgrades increments walkerTicks; the count of candidate
	// (peer, target-proto) pairs the snapshot found is summed into
	// walkerCandidates; probe outcomes increment walkerProbesSucceeded
	// (DialAndAcceptMesh handoff fired) / walkerProbesStallCooled (probe
	// failed, short retry on next tick) / walkerProbesEscalated (probe
	// hit consecutive-failure threshold and was demoted to the long
	// quality-tracker cooldown). Surface via /api/monitoring/mesh-debug
	// mesh_metrics so the walker's effectiveness is observable without
	// log scraping.
	//
	// walkerProbesSucceeded keeps its original semantics: it counts
	// every probe that completed the Noise handshake and handed off to
	// DialAndAcceptMesh. It is NOT a measure of upgrade durability —
	// a handoff that immediately enters the 60s proving window then
	// dies (UDP data-plane backpressure cascade, peer-side reject,
	// etc.) still increments walkerProbesSucceeded. To observe the
	// honest "upgrade actually stuck" rate, see the proving-stage
	// counters below.
	//
	// Per-stage attrition counters let us see WHERE walker probes die:
	//   walkerProbesStarted          → probeUpgrade entered (raw probe attempts)
	//   walkerProbesSucceeded        → handshake completed + handoff fired (existing semantics, unchanged)
	//   walkerProbesProving          → handoff resulted in a proving-window install
	//   walkerProbesProvingSucceeded → proving timer fired and confirmed the upgrade
	//   walkerProbesProvingFailed    → new session died during proving (revert-to-old fired)
	//
	// Healthy mesh: started ≈ succeeded ≈ proving ≈ proving_succeeded.
	// Big drop succeeded → proving means the upgrade reached handoff
	// but never entered the proving window (same-grade-reject, dedup
	// race, transport-equal short-circuit). Big drop proving →
	// proving_succeeded means new sessions die under load before the
	// 60s window completes — that's the case that motivated splitting
	// walkerProbesSucceeded into stages (handshake-honest but proving-
	// dishonest in the v0.0.395-class noise-UDP regression).
	walkerTicks                  atomic.Uint64
	walkerCandidates             atomic.Uint64
	walkerProbesStarted          atomic.Uint64
	walkerProbesSucceeded        atomic.Uint64
	walkerProbesProving          atomic.Uint64
	walkerProbesProvingSucceeded atomic.Uint64
	walkerProbesProvingFailed    atomic.Uint64
	walkerProbesStallCooled      atomic.Uint64
	walkerProbesEscalated        atomic.Uint64
	walkerProbesSkippedRace      atomic.Uint64 // peer already upgraded by another path between snapshot and probe
	walkerProbesSkippedSlot      atomic.Uint64 // tryDial returned false (slot cooled down)

	// walkerPendingSessions tags peer nodeIDs whose latest dial was
	// initiated by the proactive upgrade walker, so registerMeshSession's
	// upgrade branch can mark the resulting provingSession as walker-
	// owned and bill subsequent proving-window success/failure to the
	// walker's per-stage counters. Cleared by registerMeshSession when
	// the tag is consumed; if the dial never reaches register (handoff
	// goroutine dropped the session before AcceptMeshConnection ran)
	// the entry is garbage-collected by walkerPendingSessions's bounded
	// drain in tryProactiveUpgrades's next tick using the stored
	// mark-time TTL.
	//
	// Keying by peerNodeID (rather than session pointer) is the only
	// shared identity available across the two call sites: the mark
	// site holds *aether.BaseConnection from dialWithProtocol, while
	// the consume site holds an aether.Session created freshly by
	// SetupMeshSession (different pointer). Some adapters (gRPC, Relay)
	// also return nil from NetConn() so the underlying net.Conn can't
	// be used as a universal shared key either. Collision tradeoff:
	// only matters when two concurrent walker probes to the same peer
	// race between mark and consume — bounded by the per-peer
	// walkerProbeAt rate limit (>= upgradeIntervalGradeC, ~1-5min) and
	// the per-(peer,proto) tryDial slot. Acceptable for counter
	// attribution.
	walkerPendingMu       sync.Mutex
	walkerPendingSessions map[string]time.Time

	// Consecutive stall tracking per (peer, target-proto). Reset on
	// successful probe. Drives the upgradeStallEscalationThreshold gate
	// so a truly-unreachable transport stops burning probe slots after
	// a bounded number of fast retries — see probeUpgrade.
	walkerStallMu sync.Mutex
	walkerStalls  map[walkerStallKey]int

	// Role-affinity cache + telemetry. Computed under Rebalance to
	// bump TargetConnections for peers whose published roles intersect
	// the local node's interest set. See role_affinity.go for the
	// three-tier scoring model.
	affinityCacheMu  sync.RWMutex
	affinityCache    map[string]struct{}
	roleAffinityStat roleAffinityTelemetry

	// walkerProbeAt records the last upgrade-probe time for each peer
	// so the grade-adaptive cadence (minProbeIntervalForGrade) can rate-
	// limit probes per-peer without re-checking the whole peer.peers
	// map for state. Written under walkerProbeMu by the snapshot path;
	// read by the same path on the next tick.
	walkerProbeMu sync.Mutex
	walkerProbeAt map[string]time.Time

	// walkerWakeCh is a non-blocking signal channel that lets external
	// PeerRecord ingest (AddressTable.onRecord) wake the upgrade walker
	// when a fresh PeerRecord adds a Tier-0 (Scope=private + Transport=
	// NOISE_UDP, i.e. Fly 6PN ULA) address for an already-connected peer.
	// Without this signal the walker only fires on its 30s ticker; a same-
	// org peer on WS waits up to a full upgradeWalkInterval before getting
	// a noise-UDP probe after the 6PN address actually arrives via gossip.
	//
	// Buffer size 1 + drop-on-full: signals do not pile up — one pending
	// wake is enough to guarantee the walker runs once after the burst.
	// The walker itself enforces per-peer rate limits via walkerProbeAt,
	// so multiple PeerRecords landing inside the 100ms-ish dispatch window
	// collapse into a single tryProactiveUpgrades pass.
	walkerWakeCh chan struct{}

	// walkerWakeStats — number of SignalWalkerWake calls that successfully
	// queued (delivered) versus those that found the channel already full
	// (coalesced). Exposed for monitoring so the wake plumbing's health is
	// observable alongside the ticker-driven walkerTicks counter.
	walkerWakeDelivered atomic.Uint64
	walkerWakeCoalesced atomic.Uint64

	// unregisterSkippedNotOwner counts how often unregisterMeshSession's
	// identity guard short-circuits because the dispatch entry already
	// points at a newer session than the caller owned. Each increment
	// represents one case where, pre-guard, a higher-grade upgrade
	// would have been silently removed by an obsolete cleanup goroutine.
	// unregisterDeleted counts cleanups that did mutate dispatch so the
	// two together quantify how often the guard mattered.
	unregisterSkippedNotOwner atomic.Uint64
	unregisterDeleted         atomic.Uint64
}

func (m *ConnectionManager) sessionLifecycleLock(nodeID string) *sync.Mutex {
	h := fnv.New64a()
	_, _ = h.Write([]byte(nodeID))
	return &m.sessionLifecycleStripes[h.Sum64()%uint64(len(m.sessionLifecycleStripes))]
}

// NewConnectionManager creates a new manager with all available transports.
func NewConnectionManager(rt *Runtime) *ConnectionManager {
	ourOrigin := ""
	if privateIP := rt.discoverPrivateIP(); privateIP != "" {
		ourOrigin = extractOriginPrefix(privateIP)
	}

	// QUIC transport — shares UDP port 41641 with Noise via DemuxPacketConn.
	// The Noise listener classifies packets and routes QUIC packets to the demux.
	var quicTr *quictransport.QuicTransport
	qt, err := quictransport.NewQuicTransport(quictransport.QuicTransportConfig{
		LocalNode:          rt.identity.NodeID,
		PrivateKey:         rt.identity.PrivateKey,
		Allow0RTT:          true,
		ClientSessionCache: tls.NewLRUClientSessionCache(32),
	})
	if err == nil {
		quicTr = qt
		// Wire QUIC to the shared Noise UDP socket via DemuxPacketConn
		if pktConn := rt.getNoiseQUICPacketConn(); pktConn != nil {
			qt.SetPacketConn(pktConn)
			log.Printf("[PEERS] QUIC transport initialized (multiplexed on port 41641)")
		} else {
			log.Printf("[PEERS] QUIC transport initialized (own socket, no Noise listener available)")
		}
	}

	// Initialize WebSocket transport (uses same identity as Noise)
	wsTr, _ := wstransport.NewWebsocketTransport(wstransport.WebsocketTransportConfig{
		LocalNode:  rt.identity.NodeID,
		PrivateKey: rt.identity.PrivateKey,
	})

	// Initialize gRPC transport
	grpcTr, _ := grpctransport.NewGrpcTransport(grpctransport.GrpcTransportConfig{
		LocalNode:  rt.identity.NodeID,
		PrivateKey: rt.identity.PrivateKey,
	})

	// Resolve our region from platform config
	selfRegion := ""
	if rt.cfg.Platform != nil {
		selfRegion = rt.cfg.Platform.Region()
	}

	// Build connection budget (apply config overrides if provided)
	budget := DefaultConnectionBudget()
	if rt.cfg.ConnectionBudget != nil {
		cb := rt.cfg.ConnectionBudget
		if cb.MaxPerPeer > 0 {
			budget.MaxPerPeer = cb.MaxPerPeer
		}
		if cb.MaxTotal > 0 {
			budget.MaxTotal = cb.MaxTotal
		}
		if cb.MinPerPeer > 0 {
			budget.MinPerPeer = cb.MinPerPeer
		}
		if cb.PreferredPerPeer > 0 {
			budget.PreferredPerPeer = cb.PreferredPerPeer
		}
		if cb.CrossRegionBonus >= 0 {
			budget.CrossRegionBonus = cb.CrossRegionBonus
		}
	}

	mgr := &ConnectionManager{
		peers:        make(map[string]*peerConn),
		rt:           rt,
		selfID:       string(rt.identity.NodeID),
		selfRegion:   selfRegion,
		budget:       budget,
		quicTr:       quicTr,
		wsTr:         wsTr,
		grpcTr:       grpcTr,
		walkerWakeCh: make(chan struct{}, 1),
	}
	// Seed ourOrigin atomically. With PlatformInfo.PrivateIP()
	// the platform-authoritative source resolves synchronously at boot
	// on every supported cloud, so ourOrigin should be populated here.
	// Empty is now a hard misconfiguration signal (non-cloud platform
	// with no fdaa: interface AND no cfg.PrivateIP), not a transient
	// boot race — log loudly so operators see it instead of silently
	// running with the conservative-cross-org default.
	mgr.setOurOrigin(ourOrigin)
	if ourOrigin == "" {
		log.Printf("[PEERS] ERROR: ourOrigin empty at construction — Platform.PrivateIP() returned empty AND no fdaa: interface AND cfg.PrivateIP unset. Cross-org forwarder will treat ALL peers as cross-org. Set cfg.PrivateIP to fix this on non-cloud platforms.")
	}
	mgr.connectionMap = NewConnectionMap()
	mgr.drainMgr = NewDrainManager(func(peerNodeID, transport string, startedAt time.Time) {
		mgr.closeDrainedConnection(peerNodeID, transport, startedAt)
	})
	mgr.scaler = NewConnectionScaler(mgr, func(event ConnectionEvent) {
		// Default handler: log only. Endpoints that want to feed events into
		// an external topology API should register an AddEventSubscriber.
		dbgPeers.Printf("Connection event: peer=%s reason=%s grade=%s->%s",
			truncID(event.PeerNodeID), event.Reason, event.OldGrade, event.NewGrade)
	})
	mgr.scaler.connectionMap = mgr.connectionMap

	// Event-driven LAD publisher — writes a latency record immediately on
	// every connection lifecycle change so the local directory cache
	// reflects the live peer table without waiting for the next
	// publishPeerLatencies tick or a successful gossip round. Critical
	// for recovery from gossip stalls: even if LAD delta exchanges are
	// deadlocked, each node's own ledger always reflects the current
	// state and any peer that can gossip with us picks up the fresh
	// records on next exchange.
	mgr.scaler.AddEventSubscriber(rt.publishPeerStateEvent)
	mgr.reputationTracker = NewReputationTracker(mgr.scaler.EventLog())
	// Use file-based resume token store for persistence across restarts.
	// Falls back to in-memory store if DataDir is unavailable.
	if rt.cfg.DataDir != "" {
		if fs, err := resume.NewFileStore(rt.cfg.DataDir + "/hwp-resume"); err == nil {
			mgr.resumeStore = fs
		} else {
			mgr.resumeStore = resume.NewMemoryStore()
		}
	} else {
		mgr.resumeStore = resume.NewMemoryStore()
	}

	// Quality tracker: single shared instance feeds every per-peer
	// multipath manager so failure history survives session teardown
	// and reconnect. Initialised here so addMultipathSession (called
	// before Start) can safely reference it.
	mgr.qualityTracker = quality.NewTracker()

	return mgr
}

// Start runs the connection management loop. Returns when ctx is cancelled
// OR Stop() is called. The loop runs in the caller's goroutine — callers
// that need the loop to live independently must run Start in their own
// `go m.Start(ctx)`.
//
// For deterministic shutdown ordering — drain multipath managers, close
// mesh sessions, then exit — use Stop() instead of (or in addition to)
// cancelling ctx. Stop blocks until the main loop has fully exited.
// initialScanJitter returns a deterministic per-node delay in
// [0, maxInitialScanJitter) derived from the node's own ID. Deterministic
// rather than random so the fleet's nodes spread evenly across the window and
// the value is reproducible in logs/tests. Returns 0 for an empty ID or a
// non-positive cap (feature off).
func initialScanJitter(selfID string) time.Duration {
	if maxInitialScanJitter <= 0 || selfID == "" {
		return 0
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(selfID))
	return time.Duration(h.Sum64() % uint64(maxInitialScanJitter))
}

func (m *ConnectionManager) Start(ctx context.Context) {
	if m.stoppedCh == nil {
		m.stoppedCh = make(chan struct{})
	}
	defer close(m.stoppedCh)
	log.Printf("[PEERS] Connection manager started (self=%s, org=%s, region=%s)", truncID(m.selfID), m.getOurOrigin(), m.selfRegion)

	// Start gRPC mesh listener (if transport is configured)
	go m.rt.startGRPCAcceptLoop(ctx)

	// Active reach-resync backstop: periodically re-merge the swarm
	// AddressTable for connected peers whose peer.addresses lacks a
	// UDP entry. Targets the late-record race where the initial
	// scanAndConnect merge ran before the AddressTable held the
	// peer's record and never re-ran for that nodeID, leaving the
	// peer reachable via WS but invisible to noise-UDP dial path.
	go m.runReachResyncWalker(ctx, reachResyncInterval)

	// Jitter the FIRST connect scan to desynchronise a fleet-wide simultaneous
	// reform. When the whole fleet restarts at once (coordinated redeploy,
	// platform event, or a full stop/start), every node running scanAndConnect
	// at the same instant produces all-pairs dial races: A dials B while B dials
	// A, creating dual sessions whose dedup/tie-break close cascades a redial
	// (see AcceptMeshConnection Fix B), so no session survives long enough to
	// complete its gossip-stream setup. Measured on a full-fleet reform: 0
	// gossip exchanges, members frozen at 2, no peer addresses learned, and
	// therefore "no suitable address for noise-udp" — UDP never even attempted.
	//
	// The node still ACCEPTS inbound dials during the jitter window (the accept
	// path is independent of this scan), so sessions form immediately; only this
	// node's OWN outbound scan is deferred. An earlier-scanning peer dials and
	// establishes a single session; when this node finally scans, EnsureK sees
	// the existing session and does not create a racing duplicate. That turns a
	// simultaneous reform into a gradual one — the case that provably works
	// (the fleet only ever formed via staggered deploys).
	if j := initialScanJitter(m.selfID); j > 0 {
		log.Printf("[PEERS] initial connect scan jittered by %v (desync fleet reform)", j)
		select {
		case <-ctx.Done():
			return
		case <-time.After(j):
		}
	}

	// Initial scan + connect
	m.scanAndConnect(ctx)

	scanTicker := time.NewTicker(scanInterval)
	rebalanceTicker := time.NewTicker(scanInterval)      // rebalance on same cadence as scan
	priorityTicker := time.NewTicker(30 * time.Second)   // recompute per-connection priority from tenant reservations + RPC traffic
	rpcResetTicker := time.NewTicker(1 * time.Minute)    // reset per-minute RPC counters (sliding 1-minute window)
	convergeTicker := time.NewTicker(60 * time.Second)   // service convergence (merged from MeshConvergenceManager)
	congestionTicker := time.NewTicker(15 * time.Second) // feed reputation tracker with current RTT and grade per peer
	zombieTicker := time.NewTicker(10 * time.Second)     // sweep meshSessions for IsClosed entries the per-session cleanup goroutine missed
	// kRefreshTicker recomputes EnsureK targets per peer so RTT
	// spikes, drop accumulation, hotspot adjustments, and traffic-
	// weight changes propagate into the multipath manager's reconnect
	// loop without waiting for the next addMultipathSession event.
	kRefreshTicker := time.NewTicker(45 * time.Second)
	// upgradeTicker drives the proactive transport-grade upgrade walker.
	// EnsureK is reactive (fires only when a path dies). The walker is
	// proactive — every tick it probes every connected peer for any
	// strictly-greater-grade non-active transport that has a dialable
	// address. It guarantees that a peer that COULD be on noise-UDP IS on
	// noise-UDP within one upgradeWalkInterval of the underlying condition
	// allowing it.
	upgradeTicker := time.NewTicker(upgradeWalkInterval)
	defer scanTicker.Stop()
	defer rebalanceTicker.Stop()
	defer priorityTicker.Stop()
	defer rpcResetTicker.Stop()
	defer convergeTicker.Stop()
	defer congestionTicker.Stop()
	defer zombieTicker.Stop()
	defer kRefreshTicker.Stop()
	defer upgradeTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("[PEERS] Connection manager stopped")
			return
		case <-scanTicker.C:
			m.scanAndConnect(ctx)
			if m.addressTracker != nil {
				m.addressTracker.PruneOlderThan(4 * time.Hour)
			}
		case <-rebalanceTicker.C:
			if m.scaler != nil {
				m.scaler.Rebalance()
			}
		case <-priorityTicker.C:
			m.updatePriorities()
		case <-rpcResetTicker.C:
			m.resetRPCCounters()
		case <-convergeTicker.C:
			m.converge(ctx)
			m.pruneStalePeers()
		case <-congestionTicker.C:
			m.feedReputationFromRTT()
		case <-zombieTicker.C:
			m.sweepZombieSessions()
		case <-kRefreshTicker.C:
			m.refreshEnsureKTargets()
		case <-upgradeTicker.C:
			m.tryProactiveUpgrades(ctx)
		case <-m.walkerWakeCh:
			// Event-driven wake from AddressTable.onRecord — a fresh
			// PeerRecord delivered a Tier-0 (6PN ULA noise-UDP) address
			// for a peer we may already hold a lower-grade session for.
			// The walker re-snapshots every connected peer; per-peer
			// rate limits in walkerProbeAt absorb burst signals so a
			// flurry of PeerRecords within the same dispatch window
			// collapses into a single probe pass. Without this branch
			// the walker would wait up to upgradeWalkInterval (30s) to
			// discover the new address.
			m.tryProactiveUpgrades(ctx)
		}
	}
}

// Stop shuts the ConnectionManager down deterministically. Idempotent —
// repeated calls return immediately after the first. Order:
//
//  1. Cancel any pending dial budget (so in-flight scans abort)
//  2. Stop every multipath manager (each Stop is idempotent + blocks
//     until its EnsureK goroutine exits)
//  3. Wait for the Start loop to observe ctx.Done and close stoppedCh
//
// The caller is responsible for cancelling the ctx originally passed to
// Start — Stop only signals + drains state; the loop's tickers stop
// when its ctx fires.
//
// Returns once the Start loop has fully exited (stoppedCh closed). If
// Start was never called (stoppedCh nil), Stop only drains managers and
// returns.
func (m *ConnectionManager) Stop() {
	m.stopOnce.Do(func() {
		// 0. Abandon in-flight drains first. Each StartDrain leaves a monitor
		//    goroutine that fires closeDrainedConnection after its grace window;
		//    that callback releases a budget slot and cancels peer gossip on this
		//    manager, so it must not run once the teardown below has begun.
		if m.drainMgr != nil {
			m.drainMgr.Stop()
		}

		// 1. Snapshot + Stop all multipath managers. Each manager's
		//    Stop is idempotent and bounded — won't block here.
		m.multipathMu.Lock()
		mgrs := make([]interface{ Stop() }, 0, len(m.multipathManagers))
		for _, mgr := range m.multipathManagers {
			if mgr != nil {
				mgrs = append(mgrs, mgr)
			}
		}
		m.multipathMu.Unlock()
		for _, mgr := range mgrs {
			mgr.Stop()
		}
		// 2. Wait for the Start loop to finish (closed by its defer).
		//    Bypass the wait if Start was never run.
		if m.stoppedCh != nil {
			<-m.stoppedCh
		}
		log.Printf("[PEERS] Connection manager stopped (multipath managers drained)")
	})
}

// observerSilenceThreshold is the minimum gossip-silence required before
// the zombie sweep emits a propagating death-tombstone for a peer. The
// triple-gate from the liveness-tombstone-churn-latency workflow:
//
//	(a) session IsClosed() — built into sweepZombieSessions' selector
//	(b) we have the anchor role (non-anchors emit nothing; their
//	    attestations would be rejected by the swarm CRDT gate anyway)
//	(c) LAD has seen no gossip from this peer for at least this window
//
// One minute is conservative — well above any normal gossip silence on a
// healthy peer (gossip ticks every few seconds) but well below the
// 16-minute GossipLivenessTimeout the LAD passive-eviction timer uses.
// A peer that's flapping briefly never crosses this threshold and the
// CRDT auto-restores it on next live publish anyway.
const observerSilenceThreshold = 60 * time.Second

// shouldEmitObserverTombstone enforces the triple-gate before the zombie
// sweep emits a propagating death-tombstone for a peer. Returns true
// only when all three conditions are met: (a) sess.IsClosed() — the
// caller is already inside the zombie sweep so this is implicitly true,
// (b) we have the anchor role, and (c) LAD's last gossip-seen for the
// peer is older than observerSilenceThreshold.
//
// The self-anchor check uses swarm.Node.SelfRole() rather than
// RoleTable.PeerInfo(self). RoleTable is populated from gossiped
// PeerRecords — including our own, which round-trips via the mesh. On
// early boot (before our PeerRecord has been re-received) RoleTable
// returns ok=false for self, which would erroneously deny every
// tombstone the zombie sweep wants to emit. SelfRole reads the
// node-local atomic role field set at startup and is always correct.
func (m *ConnectionManager) shouldEmitObserverTombstone(nodeID string) bool {
	if m.rt == nil || m.rt.swarm == nil || m.rt.swarm.Node == nil {
		return false
	}
	if m.rt.swarm.Node.SelfRole() != swarm.RoleAnchor {
		return false
	}
	if m.rt.cache == nil {
		return false
	}
	lastSeen := m.rt.cache.LastGossipSeen(nodeID)
	if lastSeen.IsZero() {
		// Never seen in gossip — no evidence either way; refuse to
		// attest. A peer we have an aether session to but no gossip
		// from is most likely a not-yet-bootstrapped fresh peer.
		return false
	}
	return time.Since(lastSeen) >= observerSilenceThreshold
}

// emitPeerTombstone emits the matched LAD and swarm tombstones for a
// peer the local node has locally confirmed dead. Pairs both signals so
// LAD (drained by EvictPeer immediately) and swarm RoleTable +
// AddressTable (drained by PublishObserverTombstone + K-of-N quorum)
// converge in lock-step. Callers MUST gate via shouldEmitObserverTombstone
// before invoking this — the helper does no gating of its own.
func (m *ConnectionManager) emitPeerTombstone(nodeID, reason string) {
	if m.rt == nil {
		return
	}
	if m.rt.cache != nil {
		m.rt.cache.EvictPeer(nodeID, reason)
	}
	if m.rt.swarm != nil && m.rt.swarm.Node != nil {
		target := swarm.NodeID(nodeID)
		// Emit observer attestations on every PeerRecord-bearing topic
		// the swarm carries today so K-of-N convergence drains all of
		// them in one round. Today there is one such topic
		// (topicFleetPeer); future role/reach topics should be added
		// here so the tombstone fans out to every directory.
		if err := m.rt.swarm.Node.PublishObserverTombstone(topicFleetPeer, target); err != nil {
			log.Printf("[OBSERVER-TOMBSTONE] PublishObserverTombstone topic=%s peer=%s reason=%s err=%v",
				topicFleetPeer, truncID(nodeID), reason, err)
			return
		}
		log.Printf("[OBSERVER-TOMBSTONE] emitted topic=%s peer=%s reason=%s (quorum=%d, window=%s)",
			topicFleetPeer, truncID(nodeID), reason, swarm.DefaultObserverQuorum, swarm.DefaultObserverCorroborationWindow)
	}
}

// sweepZombieSessions reaps mesh sessions whose underlying transport has
// closed but whose per-session cleanup goroutine in AcceptMeshConnection
// never reached unregisterMeshSession. The most common cause is a session
// that closed via aether's internal watchdog/migration path while the
// gossip-responder loop kept spinning (only connCtx cancellation breaks
// it), and the keepalive goroutine's prior Ping implementation reported
// stale OK because it only validated writeFrame's queued-write success
// rather than actual peer reachability.
//
// Aether v0.0.22 fixed Ping at the source and mesh_connection.go's
// keepalive now fast-fails on IsClosed, so the per-session cleanup
// path is again reliable. This sweeper is the recovery net: if a
// session ever does slip past those defences, dispatch consumers see
// a fresh-state peer within at most the ticker interval (10s) instead
// of an indefinite zombie.
type zombieSessionSnapshot struct {
	nodeID string
	sess   aether.Session
}

func (m *ConnectionManager) sweepZombieSessions() {
	// Phase 1: zombie meshSessions entries — registered but underlying
	// session is IsClosed. Snapshot under RLock so we don't hold
	// dispatchMu while calling unregisterMeshSession (write-locks itself).
	// Capture the session pointer alongside nodeID so the unregister can
	// be identity-guarded — if a NEWER session has replaced this zombie
	// between snapshot and unregister, the guard skips the delete and
	// the live session is preserved.
	m.dispatchMu.RLock()
	zombies := make([]zombieSessionSnapshot, 0)
	for nodeID, sess := range m.meshSessions {
		if sess == nil || sess.IsClosed() {
			zombies = append(zombies, zombieSessionSnapshot{nodeID: nodeID, sess: sess})
		}
	}
	m.dispatchMu.RUnlock()

	m.reapZombieSessionSnapshots(zombies)

	// Phase 2: orphan multipath managers — manager exists for a peer
	// that no longer has a meshSessions entry (often because the
	// previous sweep phase reaped only one of two paths and the other
	// path subsequently closed without a follow-up sweep iteration
	// noticing because meshSessions[node] was already gone). Reap
	// closed paths and drop empty managers so dispatch doesn't pick
	// a corpse via mesh_session_finder's multipath fallback, and so
	// scanAndConnect's "addMultipathSession will recreate" path
	// doesn't see a stale manager and skip recreation.
	m.dispatchMu.RLock()
	hasMeshSession := make(map[string]bool, len(m.meshSessions))
	for nodeID := range m.meshSessions {
		hasMeshSession[nodeID] = true
	}
	m.dispatchMu.RUnlock()

	m.multipathMu.Lock()
	orphans := make([]string, 0)
	for nodeID := range m.multipathManagers {
		if !hasMeshSession[nodeID] {
			orphans = append(orphans, nodeID)
		}
	}
	m.multipathMu.Unlock()

	for _, nodeID := range orphans {
		lifecycleMu := m.sessionLifecycleLock(nodeID)
		lifecycleMu.Lock()
		// The orphan list was built from a snapshot. A fresh admission may
		// have completed since then; revalidate under the same lifecycle
		// gate used by AcceptMeshConnection before touching peer-wide state.
		m.dispatchMu.RLock()
		_, hasSession := m.meshSessions[nodeID]
		m.dispatchMu.RUnlock()
		if hasSession {
			lifecycleMu.Unlock()
			continue
		}
		log.Printf("[ZOMBIE-SWEEP] reaping orphan multipath manager for peer=%s", truncID(nodeID))
		m.reapMultipathClosed(nodeID)
		// Force peer back to disconnected so scan reconnects — same
		// rationale as Phase 1.
		m.mu.Lock()
		if p, ok := m.peers[nodeID]; ok && p.state == PeerConnected {
			p.state = PeerDisconnected
			p.connCount = 0
		}
		m.mu.Unlock()
		lifecycleMu.Unlock()
	}

	// Phase 3: stale proving sessions. Each upgrade installs a
	// provingSession with a 60-second timer that auto-cleans on
	// expiry. The timer firing inside sync.Once-protected code is
	// reliable in practice but a panic in the proving goroutine, a
	// late session-replace racing the AfterFunc, or any future change
	// that bypasses the proving cleanup path can leave an entry
	// pinning two aether.Session references (oldSession + newSession)
	// indefinitely. 90 s = 60 s nominal proving window + 30 s grace
	// for clock skew and AfterFunc scheduling jitter.
	const provingMaxAge = 90 * time.Second
	provingCutoff := time.Now().Add(-provingMaxAge)
	m.dispatchMu.Lock()
	staleProving := make([]string, 0)
	for nodeID, ps := range m.proving {
		if ps == nil || ps.startedAt.Before(provingCutoff) {
			staleProving = append(staleProving, nodeID)
		}
	}
	for _, nodeID := range staleProving {
		ps := m.proving[nodeID]
		if ps != nil && ps.timer != nil {
			ps.timer.Stop()
		}
		delete(m.proving, nodeID)
	}
	m.dispatchMu.Unlock()
	if len(staleProving) > 0 {
		log.Printf("[ZOMBIE-SWEEP] reaped %d stale proving session(s) past %v window",
			len(staleProving), provingMaxAge)
	}

	// Phase 4: orphan gossipActive flags. Set true when a per-peer
	// gossip-initiator goroutine starts (mesh_connection.go:519);
	// deleted when the goroutine exits cleanly (line 539). A panic
	// in the gossip loop or an unexpected return path can leave the
	// flag set, blocking subsequent reconnects from re-running gossip
	// initiator logic. Reap entries for peers whose meshSessions slot
	// is empty — gossipActive without a session has nothing to
	// initiate. Bounded by peer count so unlikely to dominate memory,
	// but the side-effect (suppressed initiator) is the real issue.
	m.gossipMu.Lock()
	gossipCandidates := make([]string, 0, len(m.gossipActive))
	for nodeID := range m.gossipActive {
		gossipCandidates = append(gossipCandidates, nodeID)
	}
	m.gossipMu.Unlock()
	staleGossip := make([]string, 0)
	for _, nodeID := range gossipCandidates {
		lifecycleMu := m.sessionLifecycleLock(nodeID)
		lifecycleMu.Lock()
		m.dispatchMu.RLock()
		_, hasSession := m.meshSessions[nodeID]
		m.dispatchMu.RUnlock()
		if !hasSession {
			m.gossipMu.Lock()
			if _, present := m.gossipActive[nodeID]; present {
				delete(m.gossipActive, nodeID)
				staleGossip = append(staleGossip, nodeID)
			}
			m.gossipMu.Unlock()
		}
		lifecycleMu.Unlock()
	}
	if len(staleGossip) > 0 {
		log.Printf("[ZOMBIE-SWEEP] cleared %d orphan gossipActive flag(s) for peers without sessions",
			len(staleGossip))
	}
}

// reapZombieSessionSnapshots applies Phase 1 only when unregister confirms
// that the captured dispatch identity was removed. A false result means a
// newer live session owns the peer, so multipath, peer state, LAD liveness and
// tombstone admission must all remain untouched.
func (m *ConnectionManager) reapZombieSessionSnapshots(zombies []zombieSessionSnapshot) {
	for _, z := range zombies {
		nodeID := z.nodeID
		log.Printf("[ZOMBIE-SWEEP] reaping closed session for peer=%s", truncID(nodeID))
		lifecycleMu := m.sessionLifecycleLock(nodeID)
		lifecycleMu.Lock()
		if !m.unregisterMeshSessionWithoutHook(nodeID, z.sess) {
			lifecycleMu.Unlock()
			continue
		}
		m.reapMultipathClosed(nodeID)
		// Drive the peer back to PeerDisconnected so scanAndConnect's
		// state machine re-dials. Without this, peer.state stays
		// PeerConnected (set during accept) and the scan loop's
		// "already connected" gate suppresses the redial — we'd reap
		// the session and never reconnect.
		m.mu.Lock()
		if p, ok := m.peers[nodeID]; ok && p.state == PeerConnected {
			p.state = PeerDisconnected
			p.connCount = 0
		}
		m.mu.Unlock()
		// Drop the LAD cache liveness override too — the session is
		// gone so the cache's normal timeout-based eviction is the
		// right authority again.
		if m.rt != nil && m.rt.cache != nil {
			m.rt.cache.SetGossipLivenessOverride(nodeID, false)
		}

		// Propagating-tombstone emit. Two preconditions:
		//   (a) we are an anchor — non-anchor observer attestations are
		//       rejected by the CRDT's anchor-role gate anyway, and
		//       emitting LAD EvictPeer from non-anchors floods the mesh
		//       with redundant tombstones that race with anchor-emitted
		//       ones on HLC ordering.
		//   (b) gossip-silence > observerSilenceThreshold (60s) — a peer
		//       whose session just closed but whose gossip we received
		//       within the last minute is probably mid-reconnect, not
		//       dead.
		// Anchors run BOTH emissions so swarm RoleTable/AddressTable
		// (drained by PublishObserverTombstone+K-of-N quorum) and LAD
		// (drained by EvictPeer immediately) converge in lock-step.
		if m.shouldEmitObserverTombstone(nodeID) {
			m.emitPeerTombstone(nodeID, "keepalive-dead")
		}
		lifecycleMu.Unlock()
		// Keep the external hook outside both dispatchMu and the lifecycle
		// transaction: callbacks may re-enter registration. Firing it only
		// after cleanup also prevents that fresh admission from being erased
		// by the stale zombie transaction that announced the departure.
		m.fireSessionHook(nodeID, nil, false)
	}
}

// reapMultipathClosed removes any closed sessions from the peer's
// multipath manager and drops the manager entirely if no live paths
// remain. Called by the zombie sweeper. multipath.Manager has no
// bulk-purge — snapshot AllSessions under multipathMu, then call
// RemovePath for each closed one. The drop-or-keep decision happens
// in the SAME critical section as the membership check so a concurrent
// addMultipathSession cannot install a fresh manager between the
// "is empty?" check and the delete (was H-Mesh-ReapRace).
//
// RemovePath itself takes the multipath.Manager's internal lock — which
// does not interact with multipathMu — so we release multipathMu around
// the RemovePath calls and re-acquire it for the empty-check. The fix
// for the race is in the FINAL critical section: we re-validate the
// manager pointer hasn't been replaced AND that PathCount is still 0
// before calling Stop+delete, so a concurrent addMultipathSession
// (which would install a non-zero PathCount before its goroutine even
// observes the manager) loses cleanly.
func (m *ConnectionManager) reapMultipathClosed(nodeID string) {
	m.multipathMu.Lock()
	mgr, hasMgr := m.multipathManagers[nodeID]
	if !hasMgr {
		m.multipathMu.Unlock()
		return
	}
	var closedPaths []aether.Session
	for _, sess := range mgr.AllSessions() {
		if sess == nil || sess.IsClosed() {
			closedPaths = append(closedPaths, sess)
		}
	}
	m.multipathMu.Unlock()

	for _, sess := range closedPaths {
		mgr.RemovePath(sess)
	}

	// Final-decision critical section: re-check identity AND count under
	// the same lock that addMultipathSession holds to register a new
	// session. If a concurrent add installed a different manager (or
	// added a path to ours after our snapshot), drop the reap quietly
	// — the next sweep tick will reconsider with fresh state.
	m.multipathMu.Lock()
	defer m.multipathMu.Unlock()
	current, stillPresent := m.multipathManagers[nodeID]
	if !stillPresent || current != mgr {
		return
	}
	if mgr.PathCount() != 0 {
		return
	}
	mgr.Stop()
	delete(m.multipathManagers, nodeID)
}

// stalePeerTTL is the age after which a PeerDisconnected entry with a
// once-successful connection is evicted from the in-memory peer map.
// PEX and LAD will re-learn the peer if it comes back, so this is a
// safe memory-bound on long-running nodes in high-churn fleets. 15
// minutes chosen so a stuck PeerDisconnected entry (e.g., one that
// never finished its initial gossip round with the local node) ages
// out fast enough that the topology view self-heals within observable
// human timescales — not 2 hours.
const stalePeerTTL = 15 * time.Minute

// staleDiscoveryTTL is the age after which a peer entry that was
// discovered via PEX or LAD but never successfully connected (every
// dial failed across every protocol) is evicted. Pre-v0.0.217
// pruneStalePeers skipped these entries entirely because the predicate
// keyed on lastConnected.IsZero() — meaning "no successful Accept
// ever recorded" was treated as "freshly added, don't prune". The
// real signal there is "dialed and failed", which over a half-hour
// window is strong evidence the peer ID is dead (renamed, retired,
// permanently NATed off the public mesh). 30 minutes is intentionally
// longer than stalePeerTTL because a never-connected peer might just
// be slow to come up the first time (cold-start, anchor warming) and
// we don't want to lose its address before the budget+backoff has
// finished its first scan-loop pass; 30 min covers ~90 scanInterval
// ticks which is well past first-contact.
const staleDiscoveryTTL = 30 * time.Minute

// pruneStalePeers evicts peer entries that are no longer worth tracking.
// Two pruning paths run on every tick:
//
//  1. PeerDisconnected with a non-zero lastConnected (peer connected
//     successfully at some point, then dropped) — pruned after
//     stalePeerTTL since lastConnected.
//
//  2. PeerDisconnected or PeerDiscovered with discoveredAt older than
//     staleDiscoveryTTL and lastConnected.IsZero() (peer was added to
//     the map by PEX/LAD/discoverers, dialed across the protocol
//     fallback chain, every dial failed, and we never received a
//     handshake). Without this sweep such entries leak indefinitely.
//
// In both paths we preserve peers that still have active transports
// or are mid-drain, so an in-progress graceful shutdown never has its
// peerConn ripped out from under it. Keeps the peers map and its
// satellite maps (rpcCounters, multipath, peerStore, addressTracker)
// bounded under PEX-driven discovery of many transient peers.
func (m *ConnectionManager) pruneStalePeers() {
	now := time.Now()
	disconnectCutoff := now.Add(-stalePeerTTL)
	discoveryCutoff := now.Add(-staleDiscoveryTTL)
	m.mu.Lock()
	pruned := 0
	prunedNeverConnected := 0
	evictedIDs := make([]string, 0)
	for id, p := range m.peers {
		// Preserve peers with active transports or in-progress drains.
		// DrainActive is the zero value and means "not draining" — a peer is
		// mid-drain when the field is DrainStarted, which Rebalance sets and
		// closeDrainedConnection clears.
		if p.connCount > 0 || p.drainState != DrainActive {
			continue
		}
		var prune bool
		switch p.state {
		case PeerDisconnected:
			if !p.lastConnected.IsZero() && p.lastConnected.Before(disconnectCutoff) {
				prune = true
			} else if p.lastConnected.IsZero() && !p.discoveredAt.IsZero() && p.discoveredAt.Before(discoveryCutoff) {
				prune = true
				prunedNeverConnected++
			}
		case PeerDiscovered:
			// Discovered but never even reached connectPeer — possible
			// when budget pressure starves dial attempts on a bursty
			// PEX/LAD discovery wave. Still age these out so a one-off
			// "peer X is in the directory" record doesn't pin memory
			// indefinitely on a node that never had budget for them.
			if !p.discoveredAt.IsZero() && p.discoveredAt.Before(discoveryCutoff) {
				prune = true
				prunedNeverConnected++
			}
		}
		if !prune {
			continue
		}
		delete(m.peers, id)
		evictedIDs = append(evictedIDs, id)
		pruned++
	}
	m.mu.Unlock()

	if pruned > 0 {
		// Drop the satellite maps for evicted peer IDs so memory
		// scales with currently-tracked peers, not all-peers-ever-seen.
		// Done outside m.mu to avoid lock-order issues with each
		// satellite's own lock; predicate re-takes m.mu briefly.
		notInPeers := func(id string) bool {
			m.mu.Lock()
			_, exists := m.peers[id]
			m.mu.Unlock()
			return !exists
		}
		if m.scaler != nil {
			m.scaler.pruneCountersFor(notInPeers)
		}
		// Multipath managers for evicted peers are already empty
		// (no active transport), but the manager struct itself can
		// linger if zombie sweep didn't reach it. Reap explicitly,
		// stopping the EnsureK goroutine before deletion so it
		// doesn't outlive its peer.
		m.multipathMu.Lock()
		for id, mgr := range m.multipathManagers {
			if notInPeers(id) {
				mgr.Stop()
				delete(m.multipathManagers, id)
			}
		}
		m.multipathMu.Unlock()
		// Drop quality tracker entries for evicted peers so the
		// tracker's sync.Map doesn't accumulate state for nodes that
		// are gone. Without this it grows unbounded over the node's
		// lifetime even though only currently-known peers can read
		// from it.
		if m.qualityTracker != nil {
			for _, id := range evictedIDs {
				m.qualityTracker.ForgetPeer(id)
			}
		}
		if m.addressTracker != nil {
			for _, id := range evictedIDs {
				m.addressTracker.ForgetPeer(id)
			}
		}
		// Drop upgrade-walker per-peer state for evicted peers.
		// walkerProbeAt (keyed by nodeID) was only ever written, never deleted,
		// and walkerStalls was cleared only on a successful probe — so in a
		// churning fleet both grew one entry per distinct peer ever seen.
		evictedSet := make(map[string]struct{}, len(evictedIDs))
		for _, id := range evictedIDs {
			evictedSet[id] = struct{}{}
		}
		m.walkerProbeMu.Lock()
		for _, id := range evictedIDs {
			delete(m.walkerProbeAt, id)
		}
		m.walkerProbeMu.Unlock()
		m.walkerStallMu.Lock()
		for k := range m.walkerStalls {
			if _, gone := evictedSet[k.peerID]; gone {
				delete(m.walkerStalls, k)
			}
		}
		m.walkerStallMu.Unlock()
		log.Printf("[PEERS] Pruned %d stale peer entries (%d never-connected)",
			pruned, prunedNeverConnected)
	}
}

// PeerStates returns the current state of all peers (for debug endpoint).
// BestActiveGrade returns the highest connection grade across all connected peers.
// Used by LAD publishing to advertise this node's max supported grade.
func (m *ConnectionManager) BestActiveGrade() Grade {
	m.mu.Lock()
	defer m.mu.Unlock()
	best := GradeF
	for _, peer := range m.peers {
		if g := peer.bestActiveGrade(); g.BetterThan(best) {
			best = g
		}
	}
	return best
}

// ActivePeers returns the NodeIDs of peers with at least one open
// session. Used by InitRoute to backfill direct-path advertisements
// for peers that were already connected when the route engine came up.
func (m *ConnectionManager) ActivePeers() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, 0, len(m.peers))
	for id, p := range m.peers {
		if p.connCount > 0 {
			out = append(out, id)
		}
	}
	return out
}

func (m *ConnectionManager) PeerStates() map[string]map[string]string {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make(map[string]map[string]string, len(m.peers))
	for id, p := range m.peers {
		state := map[string]string{
			"state":         p.state.String(),
			"protocol":      p.protocol.String(),
			"crossOrigin":   fmt.Sprintf("%v", p.crossOrigin),
			"crossRegion":   fmt.Sprintf("%v", p.crossRegion),
			"region":        p.peerRegion,
			"connCount":     fmt.Sprintf("%d", p.connCount),
			"priority":      p.priority.String(),
			"grade":         p.bestActiveGrade().String(),
			"bestEverGrade": p.bestEverGrade.String(),
		}
		if !p.lastConnected.IsZero() {
			state["connectedSince"] = p.lastConnected.Format(time.RFC3339)
			state["connectedDuration"] = time.Since(p.lastConnected).Truncate(time.Second).String()
		}
		if p.lastRTT > 0 {
			state["lastRTT"] = p.lastRTT.String()
		}
		result[truncID(id)] = state
	}
	return result
}

// GossipActiveSet returns the set of peer node IDs with an active gossip loop.
// Used by monitoring to distinguish primary (gossip) vs redundant (dispatch-only) connections.
func (m *ConnectionManager) GossipActiveSet() map[string]bool {
	m.gossipMu.Lock()
	defer m.gossipMu.Unlock()
	result := make(map[string]bool, len(m.gossipActive))
	for id := range m.gossipActive {
		result[truncID(id)] = true
	}
	return result
}

// BudgetStatus returns a summary of the connection budget utilization.
func (m *ConnectionManager) BudgetStatus() map[string]interface{} {
	return map[string]interface{}{
		"current_total": m.budget.CurrentTotal(),
		"max_total":     m.budget.MaxTotal,
		"utilization":   m.budget.Utilization(),
		"max_per_peer":  m.budget.MaxPerPeer,
	}
}

// CrossOrgPreambleStats reports cross-org noise-UDP forwarder dial
// telemetry: the number of dialWithProtocol calls that engaged the
// MetadataKeyCrossOrgPreamble path (attempted), and how many returned
// without / with a dial error (succeeded / failed). Paired with
// Runtime.MeshMetrics' receive-side forwarder_drops_* counters to
// localize cross-Fly-org session pinning to WebSocket. See the field
// definitions on ConnectionManager for the diagnostic decision tree.
func (m *ConnectionManager) CrossOrgPreambleStats() (attempted, succeeded, failed uint64) {
	return m.crossOrgPreambleDialsAttempted.Load(),
		m.crossOrgPreambleDialsSucceeded.Load(),
		m.crossOrgPreambleDialsFailed.Load()
}

// CrossOrgGateStats reports the cross-org dial funnel telemetry: which
// upstream stage (pickPath, bestAddress public-UDP filter, AddressTracker
// dead-cooldown, anycast-vs-direct address shape) is dropping cross-org
// noise-UDP dials before they reach the preamble code site. See the
// field definitions on ConnectionManager for the diagnostic decision
// tree mapping counter patterns to root cause.
func (m *ConnectionManager) CrossOrgGateStats() (pickPathIncluded, publicUDPCount, candidatesEmpty, allDead, nonAnycastSelected, dialNoSuitableAddr uint64) {
	return m.crossOrgPickPathNoiseUDPIncluded.Load(),
		m.crossOrgBestAddrPublicUDPCount.Load(),
		m.crossOrgBestAddrCandidatesEmpty.Load(),
		m.crossOrgBestAddrAllDead.Load(),
		m.crossOrgBestAddrNonAnycastSelected.Load(),
		m.crossOrgDialNoSuitableAddr.Load()
}

// AggregateAetherStats walks every live aether session and sums the
// per-session SessionMetrics counter fields into a flat map keyed by
// `aether_*` prefixes for merge into the MeshMetrics handler output.
//
// Why this exists: each NoiseSession maintains its own counter set
// (ACK emit-by-trigger, can-send-false-reenqueues, replay-classifier
// hits, etc.) but the per-/api/monitoring/mesh-debug surface is
// session-aggregate; without this aggregation, the per-session
// atomics are observable only by holding a session pointer (which
// callers outside the connection manager don't have).
//
// Lock discipline: takes dispatchMu in read mode just long enough to
// copy session pointers, then calls each session's Metrics() outside
// any global lock. Metrics is internally synchronized, so the per-
// session call can race with handleACK / writeFrame on that session
// without producing torn reads — counters use atomic.LoadUint64.
func (m *ConnectionManager) AggregateAetherStats() map[string]uint64 {
	m.dispatchMu.RLock()
	sessions := make([]aether.Session, 0, len(m.meshSessions))
	for _, s := range m.meshSessions {
		if s != nil && !s.IsClosed() {
			sessions = append(sessions, s)
		}
	}
	m.dispatchMu.RUnlock()

	out := map[string]uint64{
		// writeLoop wake telemetry — pair these two to diagnose missed
		// wakes during cwnd backoff: cansend_false counts re-enqueues
		// caused by CUBIC.CanSend rejection; wake_on_ack counts every
		// ACK-driven wake. A high cansend_false with stable wake_on_ack
		// suggests the writeLoop is parking instead of being unblocked
		// when cwnd grows.
		"aether_cansend_false_reenqueues": 0,
		"aether_wake_on_ack_calls":        0,
		// ACK emission breakdown by trigger. delay_timer dominates =
		// adaptive delayed-ACK floor is the per-frame latency source on
		// sub-RTT paths. immediate dominates = receiver is firing
		// Rule 1-4 (gap / control / idle / max-packets) every ACK,
		// which is the healthy case. duplicate non-zero = peer is
		// retransmitting frames whose original ACK was lost (loss in
		// the ACK direction).
		"aether_ack_emit_immediate":   0,
		"aether_ack_emit_delay_timer": 0,
		"aether_ack_emit_duplicate":   0,
		// Anti-replay + frame-validation counters retained for back-compat:
		"aether_suspicious_acks":    0,
		"aether_replay_duplicates":  0,
		"aether_replay_ancient":     0,
		"aether_replay_rejects":     0,
		"aether_seqno_wraps":        0,
		"aether_recv_window_drops":  0,
		"aether_decrypt_errors":     0,
		"aether_inbox_drops":        0,
		"aether_stream_refused":     0,
		"aether_fec_groups_evicted": 0,
		// Per-session byte and frame totals. Non-zero confirms the
		// session is actually doing work — when these read 0
		// fleet-wide while traffic is observable elsewhere, the
		// adapter's counter wiring is broken.
		"aether_bytes_sent":  0,
		"aether_bytes_recv":  0,
		"aether_frames_sent": 0,
		// Recent-RTT distribution from the BidiRPC primary stream per
		// session, summed across all sessions to produce a fleet-wide
		// upper bound. Operators reading these alongside the EMA
		// `aether_rtt_ema_*` series can spot bimodal-latency tails the
		// EMA averages away.
		"aether_rtt_p50_ns_sum":   0,
		"aether_rtt_p95_ns_sum":   0,
		"aether_rtt_p99_ns_sum":   0,
		"aether_rtt_samples_seen": 0,
		// Health-monitor SRTT — the ping/pong path's smoothed RTT.
		// Independent of per-stream RTT: fires on every Pong arrival
		// regardless of DATA traffic. On a session with no application
		// sends, aether_rtt_samples_seen stays 0 but
		// aether_health_srtt_us_sum still surfaces the ping-derived
		// liveness RTT. Gate on >0 so test loopback / pre-Pong sessions
		// don't dilute the per-session mean.
		"aether_health_srtt_us_sum":  0,
		"aether_health_srtt_samples": 0,
		// Cumulative count of CONGESTION hints applied across all sessions
		// since each session's construction. Non-zero indicates peers have
		// signalled receive-side back-pressure; sustained growth alongside
		// climbing cansend_false_reenqueues is the textbook "send pacer
		// is the bottleneck" signature. Sourced from each session's
		// CongestionThrottle.TotalHits() — lock-free atomic load, safe to
		// sample outside any session lock. Throttle() is exposed only on
		// the concrete adapter session types (*NoiseSession, *TCPSession),
		// not the aether.Session interface, so we type-assert per session
		// to the structural interface that both satisfy.
		"aether_congestion_throttle_hits": 0,
		// noise send-cipher rekey telemetry summed across all
		// active sessions. aether_rekey_total = lifetime count of
		// completed ResetSend events (every send-cipher ratchet bumps
		// it once). aether_bytes_since_rekey = bytes encrypted on the
		// send path since each session's most recent rekey, summed —
		// the same shape as aether_bytes_sent so an operator can do
		// (bytes_since_rekey / bytes_sent) to gauge how close the
		// fleet is to the next rekey on average. Sourced from each
		// session's RekeyTracker (aether/noise/rekey.go) — non-noise
		// adapters (TCP/QUIC) miss the noiseConnStats type assertion
		// and contribute zero, which is the correct behaviour.
		"aether_rekey_total":       0,
		"aether_bytes_since_rekey": 0,
		// write-syscall latency distribution summed across all active
		// sessions. p50_sum / p99_sum are the sums of each session's p50 / p99
		// write-syscall µs. Divide by aether_sessions_sampled to get the
		// cross-fleet mean percentile. High p99_sum flags kernel send-buffer
		// pressure, TLS encode overhead, or WebSocket framing cost outside
		// aether's control. Noise-only (non-noise adapters report zero via
		// the SessionMetrics.WriteSyscallP50Us/P99Us miss). P999 is not
		// available — aether's DurationHist only captures p50/p95/p99.
		"aether_write_syscall_us_p50_sum": 0,
		"aether_write_syscall_us_p99_sum": 0,
		// cwnd-utilisation per-mille distribution summed across all
		// active sessions. p50_sum / p99_sum are the sums of each session's
		// p50 / p99 CwndUtil permille. p50_sum ≈ sessions×1000 = cwnd-bound
		// for most sends; p50_sum << sessions×1000 = app-bound (cwnd is
		// bigger than needed). p99_sum near sessions×1000 with low p50_sum =
		// bursty cwnd-bound spikes in an otherwise app-bound regime. Noise-
		// only (non-noise adapters contribute zero).
		"aether_cwnd_util_permille_p50_sum": 0,
		"aether_cwnd_util_permille_p99_sum": 0,
		// per-session receive-path delivery counters summed across
		// all active adapter sessions that expose DeliveryStats(). Sourced
		// from each session's DeliveryStats accessor (aether adapter,
		// *NoiseSession + *TCPSession). QUIC / gRPC / TLS adapters route
		// data via their transport's own framing and don't go through
		// DeliverToRecvChWithSignals, so the type-assertion miss correctly
		// skips them — these counters reflect only the noise/TCP
		// receive-side delivery hot path.
		//
		// Pair with aether_recv_window_drops + aether_inbox_drops to
		// localize where receive-side loss is happening:
		//   - aether_inbox_drops       → packet-classifier inbox overflow
		//                                (before per-stream demux)
		//   - aether_recv_window_drops → reorder-buffer overflow
		//                                (per-stream, after demux)
		//   - aether_deliver_dropped   → recvCh full after backpressure
		//                                (slow application consumer)
		// aether_deliver_backpressure non-zero with low aether_deliver_dropped
		// means the application is sometimes slow but eventually drains;
		// climbing aether_deliver_dropped means the slow path is timing out.
		"aether_deliver_delivered":     0,
		"aether_deliver_dropped":       0,
		"aether_deliver_backpressure":  0,
		"aether_deliver_bytes_dropped": 0,
		// recv-window head-of-line-block latency sums across all
		// sessions. Each session contributes its p50/p99 percentile (µs).
		// Non-zero confirms reordering is occurring fleet-wide; a climbing
		// _p99_sum with a stable _p50_sum is the "rare-but-deep HOL" shape
		// that points at occasional burst reordering rather than structural
		// path disorder. Pair with aether_recv_window_drops for the full
		// receive-reorder picture.
		"aether_recv_window_hol_us_p50_sum": 0,
		"aether_recv_window_hol_us_p99_sum": 0,
		// writeLoop scheduler queue-depth distribution summed across
		// all active sessions. Each session contributes its p50 / p99
		// queue-depth sample (frames pending + TLP probes). Sustained-high
		// p50_sum (relative to aether_sessions_sampled) means work piles up
		// faster than the writeLoop drains it — cwnd-bound or syscall-bound;
		// p99_sum >> p50_sum indicates bursty arrivals. Zero on transports
		// without the histogram wired (anything other than the noise session
		// today). Gate on p50 > 0 to skip sessions that never observed a
		// non-empty queue (the healthy-fast-path zero contribution).
		"aether_scheduler_depth_p50_sum": 0,
		"aether_scheduler_depth_p99_sum": 0,
		// Stream.Send credit-starvation block latency sums (µs)
		// across all active sessions. Each session contributes its p50 / p99.
		// Elevated p99 with a healthy aether_writeloop_park_*_us_sum (OBS-2)
		// fingerprints the flow-control credit semaphore — not the wire — as
		// the application-facing latency source; elevated p50 indicates
		// sustained credit starvation across most sends. Pair with
		// aether_stream_send_fast_total (when surfaced) to compute the
		// uncontested-vs-blocked send ratio. Gate on p50 > 0 so fast-path-
		// only sessions don't pollute the sum.
		"aether_stream_send_block_us_p50_sum": 0,
		"aether_stream_send_block_us_p99_sum": 0,
		// OBS-14b: per-phase decomposition of StreamSendBlock. Each session
		// contributes its p50/p99 for each of the three phases inside
		// noiseStream.Send. The sum across sessions gives a per-phase
		// fleet-level view of where caller-block latency sits — read these
		// alongside aether_stream_send_block_us_*_sum to attribute the
		// aggregate to (a) per-stream credit starvation (waiting for the
		// peer's WINDOW_UPDATE), (b) per-conn credit starvation (typically
		// non-blocking), or (c) the post-credit encrypt + write_syscall +
		// writeloop_park path. Same fast-path gate as the aggregate — phase
		// durations below streamSendBlockFloor (50µs) are skipped at record
		// time so the percentile readout already excludes uncontended noise.
		"aether_per_stream_window_wait_us_p50_sum": 0,
		"aether_per_stream_window_wait_us_p99_sum": 0,
		"aether_conn_window_wait_us_p50_sum":       0,
		"aether_conn_window_wait_us_p99_sum":       0,
		"aether_post_credit_send_us_p50_sum":       0,
		"aether_post_credit_send_us_p99_sum":       0,
		// per-decrypt AEAD latency distribution (µs)
		// summed across all noise sessions. p50_sum near sessions×<few> +
		// p99_sum in the millisecond tier = canonical CPU-saturation
		// asymmetric shape; sustained millisecond p50_sum indicates
		// ongoing starvation rather than bursts. Zero on non-noise
		// adapters (TCP / QUIC / test loopback) — they don't run AEAD on
		// the recv hot path. Gate on p50 > 0 for the same reason as the
		// scheduler-depth and stream-send-block sums.
		"aether_decrypt_latency_us_p50_sum": 0,
		"aether_decrypt_latency_us_p99_sum": 0,
		// N1a: multipath PathFlapping tier population summed across every
		// session a peer's multipath.Manager exposes. Non-zero means at
		// least one path is currently in PathFlapping demote (recent stuck-
		// kill + within demote window). Sustained growth alongside
		// aether_multipath_failover_total in MeshMetrics is the textbook
		// "path flap in progress" signature. Sourced from SessionMetrics
		// (set by the Manager.snapshotSessionMetrics handoff in aether
		// v0.0.80), so single-path peers naturally contribute zero.
		"aether_path_flapping_total": 0,
	}
	for _, s := range sessions {
		mm := s.Metrics()
		out["aether_cansend_false_reenqueues"] += mm.CanSendFalseReenqueues
		out["aether_wake_on_ack_calls"] += mm.WakeOnAckCalls
		out["aether_ack_emit_immediate"] += mm.AckEmitImmediate
		out["aether_ack_emit_delay_timer"] += mm.AckEmitDelayTimer
		out["aether_ack_emit_duplicate"] += mm.AckEmitDuplicate
		out["aether_suspicious_acks"] += mm.SuspiciousACKs
		out["aether_replay_duplicates"] += mm.ReplayDuplicates
		out["aether_replay_ancient"] += mm.ReplayAncient
		out["aether_replay_rejects"] += mm.ReplayRejects
		out["aether_seqno_wraps"] += mm.SeqNoWraps
		out["aether_recv_window_drops"] += mm.RecvWindowDrops
		out["aether_decrypt_errors"] += mm.DecryptErrors
		out["aether_inbox_drops"] += mm.InboxDrops
		out["aether_stream_refused"] += mm.StreamRefused
		out["aether_fec_groups_evicted"] += mm.FECGroupsEvicted
		out["aether_bytes_sent"] += mm.BytesSent
		out["aether_bytes_recv"] += mm.BytesRecv
		out["aether_frames_sent"] += mm.FramesSent
		// RekeyTotal/RekeyBytesSinceLast are noise-only (non-
		// noise adapters report zero via the noiseConnStats interface
		// miss), so summing across all sessions naturally yields the
		// noise-fleet rekey total + bytes-since-rekey aggregate.
		out["aether_rekey_total"] += mm.RekeyTotal
		out["aether_bytes_since_rekey"] += mm.RekeyBytesSinceLast
		// Gate on RttP50 > 0 — any session with at least one ACK sample
		// produces a non-zero p50. Gating on p99 would silently exclude
		// every session with fewer than ~99 samples, under-counting
		// early-life sessions in samples_seen. P95/P99 may still read
		// 0 when the histogram hasn't filled, which is the correct
		// signal that those percentiles are not yet meaningful for
		// that session — the sum aggregation handles it gracefully.
		if mm.RttP50 > 0 {
			out["aether_rtt_p50_ns_sum"] += uint64(mm.RttP50.Nanoseconds())
			out["aether_rtt_p95_ns_sum"] += uint64(mm.RttP95.Nanoseconds())
			out["aether_rtt_p99_ns_sum"] += uint64(mm.RttP99.Nanoseconds())
			out["aether_rtt_samples_seen"]++
		}
		// Health-monitor SRTT aggregation — surfaces the ping/pong-derived
		// liveness RTT independently of per-stream data ACKs. Gate on >0
		// so sessions that haven't seen a matching Pong yet don't dilute
		// the sum. Per-session-mean ping RTT = us_sum / samples.
		if mm.HealthSRTTUs > 0 {
			out["aether_health_srtt_us_sum"] += mm.HealthSRTTUs
			out["aether_health_srtt_samples"]++
		}
		// CongestionThrottle.TotalHits — only the concrete adapter sessions
		// (*NoiseSession, *TCPSession) carry a throttle; QUIC / gRPC / TLS
		// adapters don't drive CONGESTION frames so they're correctly skipped
		// by the type-assertion miss. The returned pointer is nil-safe to
		// guard against future adapters that satisfy the method but don't
		// instantiate the throttle.
		if t, ok := s.(interface {
			Throttle() *aether.CongestionThrottle
		}); ok {
			if th := t.Throttle(); th != nil {
				out["aether_congestion_throttle_hits"] += th.TotalHits()
			}
		}
		// write-syscall latency sums. Cast uint64→uint64 directly;
		// zeros from non-noise adapters are correct and harmless.
		out["aether_write_syscall_us_p50_sum"] += mm.WriteSyscallP50Us
		out["aether_write_syscall_us_p99_sum"] += mm.WriteSyscallP99Us
		// cwnd-utilisation permille sums. uint32 widened to uint64.
		out["aether_cwnd_util_permille_p50_sum"] += uint64(mm.CwndUtilP50Permille)
		out["aether_cwnd_util_permille_p99_sum"] += uint64(mm.CwndUtilP99Permille)
		// receive-path delivery counters via the adapter's
		// DeliveryStats accessor. Same shape as the Throttle() probe —
		// the aether.Session interface doesn't expose DeliveryStats; we
		// type-assert to a structural interface that both NoiseSession
		// and TCPSession satisfy. QUIC/gRPC/TLS adapters miss the
		// assertion and contribute zero, which is the correct behaviour
		// (they don't route data through DeliverToRecvChWithSignals).
		// Lock-free atomic loads — safe to sample outside any session
		// lock concurrently with the readLoop's increments.
		if d, ok := s.(interface {
			DeliveryStats() *adapter.DeliveryStats
		}); ok {
			if ds := d.DeliveryStats(); ds != nil {
				out["aether_deliver_delivered"] += uint64(ds.Delivered.Load())
				out["aether_deliver_dropped"] += uint64(ds.Dropped.Load())
				out["aether_deliver_backpressure"] += uint64(ds.Backpressure.Load())
				out["aether_deliver_bytes_dropped"] += uint64(ds.BytesDropped.Load())
			}
		}
		// HOL-block latency sums. Gate on p50 > 0 so sessions with
		// no reordering (pure in-order delivery) don't pollute the sum with
		// meaningless zeros. Same gating convention as aether_rtt_p50_ns_sum.
		if mm.RecvWindowHOLP50Us > 0 {
			out["aether_recv_window_hol_us_p50_sum"] += mm.RecvWindowHOLP50Us
			out["aether_recv_window_hol_us_p99_sum"] += mm.RecvWindowHOLP99Us
		}
		// scheduler queue-depth sums. uint32 widened to uint64.
		// Gate on p50 > 0 so sessions whose writeLoop never observed a
		// non-empty queue (pure fast-path) don't dilute the percentile sum.
		if mm.SchedulerDepthP50 > 0 {
			out["aether_scheduler_depth_p50_sum"] += uint64(mm.SchedulerDepthP50)
			out["aether_scheduler_depth_p99_sum"] += uint64(mm.SchedulerDepthP99)
		}
		// stream-send-block latency sums. Sessions whose Send path
		// never blocked on credit (uncontended fast path) report p50 == 0
		// and are skipped — same shape as the RTT histogram gate.
		if mm.StreamSendBlockP50Us > 0 {
			out["aether_stream_send_block_us_p50_sum"] += mm.StreamSendBlockP50Us
			out["aether_stream_send_block_us_p99_sum"] += mm.StreamSendBlockP99Us
		}
		// OBS-14b: per-phase decomposition sums. Independently gated per
		// phase so a session that only blocked in (say) ConnWindowWait
		// still contributes to that sub-histogram without polluting the
		// other two.
		if mm.PerStreamWindowWaitP50Us > 0 {
			out["aether_per_stream_window_wait_us_p50_sum"] += mm.PerStreamWindowWaitP50Us
			out["aether_per_stream_window_wait_us_p99_sum"] += mm.PerStreamWindowWaitP99Us
		}
		if mm.ConnWindowWaitP50Us > 0 {
			out["aether_conn_window_wait_us_p50_sum"] += mm.ConnWindowWaitP50Us
			out["aether_conn_window_wait_us_p99_sum"] += mm.ConnWindowWaitP99Us
		}
		if mm.PostCreditSendP50Us > 0 {
			out["aether_post_credit_send_us_p50_sum"] += mm.PostCreditSendP50Us
			out["aether_post_credit_send_us_p99_sum"] += mm.PostCreditSendP99Us
		}
		// per-decrypt AEAD latency sums. Non-noise
		// adapters always report p50 == 0 (no AEAD on the recv hot path)
		// and are skipped, which keeps the sum a noise-only fleet view.
		if mm.DecryptLatencyP50Us > 0 {
			out["aether_decrypt_latency_us_p50_sum"] += mm.DecryptLatencyP50Us
			out["aether_decrypt_latency_us_p99_sum"] += mm.DecryptLatencyP99Us
		}
		// N1a: multipath PathFlapping tier population. uint32 widened to
		// uint64; ungated — zero from single-path peers is meaningful (it
		// confirms they're NOT flapping) and contributes nothing to the
		// sum anyway.
		out["aether_path_flapping_total"] += uint64(mm.PathFlappingCount)
	}
	out["aether_sessions_sampled"] = uint64(len(sessions))
	return out
}

// isSameRegionSameOrigin reports whether the peer is in the same Fly
// region AND the same network origin as us. Pairs satisfying this MUST
// use noise-UDP by design — they have 6PN private
// IPv6 routes available and there's no reason to fall back to a
// WebSocket-via-edge path that suffers from edge-proxy reaping.
//
// Returns false when:
//   - selfRegion is empty (boot race; conservative: don't lock to UDP)
//   - peer.peerRegion is empty (peer hasn't published region yet)
//   - regions differ
//   - peer is cross-origin (different Fly org / VPC)
//
// 🔴 CONCURRENCY. Both fields this reads — peer.crossOrigin and
// peer.peerRegion — are written under m.mu by scanAndConnect (:2407, :2501,
// :2645, :2704), SeedBootstrapPeer (:2788, :2775) and reach_resync (:170),
// while scanAndConnect dials peers in parallel goroutines. Reading them
// unlocked was a data race the race detector confirmed at :2233 and :2236.
//
// The same remedy is applied eleven lines below for the crossOrigin telemetry
// read, and in ForceConnect for peer.state; it simply never reached the read
// that decides the protocol set.
//
// The split exists because sync.Mutex is not reentrant and the three callers
// disagree about the lock: pickPath and connectPeer hold nothing,
// AcceptMeshConnection (mesh_connection.go:1048) holds m.mu. Locking inside a
// single function would have deadlocked the accept path.
func (m *ConnectionManager) isSameRegionSameOrigin(peer *peerConn) bool {
	if peer == nil {
		return false
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.isSameRegionSameOriginLocked(peer)
}

// isSameRegionSameOriginLocked is the lock-free core.
//
// Caller must hold m.mu (same contract as dialOwned).
func (m *ConnectionManager) isSameRegionSameOriginLocked(peer *peerConn) bool {
	if peer == nil || peer.crossOrigin {
		return false
	}
	if m.selfRegion == "" || peer.peerRegion == "" {
		return false
	}
	return peer.peerRegion == m.selfRegion
}

// pickPath returns the ordered list of protocols to attempt for this
// peer. Replaces the static protocolOrder cascade with a scoring model
// driven by peer attributes.
//
// Hard rule for same-region same-origin pairs: ONLY noise-UDP is
// returned. These pairs have 6PN private IPv6 routes and there is no
// acceptable fallback — falling to WS via the bootstrap host triggers
// Fly edge proxy reaping, the dominant source of grade-C session
// churn. The caller's dial loop terminates after the single attempt;
// scanAndConnect re-enters on the next tick with a fresh transient
// cooldown if it failed.
//
// Other peers (cross-region OR cross-origin) get the full ordered set:
//  1. ProtoNoiseUDP (still preferred — direct, no edge)
//  2. ProtoWebSocket (6PN-direct for same-org, edge-WS for cross-org)
//  3. ProtoQUIC, ProtoGRPC, ProtoTLS (in protocolOrder sequence)
func (m *ConnectionManager) pickPath(peer *peerConn) []Protocol {
	// ONE acquisition for BOTH reads. Locking only the telemetry read below
	// leaves the same-region decision — which reads the same two fields —
	// unlocked, and the race detector confirms it. Taking m.mu
	// twice would fix the race but let the decision and the counter observe
	// different states of one peer, so they share a single snapshot.
	sameRegionSameOrigin, crossOrigin := false, false
	if peer != nil {
		m.mu.Lock()
		sameRegionSameOrigin = m.isSameRegionSameOriginLocked(peer)
		crossOrigin = peer.crossOrigin
		m.mu.Unlock()
	}
	if sameRegionSameOrigin {
		return []Protocol{ProtoNoiseUDP}
	}
	// Copy protocolOrder so callers can't mutate the package global.
	out := make([]Protocol, 0, len(protocolOrder))
	out = append(out, protocolOrder...)
	// Cross-org dial funnel telemetry: count whenever pickPath returns
	// the full protocolOrder (which begins with ProtoNoiseUDP) for a
	// cross-org peer. Confirms NoiseUDP was offered to the dial loop.
	if crossOrigin {
		m.crossOrgPickPathNoiseUDPIncluded.Add(1)
	}
	return out
}

// isCrossRegion checks if a peer is in a different region based on reach records.
func (m *ConnectionManager) isCrossRegion(peerRegion string) bool {
	if m.selfRegion == "" || peerRegion == "" {
		return false // can't determine, assume same region
	}
	return peerRegion != m.selfRegion
}

// scanAndConnect discovers new peers from LAD and connects unconnected ones.
// dialOwned reports whether THIS node should perform the outbound dial for a
// not-yet-connected peer. Deterministic dial ownership: across any node pair
// the lexicographically-lower nodeID owns the dial; the higher nodeID waits
// for the inbound connection instead. This removes the simultaneous
// cross-dial that otherwise yields a session pair, a registerMeshSession
// tie-break, and a churned (rejected) loser session on every (re)connect —
// the residual churn left after the v0.0.265 dedup fixes.
//
// The higher-nodeID side defers only within dialOwnershipGrace of first
// becoming dial-eligible; past the grace it dials anyway so a dead or
// unreachable owner cannot strand the pair. A bootstrap host on first
// contact (lastConnected still zero) is always owned locally — a joining
// node must dial its bootstrap host, which has no prior knowledge of the
// joiner and cannot dial first. Once any session has succeeded the peer is
// governed by normal nodeID ownership like every other peer.
//
// Caller must hold m.mu (reads and mutates peer.dialEligibleSince).
func (m *ConnectionManager) dialOwned(p *peerConn, now time.Time) bool {
	if p.bootstrapHost != "" && p.lastConnected.IsZero() {
		return true
	}
	if m.selfID < p.nodeID {
		// We own the dial — drop any stale deferral marker.
		p.dialEligibleSince = time.Time{}
		return true
	}
	// Higher-nodeID side: stamp first eligibility, then defer until the
	// grace window elapses.
	if p.dialEligibleSince.IsZero() {
		p.dialEligibleSince = now
	}
	return now.Sub(p.dialEligibleSince) >= dialOwnershipGrace
}

func (m *ConnectionManager) scanAndConnect(ctx context.Context) {
	// The LAD reach cache is one input to outbound dial seeding, but it's
	// no longer the only one: SeedBootstrapPeer, the persisted peer store,
	// and discovery-layer fan-in populate m.peers directly. Skip the
	// reach-merge step when the cache yields nothing, then fall through to
	// the m.peers walk so seeded peers still get dialed.
	var reaches []lad.ReachRecord
	if m.rt.cache != nil {
		if r, err := m.rt.cache.Reach(ctx, "", ladcache.ReachQuery{}); err == nil {
			reaches = r
		}
	}

	m.mu.Lock()
	for _, rec := range reaches {
		if rec.NodeID == m.selfID || len(rec.Addresses) == 0 {
			continue
		}
		if _, exists := m.peers[rec.NodeID]; !exists {
			crossOrigin := m.isCrossOrigin(rec.Addresses)
			peerRegion := rec.Region
			crossRegion := m.isCrossRegion(peerRegion)
			m.peers[rec.NodeID] = &peerConn{
				nodeID:         rec.NodeID,
				addresses:      rec.Addresses,
				state:          PeerDiscovered,
				discoveredAt:   time.Now(),
				crossOrigin:    crossOrigin,
				crossRegion:    crossRegion,
				peerRegion:     peerRegion,
				reconnectDelay: baseCooldown,
			}
		} else {
			// Update addresses and region info.
			//
			// Only overwrite addresses when the incoming record actually has
			// some — we don't want a stale / partially-tombstoned REACH record
			// to wipe out the last-known-good address list. An empty
			// Addresses slice makes every subsequent dial fail with
			// "no suitable address for <proto>", which then counts as a dial
			// failure, the peer goes into cooldown, and the mesh can't heal
			// until gossip re-populates the reach record — which itself
			// needs a connection to succeed (chicken-and-egg).
			p := m.peers[rec.NodeID]
			if len(rec.Addresses) > 0 {
				// Detect newly-arrived transport classes and clear their
				// failure cooldowns so the dialer reconsiders them
				// fresh. Fixes the case where a peer is first seeded
				// from bootstrap with only a TLS address, dials
				// noise-UDP once (fails because no UDP address exists),
				// hits the dial-failure cooldown, and stays locked on
				// TLS/WS even after the peer's authoritative Reach
				// record (with UDP address) arrives via gossip.
				oldProtos := make(map[string]bool, len(p.addresses))
				for _, a := range p.addresses {
					oldProtos[a.Proto] = true
				}
				for _, a := range rec.Addresses {
					if oldProtos[a.Proto] {
						continue
					}
					// A new Proto showed up. Clear cooldowns for the
					// aether Protocol(s) that would consume it. Cooldown
					// state lives on the tracker, so we clear via
					// recordDialSuccess (zeroes failure count + cooldown).
					switch a.Proto {
					case "udp":
						m.recordDialSuccess(p.nodeID, ProtoNoiseUDP)
						m.recordDialSuccess(p.nodeID, ProtoQUIC)
					case "wss":
						m.recordDialSuccess(p.nodeID, ProtoWebSocket)
					case "tls":
						m.recordDialSuccess(p.nodeID, ProtoTLS)
					}
				}
				p.addresses = rec.Addresses
				// Recompute crossOrigin against the freshly-received LAD
				// reach addresses — they may carry a Scope:"private"
				// entry that wasn't present at peer-discovery time, which
				// would flip the peer from cross-origin to same-origin
				// and unlock the noise-UDP direct path in pickPath /
				// bestAddress. Without this, an LAD-only peer stays
				// stuck on the dispatch grade it was first registered at.
				p.crossOrigin = m.isCrossOrigin(p.addresses)
			}
			if rec.Region != "" {
				p.peerRegion = rec.Region
				p.crossRegion = m.isCrossRegion(rec.Region)
			}
		}
	}
	// Swarm AddressTable: PeerRecord-advertised dial candidates. Nothing
	// else publishes reach, so swarm.AddressTable is the authoritative
	// source of dialable addresses for fleet peers. Without this merge,
	// peer.addresses stays empty and EnsureK reports "no suitable address
	// for noise-udp/grpc/websocket" on every outbound dial — which then
	// blocks RPC dispatch (auth, dispatch handlers) and grade-A upgrades.
	if m.rt.swarm != nil && m.rt.swarm.AddressTable != nil {
		for nodeID, cands := range m.rt.swarm.AddressTable.All() {
			if nodeID == m.selfID || len(cands) == 0 {
				continue
			}
			addrs := make([]lad.ReachAddress, 0, len(cands))
			for _, c := range cands {
				proto := swarmTransportToReachProto(c.Transport)
				if proto == "" || c.Host == "" || c.Port == 0 {
					continue
				}
				scope := c.Scope
				if scope == "" {
					// Infer scope from the host shape so an unscoped
					// 6PN ULA / RFC1918 / IPv6 ULA reaches Tier 0a in
					// bestAddress instead of being dropped by both the
					// Tier 0a Scope!="private" gate and the Tier 1
					// ip.To4()!=nil gate. scopeForSwarmHost returns
					// "private" for those host classes and "public"
					// for everything else; the result matches the
					// per-tier filters in bestAddress.
					scope = scopeForSwarmHost(c.Host)
				}
				addrs = append(addrs, lad.ReachAddress{
					Host:  c.Host,
					Port:  int(c.Port),
					Proto: proto,
					Scope: scope,
				})
			}
			if len(addrs) == 0 {
				continue
			}
			peerRegion := ""
			if m.rt.swarm.RoleTable != nil {
				if info, ok := m.rt.swarm.RoleTable.PeerInfo(nodeID); ok {
					peerRegion = info.Region
				}
			}
			// Peer-level saturation bit — identical across all of this peer's
			// candidates (saturation is a node property), so read it from the
			// first. cands is non-empty here (guarded above).
			pSat, pUntil := cands[0].Saturated, cands[0].BackoffUntil
			if existing, ok := m.peers[nodeID]; ok {
				seen := make(map[lad.ReachAddress]struct{}, len(existing.addresses))
				for _, a := range existing.addresses {
					seen[a] = struct{}{}
				}
				oldProtos := make(map[string]bool, len(existing.addresses))
				for _, a := range existing.addresses {
					oldProtos[a.Proto] = true
				}
				merged := existing.addresses
				for _, a := range addrs {
					if _, dup := seen[a]; dup {
						continue
					}
					merged = append(merged, a)
					seen[a] = struct{}{}
					if !oldProtos[a.Proto] {
						switch a.Proto {
						case "udp":
							m.recordDialSuccess(existing.nodeID, ProtoNoiseUDP)
							m.recordDialSuccess(existing.nodeID, ProtoQUIC)
						case "wss":
							m.recordDialSuccess(existing.nodeID, ProtoWebSocket)
						case "tls":
							m.recordDialSuccess(existing.nodeID, ProtoTLS)
						}
					}
				}
				existing.addresses = merged
				// Recompute crossOrigin against the merged address set —
				// when this peer was first discovered (via PEX or
				// bootstrap) it had no Scope:"private" entries so
				// isCrossOrigin defaulted to true. Now that swarm has
				// delivered the peer's 6PN/private endpoint we have to
				// re-evaluate, otherwise pickPath stays on the public
				// path forever and same-org noise-UDP never engages.
				existing.crossOrigin = m.isCrossOrigin(merged)
				if peerRegion != "" && existing.peerRegion == "" {
					existing.peerRegion = peerRegion
					existing.crossRegion = m.isCrossRegion(peerRegion)
				}
				existing.peerSaturated = pSat
				existing.peerBackoffUntil = pUntil
			} else {
				m.peers[nodeID] = &peerConn{
					nodeID:           nodeID,
					addresses:        addrs,
					state:            PeerDiscovered,
					discoveredAt:     time.Now(),
					crossOrigin:      m.isCrossOrigin(addrs),
					crossRegion:      m.isCrossRegion(peerRegion),
					peerRegion:       peerRegion,
					reconnectDelay:   baseCooldown,
					peerSaturated:    pSat,
					peerBackoffUntil: pUntil,
				}
			}
		}
	}
	m.mu.Unlock()

	// Connect peers that need an outbound connection:
	// - PeerDiscovered: never connected
	// - PeerDisconnected: lost connection, backoff expired
	// - PeerConnected but !outboundDialed: only has incoming connection
	//   AND the incoming path doesn't already satisfy our grade target.
	//
	// Two gating conditions before adding an incoming-only peer to the
	// outbound-dial list:
	//   a) current effective grade is less than Grade A — an outbound
	//      dial could genuinely upgrade us. If we already have Grade A
	//      via incoming, dialing outbound would only produce a duplicate
	//      connection that the scaler will immediately drain.
	//   b) the transport we'd dial (noise-udp) wasn't recently drained.
	//      A recently-drained transport class is presumed redundant
	//      with a better active path; re-dialing starts a 60s churn
	//      loop (WS redial → drain → WS redial → drain).
	now := time.Now()
	m.mu.Lock()
	toConnect := make([]*peerConn, 0)
	for _, p := range m.peers {
		if p.state == PeerDiscovered {
			// Deterministic dial ownership — defer to the lower-nodeID
			// peer within the grace window (see dialOwned).
			if !m.dialOwned(p, now) {
				continue
			}
			toConnect = append(toConnect, p)
		} else if p.state == PeerDisconnected {
			if now.Before(p.lastConnected.Add(p.reconnectDelay)) {
				continue // still in backoff
			}
			if !m.dialOwned(p, now) {
				continue
			}
			toConnect = append(toConnect, p)
		} else if p.state == PeerConnected {
			// Connected — the dial race is over. Clear the deferral
			// marker so a future disconnect opens a fresh ownership
			// grace window instead of reusing a stale timestamp.
			p.dialEligibleSince = time.Time{}
			if !p.outboundDialed {
				// Gate (a): if we already have top-grade via any active
				// transport, outbound dial can't improve anything.
				if p.bestActiveGrade() >= GradeA {
					continue
				}
				// Gate (b): skip only when EVERY upgrade-candidate
				// protocol is currently within drain-redial cooldown.
				// The previous aggregate-OR gate ("ANY drained class
				// stays cool") blanket-blocks Grade-A redials for five
				// minutes after a Grade-C drain. Skip only when there is
				// no protocol left to try.
				if len(p.drainedAt) > 0 {
					bestActive := p.bestActiveGrade()
					anyUpgradeAvailable := false
					for proto, t := range p.drainedAt {
						if GradeForProtocol(proto) <= bestActive {
							continue // not an upgrade candidate
						}
						if now.Sub(t) >= drainRedialCooldown {
							anyUpgradeAvailable = true
							break
						}
					}
					// We also need to consider upgrade protos that
					// AREN'T in drainedAt at all — those are
					// available by definition. Use protocolOrder to
					// enumerate.
					if !anyUpgradeAvailable {
						for _, proto := range protocolOrder {
							if GradeForProtocol(proto) <= bestActive {
								continue
							}
							if _, drained := p.drainedAt[proto]; !drained {
								anyUpgradeAvailable = true
								break
							}
						}
					}
					if !anyUpgradeAvailable {
						continue
					}
				}
				// Gate (c): if we already have a Grade-C transport active
				// AND the only available upgrade target is also Grade-C
				// (i.e., no UDP/QUIC address exists for this peer), don't
				// dial — connectToPeer's protocol fallback would hit the
				// other Grade-C class (WS↔TLS swap) producing the exact
				// churn we want to avoid. Only dial when the peer actually
				// has an address we can reach at a better grade.
				if p.bestActiveGrade() == GradeC && !m.peerHasBetterGradeAddress(p) {
					continue
				}
				// Connected via incoming only — dial outbound for a
				// stable, upgraded connection.
				toConnect = append(toConnect, p)
			}
		}
	}
	m.mu.Unlock()

	// Refresh addresses from cache before retrying — self-published reach
	// records may have arrived via gossip since the last scan, replacing
	// incomplete bootstrap-created records with correct addresses (WSS,
	// dedicated IPs).
	//
	// Same defensive guard as scanAndConnect: only overwrite when the new
	// record actually has addresses. A tombstoned / partially-applied
	// record with empty Addresses must not wipe the in-memory list.
	if m.rt.cache != nil {
		for _, p := range toConnect {
			if recs, err := m.rt.cache.Reach(ctx, "", ladcache.ReachQuery{NodeID: p.nodeID}); err == nil && len(recs) > 0 && len(recs[0].Addresses) > 0 {
				m.mu.Lock()
				p.addresses = recs[0].Addresses
				// Recompute crossOrigin — the refreshed reach record
				// may carry a Scope:"private" 6PN entry that flips the
				// classification to same-origin and enables direct
				// noise-UDP dial via bestAddress's Tier 0 path.
				p.crossOrigin = m.isCrossOrigin(p.addresses)
				m.mu.Unlock()
			}
		}
	}

	// Stale-6PN safety: for peers we're about to (re-)dial that are NOT
	// currently connected, REPLACE peer.addresses with the latest swarm
	// AddressTable candidates — do NOT union. The scanAndConnect upstream
	// merge step is union-with-dedup so a long-lived peer accumulates
	// every Host:Port pair we've ever seen. After a peer machine restart
	// it re-publishes its 6PN ULA with the SAME Host:Port but its
	// listener is fresh; the dedup keeps the pre-restart entry and
	// noise-UDP dials the stale path. The pre-restart cached entry can
	// also outlive the actual peer endpoint when Fly reallocates the
	// 6PN address — bestAddress then picks the dead 6PN, dialNoiseUDP
	// times out msg2, and the same-region same-origin hard rule in
	// connectPeer skips recordDialFailure so no TLS fallback is
	// attempted. By replacing rather than unioning at the point of dial,
	// we ensure noise-UDP targets the freshest candidates published by
	// the peer — stale entries cannot survive into a dial attempt for a
	// disconnected/discovered peer. PeerConnected entries in toConnect
	// (incoming-only Grade-C upgrades) keep the unioned address set
	// because their existing session is the source of truth on
	// reachability; only re-dial paths need the strict replace.
	if m.rt.swarm != nil && m.rt.swarm.AddressTable != nil {
		for _, p := range toConnect {
			m.mu.Lock()
			st := p.state
			m.mu.Unlock()
			if st != PeerDiscovered && st != PeerDisconnected {
				continue
			}
			cands := m.rt.swarm.AddressTable.Get(p.nodeID)
			if len(cands) == 0 {
				continue
			}
			addrs := make([]lad.ReachAddress, 0, len(cands))
			for _, c := range cands {
				proto := swarmTransportToReachProto(c.Transport)
				if proto == "" || c.Host == "" || c.Port == 0 {
					continue
				}
				scope := c.Scope
				if scope == "" {
					scope = scopeForSwarmHost(c.Host)
				}
				addrs = append(addrs, lad.ReachAddress{
					Host:  c.Host,
					Port:  int(c.Port),
					Proto: proto,
					Scope: scope,
				})
			}
			if len(addrs) == 0 {
				continue
			}
			m.mu.Lock()
			p.addresses = addrs
			p.crossOrigin = m.isCrossOrigin(p.addresses)
			m.mu.Unlock()
		}
	}

	// Dial in parallel, respecting connection budget
	var wg sync.WaitGroup
	connectCount := 0
	for _, p := range toConnect {
		// Read connCount under m.mu — the toConnect list was built
		// after releasing the lock, and the accept-cleanup path mutates
		// connCount concurrently, so a bare read both raced and used a stale
		// value for the budget decision.
		m.mu.Lock()
		peerConnCount := p.connCount
		m.mu.Unlock()
		if !m.budget.CanConnect(peerConnCount) {
			continue // budget exhausted or peer at max
		}
		connectCount++
		wg.Add(1)
		go func(peer *peerConn) {
			defer wg.Done()
			m.connectPeer(ctx, peer)
		}(p)
	}
	wg.Wait()
}

// SeedBootstrapPeer adds a bootstrap host as a peer so connectPeer dials it
// through the normal protocol fallback chain. The bootstrap host address is
// stored so the TLS dial path can use it for HTTPS VL1 upgrade.
//
// The seed is transport-agnostic: the TLS address is guaranteed (it's how
// we got here), but we also try to merge any authoritative Reach record
// already in the LAD cache so the peer gets UDP/WSS/gRPC addresses from
// the start. Without this merge, a bootstrap-seeded peer is stuck on
// Grade C (WS/TLS) until a subsequent scanAndConnect tick discovers its
// Reach record and refreshes addresses — which on a fresh boot can be
// several minutes after the first outbound dial, during which the peer
// has already installed itself as Grade C in the multipath scorer.
func (m *ConnectionManager) SeedBootstrapPeer(nodeID, host, region string) {
	log.Printf("[SWARM] SeedBootstrapPeer: entry peer=%s host=%s region=%s", truncID(nodeID), host, region)
	m.mu.Lock()
	defer m.mu.Unlock()
	if nodeID == m.selfID {
		log.Printf("[SWARM] SeedBootstrapPeer: skip self peer=%s", truncID(nodeID))
		return
	}

	// Build the address list: start with TLS bootstrap (always known),
	// then merge whatever the LAD cache already holds for this peer.
	tlsAddr := lad.ReachAddress{Host: host, Proto: "tls", Scope: "public"}
	addresses := []lad.ReachAddress{tlsAddr}
	if m.rt != nil && m.rt.cache != nil {
		if recs, err := m.rt.cache.Reach(context.Background(), "", ladcache.ReachQuery{NodeID: nodeID}); err == nil {
			for _, rec := range recs {
				for _, a := range rec.Addresses {
					// Avoid dup of the TLS seed; anything new is merged.
					if a.Host == host && a.Proto == "tls" {
						continue
					}
					addresses = append(addresses, a)
				}
			}
		}
	}

	if existing, ok := m.peers[nodeID]; ok {
		existing.bootstrapHost = host
		if region != "" {
			existing.peerRegion = region
			existing.crossRegion = m.isCrossRegion(region)
		}
		if existing.state == PeerDisconnected {
			existing.state = PeerDiscovered
			existing.reconnectDelay = baseCooldown
		}
		// Merge new addresses onto whatever was already there.
		existing.addresses = mergeReachAddresses(existing.addresses, addresses)
		// Recompute crossOrigin against the merged set — bootstrap
		// seeding may add a Scope:"private" entry to a peer discovered with
		// only public addresses, flipping the same-origin determination.
		existing.crossOrigin = m.isCrossOrigin(existing.addresses)
		log.Printf("[SWARM] SeedBootstrapPeer: updated existing peer=%s addresses=%d", truncID(nodeID), len(existing.addresses))
		return
	}
	m.peers[nodeID] = &peerConn{
		nodeID:         nodeID,
		addresses:      addresses,
		state:          PeerDiscovered,
		discoveredAt:   time.Now(),
		crossOrigin:    m.isCrossOrigin(addresses),
		crossRegion:    m.isCrossRegion(region),
		peerRegion:     region,
		reconnectDelay: baseCooldown,
		bootstrapHost:  host,
	}
	log.Printf("[SWARM] SeedBootstrapPeer: created peer=%s host=%s region=%s addresses=%d",
		truncID(nodeID), host, region, len(addresses))
	log.Printf("[PEERS] Seeded bootstrap peer %s via host %s (%d addresses)",
		truncID(nodeID), host, len(addresses))
}

// mergeReachAddresses unions two address lists, de-duplicating on the
// (Host, Port, Proto) tuple so a peer keeps all known paths without
// accumulating duplicates across repeated seeds/refreshes.
func mergeReachAddresses(a, b []lad.ReachAddress) []lad.ReachAddress {
	seen := make(map[string]bool, len(a)+len(b))
	key := func(x lad.ReachAddress) string {
		return fmt.Sprintf("%s|%d|%s", x.Host, x.Port, x.Proto)
	}
	out := make([]lad.ReachAddress, 0, len(a)+len(b))
	for _, x := range a {
		k := key(x)
		if seen[k] {
			continue
		}
		seen[k] = true
		out = append(out, x)
	}
	for _, x := range b {
		k := key(x)
		if seen[k] {
			continue
		}
		seen[k] = true
		out = append(out, x)
	}
	return out
}

// ForceConnect resets backoff for a peer and triggers an immediate connection attempt.
// Used by MeshConvergenceManager to reconnect to missing services.
func (m *ConnectionManager) ForceConnect(ctx context.Context, nodeID string) {
	m.mu.Lock()
	peer, ok := m.peers[nodeID]
	if !ok {
		m.mu.Unlock()
		return
	}
	peer.reconnectDelay = baseCooldown
	if peer.state == PeerDisconnected {
		peer.state = PeerDiscovered
	}
	// Capture the connect decision under m.mu; peer.state is written
	// concurrently by the accept/scan paths, so reading it after Unlock raced.
	shouldConnect := peer.state == PeerDiscovered
	m.mu.Unlock()

	if shouldConnect {
		m.connectPeer(ctx, peer)
	}
}

// connectPeer tries protocols in priority order for a single peer.
func (m *ConnectionManager) connectPeer(ctx context.Context, peer *peerConn) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[PEERS] %s: connectPeer panic recovered: %v", truncID(peer.nodeID), r)
			m.mu.Lock()
			peer.state = PeerDisconnected
			m.mu.Unlock()
		}
	}()

	// Skip dial if we already have a live Aether session to this peer
	// AT GRADE A — that's the best transport, no upgrade possible.
	// For lower-grade existing sessions, fall through to the dial loop
	// so connectToPeer can try a Grade-A upgrade if the peer's reach
	// records advertise a better-grade address.
	//
	// Previously this short-circuit fired for ANY existing
	// session regardless of grade. A Grade-C inbound WS session would
	// suppress every outbound dial via this path — EnsureK was the
	// only upgrade driver, and that loop needs several other conditions to
	// fire. connectPeer participates in upgrade attempts directly when the
	// peer has a better-grade address.
	m.dispatchMu.RLock()
	sess, ok := m.meshSessions[peer.nodeID]
	m.dispatchMu.RUnlock()
	if ok && sess != nil && !sess.IsClosed() {
		existingGrade := SessionGrade(sess)
		m.mu.Lock()
		canUpgrade := existingGrade < GradeA && m.peerHasBetterGradeAddress(peer)
		m.mu.Unlock()
		if !canUpgrade {
			m.mu.Lock()
			peer.state = PeerConnected
			// Track best-ever grade via the shared promotion helper so this
			// re-register path matches the accept-side logic in
			// mesh_connection.go — without it, a peer first connected from
			// the remote side stays at GradeF forever in topology surfaces.
			peer.promoteGrade(existingGrade, time.Now())
			m.mu.Unlock()
			return
		}
		// canUpgrade: fall through to dial loop so connectToPeer can
		// attempt the better-grade transport.
	}

	m.mu.Lock()
	peer.state = PeerConnecting
	m.mu.Unlock()

	// pickPath returns a scored, ordered list of protocols to
	// try for this peer. For same-region same-origin peers it returns
	// only ProtoNoiseUDP — these pairs MUST land on noise-UDP and never
	// fall back to WebSocket (the design goal: 100% noise-UDP for
	// same-region same-org, zero WS-fallback churn). For other peers
	// it returns the full set ordered by score (class + history + grade).
	pathOrder := m.pickPath(peer)

	for _, proto := range pathOrder {
		// Skip transports under dial cooldown. The tracker exposes a
		// single growing-schedule cooldown (30s → 1m → 2m → ... → 10m
		// cap) that covers both transient failures and the long
		// sticky-bad-path suppression regime — repeatedly stuck
		// sessions push the same counter from the close-with-error
		// site, so a session that keeps tripping the aether stall
		// detector backs off without a separate sticky-path memory.
		// tryDial consumes a probe slot — call only at the actual
		// dial gate..
		if !m.tryDial(peer.nodeID, proto) {
			continue
		}

		session, err := m.dialWithProtocol(ctx, peer, proto)
		if err != nil {
			log.Printf("[PEERS] %s: %s dial failed: %v", truncID(peer.nodeID), proto, err)
			// Same-region same-origin hard rule: on noise-UDP failure
			// we DO NOT recordDialFailure (which would set the long
			// growing-schedule cooldown and cause subsequent scan
			// passes to skip UDP). Instead we record a short transient
			// backoff so the next scan pass retries UDP promptly. The
			// pickPath function returns only ProtoNoiseUDP for these
			// pairs, so the loop ends and connectPeer returns —
			// scanAndConnect will re-enter on the next tick.
			if m.isSameRegionSameOrigin(peer) && proto == ProtoNoiseUDP {
				m.recordStallCooldown(peer.nodeID, proto)
				dbgPeers.Printf("%s: same-region same-origin noise-UDP dial failed, transient cooldown (will retry)", truncID(peer.nodeID))
				continue
			}
			// Classify before recording. A local precondition
			// ("QUIC transport not initialized", "no suitable address", …)
			// implicates this node, not the peer, and records nothing; a
			// single reachability error takes the flat stall cooldown and
			// only escalates onto the exponential ladder once it repeats.
			m.recordDialOutcome(peer.nodeID, proto, err)
			continue
		}

		// Success — clear failure state for this (peer, transport).
		m.recordDialSuccess(peer.nodeID, proto)

		// Check the type assertion instead of a bare
		// session.(*aether.BaseConnection). QuicTransport.Dial returns a
		// *QuicSession, not a *BaseConnection, so on a noise-UDP→QUIC fallback
		// (same UDP address) the bare assertion panicked; the defer-recover then
		// marked the peer Disconnected but never Closed the session, leaking the
		// QUIC conn + goroutines after dial-success was already recorded.
		bs, ok := installableMeshConn(session)
		if !ok {
			log.Printf("[PEERS] %s: dial via %s returned unexpected session type %T — closing", truncID(peer.nodeID), proto, session)
			_ = session.Close()
			// Deliberately NO recordDialFailure here. The dial
			// SUCCEEDED — the handshake completed and recordDialSuccess above
			// has already (correctly) cleared this path's cooldown. An
			// unexpected session type is evidence about OUR adapter layer, not
			// about the peer or the path, and a cooldown may be recorded only
			// on evidence about the path. Recording it here suppressed a
			// reachable peer for up to 10 minutes — and because ProtoTLS and
			// ProtoWebSocket share one tracker key, a mismatch on
			// one of them suppressed the other too.
			continue
		}
		conn := bs.Conn

		m.mu.Lock()
		peer.outboundDialed = true
		peer.reconnectDelay = baseCooldown
		if bs.InitialRTT() > 0 {
			peer.initRTT = bs.InitialRTT() // dial/handshake timing — separate from gossip RTT
		}
		// Snapshot the mutable peer fields the handoff goroutine needs
		// while we hold m.mu — reading peer.peerRegion/peer.bootstrapHost in the
		// `go` call arguments raced writers under m.mu. nodeID is immutable.
		peerRegion := peer.peerRegion
		bootstrapHost := peer.bootstrapHost
		m.mu.Unlock()

		dbgPeers.Printf("%s: dialed via %s", truncID(peer.nodeID), proto)

		// Aether — negotiate and accept in goroutine so connectPeer returns immediately.
		// peerServiceName() reads the service identity from LAD member attrs
		// (populated either by a prior successful gossip exchange or by the
		// bootstrap response). Passing it into DialAndAcceptMesh lets
		// AcceptMeshConnection re-publish the Member record right after the
		// new session is established so topology views remain consistent
		// across session churn.
		svcName := m.peerServiceName(peer.nodeID)
		log.Printf("[AETHER] connectPeer dialed %s via %s, launching Aether session", truncID(peer.nodeID), proto)
		go m.rt.DialAndAcceptMesh(ctx, conn, peer.nodeID, peerRegion, proto, bootstrapHost, svcName, "", dialOwnsConnectingState)
		return
	}

	// All protocols failed — try reactivating a dormant transport (e.g., TLS bootstrap)
	m.mu.Lock()
	hasDormant := peer.getDormantTransport() != nil
	m.mu.Unlock()

	if hasDormant {
		log.Printf("[PEERS] %s: all dial protocols failed, reactivating dormant transport", truncID(peer.nodeID))
		m.reactivateDormantTransport(ctx, peer)
		return
	}

	// No dormant transport either — truly disconnected. The drop is
	// already reflected in the shared quality tracker's reliability
	// EMA via the per-session close handler, so the scaler's failure
	// factor will pick it up automatically on the next rebalance.

	m.mu.Lock()
	peer.state = PeerDisconnected
	peer.outboundDialed = false // allow re-dial on next scan
	peer.reconnectDelay = min(peer.reconnectDelay*2, maxCooldown)
	m.mu.Unlock()
	log.Printf("[PEERS] %s: all protocols failed, backoff %v", truncID(peer.nodeID), peer.reconnectDelay)

	// Dormant reactivation now handled in connectPeer() above (line 713-722)
}

// dialWithProtocol attempts a specific transport protocol.
// dialWithProtocol establishes a transport connection to a peer.
// Returns aether.Connection — the caller extracts net.Conn via Inner() and
// wraps it in an Aether session via DialAndAcceptMesh.
func (m *ConnectionManager) dialWithProtocol(ctx context.Context, peer *peerConn, proto Protocol) (aether.Connection, error) {
	dialCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	// Snapshot peer.crossOrigin under m.mu (written by scanAndConnect/
	// resync); read the local below instead of racing the field.
	m.mu.Lock()
	peerCrossOrigin := peer.crossOrigin
	m.mu.Unlock()

	// Build target address based on protocol
	addr := m.bestAddress(peer, proto)
	if addr == "" {
		// Cross-org dial funnel telemetry: dial aborted at the address-
		// resolution gate. Must approximately equal the sum of
		// crossOrgBestAddrCandidatesEmpty + crossOrgBestAddrAllDead for
		// ProtoNoiseUDP calls.
		if proto == ProtoNoiseUDP && peerCrossOrigin {
			m.crossOrgDialNoSuitableAddr.Add(1)
		}
		return nil, fmt.Errorf("no suitable address for %s: %w", proto, errLocalDialState)
	}

	target := aether.Target{
		NodeID:  aether.NodeID(peer.nodeID),
		Address: addr,
	}

	// Public anycast noise-UDP dials need a routing preamble whenever the
	// resolved address is a public hostname or anycast IP. Fly's 5-tuple
	// hash distributes the datagram across the destination app's N
	// machines; only the machine holding the target NodeID's listener
	// can complete the handshake. The preamble lets the anycast-receiving
	// machine (M_r) hairpin to the target machine (M_t) over 6PN.
	//
	// This fires for BOTH cross-org AND same-org dials when the address
	// landed on a public hostname (Tier-3 peerServiceHostname fallback,
	// or any public-anycast IPv4 entry from peer.addresses). Same-org
	// dials that resolve to a private 6PN ULA via bestAddress Tier 0
	// keep the preamble OFF — those route machine-to-machine directly
	// via Fly's 6PN mesh and don't need hairpinning.
	//
	// Non-IPv4 / non-UDP protos never need the preamble — gRPC/WSS dials
	// hit the peer's HTTPS edge which already routes per-machine.
	anycastForwarderDial := proto == ProtoNoiseUDP && isPublicAnycastHostPort(addr)
	// Cross-org dial funnel telemetry: bestAddress picked a non-anycast
	// address for a cross-org noise-UDP peer (e.g. direct public IPv4 or
	// IPv6 ULA). Preamble correctly skipped — the gap between this counter
	// and crossOrgPreambleDialsAttempted indicates whether peers
	// advertise anycast hostnames vs direct public IPs.
	if proto == ProtoNoiseUDP && peerCrossOrigin && !anycastForwarderDial {
		m.crossOrgBestAddrNonAnycastSelected.Add(1)
	}
	if anycastForwarderDial {
		if target.Metadata == nil {
			target.Metadata = make(map[string]string, 1)
		}
		target.Metadata[noise.MetadataKeyCrossOrgPreamble] = "1"
		m.crossOrgPreambleDialsAttempted.Add(1)
	}

	switch proto {
	case ProtoNoiseUDP:
		conn, err := m.rt.dialNoiseUDP(dialCtx, target)
		if anycastForwarderDial {
			if err == nil {
				m.crossOrgPreambleDialsSucceeded.Add(1)
			} else {
				m.crossOrgPreambleDialsFailed.Add(1)
			}
		}
		return conn, err
	case ProtoQUIC:
		if m.quicTr == nil {
			return nil, fmt.Errorf("QUIC transport not initialized: %w", errLocalDialState)
		}
		return m.quicTr.Dial(dialCtx, target)
	case ProtoWebSocket:
		if m.wsTr == nil {
			return nil, fmt.Errorf("WebSocket transport not initialized: %w", errLocalDialState)
		}
		// 6PN-direct path: bestAddress returned a raw private IPv6:port,
		// meaning this is a same-org Fly peer reachable over the 6PN
		// mesh. Dial plain ws:// straight to the peer's HTTP listener —
		// no TLS (6PN is WireGuard-encrypted at L3 and Noise
		// authenticates the peer at L7, so a cert adds nothing), no
		// public edge, no hijack-relay fallback (there's no edge proxy
		// to work around). A failure here is a real connectivity
		// failure and is returned as-is.
		if isSixPNHostPort(addr) {
			wsDirect := target
			wsDirect.Address = fmt.Sprintf("ws://%s/mesh/ws", addr)
			session, err := m.wsTr.Dial(dialCtx, wsDirect)
			if err == nil {
				return session, nil
			}
			return nil, fmt.Errorf("6PN-direct ws dial: %w", err)
		}
		// Try standard WebSocket first
		wsTarget := target
		wsTarget.Address = fmt.Sprintf("wss://%s/mesh/ws", addr)
		session, err := m.wsTr.Dial(dialCtx, wsTarget)
		if err == nil {
			return session, nil
		}
		// Transient DNS timeouts on Fly's regional resolver have been
		// observed even when the public name is healthy from other
		// vantage points — a single retry after a short delay clears the
		// typical resolver glitch. Permanent failures (NXDOMAIN, refused)
		// skip the retry via isTransientDNSError.
		if isTransientDNSError(err) {
			select {
			case <-dialCtx.Done():
				return nil, err
			case <-time.After(500 * time.Millisecond):
			}
			if session2, err2 := m.wsTr.Dial(dialCtx, wsTarget); err2 == nil {
				log.Printf("[PEERS] %s: WS dial succeeded after DNS retry", truncID(peer.nodeID))
				return session2, nil
			} else {
				err = err2
			}
		}
		// Sustained DNS failure escape hatch: when the public-DNS path is
		// broken end-to-end (the regional resolver itself is degraded,
		// not just glitching) we can still reach Fly peers via Fly's
		// internal DNS scheme. `app.orbtr.io` → `app-orbtr-io.flycast`
		// resolves through Fly's 6PN-private resolver, which uses an
		// independent path from the public regional resolver and stays
		// up when the latter is broken. We dial the .flycast address
		// but pass the original public hostname through Metadata as
		// the TLS SNI override — the same backend machine serves the
		// public cert on its private 6PN address, so cert validation
		// succeeds via SNI even though the TCP connection bypasses
		// public DNS entirely.
		if isDNSError(err) {
			publicHost := stripURLHost(addr)
			if internalAddr := flyInternalAddress(addr); internalAddr != "" {
				wsInternal := target
				wsInternal.Address = fmt.Sprintf("wss://%s/mesh/ws", internalAddr)
				wsInternal.Metadata = mergeMetadata(target.Metadata, map[string]string{
					"sni_host": publicHost,
				})
				if session3, err3 := m.wsTr.Dial(dialCtx, wsInternal); err3 == nil {
					log.Printf("[PEERS] %s: WS dial succeeded via Fly internal %s (public DNS sustainedly broken, SNI=%s)",
						truncID(peer.nodeID), internalAddr, publicHost)
					return session3, nil
				} else {
					log.Printf("[PEERS] %s: Fly internal fallback also failed: %v", truncID(peer.nodeID), err3)
				}
			}
		}
		// Fallback: HTTP/1.1 hijack relay — avoids proxy interference on
		// Fly.io/Cloudflare by using raw TCP framing instead of WS frames.
		log.Printf("[PEERS] %s: WS dial failed, trying hijack relay fallback: %v", truncID(peer.nodeID), err)
		hjTarget := target
		hjTarget.Address = addr // DialHijack constructs URL internally
		return m.wsTr.DialHijack(dialCtx, hjTarget)
	case ProtoGRPC:
		if m.grpcTr == nil {
			return nil, fmt.Errorf("gRPC transport not initialized: %w", errLocalDialState)
		}
		return m.grpcTr.Dial(dialCtx, target)
	case ProtoTLS:
		// TLS bootstrap dial — uses runtime's dialBootstrapHost which does
		// HTTPS upgrade → VL1 connection. Only works for bootstrap-capable hosts.
		host := addr
		if host == "" {
			return nil, fmt.Errorf("no TLS host for %s: %w", truncID(peer.nodeID), errLocalDialState)
		}
		result, err := m.rt.dialBootstrapHost(dialCtx, host)
		if err != nil {
			return nil, fmt.Errorf("TLS bootstrap dial to %s: %w", host, err)
		}
		// Create a session-like wrapper from the raw conn
		return aether.NewConnection(m.rt.identity.NodeID, aether.NodeID(result.nodeID), result.rawConn), nil
	default:
		return nil, fmt.Errorf("unknown protocol: %d: %w", proto, errLocalDialState)
	}
}

// startGossip was removed — AcceptMeshConnection owns the full gossip lifecycle.
//
// The previous tryUpgrades upgrade-chain walk (dial higher-grade
// protocols when peer is on a sub-optimal path) lives inside the
// multipath manager's EnsureK reconnect loop now: it iterates
// protocolOrder skipping already-active protocols and dials the
// highest-grade missing one. See multipath_dial.go::installEnsureKDialFn.

// bestAddress returns the most suitable address for a protocol.
//
// For HTTP-based protocols (WebSocket, gRPC), this is protocol-agnostic:
// any hostname-based reach record (wss, tls, grpc) or the peer's service_name
// from member attributes can be used, since a hostname with valid TLS works
// for all HTTP-based transports. This enables peer-to-peer WS connections
// even when the peer only has a hostname from DNS self-resolution (not a
// dedicated WSS reach record).
func (m *ConnectionManager) bestAddress(peer *peerConn, proto Protocol) string {
	// Snapshot the mutable dial inputs under m.mu. peer.addresses is
	// reassigned (peer.addresses = ...) by scanAndConnect / resyncStalePeerAddresses
	// under m.mu, while this function runs lock-free — a bare `range peer.addresses`
	// could tear the slice header (ptr,len,cap) and panic with an out-of-bounds
	// index or dial a garbage address. Copy once, then read only the locals.
	m.mu.Lock()
	peerAddresses := append([]lad.ReachAddress(nil), peer.addresses...)
	peerCrossOrigin := peer.crossOrigin
	m.mu.Unlock()

	// addressCandidate pairs an address with its tier priority (lower = better);
	// AddressTracker scores can override the static tier order while still
	// using tier as the final tiebreaker.
	type addressCandidate struct {
		addr string
		tier int // 0 = best tier, higher = worse
	}

	// Transport key uses node Protocol.String() so it matches what
	// RecordPathSuccess/RecordPathFailure write into AddressTracker.
	// Going through aether.Protocol would conflate distinct dial paths
	// (TLS bootstrap shares aether.ProtoWebSocket with plain WS).
	transport := proto.String()

	// scoredBest sorts candidates by (1) alive before dead, (2) higher
	// success rate, (3) lower RTT, (4) original tier order. Returns ""
	// if every candidate is in dead-cooldown so the caller falls back to
	// no-address rather than dialing a known-broken endpoint.
	scoredBest := func(candidates []addressCandidate) string {
		if len(candidates) == 0 {
			return ""
		}

		// TIER ORDER FIRST, UNCONDITIONALLY. Tier is a property
		// of the ADDRESS — IPv4 (1) before IPv6 (2), private 6PN (0) before
		// both — and needs no AddressTracker to evaluate. Placing it inside
		// the tracker-only sort below gives a tracker-independent property a
		// tracker dependency by CO-LOCATION: with no tracker the function
		// returns candidates[0] in APPEND order while
		// its comment claimed tier order, and IPv4 preference did not exist.
		// A nil tracker is a supported state (mesh_services.go), so that was
		// the common path, not an edge.
		sort.SliceStable(candidates, func(i, j int) bool {
			if candidates[i].tier != candidates[j].tier {
				return candidates[i].tier < candidates[j].tier
			}
			// Stable lex at equal tier so the pick does not flap when LAD
			// reorders the reach record between gossip rounds.
			return candidates[i].addr < candidates[j].addr
		})

		// Health scoring needs the tracker; tier ordering above does not.
		// An early return for a missing dependency must skip ONLY the work
		// that needs it.
		if m.addressTracker == nil {
			return candidates[0].addr
		}

		sort.SliceStable(candidates, func(i, j int) bool {
			si, oki := m.addressTracker.Score(peer.nodeID, transport, candidates[i].addr)
			sj, okj := m.addressTracker.Score(peer.nodeID, transport, candidates[j].addr)

			// No history — preserve original tier order
			if !oki && !okj {
				return candidates[i].tier < candidates[j].tier
			}
			if !oki {
				return false // unknown sorts after known
			}
			if !okj {
				return true // known sorts before unknown
			}

			// Dead paths sort last
			if si.IsAlive() != sj.IsAlive() {
				return si.IsAlive()
			}

			// Higher success rate wins
			if si.SuccessRate != sj.SuccessRate {
				return si.SuccessRate > sj.SuccessRate
			}

			// Lower RTT wins
			if si.RTT != sj.RTT {
				return si.RTT < sj.RTT
			}

			// Primary tiebreaker: original tier order.
			if candidates[i].tier != candidates[j].tier {
				return candidates[i].tier < candidates[j].tier
			}
			// Stable lex on addr string at exactly-equal tier
			// so the picked candidate doesn't flap when LAD reorders
			// the reach record between gossip rounds.
			return candidates[i].addr < candidates[j].addr
		})

		// Skip dead paths entirely — fall through to no-address.
		//
		// This is reached for a SINGLE candidate too. Behind a
		// `len(candidates) == 1` fast path, a peer whose only address is
		// known-dead is re-dialled every cycle for the whole 30-minute
		// cooldown — the tight loop AddressDeadCooldown exists to
		// prevent, and the opposite of what this comment promised. The
		// cooldown EXPIRES (measured: 30m, cleared on any success), so
		// returning "" suppresses dialling temporarily and strands nothing.
		if best, ok := m.addressTracker.Score(peer.nodeID, transport, candidates[0].addr); ok && best.IsDead() {
			return ""
		}
		return candidates[0].addr
	}

	switch proto {
	case ProtoNoiseUDP, ProtoQUIC:
		var candidates []addressCandidate

		// Tier 0: Private IPs for same-origin. Require an IPv6 ULA
		// (Fly's 6PN fdaa:) or — failing that — at
		// least skip Docker bridge RFC1918 + link-local that have no
		// chance of routing. Without this filter, a peer that publishes
		// BOTH a 6PN fdaa:.. private entry AND a 172.18.x.x Docker
		// bridge private entry can have scoredBest pick the Docker
		// IP first (no AddressTracker history; falls back to slice
		// order). That dial fails, the 6PN address is never tried in
		// that cycle, and noise-UDP looks broken for the peer.
		if !peerCrossOrigin {
			// Sub-tier 0a: IPv6 6PN (routable private). Tier value 0.
			// Sub-tier 0b: other private — only included as fallback
			// because the 6PN address didn't parse / wasn't found.
			// The unscored tier still races against AddressTracker
			// history; the explicit sub-tier ordering ensures a
			// known-routable 6PN beats an unknown Docker-bridge IP.
			//
			// Freshness preference for the noise-UDP Tier-0 path: when the
			// swarm AddressTable holds any noise-UDP candidate for this
			// peer we treat it as the authoritative source and IGNORE
			// peerAddresses 6PN entries entirely. peerAddresses is a
			// per-tick snapshot taken in scanAndConnect; an AddressTable
			// onRecord that arrives between scan ticks (e.g. after a
			// peer-machine restart re-publishes its 6PN endpoint) is
			// invisible to peerAddresses until the next scan tick fires
			// and the stale-6PN replace block runs again. Reading the
			// AddressTable directly here closes that window — the dialer
			// always targets the freshest 6PN known to the swarm fabric
			// for same-origin noise-UDP, never a cached pre-restart entry.
			// Falls back to peerAddresses when the AddressTable has no
			// noise-UDP entry for this peer (swarm not wired, or the
			// PeerRecord hasn't been received yet).
			privateAddrs := peerAddresses
			if proto == ProtoNoiseUDP && m.rt != nil && m.rt.swarm != nil && m.rt.swarm.AddressTable != nil {
				cands := m.rt.swarm.AddressTable.Get(peer.nodeID)
				hasUDP := false
				for _, c := range cands {
					if c.Transport == "noise-udp" {
						hasUDP = true
						break
					}
				}
				if hasUDP {
					fresh := make([]lad.ReachAddress, 0, len(cands))
					for _, c := range cands {
						if c.Transport != "noise-udp" || c.Host == "" || c.Port == 0 {
							continue
						}
						scope := c.Scope
						if scope == "" {
							scope = scopeForSwarmHost(c.Host)
						}
						fresh = append(fresh, lad.ReachAddress{
							Host:  c.Host,
							Port:  int(c.Port),
							Proto: "udp",
							Scope: scope,
						})
					}
					privateAddrs = fresh
				}
			}
			var v6Privates, otherPrivates []addressCandidate
			for _, a := range privateAddrs {
				if a.Scope != "private" {
					continue
				}
				ip := net.ParseIP(a.Host)
				if ip == nil {
					continue // unparseable, skip
				}
				if ip.IsLoopback() || ip.IsLinkLocalUnicast() {
					continue // never useful
				}
				cand := addressCandidate{
					addr: net.JoinHostPort(a.Host, fmt.Sprintf("%d", a.Port)),
					tier: 0,
				}
				if ip.To4() == nil && isRoutablePrivateIP(ip) {
					v6Privates = append(v6Privates, cand)
				} else {
					otherPrivates = append(otherPrivates, cand)
				}
			}
			candidates = append(candidates, v6Privates...)
			// Only fall back to other-private candidates when no 6PN
			// IPv6 address exists — Docker-bridge IPv4 has no chance
			// of routing to a same-org peer's UDP listener.
			if len(v6Privates) == 0 {
				candidates = append(candidates, otherPrivates...)
			}
		}
		// Tier 1: Public IPv4 (preferred for UDP — some platforms don't proxy UDP on IPv6)
		// Tier 2: Public IPv6 (fallback)
		publicUDPCount := 0
		for _, a := range peerAddresses {
			if a.Scope == "public" && a.Proto == "udp" {
				ip := net.ParseIP(a.Host)
				tier := 2 // IPv6
				if ip != nil && ip.To4() != nil {
					tier = 1 // IPv4
				}
				candidates = append(candidates, addressCandidate{
					addr: net.JoinHostPort(a.Host, fmt.Sprintf("%d", a.Port)),
					tier: tier,
				})
				publicUDPCount++
			}
		}

		// Tier 3: peerServiceHostname fallback on the configured VL1 UDP
		// port. CROSS-ORG ONLY. Mirrors the WS branch's Tier 2 service-
		// hostname fallback below; without it, cross-org noise-UDP has
		// no dialable address until the AddressTable merge completes
		// and the first dial loses the protocolOrder race to WS.
		//
		// Same-org dials intentionally DO NOT take this fallback even
		// though peerServiceHostname is populated for same-org peers
		// too. Anycast UDP cannot guarantee per-packet machine affinity:
		// the initial handshake via M_r → M_t preamble works, but
		// subsequent datagrams from the initiator may 5-tuple-hash to a
		// DIFFERENT machine in the destination app which has no session
		// state for this conversation, drops the packets, and the
		// session times out after ~5-30 s. This was the observable
		// symptom on the v0.0.388 fleet: walker probes succeeded, then
		// the upgraded session died inside a single ping window. The
		// correct same-org path is Tier 0 (private 6PN ULA from
		// AddressTable) which routes machine-to-machine via Fly's 6PN
		// mesh — no anycast hash, no per-packet machine selection.
		// Pending the AddressTable merge, noise-UDP stays unresolved
		// for same-org peers; the walker picks them up on the next
		// 30 s tick after the merge lands.
		if peerCrossOrigin {
			if hostname := m.peerServiceHostname(peer.nodeID); hostname != "" {
				udpPort := m.rt.cfg.VL1.UDPPort
				if udpPort > 0 {
					candidates = append(candidates, addressCandidate{
						addr: net.JoinHostPort(hostname, fmt.Sprintf("%d", udpPort)),
						tier: 3,
					})
				}
			}
		}

		// Cross-org dial funnel telemetry: surface how many public-UDP
		// candidates the peer published and whether the pool was empty.
		// Single Add() per call, not per-iteration (kept lean per the
		// instrumentation-cost lens of the Batch 5 plan).
		if peerCrossOrigin {
			if publicUDPCount > 0 {
				m.crossOrgBestAddrPublicUDPCount.Add(uint64(publicUDPCount))
			}
			if len(candidates) == 0 {
				m.crossOrgBestAddrCandidatesEmpty.Add(1)
			}
		}

		if addr := scoredBest(candidates); addr != "" {
			return addr
		}
		// scoredBest returned "" with non-empty candidates: every public
		// UDP candidate was filtered out by AddressTracker dead-cooldown.
		// Isolates sticky-suppression as the gate.
		if peerCrossOrigin && len(candidates) > 0 {
			m.crossOrgBestAddrAllDead.Add(1)
		}

		// No UDP address found — log for diagnosing why Noise/QUIC fails
		if len(peerAddresses) > 0 {
			dbgPeers.Printf("%s: no UDP address for %s (cross_origin=%v, addrs=%d)",
				truncID(peer.nodeID), proto, peerCrossOrigin, len(peerAddresses))
		}

	case ProtoWebSocket, ProtoGRPC, ProtoTLS:
		if proto == ProtoTLS && peer.bootstrapHost != "" {
			return peer.bootstrapHost
		}

		// Tier -1: 6PN-direct for same-org Fly peers (WebSocket only).
		// Dial the peer's private mesh IP on its HTTP port instead of
		// the public hostname — this bypasses the Fly edge proxy
		// entirely. The edge reaps long-lived internal mesh connections
		// (the dominant source of grade-C session churn — a healthy WS
		// session through the edge dies in 4-26 s with "readLoop
		// exited"); a direct 6PN dial is a stable machine-to-machine
		// path with no proxy lifecycle, no anycast misrouting, no TLS
		// termination hop. Cross-origin peers and the TLS bootstrap
		// path keep the public-hostname route.
		if proto == ProtoWebSocket {
			if sixpn := m.peerSixPNWSAddress(peer); sixpn != "" {
				return sixpn
			}
		}

		var candidates []addressCandidate

		// Tier 0: Explicit WSS reach record (dedicated WebSocket endpoint)
		for _, a := range peerAddresses {
			if a.Proto == "wss" {
				candidates = append(candidates, addressCandidate{
					addr: a.Host, // hostname like "devices.orbtr.io" — caller prepends wss://
					tier: 0,
				})
			}
		}

		// Tier 1: Protocol-agnostic hostname from ANY non-UDP reach record.
		// A hostname with valid TLS works for WS/gRPC too (not just the
		// original protocol). This enables peer-to-peer connections when
		// the peer has a hostname from DNS self-resolution or service_name
		// but no dedicated WSS record.
		for _, a := range peerAddresses {
			if a.Scope == "public" && a.Proto != "udp" && isHostname(a.Host) {
				candidates = append(candidates, addressCandidate{
					addr: a.Host,
					tier: 1,
				})
			}
		}

		// Tier 2: Peer's service_name from member attributes (e.g., "node.hstles.com").
		if hostname := m.peerServiceHostname(peer.nodeID); hostname != "" {
			candidates = append(candidates, addressCandidate{
				addr: hostname,
				tier: 2,
			})
		}

		if addr := scoredBest(candidates); addr != "" {
			return addr
		}

		// Debug: log why no WS/gRPC address was found
		dbgPeers.Printf("%s: no %s address found (addrs: %d, cross_origin: %v)", truncID(peer.nodeID), proto, len(peer.addresses), peer.crossOrigin)
		for i, a := range peer.addresses {
			dbgPeers.Printf("  addr[%d]: host=%s port=%d proto=%s scope=%s", i, a.Host, a.Port, a.Proto, a.Scope)
		}

		// No hostname-based address available — don't fall back to raw STUN IPs
		// (they're outbound NAT IPs that don't route inbound on Fly)
	}
	return ""
}

// isHostname returns true if the address is a DNS hostname (not a raw IP).
// Hostnames are needed for TLS SNI on Fly/Cloudflare proxied connections.
func isHostname(addr string) bool {
	// Strip port if present
	host := addr
	if h, _, err := net.SplitHostPort(addr); err == nil {
		host = h
	}
	// If it parses as an IP, it's not a hostname
	return net.ParseIP(host) == nil && host != ""
}

// isTransientDNSError reports whether err is a recoverable DNS lookup
// failure that's worth retrying once. Distinguishes transient resolver
// problems (i/o timeout, temporary failure, server misbehaving) from
// permanent ones (NXDOMAIN, refused) so we don't burn the dial budget
// retrying impossible names.
func isTransientDNSError(err error) bool {
	if err == nil {
		return false
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return dnsErr.IsTimeout || dnsErr.IsTemporary
	}
	// Wrapped error from the dialer often loses the typed net.DNSError
	// wrapping by the time it surfaces here ("dial tcp: lookup ...:
	// i/o timeout"). Fall back to substring match for the timeout
	// case — but NOT "no such host", which is permanent.
	s := err.Error()
	return strings.Contains(s, "lookup") &&
		(strings.Contains(s, "i/o timeout") || strings.Contains(s, "server misbehaving"))
}

// isDNSError reports whether err is any DNS lookup failure — transient
// or permanent. Used by the sustained-failure fallback path which
// kicks in regardless of why the public-DNS path failed: NXDOMAIN can
// indicate a DNS plane that's lying about a real hostname (observed
// during regional resolver outages on Fly), and i/o timeout is the
// normal "DNS plane is down" signal. Either way the Fly-internal
// resolver path is worth trying since it uses a separate plane.
func isDNSError(err error) bool {
	if err == nil {
		return false
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return true
	}
	s := err.Error()
	return strings.Contains(s, "lookup") &&
		(strings.Contains(s, "i/o timeout") ||
			strings.Contains(s, "server misbehaving") ||
			strings.Contains(s, "no such host") ||
			strings.Contains(s, "Temporary failure"))
}

// stripURLHost extracts the bare hostname (no port, no path, no scheme)
// from any of the URL-ish forms callers pass around — `host`,
// `host:port`, `wss://host/path`, `https://host:port/`. Returns ""
// for inputs that don't have a recognisable host. Used to derive the
// TLS SNI hint for the Fly-internal fallback path.
func stripURLHost(s string) string {
	if s == "" {
		return ""
	}
	// Trim scheme
	for _, p := range []string{"wss://", "https://", "ws://", "http://"} {
		if strings.HasPrefix(s, p) {
			s = strings.TrimPrefix(s, p)
			break
		}
	}
	// Trim path/query/fragment
	if idx := strings.IndexAny(s, "/?#"); idx >= 0 {
		s = s[:idx]
	}
	// Trim port
	if h, _, err := net.SplitHostPort(s); err == nil {
		s = h
	}
	return s
}

// mergeMetadata returns a new map containing all entries from base
// followed by overrides (overrides win on key collision). nil-safe on
// either input. Never mutates base.
func mergeMetadata(base, overrides map[string]string) map[string]string {
	out := make(map[string]string, len(base)+len(overrides))
	for k, v := range base {
		out[k] = v
	}
	for k, v := range overrides {
		out[k] = v
	}
	return out
}

// flyInternalAddress converts a public Fly app hostname to the Fly
// internal flycast form, which resolves via Fly's 6PN-private DNS
// rather than the regional public resolver. e.g.
//
//	app.orbtr.io      → app-orbtr-io.flycast
//	node.hstles.com   → node-hstles-com.flycast
//	bootstrap.hstles.com → bootstrap-hstles-com.flycast
//
// Returns "" for raw IPs, .local addresses, or empty input — those
// already bypass the public DNS path or have no Fly equivalent. The
// caller pairs this address with the original public hostname for
// TLS SNI so cert validation still works (Fly machines serve the
// same public cert on every interface, including 6PN).
//
// Why .flycast and not .internal: .flycast resolves to a single
// stable anycast IP per app and routes to the nearest healthy
// machine; .internal returns one IP per instance and would round-
// robin including unhealthy ones. flycast is the right primitive
// for "give me a working machine for this app" and is the form Fly
// recommends for app-to-app private networking.
func flyInternalAddress(hostname string) string {
	h := hostname
	// Strip port if present
	if hp, _, err := net.SplitHostPort(h); err == nil {
		h = hp
	}
	if h == "" || net.ParseIP(h) != nil {
		return ""
	}
	// .local / .internal already-internal-form hostnames don't need
	// transformation; assume the caller knows what they're doing.
	if strings.HasSuffix(h, ".local") ||
		strings.HasSuffix(h, ".internal") ||
		strings.HasSuffix(h, ".flycast") {
		return ""
	}
	// Public hostname → Fly app slug (dots → dashes).
	appName := strings.ReplaceAll(h, ".", "-")
	return appName + ".flycast"
}

// peerServiceHostname looks up the peer's service_name from LAD member
// attributes. service_name is a hostname (e.g., "node.hstles.com") that
// routes to the peer via Fly/DNS and supports TLS.
// Must NOT be called with m.mu held — acquires cache.mu internally.
func (m *ConnectionManager) peerServiceHostname(peerNodeID string) string {
	if m.rt.cache == nil {
		return ""
	}
	// nil seam: VL1/LAD disabled. A nil INTERFACE panics on any call, unlike
	// the nil *DirectoryCache this replaced, so the guard is required and not
	// defensive padding.
	if m.rt.liveDir == nil {
		return ""
	}
	member, ok, err := m.rt.liveDir.Member(context.Background(), "", ports.NodeID(peerNodeID))
	if err != nil || !ok {
		return ""
	}
	svcName := member.Attrs["serviceName"]
	// Only return if it looks like a hostname (contains a dot, not an IP)
	if svcName != "" && strings.Contains(svcName, ".") && isHostname(svcName) {
		return svcName
	}
	return ""
}

// peerSixPNWSAddress returns a same-org peer's 6PN-private WebSocket
// dial address ("[fdaa:...]:8080") — or "" if the peer is cross-origin
// or advertises no usable private address. Dialing this routes the
// connection machine-to-machine over Fly's 6PN WireGuard mesh straight
// to the peer's HTTP listener, bypassing the public edge proxy that
// otherwise reaps long-lived mesh sessions.
//
// The private address is sourced from the peer's Reach record (the
// Scope:"private" entries the Interface discoverer publishes); the
// port from its advertised http_port member attr (fleet default 8080).
// Must NOT be called with m.mu held — peerHTTPPort acquires cache.mu.
func (m *ConnectionManager) peerSixPNWSAddress(peer *peerConn) string {
	if peer == nil || peer.crossOrigin {
		return ""
	}
	var sixpnHost string
	for _, a := range peer.addresses {
		if a.Scope != "private" {
			continue
		}
		h := a.Host
		if hp, _, err := net.SplitHostPort(h); err == nil {
			h = hp
		}
		ip := net.ParseIP(h)
		// 6PN addresses are IPv6 ULA (fdaa: / fd00::/8). Skip IPv4
		// RFC1918 (Docker bridge etc.) and anything non-routable.
		if ip != nil && ip.To4() == nil && isRoutablePrivateIP(ip) {
			sixpnHost = h
			break
		}
	}
	if sixpnHost == "" {
		return ""
	}
	return net.JoinHostPort(sixpnHost, m.peerHTTPPort(peer.nodeID))
}

// peerHTTPPort returns the peer's advertised HTTP/WS port from its LAD
// member attributes, falling back to defaultMeshHTTPPort when the attr
// is absent (peers predating http_port advertisement). The whole fleet
// runs the mesh HTTP server on 8080, so the fallback is correct for
// every current endpoint.
// Must NOT be called with m.mu held — acquires cache.mu internally.
func (m *ConnectionManager) peerHTTPPort(peerNodeID string) string {
	if m.rt.cache == nil {
		return defaultMeshHTTPPort
	}
	if m.rt.liveDir == nil {
		return defaultMeshHTTPPort
	}
	member, ok, err := m.rt.liveDir.Member(context.Background(), "", ports.NodeID(peerNodeID))
	if err != nil || !ok {
		return defaultMeshHTTPPort
	}
	if p := member.Attrs["http_port"]; p != "" {
		return p
	}
	return defaultMeshHTTPPort
}

// isSixPNHostPort reports whether addr ("host:port") has a 6PN-private
// IPv6 host — i.e. bestAddress chose the Tier -1 direct path and the
// WebSocket dial should use plain ws:// straight to the peer's 6PN
// listener rather than wss:// via the public edge.
func isSixPNHostPort(addr string) bool {
	h := addr
	if hp, _, err := net.SplitHostPort(addr); err == nil {
		h = hp
	}
	ip := net.ParseIP(h)
	// Strict fdaa::/8 match — see isFly6PN comment.
	return isFly6PN(ip)
}

// isPublicAnycastHostPort reports whether addr ("host:port") is a
// public-internet routable address — the kind of address where Fly's
// per-app anycast distributes UDP via 5-tuple hash and the cross-org
// routing preamble is needed to hairpin to the right machine.
//
// Accepts:
//   - literal public IPv4 (anything outside RFC1918/loopback/link-local)
//   - literal public IPv6 (anything outside fc00::/7 ULA, which covers
//     Fly's 6PN fdaa::/8 — those are same-org direct paths)
//   - FQDN-style hostnames (containing ".") — in production the swarm
//     AddressTable publishes Fly hostnames like "bootstrap.hstles.com"
//     for the noise-UDP transport row, not pre-resolved IP literals.
//     A FQDN dial WILL go through Fly's anycast 5-tuple hash exactly
//     the same as a literal public-IP dial, so the preamble must fire.
//     Was Batch 3 telemetry finding: with the prior IP-only check, every
//     cross-org dial via hostname silently bypassed the preamble path,
//     and the 5-tuple hash landed on the target machine only ~1/N of
//     the time — leaving most cross-org peers WS-pinned with no signal
//     and forwarder_drops_* + cross_org_preamble_dials_attempted both
//     stuck at 0 (the telemetry that surfaced this bug).
//
// Filters out:
//   - loopback / unspecified / link-local literal IPs
//   - IPv4 RFC1918 ranges
//   - IPv6 ULA (fc00::/7)
//   - Bare hostnames without a dot ("localhost", per-test private DNS),
//     since those resolve to loopback/private addresses in practice.
//
// The v4-private filter and the FQDN branch are both required: without the
// FQDN branch a hostname bypasses the check silently.
func isPublicAnycastHostPort(addr string) bool {
	h := addr
	if hp, _, err := net.SplitHostPort(addr); err == nil {
		h = hp
	}
	if ip := net.ParseIP(h); ip != nil {
		if ip.IsLoopback() || ip.IsUnspecified() || ip.IsLinkLocalUnicast() {
			return false
		}
		// IPv4 private ranges (RFC1918, CGNAT, etc.).
		if v4 := ip.To4(); v4 != nil {
			return !v4.IsPrivate()
		}
		// IPv6 ULA (fc00::/7) covers Fly's 6PN fdaa::/8.
		return !ip.IsPrivate()
	}
	// Non-IP host string. Treat FQDN-style names as public anycast
	// (production case: "bootstrap.hstles.com"). Reject bare names
	// ("localhost", "node") to keep dev/test environments out of the
	// forwarder path.
	return strings.Contains(h, ".") && h != "localhost"
}

// getOurOrigin atomically reads the configured origin prefix. Returns
// empty string when discoverPrivateIP() failed at construction AND the
// retry goroutine hasn't yet succeeded — callers must handle empty as a
// "don't know yet" signal, not as "we're sure there's no origin".
func (m *ConnectionManager) getOurOrigin() string {
	if p := m.ourOriginAtomic.Load(); p != nil {
		return *p
	}
	return ""
}

// setOurOrigin atomically replaces the origin prefix. Used at
// construction. No late-detection retry is needed because
// PlatformInfo.PrivateIP() resolves synchronously on every supported cloud.
func (m *ConnectionManager) setOurOrigin(s string) {
	v := s
	m.ourOriginAtomic.Store(&v)
}

// isCrossOrigin checks if a peer is in a different network origin (e.g.,
// different Fly org, different VPC).
//
// Decision rule:
//  1. ourOrigin unknown (non-Fly deploy / boot race) → treat as CROSS
//     (conservative — emits the cross-org preamble so the forwarder
//     hairpin works on Fly; same-org peers pay 34 bytes of overhead
//     that the receiver ignores). Previously this returned false and
//     silently disabled the forwarder fleet-wide whenever the boot
//     race left ourOrigin empty.
//  2. ANY of the peer's Scope:"private" addresses matches our origin → same
//  3. Any Scope:"private" address belongs to a DIFFERENT origin → cross
//  4. NO Scope:"private" addresses at all → treat as CROSS (was same-org
//     before). Rationale: a same-org peer almost
//     always publishes a 6PN reach record. A peer with only public
//     addresses is most likely cross-org (the public-only side of a
//     cross-org pair). Treating it as cross-org costs at most a 34-byte
//     preamble that a same-org receiver ignores, but UNLOCKS the
//     forwarder hairpin for the legitimate cross-org case.
func (m *ConnectionManager) isCrossOrigin(addrs []lad.ReachAddress) bool {
	ourOrigin := m.getOurOrigin()
	if ourOrigin == "" {
		return true
	}
	sawPrivate := false
	for _, a := range addrs {
		if a.Scope != "private" {
			continue
		}
		peerOrg := extractOriginPrefix(a.Host)
		if peerOrg == "" {
			// Not a Fly 6PN host — e.g., Docker bridge IPv4 leaking
			// through. Don't let it drive the same/cross decision.
			continue
		}
		sawPrivate = true
		if peerOrg != ourOrigin {
			return true
		}
	}
	// All recognised private entries matched our origin → same-org.
	if sawPrivate {
		return false
	}
	// No usable private entry → conservative cross-org.
	return true
}

// stuckCooldownCap caps the per-peer per-protocol stuck-escalation
// ladder. With a 4× multiplier, four stuck events take noise-udp from
// 30 s → 2 min → 8 min → 32 min → 32 min (capped). After any
// successful gossip run the count resets and we drop back to the base.
const stuckCooldownCap = 32 * time.Minute

// stuckRecoverySuccesses is the number of successful gossip exchanges
// on a given protocol after which we consider the path "recovered" and
// reset stuckCount[proto] to zero. Five exchanges is well past
// proving-window noise (gossip cadence is 1-2 s adaptive) but short
// enough that a genuinely-recovered path doesn't carry an outdated
// long-cooldown for hours.
const stuckRecoverySuccesses = 5

// Sticky-bad-path memory tunables. Cooldown alone allows infinite retry
// on a permanently-broken protocol — every (cooldown elapsed + dial +
// handshake + first 60s) is wasted before the same stuck-kill happens
// again. Sticky suppression hard-skips the protocol for a longer window
// once a recent failure pattern is established, letting the working
// transport carry traffic without competing with doomed upgrade attempts.
const (
	// stickyStuckWindow is the sliding window over which stuck-kills
	// are counted. Long enough that genuine path-flap (e.g. a wifi
	// blip every 5 min) accumulates entries; short enough that a
	// once-an-hour stuck-event doesn't trip suppression.
	stickyStuckWindow = 30 * time.Minute

	// stickyStuckThreshold is the number of stuck-kills within the
	// window required to trigger suppression. Three is the smallest
	// number that gives confidence the path is genuinely bad rather
	// than a one-off transient.
	stickyStuckThreshold = 3

	// stickyStuckSuppression is the duration of the suppression once
	// triggered. One hour matches the typical fly.io anycast-routing
	// stability window and is short enough that a path that genuinely
	// recovers (e.g. fly publishes a routing fix) gets retried within
	// an acceptable downtime, but long enough that we don't waste
	// resources probing a still-broken path every minute.
	stickyStuckSuppression = 1 * time.Hour
)

// noteStuckProto is called when a session closes with aether.ErrSessionStuck.
// Increments stuckCount for the protocol and snapshots successCount so
// noteStuckRecovery can detect "recovered for real" later.
//
// Also records the stuck-event timestamp into the sticky-bad-path memory.
// If the peer has accumulated stickyStuckThreshold stuck-kills within the
// stickyStuckWindow, the protocol is suppressed for stickyStuckSuppression
// — the upgrader and dialer both honour that suppression and stay on
// whatever transport is currently working.
func (m *ConnectionManager) noteStuckProto(peer *peerConn, proto Protocol) {
	if peer == nil {
		return
	}
	if peer.stuckCount == nil {
		peer.stuckCount = make(map[Protocol]int)
	}
	if peer.stuckSuccessSince == nil {
		peer.stuckSuccessSince = make(map[Protocol]int)
	}
	peer.stuckCount[proto]++
	peer.stuckSuccessSince[proto] = peer.successCount

	// Stuck-session backoff lives on the quality.Tracker. The caller
	// in mesh_connection.go that invokes noteStuckProto also calls
	// recordDialFailure(nodeID, proto) which increments the per-(peer,
	// transport) dial-failure count and sets the growing cooldown,
	// so a session that keeps tripping the aether stall detector
	// still gets suppressed via the tracker's IsDialSuppressed query
	// the next time the dialer asks.
}

// noteStuckRecovery is called from the gossip-success path. If a peer
// has accumulated stuckRecoverySuccesses successful exchanges on the
// current protocol since its last stuck event, the protocol is treated
// as recovered and stuckCount is dropped to zero so the next failure
// (if any) starts from base cooldown rather than carrying penalty from
// an old loss episode that has clearly passed.
//
// Recovery also clears the sticky-suppression record so a protocol that
// genuinely heals (path quality improves) is not held in suppression
// purgatory for the full stickyStuckSuppression window.
func (m *ConnectionManager) noteStuckRecovery(peer *peerConn, proto Protocol) {
	if peer == nil || peer.stuckCount == nil {
		return
	}
	count, hasCount := peer.stuckCount[proto]
	if !hasCount || count == 0 {
		return
	}
	since := peer.stuckSuccessSince[proto]
	if peer.successCount-since >= stuckRecoverySuccesses {
		delete(peer.stuckCount, proto)
		delete(peer.stuckSuccessSince, proto)
		// Sustained successful gossip means the path has actually
		// healed; clear the tracker's dial-failure state so the
		// growing cooldown starts fresh from a future failure.
		m.recordDialSuccess(peer.nodeID, proto)
	}
}

// Dial-suppression / cooldown / path-score-skip logic moved to:
//
//   - quality.Tracker.IsDialSuppressed (queried via dialIsSuppressed).
//     One growing-schedule cooldown covering both transient failures
//     and the long sticky-bad-path regime — no separate boolean.
//   - quality.dialCooldownDuration (30s → 1m → 2m → 4m → 8m → 10m cap).
//     The previous per-protocol base values became obsolete when
//     transport class and physical distance were decoupled via
//     quality.RouteClass.
//   - quality.Score's reliability + failure-penalty components handle
//     the "skip if low success rate" case at dispatch ranking time;
//     EnsureK already skips suppressed transports during reconnect.

// extractOriginPrefix gets the network origin identifier from a private address.
// On Fly, this is the org prefix from fdaa: addresses. On other platforms, this
// could be a VPC ID, subnet prefix, or similar network boundary identifier.
// fdaa:a:ba33:... → "a:ba33", fdaa:4d:ce3c:... → "4d:ce3c"
func extractOriginPrefix(ip string) string {
	if !strings.HasPrefix(ip, "fdaa:") {
		return ""
	}
	parts := strings.Split(ip, ":")
	if len(parts) < 3 {
		return ""
	}
	return parts[1] + ":" + parts[2]
}

// updatePriorities recalculates connection priority for all peers based on
// anchor status and recent RPC activity. Called periodically from the scan loop.
func (m *ConnectionManager) updatePriorities() {
	// Snapshot peer data under m.mu, then release before cache lookups
	// to avoid deadlock (cache.Roles acquires cache.mu).
	type peerSnapshot struct {
		nodeID         string
		rpcsLastMinute int
		lastRPCAt      time.Time
	}
	m.mu.Lock()
	snapshots := make([]peerSnapshot, 0, len(m.peers))
	for _, peer := range m.peers {
		if peer.state != PeerConnected {
			continue
		}
		snapshots = append(snapshots, peerSnapshot{
			nodeID:         peer.nodeID,
			rpcsLastMinute: peer.rpcsLastMinute,
			lastRPCAt:      peer.lastRPCAt,
		})
	}
	m.mu.Unlock()

	// Do cache lookups (anchor checks) without holding m.mu
	anchorSet := make(map[string]bool, len(snapshots))
	for _, s := range snapshots {
		anchorSet[s.nodeID] = m.isAnchorNode(s.nodeID)
	}

	// Re-acquire m.mu to apply priority results
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, s := range snapshots {
		peer, ok := m.peers[s.nodeID]
		if !ok || peer.state != PeerConnected {
			continue
		}
		switch {
		case anchorSet[s.nodeID]:
			peer.priority = PriorityCritical
		case peer.rpcsLastMinute > 10:
			peer.priority = PriorityHigh
		case peer.rpcsLastMinute > 0:
			peer.priority = PriorityNormal
		case peer.rpcsLastMinute == 0 && !peer.lastRPCAt.IsZero() && time.Since(peer.lastRPCAt) < 5*time.Minute:
			peer.priority = PriorityLow
		default:
			peer.priority = PriorityIdle
		}
		// Sync priority into the connection budget for external queries
		m.budget.SetPriority(peer.nodeID, peer.priority)
	}
}

// isAnchorNode checks if a peer has the "anchor" role in the directory.
// Must NOT be called with m.mu held — acquires the cache's lock internally.
//
// Reads are cut by capability — Role/Address first — so this read is routed
// through the LiveDirectory port rather than reaching into
// ladcache directly. Member carries the node's role list and the LAD adapter
// populates it from the same role records, with the same empty tenant, so the
// routed read and a direct ladcache read return the same answer.
//
// Fails CLOSED when the port is absent. That direction is load-bearing: an
// anchor is pinned to PriorityCritical and thereby exempted from drain
// selection, so answering "yes" without a directory would make every peer
// undrainable rather than merely unprotected.
func (m *ConnectionManager) isAnchorNode(nodeID string) bool {
	if m.rt == nil || m.rt.liveDir == nil {
		return false
	}
	mem, ok, err := m.rt.liveDir.Member(context.Background(), "", ports.NodeID(nodeID))
	if err != nil || !ok {
		return false
	}
	for _, role := range mem.Roles {
		if role == "anchor" {
			return true
		}
	}
	return false
}

// feedReputationFromRTT pushes the latest measured RTT for each
// connected peer into the ReputationTracker so its stability factor
// reflects current network conditions.
//
// 🛑 THIS LOOP CURRENTLY DOES NOTHING, AND THE JUSTIFICATION BELOW IT WAS
// FALSE. Measured:
//
//   - Both Inject* methods open with `rep, ok := rt.scores[nodeID]; if !ok
//     { return }`, and ReputationTracker.ComputeAll is the ONLY thing that
//     ever inserts a key. ComputeAll has ZERO callers in any root, so
//     rt.scores is permanently empty and every call from here returns at
//     the guard.
//   - The old comment said "the tracker feeds LAD reach-scoring". It does
//     not and cannot: reachScore lives in ledger/cache/directory.go:1701,
//     ranks only ReachRecord fields, and the ledger module does not import
//     loom (the dependency runs the other way). There is no path from a
//     reputation score to that ranking.
//
// The calls are left in place rather than deleted because the subsystem is
// CORRECT and merely uncalled — see peer_reputation_test.go, which proves
// the same injections land once ComputeAll has run. Wiring a periodic
// ComputeAll is the open decision; until then this is an inert loop and
// should not be cited as a live input to anything.
//
// Dispatch-side congestion-demotion is not a separate feed: that capability
// lives inside quality.Score's RTT component, normalised against RouteClass
// rather than a fleet-wide constant.
func (m *ConnectionManager) feedReputationFromRTT() {
	if m.reputationTracker == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, peer := range m.peers {
		if peer.state != PeerConnected || peer.lastRTT == 0 {
			continue
		}
		m.reputationTracker.InjectRTT(peer.nodeID, peer.lastRTT)
		grade := peer.bestActiveGrade()
		gradeNorm := float64(grade) / float64(GradeA)
		// Stability "since the current grade was established" devolves to
		// "since the current connection was established" once congestion-
		// driven demotion is gone — peer.lastConnected is the right
		// timestamp, refreshed every successful gossip exchange.
		m.reputationTracker.InjectGradeInfo(peer.nodeID, peer.lastConnected, gradeNorm)
	}
}

// RecordRPC records an RPC event on the connection to a peer.
// Called by RPC dispatch when traffic flows through a peer connection —
// rpc_forward.go's callOverMeshSession, the single funnel every outbound peer
// RPC takes (both the BidiRPC and dynamic-stream paths).
//
// 🔴 THIS IS THE SOLE WRITER OF PER-PEER RPC TRAFFIC, AND TWO INDEPENDENT
// SUBSYSTEMS READ WHAT IT WRITES. Both degrade to a constant rather than to an
// error if it is not called, so neither surfaces the loss:
//
//   - peer.rpcsLastMinute / peer.lastRPCAt feed updatePriorities' ladder
//     (:4219-4224). Permanently zero meant three of its five arms — High,
//     Normal and Low — were unreachable, so EVERY non-anchor peer sat at
//     PriorityIdle and drain selection could not tell a peer carrying all the
//     traffic from one carrying none.
//   - ConnectionScaler.rpcCounters feeds the TrafficWeight factor of the
//     connection-sizing formula (connection_scaling.go:196-204). Permanently
//     empty meant trafficWeight = 1 + ln(1+0/10) = 1.0 exactly, so the fourth
//     of five factors was an identity multiply on every peer forever.
//
// Both consumers are fed from here rather than from two call sites, so the
// two counters cannot drift apart.
func (m *ConnectionManager) RecordRPC(peerNodeID string) {
	m.mu.Lock()
	peer, ok := m.peers[peerNodeID]
	if ok {
		peer.rpcsLastMinute++
		peer.lastRPCAt = time.Now()
	}
	m.mu.Unlock()

	if !ok {
		return
	}
	// 🔴 THE SCALER IS CREDITED OUTSIDE m.mu, AND THAT IS LOAD-BEARING, NOT
	// TIDINESS. pruneCountersFor (connection_scaling.go:137) holds s.mu while
	// invoking its shouldPrune callback, and the caller at :1753 passes a
	// closure that takes m.mu — so an s.mu -> m.mu order already exists.
	// Crediting the scaler while holding m.mu would add m.mu -> s.mu and close
	// a classic inversion between the scan loop's prune and this hot path.
	if m.scaler != nil {
		m.scaler.RecordRPC(peerNodeID)
	}
}

// resetRPCCounters resets the per-minute RPC counters for all peers.
// Called once per minute from the scan loop to provide a sliding window.
func (m *ConnectionManager) resetRPCCounters() {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, peer := range m.peers {
		peer.rpcsLastMinute = 0
	}
}

// closeDrainedConnection is called by DrainManager when a drain completes.
// It cancels the peer's gossip, releases the budget slot, and decrements connCount.
// resetConnectingState transitions a peer stuck in PeerConnecting back to
// PeerDisconnected. connectPeer sets PeerConnecting before handing a successful
// transport dial to the async DialAndAcceptMesh; if the Aether setup then fails
// nothing else resets the state, and neither the toConnect builder nor
// pruneStalePeers handle PeerConnecting — so a bootstrap-only / NAT'd peer was
// never re-dialed or evicted. No-op if the peer moved on (e.g. an inbound accept
// already connected it).
func (m *ConnectionManager) resetConnectingState(nodeID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if peer, ok := m.peers[nodeID]; ok && peer.state == PeerConnecting {
		peer.state = PeerDisconnected
	}
}

func (m *ConnectionManager) closeDrainedConnection(peerNodeID, transport string, drainStartedAt time.Time) {
	m.mu.Lock()
	peer, exists := m.peers[peerNodeID]
	if !exists {
		m.mu.Unlock()
		return
	}

	proto := ParseProtocol(transport)
	var cancel context.CancelFunc
	if peer.transports != nil {
		if tc, ok := peer.transports[proto]; ok && tc != nil {
			// 🔴 CANCEL ONLY THE TRANSPORT THIS DRAIN WAS STARTED FOR. The map is
			// keyed by protocol alone, so a reconnect during the grace window
			// installs a replacement under the same key. Cancelling that one
			// tears down a healthy connection the scaler never selected, and the
			// drain accounting then attributes it to a voluntary scale-down.
			// connectedAt is the only thing distinguishing the instances.
			if tc.connectedAt.After(drainStartedAt) {
				m.mu.Unlock()
				log.Printf("[DRAIN] %s to %s was replaced during the grace window; leaving the newer transport alone",
					transport, truncID(peerNodeID))
				return
			}
			// Mark the drain so AcceptMeshConnection's cleanup treats
			// this as a voluntary scale-down, not a chronic/path failure.
			tc.draining = true
			cancel = tc.cancelFunc
		}
	}

	// Do NOT decrement connCount or release the budget here.
	// Cancelling the transport's connCtx drives AcceptMeshConnection's cleanup,
	// which is the SINGLE owner of that accounting (connCount--, budget.Release,
	// state transition). Doing it in both places double-decremented
	// currentTotal — so MaxTotal stopped being enforced — and dropped a
	// multi-transport peer's connCount by two.
	peer.drainState = DrainActive

	// Record the drain time for this transport class so scanAndConnect
	// doesn't immediately re-dial it. Drains are voluntary scale-downs
	// (a better path already exists); re-dialing right away just reopens
	// the same connection only for the rebalance to drain it again a few
	// seconds later, producing a short-lifetime redial/drain cycle.
	if peer.drainedAt == nil {
		peer.drainedAt = make(map[Protocol]time.Time)
	}
	peer.drainedAt[proto] = time.Now()
	oldGrade := GradeForProtocol(peer.protocol) // Capture under the lock
	m.mu.Unlock()

	// Cancelling triggers cleanup, which performs the connCount/budget
	// accounting and (because tc.draining is set) skips the failure counters.
	if cancel != nil {
		cancel()
	}

	// Emit disconnect event for the drained connection
	if m.scaler != nil {
		m.scaler.EmitEvent(ConnectionEvent{
			PeerNodeID: peerNodeID,
			OldGrade:   oldGrade,
			NewGrade:   GradeF,
			Transport:  transport,
			Reason:     "drained",
			Timestamp:  time.Now(),
		})
	}

	log.Printf("[DRAIN] Closed drained %s connection to %s", transport, truncID(peerNodeID))
}

// RegisterConnection and UnregisterConnection were removed.
// AcceptMeshConnection owns the full connection lifecycle.

// PeerDispatchHealth returns the fraction of Aether sessions that are healthy (0.0-1.0).
func (m *ConnectionManager) PeerDispatchHealth() float64 {
	m.dispatchMu.RLock()
	defer m.dispatchMu.RUnlock()
	if m.meshSessions == nil {
		return 0
	}
	total, healthy := 0, 0
	for _, session := range m.meshSessions {
		total++
		// Nil-guard for consistency with sweepZombieSessions, which
		// treats a nil map value as possible; a nil session here would panic.
		if session != nil && !session.IsClosed() {
			healthy++
		}
	}
	if total == 0 {
		return 0
	}
	return float64(healthy) / float64(total)
}

// isCrossOriginLocked checks cross-origin using ONLY the peer's
// stored addresses — no LAD I/O, no async lookups. Safe to call
// while holding m.mu.
//
// The LAD-backed variant lives in computeCrossOriginForNode and MUST
// be called before m.mu is taken (it does network/cache I/O that
// can stall under load, and holding a hot mutex across LAD reads stalls
// the whole mesh).
//
// A `return false` stub here stamps every inbound-discovered cross-org peer
// crossOrigin=false permanently, so subsequent cross-org dials never
// emitted the routing preamble and the forwarder hairpin never engaged.
// That defeated the cross-org forwarder shipped in aether v0.0.46.
// Audit critical fix #4 split off the LAD I/O path.
func (m *ConnectionManager) isCrossOriginLocked(nodeID string) bool {
	// 1. Use the existing peerConn entry's addresses if present —
	// purely in-memory, no I/O.
	if peer, ok := m.peers[nodeID]; ok && len(peer.addresses) > 0 {
		return m.isCrossOrigin(peer.addresses)
	}
	// 2. No in-memory addresses for this peer yet — conservative
	// default: treat as cross-org so the preamble fires. A
	// misclassified same-org peer pays a 34-byte preamble that its
	// receiver ignores; a misclassified cross-org peer's noise dials
	// would otherwise fail every retry. Callers that have a chance to
	// query LAD without holding m.mu should use
	// computeCrossOriginForNode instead, which fills the gap by
	// consulting the reach cache.
	return true
}

// computeCrossOriginForNode resolves cross-origin for a node by
// querying the LAD reach cache. MUST be called without m.mu held —
// the reach cache has its own locking and can stall under contention,
// so blocking m.mu across this call would serialize the entire
// connection table.
//
// The intended call pattern, used by AcceptMeshConnection:
//
//	crossOrigin := m.computeCrossOriginForNode(nodeID) // I/O, no m.mu
//	m.mu.Lock()                                        // fast path
//	peer = &peerConn{... crossOrigin: crossOrigin}
//	m.mu.Unlock()
//
// Returns the same conservative-default (cross-org=true) as
// isCrossOriginLocked when the lookup yields no information.
func (m *ConnectionManager) computeCrossOriginForNode(nodeID string) bool {
	// Existing peerConn entry is the fastest signal — fall to that
	// when present. Guard m.mu around the map read alone (short,
	// no I/O); the LAD lookup below runs with the lock dropped.
	m.mu.Lock()
	peer, ok := m.peers[nodeID]
	var addrs []lad.ReachAddress
	if ok {
		addrs = peer.addresses
	}
	m.mu.Unlock()
	if len(addrs) > 0 {
		return m.isCrossOrigin(addrs)
	}
	// LAD lookup happens with NO m.mu held — the only contention is
	// the reach cache's own internal mutex.
	if m.rt != nil && m.rt.cache != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		defer cancel()
		recs, err := m.rt.cache.Reach(ctx, "", ladcache.ReachQuery{NodeID: nodeID})
		if err == nil && len(recs) > 0 {
			return m.isCrossOrigin(recs[0].Addresses)
		}
	}
	return true
}

// connectionInfoFor builds a ConnectionInfo snapshot for a connected peer.
// Must be called with m.mu held.
// TotalDroppedFrames returns the aggregate count of dropped frames across
// all peer connections. Aether uses its own flow control; this returns 0
// as Aether sessions handle backpressure natively.
func (m *ConnectionManager) TotalDroppedFrames() int64 {
	return 0
}

func (m *ConnectionManager) connectionInfoFor(peer *peerConn) ConnectionInfo {
	return ConnectionInfo{
		PeerNodeID:  peer.nodeID,
		Transport:   peer.protocol.String(),
		Grade:       GradeForProtocol(peer.protocol),
		RTT:         peer.lastRTT,
		Region:      peer.peerRegion,
		CrossRegion: peer.crossRegion,
		ConnCount:   peer.connCount,
		ConnectedAt: peer.lastConnected,
		State:       peer.state,
		Priority:    peer.priority,
	}
}

// connectionInfoPerTransport returns one ConnectionInfo per non-dormant
// active transport in peer.transports, so multipath-aware callers
// (notably the scaler's drain selection) can sort and choose per-class
// rather than collapsing every transport to peer.protocol — which on a
// freshly-upgraded peer reflects only the most recent accept.
//
// Per-entry Grade comes from the transportConn.grade field (not
// GradeForProtocol(peer.protocol)) so a session whose grade has been
// adjusted at runtime — by a stuck-detector demotion or a deliberate
// scaler downgrade — is sorted by its current grade, not its proto
// nominal grade. ConnectedAt comes from transportConn.connectedAt so
// shouldDrainFirst's age tiebreak preserves the older session and
// drains the newer one within the same grade.
//
// Falls back to a single-entry list built from connectionInfoFor when
// peer.transports is nil or empty (e.g., a peer that connected via a
// path that hasn't installed the multipath bookkeeping yet) so the
// caller never sees a zero-length slice for a connected peer.
//
// Must be called with m.mu held.
func (m *ConnectionManager) connectionInfoPerTransport(peer *peerConn) []ConnectionInfo {
	if peer == nil {
		return nil
	}
	if len(peer.transports) == 0 {
		return []ConnectionInfo{m.connectionInfoFor(peer)}
	}
	out := make([]ConnectionInfo, 0, len(peer.transports))
	for proto, tc := range peer.transports {
		if tc == nil || tc.isDormant {
			continue
		}
		out = append(out, ConnectionInfo{
			PeerNodeID:  peer.nodeID,
			Transport:   proto.String(),
			Grade:       tc.grade,
			RTT:         peer.lastRTT,
			Region:      peer.peerRegion,
			CrossRegion: peer.crossRegion,
			ConnCount:   peer.connCount,
			ConnectedAt: tc.connectedAt,
			State:       peer.state,
			Priority:    peer.priority,
		})
	}
	if len(out) == 0 {
		return []ConnectionInfo{m.connectionInfoFor(peer)}
	}
	return out
}

func min(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

// converge ensures at least one connection to every discovered service.
// Merged from MeshConvergenceManager — runs on 60s ticker in Start() loop.
func (m *ConnectionManager) converge(ctx context.Context) {
	if m.rt.cache == nil {
		return
	}

	// 1. Build set of known services from LAD member records
	services := map[string][]string{} // serviceName → []nodeID
	if m.rt.liveDir == nil {
		return
	}
	members, err := m.rt.liveDir.Members(ctx, "")
	if err != nil {
		return
	}
	for _, member := range members {
		if string(member.NodeID) == m.selfID {
			continue
		}
		svc := member.Attrs["serviceName"]
		if svc != "" {
			services[svc] = append(services[svc], string(member.NodeID))
		}
	}
	if len(services) == 0 {
		return
	}

	// 2. Build set of services we're connected to
	connectedServices := map[string]bool{}
	m.mu.Lock()
	for _, p := range m.peers {
		if p.state == PeerConnected || p.hasActiveTransport() {
			svc := m.peerServiceNameLocked(p.nodeID)
			if svc != "" {
				connectedServices[svc] = true
			}
		}
	}
	m.mu.Unlock()

	// 3. Also count Aether sessions as connected
	m.dispatchMu.RLock()
	if m.meshSessions != nil {
		for nodeID, session := range m.meshSessions {
			if session != nil && !session.IsClosed() { // Nil-guard
				svc := m.peerServiceName(nodeID)
				if svc != "" {
					connectedServices[svc] = true
				}
			}
		}
	}
	m.dispatchMu.RUnlock()

	// 4. For each service we're NOT connected to, force a connection attempt
	for svc, nodeIDs := range services {
		if connectedServices[svc] {
			continue
		}
		for _, nodeID := range nodeIDs {
			// §0.5.3 step 4: routed onto the port. This asks only
			// whether the node has ANY dial candidate — no address value
			// escapes, so none of the reach VOCABULARY crosses this seam and
			// the normalised/raw distinction cannot affect the answer. The
			// three sibling reads that DO propagate addresses into
			// peerConn.addresses stay on ladcache until that migration is
			// claimed separately.
			addrs, err := m.rt.liveDir.Reach(ctx, "", ports.NodeID(nodeID))
			if err != nil || len(addrs) == 0 {
				continue
			}
			log.Printf("[CONVERGE] No connection to service %s, attempting %s", svc, truncID(nodeID))
			m.ForceConnect(ctx, nodeID)
			break
		}
	}
}

// peerServiceName looks up a peer's serviceName from LAD member records.
func (m *ConnectionManager) peerServiceName(nodeID string) string {
	if m.rt.cache == nil {
		return ""
	}
	if m.rt.liveDir == nil {
		return ""
	}
	member, ok, err := m.rt.liveDir.Member(context.Background(), "", ports.NodeID(nodeID))
	if err != nil || !ok {
		return ""
	}
	return member.Attrs["serviceName"]
}

// peerServiceNameLocked is peerServiceName but safe to call with m.mu held
// (uses cache which has its own lock, no deadlock risk).
func (m *ConnectionManager) peerServiceNameLocked(nodeID string) string {
	return m.peerServiceName(nodeID)
}

// reactivateDormantTransport resumes gossip on a dormant transport (e.g., TLS)
// when all other transports to a peer have died. Probes the connection first.
func (m *ConnectionManager) reactivateDormantTransport(_ context.Context, peer *peerConn) {
	m.mu.Lock()
	tc := peer.getDormantTransport()
	if tc == nil {
		m.mu.Unlock()
		return
	}
	// All streams are already alive on dormant connections — just clear the flag.
	// No probe needed since gossip was running the whole time.
	tc.isDormant = false
	peer.state = PeerConnected
	peer.protocol = tc.protocol
	// Reactivating a dormant transport must promote the peer to that
	// transport's grade — otherwise the bestEverGrade ceiling never
	// rises past GradeF and topology surfaces under-report what we
	// know to be the best path we have to this node.
	peer.promoteGrade(tc.grade, time.Now())
	m.mu.Unlock()

	dbgPeers.Printf("%s: reactivated dormant %s transport (streams were already alive)", truncID(peer.nodeID), tc.protocol)
}
