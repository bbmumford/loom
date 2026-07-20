/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"crypto/ed25519"
	"log"
	"sync"
	"time"

	"github.com/ORBTR/aether"
	"github.com/bbmumford/route"
	"github.com/bbmumford/swarm"
	swarmpb "github.com/bbmumford/swarm/proto/pb"
	"github.com/bbmumford/loom/pkg/rpc"
)

// SwarmIntegration aggregates the swarm-side wiring on Runtime: the
// swarm.Node, the unified PeerPublisher, and the RoleTable +
// AddressTable subscribers that index incoming PeerRecords.
//
// Callers migrate from legacy `cache.Roles` + reach lookup paths to this
// substrate one call site at a time (mesh_session_finder,
// peer_connections.pickPath, etc.); each switch-over is independent.
type SwarmIntegration struct {
	Node         swarm.Node
	Publisher    *PeerPublisher
	RoleTable    *RoleTable
	AddressTable *AddressTable
	Transport    *MeshSwarmTransport
}

// InitSwarm wires the swarm engine + tables + publisher into the Runtime.
// Returns the SwarmIntegration on success. Idempotent: subsequent calls
// return the existing integration without re-initializing.
//
// Called from Runtime.Initialize after the identity is loaded and the
// rpc.Registry is built. The swarm engine is NOT auto-started — callers
// must Start() the swarm Node via the returned integration once the
// transport layer is up.
//
// No ctx parameter: every long-running site below (SessionHook,
// node.Start, publisher.run) binds to the process-lifetime rt.Context().
// Accepting a caller ctx would either be misleading (silently ignored)
// or actively wrong (closure capture surviving rt.cancel). See the
// rtCtx comment below.
func (rt *Runtime) InitSwarm(reg *rpc.Registry) (*SwarmIntegration, error) {
	log.Printf("[SWARM] InitSwarm: entry (existing=%v identity=%v)", rt.swarm != nil, rt.identity != nil)
	// Capture the reflection registry for the runtime role-activation
	// path (handler re-publish after a takeover activation).
	rt.rpcRegistry = reg
	if rt.swarm != nil {
		log.Printf("[SWARM] InitSwarm: returning existing integration")
		return rt.swarm, nil
	}
	if rt.identity == nil || rt.identity.PrivateKey == nil {
		log.Printf("[SWARM] InitSwarm: ERROR identity not set")
		return nil, ErrSwarmIdentityUnset
	}

	localID := swarm.NodeID(rt.identity.NodeID)
	log.Printf("[SWARM] InitSwarm: localID=%s", truncID(string(localID)))

	cfg := swarm.Config{
		NodeID:     localID,
		PrivKey:    ed25519.PrivateKey(rt.identity.PrivateKey),
		TreeDegree: 4,
	}
	node, err := swarm.New(cfg)
	if err != nil {
		log.Printf("[SWARM] InitSwarm: ERROR swarm.New: %v", err)
		return nil, err
	}
	log.Printf("[SWARM] InitSwarm: swarm.New ok")

	tport := NewMeshSwarmTransport(localID)
	if err := swarm.Wire(node, tport); err != nil {
		log.Printf("[SWARM] InitSwarm: ERROR swarm.Wire: %v", err)
		return nil, err
	}
	log.Printf("[SWARM] InitSwarm: swarm.Wire ok")

	roleTable, err := NewRoleTable(node, string(rt.identity.NodeID))
	if err != nil {
		log.Printf("[SWARM] InitSwarm: ERROR NewRoleTable: %v", err)
		return nil, err
	}
	log.Printf("[SWARM] InitSwarm: RoleTable subscribed to fleet.peer")
	addrTable, err := NewAddressTable(node)
	if err != nil {
		log.Printf("[SWARM] InitSwarm: ERROR NewAddressTable: %v", err)
		return nil, err
	}
	log.Printf("[SWARM] InitSwarm: AddressTable subscribed to fleet.peer")

	publisher := NewPeerPublisher(node, rt, reg)
	log.Printf("[SWARM] InitSwarm: PeerPublisher constructed (registry=%v)", reg != nil)

	// Stamp the region tag onto every PeerRecord we publish, independent of
	// whether PublishRPCHandlersToLAD ever runs. Thin gateways skip that
	// call, so without this the region tag stays empty on their records and
	// every receiver's role-affinity sameRegion tier is silently disabled
	// against them — same-region peer pairs never get the +1 K bump that
	// keeps an intra-region session warm.
	if rt.cfg.Platform != nil {
		publisher.SetRegion(rt.cfg.Platform.Region())
	}

	rt.swarm = &SwarmIntegration{
		Node:         node,
		Publisher:    publisher,
		RoleTable:    roleTable,
		AddressTable: addrTable,
		Transport:    tport,
	}

	// Wire the AddressTable -> ConnectionManager wake hook. When a
	// PeerRecord delivers a Tier-0 (6PN ULA noise-UDP) address,
	// AddressTable invokes this callback which signals the upgrade
	// walker. Without this signal the walker waits up to its 30s
	// ticker to discover the new address — a same-org peer stuck on
	// WebSocket can sit on Grade C for half a minute after the 6PN
	// endpoint actually arrives via gossip. SignalWalkerWake is non-
	// blocking and goroutine-safe so it satisfies the AddressTable's
	// onRecord contract (cannot stall the swarm subscriber dispatcher).
	//
	// ConnectionManager is allocated before InitSwarm (see runtime.go
	// "SYNCHRONOUSLY allocate ConnectionManager BEFORE bootstrap"), so
	// rt.connMgr is always non-nil here. Belt-and-braces nil check
	// guards future refactors that might break that ordering.
	if rt.connMgr != nil {
		addrTable.SetWakeCallback(rt.connMgr.SignalWalkerWake)
		log.Printf("[SWARM] InitSwarm: AddressTable wake hook wired -> ConnectionManager.SignalWalkerWake")
	}

	// Wire the observer-tombstone gate. Every inbound observer
	// attestation passes through RoleTable.IsAnchorWithPubKey before
	// counting toward the K-of-N quorum, which (a) restricts attestation
	// authority to peers with the anchor role and (b) binds the claimed
	// ObserverNodeID to the Sig-verified PubKey on the attestation. A
	// non-anchor or pubkey-mismatch attestation is silently dropped;
	// without this gate any peer could mint attestations and the K-of-N
	// quorum would be trivially Sybil-defeatable.
	node.SetObserverRoleCheck(roleTable.IsAnchorWithPubKey)
	log.Printf("[SWARM] InitSwarm: observer-tombstone gate wired (anchor + pubkey binding)")

	// Plumb the anchor role into the swarm Node's role enum.
	//
	// There are TWO notions of "role" and they were never connected. The
	// STRING role ("anchor") is advertised in the PeerRecord by PeerPublisher
	// and is what RoleTable/topology display. The swarm Node's Role ENUM
	// (RoleLeaf..RoleAnchor) is what SelfRole() returns — and NOTHING in the
	// mesh ever called SetRole, so SelfRole() returned RoleLeaf (the zero
	// value) on every node, forever.
	//
	// Every observer-tombstone emit path gates on SelfRole() == RoleAnchor:
	// the zombie-session sweep (peer_connections.shouldEmitObserverTombstone),
	// and the two staleness sweeps below. With the enum never set, ALL of them
	// silently returned on the first line — so no node ever emitted an observer
	// attestation, and the K-of-N mechanism, though fully wired, had never once
	// fired in production. That is the deepest reason the ghosts were immortal:
	// not that the sweeps were session-blind, but that the anchor gate every
	// sweep shares could never pass.
	//
	// isAnchorCapable() is the same predicate that makes PeerPublisher
	// advertise the "anchor" string role, so the enum and the advertised role
	// now agree by construction.
	if rt.isAnchorCapable() {
		if err := node.SetRole(swarm.RoleAnchor); err != nil {
			log.Printf("[SWARM] InitSwarm: SetRole(anchor) failed: %v", err)
		} else {
			log.Printf("[SWARM] InitSwarm: swarm Node role set to ANCHOR (observer attestation enabled)")
		}
	}

	// Wire the LAD liveness sweep to attest, so a node that dies abruptly can
	// finally be forgotten by the whole mesh rather than by one node at a time.
	//
	// Until now the only caller of PublishObserverTombstone was
	// ConnectionManager.sweepZombieSessions, which reaps dead SESSIONS. A node
	// that vanished without ever holding a session here — every machine
	// replaced by a deploy — is invisible to it: nothing to sweep, so nothing
	// ever attested, so the corpse was never collectively forgotten. The
	// directory's 16-minute liveness sweep is the one place that DOES see it,
	// and its verdict was local-only ("liveness-local" is deliberately not
	// gossiped), so every peer re-gossiped the corpse back to whoever had just
	// evicted it. Measured: 11 real machines, 40 identities, ghosts surviving
	// 15+ hours; node-orbtr-io alone carried 3 anchor identities for 1 machine.
	//
	// The two halves existed and were never joined. This joins them.
	//
	// Safety is the quorum's job, not ours: this publishes ONE witness. It
	// becomes a real propagating death only once K distinct anchors
	// independently reach the same verdict within the corroboration window, so
	// a node that merely blips cannot be evicted by whichever peer noticed
	// first. We gate on the anchor role only because a non-anchor's attestation
	// is dropped by the role check above and would be pure wire noise.
	//
	// Deliberately NOT reusing ConnectionManager.shouldEmitObserverTombstone:
	// its LastGossipSeen check is the right gate for the session sweep, but the
	// liveness sweep has already DELETED that entry by the time it reports —
	// the check would read zero and refuse every attestation. Crossing the
	// 16-minute liveness timeout IS the evidence here, and it clears that
	// gate's 60-second silence bar by 16x.
	if rt.cache != nil {
		rt.cache.SetOnLivenessEvict(func(nodeID string) {
			if nodeID == "" || nodeID == string(rt.identity.NodeID) {
				return
			}
			if node.SelfRole() != swarm.RoleAnchor {
				return
			}
			if err := node.PublishObserverTombstone(topicFleetPeer, swarm.NodeID(nodeID)); err != nil {
				log.Printf("[SWARM] observer attestation for %s failed: %v", truncID(nodeID), err)
				return
			}
			log.Printf("[SWARM] LIVENESS-ATTEST node=%s (one witness; needs K=%d anchors to propagate)",
				truncID(nodeID), swarm.DefaultObserverQuorum)
		})
		log.Printf("[SWARM] InitSwarm: LAD liveness sweep wired -> observer attestation (anchor-only)")
	}

	// LAD reach bridge — translate every inbound swarm PeerRecord into a
	// lad.Record{Topic:TopicReach} so the LAD-fronted topology, members
	// counter, and gossip-liveness monitor see live data. The v0.0.290+
	// swarm cutover retired the gossip-driven LAD reach WRITE side but
	// kept the READ side (snap.Reach, cache.MemberCount); this bridge
	// closes that gap without re-enabling the legacy publisher.
	//
	// Uses DirectoryCache.ApplyLocal (ledger v0.0.13+) to bypass the
	// signedTopicACL — the swarm record's signature is in swarm's
	// canonical scheme (sig.go signableBytes), not lad's, so
	// lad.VerifyRecord cannot speak it. The swarm engine has already
	// verified the signature in plumtrees.go before delivery here, so
	// bypassing the lad ACL on this in-process path is safe.
	if rt.cache != nil {
		bridgeReachFromSwarm(rt, node, rt.cache)
		log.Printf("[SWARM] InitSwarm: swarm->LAD reach bridge wired")
	} else {
		log.Printf("[SWARM] InitSwarm: WARNING rt.cache nil — LAD reach bridge NOT wired (lad_members will stay 0)")
	}
	// All long-running calls below MUST bind to the process-lifetime
	// rt.Context(). The SessionHook closure can fire from any goroutine
	// for the lifetime of the runtime; the node.Start + publisher.run
	// goroutines run until rt.cancel. A caller-scoped ctx (e.g. a request
	// handler's) would silently break the hook and tear down the engines
	// the moment the caller returned — which is why InitSwarm does not
	// accept one. Was the H7 finding (closure ctx capture).
	rtCtx := rt.Context()

	if rt.connMgr != nil {
		log.Printf("[SWARM] InitSwarm: installing SessionHook on ConnectionManager")
		rt.connMgr.SetSessionHook(func(nodeID string, session aether.Session, joined bool) {
			peerID := swarm.NodeID(nodeID)
			log.Printf("[SWARM] SessionHook fired peer=%s joined=%v", truncID(nodeID), joined)
			if joined {
				if err := tport.RegisterPeer(rtCtx, peerID, session); err != nil {
					log.Printf("[SWARM] SessionHook: RegisterPeer FAILED peer=%s: %v", truncID(nodeID), err)
					return
				}
				log.Printf("[SWARM] SessionHook: RegisterPeer ok peer=%s", truncID(nodeID))
				// Event-driven convergence: kick a full-topic Merkle
				// anti-entropy probe at the new peer so they catch up
				// in one round-trip per topic instead of waiting for
				// the engine's periodic random pick. Pair it with a
				// fresh own-record publish so a peer that needs us
				// eagerly doesn't sit on a stale PeerRecord either.
				// Without this pairing, two fresh peers converge on
				// the random-probe cadence (default 60s) AND the
				// publisher's TTL refresh (5m) — minutes-of-quiet on
				// boot vs. seconds with the hook.
				node.ProbePeer(peerID)
				publisher.PublishNow()
				// Direct-path route advertisement. Without this, the route
				// engine subscribes to fleet.route advertisements but
				// never receives any — every peer sits in AddressTable +
				// RoleTable but route_paths stays empty and RPC forwarding
				// falls back to LAD-latency instead of role-based dispatch.
				// Withdraw mirrors on session leave.
				if ri := rt.route.Load(); ri != nil && ri.Router != nil {
					ri.Router.Advertise(route.NodeID(nodeID), nil, "")
				}
				log.Printf("[SWARM] SessionHook: triggered ProbePeer + PublishNow + RouteAdvertise peer=%s", truncID(nodeID))
				return
			}
			tport.UnregisterPeer(peerID)
			if ri := rt.route.Load(); ri != nil && ri.Router != nil {
				ri.Router.Withdraw(route.NodeID(nodeID), "")
			}
			log.Printf("[SWARM] SessionHook: UnregisterPeer + RouteWithdraw ok peer=%s", truncID(nodeID))
		})
	} else {
		log.Printf("[SWARM] InitSwarm: WARNING connMgr nil — SessionHook NOT installed")
	}

	// swarm.Node engine: register with rt.wg so Shutdown's
	// rt.wg.Wait() blocks until the engine drains. node.Start blocks
	// on rtCtx; cancelling rt.ctx unblocks it.
	rt.Go("swarm.node.start", func() {
		log.Printf("[SWARM] InitSwarm: starting swarm.Node engine goroutine")
		_ = node.Start(rtCtx)
		log.Printf("[SWARM] InitSwarm: swarm.Node engine exited")
	})

	// PeerPublisher run-loop is started by publisher.Start as its own
	// goroutine that selects on ctx.Done / stopCh. Wrap publisher.Start
	// in a wg-aware goroutine wrapper so the publisher's lifecycle is
	// also covered by Shutdown's wg.Wait — the run-loop will exit
	// either when rt.cancel fires or Publisher.Stop closes stopCh.
	rt.Go("swarm.peer_publisher.run", func() {
		log.Printf("[SWARM] InitSwarm: starting PeerPublisher run-loop")
		publisher.run(rtCtx)
		log.Printf("[SWARM] InitSwarm: PeerPublisher run-loop exited")
	})
	publisher.PublishNow()

	// RoleTable staleness sweep — the ONLY thing that can retire a ghost.
	//
	// A RoleTable entry has exactly one removal path: an inbound tombstone on
	// fleet.peer (RoleTable.onRecord). No TTL, no expiry. So an entry dies only
	// by the owner's graceful tombstone — never emitted when a deploy destroys
	// the machine — or by an observer tombstone, which until now was published
	// solely by sweepZombieSessions, a sweep over dead SESSIONS that is
	// structurally blind to a peer this process never held a session with.
	// Every machine replaced by a deploy landed in that gap and became
	// immortal: measured 40 entries against 11 real machines, with the LAD
	// directory (which HAS a TTL) sitting clean at 8 beside it.
	//
	// This sweep is the missing witness. It observes the corpses the session
	// sweep cannot see and attests to them; the K-of-N quorum turns K
	// independent anchor observations into the propagating tombstone that
	// RoleTable.onRecord already knows how to apply.
	//
	// Anchor-only because SetObserverRoleCheck drops any attestation from a
	// non-anchor — a non-anchor publishing here would be pure wire noise.
	rt.Go("swarm.role_table.liveness_sweep", func() {
		sweepStaleRolesUntil(rtCtx, rt, node, roleTable)
	})

	log.Printf("[SWARM] InitSwarm: integration ready")
	return rt.swarm, nil
}

// roleStaleThreshold is how long a peer's PeerRecord may go un-refreshed
// before it is pruned from a node's local RoleTable.
//
// PeerPublisher republishes every 5 minutes, so 16 minutes is THREE
// consecutive missed republishes — a peer that is merely slow, briefly
// partitioned, or mid-restart re-publishes long before it trips. It also
// matches ledger's GossipLivenessTimeout, so both directories age peers out on
// the same clock rather than racing to different verdicts.
//
// Erring long is deliberate and asymmetric: a late prune costs one stale row in
// a topology view for a few minutes, while an early one drops a live peer from
// the local view (self-healing on its next publish, but still churn). Only the
// first failure is acceptable, so the threshold is generous.
const roleStaleThreshold = 16 * time.Minute

// roleSweepInterval is the cadence of the staleness prune — a cheap map scan
// over a fleet-sized table, so a tight interval costs nothing and keeps the
// local view within one interval of the real fleet.
const roleSweepInterval = 60 * time.Second

// sweepStaleRolesUntil prunes stale peers from this node's local RoleTable until
// ctx is cancelled. The RoleTable is a local projection of gossiped
// PeerRecords, so a stale-entry delete propagates nothing and cannot suppress a
// live peer's re-announcement — a live peer's next PeerRecord simply re-adds it.
// That is why a plain local prune is correct here where a propagating liveness
// tombstone was not: there is no shared record to poison.
func sweepStaleRolesUntil(ctx context.Context, rt *Runtime, node swarm.Node, roleTable *RoleTable) {
	ticker := time.NewTicker(roleSweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Prune this node's own stale RoleTable entries. EVERY node, no role
			// gate. The table is a local projection of gossiped PeerRecords, so
			// deleting a stale entry is always safe — it propagates nothing, and
			// a live peer's next PeerRecord (published every 5 min) re-adds it.
			// This converges machines[] to the real fleet independent of
			// topology, anchor count, or cross-node quorum: the property the
			// observer-tombstone path could not guarantee because its
			// synthesised tombstone is consumer-local and its attestations do
			// not reliably flood a partial mesh.
			if pruned := roleTable.PruneStale(time.Now().Add(-roleStaleThreshold)); pruned > 0 {
				log.Printf("[SWARM] ROLE-PRUNE removed %d stale peers from local view", pruned)
			}
		}
	}
}

// Swarm returns the current SwarmIntegration (or nil if InitSwarm has
// not yet been called).
func (rt *Runtime) Swarm() *SwarmIntegration {
	return rt.swarm
}

// LookupRoleViaSwarm is the migration helper for mesh_session_finder and
// other callers that still use `rt.cache.Roles(ctx, "", query)`. It
// returns a trimmed view (NodeID + MaxGrade) sourced from the swarm
// RoleTable, or nil if InitSwarm has not yet been called — callers should
// fall through to the legacy `rt.cache.Roles` path in that case.
func (rt *Runtime) LookupRoleViaSwarm(role, handler string) []lookupRoleResult {
	if rt.swarm == nil || rt.swarm.RoleTable == nil {
		return nil
	}
	records := rt.swarm.RoleTable.Lookup(role, handler)
	out := make([]lookupRoleResult, len(records))
	for i, r := range records {
		out[i] = lookupRoleResult{
			NodeID:   r.NodeID,
			MaxGrade: r.MaxGrade,
		}
	}
	return out
}

// lookupRoleResult is the trimmed view returned by LookupRoleViaSwarm.
// It contains exactly the fields used by callers (NodeID + MaxGrade) to
// avoid leaking lad-package types into new code paths.
type lookupRoleResult struct {
	NodeID   string
	MaxGrade int
}

// AdvertiseLocalAddress is called by the transport listener wiring once
// per local listener to inform PeerPublisher of a dialable address.
// Accumulates into the publisher's address set (de-duplicating by the
// {transport, host, port} tuple) so multiple listeners — noise-UDP,
// /mesh/ws WebSocket, HTTPS — all reach peers in a single PeerRecord.
//
// scope is "public" or "private" — peers use it to pick same-fabric
// private addresses over public ones when origin prefixes match. Empty
// scope is treated as "public" by receivers.
//
// SetAddresses on the publisher REPLACES the slice, so we read the
// current set, append+dedup, and write it back. Each call publishes
// immediately via PeerPublisher.PublishNow; rapid back-to-back
// listener wiring coalesces into a single broadcast.
func (s *SwarmIntegration) AdvertiseLocalAddress(transport swarmpb.Address_Transport, host string, port uint32, sni, scope string) {
	if s == nil || s.Publisher == nil {
		return
	}
	if host == "" || port == 0 || transport == swarmpb.Address_UNKNOWN {
		log.Printf("[SWARM] AdvertiseLocalAddress: skip transport=%v host=%q port=%d (incomplete)", transport, host, port)
		return
	}
	existing := s.Publisher.Addresses()
	for _, a := range existing {
		if a == nil {
			continue
		}
		if a.Transport == transport && a.Host == host && a.Port == port {
			// Same address already advertised — nothing to do.
			return
		}
	}
	merged := make([]*swarmpb.Address, 0, len(existing)+1)
	merged = append(merged, existing...)
	merged = append(merged, &swarmpb.Address{
		Transport: transport,
		Host:      host,
		Port:      port,
		Sni:       sni,
		Scope:     scope,
	})
	s.Publisher.SetAddresses(merged)
	log.Printf("[SWARM] AdvertiseLocalAddress: transport=%v host=%s port=%d sni=%q scope=%q (total=%d)",
		transport, host, port, sni, scope, len(merged))
}

// advertiseSwarmListeners populates the swarm PeerRecord's Addresses
// field with the listener endpoints this node accepts on. Without this
// every published PeerRecord carries addrs=0 and peers can only reach
// us via the same gossip channel they're already on — there is no
// Grade-A noise-UDP upgrade path because we never told them where the
// UDP listener is.
//
// Advertised on every PublishRPCHandlersToLAD call so the address set
// always reflects current config; the publisher dedups by
// {transport, host, port} so calling repeatedly is safe.
//
// What gets advertised:
//   - noise-UDP @ cfg.VL1.UDPPort on cfg.PublicDomain (scope=public) —
//     the Grade-A target for cross-fabric / cross-org peers
//   - noise-UDP @ cfg.VL1.UDPPort on the platform private IP
//     (scope=private) when the platform exposes one — same-org peers
//     prefer this entry, skipping the public LB and routing direct
//     over the private fabric (Fly 6PN, GCP VPC, etc.). Uses the SAME
//     listener socket as the public entry — single UDP port, two
//     advertised endpoints.
//   - WebSocket @ 443 on cfg.PublicDomain — the WSS endpoint Fly's
//     edge serves at /mesh/ws (peers that can't reach UDP)
//   - HTTP @ 443 on cfg.PublicDomain — the /mesh/vl1 bootstrap entry
//     so a brand-new peer that doesn't yet hold our record can still
//     reach us via the public hostname
func (rt *Runtime) advertiseSwarmListeners(si *SwarmIntegration) {
	if si == nil || si.Publisher == nil {
		return
	}
	publicHost := rt.cfg.PublicDomain
	if publicHost == "" && rt.publicIP != "" {
		// PublicDomain missing — fall back to the STUN-discovered IP.
		// Edge-served listeners (WSS/HTTPS) still need a DNS name to
		// pass TLS validation, so they stay un-advertised in this
		// degraded mode; only the noise-UDP listener is dialable by IP.
		publicHost = rt.publicIP
	}
	if publicHost == "" {
		log.Printf("[SWARM] advertiseSwarmListeners: no public hostname/IP available — skipping (will retry on next publish)")
		return
	}

	udpPort := rt.cfg.VL1.UDPPort
	if udpPort > 0 {
		si.AdvertiseLocalAddress(swarmpb.Address_NOISE_UDP, publicHost, uint32(udpPort), "", "public")
		// Same-fabric private endpoint: same UDP port, private IP. On
		// Fly this is the 6PN ULA (fdaa:<org>:<net>:…). Receivers that
		// see Scope:"private" and an origin-matching prefix dial the
		// private host instead of the public one — direct noise-UDP
		// over the private fabric, bypassing the anycast LB.
		if rt.cfg.Platform != nil {
			if privIP := rt.cfg.Platform.PrivateIP(); privIP != "" {
				si.AdvertiseLocalAddress(swarmpb.Address_NOISE_UDP, privIP, uint32(udpPort), "", "private")
			}
		}
		// Multi-NIC enumeration: the platform-reported PrivateIP is one
		// address; a host with multiple NICs (dual VPNs, multiple 6PN
		// families, private LAN plus Fly 6PN, etc.) has more dial
		// targets that same-org peers may need. AdvertiseLocalAddress
		// dedups by {transport, host, port} so the calls here are
		// safe to interleave with the explicit public/private entries
		// above. Scope is inferred per-address via scopeForHost.
		rt.advertiseLocalInterfaces(si, udpPort)
	}

	// WSS + HTTPS endpoints ride the Fly edge at 443 with the public
	// domain as SNI. Same {host, port} for both — different transport
	// enum so dialers can pick the right path. Skip when we only have
	// an IP (no DNS name → TLS validation would fail).
	if rt.cfg.PublicDomain != "" {
		si.AdvertiseLocalAddress(swarmpb.Address_WEBSOCKET, rt.cfg.PublicDomain, 443, rt.cfg.PublicDomain, "public")
		si.AdvertiseLocalAddress(swarmpb.Address_HTTP, rt.cfg.PublicDomain, 443, rt.cfg.PublicDomain, "public")
	}
}

// installSwarmGradeUpgradeHook subscribes the swarm publisher to
// ConnectionScaler events so a local grade promotion (e.g. peer
// upgraded from WS → noise-UDP) re-publishes our PeerRecord with the
// new MaxGrade immediately. Without this, we'd advertise stale grades
// for up to the publisher's 5-minute TTL refresh — peers can't tell
// when we become reachable at a better grade.
//
// MaxGrade reflects the BEST grade across all our active peer
// connections (BestActiveGrade), not just the one that just
// transitioned. Idempotent: scaler.AddEventSubscriber appends, so
// callers must guard against double-install. We track installation
// on Runtime so PublishRPCHandlersToLAD can be invoked repeatedly
// (e.g. on role changes) without piling subscribers.
func (rt *Runtime) installSwarmGradeUpgradeHook(si *SwarmIntegration) {
	if si == nil || si.Publisher == nil || rt.connMgr == nil || rt.connMgr.scaler == nil {
		return
	}
	rt.swarmGradeHookMu.Lock()
	if rt.swarmGradeHookInstalled {
		rt.swarmGradeHookMu.Unlock()
		return
	}
	rt.swarmGradeHookInstalled = true
	rt.swarmGradeHookMu.Unlock()

	publisher := si.Publisher
	connMgr := rt.connMgr
	var lastGrade Grade = GradeF
	var lastMu sync.Mutex
	rt.connMgr.scaler.AddEventSubscriber(func(_ ConnectionEvent) {
		best := connMgr.BestActiveGrade()
		lastMu.Lock()
		changed := best != lastGrade
		if changed {
			lastGrade = best
		}
		lastMu.Unlock()
		if !changed {
			return
		}
		publisher.SetMaxGrade(swarmGradeFromInternal(best))
		log.Printf("[SWARM] grade-upgrade hook: MaxGrade=%v republished", best)
	})
	log.Printf("[SWARM] installSwarmGradeUpgradeHook: subscribed to ConnectionScaler events")
}
