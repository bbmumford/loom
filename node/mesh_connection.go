/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/adapter"
	aethercrypto "github.com/ORBTR/aether/crypto"
	"github.com/ORBTR/aether/multipath"
	"github.com/ORBTR/aether/quality"
	"github.com/ORBTR/aether/resume"
	lad "github.com/bbmumford/ledger"
	"github.com/bbmumford/loom/core/directory/gossip"
	"github.com/bbmumford/loom/core/streams"
)

// AcceptMeshConnectionOpts holds parameters for accepting an Aether connection.
type AcceptMeshConnectionOpts struct {
	Session       aether.Session
	NodeID        string
	Region        string
	Proto         Protocol
	IsInitiator   bool
	BootstrapHost string
	// ServiceName identifies the peer's service (e.g., "node.hstles.com").
	// When present, a lad.MemberRecord is published for the peer as soon
	// as the session is accepted so topology views resolve the peer
	// immediately rather than waiting for its own gossip to propagate.
	// Empty is allowed — falls back to gossip propagation.
	ServiceName string
	// Roles is the peer's comma-separated role list (e.g., "anchor,bootstrap").
	// Included in the pre-published Member record when non-empty.
	Roles string
}

// AcceptMeshConnection takes full ownership of an Aether session's lifecycle:
// opens well-known streams (gossip, RPC, keepalive, control), runs gossip,
// serves incoming RPCs, and manages keepalive. Blocks until the session closes.
//
// All connections use Aether natively. Owns the full session lifecycle:
// stream setup, gossip, RPC, keepalive, control, cleanup.
func (m *ConnectionManager) AcceptMeshConnection(ctx context.Context, opts AcceptMeshConnectionOpts) error {
	nodeID := aether.NormalizeNodeID(opts.NodeID)

	dbgSessionHealth.Printf("accepting Aether connection nodeID=%s proto=%s initiator=%v", truncID(nodeID), opts.Proto, opts.IsInitiator)
	log.Printf("[AETHER] AcceptMeshConnection: nodeID=%s, proto=%s, initiator=%v",
		truncID(nodeID), opts.Proto, opts.IsInitiator)

	// Create a cancellable context for this connection's lifetime
	connCtx, connCancel := context.WithCancel(ctx)
	defer connCancel()

	// Register peer + transport
	tc := &transportConn{
		protocol:      opts.Proto,
		grade:         GradeForProtocol(opts.Proto),
		isMuxed:       true, // Aether sessions are always muxed
		cancelFunc:    connCancel,
		connectedAt:   time.Now(),
		bootstrapHost: opts.BootstrapHost,
	}

	// Compute cross-origin BEFORE taking m.mu — the LAD reach lookup
	// inside computeCrossOriginForNode can stall under cache
	// contention, and holding m.mu across that I/O would serialize
	// the entire connection table. Audit critical fix #4.
	preComputedCrossOrigin := m.computeCrossOriginForNode(nodeID)

	lifecycleMu := m.sessionLifecycleLock(nodeID)
	lifecycleMu.Lock()
	m.mu.Lock()
	peer, peerExisted := m.peers[nodeID]
	// previousProtocol must be snapshotted BEFORE either branch
	// below overwrites peer.protocol — including the new-peer-creation
	// case where the literal "no previous proto" value is the zero
	// Protocol. Without this, the reject rollback path would restore
	// a freshly-created peer to opts.Proto (the rejected proto), which
	// is a no-op. The audit (H5) caught the corner case where a
	// same-grade rejection against an unknown peer leaks a stub
	// peer entry with state=PeerConnected, connCount=0 (after the
	// rollback), protocol=<rejected>. We now (a) snapshot before
	// create, and (b) delete the freshly-created peer entirely on
	// reject so no stub remains.
	var previousProtocol Protocol
	if peerExisted {
		previousProtocol = peer.protocol
	}
	if !peerExisted {
		peer = &peerConn{
			nodeID:     nodeID,
			state:      PeerConnected,
			protocol:   opts.Proto,
			isMuxed:    true,
			peerRegion: opts.Region,
			// Use the precomputed result. If an entry was created
			// between the Lock in computeCrossOriginForNode and
			// this Lock (PEX populated the peer), the stored
			// addresses would yield the same answer — the LAD
			// fallback was just a tiebreaker for the brand-new
			// case, which still holds.
			crossOrigin:  preComputedCrossOrigin,
			crossRegion:  m.isCrossRegion(opts.Region),
			discoveredAt: time.Now(),
		}
		m.peers[nodeID] = peer
	}
	peer.addTransport(tc)
	peer.state = PeerConnected
	// (previousProtocol was snapshotted above, before peer creation
	// branch, so the reject rollback can restore the actual prior
	// value — or signal "fresh peer, delete on reject" via
	// !peerExisted.)
	peer.protocol = opts.Proto
	// Existing peer entries created by LAD discovery / PEX / runtime bootstrap
	// default to isMuxed=false. Without this, ConnectionReporter labels every
	// session on a pre-discovered peer as "gossip" instead of "gossip+dispatch"
	// — purely cosmetic, but it misleads topology dashboards into thinking
	// dispatch isn't wired up.
	peer.isMuxed = true
	now := time.Now()
	peer.lastConnected = now
	peer.lastConnectedAt = now
	peer.connCount++
	// Promote grade on every accept, whether we're the incoming side or
	// the initiator re-registering a session. Without this, bestEverGrade
	// stays at GradeF for any peer whose first session was established
	// by the remote side — the only other writer runs in the
	// connectToPeer "existing session" branch and never fires for an
	// accept. oldGrade is the best grade across other already-active
	// transports for this peer, so the emitted ConnectionEvent reports
	// the actual transport upgrade (e.g. "C→A") rather than always
	// GradeF for the upgrade-on-an-already-connected-peer case.
	oldGrade := peer.bestActiveGrade()
	peer.promoteGrade(tc.grade, now)
	m.mu.Unlock()

	// Honour Acquire's result. When over MaxTotal (a race past the
	// scanAndConnect CanConnect gate) Acquire returns false WITHOUT incrementing
	// currentTotal, while every teardown path still calls Release() — so
	// ignoring the return decrements a slot that was never acquired, and
	// currentTotal drifted below the true count until MaxTotal stopped being
	// enforced. Keep the established session but gate every Release on this flag.
	budgetAcquired := m.budget.Acquire()
	if !budgetAcquired {
		log.Printf("[AETHER] %s: connection budget over MaxTotal at register (currentTotal=%d) — not reserving a slot", truncID(nodeID), m.budget.CurrentTotal())
	}
	log.Printf("[AETHER] %s: registered %s connection (aether, region=%s)", truncID(nodeID), opts.Proto, opts.Region)

	// Register Aether session for dispatch lookup FIRST, before any other
	// side-effect work (ledger publish, scaler EmitEvent, ConnProvider type
	// assertion). The previous order had registerMeshSession at the very end
	// of this prologue, after EmitEvent's synchronous fanout to subscribers
	// (publishPeerStateEvent → reporter.ConnectionTo + ledger.Append using
	// context.Background — no timeout). When ledger contention or any
	// subscriber blocked, AcceptMeshConnection hung between line 110 (peer
	// state already committed via PeerConnected) and registerMeshSession,
	// leaving the system inconsistent: peer.state == PeerConnected but
	// meshSessions == nil. Topology showed the peer as connected but
	// dispatch RPCs returned "no Aether session to <peer>".
	//
	// Grade-aware rejection (lower-grade session vs existing) is now an
	// EARLY return — peer.state was already set to PeerConnected above,
	// which is consistent with the existing behaviour even when register
	// rejects (the connection was accepted at the transport layer).
	//
	// MUST roll back the per-accept counters set above (peer.connCount++,
	// peer.addTransport, m.budget.Acquire) because the cleanup label
	// won't run on this early-return path. Without rollback, every
	// same-grade rejection leaks one connCount unit + one budget slot
	// + one transport-map entry per peer per reject. Under the
	// v0.0.228-era WS flap this drove peer.connCount to 40+ for stable
	// peers, which then made the scaler perpetually try to "scale down"
	// the phantom connections (drain decrement * 1s tick, never
	// catching up to the leak rate).
	if !m.registerMeshSession(ctx, nodeID, opts.Session, opts.IsInitiator) {
		m.mu.Lock()
		if peer.connCount > 0 {
			peer.connCount--
		}
		peer.removeTransport(opts.Proto, tc)
		if peerExisted {
			// Restore peer.protocol so downstream code (scaler,
			// drain guards, topology display) sees the actual
			// active protocol — not the rejected one.
			peer.protocol = previousProtocol
		} else {
			// Audit H5: the peer was created in THIS call for a
			// session that just got rejected. There are no other
			// active transports on it (no addTransport ran except
			// the one we just removed) and no consumer has seen
			// the entry yet (registerMeshSession failed before
			// emitting events). Delete the stub instead of
			// leaving a connCount=0 / protocol=<rejected> ghost
			// that the scaler / topology view would treat as a
			// connected peer.
			if peer.connCount == 0 && len(peer.transports) == 0 {
				delete(m.peers, nodeID)
			}
		}
		m.mu.Unlock()
		if budgetAcquired { // MESH-C07: only release a slot we actually acquired
			m.budget.Release()
		}
		lifecycleMu.Unlock()
		return nil // session rejected — equal or lower grade than existing
	}

	// Register with multipath manager for failover tracking
	m.addMultipathSession(nodeID, opts.Session, opts.Proto)
	lifecycleMu.Unlock()

	// Publish a Member record for the accepted peer so its service name +
	// region reach our local LAD immediately. Without this, peers accepted
	// via the aether path (noise-UDP, QUIC, WebSocket upgrade) show up as
	// "(unresolved)" in topology views until their own gossip propagates
	// a self-Member record — which can take many seconds under convergence
	// stress. The WS-bootstrap HTTP accept path does an equivalent publish
	// inline; this mirrors that behaviour for every other transport.
	if opts.ServiceName != "" && m.rt.ledger != nil {
		attrs := map[string]string{"serviceName": opts.ServiceName}
		if opts.Region != "" {
			attrs["region"] = opts.Region
		}
		if opts.Roles != "" {
			attrs["roles"] = opts.Roles
		}
		if body, err := json.Marshal(lad.MemberRecord{
			NodeID:    nodeID,
			CreatedAt: time.Now().UTC(),
			Attrs:     attrs,
		}); err == nil {
			_ = m.rt.ledger.Append(ctx, lad.Record{
				Topic:        lad.TopicMember,
				NodeID:       nodeID,
				Body:         json.RawMessage(body),
				Timestamp:    time.Now().UTC(),
				LamportClock: m.rt.lamportClock.Tick(),
			})
		}
	}

	// Emit connection event with the correct oldGrade — GradeF still
	// applies to first-ever connects, but transport upgrades (Grade C
	// → A on the same peer) now reflect reality.
	if m.scaler != nil {
		m.scaler.EmitEvent(ConnectionEvent{
			PeerNodeID: nodeID,
			OldGrade:   oldGrade,
			NewGrade:   tc.grade,
			Transport:  opts.Proto.String(),
			Reason:     "connected",
			Timestamp:  now,
		})
	}

	// Force a fresh reach publish on every successful accept so the new
	// peer (and every other peer reachable via gossip from this point)
	// sees our current address set, service identity, and metadata
	// without waiting for the publisher's next adaptive tick — STABLE
	// mode is on a 5-minute cadence and would leave a fresh peer
	// without our reach record for up to that whole window. This is
	// the "we're online with new peer X" announcement and pairs with
	// refreshStaleReachRecords on the receiving side: the sender
	// always emits a fresh snapshot post-handshake; the receiver
	// actively pulls anything missing from its own cache. Together
	// they close the announce-driven feedback loop's failure modes
	// (dropped FreshnessAnnounces, digest collisions, cache-stub
	// false-matches) with a deterministic post-connect handshake.
	if m.rt != nil && m.rt.swarm != nil && m.rt.swarm.Publisher != nil {
		m.rt.swarm.Publisher.PublishNow()
	}

	// Tell the LAD cache that this peer is authoritatively alive
	// while we hold the session. EvictExpired checks this override
	// before liveness-tombstoning member/reach records and skips any
	// peer with override-true, eliminating the (unresolved) drift
	// pattern where connected peers had their reach records evicted
	// because the cached lastGossipAt clock disagreed with the
	// connection-manager truth. Cleared in the cleanup path below
	// when the session closes (allDead branch).
	if m.rt != nil && m.rt.cache != nil {
		m.rt.cache.SetGossipLivenessOverride(nodeID, true)
	}

	// Capture remote address for path scoring (before any gotos).
	// Type-assert to ConnProvider to access the underlying net.Conn.
	var remoteAddr string
	if cp, ok := opts.Session.(aether.ConnProvider); ok {
		if nc := cp.NetConn(); nc != nil && nc.RemoteAddr() != nil {
			remoteAddr = nc.RemoteAddr().String()
		}
	}

	// Derive per-session encryption keys from session key (if available)
	if sessionKey := opts.Session.SessionKey(); len(sessionKey) >= 32 {
		kr, err := aethercrypto.DeriveKeyring(sessionKey, string(m.selfID), nodeID)
		if err == nil {
			// Wire encryption into Noise adapter (if it's a Noise session)
			if noiseSession, ok := opts.Session.(*adapter.NoiseSession); ok {
				noiseSession.SetSessionKey(kr.EncryptionKey)
				dbgSessionHealth.Printf("%s: per-frame encryption enabled via keyring", truncID(nodeID))
			}
		}
	}

	// 0-RTT resume token will be sent on the control stream after it's opened (below)
	var resumePayload []byte
	if opts.IsInitiator && m.resumeStore != nil {
		if token, sessionKey, loadErr := m.resumeStore.Load(nodeID); loadErr == nil {
			if valErr := token.Validate(sessionKey); valErr == nil {
				resumePayload = aether.EncodeHandshake(aether.HandshakePayload{
					HandshakeType: aether.HandshakeSessionResume,
					Payload:       token.Encode(),
				})
			}
		}
	}

	// Open all 5 well-known streams (initiator opens, responder accepts)
	var gossipStream, rpcStream, keepaliveStream, controlStream, reconcileStream aether.Stream
	var err error

	if opts.IsInitiator {
		// Initiator: open streams in order.
		//
		// InitialCredit is set per stream class so each stream gets a
		// window sized for its traffic profile. Gossip needs a big
		// window to absorb startup full-sync storms; RPC/keepalive/control
		// carry small messages and don't need the default 1 MB. Smaller
		// windows for the chatty-but-tiny streams cut per-peer memory
		// footprint without reducing throughput on the streams that matter.
		gossipStream, err = opts.Session.OpenStream(connCtx, aether.StreamConfig{
			StreamID: streams.StreamGossip, Reliability: aether.BestEffort, Priority: 64,
			// 1 MB initial, 2 MB ceiling. The receiver's grant debouncer
			// (5 ms coalesce, 10% immediate floor) emits grants long
			// before the sender can exhaust the window on a convergence
			// burst. MaxCredit caps auto-tuner growth so per-peer memory
			// stays bounded — at 2 MB × ~20 peers that's 40 MB of receive
			// buffers per node instead of the 80-160 MB the earlier
			// 4 MB/8 MB setting produced.
			InitialCredit: 1 * 1024 * 1024,
			MaxCredit:     2 * 1024 * 1024,
		})
		if err != nil {
			log.Printf("[AETHER] %s: failed to open gossip stream: %v", truncID(nodeID), err)
			goto cleanup
		}
		rpcStream, err = opts.Session.OpenStream(connCtx, aether.StreamConfig{
			StreamID: streams.StreamRPC, Reliability: aether.ReliableOrdered, Priority: 128,
			InitialCredit: 256 * 1024, // 256 KB — RPC payloads are typically <10 KB but can batch
		})
		if err != nil {
			log.Printf("[AETHER] %s: failed to open RPC stream: %v", truncID(nodeID), err)
			goto cleanup
		}
		keepaliveStream, err = opts.Session.OpenStream(connCtx, aether.StreamConfig{
			StreamID: streams.StreamKeepalive, Reliability: aether.UnreliableSequenced, Priority: 255,
			LatencyClass: aether.ClassREALTIME, FECLevel: 1, // FECBasicXOR — recovers 1 dropped keepalive per group
			InitialCredit: 16 * 1024, // 16 KB — pings are <50 bytes
		})
		if err != nil {
			log.Printf("[AETHER] %s: failed to open keepalive stream: %v", truncID(nodeID), err)
			goto cleanup
		}
		controlStream, err = opts.Session.OpenStream(connCtx, aether.StreamConfig{
			StreamID: streams.StreamControl, Reliability: aether.ReliableOrdered, Priority: 240,
			LatencyClass:  aether.ClassREALTIME,
			InitialCredit: 64 * 1024, // 64 KB — control frames are small but can batch
		})
		if err != nil {
			log.Printf("[AETHER] %s: failed to open control stream: %v", truncID(nodeID), err)
			goto cleanup
		}
		// Reconcile stream — carries G4 IBLT cells + G5 snapshot frames.
		// Same priority class as gossip (lower than RPC/keepalive/control)
		// because reconcile is bulk-ish convergence traffic. Initial
		// credit smaller than gossip's because typical G4 rounds are
		// O(|Δ|) which is far smaller than gossip's full-sync storms;
		// a 256 KB starting window covers >99 % of observed rounds and
		// the auto-tuner can grow if needed.
		reconcileStream, err = opts.Session.OpenStream(connCtx, aether.StreamConfig{
			StreamID: streams.StreamReconcile, Reliability: aether.BestEffort, Priority: 64,
			InitialCredit: 256 * 1024,
			MaxCredit:     1 * 1024 * 1024,
		})
		if err != nil {
			log.Printf("[AETHER] %s: failed to open reconcile stream: %v", truncID(nodeID), err)
			goto cleanup
		}
	} else {
		// Responder: accept each well-known stream by its explicit StreamID.
		// AcceptStreamByID blocks until a stream with EXACTLY the requested
		// ID arrives, regardless of the order frames hit the wire. The prior
		// FIFO AcceptStream was wire-ordering-dependent: when an application
		// stream (e.g. swarm stream 100) opened before the responder reached
		// the gossip accept, the FIFO returned the wrong stream first and the
		// gossip-responder loop ended up bound to the swarm stream, killing
		// the session within milliseconds. AcceptStreamByID closes that race
		// on the responder side; the initiator side keeps using OpenStream
		// (which is already StreamID-explicit) above.
		gossipStream, err = opts.Session.AcceptStreamByID(connCtx, streams.StreamGossip)
		if err != nil {
			log.Printf("[AETHER] %s: failed to accept gossip stream: %v", truncID(nodeID), err)
			goto cleanup
		}
		rpcStream, err = opts.Session.AcceptStreamByID(connCtx, streams.StreamRPC)
		if err != nil {
			log.Printf("[AETHER] %s: failed to accept RPC stream: %v", truncID(nodeID), err)
			goto cleanup
		}
		keepaliveStream, err = opts.Session.AcceptStreamByID(connCtx, streams.StreamKeepalive)
		if err != nil {
			log.Printf("[AETHER] %s: failed to accept keepalive stream: %v", truncID(nodeID), err)
			goto cleanup
		}
		controlStream, err = opts.Session.AcceptStreamByID(connCtx, streams.StreamControl)
		if err != nil {
			log.Printf("[AETHER] %s: failed to accept control stream: %v", truncID(nodeID), err)
			goto cleanup
		}
		reconcileStream, err = opts.Session.AcceptStreamByID(connCtx, streams.StreamReconcile)
		if err != nil {
			log.Printf("[AETHER] %s: failed to accept reconcile stream: %v", truncID(nodeID), err)
			goto cleanup
		}
	}

	dbgSessionHealth.Printf("%s: all 5 streams open gossip=%d rpc=%d keepalive=%d control=%d reconcile=%d",
		truncID(nodeID), gossipStream.StreamID(), rpcStream.StreamID(),
		keepaliveStream.StreamID(), controlStream.StreamID(), reconcileStream.StreamID())
	log.Printf("[AETHER] %s: streams open (gossip=%d, rpc=%d, keepalive=%d, control=%d, reconcile=%d)", truncID(nodeID),
		gossipStream.StreamID(), rpcStream.StreamID(),
		keepaliveStream.StreamID(), controlStream.StreamID(), reconcileStream.StreamID())

	// Send 0-RTT resume token on the now-open control stream
	if len(resumePayload) > 0 {
		if err := controlStream.Send(connCtx, resumePayload); err != nil {
			// Surface the failure, not only the success. A failed resume-token
			// push is best-effort, but logging only the success hides a real
			// control-stream send fault.
			log.Printf("[AETHER] %s: failed to send 0-RTT resume token on control stream: %v", truncID(nodeID), err)
		} else {
			log.Printf("[AETHER] %s: sent 0-RTT resume token on control stream", truncID(nodeID))
		}
	}

	// Record successful path for path scoring
	m.RecordPathSuccess(nodeID, opts.Proto, remoteAddr, opts.Session)

	// Fan out the session-installed event to SetSessionHook consumers
	// (swarm transport RegisterPeer + future hookers). Must fire AFTER
	// the five well-known streams (gossip, RPC, keepalive, control,
	// reconcile) have completed OPEN/ACCEPT on both sides — the swarm
	// transport opens application-stream 100 here, and doing that
	// before stream 0 has been accepted would steer the responder's
	// FIFO AcceptStream into delivering stream 100 in gossip's slot,
	// killing the session within milliseconds of register. Symmetric
	// on the initiator side: by this point all 5 streams have written
	// their OPEN frames, so an inbound OPEN(100) from the peer hits a
	// session whose foreground is already past its accept block.
	m.fireSessionHook(nodeID, opts.Session, true)

	// Start services on the streams
	{
		// Bidirectional RPC on Stream 1 — both sides can initiate requests.
		// Replaces the one-directional ServeMeshStream. The forwarder uses
		// bidiRPC.Call() instead of opening dynamic streams.
		//
		// Snapshot (transport, scope) for OBS-7 bidirpc_call_latency_ms
		// tagging. opts.Proto.String() emits the Library's transport label
		// ("noise-udp" / "websocket" / "grpc" / "tls"); normalise WS down
		// to "ws" so the metric label set matches the spec's
		// (noise-udp / ws / tcp) vocabulary. Scope reads peer.crossOrigin
		// (set during the peer-table prologue above under m.mu) without
		// re-locking — bool reads are atomic in practice and a stale read
		// only affects this bidi's tag for the brief reclassification
		// window, which the histogram absorbs.
		bidiTransport := opts.Proto.String()
		switch bidiTransport {
		case "websocket":
			bidiTransport = "ws"
		case "tls":
			// TLS bootstrap rides the WebSocket aether adapter — group
			// it with ws so the histogram doesn't fragment over the
			// fallback transport.
			bidiTransport = "ws"
		}
		// Scope label tracks both cross-origin (different Fly org) and
		// cross-region (same Fly org but different region). Pre-v0.0.405
		// the label was just "same-origin" / "cross-org", which lumped
		// same-region same-org (~1ms RTT) and cross-region same-org
		// (~200ms RTT) into one bucket, making the p99 unreadable for
		// real local latency. The 3-way split below makes the histogram
		// honest. See workflow wd4zasivv synthesis.
		bidiScope := "same-org-same-region"
		switch {
		case peer != nil && peer.crossOrigin:
			bidiScope = "cross-org"
		case peer != nil && peer.peerRegion != "" && peer.peerRegion != m.rt.cfg.Platform.Region():
			bidiScope = "same-org-cross-region"
		}
		bidiRPC := NewBidiRPC(rpcStream, aether.NodeID(nodeID), m.rt.rpcServer, bidiTransport, bidiScope)
		m.registerBidiRPC(opts.Session, bidiRPC)

		// Dynamic stream handler — accepts streams opened by the remote's AetherCaller.
		// Each dynamic RPC stream (ID 10+) carries one request-response pair.
		go func() {
			for {
				stream, err := opts.Session.AcceptStream(connCtx)
				if err != nil {
					dbgAether.Printf("%s: dynamic stream handler exit: %v", truncID(nodeID), err)
					return
				}
				dbgAether.Printf("%s: accepted dynamic stream %d", truncID(nodeID), stream.StreamID())
				go m.rt.rpcServer.ServeMeshStream(connCtx, stream, aether.NodeID(nodeID))
			}
		}()

		// Keepalive on stream 2 — grade-aware interval with retry before killing.
		// Grade A (Noise): 30s interval — reliable transport, infrequent pings.
		// Grade B (QUIC/gRPC): 20s interval — moderate.
		// Grade C (WebSocket): 10s interval — frequent to beat proxy idle timeouts.
		// Retries 3 times at KeepaliveTimeout intervals before declaring dead.
		//
		// Keepalive cadence is driven by the session's own measured
		// SRTT (adaptiveKeepaliveInterval) rather than a static
		// per-grade table. A 10 s GradeC tick — chosen for fly's
		// edge-proxy idle close — was over-aggressive for inter-
		// machine WS sessions that bypass the proxy and produced a
		// chronic 10 s flap on cross-region pairs. clamp(2 × SRTT,
		// 5 s, 30 s) lets a 250 ms cross-region path keep the same
		// 5 s probe cadence the proxy actually needs while letting
		// a stable 1 s RTT path relax to 2 s and a satellite-class
		// 15 s path probe at 30 s.
		go func() {
			// Per-ping timeout derives from RTO (RFC 6298) so paths with
			// high RTT but low jitter get tight budgets, bursty ones get
			// looser. Death-budget is now path-aware: when the per-peer
			// multipath manager has a live standby ready to take over
			// (PathCount>=2), 3 strikes still closes — failover is
			// instant via the cleanup path's OnPrimaryFailure call so
			// dispatch hops to the standby without the user-visible
			// reconnect. When there's no standby (PathCount==1), the
			// budget extends to 5 strikes so a healthy path under a
			// transient CPU-steal or jitter burst doesn't get torn
			// down with no alternative — a full reconnect costs an
			// auth roundtrip and a handshake, an extra ~30 s of
			// patience is worth more than the latency cost.
			grade := SessionGrade(opts.Session)
			// Capture install time so the tighter has-standby retry
			// budget only applies once the path has had a chance to
			// prove itself. A freshly-installed noise-UDP path on a
			// peer where the old WS session is still in its proving
			// window momentarily reads PathCount==2; without a grace
			// window, the first three ping timeouts (~6 s of attempts
			// after the first ping at T+5 s) would close the brand-new
			// path before Fly's anycast routing or the peer's noise
			// listener has settled. The grace lets the path live with
			// the more lenient no-standby budget for its initial
			// stability window; after the grace the standard
			// PathCount>=2 budget kicks in.
			sessionInstalledAt := time.Now()
			const (
				retriesWithStandby = 3
				retriesNoStandby   = 5
				// newPathGraceWindow defers application of the tighter
				// retriesWithStandby budget for newly-installed paths,
				// long enough to absorb Fly anycast convergence + peer
				// listener catch-up while short enough that a genuinely
				// dead path is still reaped within ~ten of seconds.
				//
				// Sized to MATCH the per-peer proving timer (60 s — the
				// AfterFunc that closes the old session once the new one
				// has been registered). For the entire proving window
				// PathCount() returns 2 (new + proving-old), but the
				// "standby" is the OUTGOING old session — closing the
				// new path on a tightened 3-retry budget during that
				// window throws away the very upgrade we are trying to
				// land. The 34 s noise-UDP death cluster observed in
				// connection_history was an exact arithmetic match:
				// 30 s grace expiry + 3 retries × 1 s fast-retry gap
				// + 1 s last-retry wait = 34 s. Matching the grace to
				// the proving window eliminates the false-standby
				// budget tightening; a genuinely dead path is still
				// reaped within retriesNoStandby × pingTimeout once
				// proving completes and PathCount drops back to 1.
				newPathGraceWindow = 60 * time.Second
			)
			// fastRetryGap is the inter-retry gap on the FAST path
			// (after a ping has failed). Previously the loop fell back
			// to the full interval (5–30 s) between retries, so a
			// genuinely dead session took 3 × interval = 15–90 s to
			// declare. With a 1 s fast retry, the death budget after
			// the first failed ping is bounded by maxKeepaliveRetries
			// × pingTimeout — fast enough that the multipath manager
			// can fail over before the dispatcher repeatedly hits a
			// zombie session.
			const fastRetryGap = 1 * time.Second
			for {
				interval := adaptiveKeepaliveInterval(opts.Session)
				select {
				case <-connCtx.Done():
					return
				case <-time.After(interval):
				}
				// Fast-fail if the session is already closed. Without this,
				// keepalive sat through up to maxKeepaliveRetries × interval
				// before triggering connCancel even when IsClosed was already
				// true — long enough for dispatch to repeatedly hit the
				// zombie session and for the multipath manager to keep
				// selecting closed paths.
				if opts.Session.IsClosed() {
					log.Printf("[AETHER] %s: keepalive detected session closed (grade=%s, interval=%v), cancelling",
						truncID(nodeID), grade, interval)
					connCancel()
					return
				}
				// Look up the per-peer multipath manager to pick the
				// retry budget. Re-read on every outer-loop iteration
				// because path topology evolves: a peer can start with
				// PathCount=1, gain a standby via EnsureK during the
				// session's life, then lose it again. The retry budget
				// should reflect the picture at the moment a ping
				// fails, not the picture at registerMeshSession time.
				maxKeepaliveRetries := retriesNoStandby
				// Only use the tighter has-standby budget once the new
				// path has lived past newPathGraceWindow. Inside that
				// window the standby that PathCount reports is the
				// outgoing OLD session sitting in its 60 s proving
				// timer, not a genuine failover target — closing the
				// new path on 3 strikes during that window throws
				// away exactly the upgrade we are trying to land.
				if time.Since(sessionInstalledAt) > newPathGraceWindow {
					m.multipathMu.Lock()
					if mgr, ok := m.multipathManagers[nodeID]; ok {
						if mgr.PathCount() >= 2 {
							maxKeepaliveRetries = retriesWithStandby
						}
					}
					m.multipathMu.Unlock()
				}
				// Fast-retry inner loop: once a ping fails we don't go
				// back to the outer interval wait. Detection of a dead
				// session is bounded by maxKeepaliveRetries × pingTimeout
				// (worst case) rather than maxKeepaliveRetries × (interval
				// + pingTimeout). Health().RTO() (RFC 6298) gives the
				// per-attempt timeout which is already adapted to the
				// path's mean+jitter.
				dead := false
				for retry := 0; retry < maxKeepaliveRetries; retry++ {
					// BaseSession.Health() returns nil before
					// SetHealthMonitor has been called (see
					// aether/base_session.go). A nil health here means
					// the path is mid-handshake; treat as
					// estimator-unconverged so the keepalive budget
					// stays warm rather than panicking the loop.
					var rto time.Duration
					if h := opts.Session.Health(); h != nil {
						rto = h.RTO()
					}
					pingTimeout := adaptiveKeepaliveTimeout(opts.Session, rto)
					pingCtx, pingCancel := context.WithTimeout(connCtx, pingTimeout)
					_, err := opts.Session.Ping(pingCtx)
					pingCancel()
					if err == nil {
						break // recovered — return to outer loop
					}
					attempt := retry + 1
					log.Printf("[AETHER] %s: keepalive ping failed (%d/%d, grade=%s, rto=%v, timeout=%v, interval=%v): %v",
						truncID(nodeID), attempt, maxKeepaliveRetries, grade, rto, pingTimeout, interval, err)
					if attempt >= maxKeepaliveRetries {
						log.Printf("[AETHER] %s: keepalive dead after %d failures (grade=%s, retryBudget=%d), closing",
							truncID(nodeID), attempt, grade, maxKeepaliveRetries)
						dead = true
						break
					}
					// Bail early if the session was closed under us —
					// no point waiting a fast-retry gap to retry a
					// guaranteed-failure ping.
					if opts.Session.IsClosed() {
						log.Printf("[AETHER] %s: keepalive detected session closed mid-retry (grade=%s), cancelling",
							truncID(nodeID), grade)
						dead = true
						break
					}
					select {
					case <-connCtx.Done():
						return
					case <-time.After(fastRetryGap):
					}
				}
				if dead {
					connCancel()
					return
				}
			}
		}()

		// Control stream reader on stream 3
		go func() {
			for {
				data, err := controlStream.Receive(connCtx)
				if err != nil {
					dbgAether.Printf("%s: control stream exit: %v", truncID(nodeID), err)
					return
				}
				if len(data) == 0 {
					continue
				}
				dbgSessionHealth.Printf("%s: control frame received (%d bytes)", truncID(nodeID), len(data))

				// Control frame dispatch: first byte is the frame type from the Aether codec
				// HANDSHAKE frames have HandshakeType as first byte of payload
				// WHOIS/RENDEZVOUS/NETWORK_CONFIG arrive as raw payloads from the adapter
				// For now, log and handle HANDSHAKE (session resume validation)
				if len(data) > 1 {
					hsType := aether.HandshakeType(data[0])
					switch hsType {
					case aether.HandshakeSessionResume:
						// Responder received a resume token — validate it
						if m.resumeStore != nil {
							token, decErr := resume.DecodeToken(data[1:])
							if decErr == nil {
								if sessionKey := opts.Session.SessionKey(); sessionKey != nil {
									if valErr := token.Validate(sessionKey); valErr == nil {
										log.Printf("[AETHER] %s: 0-RTT resume token validated", truncID(nodeID))
									}
								}
							}
						}
					case aether.HandshakeKeyRotation:
						log.Printf("[AETHER] %s: key rotation request received", truncID(nodeID))
					case aether.HandshakeCapUpdate:
						log.Printf("[AETHER] %s: capability update received", truncID(nodeID))
					case aether.HandshakeAddressMigration:
						log.Printf("[AETHER] %s: address migration received on control stream", truncID(nodeID))
					default:
						log.Printf("[AETHER] %s: unknown control message type %d", truncID(nodeID), hsType)
					}
				}
			}
		}()

		gossipCallback := func(res gossip.GossipResult) {
			rtt := res.NetworkRTT
			if rtt <= 0 {
				rtt = res.RTT
			}
			// Responder-side callbacks pass rtt<=0 (they cannot measure
			// a wire round trip — only the initiator's leg can). Skip
			// the update entirely so the initiator's authoritative
			// sample is not clobbered by a same-cycle responder call
			// with a stale or zero value.
			if rtt <= 0 {
				return
			}
			m.rt.publishGossipLatencyWithTransport(nodeID, rtt, opts.Proto.String())

			// Multipath L3 feed: gossip-measured RTT is the most
			// reliable per-path signal we have (application-level, not
			// transport-level guess). Pass loss=0 — Aether's
			// SessionMetrics doesn't expose a direct loss counter, so
			// weight is driven by Quality + RTT only.
			m.RecordMultipathStats(nodeID, opts.Session, rtt, 0.0)

			// Confidence gate on peer.lastRTT: a single first-window
			// gossip sample can carry hundreds of milliseconds of
			// handshake / scheduling jitter (observed >1000 ms on
			// 3-5 ms physical paths immediately post-deploy). Every
			// downstream consumer — TargetConnections' LatencyFactor,
			// connection_reporter telemetry, reputationTracker's
			// InjectRTT, debug state dumps — propagates that single
			// sample, multiplying scaling targets and corrupting the
			// reliability EMA for the rest of the warmup window.
			// Skip the assignment until the session's RFC 6298
			// estimator has consumed at least 3 correlated pongs;
			// before that, the underlying SRTT/RTO are dominated by
			// the InitialRTO seed and tell the rest of the system
			// nothing useful. lastExchange / successCount / stuck
			// recovery still update — only the RTT field waits.
			rttTrusted := false
			if opts.Session != nil {
				if h := opts.Session.Health(); h != nil {
					rttTrusted = h.SampleCount() >= 3
				}
			}

			m.mu.Lock()
			if rttTrusted {
				peer.lastRTT = rtt
			}
			peer.lastExchange = time.Now()
			peer.successCount++
			// Stuck-recovery accounting: every successful gossip
			// exchange on this peer's current protocol counts toward
			// the recovery threshold. Once enough exchanges have
			// landed since the last stuck event the protocol's
			// stuckCount is reset so the next failure starts from
			// the base cooldown rather than carrying penalty for an
			// outage that has clearly passed.
			m.noteStuckRecovery(peer, opts.Proto)
			m.mu.Unlock()

			if m.rt.cache != nil {
				m.rt.cache.RecordGossipSeen(nodeID)
			}

			// ConnectionOffer handling. When a peer signals
			// WantDirectConnect, the deterministic tiebreaker decides
			// who dials. The multipath manager's EnsureK loop picks
			// this up on its next tick (or the 45 s refresh ticker)
			// and dials a higher-grade transport if one is missing —
			// no manual upgrade hand-off needed.
			_ = res
		}

		// Aether gossip uses initiator/responder roles because Aether streams are
		// message-oriented (Send/Receive), not byte-streams. If both sides
		// independently write-then-read, they deadlock — each waits for a
		// response the other never sends.
		//
		// The responder ALWAYS runs — even on redundant connections — because
		// the peer's initiator depends on it to reply. Only the initiator role
		// is deduplicated: one initiator per peer avoids redundant outbound
		// exchanges.

		if opts.IsInitiator {
			// Grade-aware gossip-initiator dedup with preemption. The
			// outer for-loop attempts to acquire the per-peer dedup
			// slot. Three outcomes per attempt:
			//
			//   1. slot empty → install ourselves, run gossip until
			//      our derived gossipCtx is cancelled (either by
			//      connCtx ending or by a higher-grade peer
			//      preempting us).
			//
			//   2. slot held by lower-grade owner → cancel theirs,
			//      install ourselves, run.
			//
			//   3. slot held by equal-or-higher-grade owner → wait
			//      5 s and retry. We never preempt an equal-grade
			//      owner because it's already doing the same job
			//      we'd be doing, and equal-grade thrash would
			//      churn for no gain.
			//
			// On gossip exit (whether preempted, ctx cancelled, or
			// peer-side error) we clear the slot ONLY if we still
			// own it — a preemptor will have already overwritten it
			// with their own owner record, and clearing wholesale
			// would leak the new owner's pointer.
			ourGrade := tc.grade
			for connCtx.Err() == nil {
				m.gossipMu.Lock()
				if m.gossipActive == nil {
					m.gossipActive = make(map[string]*gossipOwner)
				}
				existing := m.gossipActive[nodeID]
				preempt := false
				if existing != nil && existing.grade < ourGrade {
					// Preempt the lower-grade owner.
					preempt = true
				}
				if existing == nil || preempt {
					if preempt {
						// Cancel the old owner's gossip ctx; their
						// outer loop will see the slot reassigned
						// and back off into wait mode.
						existing.cancel()
						dbgAether.Printf("%s: preempting gossip from grade %s to grade %s",
							truncID(nodeID), existing.grade, ourGrade)
					}
					gossipCtx, gossipCancel := context.WithCancel(connCtx)
					owner := &gossipOwner{grade: ourGrade, cancel: gossipCancel}
					m.gossipActive[nodeID] = owner
					m.gossipMu.Unlock()

					dbgAether.Printf("%s: gossip initiator starting (grade=%s, interval=%v)",
						truncID(nodeID), ourGrade, m.rt.cfg.GossipInterval)
					gossip.RunGossipLoopWithPeer(gossipCtx, gossipStream, reconcileStream, m.rt.cache, m.rt.cfg.GossipInterval, nodeID,
						gossipCallback)
					gossipCancel()

					// Clear the slot only if we still own it. A
					// higher-grade preemptor will have overwritten
					// the entry already; deleting unconditionally
					// would discard their pointer and break the
					// invariant that gossipActive[nodeID] is
					// either nil or a live owner.
					m.gossipMu.Lock()
					if m.gossipActive[nodeID] == owner {
						delete(m.gossipActive, nodeID)
					}
					m.gossipMu.Unlock()

					if connCtx.Err() != nil {
						break
					}
					dbgAether.Printf("%s: gossip exited, retrying in 5s", truncID(nodeID))
					select {
					case <-connCtx.Done():
					case <-time.After(5 * time.Second):
					}
					continue
				}
				m.gossipMu.Unlock()

				// Equal-or-higher-grade owner already running. Wait
				// and re-check; if the existing owner exits or gets
				// preempted by an even higher grade and our grade is
				// the new highest, we'll take over on the next pass.
				select {
				case <-connCtx.Done():
				case <-time.After(5 * time.Second):
				}
			}
		} else {
			// Responder always runs — the peer's initiator needs someone to reply.
			// Restart on exit so the peer's retrying initiator has a responder.
			for connCtx.Err() == nil {
				dbgAether.Printf("%s: gossip responder starting", truncID(nodeID))
				gossip.RunGossipResponder(connCtx, gossipStream, reconcileStream, m.rt.cache, nodeID,
					gossipCallback)
				if connCtx.Err() != nil {
					break
				}
				dbgAether.Printf("%s: responder exited, restarting in 5s", truncID(nodeID))
				select {
				case <-connCtx.Done():
				case <-time.After(5 * time.Second):
				}
			}
		}
	}

cleanup:
	// Gossip loop exited — session is dead or context cancelled
	closeReason := aether.SessionCloseErr(opts.Session)
	dbgAether.Printf("%s: AcceptMeshConnection cleanup (ctxErr=%v, sessionClosed=%v, closeErr=%v)",
		truncID(nodeID), connCtx.Err(), opts.Session.IsClosed(), closeReason)
	connCancel()

	// Track lifetime
	m.mu.Lock()
	// Measure THIS transport's lifetime from tc.connectedAt, not the peer-level
	// lastConnectedAt: that field is overwritten on every accept, so an older
	// transport closing after a newer one was accepted reads ~0 and is
	// misclassified as a dedup race or chronic failure, corrupting the counters.
	lifetime := time.Since(peer.lastConnectedAt)
	if tc != nil && !tc.connectedAt.IsZero() {
		lifetime = time.Since(tc.connectedAt)
	}
	peer.lastConnLifetime = lifetime
	// A drain-initiated close is a voluntary scale-down, not a failure. Counting
	// it as a chronic path failure inflates the peer's cooldown and drives the
	// reconnect churn the drain exists to reduce.
	draining := tc != nil && tc.draining
	// Sessions that die in under dedupRejectWindow are almost always the
	// loser of a dedup race — both sides dialed simultaneously, each
	// probed the other's pre-existing session, one side closed its new
	// session, and the other side observes a near-instant tear-down. This
	// isn't an infrastructure failure (the peer is healthy) so we skip the
	// chronic counter to avoid triggering extended cooldown and the
	// downstream "no suitable address" cascade when the peer manager later
	// tries to reconnect on a peer whose transports were removed.
	//
	// The window is grade-aware. On grade-A (Noise-UDP) the dedup race
	// completes in <500 ms — both sides know each other's session within
	// one RTT plus tie-break verdict. On grade-C (WebSocket / TLS) the
	// race extends to several seconds: the WS handshake itself takes
	// 200–500 ms cross-region, the peer's keepalive interval is ~5 s, and
	// the peer's tie-break can fire any time before the first keepalive
	// confirms our half. Treating any grade-C close in the first ~6 s as
	// a transient race rather than a chronic peer failure prevents the
	// observed cascade where a 788 ms "chronic" tick on a flaky cross-
	// region path bumps the cooldown timer, which delays reconnect, which
	// makes the next dial more likely to lose another race.
	dedupRejectWindow := 500 * time.Millisecond
	if GradeForProtocol(opts.Proto) == GradeC {
		dedupRejectWindow = 6 * time.Second
	}
	switch {
	case draining:
		log.Printf("[AETHER] %s: drained connection (%v, grade %s) — voluntary scale-down, not counted as chronic",
			truncID(nodeID), lifetime, GradeForProtocol(opts.Proto))
	case lifetime < dedupRejectWindow:
		log.Printf("[AETHER] %s: short-lived connection (%v, grade %s) — likely dedup race, not counted as chronic",
			truncID(nodeID), lifetime, GradeForProtocol(opts.Proto))
	case lifetime < chronicThreshold:
		peer.chronicFailCount++
		log.Printf("[AETHER] %s: chronic failure #%d (lived %v, grade %s)",
			truncID(nodeID), peer.chronicFailCount, lifetime, GradeForProtocol(opts.Proto))
		m.RecordPathFailure(nodeID, opts.Proto, remoteAddr)
	default:
		peer.chronicFailCount = 0
	}

	// Stuck-session downgrade: the session closed because aether's
	// session-level stall detector tripped — i.e. the peer is
	// reachable enough to handshake but data doesn't flow (typical on
	// cross-region UDP with asymmetric packet loss, where even periodic
	// WINDOW_UPDATE re-emission can't recover). Mark this protocol
	// failed with its normal cooldown so the next connectPeer scan
	// walks protocolOrder past it to the next grade (Noise-UDP → QUIC
	// → gRPC → WebSocket → TLS). Symmetric with dial-failure handling
	// at peer_connections.go:767, so the rest of the fallback machine
	// (suppression counter, exponential backoff, dormant reactivation)
	// applies unchanged.
	if errors.Is(closeReason, aether.ErrSessionStuck) {
		// Tick the per-peer-per-protocol stuck count so noteStuckRecovery
		// can detect a real recovery later. Cooldown policy is then
		// chosen based on how many times we've already tripped here:
		//
		//   stuckCount < stallTransientThreshold → flat stallCooldown
		//     The first few stalls on a path may be transient asymmetric-
		//     loss bursts (typical of cross-region UDP) that clear in
		//     seconds. Flat 60s lets WS carry traffic during the window
		//     and re-probes UDP quickly when the burst passes — the
		//     happy path the original `recordStallCooldown` was tuned
		//     for.
		//
		//   stuckCount ≥ stallTransientThreshold → recordDialFailure
		//     Three+ stalls in a row without an intervening recovery
		//     indicates this isn't transient — the path is permanently
		//     broken for this (peer, transport) pair (cross-region 6PN
		//     UDP with packet loss high enough that the noise reliability
		//     layer can never drain its window). Escalate to the
		//     exponential dial-failure schedule (30s → 1m → 2m → 4m →
		//     8m → 10m capped) so we stop burning a full handshake
		//     every two minutes on a doomed path. WS continues to carry
		//     traffic; once 5 successful gossip exchanges land on UDP
		//     noteStuckRecovery resets stuckCount and we drop back to
		//     the flat cooldown. Prior to this fix the flat 60s applied
		//     forever, producing observable ~2/min fleet-wide churn from
		//     a handful of permanently-broken cross-region UDP paths.
		m.noteStuckProto(peer, opts.Proto)
		stuckCt := peer.stuckCount[opts.Proto]
		// Hard rule: same-region same-origin peers must stay on
		// noise-UDP. Even if UDP stalls repeatedly, escalating to the
		// exponential dial-failure cooldown causes pickPath's caller
		// to skip UDP and the peer falls to WS — exactly the
		// regression this rule prevents. Cap at the flat stall
		// cooldown so retries stay frequent. AddressTracker history
		// still penalises bad paths; the cap just keeps us trying.
		// *Locked: this block already holds m.mu, and sync.Mutex is not
		// reentrant — the unlocked variant would self-deadlock.
		sameRegionSameOrigin := m.isSameRegionSameOriginLocked(peer)
		if stuckCt >= stallTransientThreshold && !(sameRegionSameOrigin && opts.Proto == ProtoNoiseUDP) {
			m.recordDialFailure(nodeID, opts.Proto)
			log.Printf("[AETHER] %s: session stuck on %s (grade %s, stuckCount=%d ≥ %d) — escalated to exponential dial-failure cooldown",
				truncID(nodeID), opts.Proto, GradeForProtocol(opts.Proto), stuckCt, stallTransientThreshold)
		} else {
			m.recordStallCooldown(nodeID, opts.Proto)
			if sameRegionSameOrigin && opts.Proto == ProtoNoiseUDP {
				log.Printf("[AETHER] %s: same-region same-origin noise-UDP stall (stuckCount=%d) — flat cooldown only (no exponential escalation)",
					truncID(nodeID), stuckCt)
			} else {
				log.Printf("[AETHER] %s: session stuck on %s (grade %s, stuckCount=%d) — stall cooldown %s applied",
					truncID(nodeID), opts.Proto, GradeForProtocol(opts.Proto), stuckCt, stallCooldown)
			}
		}
		m.RecordPathFailure(nodeID, opts.Proto, remoteAddr)
	}

	if peer.connCount > 0 {
		peer.connCount--
	}
	allDead := peer.removeTransport(opts.Proto, tc)
	if allDead {
		peer.state = PeerDisconnected
	}
	m.mu.Unlock()

	// When this was the last live transport for the peer, drop the
	// authoritative liveness override so the LAD cache's normal
	// timeout-based eviction can take over for genuinely-departed
	// peers. We don't clear on every transport drop because a peer
	// with multiple paths (noise-udp + ws) only loses connectivity
	// when all paths are gone.
	if allDead && m.rt != nil && m.rt.cache != nil {
		m.rt.cache.SetGossipLivenessOverride(nodeID, false)
	}

	// Save resume token for 0-RTT reconnection
	if m.resumeStore != nil {
		if sessionKey := opts.Session.SessionKey(); sessionKey != nil {
			if token, err := resume.GenerateToken(sessionKey); err == nil {
				m.resumeStore.Save(nodeID, token, sessionKey)
				log.Printf("[AETHER] %s: saved resume token for 0-RTT", truncID(nodeID))
			}
		}
	}

	// Multipath failover: if a standby session exists, promote it instead of
	// full reconnect. Call OnPrimaryFailure before removing the dying session
	// so the manager can promote the best standby to active.
	failedOver := false
	m.multipathMu.Lock()
	if mgr, ok := m.multipathManagers[nodeID]; ok && mgr.PathCount() > 1 {
		mgr.OnPrimaryFailure()
		// Count every primary-failure → promotion event regardless
		// of whether a live standby was actually found (the manager still
		// attempted a takeover and changed its internal primary index).
		m.multipathFailoverTotal.Add(1)
		if standby, ok := mgr.PrimarySession(); ok && !standby.IsClosed() {
			// Standby promoted — update dispatch registry to use the new primary
			m.dispatchMu.Lock()
			if m.meshSessions != nil {
				m.meshSessions[nodeID] = standby
				// Carry the standby's REAL initiator into the per-nodeID
				// dedup hint. sessionInitiators is the source-of-truth
				// (populated at registerMeshSession); falling back to
				// deletion (previous behaviour) caused both peers to
				// compute the tie-break against the wrong oldInitiator
				// after a parallel failover (H-Mesh-Failover-Initiators).
				if m.meshSessionInitiators != nil {
					if standbyInit, known := m.sessionInitiators[standby]; known {
						m.meshSessionInitiators[nodeID] = standbyInit
					} else {
						// Truly unknown — drop entry. Subsequent register
						// calls will refresh it.
						delete(m.meshSessionInitiators, nodeID)
					}
				}
				// Refresh registration time so the same-initiator
				// dedup window is measured from when this standby
				// became the dispatch target, not from the original
				// (now-dead) primary's install.
				if m.sessionRegisteredAt != nil {
					m.sessionRegisteredAt[nodeID] = time.Now()
				}
				// Per-session bidi map (bidisBySession) means the surviving
				// session ALREADY owns its own bidi entry — failover only
				// needs to swap m.meshSessions[nodeID], no bidi touch.
				// GetBidiRPC routes through the active session pointer
				// from GetMeshSession, so the next dispatch automatically
				// uses the standby's bidi without an extra round-trip.
				//
				// A `delete(m.bidiRPCs, nodeID)` here would break failover:
				// under a nodeID-keyed scheme it ALSO deletes the surviving
				// session's bidi (whichever session registered last), making
				// every RPC after failover pay one dynamic-stream-open RTT
				// (~100 ms cross-region). Omitting it
				// delete restores the bidi-fast-path through failover.
				log.Printf("[AETHER] %s: multipath failover — promoted standby %s (grade %d) as primary",
					truncID(nodeID), standby.Protocol(), SessionGrade(standby))
			}
			m.dispatchMu.Unlock()
			failedOver = true
		}
	}
	m.multipathMu.Unlock()

	if budgetAcquired { // MESH-C07: balance Release strictly against a successful Acquire
		m.budget.Release()
	}
	m.removeMultipathSession(nodeID, opts.Session)

	// Drop THIS session's bidi entry — its rpcStream is gone, so the
	// bidi's readLoop has already exited. Sibling sessions (multipath
	// peers) keep their entries; only the dying one's is removed.
	// Done before unregisterMeshSession so a concurrent GetBidiRPC
	// for this session can't briefly return a dead bidi.
	m.unregisterBidiForSession(opts.Session)

	if failedOver {
		// Standby took over — don't unregister the mesh session or emit disconnect
		log.Printf("[AETHER] %s: %s connection ended (lived %v), standby active",
			truncID(nodeID), opts.Proto, lifetime)
		return nil
	}

	// Pass opts.Session so unregisterMeshSession only mutates dispatch
	// when this is still the registered session. A newer upgrade may
	// have already taken over (e.g. walker installed noise-UDP and the
	// proving period completed) — in that case this cleanup is obsolete
	// and must not remove the live higher-grade session.
	m.unregisterMeshSession(nodeID, opts.Session)

	// Close our own session. cleanup cancels connCtx and unregisters
	// dispatch/multipath/bidi but does not close opts.Session, so a session whose
	// connCtx is cancelled while it is still healthy is orphaned until aether's
	// idle timeout reaps it — holding the
	// peer-side half open too. The failed-over path returned above (the standby
	// owns liveness), so this only closes a session that is genuinely done.
	if !opts.Session.IsClosed() {
		_ = opts.Session.Close()
	}

	if m.scaler != nil {
		m.scaler.EmitEvent(ConnectionEvent{
			PeerNodeID: nodeID,
			OldGrade:   GradeForProtocol(opts.Proto),
			NewGrade:   GradeF,
			Transport:  opts.Proto.String(),
			Reason:     "disconnected",
			Timestamp:  time.Now(),
		})
	}

	// Feed the close event into the shared quality tracker. Short-lived
	// closes are treated as clean: they're almost always the loser of a
	// dedup race rather than a real transport-level failure, and
	// counting them as error closes drives the reliability EMA down,
	// which inflates the scaler's failure factor, which inflates K,
	// which causes more parallel dials and produces MORE dedup races —
	// a self-amplifying feedback loop that turned the initial v0.0.245
	// rollout into 65 active sessions across 9 peers within 30 minutes.
	// The 500 ms dedup-reject window matches the chronic-skip guard
	// above, so the two concur on what counts as a real failure.
	if m.qualityTracker != nil {
		// Classify the close into one of three reasons so the
		// reliability EMA gets the right signal. Audit H9: the
		// original implementation used lifetime alone as a proxy
		// for "dedup race" — meaning ANY session that died in the
		// first 500 ms (or 6 s for Grade-C) was treated as a
		// race-loser and contributed no signal to reliability EMA.
		// That misclassified genuinely-broken paths (handshake
		// failed, AEAD verify failed, immediate RST) as harmless
		// churn, leaving their reliability score at 1.0 forever.
		//
		// Now: a close is CloseRaceLoser ONLY when there's a
		// matching dedup-reject record stamped by registerMeshSession
		// for the same (peer, proto). All other short closes are
		// CloseTransportError — the chronic-fail bucket above
		// already incremented for the same lifetime range, so the
		// reliability tracker now agrees.
		key := quality.Key(nodeID, mapProtocol(opts.Proto).String())
		var reason quality.CloseReason
		hasDedupReject := false
		m.dispatchMu.RLock()
		if rejAt := m.dedupRejectAtFor(nodeID, mapProtocol(opts.Proto).String()); !rejAt.IsZero() {
			// The reject record is fresh if it was stamped within
			// the same lifetime window — i.e., at most one
			// dedupRejectWindow ago, which matches the race
			// timeframe.
			if time.Since(rejAt) < dedupRejectWindow*4 {
				hasDedupReject = true
			}
		}
		m.dispatchMu.RUnlock()
		switch {
		case hasDedupReject:
			reason = quality.CloseRaceLoser
		case lifetime < chronicThreshold:
			reason = quality.CloseTransportError
		default:
			reason = quality.CloseClean
		}
		m.qualityTracker.RecordCloseReason(key, reason)
	}

	log.Printf("[AETHER] %s: %s connection ended (lived %v)", truncID(nodeID), opts.Proto, lifetime)
	return nil
}

// ────────────────────────────────────────────────────────────────────────────
// Aether Session Registry (for dispatch lookup)
// ────────────────────────────────────────────────────────────────────────────

// provingSession tracks an upgrade in progress. The old session is kept alive
// for 60s while the new session proves it's stable. If the new session dies
// during proving, the old session is restored as primary dispatch target.
//
// fromWalker is set when the new session originated from a proactive
// upgrade-walker probe (registerMeshSession consults
// ConnectionManager.walkerPendingSessions to decide). Used by the proving
// timer and unregisterMeshSession's revert branch to bill walker proving
// success/failure to walkerProbesProvingSucceeded / walkerProbesProvingFailed
// — the honest "did the upgrade actually stick" counters complementary to
// walkerProbesSucceeded (which only proves the handshake completed).
type provingSession struct {
	oldSession aether.Session
	newSession aether.Session
	startedAt  time.Time
	timer      *time.Timer
	fromWalker bool
}

// completeProving is the body of the 60-second proving timer installed by
// registerMeshSession's upgrade-promote branch: the new session survived its
// window, so the old fallback can be closed and a walker-initiated upgrade
// billed as durably successful.
//
// A named function rather than an inline time.AfterFunc closure, so it is
// reachable from a test without waiting 60 seconds. Inlined, the
// walkerProbesProvingSucceeded billing — half of the pair answering "did
// walker probes produce DURABLE upgrades?" — cannot be exercised at all.
//
// Before closing the old session it re-checks that the new one is STILL the
// dispatch entry. If a third upgrade has replaced it, the old may have been
// reinstalled as a transient fallback by unregisterMeshSession's revert
// branch, and closing it would clobber whichever session currently answers
// RPC dispatch. The identity check `p == ps` does the same for the proving
// entry itself.
func (m *ConnectionManager) completeProving(nodeID string, ps *provingSession, old, session aether.Session) {
	m.dispatchMu.Lock()
	defer m.dispatchMu.Unlock()
	p, ok := m.proving[nodeID]
	if !ok || p != ps {
		return
	}
	delete(m.proving, nodeID)
	newStillCurrent := m.meshSessions[nodeID] == session
	log.Printf("[AETHER] Proving complete for %s: %s → %s upgrade confirmed (newStillCurrent=%v)",
		truncID(nodeID), old.Protocol(), session.Protocol(), newStillCurrent)
	log.Printf("[DISPATCH-TRACE] proving complete: peer=%s oldProto=%s newProto=%s closingOld=%v newStillCurrent=%v",
		truncID(nodeID), old.Protocol(), session.Protocol(), !old.IsClosed() && newStillCurrent, newStillCurrent)
	if newStillCurrent && !old.IsClosed() {
		go old.Close()
	}
	// Bill walker-initiated proving success only when the new session was
	// still the dispatch entry at the moment proving completed. Anything else
	// means a later upgrade already replaced this one and the outcome is owned
	// by that later proving cycle.
	if ps.fromWalker && newStillCurrent {
		m.walkerProbesProvingSucceeded.Add(1)
	}
}

// resetReconnectDelay collapses a peer's escalated reconnect backoff back to
// the boot default. Called from observed-good signal sites (addTransport on
// the peer side, registerMeshSession's success branches here) so a peer whose
// backoff has escalated to maxCooldown returns to the small initial delay
// once a fresh session lands. Caller MUST NOT hold m.mu.
func (m *ConnectionManager) resetReconnectDelay(nodeID string) {
	m.mu.Lock()
	if p, ok := m.peers[nodeID]; ok {
		p.reconnectDelay = baseCooldown
	}
	m.mu.Unlock()
}

// registerMeshSession stores an Aether session for dispatch.
// Called by AcceptMeshConnection when a session is established.
//
// Grade-aware: rejects connections of equal or lower grade than the existing
// session. Only accepts upgrades (higher grade). Returns false if rejected.
//
// For same-grade reconnect attempts, the existing session is probed (short
// Ping) before the reject decision is made — a TCP half-close (e.g. Fly edge
// proxy drops peer-side FIN) can leave the server-side socket appearing open
// until the next keepalive fires 30-90s later. Without the probe, a peer that
// noticed its side was dead and reconnected would be rejected until that
// window elapsed, producing the observed WS reconnect churn.
func (m *ConnectionManager) registerMeshSession(ctx context.Context, nodeID string, session aether.Session, isInitiator bool) bool {
	m.dispatchMu.Lock()
	if m.meshSessions == nil {
		m.meshSessions = make(map[string]aether.Session)
	}
	if m.meshSessionInitiators == nil {
		m.meshSessionInitiators = make(map[string]bool)
	}
	if m.sessionRegisteredAt == nil {
		m.sessionRegisteredAt = make(map[string]time.Time)
	}
	if m.dedupRejectAt == nil {
		m.dedupRejectAt = make(map[string]time.Time)
	}
	if m.sessionInitiators == nil {
		m.sessionInitiators = make(map[aether.Session]bool)
	}

	newGrade := SessionGrade(session)
	log.Printf("[DISPATCH-TRACE] register entry: peer=%s newProto=%s newGrade=%d existing=%v initiator=%v meshSessionsLen=%d",
		truncID(nodeID), session.Protocol(), newGrade, m.meshSessions[nodeID] != nil, isInitiator, len(m.meshSessions))

	if old, ok := m.meshSessions[nodeID]; ok && old != session && !old.IsClosed() {
		oldGrade := SessionGrade(old)
		oldInitiatedByUs := m.meshSessionInitiators[nodeID]

		if newGrade < oldGrade {
			// Lower grade — check if it can coexist as a dormant fallback
			// (e.g., TLS Bootstrap alongside active WebSocket).
			//
			// We DO NOT probe here. Probing on a busy session (gossip
			// in flight on streams) can race with stream reads/writes —
			// the Ping write may queue behind a frame that's stuck on
			// the wire, exceeding the probe timeout, and the probe
			// concludes the session is dead when it isn't. We've
			// observed this cascading session-closures across the
			// fleet and that pattern is gone now that the probe is
			// removed.
			//
			// Half-closed primaries are caught a different way: when
			// the gossip stream's read loop sees the underlying TCP
			// FIN (eventually surfaced via keepalive), unregister-
			// MeshSession runs and clears the slot. The dormant
			// session is then promoted on the next reconnect.
			// A strictly lower grade ALWAYS coexists as a dormant
			// fallback — there is no reject case here. The removed
			// `CanCoexistWith` check was a tautology at this point
			// (`newGrade < oldGrade` already implies they differ), so
			// the rejection it guarded could never fire.
			log.Printf("[AETHER] Accepting %s as dormant fallback for %s (existing %s, grade %d > %d)",
				session.Protocol(), truncID(nodeID), old.Protocol(), oldGrade, newGrade)
			log.Printf("[DISPATCH-TRACE] register exit: peer=%s decision=dormant-fallback newProto=%s oldProto=%s newGrade=%d oldGrade=%d meshSessions[peer]=%s",
				truncID(nodeID), session.Protocol(), old.Protocol(), newGrade, oldGrade, "unchanged")
			m.dispatchMu.Unlock()
			m.resetReconnectDelay(nodeID)
			return true
		}

		if newGrade == oldGrade {
			// Same grade — apply deterministic tie-break BEFORE the
			// liveness probe so simultaneous-dial races converge to the
			// same winning session on both peers.
			//
			// The simultaneous-dial pattern that this prevents: A and B
			// run scanAndConnect on the same cadence and dial each other
			// at nearly the same instant. Each side's outbound succeeds
			// and registers as its primary. Then each side receives the
			// peer's outbound (an inbound on the receive side). Without a
			// deterministic rule each side independently rejected the
			// inbound and kept its own outbound — but the peer was doing
			// the same with the OTHER session, so each side ended up with
			// a session the peer had discarded. Keepalive failed inside
			// 5–9 s on the abandoned half, both sides closed, both sides
			// redialed, and the cycle repeated. Operator visible: peers
			// flapping every ~5 s, longest_session_seconds bounded by
			// the keepalive interval.
			//
			// Tie-break rule: "the session whose initiator has the lower
			// nodeID wins." Both peers compute the same initiator nodeID
			// pair (just labelled differently locally — one side's
			// outbound is the other side's inbound) and converge on the
			// same winning session.
			//
			//   Local view:                                  Initiator nodeID
			//   ──────────────────────────────────────────   ────────────────
			//   our outbound (we initiated)         →        m.selfID
			//   their outbound (we accepted, !init) →        nodeID (peer)
			//
			// If the existing AND the new session were both initiated by
			// the same side (e.g., us redialing while a stale outbound
			// hangs on), the comparison returns equal and we fall back to
			// the liveness-probe path: keep whichever passes the probe.
			oldInitiator := nodeID
			if oldInitiatedByUs {
				oldInitiator = m.selfID
			}
			newInitiator := nodeID
			if isInitiator {
				newInitiator = m.selfID
			}

			if oldInitiator != newInitiator {
				// Lower wins. Both peers see the same (initiator-nodeID,
				// other-nodeID) pair so this comparison is symmetric.
				keepNew := newInitiator < oldInitiator
				if !keepNew {
					log.Printf("[AETHER] Rejecting %s session for %s (tie-break: existing initiator=%s wins over new=%s)",
						session.Protocol(), truncID(nodeID), truncID(oldInitiator), truncID(newInitiator))
					log.Printf("[DISPATCH-TRACE] register exit: peer=%s decision=reject-tiebreak-loser newProto=%s oldProto=%s grade=%d oldInit=%s newInit=%s",
						truncID(nodeID), session.Protocol(), old.Protocol(), newGrade, truncID(oldInitiator), truncID(newInitiator))
					m.recordDedupRejectLocked(nodeID, session.Protocol().String())
					m.dispatchMu.Unlock()
					go session.Close()
					return false
				}
				// New wins — replace old.
				log.Printf("[AETHER] Replacing %s session for %s (tie-break: new initiator=%s wins over existing=%s)",
					session.Protocol(), truncID(nodeID), truncID(newInitiator), truncID(oldInitiator))
				m.evictGossipOwnerLocked(nodeID, "tie-break-replace")
				go old.Close()
				delete(m.sessionInitiators, old)
				m.meshSessions[nodeID] = session
				m.meshSessionInitiators[nodeID] = isInitiator
				m.sessionInitiators[session] = isInitiator
				m.sessionRegisteredAt[nodeID] = time.Now()
				m.dispatchMu.Unlock()
				log.Printf("[DISPATCH-TRACE] register exit: peer=%s decision=replaced-tiebreak-winner newProto=%s oldProto=%s grade=%d oldInit=%s newInit=%s meshSessionsLen=%d",
					truncID(nodeID), session.Protocol(), old.Protocol(), newGrade, truncID(oldInitiator), truncID(newInitiator), len(m.meshSessions))
				m.resetReconnectDelay(nodeID)
				return true
			}

			// Same-initiator (both sessions initiated by our side): the
			// new session is a duplicate dial — EnsureK or scanAndConnect
			// reaching a peer we already have a session with.
			//
			// Fix B — never close a not-yet-closed session here. The
			// previous probe-and-maybe-replace logic closed the existing
			// session whenever a liveness probe failed; on a young,
			// in-use session that probe false-negatives often, and the
			// resulting `go old.Close()` cascades to the peer (its
			// readLoop hits EOF → both sides redial). Verified as the
			// dominant churn driver: decision=replaced-dead-same-grade
			// alone was ~20/min fleet-wide.
			//
			// The keepalive loop + the aether stall detector are the
			// SOLE authority on session liveness — they probe
			// continuously and close a genuinely-dead session within
			// bounded time (≈15-60 s). Dedup's only job is "don't add a
			// duplicate". So: if the existing session is still open,
			// keep it and reject the duplicate (closing the unused new
			// session is safe — it was never carrying traffic, so its
			// teardown doesn't disturb the peer's primary). Only when
			// the existing session has ALREADY closed do we install the
			// new one as primary.
			if !old.IsClosed() {
				m.recordDedupRejectLocked(nodeID, session.Protocol().String())
				m.dispatchMu.Unlock()
				log.Printf("[AETHER] Rejecting %s session for %s (existing %s still open, same grade %d, same-initiator — keepalive owns liveness)",
					session.Protocol(), truncID(nodeID), old.Protocol(), newGrade)
				log.Printf("[DISPATCH-TRACE] register exit: peer=%s decision=reject-same-initiator-old-alive newProto=%s oldProto=%s grade=%d",
					truncID(nodeID), session.Protocol(), old.Protocol(), newGrade)
				go session.Close()
				return false
			}

			// Existing session already closed (readLoop exit / keepalive
			// kill) — install the new one as primary.
			m.evictGossipOwnerLocked(nodeID, "same-initiator-old-closed")
			delete(m.sessionInitiators, old)
			m.meshSessions[nodeID] = session
			m.meshSessionInitiators[nodeID] = isInitiator
			m.sessionInitiators[session] = isInitiator
			m.sessionRegisteredAt[nodeID] = time.Now()
			m.dispatchMu.Unlock()
			log.Printf("[AETHER] Registered session for %s (proto=%s, grade=%d)", truncID(nodeID), session.Protocol(), newGrade)
			log.Printf("[DISPATCH-TRACE] register exit: peer=%s decision=installed-same-initiator-old-closed newProto=%s grade=%d meshSessionsLen=%d",
				truncID(nodeID), session.Protocol(), newGrade, len(m.meshSessions))
			log.Printf("[AETHER] Registered session for %s (proto=%s, grade=%d)", truncID(nodeID), session.Protocol(), newGrade)
			log.Printf("[DISPATCH-TRACE] register exit: peer=%s decision=replaced-dead-same-grade newProto=%s oldProto=%s grade=%d meshSessionsLen=%d",
				truncID(nodeID), session.Protocol(), old.Protocol(), newGrade, len(m.meshSessions))
			m.resetReconnectDelay(nodeID)
			return true
		}

		// newGrade > oldGrade — fall through to upgrade path below. We still
		// hold dispatchMu from the outer Lock call.

		// Grade-upgrade promote: the OLD session is being demoted as
		// dispatch primary. Its bidi entry is no longer the routing
		// target (GetBidiRPC routes through the active session, which
		// the upgrade has just swapped to the new session). The old
		// session may still survive the proving window as a fallback,
		// and its bidi remains live for that purpose; the active-
		// session lookup in GetBidiRPC naturally selects the new
		// session's bidi instead. No explicit eviction needed.
		// Kept for log-trace continuity:
		log.Printf("[AETHER] Grade upgrade for %s (%d→%d) — dispatch routes through new session's bidi via active-session lookup",
			truncID(nodeID), oldGrade, newGrade)

		// Proving timer — keep the old session alive for 60s as a fallback.
		// If the new (upgraded) session dies during the proving window,
		// unregisterMeshSession reverts to the old session rather than
		// leaving the peer entirely disconnected.
		// Evict the gossip dedup owner so the new session's higher-grade
		// initiator can take over without waiting for the lower-grade
		// owner's natural exit. evictGossipOwnerLocked cancels the
		// owner's gossip context (causing RunGossipLoopWithPeer to
		// return) and clears the slot.
		m.evictGossipOwnerLocked(nodeID, "upgrade-promote")
		log.Printf("[AETHER] Upgrading %s → %s for %s (grade %d → %d, proving 60s)",
			old.Protocol(), session.Protocol(), truncID(nodeID), oldGrade, newGrade)

		if m.proving == nil {
			m.proving = make(map[string]*provingSession)
		}

		// Cancel any existing proving session for this peer
		if prev, exists := m.proving[nodeID]; exists {
			prev.timer.Stop()
			go prev.oldSession.Close()
		}

		// Walker attribution: if the peer was tagged by the proactive
		// upgrade walker's probeUpgrade handoff, mark the provingSession
		// so the timer / revert paths can bill the outcome to
		// walkerProbesProvingSucceeded vs walkerProbesProvingFailed.
		// Consume the tag here so subsequent registrations of unrelated
		// sessions don't inherit walker attribution. Keyed by peer
		// nodeID — see walkerPendingSessions field comment for the
		// shared-key rationale.
		fromWalker := m.consumeWalkerPendingSession(nodeID)
		if fromWalker {
			m.walkerProbesProving.Add(1)
		}
		ps := &provingSession{
			oldSession: old,
			newSession: session,
			startedAt:  time.Now(),
			fromWalker: fromWalker,
		}
		ps.timer = time.AfterFunc(60*time.Second, func() {
			m.completeProving(nodeID, ps, old, session)
		})
		m.proving[nodeID] = ps
	}

	m.meshSessions[nodeID] = session
	m.meshSessionInitiators[nodeID] = isInitiator
	m.sessionInitiators[session] = isInitiator
	m.sessionRegisteredAt[nodeID] = time.Now()
	m.dispatchMu.Unlock()
	// Session-hook fanout (swarm-transport RegisterPeer + any other
	// SetSessionHook consumer) is deferred to AcceptMeshConnection
	// AFTER the five well-known streams (gossip, RPC, keepalive,
	// control, reconcile) finish open/accept on both sides. Firing
	// the hook here lets the swarm transport open application-stream
	// 100 BEFORE the responder reaches AcceptStream for gossip
	// (stream 0); the FIFO accept channel then returns stream 100 in
	// gossip's slot and the foreground gossip loop ends up bound to
	// the swarm stream — observable as a session that registers and
	// then dies within milliseconds because gossip's reader gets data
	// it can't decode while the swarm reader competes for the same
	// stream. See AcceptMeshConnection for the post-streams-open
	// fireSessionHook call site.
	log.Printf("[AETHER] Registered session for %s (proto=%s, grade=%d)", truncID(nodeID), session.Protocol(), newGrade)
	// Trace exit covers both the upgrade path (proving started) and the
	// fresh-install path (no existing session). The branch can be inferred
	// from the presence of a prior "[AETHER] Upgrading" log line.
	log.Printf("[DISPATCH-TRACE] register exit: peer=%s decision=installed-as-primary newProto=%s grade=%d initiator=%v meshSessionsLen=%d",
		truncID(nodeID), session.Protocol(), newGrade, isInitiator, len(m.meshSessions))
	m.resetReconnectDelay(nodeID)
	return true
}

// registerBidiRPC stores a BidiRPC channel for a session's Stream 1.
// Keyed by the session pointer so that multipath siblings (multiple
// sessions to the same peer) coexist with their own bidis instead of
// the latest registration clobbering the previous one. See the
// bidisBySession field doc for the failure mode the nodeID-keyed
// version produced.
func (m *ConnectionManager) registerBidiRPC(sess aether.Session, bidi *BidiRPC) {
	m.dispatchMu.Lock()
	defer m.dispatchMu.Unlock()
	if m.bidisBySession == nil {
		m.bidisBySession = make(map[aether.Session]*BidiRPC)
	}
	m.bidisBySession[sess] = bidi
}

// unregisterBidiForSession removes the bidi entry for a specific session
// pointer. Called by the session's cleanup path; safe to call even if
// no entry exists. Does not touch sibling sessions' entries for the
// same peer.
func (m *ConnectionManager) unregisterBidiForSession(sess aether.Session) {
	m.dispatchMu.Lock()
	defer m.dispatchMu.Unlock()
	delete(m.bidisBySession, sess)
}

// unregisterMeshSession removes an Aether session and BidiRPC from the
// dispatch registry. If the dying session is the freshly-upgraded
// session still within its 60s proving window, the previous (still-
// alive) session is restored as primary instead of the peer being
// marked disconnected.
//
// The expected parameter is the session pointer the caller owns. When
// non-nil and the current dispatch entry no longer matches, this
// function returns without mutating dispatch. That guard prevents an
// OLD session's cleanup goroutine from silently removing a NEWER
// session that registerMeshSession installed after the old session's
// proving period completed — exactly the scenario that produced
// walker_probes_succeeded growing into the thousands while every
// peer's bestEverGrade stayed at C: noise-UDP installed, promoted,
// the OLD WS session's deferred cleanup later called this function,
// found the dispatch entry pointing at noise-UDP, and deleted it.
//
// A nil expected value is still an identity: the zombie sweep can
// snapshot an explicitly nil map slot. If registration replaces that
// slot before cleanup runs, the replacement must survive just as it
// would for any other stale session pointer.
// unregisterMeshSession reports whether it removed the dispatch entry owned by
// expected. Callers that perform peer-wide cleanup must honor the result:
// false means a newer session owns the peer and none of that cleanup belongs to
// this stale caller.
func (m *ConnectionManager) unregisterMeshSession(nodeID string, expected aether.Session) bool {
	removed := m.unregisterMeshSessionWithoutHook(nodeID, expected)
	if removed {
		// Fire after dispatchMu is released. The hook may re-enter dispatch.
		m.fireSessionHook(nodeID, nil, false)
	}
	return removed
}

// unregisterMeshSessionWithoutHook performs only the dispatch mutation. The
// zombie sweeper uses it while holding the per-peer lifecycle transaction and
// fires the external hook after every downstream peer-wide cleanup completes.
// Other callers should use unregisterMeshSession.
func (m *ConnectionManager) unregisterMeshSessionWithoutHook(nodeID string, expected aether.Session) bool {
	m.dispatchMu.Lock()
	defer m.dispatchMu.Unlock()

	hadSession := false
	var hadProto string
	if cur, ok := m.meshSessions[nodeID]; ok && cur != nil {
		hadSession = true
		hadProto = cur.Protocol().String()
	}
	log.Printf("[DISPATCH-TRACE] unregister entry: peer=%s hadSession=%v hadProto=%s meshSessionsLen=%d proving=%v expected=%p",
		truncID(nodeID), hadSession, hadProto, len(m.meshSessions), m.proving != nil && m.proving[nodeID] != nil, expected)

	// Nil is the value captured from a present-but-empty zombie slot, not a
	// wildcard. Registration may replace that slot after the sweep releases
	// dispatchMu; refuse to delete either an absent entry or its live
	// replacement. Non-nil ownership keeps the established proving semantics
	// below unchanged.
	if expected == nil {
		current, present := m.meshSessions[nodeID]
		if !present || current != nil {
			m.unregisterSkippedNotOwner.Add(1)
			currentProto := ""
			if current != nil {
				currentProto = current.Protocol().String()
			}
			log.Printf("[DISPATCH-TRACE] unregister exit: peer=%s decision=skipped-nil-not-owner currentProto=%s meshSessionsLen=%d",
				truncID(nodeID), currentProto, len(m.meshSessions))
			return false
		}
	}

	// If the dying session is the proving new session, revert to the old
	// session instead of removing the peer entry entirely. The proving
	// revert must only fire when THIS caller owns the newSession — if
	// expected is non-nil and doesn't match, a later upgrade has already
	// taken over and the revert would clobber a session we don't own.
	if m.proving != nil {
		if ps, ok := m.proving[nodeID]; ok {
			current := m.meshSessions[nodeID]
			if expected != nil && expected != ps.newSession && expected != ps.oldSession {
				// Neither party of the proving window owns this expected
				// pointer — a third (newer) session has taken over. Skip
				// the revert; the new owner's lifecycle drives dispatch.
				m.unregisterSkippedNotOwner.Add(1)
				log.Printf("[DISPATCH-TRACE] unregister exit: peer=%s decision=skipped-proving-mismatch expectedProto=%s",
					truncID(nodeID), expected.Protocol())
				return false
			}
			if current == ps.newSession && !ps.oldSession.IsClosed() {
				// New session died during proving — revert to old
				ps.timer.Stop()
				delete(m.proving, nodeID)
				m.meshSessions[nodeID] = ps.oldSession
				// We don't have the old session's initiator preserved
				// across proving — clear the hint so the next register
				// call refreshes it.
				if m.meshSessionInitiators != nil {
					delete(m.meshSessionInitiators, nodeID)
				}
				if m.sessionRegisteredAt != nil {
					m.sessionRegisteredAt[nodeID] = time.Now()
				}
				// Walker attribution: bill the failure to the walker's
				// honest proving-failed counter if this proving session
				// was initiated by a walker probe. Pairs with the
				// success branch in the proving timer above; together
				// they answer "did walker probes actually produce
				// durable upgrades?" — a question walkerProbesSucceeded
				// can't answer because it fires on handshake completion.
				if ps.fromWalker {
					m.walkerProbesProvingFailed.Add(1)
				}
				log.Printf("[AETHER] Proving failed for %s: reverted to %s (grade %d)",
					truncID(nodeID), ps.oldSession.Protocol(), SessionGrade(ps.oldSession))
				log.Printf("[DISPATCH-TRACE] unregister exit: peer=%s decision=reverted-to-proving-old oldProto=%s",
					truncID(nodeID), ps.oldSession.Protocol())
				return false
			}
			// Old session died during proving — that's fine, just clean up
			ps.timer.Stop()
			delete(m.proving, nodeID)
			log.Printf("[DISPATCH-TRACE] unregister: peer=%s proving-old-died cleanup-only", truncID(nodeID))
		}
	}

	// Identity guard: when the caller owns a specific session, only mutate
	// dispatch if the current entry IS that session. Otherwise a newer
	// session has installed itself and this cleanup is obsolete.
	if expected != nil {
		current, present := m.meshSessions[nodeID]
		// Presence is checked here for the same reason the nil-expected guard
		// above checks it. Without it an ABSENT entry yields current == nil, the
		// ownership comparison is skipped, and the teardown below runs anyway:
		// the delete is a no-op but unregisterDeleted counts a removal that did
		// not happen, clearDedupRejectAllForPeer drops the peer's dial back-off,
		// and the true return makes unregisterMeshSession fire a disconnect hook
		// for a session that was not there. Two concurrent teardowns for one peer
		// reach exactly this state — the second finds the entry already gone.
		//
		// A present-but-nil slot is still deletable: that is the zombie slot the
		// sweeper is meant to clear, and only a genuinely missing key is refused.
		if !present || (current != nil && current != expected) {
			m.unregisterSkippedNotOwner.Add(1)
			// current is nil on the absent-entry refusal, so its protocol is
			// read conditionally — the same shape the nil-expected guard above
			// uses for the same reason.
			currentProto := ""
			if current != nil {
				currentProto = current.Protocol().String()
			}
			log.Printf("[DISPATCH-TRACE] unregister exit: peer=%s decision=skipped-not-owner expectedProto=%s currentProto=%s meshSessionsLen=%d",
				truncID(nodeID), expected.Protocol(), currentProto, len(m.meshSessions))
			return false
		}
	}

	var deadSession aether.Session
	if m.meshSessions != nil {
		deadSession = m.meshSessions[nodeID]
		delete(m.meshSessions, nodeID)
	}
	m.unregisterDeleted.Add(1)
	if m.meshSessionInitiators != nil {
		delete(m.meshSessionInitiators, nodeID)
	}
	if m.sessionInitiators != nil && deadSession != nil {
		delete(m.sessionInitiators, deadSession)
	}
	if m.sessionRegisteredAt != nil {
		delete(m.sessionRegisteredAt, nodeID)
	}
	// Clear the dedup back-off — the session is genuinely gone now, so a
	// fresh dial is no longer a duplicate. Keeping a stale stamp would
	// suppress EnsureK path-building for up to dedupRejectCooldown after
	// a real disconnect.
	m.clearDedupRejectAllForPeer(nodeID)
	// Bidi cleanup is now done per-session via unregisterBidiForSession
	// from the dying session's own cleanup path (see AcceptMeshConnection
	// cleanup label). No nodeID-keyed bidi to delete here.
	log.Printf("[DISPATCH-TRACE] unregister exit: peer=%s decision=removed meshSessionsLen=%d", truncID(nodeID), len(m.meshSessions))
	return true
}

// GetBidiRPC returns the bidirectional RPC channel for the currently
// active session to a peer. Looks up the active session via
// GetMeshSession and returns the bidi entry registered against it.
//
// Returns (nil, false) when:
//   - No active session is known for nodeID (peer disconnected /
//     connecting), OR
//   - The active session has no bidi registered yet (race between
//     session install and AcceptMeshConnection's bidi registration).
//
// Routing through the active session pointer means a failover that
// promotes a standby to primary immediately exposes that standby's
// bidi — no eviction or re-registration step is needed, because every
// session always owned its own bidi. The old nodeID-keyed scheme lost
// access to surviving bidis when multipath registrations overwrote the
// map entry; see bidisBySession field doc.
func (m *ConnectionManager) GetBidiRPC(nodeID string) (*BidiRPC, bool) {
	sess, ok := m.GetMeshSession(nodeID)
	if !ok || sess == nil {
		return nil, false
	}
	m.dispatchMu.RLock()
	defer m.dispatchMu.RUnlock()
	if m.bidisBySession == nil {
		return nil, false
	}
	bidi, ok := m.bidisBySession[sess]
	return bidi, ok
}

// GetMeshSession returns the Aether session for a specific node, used
// by AetherCaller for direct dispatch.
//
// When a multipath manager exists for the peer, picks the highest-
// DispatchRank alive session via PickByQuality(OpDefault). Score
// inputs come from the shared quality.Tracker (per-(peer, transport)
// baselines) and region-affinity is applied as a tiebreaker, so a
// healthy same-region session wins over a healthy cross-region session
// even though both are "alive". No explicit primary — whichever
// session ranks highest at the moment of dispatch is the primary.
//
// Fallback chain (returns first hit):
//
//  1. PickByQuality(OpDefault) — quality-driven primary.
//  2. AllSessions(), first non-closed — guards against a zombie
//     primary that hasn't been pruned yet.
//  3. meshSessions[nodeID] — for peers whose multipath manager hasn't
//     been initialised (narrow window between registerMeshSession and
//     addMultipathSession during connect).
//
// For op-class-specific dispatch (latency-sensitive, bulk, gossip),
// use GetMeshSessionForOp. For per-frame WDRR by frame size, use
// GetMeshSessionForBytes.
func (m *ConnectionManager) GetMeshSession(nodeID string) (aether.Session, bool) {
	mpExists := false
	mpPrimaryClosed := false
	mpAllClosed := 0
	mpAllOpen := 0
	if mgr, ok := m.GetMultipathManager(nodeID); ok {
		mpExists = true
		// Quality-driven pick comes first. PickByQuality returns
		// the highest-DispatchRank alive session; falls back to
		// PrimarySession internally when no quality config is
		// attached (legacy callers).
		if sess, ok := mgr.PickByQuality(quality.OpDefault); ok && sess != nil && !sess.IsClosed() {
			return sess, true
		}
		// PickByQuality returned a closed session or none — walk all
		// paths to find any open one. Same fallback the previous
		// implementation used; preserves dispatch availability when
		// the score-driven primary briefly points at a zombie.
		for _, sess := range mgr.AllSessions() {
			if sess == nil {
				continue
			}
			if !sess.IsClosed() {
				mpAllOpen++
				return sess, true
			}
			mpAllClosed++
		}
		mpPrimaryClosed = true
	}
	m.dispatchMu.RLock()
	defer m.dispatchMu.RUnlock()
	if m.meshSessions == nil {
		log.Printf("[DISPATCH-TRACE] GetMeshSession=false peer=%s reason=meshSessions-nil multipath=%v", truncID(nodeID), mpExists)
		return nil, false
	}
	session, ok := m.meshSessions[nodeID]
	if !ok {
		log.Printf("[DISPATCH-TRACE] GetMeshSession=false peer=%s reason=key-absent multipath=%v mpPrimaryClosed=%v mpAllClosed=%d meshSessionsLen=%d",
			truncID(nodeID), mpExists, mpPrimaryClosed, mpAllClosed, len(m.meshSessions))
		return nil, false
	}
	if session.IsClosed() {
		log.Printf("[DISPATCH-TRACE] GetMeshSession=false peer=%s reason=session-closed proto=%s multipath=%v mpPrimaryClosed=%v mpAllClosed=%d",
			truncID(nodeID), session.Protocol(), mpExists, mpPrimaryClosed, mpAllClosed)
		return nil, false
	}
	return session, ok
}

// GetMeshSessionForBytes returns the appropriate session for a frame
// of the given size, honouring multipath L3 weighted round-robin when
// enabled. This is the preferred accessor for bulk / non-REALTIME
// sends: weighted distribution across healthy paths maximises
// throughput without blocking on primary failure.
//
// Falls through to GetMeshSession (primary path) when the peer has
// only one path or the multipath manager is not configured for L3.
// Callers sending REALTIME-class traffic should use GetAllMeshSessions
// instead so both paths receive the frame (duplication is defence
// against single-path loss).
func (m *ConnectionManager) GetMeshSessionForBytes(nodeID string, frameSize int) (aether.Session, bool) {
	if mgr, ok := m.GetMultipathManager(nodeID); ok {
		if picked := mgr.PickPath(frameSize); picked != nil && !picked.IsClosed() {
			return picked, true
		}
	}
	return m.GetMeshSession(nodeID)
}

// GetMeshSessionForOp returns the highest-DispatchRank alive session
// for the given op class:
//
//   - OpLatencySensitive favours the path with the best RTT score
//     (interactive RPC, screen key frames, input events).
//   - OpBulkTransfer favours the path with the best throughput score
//     (file uploads, snapshot syncs, cold-start bootstraps).
//   - OpGossip favours the most stable path — flap-prone paths cost
//     gossip more than slow ones.
//   - OpDefault uses the pure aggregate score plus region affinity.
//
// Falls back to GetMeshSession when the manager hasn't been quality-
// configured yet (very narrow window during connect).
func (m *ConnectionManager) GetMeshSessionForOp(nodeID string, op quality.DispatchOp) (aether.Session, bool) {
	if mgr, ok := m.GetMultipathManager(nodeID); ok {
		if picked, picked_ok := mgr.PickByQuality(op); picked_ok && picked != nil && !picked.IsClosed() {
			return picked, true
		}
	}
	return m.GetMeshSession(nodeID)
}

// GetAllMeshSessions returns every active path to the peer — used by
// callers sending REALTIME-class frames so they can duplicate across
// paths for instant loss tolerance. Returns nil when no multipath
// manager exists (single-path peer).
func (m *ConnectionManager) GetAllMeshSessions(nodeID string) []aether.Session {
	mgr, ok := m.GetMultipathManager(nodeID)
	if !ok {
		return nil
	}
	all := mgr.AllSessions()
	sessions := make([]aether.Session, 0, len(all))
	for _, s := range all {
		if s != nil && !s.IsClosed() {
			sessions = append(sessions, s)
		}
	}
	return sessions
}

// GetAnyMeshSession returns the highest-grade available Aether session
// for mesh forwarding. Used by AetherCaller for forwarding dispatch
// (Path 3).
//
// Deterministic by construction. Iterating m.meshSessions and returning the
// first non-closed entry is not: Go randomises map order, so a cross-region
// anchor Grade-C session wins over a same-region peer Grade-A session on one
// call and loses on the next — chronic high-latency forwarding
// hops for blind-relay traffic. Now we scan all sessions and prefer
// the highest grade.
func (m *ConnectionManager) GetAnyMeshSession() (aether.Session, bool) {
	m.dispatchMu.RLock()
	defer m.dispatchMu.RUnlock()
	if m.meshSessions == nil {
		return nil, false
	}
	var best aether.Session
	var bestGrade Grade
	for _, session := range m.meshSessions {
		if session == nil || session.IsClosed() {
			continue
		}
		g := SessionGrade(session)
		if best == nil || g > bestGrade {
			best = session
			bestGrade = g
		}
	}
	if best == nil {
		return nil, false
	}
	return best, true
}

// ────────────────────────────────────────────────────────────────────────────
// Multipath Management
// ────────────────────────────────────────────────────────────────────────────

// addMultipathSession registers an Aether session with the per-peer multipath manager.
func (m *ConnectionManager) addMultipathSession(nodeID string, session aether.Session, proto Protocol) {
	// Read the peer's region under the connection manager lock so the
	// multipath path metadata can classify same-region vs cross-region
	// vs inter-continental for quality score normalisation. Without
	// this, every cross-region path would score the same as a same-
	// region one and the score would lose its physical-distance signal.
	m.mu.Lock()
	var remoteRegion string
	if p, ok := m.peers[nodeID]; ok {
		remoteRegion = p.peerRegion
	}
	m.mu.Unlock()

	m.multipathMu.Lock()
	defer m.multipathMu.Unlock()
	if m.multipathManagers == nil {
		m.multipathManagers = make(map[string]*multipath.Manager)
	}
	mgr, ok := m.multipathManagers[nodeID]
	if !ok {
		mgr = multipath.NewManager()
		// Level 2: duplicate REALTIME frames across all paths so
		// interactive traffic is resilient to single-path failure
		// without an RTO-wait.
		mgr.EnableRedundantRealtime()
		// Level 3: weighted-deficit round-robin distributes non-
		// REALTIME traffic across paths proportional to each path's
		// quality score. Path scores are updated from gossip RTT /
		// loss measurements via RecordMultipathStats below.
		mgr.EnableWeightedLoadBalance()

		// Set the multipath probe cadence BEFORE EnsureK starts the
		// probe loop. EnsureK's first call invokes
		// q.probeOnce.Do(go m.RunProbeLoop(ctx)) which snapshots
		// probeInterval at start; setting it after has no effect on
		// the running ticker. 5s beats the worst-case UDP idle close
		// (kernel UDP conntrack ~10s for unreplied flows / Fly
		// cross-region NAT mapping reap ~13s) with a small safety
		// margin, so noise-UDP standbys actually receive PING traffic
		// before their underlying conn is reaped. Without this, every
		// non-primary noise-UDP path dies with the misleading
		// "stalled=13s" reporting line and EnsureK redials in a
		// churn loop that dominates fleet instability.
		mgr.SetProbeInterval(5 * time.Second)

		// Bind the shared quality.Tracker and wire the EnsureK
		// reconnect loop's DialFunc. Target K comes from
		// ConnectionScaler.TargetConnections (latency × grade ×
		// failure × traffic-weight × hotspot-adjustment formula).
		// The dial callback picks the highest-grade protocol not
		// already represented in the manager, respects dial cooldown,
		// and reuses dialWithProtocol so the dial path is identical
		// to scanAndConnect's. scanAndConnect handles the first
		// session per peer; EnsureK keeps K total sessions alive.
		// EnsureK's first call also spawns RunProbeLoop via
		// q.probeOnce.Do — see quality_aware.go probeOnce. We rely
		// on that single launch and do NOT start a second probe
		// goroutine here.
		if m.qualityTracker != nil {
			m.installEnsureKDialFn(mgr, nodeID)
		}

		m.multipathManagers[nodeID] = mgr
	}

	// AddPathQA records the path with region metadata for score
	// classification. AddPath is still called internally so the legacy
	// integer-Quality scheduler (WDRR) keeps working for callers that
	// haven't migrated.
	mgr.AddPathQA(session, mapProtocol(proto), remoteRegion)

	// [MULTIPATH] high-signal log: every path register reveals when a
	// peer becomes multi-pathed (e.g. ws + noise-udp) — paired with the
	// existing failover log this gives full multipath state lifecycle.
	log.Printf("[MULTIPATH] add peer=%s proto=%s grade=%s pathCount=%d remoteRegion=%s",
		truncID(nodeID), proto, GradeForProtocol(proto), mgr.PathCount(), remoteRegion)
}

// RecordMultipathStats feeds per-path RTT + loss into the multipath
// manager's weighted scheduler. Called from the gossip exchange
// callback (which already measures per-session RTT) so the L3 WDRR
// weights stay current. Loss is inferred from session metrics —
// derivation lives in the caller since Aether's `Session.Metrics()`
// doesn't expose a direct loss rate, so we pass 0.0 when unavailable
// and let Quality + RTT drive the weight.
func (m *ConnectionManager) RecordMultipathStats(nodeID string, session aether.Session, rtt time.Duration, loss float64) {
	m.multipathMu.Lock()
	mgr, ok := m.multipathManagers[nodeID]
	m.multipathMu.Unlock()
	if !ok || mgr == nil {
		return
	}
	mgr.RecordPathStats(session, rtt, loss)
}

// removeMultipathSession removes an Aether session from the per-peer multipath manager.
func (m *ConnectionManager) removeMultipathSession(nodeID string, session aether.Session) {
	m.multipathMu.Lock()
	defer m.multipathMu.Unlock()
	if m.multipathManagers == nil {
		return
	}
	mgr, ok := m.multipathManagers[nodeID]
	if !ok {
		return
	}
	mgr.RemovePath(session)
	if mgr.PathCount() == 0 {
		// Stop the EnsureK goroutine before dropping the manager.
		// Without this, the goroutine survives and keeps calling
		// DialFunc forever — every successive accept then creates a
		// new manager with its own goroutine, and the orphaned ones
		// keep dialing through the still-alive ConnectionManager,
		// which routes the resulting sessions back into the *live*
		// manager. The result is a runaway dial storm where every
		// disconnect/reconnect cycle adds another orphan loop.
		mgr.Stop()
		delete(m.multipathManagers, nodeID)
	}
}

// GetMultipathManager returns the multipath manager for a peer (if it has multiple paths).
func (m *ConnectionManager) GetMultipathManager(nodeID string) (*multipath.Manager, bool) {
	m.multipathMu.Lock()
	defer m.multipathMu.Unlock()
	if m.multipathManagers == nil {
		return nil, false
	}
	mgr, ok := m.multipathManagers[nodeID]
	return mgr, ok
}

// MeshSessionCount returns the number of active Aether sessions.
func (m *ConnectionManager) MeshSessionCount() int {
	m.dispatchMu.RLock()
	defer m.dispatchMu.RUnlock()
	if m.meshSessions == nil {
		return 0
	}
	count := 0
	for _, s := range m.meshSessions {
		if !s.IsClosed() {
			count++
		}
	}
	return count
}

// MeshSessionMetrics holds per-session monitoring data for the mesh-debug endpoint.
type MeshSessionMetrics struct {
	NodeID        string        `json:"nodeID"`
	Protocol      string        `json:"protocol"`
	RTT           time.Duration `json:"rtt"`
	CWND          int64         `json:"cwnd"`
	ActiveStreams int           `json:"activeStreams"`
	BytesSent     uint64        `json:"bytesSent"`
	BytesRecv     uint64        `json:"bytesRecv"`
	IsAether      bool          `json:"isAether"`
}

// MeshMetrics returns monitoring data for all active Aether sessions.
// Used by mesh-debug and monitoring endpoints for observability.
func (m *ConnectionManager) SessionMetrics() []MeshSessionMetrics {
	m.dispatchMu.RLock()
	defer m.dispatchMu.RUnlock()
	if m.meshSessions == nil {
		return nil
	}

	metrics := make([]MeshSessionMetrics, 0, len(m.meshSessions))
	for nodeID, session := range m.meshSessions {
		if session.IsClosed() {
			continue
		}
		sm := session.Metrics()
		metrics = append(metrics, MeshSessionMetrics{
			NodeID:        nodeID,
			Protocol:      session.Protocol().String(),
			RTT:           sm.RTT,
			CWND:          sm.CWND,
			ActiveStreams: sm.ActiveStreams,
			BytesSent:     sm.BytesSent,
			BytesRecv:     sm.BytesRecv,
			IsAether:      true,
		})
	}
	return metrics
}
