/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"context"
	"log"
	"net"
	"sync/atomic"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/adapter"
)

// Mesh session-establishment counters, exposed as mesh_initiator_sessions /
// mesh_responder_sessions. They exist to make the resumption gap documented at
// DialAndAcceptMesh a RUNTIME fact rather than a source reading: every initiator
// session counted here was established by wrapping an already-dialled net.Conn,
// which is precisely the path that cannot reach aether's tryResumeDial.
//
// ⚠ Deliberately NOT a mesh_resume_attempted counter. Nothing on this path can
// increment one, so it would read 0 by construction — a tautology that looks
// like evidence. These two count something that can come out either way, and
// the bypass is established by the capability census in runtime.go plus the
// fact that SetupMeshSession below has no resume call in it at all.
var meshInitiatorSessions, meshResponderSessions uint64

// SetSessionOptions overrides the Aether SessionOptions used by every
// subsequent call to SetupMeshSession. Call before the first peer session
// is created so tuning (idle timeout, max streams, congestion algo, etc.)
// reaches production; sessions already in flight are not affected.
func (rt *Runtime) SetSessionOptions(opts aether.SessionOptions) {
	rt.sessionOpts = opts
}

// SessionOptions returns the current Aether SessionOptions snapshot.
func (rt *Runtime) SessionOptions() aether.SessionOptions {
	return rt.sessionOpts
}

// SetupMeshSession creates the appropriate transport adapter for the
// connection. Aether has no version negotiation — there is only one wire
// protocol and identity is established by the underlying crypto handshake
// (Noise XX/XK, QUIC TLS, WS Ed25519 header, gRPC metadata).
//
// Uses the options-accepting factory so runtime-level tuning (idle timeout,
// max concurrent streams, FEC/compression toggles, congestion algo, frame
// logging) reaches the adapter. Override via Runtime.SetSessionOptions
// before the first session is established.
func (rt *Runtime) SetupMeshSession(ctx context.Context, conn net.Conn, nodeID string,
	proto Protocol, isInitiator bool) (aether.Session, error) {

	// Map node Protocol to aether.Protocol
	transportProto := mapProtocol(proto)

	// Create transport-appropriate adapter
	session, err := adapter.NewSessionForProtocol(conn, transportProto,
		rt.identity.NodeID, aether.NodeID(nodeID), rt.sessionOpts)
	if err != nil {
		return nil, err
	}

	// Count AFTER the adapter succeeds, so the gauge measures sessions actually
	// established rather than attempts — a failed setup returns above and must
	// not inflate the denominator the resumption reading depends on.
	if isInitiator {
		atomic.AddUint64(&meshInitiatorSessions, 1)
	} else {
		atomic.AddUint64(&meshResponderSessions, 1)
	}

	dbgNodeIdentity.Printf("Aether session: local=%s remote=%s proto=%s initiator=%v",
		rt.identity.NodeID.Short(), truncID(nodeID), proto, isInitiator)
	log.Printf("[AETHER] Session created: nodeID=%s, proto=%s, initiator=%v",
		truncID(nodeID), proto, isInitiator)

	return session, nil
}

// AcceptIncomingMeshSession handles an incoming transport session by creating
// the Aether adapter and registering it with the connection manager.
//
// serviceName, region, and roles are optional: supply them when the caller
// learned the peer's advertised identity during transport setup (e.g., HTTP
// headers on a WS upgrade carry X-VL1-Region / X-VL1-Service-Name /
// X-VL1-Roles). When region is non-empty it populates peer.peerRegion at
// accept-time so the role-affinity sameRegion tier and cross-region budget
// are evaluated correctly on the very next Rebalance tick; otherwise the
// receiver waits up to a scanAndConnect cycle (20s) for the next swarm
// PeerRecord to fill it in.
func (rt *Runtime) AcceptIncomingMeshSession(ctx context.Context, conn net.Conn,
	nodeID string, proto Protocol, bootstrapHost string, serviceName, region, roles string) {

	dbgNodeTLS.Printf("incoming Aether session: node=%s proto=%s", truncID(nodeID), proto)
	session, err := rt.SetupMeshSession(ctx, conn, nodeID, proto, false)
	if err != nil {
		dbgNodeTLS.Printf("Aether setup failed for %s: %v", truncID(nodeID), err)
		log.Printf("[AETHER] Setup failed for %s: %v", truncID(nodeID), err)
		conn.Close()
		return
	}

	if rt.connMgr != nil {
		rt.connMgr.AcceptMeshConnection(ctx, AcceptMeshConnectionOpts{
			Session:       session,
			NodeID:        nodeID,
			Region:        region,
			Proto:         proto,
			IsInitiator:   false,
			BootstrapHost: bootstrapHost,
			ServiceName:   serviceName,
			Roles:         roles,
		})
	}
}

// DialAndAcceptMesh dials a peer and starts the connection lifecycle.
// Used by connectPeer in the ConnectionManager.
//
// serviceName and roles are optional: supply when known from reach record
// attrs or a bootstrap handshake response; drives the pre-publish of the
// peer's Member record so it doesn't appear as "(unresolved)" while its own
// gossip catches up.
func (rt *Runtime) DialAndAcceptMesh(ctx context.Context, conn net.Conn,
	nodeID string, region string, proto Protocol, bootstrapHost string, serviceName, roles string) {

	session, err := rt.SetupMeshSession(ctx, conn, nodeID, proto, true)
	if err != nil {
		log.Printf("[AETHER] Dial setup failed for %s: %v", truncID(nodeID), err)
		conn.Close()
		// MESH-B04: connectPeer set the peer to PeerConnecting before this async
		// handoff; on setup failure reset it so scanAndConnect re-dials rather
		// than leaving it wedged in PeerConnecting forever.
		if rt.connMgr != nil {
			rt.connMgr.resetConnectingState(nodeID)
		}
		return
	}

	// SESSION RESUMPTION: the ordinary noise-UDP mesh dial DOES attempt aether's
	// 0.5-RTT resume, contrary to what the TODO that used to sit here implied.
	// The chain is dialWithProtocol -> Runtime.dialNoiseUDP (runtime.go) ->
	// tr.Dial -> NoiseTransport.Dial (aether/noise/transport.go:424), and
	// tryResumeDial lives inside that function at :462. So by the time a conn
	// reaches DialAndAcceptMesh the resume attempt has already happened or been
	// declined upstream.
	//
	// ⚠ THE TRAP THAT MAKES THIS EASY TO GET BACKWARDS, recorded because I did:
	// this function's parameter is a net.Conn and the call sites pass
	// baseConn.Conn, which READS like a raw socket dialled locally. It is not —
	// it is the connection returned by the resume-capable transport dial, with
	// the aether.BaseConnection wrapper stripped off. Tracing SetupMeshSession
	// downward shows no ticket lookup and invites the conclusion that resumption
	// is unreachable from the mesh; the conclusion is wrong because the dial
	// happened one layer UP, before the handoff. Follow a conn parameter back to
	// its origin before concluding anything about what its dial did or did not do.
	//
	// ALL FOUR mesh dial sites are now traced, and every one of them reaches the
	// resume-capable dial for noise-UDP. The earlier note here left the last two
	// open; they are closed, and they close against the original claim rather
	// than for it:
	//
	//   peer_connections.go:2940  <- dialWithProtocol
	//   multipath_dial.go:345     <- dialWithProtocol
	//   upgrade_walker.go:457     <- dialWithProtocol (assigned at :398)
	//   holepunch.go:317          <- nat.ExecutePunch, whose dial closure at
	//                                holepunch.go:283-285 IS rt.dialNoiseUDP
	//
	// The hole-punch case is the one that looked most likely to be a genuine
	// bypass — it hands over a NAT-punched socket — and it is not: the punch
	// coordinator is handed a dial closure that routes through the same
	// transport. Every path converges on dialNoiseUDP -> tr.Dial ->
	// NoiseTransport.Dial, takes its resume attempt there, and then hands the
	// unwrapped conn down here to be re-wrapped as the mesh-level session.
	//
	// The prior comment here claimed wiring "needs unification of AetherSession
	// and aether.Session" plus ticket storage and reconnect-path passing. That
	// part was genuinely stale and is worth keeping noted: AetherSession exists
	// nowhere in loom/ or aether/ except in that sentence, and reconnect-path
	// ticket passing is already implemented in aether. A TODO that overstates its
	// own blockers reads as a considered estimate and stops anyone re-measuring.

	if rt.connMgr != nil {
		rt.connMgr.AcceptMeshConnection(ctx, AcceptMeshConnectionOpts{
			Session:       session,
			NodeID:        nodeID,
			Region:        region,
			Proto:         proto,
			IsInitiator:   true,
			BootstrapHost: bootstrapHost,
			ServiceName:   serviceName,
			Roles:         roles,
		})
	}
}

// mapProtocol converts node.Protocol to aether.Protocol.
func mapProtocol(p Protocol) aether.Protocol {
	switch p {
	case ProtoNoiseUDP:
		return aether.ProtoNoise
	case ProtoQUIC:
		return aether.ProtoQUIC
	case ProtoWebSocket:
		return aether.ProtoWebSocket
	case ProtoGRPC:
		return aether.ProtoGRPC
	case ProtoTLS:
		return aether.ProtoWebSocket // TLS uses same adapter as WS (both reliable stream)
	default:
		return aether.ProtoUnknown
	}
}

