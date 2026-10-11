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
			Partition:     transportPartitionOf(conn),
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
// ownsConnectingState distinguishes the one caller that put the peer into
// PeerConnecting from the three that did not. connectPeer sets that state
// immediately before handing off here, so only its dial may clear it on
// failure; the upgrade walker, hole puncher and multipath dialer all reach
// this function for peers whose connecting state, if any, belongs to a
// concurrent connectPeer.
type ownsConnectingState bool

const (
	dialOwnsConnectingState    ownsConnectingState = true
	dialBorrowsConnectingState ownsConnectingState = false
)

func (rt *Runtime) DialAndAcceptMesh(ctx context.Context, conn net.Conn,
	nodeID string, region string, proto Protocol, bootstrapHost string, serviceName, roles string,
	owns ownsConnectingState) {

	session, err := rt.SetupMeshSession(ctx, conn, nodeID, proto, true)
	if err != nil {
		log.Printf("[AETHER] Dial setup failed for %s: %v", truncID(nodeID), err)
		conn.Close()
		// connectPeer set the peer to PeerConnecting before this async handoff;
		// on setup failure reset it so scanAndConnect re-dials rather than
		// leaving it wedged in PeerConnecting forever.
		//
		// Gated on the dial claim because resetConnectingState checks the STATE, not
		// who established it. A walker probe, hole punch or multipath dial that
		// fails while connectPeer has concurrently marked the same peer
		// PeerConnecting would otherwise clear a live connect attempt's state,
		// and scanAndConnect would re-dial a peer already being dialled.
		if rt.connMgr != nil {
			// A completed transport dial that then fails its mesh handshake is
			// still a failed path, and until this it recorded nothing. The dial
			// sites credit recordDialSuccess the moment dialWithProtocol returns
			// a conn (connectPeer and dialAdditionalPath both do), which clears
			// the per-(peer, protocol) cooldown ladder. With no counterpart on
			// this branch a peer that accepts connections but never completes a
			// session had its ladder cleared on every attempt and was re-dialled
			// at the base delay forever, so the back-off could not escalate.
			//
			// Recorded for every caller, not only the owning one: the handshake
			// failed on this protocol to this peer whoever initiated it, and the
			// ladder is keyed by that pair rather than by the dialer.
			rt.connMgr.recordDialFailure(nodeID, proto)

			if bool(owns) {
				rt.connMgr.resetConnectingState(nodeID)
			}
		}
		return
	}

	// SESSION RESUMPTION: the ordinary noise-UDP mesh dial DOES attempt aether's
	// 0.5-RTT resume, so no resume call belongs here.
	// The chain is dialWithProtocol -> Runtime.dialNoiseUDP (runtime.go) ->
	// tr.Dial -> aether's NoiseTransport.Dial, which runs tryResumeDial. So by
	// the time a conn reaches DialAndAcceptMesh the resume attempt has already
	// happened or been declined upstream.
	//
	// ⚠ THIS FUNCTION'S PARAMETER IS A net.Conn and the call sites pass
	// baseConn.Conn, which READS like a raw socket dialled locally. It is not —
	// it is the connection returned by the resume-capable transport dial, with
	// the aether.BaseConnection wrapper stripped off. Tracing SetupMeshSession
	// downward shows no ticket lookup because the dial happened one layer UP,
	// before the handoff, so a conn parameter has to be followed back to its
	// origin to say what its dial did.
	//
	// Every caller of this function reaches the resume-capable dial for
	// noise-UDP first:
	//
	//   ConnectionManager.connectPeer        <- dialWithProtocol
	//   ConnectionManager.dialAdditionalPath <- dialWithProtocol
	//   ConnectionManager.probeUpgrade       <- dialWithProtocol
	//   ConnectionManager.attemptHolePunch   <- nat.ExecutePunch, whose dial
	//                                           closure IS rt.dialNoiseUDP
	//
	// The hole-punch case hands over a NAT-punched socket, and it still takes
	// the resume attempt: the punch coordinator is handed a dial closure that
	// routes through the same transport. Each of these paths converges on
	// dialNoiseUDP -> tr.Dial -> NoiseTransport.Dial, takes its resume attempt
	// there, and then hands the unwrapped conn down here to be re-wrapped as
	// the mesh-level session.
	//
	// Two more sites dial noise-UDP through that chain, so they take the resume
	// attempt as well, but they return the connection to their own caller and
	// never reach this function: TopologyRouter.dialDirect, through
	// dialWithProtocol, and Runtime.DialByProtocol, through dialNoiseUDP.

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
			Partition:     transportPartitionOf(conn),
		})
	}
}

// mapProtocol converts node.Protocol to aether.Protocol.
// mapProtocol selects the aether transport adapter for a node protocol.
//
// 🔴 LOSSY BY DESIGN — DO NOT KEY PER-PROTOCOL STATE ON THE RESULT. ProtoTLS
// and ProtoWebSocket both return aether.ProtoWebSocket because they share one
// reliable-stream adapter, so the output cannot distinguish them. Used as a map
// key it merges two dial paths into one bucket and a failure on either
// suppresses both. Where a key is wanted, use the node Protocol directly —
// dialKeyFor in multipath_dial.go is the shape to copy.
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
