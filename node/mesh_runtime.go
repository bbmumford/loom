/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"context"
	"log"
	"net"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/adapter"
)

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

	// TODO: wire session resumption end-to-end. The ticket infrastructure
	// (TicketCapable interface, TicketStore, IssueTicket/ResumeSession in
	// noise/) already exists, but full wiring needs unification of
	// AetherSession and aether.Session, FilePeerStore ticket storage, and
	// reconnect-path ticket passing before it can be enabled.

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

