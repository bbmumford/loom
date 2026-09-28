/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package streams

import "github.com/ORBTR/aether"

// Well-known stream IDs for loom mesh nodes.
// These are application-level conventions, not Aether protocol requirements.
// Other Aether consumers define their own stream IDs.
const (
	StreamGossip    uint64 = 0 // LAD gossip exchange (G1 delta + G3 rumor ACK)
	StreamRPC       uint64 = 1 // Bidirectional RPC dispatch
	StreamKeepalive uint64 = 2 // Ping/pong connection liveness
	StreamControl   uint64 = 3 // Handshake, key rotation, migration
	// StreamReconcile carries G4 IBLT reconciliation and G5 snapshot
	// frames on a stream separate from G1/G3 gossip. Sharing
	// StreamGossip with magic-byte multiplexing breaks when a G4
	// initiator round times out: its abandoned background read can
	// pull the peer's late-arriving G4 response and discard it; the
	// NEXT path of execution is G1 fallback on the same stream, and
	// G1's blocking io.ReadFull would receive the residual G4 bytes
	// from the stream's recvCh and decode the magic as 0x4734 ("G4")
	// instead of 0x4731 ("G1") — an "invalid magic" error that closes
	// the session. With G4 on its own stream an abandoned G4 read
	// can't poison G1's read path: each protocol owns its frame queue.
	StreamReconcile uint64 = 4

	// ApplicationBand is the first ID of aether's application band: its
	// StreamConfig contract puts application streams at IDs >= 100. Every
	// ID from here up is claimed by ID; dynamic RPC streams (pkg/dispatch)
	// stay below it, and the dynamic accept loop refuses an unclaimed stream
	// at or above it.
	ApplicationBand uint64 = 100

	// Application streams bound per peer pair. Each is opened by the lower
	// NodeID and claimed by the higher with AcceptStreamByID, so both ends
	// must agree on the number: a change here is a wire change.

	// StreamSwarm carries the swarm protocol (node/swarm_transport.go
	// RegisterPeer and swarmStreamConfig).
	StreamSwarm uint64 = 100
	// StreamMeshDB carries the mesh database transport
	// (node/meshdb_transport.go): one yamux session per peer pair, whose
	// streams carry every tursoraft Raft cluster the two share. pkg/dispatch
	// draws dynamic RPC stream IDs from [10, 100), so no RPC opens on it.
	StreamMeshDB uint64 = 101
)

// MeshStreamLayout returns the Aether StreamLayout for loom mesh connections.
// Sets RPC=StreamRPC so the reliability engine emits immediate ACKs on
// request/response frames instead of waiting for the delayed-ACK timer —
// removes the back-to-back-stall on chatty bidirpc workloads.
func MeshStreamLayout() aether.StreamLayout {
	return aether.StreamLayout{
		Keepalive: StreamKeepalive,
		Control:   StreamControl,
		RPC:       StreamRPC,
	}
}

// DefaultLatencyClass returns the scheduling priority class for loom mesh streams.
func DefaultLatencyClass(streamID uint64) aether.LatencyClass {
	switch streamID {
	case StreamKeepalive, StreamControl:
		return aether.ClassREALTIME
	case 102, 500: // screen input, emergency policy
		return aether.ClassREALTIME
	case StreamRPC:
		return aether.ClassINTERACTIVE
	case StreamSwarm, StreamMeshDB, 200, 201, 202, 203, 204, 205, 501:
		return aether.ClassINTERACTIVE
	default:
		return aether.ClassBULK
	}
}
