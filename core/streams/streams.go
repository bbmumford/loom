/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package streams

import "github.com/ORBTR/aether"

// Well-known stream IDs for HSTLES/ORBTR mesh nodes.
// These are application-level conventions, not Aether protocol requirements.
// Other Aether consumers (Hospitium, Mercury) define their own stream IDs.
const (
	StreamGossip    uint64 = 0 // LAD gossip exchange (G1 delta + G3 rumor ACK)
	StreamRPC       uint64 = 1 // Bidirectional RPC dispatch
	StreamKeepalive uint64 = 2 // Ping/pong connection liveness
	StreamControl   uint64 = 3 // Handshake, key rotation, migration
	// StreamReconcile carries G4 IBLT reconciliation and G5 snapshot
	// frames on a stream separate from G1/G3 gossip. Previously G4
	// shared StreamGossip with magic-byte multiplexing — when a G4
	// initiator round timed out, its abandoned background read could
	// pull the peer's late-arriving G4 response and discard it; the
	// NEXT path of execution was G1 fallback on the same stream, and
	// G1's blocking io.ReadFull would receive the residual G4 bytes
	// from the stream's recvCh and decode the magic as 0x4734 ("G4")
	// instead of 0x4731 ("G1") — surfacing as the chronic-flap
	// "invalid magic" error and closing the session. With G4 on its
	// own stream an abandoned G4 read can't poison G1's read path:
	// each protocol owns its frame queue.
	StreamReconcile uint64 = 4
)

// MeshStreamLayout returns the Aether StreamLayout for HSTLES mesh connections.
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

// DefaultLatencyClass returns the scheduling priority class for HSTLES streams.
func DefaultLatencyClass(streamID uint64) aether.LatencyClass {
	switch streamID {
	case StreamKeepalive, StreamControl:
		return aether.ClassREALTIME
	case 102, 500: // screen input, emergency policy
		return aether.ClassREALTIME
	case StreamRPC:
		return aether.ClassINTERACTIVE
	case 100, 200, 201, 202, 203, 204, 205, 501:
		return aether.ClassINTERACTIVE
	default:
		return aether.ClassBULK
	}
}
