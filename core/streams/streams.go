/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package streams

import "github.com/ORBTR/aether"

// RegistryVersion is the version of the assignments in this file. Both ends
// of a session read this table to decide what arrives on an ID, so adding,
// moving or dropping an assignment is a wire change and raises this number:
// two peers reading different versions do not agree on what an ID carries.
// It is the streams.registry entry of every mesh session's wire version offer
// (WireVersions), so two such peers refuse the session in its handshake.
const RegistryVersion uint16 = 2

// WireVersionRegistry names this table in a session's wire version offer.
const WireVersionRegistry = "streams.registry"

// WireVersions returns the entry this package adds to a mesh session's wire
// version offer: streams.registry at RegistryVersion, required, because a
// session whose two ends read different tables cannot agree on what any
// stream carries. node's meshWireVersions adds it to the Noise transports'
// and the byte-stream sessions' options.
func WireVersions() aether.WireVersions {
	return aether.WireVersions{{Name: WireVersionRegistry, Versions: []uint16{RegistryVersion}, Required: true}}
}

// Setup streams. node/mesh_connection.go opens 0-4 on the initiator and
// claims the same numbers with AcceptStreamByID on the responder, so both
// ends must agree on every one.
const (
	StreamGossip    uint64 = 0 // gossip: G1 exchange, G2 digest probe, GB bucket digest, G3 rumor, G5 rumor ACK
	StreamRPC       uint64 = 1 // Bidirectional RPC dispatch
	StreamKeepalive uint64 = 2 // Ping/pong connection liveness
	StreamControl   uint64 = 3 // Handshake, key rotation, migration
	// StreamReconcile carries G4 set-difference reconciliation and G6
	// snapshot frames on a stream separate from the gossip stream. Sharing
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
)

// DynamicRPCBase is the first ID pkg/dispatch draws a per-call RPC stream
// from; the band runs up to but not into ApplicationBand.
// pkg/dispatch/streamid.go reads both bounds from here, so the band is
// declared once and the allocator carries no stream number of its own.
const DynamicRPCBase uint64 = 10

// ApplicationBand is the first ID of aether's application band: its
// StreamConfig contract puts application streams at IDs >= 100. Every
// ID from here up is claimed by ID; dynamic RPC streams (pkg/dispatch)
// stay below it, and the dynamic accept loop refuses an unclaimed stream
// at or above it.
const ApplicationBand uint64 = 100

// Streams bound per peer pair. Each is opened by the lower NodeID and
// claimed by the higher with AcceptStreamByID, so both ends must agree on
// the number: a change here is a wire change.
const (
	// StreamSwarm carries the swarm protocol (node/swarm_transport.go
	// RegisterPeer and swarmStreamConfig).
	StreamSwarm uint64 = 100
	// StreamMeshDB carries the mesh database transport
	// (node/meshdb_transport.go): one yamux session per peer pair, whose
	// streams carry every tursoraft Raft cluster the two share. pkg/dispatch
	// draws dynamic RPC stream IDs from [DynamicRPCBase, ApplicationBand), so
	// no RPC opens on it.
	StreamMeshDB uint64 = 101
)

// Application bands. Each is the first ID of a contiguous span whose last ID
// is named beside it, so a consumer adding a stream inside a band takes the
// next free ID in that band and never reaches into the next one.
const (
	// StreamTunnelBase is the first tunnel stream; the side that opens a tunnel numbers
	// each further tunnel upward from it, up to StreamTunnelLast.
	StreamTunnelBase uint64 = 200
	StreamTunnelLast uint64 = 299

	// StreamFileTransferBase is the first file-transfer stream; the side that opens a
	// transfer numbers each further transfer upward from it.
	StreamFileTransferBase uint64 = 300
	StreamFileTransferLast uint64 = 399

	// StreamTelemetry carries the device's metrics, events and heartbeat.
	StreamTelemetry uint64 = 400

	// StreamPolicyPush carries policy updates to the device and
	// StreamPolicyAck its acknowledgements.
	StreamPolicyPush uint64 = 500
	StreamPolicyAck  uint64 = 501

	// StreamDynamicBase is the first dynamic peer stream: the agent-to-agent
	// virtual wire and the channels of a remote-access session. A device's
	// dynamic accept path admits this band and dispatches by ID, so a stream
	// below it is never handed to a dynamic handler.
	StreamDynamicBase uint64 = 600
	StreamDynamicLast uint64 = 699

	// StreamPlatformConversation, StreamPlatformCoordination and
	// StreamPlatformDispatch are the arms of the worker crossing: one session
	// per worker carries each as a distinct stream instead of a separate dial.
	// The worker opens all three and the edge's workeraccept claims each by ID.
	// Dispatch carries the frames an addressed push sends a named worker. IDs
	// above them in the 700 band are left for the worker-edge channels that
	// declare them.
	StreamPlatformConversation uint64 = 700
	StreamPlatformCoordination uint64 = 701
	StreamPlatformDispatch     uint64 = 702

	// StreamHandlerBase through StreamHandlerLast carry one handler stream
	// each (node.serveHandlerStream): an open frame naming the handler and the
	// caller's principal, then the handler's own frames both ways until either
	// end closes. Above the assigned bands so a consumer holding a long-lived
	// stream does not spend the short-lived RPC band.
	StreamHandlerBase uint64 = 1000
	StreamHandlerLast uint64 = 1999
)

// Channels of one remote-access session, numbered inside the dynamic peer
// band. They are the screen, terminal, file and web channels an operator's
// session opens; screen video, input and control are these three IDs and no
// others, so a device never binds a screen stream in the per-peer band.
const (
	StreamRemoteTerminal      uint64 = 600
	StreamRemoteFilesData     uint64 = 601
	StreamRemoteFilesControl  uint64 = 602
	StreamRemoteScreenVideo   uint64 = 603
	StreamRemoteScreenInput   uint64 = 604
	StreamRemoteScreenControl uint64 = 605
	StreamRemoteWeb           uint64 = 606

	// RemoteAccessDynamicFloor is the first ID a remote-access session
	// numbers an unnamed channel from, left clear of the named ones above so
	// a new named channel can be added without moving a live number.
	RemoteAccessDynamicFloor uint64 = 610
)

// Entry is one assignment: a single ID when First == Last, otherwise the
// inclusive band [First, Last]. Latency is the class DefaultLatencyClass
// answers for every ID in it, which an opener overrides for one stream by
// setting aether.StreamConfig.LatencyClass. OpenedBy names the side that
// opens the stream, because aether's createStream hands a duplicate OPEN the
// stream already open under that ID: a second opener lands on the first
// one's stream instead of its own.
type Entry struct {
	Name     string
	First    uint64
	Last     uint64
	Latency  aether.LatencyClass
	OpenedBy string
}

// Registry returns every assignment, lowest ID first. The slice is built per
// call so no caller can edit the table both ends of a session read.
func Registry() []Entry {
	return []Entry{
		{Name: "gossip", First: StreamGossip, Last: StreamGossip, Latency: aether.ClassBULK, OpenedBy: "session setup"},
		{Name: "rpc", First: StreamRPC, Last: StreamRPC, Latency: aether.ClassINTERACTIVE, OpenedBy: "session setup"},
		{Name: "keepalive", First: StreamKeepalive, Last: StreamKeepalive, Latency: aether.ClassREALTIME, OpenedBy: "session setup"},
		{Name: "control", First: StreamControl, Last: StreamControl, Latency: aether.ClassREALTIME, OpenedBy: "session setup"},
		{Name: "reconcile", First: StreamReconcile, Last: StreamReconcile, Latency: aether.ClassBULK, OpenedBy: "session setup"},
		{Name: "dynamic rpc", First: DynamicRPCBase, Last: ApplicationBand - 1, Latency: aether.ClassINTERACTIVE, OpenedBy: "the caller, one per call"},
		{Name: "swarm", First: StreamSwarm, Last: StreamSwarm, Latency: aether.ClassINTERACTIVE, OpenedBy: "the lower NodeID"},
		{Name: "mesh database", First: StreamMeshDB, Last: StreamMeshDB, Latency: aether.ClassINTERACTIVE, OpenedBy: "the lower NodeID"},
		{Name: "tunnel", First: StreamTunnelBase, Last: StreamTunnelLast, Latency: aether.ClassINTERACTIVE, OpenedBy: "the side that opens the tunnel"},
		{Name: "file transfer", First: StreamFileTransferBase, Last: StreamFileTransferLast, Latency: aether.ClassBULK, OpenedBy: "the side that opens the transfer"},
		{Name: "telemetry", First: StreamTelemetry, Last: StreamTelemetry, Latency: aether.ClassBULK, OpenedBy: "the device"},
		{Name: "policy push", First: StreamPolicyPush, Last: StreamPolicyPush, Latency: aether.ClassINTERACTIVE, OpenedBy: "the side that pushes policy"},
		{Name: "policy ack", First: StreamPolicyAck, Last: StreamPolicyAck, Latency: aether.ClassINTERACTIVE, OpenedBy: "the device"},
		{Name: "dynamic peer", First: StreamDynamicBase, Last: StreamDynamicLast, Latency: aether.ClassBULK, OpenedBy: "the dialler"},
		{Name: "platform conversation", First: StreamPlatformConversation, Last: StreamPlatformConversation, Latency: aether.ClassINTERACTIVE, OpenedBy: "the worker"},
		{Name: "platform coordination", First: StreamPlatformCoordination, Last: StreamPlatformCoordination, Latency: aether.ClassINTERACTIVE, OpenedBy: "the worker"},
		{Name: "platform dispatch", First: StreamPlatformDispatch, Last: StreamPlatformDispatch, Latency: aether.ClassINTERACTIVE, OpenedBy: "the worker"},
		{Name: "handler stream", First: StreamHandlerBase, Last: StreamHandlerLast, Latency: aether.ClassINTERACTIVE, OpenedBy: "the caller, one per stream"},
	}
}

// Channel is one named channel of a remote-access session. It has an ID
// inside the dynamic peer band rather than a band of its own, because the
// channels live only for one session and the device's dynamic accept path
// already admits that band.
type Channel struct {
	Name    string
	ID      uint64
	Latency aether.LatencyClass
}

// RemoteAccessChannels returns the named channels of a remote-access
// session, lowest ID first. A bridge or agent that opens one of these reads
// its ID here, so the two ends of a channel cannot disagree on the number.
func RemoteAccessChannels() []Channel {
	return []Channel{
		{Name: "terminal", ID: StreamRemoteTerminal, Latency: aether.ClassINTERACTIVE},
		{Name: "files-data", ID: StreamRemoteFilesData, Latency: aether.ClassBULK},
		{Name: "files-control", ID: StreamRemoteFilesControl, Latency: aether.ClassINTERACTIVE},
		{Name: "screen-video", ID: StreamRemoteScreenVideo, Latency: aether.ClassREALTIME},
		{Name: "screen-input", ID: StreamRemoteScreenInput, Latency: aether.ClassREALTIME},
		{Name: "screen-control", ID: StreamRemoteScreenControl, Latency: aether.ClassINTERACTIVE},
		{Name: "web", ID: StreamRemoteWeb, Latency: aether.ClassINTERACTIVE},
	}
}

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

// DefaultLatencyClass returns the scheduling class the registry assigns an
// ID. A remote-access channel's own class wins over the dynamic peer band it
// sits inside; an ID the registry assigns to nothing gets ClassBULK, so an
// unrecognised stream never outranks a declared one.
func DefaultLatencyClass(streamID uint64) aether.LatencyClass {
	for _, ch := range RemoteAccessChannels() {
		if ch.ID == streamID {
			return ch.Latency
		}
	}
	for _, e := range Registry() {
		if streamID >= e.First && streamID <= e.Last {
			return e.Latency
		}
	}
	return aether.ClassBULK
}
