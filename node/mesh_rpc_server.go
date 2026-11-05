/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"context"
	"fmt"
	"log"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/rpc/pb"
	"github.com/bbmumford/loom/core/streams"
)

// ServeMeshStream serves incoming RPC requests on an Aether stream.
// Wire format: pb.RPCRequest/RPCResponse (protobuf binary encoding).
func (s *RPCServer) ServeMeshStream(ctx context.Context, stream aether.Stream, remoteID aether.NodeID) {
	// Scope this peer's dedup entries to itself. Without a caller
	// identity every request on this stream shares one process-global
	// namespace with every other peer's, keyed on a wire-supplied id.
	ctx = withCallerNode(ctx, string(remoteID))
	dbgRPC.Printf("serving Aether stream %d from %s", stream.StreamID(), remoteID.Short())
	s.logger.Printf("[RPC-AETHER] Serving stream %d from %s", stream.StreamID(), remoteID.Short())

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Read request bytes from Aether stream
		reqBytes, err := stream.Receive(ctx)
		if err != nil {
			log.Printf("[RPC-AETHER] Stream %d closed: %v", stream.StreamID(), err)
			return
		}

		// Unmarshal protobuf request
		req, err := pb.UnmarshalRequest(reqBytes)
		if err != nil {
			s.logger.Printf("[RPC-AETHER] Unmarshal error from %s: %v", remoteID.Short(), err)
			continue
		}

		// Dispatch to handler with panic recovery (prevents process crash)
		dbgRPC.Printf("Aether request from %s: handler=%s", remoteID.Short(), req.Handler)
		var resp *pb.RPCResponse
		func() {
			defer func() {
				if r := recover(); r != nil {
					log.Printf("[RPC-AETHER] handler panic for %s: %v", req.Handler, r)
					resp = &pb.RPCResponse{
						Id:      req.Id,
						Success: false,
						Error:   fmt.Sprintf("handler panic: %v", r),
					}
				}
			}()
			resp = s.handleRequest(ctx, req)
		}()

		// Marshal protobuf response
		respBytes, err := pb.MarshalResponse(resp)
		if err != nil {
			s.logger.Printf("[RPC-AETHER] Marshal response error: %v", err)
			continue
		}

		// Send response on Aether stream
		if err := stream.Send(ctx, respBytes); err != nil {
			s.logger.Printf("[RPC-AETHER] Send response error on stream %d: %v", stream.StreamID(), err)
			// Nothing reads this stream after the return, and its unread messages
			// would hold the opener's connection credit until aether's idle sweep.
			_ = stream.Reset(aether.ResetCancel)
			return
		}
	}
}

// serveUnclaimedStream serves a stream that reached the session's dynamic
// accept loop, which receives every stream no consumer claimed by ID.
//
// Only IDs from streams.DynamicRPCBase up to streams.ApplicationBand carry RPC:
// pkg/dispatch opens dynamic RPC streams there. An application-band stream
// (StreamSwarm, StreamMeshDB and the like) lands here when its consumer does not run
// on this node or had not armed AcceptStreamByID within aether's backlog grace. A
// setup stream lands here when the peer reopens it after a typed reset
// (streamReopener) while this node's consumer is still reading what the old
// generation's queue holds. Served as RPC, the first frame of either would be parsed
// as a request. A reset answers it, so the opener's consumer fails at once and
// opens again. The handler-stream band is the one application band served here
// (serveHandlerStream).
func (s *RPCServer) serveUnclaimedStream(ctx context.Context, stream aether.Stream, remoteID aether.NodeID) {
	id := stream.StreamID()
	if id >= streams.StreamHandlerBase && id <= streams.StreamHandlerLast {
		s.serveHandlerStream(ctx, stream, remoteID)
		return
	}
	if id >= streams.ApplicationBand || id < streams.DynamicRPCBase {
		log.Printf("[RPC-AETHER] refused unclaimed stream %d from %s", id, remoteID.Short())
		_ = stream.Reset(aether.ResetRefused)
		return
	}
	s.ServeMeshStream(ctx, stream, remoteID)
}

// ServeMeshStream is one-directional. A bidirectional server on a mesh
// stream needs request/response correlation (an uncorrelated one logs an
// inbound response as "Unrecognized message" and drops it), panic recovery,
// and Marshal/Send error handling; BidiRPC (bidi_rpc.go) provides all three
// and is the bidirectional path every production caller uses.

// ErrAetherHandlerNotFound is returned when no handler is registered for a method.
var ErrAetherHandlerNotFound = fmt.Errorf("aether rpc: handler not found")
