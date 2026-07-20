/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"context"
	"fmt"
	"log"

	"github.com/ORBTR/aether/rpc/pb"
	"github.com/ORBTR/aether"
)

// ServeMeshStream serves incoming RPC requests on an Aether stream.
// Wire format: pb.RPCRequest/RPCResponse (protobuf binary encoding).
func (s *RPCServer) ServeMeshStream(ctx context.Context, stream aether.Stream, remoteID aether.NodeID) {
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
			return
		}
	}
}

// MESH-D09: ServeMeshStreamBidirectional was removed. It had no callers, and as
// written it was broken as a bidirectional server — no request/response
// correlation (any inbound response was logged "Unrecognized message" and
// dropped, the exact order-based hazard the review flagged), no panic recovery,
// and it ignored Marshal/Send errors. The correlated bidirectional path is
// BidiRPC (bidi_rpc.go), which every production caller already uses.

// ErrAetherHandlerNotFound is returned when no handler is registered for a method.
var ErrAetherHandlerNotFound = fmt.Errorf("aether rpc: handler not found")
