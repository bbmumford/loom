/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package dispatch

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/rpc/pb"
	"github.com/bbmumford/loom/core/streams"
)

// NodeStream is one handler stream the caller opened to a named node: frames
// both ways until either end closes.
type NodeStream interface {
	Send(ctx context.Context, payload []byte) error
	Recv(ctx context.Context) ([]byte, error)
	Close() error
}

// NodeStreamer is a Caller that opens a handler stream to exactly one node.
// HWPCaller is one. Same contract as TargetedCaller: that node or an error
// wrapping ErrNoRouteToNode, never another node.
type NodeStreamer interface {
	OpenNodeStream(ctx context.Context, nodeID, role, handler string) (NodeStream, error)
}

// NodeSessionFinder is a SessionFinder that names the session it holds to one
// node. Optional beside SessionFinder, so a finder that cannot name one
// refuses every handler stream rather than breaking at build.
type NodeSessionFinder interface {
	MeshSessionForNode(nodeID string) (aether.Session, bool)
}

// handlerStreamIDs allocates IDs in the handler-stream band; collisions with a
// stream still open are answered by the session's OpenStream and retried.
var handlerStreamIDs atomic.Uint64

// handlerStreamAttempts bounds the IDs one open tries.
const handlerStreamAttempts = 8

func nextHandlerStreamID() uint64 {
	band := streams.StreamHandlerLast - streams.StreamHandlerBase + 1
	return streams.StreamHandlerBase + handlerStreamIDs.Add(1)%band
}

// OpenNodeStream opens a handler stream to nodeID, carrying the caller's sealed
// principal in its open frame, and returns once the far side's handler accepted
// it. A refused principal or scope comes back as the far side's sentence.
func (c *HWPCaller) OpenNodeStream(ctx context.Context, nodeID, role, handler string) (NodeStream, error) {
	if c == nil || c.finder == nil {
		return nil, fmt.Errorf("stream %s: no session finder: %w", handler, ErrNoRouteToNode)
	}
	finder, ok := c.finder.(NodeSessionFinder)
	if !ok {
		return nil, fmt.Errorf("stream %s: the finder names no session by node: %w", handler, ErrNoRouteToNode)
	}
	if err := c.checkAnswerers(role, handler, nodeID); err != nil {
		return nil, fmt.Errorf("stream %s to node %s: %w", handler, nodeID, err)
	}
	session, ok := finder.MeshSessionForNode(nodeID)
	if !ok || session == nil || session.IsClosed() {
		return nil, fmt.Errorf("stream %s to node %s: %w", handler, nodeID, ErrNoRouteToNode)
	}
	return OpenHandlerStream(ctx, session, buildRPCRequestCtx(ctx, role, handler, nil, 0))
}

// OpenHandlerStream opens one stream of the handler-stream band on session,
// sends req as its open frame and waits for the far side's answer.
func OpenHandlerStream(ctx context.Context, session aether.Session, req *pb.RPCRequest) (NodeStream, error) {
	open, err := pb.MarshalRequest(req)
	if err != nil {
		return nil, fmt.Errorf("stream %s: marshal open: %w", req.Handler, err)
	}
	var stream aether.Stream
	for attempt := 0; attempt < handlerStreamAttempts; attempt++ {
		stream, err = session.OpenStream(ctx, aether.StreamConfig{
			StreamID: nextHandlerStreamID(), Reliability: aether.ReliableOrdered, Priority: 128,
		})
		if err == nil || errors.Is(err, aether.ErrStreamCapExhausted) || ctx.Err() != nil {
			break
		}
	}
	if err != nil {
		return nil, fmt.Errorf("stream %s: open: %w", req.Handler, err)
	}
	if err := stream.Send(ctx, open); err != nil {
		_ = stream.Close()
		return nil, fmt.Errorf("stream %s: send open: %w", req.Handler, err)
	}
	answer, err := stream.Receive(ctx)
	if err != nil {
		_ = stream.Close()
		return nil, fmt.Errorf("stream %s: read answer: %w", req.Handler, err)
	}
	resp, err := pb.UnmarshalResponse(answer)
	if err != nil {
		_ = stream.Close()
		return nil, fmt.Errorf("stream %s: read answer: %w", req.Handler, err)
	}
	if !resp.Success {
		_ = stream.Close()
		return nil, fmt.Errorf("stream %s refused: %s", req.Handler, resp.Error)
	}
	return nodeStream{stream}, nil
}

type nodeStream struct{ stream aether.Stream }

func (s nodeStream) Send(ctx context.Context, payload []byte) error {
	return s.stream.Send(ctx, payload)
}
func (s nodeStream) Recv(ctx context.Context) ([]byte, error) { return s.stream.Receive(ctx) }
func (s nodeStream) Close() error                             { return s.stream.Close() }
