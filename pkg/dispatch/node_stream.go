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
	"time"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/rpc/pb"
	"github.com/bbmumford/loom/core/streams"
)

// NodeStream is one handler stream the caller opened to a named node: frames
// both ways until either end closes. Recv returns io.EOF only at the far
// handler's clean end; the handler's failure comes back as the typed error a
// refused call's would, and a stream cut short as ErrNoEndFrame.
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
// stream still open are answered by the session's OpenStream with
// aether.ErrDuplicateStreamID and retried on the next ID.
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
	// The pre-send check every send entry makes; an open frame carries no payload.
	if _, err := c.preSend(ctx, role, handler, nodeID, nil); err != nil {
		return nil, fmt.Errorf("stream %s to node %s: %w", handler, nodeID, err)
	}
	session, ok := finder.MeshSessionForNode(nodeID)
	if !ok || session == nil || session.IsClosed() {
		return nil, fmt.Errorf("stream %s to node %s: %w", handler, nodeID, ErrNoRouteToNode)
	}
	return OpenHandlerStream(ctx, session, buildRPCRequestCtx(ctx, role, handler, nil, 0))
}

// OpenHandlerStream opens one stream of the handler-stream band on session,
// sends req as its open frame and waits for the far side's answer. A refusal is
// read as HWPCaller reads a failed response (responseError): the kind the far
// side stamped (StampFailed) comes back typed, so a refusal of the caller's
// authority is scope.ErrDenied and a starting or route refusal is its own error,
// as the same refusal of a call is. The
// process's admission is asked first, with the role req's context names, its
// handler and its payload, because a caller holding a session reaches this
// without HWPCaller: a refused stream is never opened.
func OpenHandlerStream(ctx context.Context, session aether.Session, req *pb.RPCRequest) (NodeStream, error) {
	if err := Admit(ctx, req.GetContext()["role"], req.GetHandler(), req.GetPayload()); err != nil {
		return nil, fmt.Errorf("stream %s: %w", req.GetHandler(), err)
	}
	open, err := pb.MarshalRequest(req)
	if err != nil {
		return nil, fmt.Errorf("stream %s: marshal open: %w", req.Handler, err)
	}
	var stream aether.Stream
	for attempt := 0; attempt < handlerStreamAttempts; attempt++ {
		stream, err = session.OpenStream(ctx, aether.StreamConfig{
			StreamID: nextHandlerStreamID(), Reliability: aether.ReliableOrdered, Priority: 128,
		})
		if err == nil || !errors.Is(err, aether.ErrDuplicateStreamID) || ctx.Err() != nil {
			break
		}
	}
	if err != nil {
		return nil, fmt.Errorf("stream %s: open: %w", req.Handler, err)
	}
	// Each failure below resets the stream rather than closing it: nothing reads it
	// afterwards, and a closed stream's unread messages hold the far side's
	// connection credit until aether's idle sweep.
	if err := stream.Send(ctx, open); err != nil {
		_ = stream.Reset(aether.ResetCancel)
		return nil, fmt.Errorf("stream %s: send open: %w", req.Handler, err)
	}
	answer, err := stream.Receive(ctx)
	if err != nil {
		_ = stream.Reset(aether.ResetCancel)
		return nil, fmt.Errorf("stream %s: read answer: %w", req.Handler, err)
	}
	resp, err := pb.UnmarshalResponse(answer)
	if err != nil {
		_ = stream.Reset(aether.ResetCancel)
		return nil, fmt.Errorf("stream %s: read answer: %w", req.Handler, err)
	}
	if !resp.Success {
		_ = stream.Reset(aether.ResetCancel)
		return nil, fmt.Errorf("stream %s refused: %w", req.Handler, responseError(req.Handler, resp))
	}
	return nodeStream{NewStreamFrames(stream, req.GetHandler(), req.GetId())}, nil
}

// nodeStream is the opener's side of an accepted handler stream, its frames
// enveloped (StreamFrames): Recv returns the handler's failure typed, io.EOF
// for its clean end, and ErrNoEndFrame for a stream that ended without one.
type nodeStream struct{ frames *StreamFrames }

func (s nodeStream) Send(ctx context.Context, payload []byte) error {
	return s.frames.Send(ctx, payload)
}
func (s nodeStream) Recv(ctx context.Context) ([]byte, error) { return s.frames.Recv(ctx) }

// Close sends the opener's clean end frame, which the handler's Recv reads as
// io.EOF, bounded by abandonedDrainWait, then closes (CloseAbandoned).
func (s nodeStream) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), abandonedDrainWait)
	_ = s.frames.End(ctx)
	cancel()
	return CloseAbandoned(s.frames.stream)
}

// abandonedDrainWait bounds how long CloseAbandoned keeps reading a stream whose
// consumer has finished, which is about one round trip for a peer that closes in
// turn. A peer that keeps sending past it is reset.
const abandonedDrainWait = 5 * time.Second

// CloseAbandoned closes st for a consumer that will read nothing more from it: what
// Send queued still reaches the peer, ahead of the CLOSE. It then reads off what the
// peer has sent or still sends, in the background, and resets the stream at its end
// or at abandonedDrainWait. A closed stream's unread messages otherwise hold the
// peer's connection credit until aether's idle sweep, and sixteen such streams stall
// its session; a Reset of a stream that has already left writes nothing and returns
// the credit its queue still holds. Its callers are nodeStream.Close, node's
// meshMessageStream.Close and MeshSwarmTransport's teardown, none of which reads
// the stream after it: this goroutine would take what such a read was waiting for.
func CloseAbandoned(st aether.Stream) error {
	err := st.Close()
	go func() {
		defer func() { _ = recover() }()
		ctx, cancel := context.WithTimeout(context.Background(), abandonedDrainWait)
		defer cancel()
		for {
			if _, rerr := st.Receive(ctx); rerr != nil {
				break
			}
		}
		_ = st.Reset(aether.ResetCancel)
	}()
	return err
}
