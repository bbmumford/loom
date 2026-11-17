/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/rpc/pb"
	"github.com/bbmumford/loom/node/handlers"
	"github.com/bbmumford/loom/pkg/dispatch"
	"github.com/bbmumford/loom/pkg/rpc"
	rpcscope "github.com/bbmumford/loom/pkg/rpc/scope"
	"github.com/bbmumford/loom/pkg/trace"
)

// handlerStreamOpenWait bounds how long an accepted handler stream waits for
// its open frame, so a peer that opens one and says nothing holds no goroutine.
const handlerStreamOpenWait = 30 * time.Second

// serveHandlerStream serves one stream of the handler-stream band: a
// long-lived exchange of frames with one StreamHandler, reached across the
// mesh as HandlerRegistry.DispatchStream reaches it in process.
//
// The first frame is a pb.RPCRequest naming the handler and carrying the
// caller's principal, which is judged as an RPC's is: against the transport
// partition the session bound, by the consumer's admitter, so a stream admits
// nobody an RPC would refuse. A principal refused, or a handler refused by
// DispatchStream's scope check, answers one failed pb.RPCResponse stamped with the
// refusal's kind and the stream closes before the handler runs. Once the handler first uses the stream it is
// answered one successful pb.RPCResponse (meshMessageStream.accept), after which
// every frame both ways is enveloped (dispatch.StreamFrames): the handler's own
// payloads, then one end frame carrying the handler's return, so the opener's
// Recv returns its failure typed and io.EOF only for its clean end.
func (s *RPCServer) serveHandlerStream(ctx context.Context, stream aether.Stream, remoteID aether.NodeID) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("[RPC-AETHER] handler stream %d panic: %v", stream.StreamID(), r)
			_ = stream.Reset(aether.ResetRefused)
		}
	}()
	ctx, cancel := context.WithCancel(withCallerNode(ctx, string(remoteID)))
	defer cancel()

	openCtx, openCancel := context.WithTimeout(ctx, handlerStreamOpenWait)
	frame, err := stream.Receive(openCtx)
	openCancel()
	if err != nil {
		_ = stream.Reset(aether.ResetRefused)
		return
	}
	req, err := pb.UnmarshalRequest(frame)
	if err != nil {
		_ = stream.Reset(aether.ResetRefused)
		return
	}
	adapter := &meshMessageStream{ctx: ctx, stream: stream, id: req.Id, frames: dispatch.NewStreamFrames(stream, req.Handler, req.Id)}
	defer adapter.Close()

	transportPartition := handlers.TransportPartitionFromContext(ctx)
	if transportPartition != "" && s.authValidator != nil {
		ctx = s.authValidator.WithPartitionID(ctx, transportPartition)
	}
	if req.Principal != nil {
		ctx, err = s.bindAuthenticatedPrincipal(ctx, req, transportPartition)
		if err != nil {
			adapter.refuse(fmt.Sprintf("authenticated principal denied: %v", err), fmt.Errorf("%w: %w", rpcscope.ErrDenied, err))
			return
		}
	}
	if req.TraceId != "" {
		ctx = trace.WithID(ctx, req.TraceId)
	}
	adapter.ctx = ctx
	adapter.finish(s.registry.DispatchStream(ctx, req.Handler, adapter))
}

// meshMessageStream is a handlers.MessageStream over one aether stream.
//
// The handler's first Send or Recv answers the opener's open frame with success,
// because DispatchStream runs its scope check before the handler can reach
// either, so reaching one means the handler was admitted. refuse answers a
// failure in its place; once accepted it is a no-op, and the handler's return
// reaches the opener as the end frame finish sends.
type meshMessageStream struct {
	ctx    context.Context
	stream aether.Stream
	id     string
	frames *dispatch.StreamFrames

	once     sync.Once
	err      error
	accepted atomic.Bool
}

func (m *meshMessageStream) answer(resp *pb.RPCResponse) {
	m.once.Do(func() {
		wire, err := pb.MarshalResponse(resp)
		if err == nil {
			err = m.stream.Send(m.ctx, wire)
		}
		m.err = err
		m.accepted.Store(err == nil && resp.Success)
	})
}

func (m *meshMessageStream) accept() error {
	m.answer(&pb.RPCResponse{Id: m.id, Success: true})
	return m.err
}

// refuse answers the open frame with a failure whose text is reason and whose
// refusal kind is cause's (dispatch.StampFailed), so the opener's
// OpenHandlerStream returns the typed refusal a refused call returns: a denied
// principal or scope is scope.ErrDenied there, as RPCServer.handleRequest and
// executeLocal stamp it for a call.
func (m *meshMessageStream) refuse(reason string, cause error) {
	resp := &pb.RPCResponse{Id: m.id, Success: false, Error: reason}
	dispatch.StampFailed(resp, cause)
	m.answer(resp)
}

// errStreamFinished answers a Send or Recv made after the handler returned
// without having used its stream: the open frame is no longer answered.
var errStreamFinished = errors.New("handler stream: the handler has returned")

// finish ends the stream for the handler's return err, after DispatchStream
// returned. Before acceptance a failure is the refused answer (refuse) and a nil
// return answers nothing; after it, the end frame goes to the opener: clean for
// nil, read as io.EOF, else err with the fields rpc.StreamFailureOf reports for
// it (the HTTP stream's error event's), read as the typed error carrying them.
func (m *meshMessageStream) finish(err error) {
	if err != nil {
		m.refuse(err.Error(), err)
	} else {
		m.once.Do(func() { m.err = errStreamFinished })
	}
	switch {
	case !m.accepted.Load():
	case err == nil:
		_ = m.frames.End(m.ctx)
	default:
		_ = m.frames.Fail(m.ctx, err, rpc.StreamFailureOf(err))
	}
}

// Send writes one payload frame to the opener.
func (m *meshMessageStream) Send(payload []byte) error {
	if err := m.accept(); err != nil {
		return err
	}
	return m.frames.Send(m.ctx, payload)
}

// Recv blocks for the opener's next payload frame; io.EOF is the opener's
// clean end (dispatch.StreamFrames.Recv).
func (m *meshMessageStream) Recv() ([]byte, error) {
	if err := m.accept(); err != nil {
		return nil, err
	}
	return m.frames.Recv(m.ctx)
}

// Context is the stream's serving context, ended when the session goes.
func (m *meshMessageStream) Context() context.Context { return m.ctx }

// Close ends the stream from this side: an accepted stream's clean end frame
// first, unless finish sent its end, then dispatch.CloseAbandoned, which reads
// off what the opener still sends because the handler has finished reading. A
// handler that closes its own stream has ended it cleanly; an error it returns
// afterwards no longer reaches the opener.
func (m *meshMessageStream) Close() error {
	if m.accepted.Load() {
		_ = m.frames.End(m.ctx)
	}
	return dispatch.CloseAbandoned(m.stream)
}

var _ handlers.MessageStream = (*meshMessageStream)(nil)
