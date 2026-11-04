/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/rpc/pb"
	"github.com/bbmumford/loom/node/handlers"
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
// DispatchStream's scope check, answers one failed pb.RPCResponse and the stream
// closes before the handler runs. Once the handler first uses the stream it is
// answered one successful pb.RPCResponse (meshMessageStream.accept), after which
// every frame is the handler's own.
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
	adapter := &meshMessageStream{ctx: ctx, stream: stream, id: req.Id}
	defer adapter.Close()

	transportPartition := handlers.TransportPartitionFromContext(ctx)
	if transportPartition != "" && s.authValidator != nil {
		ctx = s.authValidator.WithPartitionID(ctx, transportPartition)
	}
	if req.Principal != nil {
		ctx, err = s.bindAuthenticatedPrincipal(ctx, req, transportPartition)
		if err != nil {
			adapter.refuse(fmt.Sprintf("authenticated principal denied: %v", err))
			return
		}
	}
	if req.TraceId != "" {
		ctx = trace.WithID(ctx, req.TraceId)
	}
	adapter.ctx = ctx
	if err := s.registry.DispatchStream(ctx, req.Handler, adapter); err != nil {
		adapter.refuse(err.Error())
	}
}

// meshMessageStream is a handlers.MessageStream over one aether stream.
//
// The handler's first Send or Recv answers the opener's open frame with success,
// because DispatchStream runs its scope check before the handler can reach
// either, so reaching one means the handler was admitted. refuse answers a
// failure in its place; once accepted it is a no-op, and the close that follows
// is the opener's signal.
type meshMessageStream struct {
	ctx    context.Context
	stream aether.Stream
	id     string

	once sync.Once
	err  error
}

func (m *meshMessageStream) answer(resp *pb.RPCResponse) {
	m.once.Do(func() {
		wire, err := pb.MarshalResponse(resp)
		if err == nil {
			err = m.stream.Send(m.ctx, wire)
		}
		m.err = err
	})
}

func (m *meshMessageStream) accept() error {
	m.answer(&pb.RPCResponse{Id: m.id, Success: true})
	return m.err
}

func (m *meshMessageStream) refuse(reason string) {
	m.answer(&pb.RPCResponse{Id: m.id, Success: false, Error: reason})
}

// Send writes one frame to the opener.
func (m *meshMessageStream) Send(payload []byte) error {
	if err := m.accept(); err != nil {
		return err
	}
	return m.stream.Send(m.ctx, payload)
}

// Recv blocks for the opener's next frame.
func (m *meshMessageStream) Recv() ([]byte, error) {
	if err := m.accept(); err != nil {
		return nil, err
	}
	return m.stream.Receive(m.ctx)
}

// Context is the stream's serving context, ended when the session goes.
func (m *meshMessageStream) Context() context.Context { return m.ctx }

// Close ends the stream from this side.
func (m *meshMessageStream) Close() error { return m.stream.Close() }

var _ handlers.MessageStream = (*meshMessageStream)(nil)
