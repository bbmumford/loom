/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package rpchttp

// stream.go — the HTTP arm of an operation's handler stream (rpc.Handler.Stream),
// so one declaration is served by name over the mesh (node.serveHandlerStream)
// and over HTTP at POST /rpc/<fqn>.
//
// The request body is the operation's request message in protojson; the stream
// receives it as its first frame. The answer is a server-sent event stream: one
// event per frame the stream sends, its data the protojson of the operation's
// response message with protojson's default JSON names, flushed as it is sent. A
// refusal before the first frame (the scope judge, a webhook signature, the
// stream's own early error) is the bridge's usual error answer with its status;
// a failure after it is one "error" event carrying {"error": message}, because
// the stream's status is already sent. The stream ends when the operation
// returns or the caller goes away.
//
// Consumers: dispatch routes here every operation that declares a Stream, local
// to this registry (a proxied operation has neither Func nor Stream here). A
// streamed operation keeps a Func refusing the single-shot call, because a Func is
// what exports it to the mesh registry; the bridge serves the Stream.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/bbmumford/loom/pkg/rpc"
)

// serveStream runs h's Stream for one HTTP request whose body parsed into req.
func (b *Bridge) serveStream(w http.ResponseWriter, r *http.Request, h *rpc.Handler, req proto.Message) {
	if err := rpc.EnforceScope(r.Context(), h, b.registry.PresencePolicy(), b.registry.AuthzPolicy()); err != nil {
		writeBridgeError(w, &bridgeError{status: classifyHandlerStatus(err), cause: err, op: h.FQN()})
		return
	}
	if h.WebhookSignature != nil {
		if err := rpc.VerifyWebhookSignature(h.WebhookSignature, req); err != nil {
			writeBridgeError(w, &bridgeError{status: classifyHandlerStatus(err), cause: err, op: h.FQN()})
			return
		}
	}
	first, err := proto.Marshal(req)
	if err != nil {
		writeBridgeError(w, &bridgeError{status: http.StatusBadRequest, cause: err, op: h.FQN(), where: "marshal request"})
		return
	}
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	s := &httpStream{w: w, h: h, ctx: ctx, first: first}
	err = h.Stream(ctx, s)
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case err == nil:
	case !s.started:
		writeBridgeError(w, &bridgeError{status: classifyHandlerStatus(err), cause: err, op: h.FQN()})
	case ctx.Err() == nil:
		s.writeEvent("error", errorFrame(err))
	}
}

// errorFrame is the data of the error event a stream answers after its first
// frame: the classified message of a 4xx cause, the status text of any other.
func errorFrame(err error) string {
	msg := http.StatusText(http.StatusInternalServerError)
	if status := classifyHandlerStatus(err); status < 500 {
		msg = err.Error()
	}
	data, _ := json.Marshal(map[string]string{"error": msg})
	return string(data)
}

// httpStream is the handlers.MessageStream of one HTTP request: the request as
// the only received frame, each sent frame an event on the response.
type httpStream struct {
	w     http.ResponseWriter
	h     *rpc.Handler
	ctx   context.Context
	first []byte

	mu       sync.Mutex
	received bool
	started  bool
	closed   bool
}

// Send writes payload, a binary frame of the operation's response message, as
// one event carrying its protojson.
func (s *httpStream) Send(payload []byte) error {
	msg := rpc.NewInstance(s.h.Response)
	if err := proto.Unmarshal(payload, msg); err != nil {
		return fmt.Errorf("rpchttp stream %s: frame is not a %T: %w", s.h.FQN(), s.h.Response, err)
	}
	data, err := protojson.MarshalOptions{EmitUnpopulated: false}.Marshal(msg)
	if err != nil {
		return fmt.Errorf("rpchttp stream %s: marshal frame: %w", s.h.FQN(), err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return io.ErrClosedPipe
	}
	if err := s.ctx.Err(); err != nil {
		return err
	}
	return s.writeEvent("", string(data))
}

// writeEvent writes one event, named when event is non-empty, and flushes. The
// caller holds mu.
func (s *httpStream) writeEvent(event, data string) error {
	if !s.started {
		hdr := s.w.Header()
		hdr.Set("Content-Type", "text/event-stream")
		hdr.Set("Cache-Control", "no-cache")
		hdr.Set("X-Accel-Buffering", "no")
		s.w.WriteHeader(http.StatusOK)
		s.started = true
	}
	var frame []byte
	if event != "" {
		frame = append(frame, "event: "+event+"\n"...)
	}
	frame = append(frame, "data: "+data+"\n\n"...)
	if _, err := s.w.Write(frame); err != nil {
		return err
	}
	if f, ok := s.w.(http.Flusher); ok {
		f.Flush()
	}
	return nil
}

// Recv returns the request once; the caller sends nothing more on a server
// stream, so a second Recv answers io.EOF.
func (s *httpStream) Recv() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.received {
		return nil, io.EOF
	}
	s.received = true
	return s.first, nil
}

// Context is the request's context, canceled when the caller goes away or the
// stream is served.
func (s *httpStream) Context() context.Context { return s.ctx }

// Close ends the stream from the operation's end; later sends fail.
func (s *httpStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}
