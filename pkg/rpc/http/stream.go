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
// returns or the caller goes away; nothing is written after the operation
// returns.
//
// A resumable stream declares a cursor: a response field whose JSON name is
// "cursor" is written as each event's id, and a request whose own cursor field
// is unset takes a reconnecting client's Last-Event-ID, so an EventSource that
// reconnects resumes after the last event it read. While a stream is open and
// silent for streamKeepalive, a comment line keeps idle proxies from closing it.
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
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/bbmumford/loom/pkg/rpc"
)

// cursorJSONName is the JSON name of the field a resumable stream's request and
// response messages carry their position in.
const cursorJSONName = "cursor"

// streamKeepalive is how long an open stream may stay silent before a comment
// line is written; a test shortens it.
var streamKeepalive = 15 * time.Second

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
	resumeFromLastEventID(r, req)
	first, err := proto.Marshal(req)
	if err != nil {
		writeBridgeError(w, &bridgeError{status: http.StatusBadRequest, cause: err, op: h.FQN(), where: "marshal request"})
		return
	}
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	s := &httpStream{w: w, h: h, ctx: ctx, first: first, done: make(chan struct{})}
	err = h.Stream(ctx, s)
	s.finish()
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case err == nil:
	case !s.started:
		writeBridgeError(w, &bridgeError{status: classifyHandlerStatus(err), cause: err, op: h.FQN()})
	case ctx.Err() == nil:
		s.writeFrame("error", "", errorFrame(err))
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

	mu        sync.Mutex
	received  bool
	started   bool
	closed    bool
	lastWrite time.Time
	done      chan struct{}
	alive     sync.WaitGroup
}

// Send writes payload, a binary frame of the operation's response message, as
// one event carrying its protojson, with the frame's cursor as its id.
func (s *httpStream) Send(payload []byte) error {
	msg := rpc.NewInstance(s.h.Response)
	if err := proto.Unmarshal(payload, msg); err != nil {
		return fmt.Errorf("rpchttp stream %s: frame is not a %T: %w", s.h.FQN(), s.h.Response, err)
	}
	data, err := protojson.MarshalOptions{EmitUnpopulated: false}.Marshal(msg)
	if err != nil {
		return fmt.Errorf("rpchttp stream %s: marshal frame: %w", s.h.FQN(), err)
	}
	id, _ := cursorOf(msg)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return io.ErrClosedPipe
	}
	if err := s.ctx.Err(); err != nil {
		return err
	}
	return s.writeFrame("", id, string(data))
}

// open writes the event-stream status and headers once and starts the
// keepalive. The caller holds mu.
func (s *httpStream) open() {
	if s.started {
		return
	}
	s.started = true
	hdr := s.w.Header()
	hdr.Set("Content-Type", "text/event-stream")
	hdr.Set("Cache-Control", "no-cache")
	hdr.Set("X-Accel-Buffering", "no")
	s.w.WriteHeader(http.StatusOK)
	s.lastWrite = time.Now()
	s.alive.Add(1)
	go s.keepalive()
}

// writeFrame writes one event, named when event is non-empty and with an id when
// id is, and flushes. The caller holds mu.
func (s *httpStream) writeFrame(event, id, data string) error {
	s.open()
	var frame strings.Builder
	if event != "" {
		frame.WriteString("event: " + event + "\n")
	}
	if id != "" && !strings.ContainsAny(id, "\r\n") {
		frame.WriteString("id: " + id + "\n")
	}
	frame.WriteString("data: " + data + "\n\n")
	if _, err := io.WriteString(s.w, frame.String()); err != nil {
		return err
	}
	s.flush()
	s.lastWrite = time.Now()
	return nil
}

func (s *httpStream) flush() {
	if f, ok := s.w.(http.Flusher); ok {
		f.Flush()
	}
}

// keepalive writes a comment line whenever the open stream has been silent for
// streamKeepalive, until the stream finishes or the caller goes away.
func (s *httpStream) keepalive() {
	defer s.alive.Done()
	tick := time.NewTicker(streamKeepalive / 5)
	defer tick.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-s.ctx.Done():
			return
		case <-tick.C:
			s.mu.Lock()
			if !s.closed && time.Since(s.lastWrite) >= streamKeepalive {
				if _, err := io.WriteString(s.w, ": keepalive\n\n"); err == nil {
					s.flush()
					s.lastWrite = time.Now()
				}
			}
			s.mu.Unlock()
		}
	}
}

// finish ends every write from the operation's side: later sends fail, and the
// keepalive is stopped and waited for, so only serveStream's own error event can
// follow.
func (s *httpStream) finish() {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	close(s.done)
	s.alive.Wait()
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

// resumeFromLastEventID sets req's cursor field from the Last-Event-ID header
// when the request declares a scalar cursor and left it unset.
func resumeFromLastEventID(r *http.Request, req proto.Message) {
	id := strings.TrimSpace(r.Header.Get("Last-Event-ID"))
	if id == "" {
		return
	}
	m := req.ProtoReflect()
	fd := m.Descriptor().Fields().ByJSONName(cursorJSONName)
	if fd == nil || fd.IsList() || fd.IsMap() || m.Has(fd) {
		return
	}
	switch fd.Kind() {
	case protoreflect.StringKind:
		m.Set(fd, protoreflect.ValueOfString(id))
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		if v, err := strconv.ParseInt(id, 10, 64); err == nil {
			m.Set(fd, protoreflect.ValueOfInt64(v))
		}
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		if v, err := strconv.ParseUint(id, 10, 64); err == nil {
			m.Set(fd, protoreflect.ValueOfUint64(v))
		}
	}
}

// cursorOf returns frame's cursor field as text, when it declares a scalar one
// and it is set.
func cursorOf(frame proto.Message) (string, bool) {
	m := frame.ProtoReflect()
	fd := m.Descriptor().Fields().ByJSONName(cursorJSONName)
	if fd == nil || fd.IsList() || fd.IsMap() || !m.Has(fd) {
		return "", false
	}
	v := m.Get(fd)
	switch fd.Kind() {
	case protoreflect.StringKind:
		return v.String(), v.String() != ""
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
		return strconv.FormatInt(v.Int(), 10), true
	case protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		return strconv.FormatUint(v.Uint(), 10), true
	}
	return "", false
}
