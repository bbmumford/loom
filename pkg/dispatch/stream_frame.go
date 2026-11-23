/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
 */
package dispatch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"sync"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/rpc/pb"
)

// Every frame of a handler stream after its open answer is enveloped: its first
// byte says whether the rest is a payload of the handler's or the opener's own,
// or the end of the sender's side. An end frame is a pb.RPCResponse: Success
// for a clean end, else a failure carrying the error's text and the refusal kind
// StampFailed records, as a refused call's failed response does, so the reader
// rebuilds the same typed error (responseError), and the StreamFailure the
// sender reported, so the reader reports the failure as the sender did.
const (
	streamFramePayload byte = 0
	streamFrameEnd     byte = 1
)

// The metadata keys a failed end frame carries its StreamFailure under, beside
// the refusal kind StampFailed writes (refusalKindKey, read as Kind).
const (
	streamFailureErrorKey  = "error"
	streamFailureCodeKey   = "code"
	streamFailureStatusKey = "status"
)

// StreamFailure is what a handler stream reports for a failure after its first
// frame: Error and Code as the HTTP bridge's body for a unary call failing the
// same way, Status that call's HTTP status, Kind the mesh refusal kind
// (RefusalKind). pkg/rpc.StreamFailureOf computes it; the HTTP stream's error
// event writes it and a failed end frame carries it.
type StreamFailure struct {
	Error  string
	Code   string
	Status int
	Kind   string
}

// StreamFailureError is the error a handler stream's reader gets for the far
// side's failed end frame: Failure as the far side reported it, Err the typed
// refusal the end frame rebuilds (responseError), which it unwraps to.
type StreamFailureError struct {
	Handler string
	Failure StreamFailure
	Err     error
}

func (e *StreamFailureError) Error() string {
	return fmt.Sprintf("stream %s ended: %v", e.Handler, e.Err)
}

func (e *StreamFailureError) Unwrap() error { return e.Err }

// StreamFailureOf answers the StreamFailure a failed end frame carried, when err
// holds one.
func StreamFailureOf(err error) (StreamFailure, bool) {
	var e *StreamFailureError
	if errors.As(err, &e) {
		return e.Failure, true
	}
	return StreamFailure{}, false
}

// ErrNoEndFrame is what a handler stream's reader gets when the stream ended
// without the far side's end frame: the far node, its session or the stream
// went away mid-stream. It wraps io.ErrUnexpectedEOF, never io.EOF, so a
// reader that takes io.EOF as a clean end does not take this for one.
var ErrNoEndFrame = fmt.Errorf("the stream ended with no end frame: %w", io.ErrUnexpectedEOF)

// StreamFrames reads and writes the enveloped frames of one accepted handler
// stream. nodeStream holds one on the opener's side and node's meshMessageStream
// on the handler's side. Once an end frame, or the stream's end, is read, every
// later Recv returns the same end; End sends this side's end frame at most once.
type StreamFrames struct {
	stream  aether.Stream
	handler string
	id      string

	mu      sync.Mutex
	readEnd error

	endOnce sync.Once
}

// NewStreamFrames is the framing for stream, opened for handler with the open
// frame whose request ID is id (the ID every end frame carries).
func NewStreamFrames(stream aether.Stream, handler, id string) *StreamFrames {
	return &StreamFrames{stream: stream, handler: handler, id: id}
}

// Send writes payload as one payload frame.
func (f *StreamFrames) Send(ctx context.Context, payload []byte) error {
	frame := make([]byte, 0, len(payload)+1)
	frame = append(frame, streamFramePayload)
	return f.stream.Send(ctx, append(frame, payload...))
}

// Recv reads the next frame: a payload frame's bytes; io.EOF for the far side's
// clean end; the typed error the far side's failed end frame carries, wrapped
// with the handler's name; ErrNoEndFrame for a stream that ended without one.
// A context that ends, or a reset, is returned as the stream's own error.
func (f *StreamFrames) Recv(ctx context.Context) ([]byte, error) {
	f.mu.Lock()
	end := f.readEnd
	f.mu.Unlock()
	if end != nil {
		return nil, end
	}
	frame, err := f.stream.Receive(ctx)
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, f.ended(fmt.Errorf("stream %s: %w", f.handler, ErrNoEndFrame))
		}
		return nil, fmt.Errorf("stream %s: %w", f.handler, err)
	}
	if len(frame) == 0 {
		return nil, fmt.Errorf("stream %s: a frame with no envelope", f.handler)
	}
	switch frame[0] {
	case streamFramePayload:
		return frame[1:], nil
	case streamFrameEnd:
		resp, err := pb.UnmarshalResponse(frame[1:])
		if err != nil {
			return nil, f.ended(fmt.Errorf("stream %s: read the end frame: %w", f.handler, err))
		}
		if resp.Success {
			return nil, f.ended(io.EOF)
		}
		status, _ := strconv.Atoi(resp.Metadata[streamFailureStatusKey])
		return nil, f.ended(&StreamFailureError{
			Handler: f.handler,
			Failure: StreamFailure{
				Error:  resp.Metadata[streamFailureErrorKey],
				Code:   resp.Metadata[streamFailureCodeKey],
				Status: status,
				Kind:   resp.Metadata[refusalKindKey],
			},
			Err: responseError(f.handler, resp),
		})
	default:
		return nil, fmt.Errorf("stream %s: a frame tagged %d is neither a payload nor an end", f.handler, frame[0])
	}
}

func (f *StreamFrames) ended(err error) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.readEnd == nil {
		f.readEnd = err
	}
	return f.readEnd
}

// End sends this side's clean end frame, which the far side's Recv reads as
// io.EOF. Only the first End or Fail sends; a later one returns nil.
func (f *StreamFrames) End(ctx context.Context) error {
	return f.end(ctx, &pb.RPCResponse{Id: f.id, Success: true})
}

// Fail sends this side's end frame for the failure err, reported as failure:
// err's text and the refusal kind StampFailed records, so the far side rebuilds
// the typed error, and failure's fields under their metadata keys, so it
// reports what this side reported (StreamFailureOf). Only the first End or Fail
// sends.
func (f *StreamFrames) Fail(ctx context.Context, err error, failure StreamFailure) error {
	resp := &pb.RPCResponse{Id: f.id, Success: false, Error: err.Error()}
	StampFailed(resp, err)
	if resp.Metadata == nil {
		resp.Metadata = map[string]string{}
	}
	resp.Metadata[streamFailureErrorKey] = failure.Error
	if failure.Code != "" {
		resp.Metadata[streamFailureCodeKey] = failure.Code
	}
	resp.Metadata[streamFailureStatusKey] = strconv.Itoa(failure.Status)
	return f.end(ctx, resp)
}

func (f *StreamFrames) end(ctx context.Context, resp *pb.RPCResponse) error {
	var sent error
	f.endOnce.Do(func() {
		wire, err := pb.MarshalResponse(resp)
		if err != nil {
			sent = fmt.Errorf("stream %s: marshal the end frame: %w", f.handler, err)
			return
		}
		frame := make([]byte, 0, len(wire)+1)
		frame = append(frame, streamFrameEnd)
		sent = f.stream.Send(ctx, append(frame, wire...))
	})
	return sent
}
