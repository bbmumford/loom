/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
 */
package dispatch

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/rpc/pb"
	rpcscope "github.com/bbmumford/loom/pkg/rpc/scope"
)

// queuedStream answers Receive from its frames in order, then io.EOF, and
// keeps what is sent.
type queuedStream struct {
	aether.Stream
	frames [][]byte
	sent   [][]byte
}

func (s *queuedStream) Receive(context.Context) ([]byte, error) {
	if len(s.frames) == 0 {
		return nil, io.EOF
	}
	f := s.frames[0]
	s.frames = s.frames[1:]
	return f, nil
}

func (s *queuedStream) Send(_ context.Context, data []byte) error {
	s.sent = append(s.sent, append([]byte(nil), data...))
	return nil
}

// A frame with no envelope, or one tagged neither payload nor end, is refused
// rather than handed over as a payload; a payload frame's bytes are handed over
// without their tag.
func TestAFrameOutsideTheEnvelopeIsRefused(t *testing.T) {
	st := &queuedStream{frames: [][]byte{{}, {7, 'x'}, {streamFramePayload, 'o', 'k'}}}
	f := NewStreamFrames(st, "orbtr.test.Stream", "id-1")
	ctx := context.Background()
	if got, err := f.Recv(ctx); err == nil || !strings.Contains(err.Error(), "no envelope") {
		t.Fatalf("an empty frame read %q, %v", got, err)
	}
	if got, err := f.Recv(ctx); err == nil || !strings.Contains(err.Error(), "neither a payload nor an end") {
		t.Fatalf("a frame tagged 7 read %q, %v", got, err)
	}
	if got, err := f.Recv(ctx); err != nil || string(got) != "ok" {
		t.Fatalf("a payload frame read %q, %v", got, err)
	}
}

// The far side's end is read once and kept: a frame that follows it is never
// handed over, and every later Recv returns the same end.
func TestAStreamsEndIsKept(t *testing.T) {
	end := &queuedStream{}
	if err := NewStreamFrames(end, "orbtr.test.Stream", "id-1").Fail(context.Background(), rpcscope.ErrDenied, StreamFailure{Status: 403}); err != nil {
		t.Fatal(err)
	}
	st := &queuedStream{frames: [][]byte{end.sent[0], {streamFramePayload, 'l', 'a', 't', 'e'}}}
	f := NewStreamFrames(st, "orbtr.test.Stream", "id-1")
	ctx := context.Background()
	_, first := f.Recv(ctx)
	if !errors.Is(first, rpcscope.ErrDenied) {
		t.Fatalf("the end frame read %v, want scope.ErrDenied", first)
	}
	if got, again := f.Recv(ctx); again == nil || again.Error() != first.Error() {
		t.Fatalf("a Recv after the end read %q, %v, want the same end", got, again)
	}
}

// Fail sends one end frame, stamped with the refusal kind, its request ID and
// the reported failure's fields, which the reader gets back unchanged beside
// the typed refusal; an End after it sends nothing.
func TestFailSendsOneEndFrameCarryingTheReportedFailure(t *testing.T) {
	st := &queuedStream{}
	f := NewStreamFrames(st, "orbtr.test.Stream", "id-1")
	reported := StreamFailure{Error: "portClosed: the port closed", Code: "portClosed", Status: 409, Kind: "denied"}
	if err := f.Fail(context.Background(), rpcscope.ErrDenied, reported); err != nil {
		t.Fatal(err)
	}
	if err := f.End(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(st.sent) != 1 || len(st.sent[0]) == 0 || st.sent[0][0] != streamFrameEnd {
		t.Fatalf("sent %d frames, want one end frame", len(st.sent))
	}
	resp, err := pb.UnmarshalResponse(st.sent[0][1:])
	if err != nil {
		t.Fatal(err)
	}
	if resp.Success || resp.Id != "id-1" || resp.Error == "" || resp.Metadata[refusalKindKey] != refusalKindDenied {
		t.Fatalf("the end frame is %+v, want a failure with id-1, the text and the denied kind", resp)
	}
	_, got := NewStreamFrames(&queuedStream{frames: st.sent}, "orbtr.test.Stream", "id-1").Recv(context.Background())
	failure, ok := StreamFailureOf(got)
	if !ok || failure != reported {
		t.Fatalf("the reader got %+v (%v), want the reported failure %+v", failure, ok, reported)
	}
	if !errors.Is(got, rpcscope.ErrDenied) {
		t.Fatalf("the reader got %v, want the typed denial beside the failure", got)
	}
}
