/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
 */
package node

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/rpc/pb"
	"github.com/bbmumford/loom/node/handlers"
	"github.com/bbmumford/loom/pkg/dispatch"
	rpcscope "github.com/bbmumford/loom/pkg/rpc/scope"
)

// endingProbe is a stream handler that sends one frame and returns ends; with
// read set it first reports to recvd what one Recv after that frame answered,
// and with closes set it closes its own stream before returning.
type endingProbe struct {
	typedPrincipalProbe
	ends   error
	read   bool
	closes bool
	recvd  chan error
}

func (h *endingProbe) HandleStream(_ context.Context, stream handlers.MessageStream) error {
	if err := stream.Send([]byte("first")); err != nil {
		return err
	}
	if h.read {
		_, err := stream.Recv()
		h.recvd <- err
	}
	if h.closes {
		_ = stream.Close()
	}
	return h.ends
}

func endingServer(t *testing.T, probe *endingProbe) *RPCServer {
	t.Helper()
	registry := handlers.NewHandlerRegistry()
	registry.SetPresencePolicy(typedPrincipalScopePolicy())
	if err := registry.RegisterStream(probe); err != nil {
		t.Fatal(err)
	}
	server := NewRPCServer(registry)
	server.SetAuthValidator(typedPrincipalValidator{})
	return server
}

// A handler's failure after it accepted its stream reaches the opener's Recv as
// the typed error a refused call carries, with the handler's sentence: a denial
// is scope.ErrDenied, any other failure is no denial, and neither is io.EOF,
// which the opener reads only for the handler's clean end.
func TestAHandlerStreamsFailureAfterAcceptanceReachesTheOpenerTyped(t *testing.T) {
	for _, c := range []struct {
		name   string
		ends   error
		denied bool
	}{
		{"a denial", fmt.Errorf("the port closed by policy: %w", rpcscope.ErrDenied), true},
		{"a handler's failure", errors.New("the cage went away"), false},
		{"a clean end", nil, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			server := endingServer(t, &endingProbe{ends: c.ends})
			stream, err := dispatch.OpenHandlerStream(ctx, oneStreamSession{server: server, partition: "orbtr"}, typedPrincipalRequest())
			if err != nil {
				t.Fatalf("open: %v", err)
			}
			defer stream.Close()
			if got, err := stream.Recv(ctx); err != nil || string(got) != "first" {
				t.Fatalf("first frame %q, %v", got, err)
			}
			_, err = stream.Recv(ctx)
			if c.ends == nil {
				if err != io.EOF {
					t.Fatalf("after a clean end the opener read %v, want io.EOF", err)
				}
				return
			}
			if err == nil || errors.Is(err, io.EOF) {
				t.Fatalf("after the handler failed the opener read %v: a clean end", err)
			}
			if !strings.Contains(err.Error(), c.ends.Error()) {
				t.Fatalf("the opener read %q, want the handler's sentence %q", err, c.ends)
			}
			if got := errors.Is(err, rpcscope.ErrDenied); got != c.denied {
				t.Fatalf("the opener read %v: denied %v, want %v", err, got, c.denied)
			}
			if _, again := stream.Recv(ctx); again == nil || again.Error() != err.Error() {
				t.Fatalf("a Recv after the end read %v, want the same end %v", again, err)
			}
		})
	}
}

// cutShortSession's far side accepts the stream and then closes it with no end
// frame, as a far node whose session drops mid-stream leaves it.
type cutShortSession struct{ aether.Session }

func (cutShortSession) OpenStream(_ context.Context, cfg aether.StreamConfig) (aether.Stream, error) {
	near, far := streamPair(cfg.StreamID)
	go func() {
		open, err := far.Receive(context.Background())
		if err != nil {
			return
		}
		req, err := pb.UnmarshalRequest(open)
		if err != nil {
			return
		}
		wire, err := pb.MarshalResponse(&pb.RPCResponse{Id: req.Id, Success: true})
		if err != nil {
			return
		}
		_ = far.Send(context.Background(), wire)
		_ = far.Close()
	}()
	return near, nil
}

// A stream that ends without the far side's end frame is never read as a clean
// end: the opener's Recv returns dispatch.ErrNoEndFrame, which is
// io.ErrUnexpectedEOF and not io.EOF.
func TestAHandlerStreamCutShortIsNotACleanEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	stream, err := dispatch.OpenHandlerStream(ctx, cutShortSession{}, typedPrincipalRequest())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer stream.Close()
	_, err = stream.Recv(ctx)
	if !errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) {
		t.Fatalf("a stream cut short read %v, want io.ErrUnexpectedEOF and not io.EOF", err)
	}
}

// The opener's Close reaches the handler's Recv as io.EOF, the clean end, and
// not as a stream cut short.
func TestTheOpenersCloseReachesTheHandlerAsACleanEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	probe := &endingProbe{read: true, recvd: make(chan error, 1)}
	server := endingServer(t, probe)
	stream, err := dispatch.OpenHandlerStream(ctx, oneStreamSession{server: server, partition: "orbtr"}, typedPrincipalRequest())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if got, err := stream.Recv(ctx); err != nil || string(got) != "first" {
		t.Fatalf("first frame %q, %v", got, err)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-probe.recvd:
		if err != io.EOF {
			t.Fatalf("the handler read %v after the opener closed, want io.EOF", err)
		}
	case <-ctx.Done():
		t.Fatal("the handler's Recv did not end when the opener closed")
	}
}

// selfClosingStream refuses a Send after its own Close, as an aether stream does.
type selfClosingStream struct {
	*streamEnd
	closed atomic.Bool
}

func (s *selfClosingStream) Close() error {
	s.closed.Store(true)
	return s.streamEnd.Close()
}

func (s *selfClosingStream) Send(ctx context.Context, data []byte) error {
	if s.closed.Load() {
		return io.ErrClosedPipe
	}
	return s.streamEnd.Send(ctx, data)
}

// selfClosingSession is oneStreamSession with the server's end a selfClosingStream.
type selfClosingSession struct {
	aether.Session
	server *RPCServer
}

func (s selfClosingSession) OpenStream(_ context.Context, cfg aether.StreamConfig) (aether.Stream, error) {
	near, far := streamPair(cfg.StreamID)
	serving := handlers.WithTransportPartition(context.Background(), "orbtr")
	go s.server.serveUnclaimedStream(serving, &selfClosingStream{streamEnd: far}, aether.NodeID(testNodeIDB))
	return near, nil
}

// A handler that closes its own stream ends it cleanly: its Close sends the end
// frame the opener reads as io.EOF, before the stream can no longer carry one,
// and an error it returns afterwards no longer reaches the opener.
func TestAHandlerThatClosesItsStreamEndsItCleanly(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	server := endingServer(t, &endingProbe{closes: true, ends: errors.New("returned after its close")})
	stream, err := dispatch.OpenHandlerStream(ctx, selfClosingSession{server: server}, typedPrincipalRequest())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer stream.Close()
	if got, err := stream.Recv(ctx); err != nil || string(got) != "first" {
		t.Fatalf("first frame %q, %v", got, err)
	}
	if _, err := stream.Recv(ctx); err != io.EOF {
		t.Fatalf("after the handler closed its stream the opener read %v, want io.EOF", err)
	}
}
