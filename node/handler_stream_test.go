/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"context"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/rpc/pb"
	"github.com/bbmumford/loom/core/streams"
	"github.com/bbmumford/loom/node/handlers"
	"github.com/bbmumford/loom/pkg/dispatch"
	rpcscope "github.com/bbmumford/loom/pkg/rpc/scope"
	"github.com/bbmumford/loom/ports"
)

// streamEnd is one end of an in-memory ordered stream. Closing or resetting
// either end ends both, as a stream's FIN reaches its far side.
type streamEnd struct {
	aether.Stream
	id       uint64
	out, in  chan []byte
	done     chan struct{}
	doneOnce *sync.Once
}

func streamPair(id uint64) (*streamEnd, *streamEnd) {
	a, b := make(chan []byte, 8), make(chan []byte, 8)
	done, once := make(chan struct{}), &sync.Once{}
	return &streamEnd{id: id, out: a, in: b, done: done, doneOnce: once},
		&streamEnd{id: id, out: b, in: a, done: done, doneOnce: once}
}

func (s *streamEnd) StreamID() uint64 { return s.id }
func (s *streamEnd) Send(ctx context.Context, data []byte) error {
	select {
	case s.out <- append([]byte(nil), data...):
		return nil
	case <-s.done:
		return io.ErrClosedPipe
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (s *streamEnd) Receive(ctx context.Context) ([]byte, error) {
	select {
	case data := <-s.in:
		return data, nil
	case <-s.done:
		select {
		case data := <-s.in:
			return data, nil
		default:
		}
		return nil, io.EOF
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
func (s *streamEnd) Close() error {
	s.doneOnce.Do(func() { close(s.done) })
	return nil
}
func (s *streamEnd) Reset(aether.ResetReason) error { return s.Close() }

// streamingProbe is a handler reached as a stream: it upper-cases each frame
// until the opener closes.
type streamingProbe struct {
	typedPrincipalProbe
	reached   atomic.Int32
	principal atomic.Value
	ended     chan struct{}
}

func (h *streamingProbe) HandleStream(ctx context.Context, stream handlers.MessageStream) error {
	h.reached.Add(1)
	if p, ok := rpcscope.AuthenticatedPrincipalFromContext(ctx); ok {
		h.principal.Store(p.Identity().Partition())
	}
	defer close(h.ended)
	for {
		frame, err := stream.Recv()
		if err != nil {
			return nil
		}
		if err := stream.Send([]byte(strings.ToUpper(string(frame)))); err != nil {
			return err
		}
	}
}

// oneStreamSession is a session whose OpenStream hands the far end to the
// server, as the session's dynamic accept loop does.
type oneStreamSession struct {
	aether.Session
	server    *RPCServer
	partition string
}

func (s oneStreamSession) OpenStream(ctx context.Context, cfg aether.StreamConfig) (aether.Stream, error) {
	near, far := streamPair(cfg.StreamID)
	serving := handlers.WithTransportPartition(context.Background(), s.partition)
	go s.server.serveUnclaimedStream(serving, far, aether.NodeID(testNodeIDB))
	return near, nil
}

func streamingServer(t *testing.T, v ports.AuthValidator) (*RPCServer, *streamingProbe) {
	t.Helper()
	registry := handlers.NewHandlerRegistry()
	registry.SetPresencePolicy(typedPrincipalScopePolicy())
	probe := &streamingProbe{ended: make(chan struct{})}
	if err := registry.RegisterStream(probe); err != nil {
		t.Fatal(err)
	}
	server := NewRPCServer(registry)
	server.SetAuthValidator(v)
	return server, probe
}

// A handler stream carries the caller's principal in its open frame, judged
// as an RPC's is against the session's transport partition, then the handler's
// own frames both ways; the handler sees the principal and ends when the
// opener closes.
func TestAHandlerStreamReachesItsHandlerUnderTheAdmittedPrincipal(t *testing.T) {
	server, probe := streamingServer(t, typedPrincipalValidator{})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := dispatch.OpenHandlerStream(ctx, oneStreamSession{server: server, partition: "orbtr"}, typedPrincipalRequest())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	for _, word := range []string{"one", "two"} {
		if err := stream.Send(ctx, []byte(word)); err != nil {
			t.Fatalf("send: %v", err)
		}
		got, err := stream.Recv(ctx)
		if err != nil || string(got) != strings.ToUpper(word) {
			t.Fatalf("recv %q, %v", got, err)
		}
	}
	if got := probe.principal.Load(); got != "org-A" {
		t.Fatalf("the handler ran under partition %v, want the caller's org-A", got)
	}
	if err := stream.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-probe.ended:
	case <-ctx.Done():
		t.Fatal("the handler did not end when the opener closed")
	}
}

// Everything a principal or a scope check refuses is refused before the handler
// runs, and comes back as the far side's sentence.
func TestAHandlerStreamIsRefusedBeforeItsHandlerRuns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, c := range []struct {
		name      string
		validator ports.AuthValidator
		partition string
		request   func() *pb.RPCRequest
		want      string
	}{
		{"an admitter refusing the principal", refusingAdmitterValidator{}, "orbtr", typedPrincipalRequest, "refused by the consumer"},
		{"a validator with no admitter", noAdmitterValidator{}, "orbtr", typedPrincipalRequest, "requires a consumer principal admitter"},
		{"a session bound to no partition", typedPrincipalValidator{}, "", typedPrincipalRequest, "partition-bound transport"},
		{"a session bound to another partition", typedPrincipalValidator{}, "elsewhere", typedPrincipalRequest, "does not match the transport"},
		{"no principal at all", typedPrincipalValidator{}, "orbtr", func() *pb.RPCRequest {
			r := typedPrincipalRequest()
			r.Principal = nil
			return r
		}, "scope denied"},
		{"a handler this node does not hold", typedPrincipalValidator{}, "orbtr", func() *pb.RPCRequest {
			r := typedPrincipalRequest()
			r.Handler = "orbtr.test.Elsewhere"
			return r
		}, "not found"},
	} {
		t.Run(c.name, func(t *testing.T) {
			server, probe := streamingServer(t, c.validator)
			_, err := dispatch.OpenHandlerStream(ctx, oneStreamSession{server: server, partition: c.partition}, c.request())
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("open: %v, want a refusal naming %q", err, c.want)
			}
			if probe.reached.Load() != 0 {
				t.Fatal("the handler ran")
			}
		})
	}
}

// An operation that is not a stream handler refuses a stream, and the band
// beside the handler-stream band is still refused as an unclaimed application
// stream, never read as an open frame.
func TestOnlyAStreamHandlerInTheBandIsServed(t *testing.T) {
	registry := handlers.NewHandlerRegistry()
	registry.SetPresencePolicy(typedPrincipalScopePolicy())
	if err := registry.RegisterRPC(&typedPrincipalProbe{}); err != nil {
		t.Fatal(err)
	}
	server := NewRPCServer(registry)
	server.SetAuthValidator(typedPrincipalValidator{})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := dispatch.OpenHandlerStream(ctx, oneStreamSession{server: server, partition: "orbtr"}, typedPrincipalRequest()); err == nil {
		t.Fatal("a handler that is no stream handler accepted a stream")
	}

	near, far := streamPair(streams.StreamHandlerLast + 1)
	go server.serveUnclaimedStream(handlers.WithTransportPartition(context.Background(), "orbtr"), far, aether.NodeID(testNodeIDB))
	if got, err := near.Receive(ctx); err == nil {
		t.Fatalf("a stream past the band was answered %q", got)
	}
}

var _ aether.Stream = (*streamEnd)(nil)
