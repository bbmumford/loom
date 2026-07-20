/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package dispatch

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/health"
)

// mockSession implements aether.Session for stream pool tests.
type mockSession struct {
	openCount int32
	closed    bool
}

func (m *mockSession) OpenStream(ctx context.Context, cfg aether.StreamConfig) (aether.Stream, error) {
	atomic.AddInt32(&m.openCount, 1)
	return &mockStream{id: cfg.StreamID}, nil
}
func (m *mockSession) AcceptStream(ctx context.Context) (aether.Stream, error) { return nil, nil }
func (m *mockSession) AcceptStreamByID(ctx context.Context, streamID uint64) (aether.Stream, error) {
	return nil, nil
}
func (m *mockSession) IsClosed() bool                                       { return m.closed }
func (m *mockSession) Close() error                                         { return nil }
func (m *mockSession) LocalNodeID() aether.NodeID                        { return "" }
func (m *mockSession) RemoteNodeID() aether.NodeID                       { return "" }
func (m *mockSession) LocalPeerID() aether.PeerID                             { return aether.PeerID{} }
func (m *mockSession) RemotePeerID() aether.PeerID                            { return aether.PeerID{} }
func (m *mockSession) Capabilities() aether.Capabilities                      { return 0 }
func (m *mockSession) Ping(ctx context.Context) (time.Duration, error)     { return 0, nil }
func (m *mockSession) GoAway(ctx context.Context, r aether.GoAwayReason, msg string) error {
	return nil
}
func (m *mockSession) Health() *health.Monitor              { return nil }
func (m *mockSession) SessionKey() []byte                   { return nil }
func (m *mockSession) ConnectionID() aether.ConnectionID       { return aether.ConnectionID{} }
func (m *mockSession) CongestionWindow() int64              { return 0 }
func (m *mockSession) Protocol() aether.Protocol         { return aether.ProtoWebSocket }
func (m *mockSession) Metrics() aether.SessionMetrics          { return aether.SessionMetrics{} }

type mockStream struct {
	id     uint64
	closed bool
}

func (s *mockStream) StreamID() uint64                                     { return s.id }
func (s *mockStream) Send(ctx context.Context, data []byte) error          { return nil }
func (s *mockStream) Receive(ctx context.Context) ([]byte, error)          { return nil, nil }
func (s *mockStream) Close() error                                         { s.closed = true; return nil }
func (s *mockStream) Reset(reason aether.ResetReason) error                   { return nil }
func (s *mockStream) SetPriority(weight uint8, dependency uint64)          {}
func (s *mockStream) Config() aether.StreamConfig                             { return aether.StreamConfig{} }
func (s *mockStream) IsOpen() bool                                         { return !s.closed }
func (s *mockStream) Conn() net.Conn                                       { return nil }

func TestStreamPool_GetReturnsPreOpenedStream(t *testing.T) {
	sess := &mockSession{}
	pool := NewStreamPool(2)
	pool.Fill(sess)

	if atomic.LoadInt32(&sess.openCount) != 2 {
		t.Fatalf("expected 2 pre-opened streams, got %d", sess.openCount)
	}

	stream, err := pool.Get(context.Background(), sess)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if stream == nil {
		t.Fatal("expected non-nil stream")
	}
	if pool.Len() != 1 {
		t.Fatalf("expected 1 remaining, got %d", pool.Len())
	}
}

func TestStreamPool_FallsBackToFreshStream(t *testing.T) {
	sess := &mockSession{}
	pool := NewStreamPool(2)
	// Don't Fill — pool is empty

	stream, err := pool.Get(context.Background(), sess)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if stream == nil {
		t.Fatal("expected non-nil stream from fresh open")
	}
	if atomic.LoadInt32(&sess.openCount) != 1 {
		t.Fatalf("expected 1 fresh open, got %d", sess.openCount)
	}
}

func TestStreamPool_DrainClosesAll(t *testing.T) {
	sess := &mockSession{}
	pool := NewStreamPool(2)
	pool.Fill(sess)
	pool.Drain()
	if pool.Len() != 0 {
		t.Fatalf("expected 0 after drain, got %d", pool.Len())
	}
}
