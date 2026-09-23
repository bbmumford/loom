/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package dispatch

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/health"
	"github.com/ORBTR/aether/rpc/pb"
	"github.com/bbmumford/loom/core/streams"
)

// The dynamic allocator must never return an ID in aether's application band
// (>= 100): streams.StreamSwarm and streams.StreamMeshDB are bound there per
// peer pair, and aether hands an OPEN for an ID already open the existing
// stream, so an RPC opened on 100 or 101 would write into the swarm or mesh
// database stream.

// idSession records every StreamID passed to OpenStream and hands out
// idStreams, which close on Reset or Close like aether's streams do.
type idSession struct {
	mu     sync.Mutex
	opened []uint64
	resp   []byte
}

func (s *idSession) OpenStream(_ context.Context, cfg aether.StreamConfig) (aether.Stream, error) {
	s.mu.Lock()
	s.opened = append(s.opened, cfg.StreamID)
	s.mu.Unlock()
	return &idStream{id: cfg.StreamID, resp: s.resp}, nil
}

func (s *idSession) openedIDs() []uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]uint64(nil), s.opened...)
}

func (s *idSession) AcceptStream(context.Context) (aether.Stream, error) {
	return nil, context.Canceled
}
func (s *idSession) AcceptStreamByID(context.Context, uint64) (aether.Stream, error) {
	return nil, context.Canceled
}
func (s *idSession) LocalNodeID() aether.NodeID                  { return "local-test" }
func (s *idSession) RemoteNodeID() aether.NodeID                 { return "remote-test" }
func (s *idSession) LocalPeerID() aether.PeerID                  { return aether.PeerID{} }
func (s *idSession) RemotePeerID() aether.PeerID                 { return aether.PeerID{} }
func (s *idSession) Capabilities() aether.Capabilities           { return 0 }
func (s *idSession) Ping(context.Context) (time.Duration, error) { return 0, nil }
func (s *idSession) GoAway(context.Context, aether.GoAwayReason, string) error {
	return nil
}
func (s *idSession) Close() error                      { return nil }
func (s *idSession) IsClosed() bool                    { return false }
func (s *idSession) Health() *health.Monitor           { return nil }
func (s *idSession) SessionKey() []byte                { return nil }
func (s *idSession) ConnectionID() aether.ConnectionID { return aether.ConnectionID{} }
func (s *idSession) CongestionWindow() int64           { return 0 }
func (s *idSession) Protocol() aether.Protocol         { return aether.ProtoNoise }
func (s *idSession) Metrics() aether.SessionMetrics    { return aether.SessionMetrics{} }

type idStream struct {
	id   uint64
	resp []byte

	mu     sync.Mutex
	closed bool
	resets int
}

func (s *idStream) StreamID() uint64                        { return s.id }
func (s *idStream) Send(context.Context, []byte) error      { return nil }
func (s *idStream) Receive(context.Context) ([]byte, error) { return s.resp, nil }
func (s *idStream) Close() error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	return nil
}
func (s *idStream) Reset(aether.ResetReason) error {
	s.mu.Lock()
	s.closed = true
	s.resets++
	s.mu.Unlock()
	return nil
}
func (s *idStream) SetPriority(uint8, uint64)   {}
func (s *idStream) Config() aether.StreamConfig { return aether.StreamConfig{StreamID: s.id} }
func (s *idStream) IsOpen() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return !s.closed
}
func (s *idStream) Conn() net.Conn { return nil }

// A session that opens and discards streams far past the size of the dynamic
// range never receives an application-band ID.
func TestDynamicStreamIDsNeverEnterTheApplicationBand(t *testing.T) {
	sess := &idSession{}
	defer forgetSession(sess)
	pool := NewStreamPool(0)
	for i := 0; i < 300; i++ {
		st, err := pool.Get(context.Background(), sess)
		if err != nil {
			t.Fatalf("allocation %d: %v", i+1, err)
		}
		if id := st.StreamID(); id < dynamicStreamIDFloor || id >= uint64(streams.StreamSwarm) {
			t.Fatalf("allocation %d returned stream ID %d, outside [%d, %d)", i+1, id, dynamicStreamIDFloor, streams.StreamSwarm)
		}
		_ = st.Reset(aether.ResetCancel)
	}
}

// With every dynamic ID held by an open stream, the next allocation fails and
// OpenStream is never asked for an application-band ID.
func TestDynamicStreamIDsRefuseWhenEveryIDIsHeld(t *testing.T) {
	sess := &idSession{}
	defer forgetSession(sess)
	pool := NewStreamPool(0)
	held := make([]aether.Stream, 0, 90)
	for i := 0; i < 90; i++ {
		st, err := pool.Get(context.Background(), sess)
		if err != nil {
			t.Fatalf("allocation %d: %v", i+1, err)
		}
		held = append(held, st)
	}
	st, err := pool.Get(context.Background(), sess)
	if err == nil {
		t.Fatalf("allocation 91 with every dynamic ID held returned stream %d, want an error", st.StreamID())
	}
	if !errors.Is(err, errDynamicStreamIDsExhausted) {
		t.Fatalf("allocation 91: err = %v, want errDynamicStreamIDsExhausted", err)
	}
	for _, id := range sess.openedIDs() {
		if id >= uint64(streams.StreamSwarm) {
			t.Fatalf("OpenStream was asked for stream ID %d", id)
		}
	}

	// Control: releasing one stream frees exactly its ID.
	freed := held[37]
	releaseDynamicStream(sess, freed)
	st, err = pool.Get(context.Background(), sess)
	if err != nil {
		t.Fatalf("allocation after a release: %v", err)
	}
	if st.StreamID() != freed.StreamID() {
		t.Fatalf("allocation after releasing ID %d returned %d", freed.StreamID(), st.StreamID())
	}
}

// noRouteFinder reports no routes, so HWPCaller.Call takes its sequential
// callOnce path (pooled stream) instead of the parallel probe.
type noRouteFinder struct{ mockFinder }

func (f *noRouteFinder) FindRoutes(context.Context, string, string) []aether.ProbeRoute { return nil }

// Every RPC over a dynamic stream resets it when the call is done, on both the
// parallel-probe path and the sequential callOnce path, so a session carries
// any number of sequential calls without exhausting the range or leaving it.
func TestCallResetsItsDynamicStream(t *testing.T) {
	resp, err := pb.MarshalResponse(&pb.RPCResponse{Success: true, Payload: []byte("pong")})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name   string
		finder func(aether.Session) SessionFinder
	}{
		{"probe", func(s aether.Session) SessionFinder { return &mockFinder{session: s} }},
		{"callOnce", func(s aether.Session) SessionFinder { return &noRouteFinder{mockFinder{session: s}} }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sess := &idSession{resp: resp}
			defer forgetSession(sess)
			caller := NewHWPCaller(tc.finder(sess))
			defer caller.Close()

			for i := 0; i < 250; i++ {
				out, err := caller.Call(context.Background(), "platform", "platform.CheckHealth", nil)
				if err != nil {
					t.Fatalf("call %d: %v", i+1, err)
				}
				if string(out) != "pong" {
					t.Fatalf("call %d returned %q", i+1, out)
				}
			}
			for _, id := range sess.openedIDs() {
				if id < dynamicStreamIDFloor || id >= uint64(streams.StreamSwarm) {
					t.Fatalf("a call opened stream ID %d", id)
				}
			}
		})
	}
}
