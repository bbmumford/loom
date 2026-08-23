/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	aether "github.com/ORBTR/aether"
	"github.com/bbmumford/swarm"
)

// fakeStream is an aether.Stream whose Receive is driven by the test: it
// yields whatever is pushed onto frames, and returns on ctx cancellation.
type fakeStream struct {
	id     uint64
	frames chan []byte

	mu     sync.Mutex
	closed bool
}

func newFakeStream(id uint64) *fakeStream {
	return &fakeStream{id: id, frames: make(chan []byte, 8)}
}

func (s *fakeStream) StreamID() uint64 { return s.id }

func (s *fakeStream) Send(ctx context.Context, data []byte) error { return nil }

func (s *fakeStream) Receive(ctx context.Context) ([]byte, error) {
	select {
	case f := <-s.frames:
		return f, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (s *fakeStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

func (s *fakeStream) Reset(aether.ResetReason) error       { return nil }
func (s *fakeStream) SetPriority(weight uint8, dep uint64) {}
func (s *fakeStream) Config() aether.StreamConfig          { return swarmStreamConfig() }
func (s *fakeStream) IsOpen() bool                         { return true }
func (s *fakeStream) Conn() net.Conn                       { return nil }

// Stop must JOIN the per-peer read loops, not merely signal them.
//
// Without the join, Stop returns while a read loop is still inside the
// OnReceive callback — which reaches into the swarm engine the caller has just
// finished shutting down. The caller has no way to know: the transport reports
// itself stopped, the map is empty, and the frame lands in a half-torn-down
// engine afterwards. This is the same defect class as swarm's own blocker 1
// ("join its goroutines"), on the loom side of the seam.
func TestStopJoinsReadLoopsBeforeReturning(t *testing.T) {
	tp := NewMeshSwarmTransport(swarm.NodeID("self"))

	inCallback := make(chan struct{})
	release := make(chan struct{})
	var callbackActive atomic.Bool
	var afterStop atomic.Bool

	tp.OnReceive(func(from swarm.NodeID, frame []byte) {
		callbackActive.Store(true)
		close(inCallback)
		<-release // hold the read loop inside the callback
		if afterStop.Load() {
			t.Errorf("a receiver callback was still running after Stop() returned — " +
				"the read loop was signalled but never joined")
		}
		callbackActive.Store(false)
	})

	st := newFakeStream(100)
	tp.AcceptPeer(swarm.NodeID("peer-1"), nil, st)

	// Drive one frame into the read loop and wait until it is inside the
	// callback, so Stop is guaranteed to race a live loop.
	st.frames <- []byte("frame")
	select {
	case <-inCallback:
	case <-time.After(3 * time.Second):
		t.Fatal("read loop never entered the receiver callback — the test would be vacuous")
	}
	if !callbackActive.Load() {
		t.Fatal("callback not active; the race this test creates did not happen")
	}

	stopped := make(chan struct{})
	go func() {
		tp.Stop()
		close(stopped)
	}()

	// Stop must NOT return while the callback is still held.
	select {
	case <-stopped:
		afterStop.Store(true)
		close(release)
		t.Fatal("Stop() returned while a read loop was still inside the receiver " +
			"callback — shutdown does not join the read loops")
	case <-time.After(200 * time.Millisecond):
		// Correct: Stop is waiting. Release the loop and let it finish.
	}
	close(release)

	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop() did not return after the read loop was released — the join deadlocked")
	}
	afterStop.Store(true)
	if callbackActive.Load() {
		t.Fatal("Stop() returned with a callback still active")
	}
}

// Stop must stay safe to call twice — the shutdown path is reached from both
// the runtime teardown and deferred cleanups.
func TestStopIsIdempotent(t *testing.T) {
	tp := NewMeshSwarmTransport(swarm.NodeID("self"))
	st := newFakeStream(100)
	tp.AcceptPeer(swarm.NodeID("peer-1"), nil, st)

	done := make(chan struct{})
	go func() {
		tp.Stop()
		tp.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a second Stop() blocked or panicked")
	}
}

// Attaching a peer after Stop must be refused, not silently accepted. A late
// attach would spawn a read loop that outlives shutdown — and would race
// wg.Add against the wg.Wait already in progress, which the race detector
// reports and the runtime may panic on.
func TestAttachAfterStopIsRefused(t *testing.T) {
	tp := NewMeshSwarmTransport(swarm.NodeID("self"))
	tp.Stop()

	st := newFakeStream(100)
	tp.AcceptPeer(swarm.NodeID("late-peer"), nil, st)

	if peers := tp.Peers(); len(peers) != 0 {
		t.Fatalf("transport attached %d peer(s) after Stop: %v", len(peers), peers)
	}
	st.mu.Lock()
	closed := st.closed
	st.mu.Unlock()
	if !closed {
		t.Fatal("the refused peer's stream was not closed — the transport retained a live stream after shutdown")
	}
}

// The join/leave pairing contract, across all three teardown paths. The
// consumer is swarm's peer table (Ensure on join, Remove on leave), so what
// must hold is "no leave for a peer that is still present" — not a naive
// one-join-one-leave count.
//
// Case 2 is the one with teeth and it had NO test: a grade upgrade
// (WS -> noise-UDP) replaces the stream for the SAME peer, and firing the old
// loop's leave would Remove a peer that was just promoted — every later Send
// then reports "not registered" and the δ-CRDT pump never writes to the new
// session. That suppression lived only in a comment.
func TestJoinLeavePairingAcrossTeardownPaths(t *testing.T) {
	newTransport := func() (*MeshSwarmTransport, *[]string, *sync.Mutex) {
		tp := NewMeshSwarmTransport(swarm.NodeID("self"))
		var mu sync.Mutex
		var events []string
		tp.OnPeerJoin(func(id swarm.NodeID) {
			mu.Lock()
			events = append(events, "join:"+string(id))
			mu.Unlock()
		})
		tp.OnPeerLeave(func(id swarm.NodeID) {
			mu.Lock()
			events = append(events, "leave:"+string(id))
			mu.Unlock()
		})
		return tp, &events, &mu
	}
	read := func(mu *sync.Mutex, ev *[]string) []string {
		mu.Lock()
		defer mu.Unlock()
		return append([]string(nil), *ev...)
	}
	waitFor := func(t *testing.T, mu *sync.Mutex, ev *[]string, want int) []string {
		t.Helper()
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			if got := read(mu, ev); len(got) >= want {
				return got
			}
			time.Sleep(5 * time.Millisecond)
		}
		return read(mu, ev)
	}

	t.Run("plain disconnect pairs join with leave", func(t *testing.T) {
		tp, ev, mu := newTransport()
		defer tp.Stop()
		st := newFakeStream(100)
		tp.AcceptPeer(swarm.NodeID("p"), nil, st)
		waitFor(t, mu, ev, 1)

		// End the peer's read loop the way a dropped session does.
		tp.UnregisterPeer(swarm.NodeID("p"))
		got := waitFor(t, mu, ev, 2)
		if len(got) != 2 || got[0] != "join:p" || got[1] != "leave:p" {
			t.Fatalf("events = %v, want exactly [join:p leave:p]", got)
		}
	})

	t.Run("stream replacement does NOT leave the promoted peer", func(t *testing.T) {
		tp, ev, mu := newTransport()
		defer tp.Stop()
		tp.AcceptPeer(swarm.NodeID("p"), nil, newFakeStream(100))
		waitFor(t, mu, ev, 1)

		// The upgrade: same peer, new stream. The old read loop exits and runs
		// its teardown, which must find itself superseded and stay silent.
		tp.AcceptPeer(swarm.NodeID("p"), nil, newFakeStream(101))
		waitFor(t, mu, ev, 2)
		time.Sleep(150 * time.Millisecond) // give any stray leave time to fire

		got := read(mu, ev)
		for _, e := range got {
			if e == "leave:p" {
				t.Fatalf("events = %v — the superseded read loop emitted a leave for a "+
					"peer that is still present; the promoted session would be dropped", got)
			}
		}
		if peers := tp.Peers(); len(peers) != 1 || peers[0] != swarm.NodeID("p") {
			t.Fatalf("peers = %v, want the promoted peer still registered", peers)
		}
	})

	t.Run("explicit unregister emits exactly one leave", func(t *testing.T) {
		tp, ev, mu := newTransport()
		defer tp.Stop()
		tp.AcceptPeer(swarm.NodeID("p"), nil, newFakeStream(100))
		waitFor(t, mu, ev, 1)

		tp.UnregisterPeer(swarm.NodeID("p"))
		waitFor(t, mu, ev, 2)
		// The read loop exits AFTER the explicit unregister and runs its own
		// teardown; it must not emit a second leave.
		time.Sleep(150 * time.Millisecond)

		got := read(mu, ev)
		leaves := 0
		for _, e := range got {
			if e == "leave:p" {
				leaves++
			}
		}
		if leaves != 1 {
			t.Fatalf("events = %v — %d leaves for one peer, want exactly 1", got, leaves)
		}
	})
}
