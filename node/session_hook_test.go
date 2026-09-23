/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/ORBTR/aether"
	"github.com/bbmumford/loom/core/streams"
	"github.com/bbmumford/loom/pkg/rpc"
)

// hookEvent is one session event as a subscriber saw it.
type hookEvent struct {
	nodeID  string
	session aether.Session
	joined  bool
}

// hookRecorder is a session-hook subscriber that keeps every event it receives.
type hookRecorder struct {
	mu     sync.Mutex
	events []hookEvent
}

func (r *hookRecorder) hook(nodeID string, session aether.Session, joined bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, hookEvent{nodeID: nodeID, session: session, joined: joined})
}

func (r *hookRecorder) snapshot() []hookEvent {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]hookEvent(nil), r.events...)
}

// streamIDSession records the stream IDs the swarm transport opens and claims
// on it, and hands back a live fake stream so attachPeer runs.
type streamIDSession struct {
	*probeSession
	mu       sync.Mutex
	opened   []uint64
	accepted []uint64
}

func (s *streamIDSession) OpenStream(_ context.Context, cfg aether.StreamConfig) (aether.Stream, error) {
	s.mu.Lock()
	s.opened = append(s.opened, cfg.StreamID)
	s.mu.Unlock()
	return newFakeStream(cfg.StreamID), nil
}

func (s *streamIDSession) AcceptStreamByID(_ context.Context, streamID uint64) (aether.Stream, error) {
	s.mu.Lock()
	s.accepted = append(s.accepted, streamID)
	s.mu.Unlock()
	return newFakeStream(streamID), nil
}

func (s *streamIDSession) ids() (opened, accepted []uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]uint64(nil), s.opened...), append([]uint64(nil), s.accepted...)
}

// newSwarmHookRuntime runs InitSwarm on a runtime that has a
// ConnectionManager, so swarm subscribes to session events exactly as
// Initialize wires it.
func newSwarmHookRuntime(t *testing.T) (*Runtime, *ConnectionManager) {
	t.Helper()
	rt := newPeerPublicationTestRuntime(t)
	m := registerTestManager()
	rt.connMgr = m
	if _, err := rt.InitSwarm(rpc.NewRegistry()); err != nil {
		t.Fatalf("InitSwarm: %v", err)
	}
	return rt, m
}

func waitForSwarmPeers(t *testing.T, tr *MeshSwarmTransport, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for probePeerCount(tr) != want && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := probePeerCount(tr); got != want {
		t.Fatalf("swarm transport peers = %d, want %d", got, want)
	}
}

// Swarm subscribes in InitSwarm; a second subscriber added afterwards must not
// displace it. Both must see the join (swarm binds its stream) and the leave
// (swarm unbinds), for either side of the NodeID tie-break.
func TestSessionHookFanOutReachesSwarmAndASecondSubscriber(t *testing.T) {
	for _, tc := range []struct {
		name      string
		initiator bool // this node has the lower NodeID and opens the swarm stream
	}{
		{name: "initiator", initiator: true},
		{name: "responder", initiator: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rt, m := newSwarmHookRuntime(t)
			second := &hookRecorder{}
			remove := m.AddSessionHook(second.hook)
			defer remove()

			self := string(rt.identity.NodeID)
			peer := self + "~" // sorts above self
			if !tc.initiator {
				peer = self[:len(self)-1] // a proper prefix sorts below self
			}
			sess := &streamIDSession{probeSession: noiseSession()}
			if !m.registerMeshSession(context.Background(), peer, sess, tc.initiator) {
				t.Fatal("registerMeshSession refused a fresh session")
			}
			// AcceptMeshConnection's post-streams join notification.
			m.fireSessionHook(peer, sess, true)

			waitForSwarmPeers(t, rt.swarm.Transport, 1)
			opened, accepted := sess.ids()
			if tc.initiator {
				if !reflect.DeepEqual(opened, []uint64{streams.StreamSwarm}) || len(accepted) != 0 {
					t.Fatalf("initiator opened %v accepted %v, want opened [%d] only", opened, accepted, streams.StreamSwarm)
				}
			} else if !reflect.DeepEqual(accepted, []uint64{streams.StreamSwarm}) || len(opened) != 0 {
				t.Fatalf("responder opened %v accepted %v, want accepted [%d] only", opened, accepted, streams.StreamSwarm)
			}

			// The real removal path fires the leave after dispatch cleanup.
			if !m.unregisterMeshSession(peer, sess) {
				t.Fatal("unregisterMeshSession did not remove the registered session")
			}
			waitForSwarmPeers(t, rt.swarm.Transport, 0)

			want := []hookEvent{
				{nodeID: peer, session: sess, joined: true},
				{nodeID: peer, session: nil, joined: false},
			}
			if got := second.snapshot(); !reflect.DeepEqual(got, want) {
				t.Fatalf("second subscriber saw %+v, want %+v", got, want)
			}
		})
	}
}

func TestAddSessionHookRemoverStopsOnlyItsOwnSubscription(t *testing.T) {
	m := registerTestManager()
	a, b := &hookRecorder{}, &hookRecorder{}
	removeA := m.AddSessionHook(a.hook)
	m.AddSessionHook(b.hook)
	m.AddSessionHook(nil)()

	m.fireSessionHook("p1", nil, false)
	removeA()
	removeA() // idempotent: must not remove anyone else
	m.fireSessionHook("p2", nil, false)

	if got := a.snapshot(); len(got) != 1 || got[0].nodeID != "p1" {
		t.Fatalf("removed subscriber saw %+v, want only p1", got)
	}
	if got := b.snapshot(); len(got) != 2 || got[0].nodeID != "p1" || got[1].nodeID != "p2" {
		t.Fatalf("remaining subscriber saw %+v, want p1 then p2", got)
	}
}

// SetSessionHook's slot is replaceable, but replacing or clearing it must never
// silence an AddSessionHook subscriber.
func TestSetSessionHookReplacesOnlyItsOwnSlot(t *testing.T) {
	m := registerTestManager()
	first, replacement, added := &hookRecorder{}, &hookRecorder{}, &hookRecorder{}
	m.SetSessionHook(first.hook)
	m.AddSessionHook(added.hook)
	m.SetSessionHook(replacement.hook)
	m.fireSessionHook("p1", nil, false)
	m.SetSessionHook(nil)
	m.fireSessionHook("p2", nil, false)

	if got := first.snapshot(); len(got) != 0 {
		t.Fatalf("replaced slot callback still fired: %+v", got)
	}
	if got := replacement.snapshot(); len(got) != 1 || got[0].nodeID != "p1" {
		t.Fatalf("slot callback saw %+v, want only p1", got)
	}
	if got := added.snapshot(); len(got) != 2 {
		t.Fatalf("added subscriber saw %+v, want p1 and p2", got)
	}
}

// Subscribers run with no hook lock held, so one may add or remove
// subscriptions from inside its callback. A subscription added during a
// delivery starts with the next event.
func TestSessionHookSubscriberMayReenterSubscription(t *testing.T) {
	m := registerTestManager()
	late := &hookRecorder{}
	var removeSelf func()
	removeSelf = m.AddSessionHook(func(string, aether.Session, bool) {
		removeSelf()
		m.AddSessionHook(late.hook)
	})

	done := make(chan struct{})
	go func() {
		m.fireSessionHook("p1", nil, false)
		m.fireSessionHook("p2", nil, false)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("a subscriber re-entering AddSessionHook deadlocked the fire")
	}
	if got := late.snapshot(); len(got) != 1 || got[0].nodeID != "p2" {
		t.Fatalf("subscription added mid-delivery saw %+v, want only p2", got)
	}
}

// Adds, removals and deliveries race freely; run under -race.
func TestSessionHookConcurrentSubscribeAndFire(t *testing.T) {
	m := registerTestManager()
	stable := &hookRecorder{}
	m.AddSessionHook(stable.hook)

	const fires = 200
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < fires; i++ {
			m.AddSessionHook(func(string, aether.Session, bool) {})()
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < fires; i++ {
			m.fireSessionHook("p", nil, false)
		}
	}()
	wg.Wait()
	if got := len(stable.snapshot()); got != fires {
		t.Fatalf("stable subscriber saw %d events, want %d", got, fires)
	}
}
