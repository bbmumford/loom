/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"sync"

	"github.com/ORBTR/aether"
)

// sessionHookFunc is the shape of a session lifecycle subscriber.
// On registration session is non-nil and joined is true.
// On unregistration session is nil and joined is false.
type sessionHookFunc = func(nodeID string, session aether.Session, joined bool)

// sessionHookSet holds every subscriber to ConnectionManager's session
// install/remove events: the one replaceable SetSessionHook slot plus any
// number of AddSessionHook subscriptions.
//
// subs is replaced on every add or remove and never mutated in place, so
// fire iterates a snapshot taken under a read lock and calls subscribers
// with no lock held. A subscriber may therefore add or remove
// subscriptions from inside its callback; with the read lock still held,
// that call's write lock would wait on it forever.
type sessionHookSet struct {
	mu     sync.RWMutex
	single sessionHookFunc
	subs   []sessionHookSub
	nextID uint64
}

// sessionHookSub is one AddSessionHook subscription. id lets the remover
// find its own entry after other subscriptions have come and gone.
type sessionHookSub struct {
	id uint64
	fn sessionHookFunc
}

// SetSessionHook installs the callback in a single replaceable slot: a
// later call replaces the previous callback, and nil clears it. The slot
// fires alongside every AddSessionHook subscriber, so replacing it never
// silences them. Subscribers that must coexist use AddSessionHook.
//
// Callbacks fire after the dispatch lock is released and outside any
// critical path, so they may call back into the manager: a removal
// callback may re-admit the peer through registerMeshSession
// (session_reapers_test drives exactly that re-entry). A join fires on the
// goroutine admitting the session (AcceptMeshConnection), before that
// session's service loops start; a leave fires on the goroutine tearing it
// down (AcceptMeshConnection's cleanup once its gossip loop exits, or
// sweepZombieSessions). Either way a callback must not block.
func (m *ConnectionManager) SetSessionHook(fn func(nodeID string, session aether.Session, joined bool)) {
	m.sessionHooks.mu.Lock()
	defer m.sessionHooks.mu.Unlock()
	m.sessionHooks.single = fn
}

// AddSessionHook subscribes fn to every session install and removal
// alongside all other subscribers, and returns a func that removes this
// subscription. The remover is idempotent. A nil fn subscribes nothing
// and returns a no-op remover.
//
// Subscribers fire in registration order, after the SetSessionHook slot.
// An event already being delivered when the remover runs may still reach
// fn once. The swarm transport (InitSwarm) subscribes here so another
// subscriber — one per application stream that binds per peer — cannot
// displace it.
func (m *ConnectionManager) AddSessionHook(fn func(nodeID string, session aether.Session, joined bool)) (remove func()) {
	if fn == nil {
		return func() {}
	}
	h := &m.sessionHooks
	h.mu.Lock()
	h.nextID++
	id := h.nextID
	next := make([]sessionHookSub, len(h.subs), len(h.subs)+1)
	copy(next, h.subs)
	h.subs = append(next, sessionHookSub{id: id, fn: fn})
	h.mu.Unlock()
	return func() { h.remove(id) }
}

// remove drops the subscription with the given id; an id already removed
// is a no-op.
func (h *sessionHookSet) remove(id uint64) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for i, s := range h.subs {
		if s.id != id {
			continue
		}
		next := make([]sessionHookSub, 0, len(h.subs)-1)
		next = append(next, h.subs[:i]...)
		h.subs = append(next, h.subs[i+1:]...)
		return
	}
}

// fireSessionHook delivers one session event to the SetSessionHook slot
// and then to every AddSessionHook subscriber, with no lock held.
func (m *ConnectionManager) fireSessionHook(nodeID string, session aether.Session, joined bool) {
	m.sessionHooks.mu.RLock()
	single := m.sessionHooks.single
	subs := m.sessionHooks.subs
	m.sessionHooks.mu.RUnlock()
	if single != nil {
		single(nodeID, session, joined)
	}
	for _, s := range subs {
		s.fn(nodeID, session, joined)
	}
}
