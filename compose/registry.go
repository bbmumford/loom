/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package compose

import (
	"context"
	"fmt"
	"sync"
)

// Registry is the standard TriggerRegistry: kind handlers arm instances;
// instances registered before their kind park as pending and replay when
// the kind arrives (late binding, order-independent composition).
//
// Arming runs each instance on its own tracked goroutine bound to the
// runtime lifetime ctx supplied at construction — KindHandler.Arm blocks
// while the instance is live and returns to tear down.
type Registry struct {
	mu       sync.Mutex
	ctx      context.Context
	spawn    func(name string, fn func(ctx context.Context)) // rt.Go-style tracked spawner
	invoke   func(ctx context.Context, fn FunctionID, event []byte) FunctionResult
	kinds    map[TriggerKind]KindHandler
	entries  map[string]*triggerEntry // by Trigger.ID
	closed   bool
}

type triggerEntry struct {
	trigger Trigger
	cancel  context.CancelFunc // non-nil once armed
}

// NewRegistry builds a trigger registry.
//
//   - ctx is the runtime lifetime context (rt.Context()); every armed
//     instance's watch goroutine binds it, never a caller ctx (H7 rule).
//   - spawn launches tracked goroutines (rt.Go). Must not be nil.
//   - invoke dispatches a FunctionID with an event payload — wire it to the
//     local dispatch path (registry.Dispatch / rpc.Call fallthrough).
func NewRegistry(
	ctx context.Context,
	spawn func(name string, fn func(ctx context.Context)),
	invoke func(ctx context.Context, fn FunctionID, event []byte) FunctionResult,
) *Registry {
	return &Registry{
		ctx:     ctx,
		spawn:   spawn,
		invoke:  invoke,
		kinds:   make(map[TriggerKind]KindHandler),
		entries: make(map[string]*triggerEntry),
	}
}

// RegisterKind installs the handler for a kind and replays pending
// instances of that kind against it. Registering the same kind twice
// replaces the handler for FUTURE instances only (armed instances keep
// running on the old handler until removed — tearing them down implicitly
// would drop live triggers on a hot swap).
func (r *Registry) RegisterKind(kind TriggerKind, h KindHandler) error {
	if h == nil {
		return fmt.Errorf("compose: nil handler for kind %q", kind)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return fmt.Errorf("compose: registry closed")
	}
	r.kinds[kind] = h
	// Late binding: replay parked instances of this kind.
	for _, e := range r.entries {
		if e.trigger.Kind == kind && e.cancel == nil {
			r.armLocked(e, h)
		}
	}
	return nil
}

// RegisterTrigger stores + arms an instance (parked pending if its kind is
// not yet registered). IDs are unique; re-registering an ID replaces the
// instance (old one is torn down first).
func (r *Registry) RegisterTrigger(t Trigger) error {
	if t.ID == "" {
		return fmt.Errorf("compose: trigger missing ID")
	}
	if t.Kind == "" || t.Function == "" {
		return fmt.Errorf("compose: trigger %q missing kind/function", t.ID)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return fmt.Errorf("compose: registry closed")
	}
	if old, ok := r.entries[t.ID]; ok && old.cancel != nil {
		old.cancel()
	}
	e := &triggerEntry{trigger: t}
	r.entries[t.ID] = e
	if h, ok := r.kinds[t.Kind]; ok {
		r.armLocked(e, h)
	}
	return nil
}

// RemoveTrigger disarms and forgets an instance. Unknown IDs are a no-op.
func (r *Registry) RemoveTrigger(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e, ok := r.entries[id]; ok {
		if e.cancel != nil {
			e.cancel()
		}
		delete(r.entries, id)
	}
	return nil
}

// Triggers enumerates registered instances (armed and pending).
func (r *Registry) Triggers() []Trigger {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Trigger, 0, len(r.entries))
	for _, e := range r.entries {
		out = append(out, e.trigger)
	}
	return out
}

// Armed reports whether an instance is currently armed (vs parked pending).
func (r *Registry) Armed(id string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.entries[id]
	return ok && e.cancel != nil
}

// Close tears down every armed instance. The registry rejects further
// registrations; used on runtime shutdown.
func (r *Registry) Close() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	for _, e := range r.entries {
		if e.cancel != nil {
			e.cancel()
			e.cancel = nil
		}
	}
}

// armLocked starts the instance's watch goroutine. Caller holds r.mu.
func (r *Registry) armLocked(e *triggerEntry, h KindHandler) {
	instCtx, cancel := context.WithCancel(r.ctx)
	e.cancel = cancel
	t := e.trigger
	r.spawn("compose.trigger."+t.ID, func(_ context.Context) {
		// Arm blocks for the instance's lifetime; instCtx tears it down.
		if err := h.Arm(instCtx, t, r.invoke); err != nil && instCtx.Err() == nil {
			// Arm failed while the instance was still wanted — park it
			// back to pending so a future RegisterKind (e.g. after a
			// dependency comes up) can retry, and surface the error to
			// the debug log rather than dropping it silently.
			r.mu.Lock()
			if cur, ok := r.entries[t.ID]; ok && cur == e {
				cur.cancel = nil
			}
			r.mu.Unlock()
			dbgCompose.Printf("trigger %s (kind %s) disarmed with error: %v", t.ID, t.Kind, err)
		}
	})
}
