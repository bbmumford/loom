/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package compose

import (
	"context"
	"crypto/sha256"
	"fmt"
	"math"
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
	mu      sync.Mutex
	ctx     context.Context
	spawn   func(name string, fn func(ctx context.Context)) // rt.Go-style tracked spawner
	invoke  func(ctx context.Context, registration TriggerInvocation, event []byte) FunctionResult
	kinds   map[TriggerKind]KindHandler
	entries map[string]*triggerEntry // by Trigger.ID
	closed  bool
	nextGen uint64
}

type triggerEntry struct {
	trigger     Trigger
	generation  uint64
	specDigest  [sha256.Size]byte
	cancel      context.CancelFunc // non-nil once armed
	invocations int
	drained     *sync.Cond
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
	invoke func(ctx context.Context, registration TriggerInvocation, event []byte) FunctionResult,
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
	_, err := r.RegisterTriggerWithInvocation(t)
	return err
}

// RegisterTriggerWithInvocation stores one trigger and returns the exact,
// immutable registration evidence created by this registry issuance. Product
// composition uses the receipt to bind current authority without guessing the
// registry-local generation.
func (r *Registry) RegisterTriggerWithInvocation(
	t Trigger,
) (TriggerInvocation, error) {
	if t.ID == "" {
		return TriggerInvocation{}, fmt.Errorf("compose: trigger missing ID")
	}
	if t.Kind == "" || t.Function == "" {
		return TriggerInvocation{}, fmt.Errorf(
			"compose: trigger %q missing kind/function",
			t.ID,
		)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return TriggerInvocation{}, fmt.Errorf("compose: registry closed")
	}
	// Generation reuse would let a future byte-identical registration match
	// stale evidence. Exhaustion is permanent for this registry.
	if r.nextGen == math.MaxUint64 {
		return TriggerInvocation{}, fmt.Errorf(
			"compose: trigger registration generation exhausted",
		)
	}
	if old, ok := r.entries[t.ID]; ok {
		r.invalidateEntryLocked(old)
		// Removing the old pointer before waiting makes every later callback
		// fail its currentness check. RegisterTrigger does not publish the
		// replacement until every invocation that already won the old
		// generation lease has returned.
		delete(r.entries, t.ID)
		r.drainEntryLocked(old)
	}
	t.Spec = append([]byte(nil), t.Spec...)
	r.nextGen++
	e := &triggerEntry{
		trigger:    t,
		generation: r.nextGen,
		specDigest: sha256.Sum256(t.Spec),
	}
	e.drained = sync.NewCond(&r.mu)
	r.entries[t.ID] = e
	if h, ok := r.kinds[t.Kind]; ok {
		r.armLocked(e, h)
	}
	return triggerInvocation(e), nil
}

// RemoveTrigger disarms and forgets an instance. Unknown IDs are a no-op.
func (r *Registry) RemoveTrigger(id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e, ok := r.entries[id]; ok {
		r.invalidateEntryLocked(e)
		delete(r.entries, id)
		r.drainEntryLocked(e)
	}
	return nil
}

// RemoveTriggerExact disarms and forgets only the registration represented by
// invocation. A stale product rollback cannot remove a same-ID replacement.
func (r *Registry) RemoveTriggerExact(invocation TriggerInvocation) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.entries[invocation.ID]
	if !ok || triggerInvocation(e) != invocation {
		return fmt.Errorf("compose: trigger registration is no longer current")
	}
	r.invalidateEntryLocked(e)
	delete(r.entries, invocation.ID)
	r.drainEntryLocked(e)
	return nil
}

// CurrentTriggerInvocation returns exact immutable evidence only while the
// registration exists. Product policy still decides whether pending or armed
// instances are authorized; the registry receipt itself mints nothing.
func (r *Registry) CurrentTriggerInvocation(
	id string,
) (TriggerInvocation, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	e, ok := r.entries[id]
	if !ok || r.closed {
		return TriggerInvocation{}, false
	}
	return triggerInvocation(e), true
}

// Triggers enumerates registered instances (armed and pending).
func (r *Registry) Triggers() []Trigger {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]Trigger, 0, len(r.entries))
	for _, e := range r.entries {
		t := e.trigger
		t.Spec = append([]byte(nil), t.Spec...)
		out = append(out, t)
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
		r.invalidateEntryLocked(e)
	}
	for _, e := range r.entries {
		r.drainEntryLocked(e)
	}
}

func (r *Registry) invalidateEntryLocked(e *triggerEntry) {
	if e == nil || e.cancel == nil {
		return
	}
	e.cancel()
	e.cancel = nil
}

func (r *Registry) drainEntryLocked(e *triggerEntry) {
	if e == nil || e.drained == nil {
		return
	}
	for e.invocations > 0 {
		e.drained.Wait()
	}
}

func (r *Registry) releaseInvocation(e *triggerEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if e == nil || e.invocations <= 0 {
		panic("compose: trigger invocation lease underflow")
	}
	e.invocations--
	e.drained.Broadcast()
}

// armLocked starts the instance's watch goroutine. Caller holds r.mu.
func (r *Registry) armLocked(e *triggerEntry, h KindHandler) {
	instCtx, cancel := context.WithCancel(r.ctx)
	e.cancel = cancel
	t := e.trigger
	invocation := triggerInvocation(e)
	invoke := func(
		ctx context.Context,
		fn FunctionID,
		event []byte,
	) FunctionResult {
		if fn != invocation.Function {
			return FunctionResult{
				Kind: ResultFailure,
				Err:  "compose: trigger attempted a function outside its registration",
			}
		}
		// Kind handlers may retain this callback after their instance has
		// been canceled. Cancellation alone is not an authority boundary:
		// an old handler can ignore it, and an identical re-registration can
		// otherwise make stale evidence appear current. Linearize every fire
		// against the registry immediately before product dispatch.
		r.mu.Lock()
		current := r.entries[invocation.ID]
		live := !r.closed &&
			current == e &&
			current.generation == invocation.RegistrationGeneration &&
			current.cancel != nil &&
			instCtx.Err() == nil
		if live {
			e.invocations++
		}
		r.mu.Unlock()
		if !live {
			return FunctionResult{
				Kind: ResultFailure,
				Err:  "compose: trigger registration is no longer current",
			}
		}
		defer r.releaseInvocation(e)
		return r.invoke(ctx, invocation, event)
	}
	r.spawn("compose.trigger."+t.ID, func(_ context.Context) {
		// Arm blocks for the instance's lifetime; instCtx tears it down.
		err := h.Arm(instCtx, t, invoke)
		// Returning from Arm ends this exact instance even when it returns
		// nil. Clear its live marker only if the entry has not been replaced;
		// a retained callback must fail from this point onward.
		r.mu.Lock()
		current := r.entries[t.ID]
		stillCurrent := current == e
		if stillCurrent {
			r.invalidateEntryLocked(current)
		}
		// Arm owns the instance resource. If it returns while a callback it
		// launched is still in product authorization/dispatch, keep the arm
		// goroutine alive until that exact invocation lease drains.
		r.drainEntryLocked(e)
		wanted := stillCurrent && !r.closed && instCtx.Err() == nil
		r.mu.Unlock()
		if err != nil && wanted {
			// Arm failed while the instance was still wanted — it is now
			// parked pending so a future RegisterKind (e.g. after a
			// dependency comes up) can retry.
			dbgCompose.Printf("trigger %s (kind %s) disarmed with error: %v", t.ID, t.Kind, err)
		}
	})
}

func triggerInvocation(e *triggerEntry) TriggerInvocation {
	if e == nil {
		return TriggerInvocation{}
	}
	t := e.trigger
	return TriggerInvocation{
		ID:                     t.ID,
		Kind:                   t.Kind,
		Function:               t.Function,
		RegistrationRevision:   t.RegistrationRevision,
		RegistrationGeneration: e.generation,
		SpecDigest:             e.specDigest,
	}
}
