/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package compose

import "sync"

// Tracker is the standard ScopeTracker. It is wired into the registration
// path (the registrar calls Note on every successful registration) so
// activators cannot forget to report; Begin/End bracket an owner's
// registration window.
//
// Concurrency: multiple owners may hold open scopes simultaneously (two
// roles activating in parallel). A registration is attributed to the owner
// named at Note time — the registrar passes the owner explicitly, so
// concurrent activations don't bleed into each other the way a single
// "current scope" global would.
type Tracker struct {
	mu    sync.Mutex
	open  map[string][]FunctionID // owner → IDs captured in the open scope
	owned map[string][]FunctionID // owner → IDs across completed scopes
}

// NewTracker returns an empty tracker.
func NewTracker() *Tracker {
	return &Tracker{
		open:  make(map[string][]FunctionID),
		owned: make(map[string][]FunctionID),
	}
}

// Begin opens a capture scope for owner. Re-opening an already-open scope
// is a no-op (the existing captures are kept).
func (t *Tracker) Begin(owner string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.open[owner]; !ok {
		t.open[owner] = []FunctionID{}
	}
}

// Note attributes a registered function ID to owner's open scope. Called by
// the registration path, not by activators. A Note with no open scope for
// owner attributes directly to the owner's completed set — registration
// outside a bracket is still owned, never lost.
func (t *Tracker) Note(owner string, id FunctionID) {
	if owner == "" || id == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if ids, ok := t.open[owner]; ok {
		t.open[owner] = append(ids, id)
		return
	}
	t.owned[owner] = appendUnique(t.owned[owner], id)
}

// End closes owner's scope and returns the IDs captured while it was open.
// The captures also merge into the owner's completed set (Owned).
func (t *Tracker) End(owner string) []FunctionID {
	t.mu.Lock()
	defer t.mu.Unlock()
	ids := t.open[owner]
	delete(t.open, owner)
	for _, id := range ids {
		t.owned[owner] = appendUnique(t.owned[owner], id)
	}
	return append([]FunctionID(nil), ids...)
}

// Owned returns every ID attributed to owner across completed scopes.
func (t *Tracker) Owned(owner string) []FunctionID {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]FunctionID(nil), t.owned[owner]...)
}

// Release forgets owner's completed set (after a successful teardown
// unregistered them) and returns what was forgotten.
func (t *Tracker) Release(owner string) []FunctionID {
	t.mu.Lock()
	defer t.mu.Unlock()
	ids := t.owned[owner]
	delete(t.owned, owner)
	return ids
}

func appendUnique(ids []FunctionID, id FunctionID) []FunctionID {
	for _, existing := range ids {
		if existing == id {
			return ids
		}
	}
	return append(ids, id)
}
