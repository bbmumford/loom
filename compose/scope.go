/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package compose

// ScopeTracker is the begin/end-worker-scope mechanism: registrations made
// between Begin and End are attributed to the owner, so teardown can remove
// exactly the function IDs one role (or worker) registered — nothing more,
// nothing less.
//
// IT IS WHERE A PER-ROLE STOP HAS TO COME FROM, because nothing else withholds
// a handler once it is registered. SetEnabledRoles records a set and
// HandlerRegistry.IsRoleEnabled is the only thing that reads it; no caller on
// the dispatch path consults either, so the set is written and never asked.
// ⇒ A node that activated a role keeps serving it whatever its enabled set says,
// and a design treating SetEnabledRoles as a runtime stop rests on nothing.
// Removing the registrations is the only thing that stops a role, and the mesh
// has no other way to know which handlers belong to which one.
//
// Implementations hook the registration path (rpc.Registry.Register and the
// handler-registry adapter) rather than requiring callers to report IDs
// manually — a role activator that forgets to report would silently leak
// handlers past its own teardown.
type ScopeTracker interface {
	// Begin opens a capture scope for owner (e.g. "role:auth"). Nested or
	// concurrent scopes for distinct owners must not bleed into each other.
	Begin(owner string)

	// End closes the owner's scope and returns every FunctionID registered
	// while it was open.
	End(owner string) []FunctionID

	// Owned returns the IDs captured for owner across its completed scopes.
	Owned(owner string) []FunctionID
}
