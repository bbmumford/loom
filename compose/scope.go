/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package compose

// ScopeTracker is iii's begin/end-worker-scope mechanism: registrations made
// between Begin and End are attributed to the owner, so teardown can remove
// exactly the function IDs one role (or worker) registered — nothing more,
// nothing less. This is the mechanism that gives Phase-1 role takeover a
// real per-role STOP: today SetEnabledRoles only gates dispatch, it does not
// unregister, and the mesh has no other way to know which handlers belong
// to which role.
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
