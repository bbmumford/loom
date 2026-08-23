/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

// Package scope holds the single canonical TenantScope enum shared by the
// rpc package (rpc.TenantScope, rpc.Scope*) and the legacy handlers package
// (mesh/node/handlers.TenantScope, handlers.TenantScope*). Both surfaces
// re-export the symbols declared here via Go type aliases and constant
// forwards, so a value round-trips between the two packages without any
// possibility of drift.
//
// Finding rev-069 previously flagged two parallel TenantScope enums:
//   - rpc.TenantScope (6 tiers, includes Profile + fail-closed Unknown)
//   - handlers.TenantScope (5 tiers, missing Profile)
//
// A handler migrated between the two silently lost the Profile tier and
// broke the EnforceScope vs interface-method contract. This subpackage
// eliminates the split at the source: rpc.TenantScope and
// handlers.TenantScope are now the SAME type identity (alias -> alias ->
// this package's declaration). The rpc surface remains the canonical
// caller-facing import — new code should use rpc.Scope* — but the scope
// subpackage lets the handlers alias exist without an import cycle
// (rpc/adapter.go imports handlers, so handlers cannot import rpc).
//
// New scope tiers MUST be added HERE (not in rpc or handlers) so both
// re-exports pick them up simultaneously.
package scope

// TenantScope controls tenant isolation enforcement at the handler dispatch
// level. It is deliberately a defined type: an arbitrary string must not be
// assignable as a scope declaration, and its zero value must remain detectably
// invalid. The string representation still survives JSON / protobuf / wire
// round-trips without a bespoke encoder.
type TenantScope string

// Canonical scope constants. String values are load-bearing wire tokens —
// they appear in proto descriptors, telemetry, and the reflection
// registry, so they MUST NOT change without a coordinated redeploy.
//
// The rpc package re-exports these as rpc.Scope{None,Platform,Tenant,Org,
// User,Profile,Unknown}; the handlers package re-exports them as
// handlers.TenantScope{None,Platform,Tenant,Org,User,Profile,Unknown}.
// Both surfaces MUST stay 1:1 with this list.
const (
	// Unset is the invalid zero value. It means no tenant-enforcement decision
	// was made and MUST be rejected at registration. It is not re-exported as
	// a caller-facing rpc.Scope* constant: callers must choose a real tier.
	Unset TenantScope = ""

	// None opts a handler out of tenant enforcement. Used for liveness,
	// /heartbeat, OAuth init and other public unauthenticated surfaces. Its
	// non-empty token makes that opt-out distinguishable from omission.
	None TenantScope = "none"

	// Platform restricts a handler to the HSTLES platform itself (tenant
	// id is in the configured platform-tenants set). Non-platform tenants
	// are rejected with ErrScopeDenied.
	Platform TenantScope = "platform"

	// Tenant requires that some tenant id is stamped on the request.
	// Any valid app tenant satisfies this tier — no organisation or user
	// requirement. Identity-establishing handlers (ValidateSession, OAuth
	// callbacks, MintSession) MUST use this tier — Org/User deadlock them
	// against the chicken-and-egg requirement of carrying their own ids.
	Tenant TenantScope = "tenant"

	// Org requires tenant id + organisation id. Used for handlers that
	// operate on organisation-scoped resources.
	Org TenantScope = "org"

	// User requires tenant id + user id. Used for handlers that operate
	// on a specific user's own data.
	User TenantScope = "user"

	// Profile requires tenant id at the dispatch tier; profile membership
	// is a scope-string check at the middleware tier
	// (RequireScopesWithParam — Quorum ADR-0012). Per finding rev-069
	// this tier was ADDED to close a fail-open gap: handlers declared
	// with Profile in the rpc package previously fell through to the
	// default arm in handlers.validateTenantScope and silently accepted
	// anonymous callers.
	Profile TenantScope = "profile"

	// Unknown is the fail-closed sentinel installed by rpc.mapProtoScope
	// when it sees a proto enum it does not recognise (including an
	// explicit TENANT_SCOPE_UNSPECIFIED in a .proto file). EnforceScope
	// and validateTenantScope's default branches reject any handler bound
	// to this scope. NEVER set this scope manually — it exists only to
	// make decay-to-None fail closed.
	Unknown TenantScope = "unknown"
)

// IsDeclared reports whether s is a valid registration-time declaration.
// Unknown is intentionally excluded: it is a fail-closed translation sentinel,
// not a tier a registry may admit.
func IsDeclared(s TenantScope) bool {
	switch s {
	case None, Platform, Tenant, Org, User, Profile:
		return true
	default:
		return false
	}
}
