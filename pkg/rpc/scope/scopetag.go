/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

// Package scope holds the single canonical ScopeTag label shared by the rpc
// package (rpc.ScopeTag, rpc.Scope*) and the legacy handlers package
// (mesh/node/handlers.Scope, handlers.Scope*). Both surfaces
// re-export the symbols declared here via Go type aliases and constant
// forwards, so a value round-trips between the two packages without any
// possibility of drift.
//
// loom treats a ScopeTag as an OPAQUE label. It records which tag a handler
// declared and hands that tag, unread, to the consumer-supplied
// PresencePolicy at enforcement time. loom itself decides nothing about what
// a tag requires — the only tag it interprets structurally is ScopeNone (the
// enforcement opt-out) and the two non-declarable sentinels (ScopeUnset /
// ScopeUnknown), which it refuses to admit or, at dispatch, fails closed on.
//
// The value constants are retained (rather than collapsed to a single opaque
// string) to avoid a 434-site consumer churn: existing call sites keep
// naming rpc.ScopeTenant / handlers.ScopeOrg and keep compiling.
package scope

// ScopeTag is the opaque tenant/scope label a handler declares at registration.
// It is deliberately a defined type: an arbitrary string must not be assignable
// as a scope declaration, and its zero value must remain detectably invalid.
// The string representation still survives JSON / protobuf / wire round-trips
// without a bespoke encoder. loom never interprets a tag's meaning; a
// consumer's PresencePolicy does.
type ScopeTag string

// Canonical scope tags. String values are load-bearing wire tokens — they
// appear in proto descriptors, telemetry, and the reflection registry, so they
// MUST NOT change without a coordinated redeploy. loom carries them as opaque
// labels; only a consumer's PresencePolicy assigns them meaning.
//
// The rpc package re-exports these as rpc.ScopeTag{None,Platform,Tenant,Org,
// User,Profile,Partition,Unknown}; the handlers package re-exports them as
// handlers.Scope{None,Platform,Tenant,Org,User,Profile,Partition,Unknown}.
// Both surfaces MUST stay 1:1 with this list.
const (
	// ScopeUnset is the invalid zero value. It means no scope decision was
	// made and MUST be rejected at registration and fail closed at dispatch.
	// It is not re-exported as a caller-facing rpc.Scope* constant: callers
	// must choose a real tag.
	ScopeUnset ScopeTag = ""

	// ScopeNone opts a handler out of scope enforcement. Used for liveness,
	// /heartbeat, OAuth init and other public unauthenticated surfaces. Its
	// non-empty token makes that opt-out distinguishable from omission, and it
	// is the ONE tag loom admits without consulting a PresencePolicy.
	ScopeNone ScopeTag = "none"

	// ScopePlatform is an opaque label a PresencePolicy may treat as
	// "restricted to the platform itself". loom assigns it no meaning.
	ScopePlatform ScopeTag = "platform"

	// ScopeTenant is an opaque label a PresencePolicy may treat as "requires a
	// tenant-bound caller". loom assigns it no meaning.
	ScopeTenant ScopeTag = "tenant"

	// ScopeOrg is an opaque label a PresencePolicy may treat as "requires a
	// customer-organisation-bound caller". loom assigns it no meaning.
	ScopeOrg ScopeTag = "org"

	// ScopeUser is an opaque label a PresencePolicy may treat as "requires a
	// user-bound caller". loom assigns it no meaning.
	ScopeUser ScopeTag = "user"

	// ScopeProfile is an opaque label a PresencePolicy may treat as "requires
	// a billing-profile-bound caller". loom assigns it no meaning.
	ScopeProfile ScopeTag = "profile"

	// ScopePartition is an opaque label a PresencePolicy may treat as "requires
	// a partition-bound caller". loom assigns it no meaning.
	ScopePartition ScopeTag = "partition"

	// ScopeUnknown is the fail-closed sentinel installed by rpc.mapProtoScope
	// when it sees a proto enum it does not recognise (including an explicit
	// TENANT_SCOPE_UNSPECIFIED in a .proto file). It is not a declarable tag:
	// registration rejects it and dispatch fails closed. NEVER set it
	// manually — it exists only to make decay-to-None fail closed.
	ScopeUnknown ScopeTag = "unknown"
)

// IsDeclared reports whether s is a valid registration-time declaration.
// loom admits ANY tag except the two non-declarable sentinels: ScopeUnset (the
// zero value — no scope decision was made) and ScopeUnknown (the fail-closed
// proto-decay marker). It does NOT enumerate a fixed ladder of levels: loom
// assigns a tag no meaning, so it has no basis to reject a consumer-defined
// opaque axis (e.g. a partition tag) at registration. This mirrors EnforceScope,
// which already delegates every non-sentinel tag to the consumer's
// PresencePolicy and denies when the policy reports it unsatisfied — so a tag
// loom's admission does not recognise still fails CLOSED at enforcement (an
// unconfigured policy denies every non-None handler). The blessed ScopePlatform
// /Tenant/Org/User/Profile constants remain as consumer conveniences, not as
// loom's admissible set.
func IsDeclared(s ScopeTag) bool {
	return s != ScopeUnset && s != ScopeUnknown
}
