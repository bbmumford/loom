/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

// Package scope holds the single canonical ScopeTag label shared by the rpc
// package (rpc.ScopeTag, rpc.Scope*) and the node/handlers package
// (handlers.Scope, handlers.Scope*). Both surfaces
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
// The tags stay separate values rather than collapsing to one opaque string
// because a handler declares WHICH parts of the identity it needs present, and
// a single token could not carry that.
package scope

// ScopeTag is the opaque partition/scope label a handler declares at registration.
// It is deliberately a defined type: an arbitrary string must not be assignable
// as a scope declaration, and its zero value must remain detectably invalid.
// The string representation still survives JSON / protobuf / wire round-trips
// without a bespoke encoder. loom never interprets a tag's meaning; a
// consumer's PresencePolicy does.
type ScopeTag string

// Canonical scope tags. String values are load-bearing wire tokens — they appear
// in proto descriptors, telemetry, and the reflection registry, so a producer and
// a consumer agree only while they spell them the same way; change one and every
// peer must be rebuilt together. loom carries them as opaque labels; only a
// consumer's PresencePolicy assigns them meaning.
//
// The rpc and handlers packages re-export every tag below. All three surfaces
// MUST stay 1:1 with this list.
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

	// The declarable tags name what must be PRESENT on the authenticated
	// identity, which is what a PresencePolicy actually tests — never who the
	// caller is. scopePresencePolicy evaluates exactly these; a consumer policy
	// may read them differently, but loom itself assigns them no identity.

	// ScopeAllowlisted: the reference policy admits an identity whose partition
	// is in the allowlist passed to NewScopePresencePolicy. It is the narrowest
	// tag: the policy decides membership, and an empty allowlist admits no
	// identity.
	ScopeAllowlisted ScopeTag = "allowlisted"

	// ScopePartition: the reference policy admits an identity with a non-empty
	// partition and asks nothing more, whatever that partition denotes to the
	// consumer.
	ScopePartition ScopeTag = "partition"

	// ScopeWithinPartition: the reference policy evaluates it as
	// ScopePartition, because the identity carries one partition and which
	// partition a handler may act on is the handler's own selector. A consumer
	// policy that reads the within-transport axis separately can require more.
	ScopeWithinPartition ScopeTag = "within_partition"

	// ScopeSubject: the reference policy admits an identity with a non-empty
	// partition and a non-empty authenticated subject, i.e. an individual
	// principal rather than only a partition.
	ScopeSubject ScopeTag = "subject"

	// ScopeUnknown is the fail-closed sentinel for a scope that arrived as
	// something this package does not recognise. It is not declarable:
	// registration refuses it, and the presence default arm denies it, so a tag
	// that decayed to it reaches nothing.
	//
	// Nothing produces it today — no caller-supplied value is converted into a
	// ScopeTag anywhere, so the only way to hold it is to set it, and that is
	// what the refusals above exist to catch. It stays because a decay path that
	// appears later lands on a value that already denies, rather than on one
	// that has to be noticed first.
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
// unconfigured policy denies every non-None handler). The declared tags below
// are consumer conveniences, not loom's admissible set.
func IsDeclared(s ScopeTag) bool {
	return s != ScopeUnset && s != ScopeUnknown
}
