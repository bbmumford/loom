/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package scope

import (
	"fmt"
	"strings"
)

// ScopeValue is one opaque, named identity axis a trusted server-side
// transport, session middleware, or product adapter established. loom carries
// Name and Value without interpreting either; the consuming project assigns
// meaning to Name. loom never derives one axis from another.
type ScopeValue struct {
	Name  string
	Value string
}

// AuthenticatedIdentity is the opaque identity presence a trusted server-side
// transport, session middleware, or product adapter established before scope
// enforcement. Scopes is an ordered set of named axes; Subject is the
// authenticated principal subject. loom interprets neither a Name nor the
// Subject: a consumer's PresencePolicy decides what a handler's declared
// ScopeTag requires of them.
//
// Request bodies, RPCRequest.Context maps, task Metadata and other mutable
// caller fields MUST NOT be copied into this value. A consuming project that
// needs an axis (e.g. a customer organisation) resolves it from its own store
// first, then supplies the resulting presence to the dispatch adapter.
type AuthenticatedIdentity struct {
	Scopes  []ScopeValue
	Subject string
	// partition is the opaque server-established partition scope ("whose data") a
	// store selector keys on. loom stores and returns it without interpreting it:
	// it carries no notion of owner, customer, org, or partition — it is an opaque
	// string the consuming project's injected identity policy (the trusted ingress
	// that resolved it) sets via WithPartition. A store selects on this value with
	// exact-match, deny-before-read.
	partition string
}

// Scope returns the value of the named axis, or "" when the axis is absent.
func (a AuthenticatedIdentity) Scope(name string) string {
	for i := range a.Scopes {
		if a.Scopes[i].Name == name {
			return a.Scopes[i].Value
		}
	}
	return ""
}

// Has reports whether the named axis is present with a non-empty value.
func (a AuthenticatedIdentity) Has(name string) bool {
	return a.Scope(name) != ""
}

// Partition returns the server-established partition scope: the opaque "whose
// data" binding a store selector keys on. The consuming project's injected
// policy resolved which of the identity's data is the partition and set it via
// WithPartition; loom returns it without naming or interpreting any axis — no
// owner, customer, org, or tenant vocabulary. "" when unset — fail-closed at the
// read (an absent partition denies, never grants).
func (a AuthenticatedIdentity) Partition() string { return a.partition }

// WithPartition returns a copy of the identity with the opaque partition scope
// set. The consuming project's injected identity policy (the trusted ingress
// that resolved "whose data") calls it; loom bakes in no notion of what the
// partition names. A defensive copy of the axis slice is taken so the returned
// value shares no mutable state with the receiver.
func (a AuthenticatedIdentity) WithPartition(partition string) AuthenticatedIdentity {
	out := a.clone()
	out.partition = partition
	return out
}

// clone returns a defensive copy of the identity's axis slice.
func (a AuthenticatedIdentity) clone() AuthenticatedIdentity {
	if len(a.Scopes) == 0 {
		return AuthenticatedIdentity{Subject: a.Subject, partition: a.partition}
	}
	out := AuthenticatedIdentity{
		Scopes:    make([]ScopeValue, len(a.Scopes)),
		Subject:   a.Subject,
		partition: a.partition,
	}
	copy(out.Scopes, a.Scopes)
	return out
}

// NewIdentity builds the identity a trusted ingress established: the
// authenticated subject and the opaque partition its store selectors key on.
// Callers pass only server-established values, never a request-body field. An
// empty partition leaves the identity without one, so a partition-keyed read
// denies.
func NewIdentity(subject, partition string) AuthenticatedIdentity {
	return AuthenticatedIdentity{Subject: subject, partition: partition}
}

// NewCompleteIdentity is the validating constructor for an authoritative identity
// snapshot: a subject plus zero or more axes, each with a non-empty canonical
// Name and a non-empty Value, and no duplicate Name. It returns a defensively
// cloned identity so a slice the caller retains cannot mutate stamped authority.
// A trusted ingress resolves each axis (platform tenant from transport/config,
// customer org from session/store, …) and assembles ONE complete identity here,
// rather than stamping raw single axes that would replace each other's snapshot.
func NewCompleteIdentity(subject string, axes ...ScopeValue) (AuthenticatedIdentity, error) {
	if err := validateAxes(axes); err != nil {
		return AuthenticatedIdentity{}, err
	}
	return AuthenticatedIdentity{Scopes: cloneAxes(axes), Subject: subject}, nil
}

// validateAxes enforces the deterministic exact-one-axis invariant: every axis
// name is non-empty and canonical (no surrounding space), every value is
// non-empty, and no name repeats. Duplicate names would make Scope(name)
// order-dependent (it returns the first match), silently shadowing an axis —
// the alias hazard the constructor exists to close. An empty axis list is valid
// (a subject-only or absent-axis presence); Scope reports "" for an absent axis.
func validateAxes(axes []ScopeValue) error {
	seen := make(map[string]struct{}, len(axes))
	for i, a := range axes {
		if a.Name == "" || strings.TrimSpace(a.Name) != a.Name {
			return fmt.Errorf("scope: axis %d name is empty or non-canonical", i)
		}
		if a.Value == "" {
			return fmt.Errorf("scope: axis %q has an empty value", a.Name)
		}
		if _, dup := seen[a.Name]; dup {
			return fmt.Errorf("scope: duplicate axis name %q", a.Name)
		}
		seen[a.Name] = struct{}{}
	}
	return nil
}

// cloneAxes copies the axis slice so no caller-retained backing array aliases a
// stamped identity.
func cloneAxes(axes []ScopeValue) []ScopeValue {
	if len(axes) == 0 {
		return nil
	}
	out := make([]ScopeValue, len(axes))
	copy(out, axes)
	return out
}

// validate reports whether the identity's axes satisfy the canonical invariant.
// The context stampers call it so an unvalidated raw struct literal (which the
// exported fields still permit while HSTLES compiles) cannot enter authority.
func (a AuthenticatedIdentity) validate() error { return validateAxes(a.Scopes) }
