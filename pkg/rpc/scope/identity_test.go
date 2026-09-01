/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package scope

import (
	"context"
	"testing"
)

func TestAuthenticatedIdentityAxisLookup(t *testing.T) {
	id := AuthenticatedIdentity{
		Scopes: []ScopeValue{
			{Name: AxisTransportPinned, Value: "orbtr"},
			{Name: AxisWithinTransport, Value: "org-1"},
		},
		Subject: "user-1",
	}
	if got := id.Scope(AxisTransportPinned); got != "orbtr" {
		t.Fatalf("Scope(platform) = %q, want orbtr", got)
	}
	if got := id.Scope(AxisWithinTransport); got != "org-1" {
		t.Fatalf("Scope(org) = %q, want org-1", got)
	}
	if got := id.Scope("absent"); got != "" {
		t.Fatalf("Scope(absent) = %q, want empty", got)
	}
	if !id.Has(AxisTransportPinned) {
		t.Fatal("Has(platform) = false, want true")
	}
	if id.Has("absent") {
		t.Fatal("Has(absent) = true, want false")
	}
}

func TestAuthenticatedIdentityCloneIsDefensive(t *testing.T) {
	id := AuthenticatedIdentity{
		Scopes:  []ScopeValue{{Name: AxisTransportPinned, Value: "orbtr"}},
		Subject: "user-1",
	}
	clone := id.clone()
	id.Scopes[0].Value = "forged"
	if got := clone.Scope(AxisTransportPinned); got != "orbtr" {
		t.Fatalf("clone shares backing array: Scope(platform) = %q, want orbtr", got)
	}
}

// TestIsDeclaredAdmitsOpaqueRejectsSentinels proves loom's admission is
// taxonomy-agnostic: the blessed level constants AND any consumer-defined
// opaque axis (e.g. a partition tag) are declarable, while ONLY the two
// non-declarable sentinels (ScopeUnset zero value, ScopeUnknown proto-decay
// marker) are refused. loom no longer enumerates a fixed ladder at
// registration — a tag it does not recognise is admitted and left to fail
// CLOSED at EnforceScope via the consumer PresencePolicy (proven in the rpc
// package's enforcement tests), never fail open.
func TestIsDeclaredAdmitsOpaqueRejectsSentinels(t *testing.T) {
	declarable := []ScopeTag{
		ScopeNone, ScopePlatform, ScopeTenant, ScopeOrg, ScopeUser, ScopeProfile,
		ScopeTag("partition"), ScopeTag("consumer-defined-axis"),
	}
	for _, s := range declarable {
		if !IsDeclared(s) {
			t.Fatalf("IsDeclared(%q) = false, want true (opaque consumer axis must admit)", s)
		}
	}
	notDeclarable := []ScopeTag{ScopeUnset, ScopeUnknown}
	for _, s := range notDeclarable {
		if IsDeclared(s) {
			t.Fatalf("IsDeclared(%q) = true, want false (sentinels must fail closed)", s)
		}
	}
}

func TestWithAuthenticatedScopeReplacesWholeSnapshot(t *testing.T) {
	ctx := WithAuthenticatedIdentity(context.Background(), AuthenticatedIdentity{
		Scopes: []ScopeValue{
			{Name: AxisTransportPinned, Value: "old-platform"},
			{Name: AxisWithinTransport, Value: "old-org"},
		},
		Subject: "old-user",
	})

	got := AuthenticatedIdentityFromContext(
		WithAuthenticatedScope(ctx, AxisTransportPinned, "new-platform"),
	)
	if got.Scope(AxisTransportPinned) != "new-platform" {
		t.Fatalf("scope rebind lost the new axis: %+v", got)
	}
	if got.Scope(AxisWithinTransport) != "" || got.Subject != "" {
		t.Fatalf("scope rebind retained stale identity fields: %+v", got)
	}
	if len(got.Scopes) != 1 {
		t.Fatalf("scope rebind must carry exactly one axis, got %d", len(got.Scopes))
	}
}

// TestNewCompleteIdentityValidates pins the axis invariant: duplicate names,
// empty/non-canonical names and empty values are rejected; a well-formed
// two-axis identity is accepted and defensively cloned from the caller's input.
func TestNewCompleteIdentityValidates(t *testing.T) {
	bad := []struct {
		name string
		axes []ScopeValue
	}{
		{"duplicate-name", []ScopeValue{{Name: AxisTransportPinned, Value: "a"}, {Name: AxisTransportPinned, Value: "b"}}},
		{"empty-name", []ScopeValue{{Name: "", Value: "v"}}},
		{"noncanonical-name", []ScopeValue{{Name: " x", Value: "v"}}},
		{"empty-value", []ScopeValue{{Name: AxisTransportPinned, Value: ""}}},
	}
	for _, c := range bad {
		if _, err := NewCompleteIdentity("u", c.axes...); err == nil {
			t.Errorf("%s: expected rejection, got nil error", c.name)
		}
	}
	in := []ScopeValue{{Name: AxisTransportPinned, Value: "orbtr"}, {Name: AxisWithinTransport, Value: "acme"}}
	id, err := NewCompleteIdentity("user-1", in...)
	if err != nil {
		t.Fatalf("well-formed two-axis identity rejected: %v", err)
	}
	in[0].Value = "evil" // mutate caller input after construction
	if id.Scope(AxisTransportPinned) != "orbtr" {
		t.Fatal("NewCompleteIdentity did not clone its axis input")
	}
}

// TestNewCompleteIdentityPeerAndAbsentAxes: both peer axes are readable together,
// and an absent axis reads empty (an empty axis list is a valid subject-only
// presence).
func TestNewCompleteIdentityPeerAndAbsentAxes(t *testing.T) {
	id, err := NewCompleteIdentity("u",
		ScopeValue{Name: AxisTransportPinned, Value: "orbtr"},
		ScopeValue{Name: AxisWithinTransport, Value: "acme"})
	if err != nil {
		t.Fatal(err)
	}
	if id.Scope(AxisTransportPinned) != "orbtr" || id.Scope(AxisWithinTransport) != "acme" {
		t.Fatalf("peer axes must both be readable: %+v", id.Scopes)
	}
	if id.Scope("no-such-axis") != "" {
		t.Fatal("absent axis must read empty")
	}
	if _, err := NewCompleteIdentity("subject-only"); err != nil {
		t.Fatalf("subject-only (zero-axis) identity must be valid: %v", err)
	}
}
