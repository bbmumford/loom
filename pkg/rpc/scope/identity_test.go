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
			{Name: AxisPlatformTenant, Value: "orbtr"},
			{Name: AxisCustomerOrg, Value: "org-1"},
		},
		Subject: "user-1",
	}
	if got := id.Scope(AxisPlatformTenant); got != "orbtr" {
		t.Fatalf("Scope(platform) = %q, want orbtr", got)
	}
	if got := id.Scope(AxisCustomerOrg); got != "org-1" {
		t.Fatalf("Scope(org) = %q, want org-1", got)
	}
	if got := id.Scope("absent"); got != "" {
		t.Fatalf("Scope(absent) = %q, want empty", got)
	}
	if !id.Has(AxisPlatformTenant) {
		t.Fatal("Has(platform) = false, want true")
	}
	if id.Has("absent") {
		t.Fatal("Has(absent) = true, want false")
	}
}

func TestAuthenticatedIdentityCloneIsDefensive(t *testing.T) {
	id := AuthenticatedIdentity{
		Scopes:  []ScopeValue{{Name: AxisPlatformTenant, Value: "orbtr"}},
		Subject: "user-1",
	}
	clone := id.clone()
	id.Scopes[0].Value = "forged"
	if got := clone.Scope(AxisPlatformTenant); got != "orbtr" {
		t.Fatalf("clone shares backing array: Scope(platform) = %q, want orbtr", got)
	}
}

// TestIsDeclaredFailsClosed is a registration-time fail-closed proof: the two
// sentinels (ScopeUnset zero value, ScopeUnknown proto-decay marker) and any
// unrecognised tag are NOT declarable, so a registry refuses to admit them.
func TestIsDeclaredFailsClosed(t *testing.T) {
	declarable := []ScopeTag{ScopeNone, ScopePlatform, ScopeTenant, ScopeOrg, ScopeUser, ScopeProfile}
	for _, s := range declarable {
		if !IsDeclared(s) {
			t.Fatalf("IsDeclared(%q) = false, want true", s)
		}
	}
	notDeclarable := []ScopeTag{ScopeUnset, ScopeUnknown, ScopeTag("garbage")}
	for _, s := range notDeclarable {
		if IsDeclared(s) {
			t.Fatalf("IsDeclared(%q) = true, want false (must fail closed)", s)
		}
	}
}

func TestWithAuthenticatedScopeReplacesWholeSnapshot(t *testing.T) {
	ctx := WithAuthenticatedIdentity(context.Background(), AuthenticatedIdentity{
		Scopes: []ScopeValue{
			{Name: AxisPlatformTenant, Value: "old-platform"},
			{Name: AxisCustomerOrg, Value: "old-org"},
		},
		Subject: "old-user",
	})

	got := AuthenticatedIdentityFromContext(
		WithAuthenticatedScope(ctx, AxisPlatformTenant, "new-platform"),
	)
	if got.Scope(AxisPlatformTenant) != "new-platform" {
		t.Fatalf("scope rebind lost the new axis: %+v", got)
	}
	if got.Scope(AxisCustomerOrg) != "" || got.Subject != "" {
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
		{"duplicate-name", []ScopeValue{{Name: AxisPlatformTenant, Value: "a"}, {Name: AxisPlatformTenant, Value: "b"}}},
		{"empty-name", []ScopeValue{{Name: "", Value: "v"}}},
		{"noncanonical-name", []ScopeValue{{Name: " x", Value: "v"}}},
		{"empty-value", []ScopeValue{{Name: AxisPlatformTenant, Value: ""}}},
	}
	for _, c := range bad {
		if _, err := NewCompleteIdentity("u", c.axes...); err == nil {
			t.Errorf("%s: expected rejection, got nil error", c.name)
		}
	}
	in := []ScopeValue{{Name: AxisPlatformTenant, Value: "orbtr"}, {Name: AxisCustomerOrg, Value: "acme"}}
	id, err := NewCompleteIdentity("user-1", in...)
	if err != nil {
		t.Fatalf("well-formed two-axis identity rejected: %v", err)
	}
	in[0].Value = "evil" // mutate caller input after construction
	if id.Scope(AxisPlatformTenant) != "orbtr" {
		t.Fatal("NewCompleteIdentity did not clone its axis input")
	}
}

// TestNewCompleteIdentityPeerAndAbsentAxes: both peer axes are readable together,
// and an absent axis reads empty (an empty axis list is a valid subject-only
// presence).
func TestNewCompleteIdentityPeerAndAbsentAxes(t *testing.T) {
	id, err := NewCompleteIdentity("u",
		ScopeValue{Name: AxisPlatformTenant, Value: "orbtr"},
		ScopeValue{Name: AxisCustomerOrg, Value: "acme"})
	if err != nil {
		t.Fatal(err)
	}
	if id.Scope(AxisPlatformTenant) != "orbtr" || id.Scope(AxisCustomerOrg) != "acme" {
		t.Fatalf("peer axes must both be readable: %+v", id.Scopes)
	}
	if id.Scope("no-such-axis") != "" {
		t.Fatal("absent axis must read empty")
	}
	if _, err := NewCompleteIdentity("subject-only"); err != nil {
		t.Fatalf("subject-only (zero-axis) identity must be valid: %v", err)
	}
}
