/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package scope

import (
	"context"
	"testing"
)

func mustIdentity(t *testing.T, subject string, axes ...ScopeValue) AuthenticatedIdentity {
	t.Helper()
	id, err := NewCompleteIdentity(subject, axes...)
	if err != nil {
		t.Fatalf("NewCompleteIdentity(%q, %v): unexpected error %v", subject, axes, err)
	}
	return id
}

// (a) A caller that mutates the axis slice AFTER stamping must not reach the
// stored authority — the stamper defensively clones.
func TestWithAuthenticatedIdentity_ClonesOnStamp(t *testing.T) {
	axes := []ScopeValue{{Name: AxisPlatformTenant, Value: "orbtr"}, {Name: AxisCustomerOrg, Value: "acme"}}
	id, err := NewCompleteIdentity("user-1", axes...)
	if err != nil {
		t.Fatal(err)
	}
	ctx := WithAuthenticatedIdentity(context.Background(), id)
	axes[0].Value = "evil"      // mutate the original constructor input
	id.Scopes[1].Value = "evil" // mutate the returned identity's backing array
	got := AuthenticatedIdentityFromContext(ctx)
	if got.Scope(AxisPlatformTenant) != "orbtr" || got.Scope(AxisCustomerOrg) != "acme" {
		t.Fatalf("stamped identity mutated via retained input: %+v", got.Scopes)
	}
}

// (b) A caller that mutates the slice RETURNED by a read must not reach the
// stored authority — the reader defensively clones.
func TestAuthenticatedIdentityFromContext_ClonesOnRead(t *testing.T) {
	ctx := WithAuthenticatedIdentity(context.Background(),
		mustIdentity(t, "u", ScopeValue{Name: AxisPlatformTenant, Value: "orbtr"}))
	first := AuthenticatedIdentityFromContext(ctx)
	first.Scopes[0].Value = "evil"
	second := AuthenticatedIdentityFromContext(ctx)
	if second.Scope(AxisPlatformTenant) != "orbtr" {
		t.Fatalf("stored identity mutated via read-returned slice: got %q", second.Scope(AxisPlatformTenant))
	}
}

// (c) An INVALID stamp over a poisoned prior context must clear BOTH the identity
// and the principal — never preserve prior authority (fail-closed).
func TestWithAuthenticatedScope_InvalidFailsClosedClearsBoth(t *testing.T) {
	good := mustIdentity(t, "u", ScopeValue{Name: AxisPlatformTenant, Value: "orbtr"})
	p, err := NewAuthenticatedPrincipal(good, []string{"grant.read"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := WithAuthenticatedPrincipal(context.Background(), p)
	if AuthenticatedIdentityFromContext(ctx).Scope(AxisPlatformTenant) != "orbtr" {
		t.Fatal("precondition: prior identity must be present")
	}
	if _, ok := AuthenticatedPrincipalFromContext(ctx); !ok {
		t.Fatal("precondition: prior principal must be present")
	}
	// invalid: empty value fails the axis invariant
	ctx = WithAuthenticatedScope(ctx, AxisPlatformTenant, "")
	if AuthenticatedIdentityFromContext(ctx).Scope(AxisPlatformTenant) != "" {
		t.Fatal("invalid stamp preserved prior identity (fail-open)")
	}
	if _, ok := AuthenticatedPrincipalFromContext(ctx); ok {
		t.Fatal("invalid stamp preserved prior principal (fail-open)")
	}
}

// (d) A VALID raw identity replacement carries no grants, so it must clear the
// prior principal — no contradictory identity/grants snapshot survives.
func TestWithAuthenticatedIdentity_ValidRawClearsPriorPrincipal(t *testing.T) {
	good := mustIdentity(t, "u", ScopeValue{Name: AxisPlatformTenant, Value: "orbtr"})
	p, _ := NewAuthenticatedPrincipal(good, []string{"grant.read"})
	ctx := WithAuthenticatedPrincipal(context.Background(), p)
	ctx = WithAuthenticatedIdentity(ctx, mustIdentity(t, "u2", ScopeValue{Name: AxisCustomerOrg, Value: "acme"}))
	if _, ok := AuthenticatedPrincipalFromContext(ctx); ok {
		t.Fatal("raw identity replacement left prior principal grants live (contradictory snapshot)")
	}
	if AuthenticatedIdentityFromContext(ctx).Scope(AxisCustomerOrg) != "acme" {
		t.Fatal("raw identity replacement did not take effect")
	}
}

// (e) WithAuthenticatedPrincipal installs a COHERENT identity+grants pair
// atomically and does not clear the principal it just stamped.
func TestWithAuthenticatedPrincipal_CoherentAndAtomic(t *testing.T) {
	id := mustIdentity(t, "u",
		ScopeValue{Name: AxisPlatformTenant, Value: "orbtr"},
		ScopeValue{Name: AxisCustomerOrg, Value: "acme"})
	p, err := NewAuthenticatedPrincipal(id, []string{"grant.read", "grant.write"})
	if err != nil {
		t.Fatal(err)
	}
	ctx := WithAuthenticatedPrincipal(context.Background(), p)
	got := AuthenticatedIdentityFromContext(ctx)
	if got.Scope(AxisPlatformTenant) != "orbtr" || got.Scope(AxisCustomerOrg) != "acme" {
		t.Fatalf("principal identity projection incomplete: %+v", got.Scopes)
	}
	gotP, ok := AuthenticatedPrincipalFromContext(ctx)
	if !ok || len(gotP.Scopes()) != 2 {
		t.Fatalf("principal grants not installed coherently: ok=%v grants=%v", ok, gotP.Scopes())
	}
}

// An empty/invalid principal fails closed, clearing a prior snapshot.
func TestWithAuthenticatedPrincipal_InvalidFailsClosed(t *testing.T) {
	good := mustIdentity(t, "u", ScopeValue{Name: AxisPlatformTenant, Value: "orbtr"})
	p, _ := NewAuthenticatedPrincipal(good, []string{"grant.read"})
	ctx := WithAuthenticatedPrincipal(context.Background(), p)
	ctx = WithAuthenticatedPrincipal(ctx, AuthenticatedPrincipal{}) // empty
	if AuthenticatedIdentityFromContext(ctx).Scope(AxisPlatformTenant) != "" {
		t.Fatal("empty principal preserved prior identity")
	}
	if _, ok := AuthenticatedPrincipalFromContext(ctx); ok {
		t.Fatal("empty principal preserved prior principal")
	}
}

// HSTLES-shaped positive: a (platform, customer-org, user) identity — the shape
// hstles NewScopeIdentity produces — survives validation and reads back whole.
func TestHSTLESShapedPositiveSurvives(t *testing.T) {
	id := mustIdentity(t, "user-x",
		ScopeValue{Name: AxisPlatformTenant, Value: "orbtr"},
		ScopeValue{Name: AxisCustomerOrg, Value: "org-A"})
	ctx := WithAuthenticatedIdentity(context.Background(), id)
	got := AuthenticatedIdentityFromContext(ctx)
	if got.Scope(AxisPlatformTenant) != "orbtr" || got.Scope(AxisCustomerOrg) != "org-A" || got.Subject != "user-x" {
		t.Fatalf("HSTLES-shaped identity did not survive: %+v subj=%q", got.Scopes, got.Subject)
	}
}
