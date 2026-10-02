/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package scope

import (
	"context"
	"testing"
)

func withinTransportIdentity() AuthenticatedIdentity {
	return AuthenticatedIdentity{
		Scopes: []ScopeValue{
			{Name: AxisTransportPinned, Value: "orbtr"},
			{Name: AxisWithinTransport, Value: "org-A"},
		},
		Subject: "user-A",
	}
}

func TestAuthenticatedPrincipalIsDefensivelyCopied(t *testing.T) {
	inputScopes := []string{"orbtr.io.plugins.read", "orbtr.io.plugins.publish"}
	identity := withinTransportIdentity()
	principal, err := NewAuthenticatedPrincipal(identity, inputScopes)
	if err != nil {
		t.Fatalf("NewAuthenticatedPrincipal: %v", err)
	}
	ctx := WithAuthenticatedPrincipal(context.Background(), principal)

	// Mutate BOTH inputs after construction: neither the grants slice nor the
	// identity axis slice may reach the stored snapshot.
	inputScopes[0] = "forged.input"
	identity.Scopes[0].Value = "forged.axis"

	first, ok := AuthenticatedPrincipalFromContext(ctx)
	if !ok {
		t.Fatal("authenticated principal missing from context")
	}
	gotScopes := first.Scopes()
	gotScopes[1] = "forged.output"

	second, ok := AuthenticatedPrincipalFromContext(ctx)
	if !ok {
		t.Fatal("authenticated principal disappeared from context")
	}
	if got := second.Scopes(); got[0] != "orbtr.io.plugins.read" || got[1] != "orbtr.io.plugins.publish" {
		t.Fatalf("stored scopes mutated through retained slice: %v", got)
	}
	got := AuthenticatedIdentityFromContext(ctx)
	if got.Scope(AxisTransportPinned) != "orbtr" ||
		got.Scope(AxisWithinTransport) != "org-A" ||
		got.Subject != "user-A" {
		t.Fatalf("scope identity = %#v", got)
	}
}

// TestAuthenticatedPrincipalRejectsNonCanonicalGrants proves loom still
// validates the ONE thing it copies literally — the grant strings — even
// though it treats the identity axes as opaque.
func TestAuthenticatedPrincipalRejectsNonCanonicalGrants(t *testing.T) {
	tests := []struct {
		name   string
		grants []string
	}{
		{name: "empty scope", grants: []string{""}},
		{name: "padded scope", grants: []string{"scope.read "}},
		{name: "leading space scope", grants: []string{" scope.read"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewAuthenticatedPrincipal(withinTransportIdentity(), tc.grants); err == nil {
				t.Fatal("NewAuthenticatedPrincipal unexpectedly accepted non-canonical grants")
			}
		})
	}
}

func TestAuthenticatedPrincipalReplacementDoesNotMergeAxes(t *testing.T) {
	withinTransport, err := NewAuthenticatedPrincipal(withinTransportIdentity(), []string{"org.read"})
	if err != nil {
		t.Fatal(err)
	}
	platformOnly, err := NewAuthenticatedPrincipal(AuthenticatedIdentity{
		Scopes: []ScopeValue{{Name: AxisTransportPinned, Value: "orbtr"}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	ctx := WithAuthenticatedPrincipal(context.Background(), withinTransport)
	ctx = WithAuthenticatedPrincipal(ctx, platformOnly)

	got, ok := AuthenticatedPrincipalFromContext(ctx)
	if !ok {
		t.Fatal("replacement principal missing")
	}
	if identity := got.Identity(); identity.Scope(AxisWithinTransport) != "" || identity.Subject != "" {
		t.Fatalf("replacement retained prior axes: %#v", identity)
	}
	if scopes := got.Scopes(); len(scopes) != 0 {
		t.Fatalf("replacement retained prior scopes: %v", scopes)
	}
}

// TestEmptyPrincipalIsNotStamped proves an identity with no axes and no subject
// confers no authority: it is not Valid and WithAuthenticatedPrincipal leaves
// the context without a principal.
func TestEmptyPrincipalIsNotStamped(t *testing.T) {
	empty, err := NewAuthenticatedPrincipal(AuthenticatedIdentity{}, nil)
	if err != nil {
		t.Fatalf("NewAuthenticatedPrincipal(empty): %v", err)
	}
	if empty.Valid() {
		t.Fatal("empty principal reported Valid")
	}
	ctx := WithAuthenticatedPrincipal(context.Background(), empty)
	if _, ok := AuthenticatedPrincipalFromContext(ctx); ok {
		t.Fatal("empty principal was stamped onto context")
	}
}
