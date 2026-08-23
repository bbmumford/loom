/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package scope

import (
	"context"
	"testing"
)

func TestAuthenticatedPrincipalIsDefensivelyCopied(t *testing.T) {
	inputScopes := []string{"orbtr.io.plugins.read", "orbtr.io.plugins.publish"}
	principal, err := NewAuthenticatedPrincipal("orbtr", "org-A", "user-A", inputScopes)
	if err != nil {
		t.Fatalf("NewAuthenticatedPrincipal: %v", err)
	}
	ctx := WithAuthenticatedPrincipal(context.Background(), principal)

	inputScopes[0] = "forged.input"
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
	if got := AuthenticatedIdentityFromContext(ctx); got != (AuthenticatedIdentity{
		PlatformTenantID: "orbtr",
		OrganizationID:   "org-A",
		UserID:           "user-A",
	}) {
		t.Fatalf("scope identity = %#v", got)
	}
}

func TestAuthenticatedPrincipalRejectsNonCanonicalAuthority(t *testing.T) {
	tests := []struct {
		name     string
		platform string
		org      string
		user     string
		scopes   []string
	}{
		{name: "missing platform"},
		{name: "padded platform", platform: " orbtr"},
		{name: "padded org", platform: "orbtr", org: "org-A "},
		{name: "padded user", platform: "orbtr", user: " user-A"},
		{name: "empty scope", platform: "orbtr", scopes: []string{""}},
		{name: "padded scope", platform: "orbtr", scopes: []string{"scope.read "}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NewAuthenticatedPrincipal(
				tc.platform,
				tc.org,
				tc.user,
				tc.scopes,
			); err == nil {
				t.Fatal("NewAuthenticatedPrincipal unexpectedly accepted invalid authority")
			}
		})
	}
}

func TestAuthenticatedPrincipalReplacementDoesNotMergeAxes(t *testing.T) {
	customer, err := NewAuthenticatedPrincipal(
		"orbtr",
		"org-A",
		"user-A",
		[]string{"org.read"},
	)
	if err != nil {
		t.Fatal(err)
	}
	platformOnly, err := NewAuthenticatedPrincipal("orbtr", "", "", nil)
	if err != nil {
		t.Fatal(err)
	}

	ctx := WithAuthenticatedPrincipal(context.Background(), customer)
	ctx = WithAuthenticatedPrincipal(ctx, platformOnly)

	got, ok := AuthenticatedPrincipalFromContext(ctx)
	if !ok {
		t.Fatal("replacement principal missing")
	}
	if identity := got.Identity(); identity.OrganizationID != "" || identity.UserID != "" {
		t.Fatalf("replacement retained prior axes: %#v", identity)
	}
	if scopes := got.Scopes(); len(scopes) != 0 {
		t.Fatalf("replacement retained prior scopes: %v", scopes)
	}
}
