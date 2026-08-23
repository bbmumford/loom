/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package dispatch

import (
	"context"
	"testing"
	"time"

	tenantScope "github.com/bbmumford/loom/pkg/rpc/scope"
)

func TestBuildRPCRequestCtxCarriesTypedPrincipalOutsideMutableContext(t *testing.T) {
	principal, err := tenantScope.NewAuthenticatedPrincipal(
		"orbtr",
		"org-A",
		"user-A",
		[]string{"orbtr.io.plugins.read", "orbtr.io.plugins.publish"},
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx := tenantScope.WithAuthenticatedPrincipal(context.Background(), principal)

	req := buildRPCRequestCtx(
		ctx,
		"orbtr.io.plugins",
		"orbtr.io.plugins.List",
		nil,
		time.Second,
		"orbtr",
	)

	if req.Principal == nil {
		t.Fatal("typed principal omitted from RPC envelope")
	}
	if got := req.Principal.PlatformTenantId; got != "orbtr" {
		t.Fatalf("platform tenant = %q", got)
	}
	if got := req.Principal.CustomerOrgId; got != "org-A" {
		t.Fatalf("customer org = %q", got)
	}
	if got := req.Principal.UserId; got != "user-A" {
		t.Fatalf("user = %q", got)
	}
	if got := req.Principal.Scopes; len(got) != 2 ||
		got[0] != "orbtr.io.plugins.read" ||
		got[1] != "orbtr.io.plugins.publish" {
		t.Fatalf("scopes = %v", got)
	}
	if _, ok := req.Context["orgId"]; ok {
		t.Fatalf("customer authority leaked into mutable camel context: %v", req.Context)
	}
	if _, ok := req.Context["org_id"]; ok {
		t.Fatalf("customer authority leaked into mutable snake context: %v", req.Context)
	}
}

func TestBuildRPCRequestCtxDoesNotRewritePrincipalToCallerPlatform(t *testing.T) {
	principal, err := tenantScope.NewAuthenticatedPrincipal(
		"foreign-platform",
		"org-A",
		"user-A",
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx := tenantScope.WithAuthenticatedPrincipal(context.Background(), principal)

	req := buildRPCRequestCtx(ctx, "role", "handler", nil, time.Second, "orbtr")
	if req.Principal == nil {
		t.Fatal("mismatched principal was silently dropped")
	}
	if got := req.Principal.PlatformTenantId; got != "foreign-platform" {
		t.Fatalf("principal rewritten to dispatch platform: %q", got)
	}
	if got := req.Context["tenantId"]; got != "orbtr" {
		t.Fatalf("dispatch platform context = %q", got)
	}
}
