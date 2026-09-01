/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package dispatch

import (
	"context"
	"testing"
	"time"

	"github.com/ORBTR/aether/rpc/pb"
	tenantScope "github.com/bbmumford/loom/pkg/rpc/scope"
)

// principalAxis returns the value the wire principal carries under the named
// axis, or "" — the send path projects loom's opaque axes as generic ScopeAxis.
func principalAxis(p *pb.AuthenticatedPrincipal, name string) string {
	for _, a := range p.GetAxes() {
		if a.GetName() == name {
			return a.GetValue()
		}
	}
	return ""
}

func TestBuildRPCRequestCtxCarriesTypedPrincipalOutsideMutableContext(t *testing.T) {
	identity := tenantScope.AuthenticatedIdentity{
		Scopes: []tenantScope.ScopeValue{
			{Name: tenantScope.AxisTransportPinned, Value: "orbtr"},
			{Name: tenantScope.AxisWithinTransport, Value: "org-A"},
		},
		Subject: "user-A",
	}
	principal, err := tenantScope.NewAuthenticatedPrincipal(
		identity,
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
	if got := principalAxis(req.Principal, tenantScope.AxisTransportPinned); got != "orbtr" {
		t.Fatalf("platform tenant axis = %q", got)
	}
	if got := principalAxis(req.Principal, tenantScope.AxisWithinTransport); got != "org-A" {
		t.Fatalf("customer org axis = %q", got)
	}
	if got := req.Principal.Subject; got != "user-A" {
		t.Fatalf("subject = %q", got)
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
	identity := tenantScope.AuthenticatedIdentity{
		Scopes: []tenantScope.ScopeValue{
			{Name: tenantScope.AxisTransportPinned, Value: "foreign-platform"},
			{Name: tenantScope.AxisWithinTransport, Value: "org-A"},
		},
		Subject: "user-A",
	}
	principal, err := tenantScope.NewAuthenticatedPrincipal(identity, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := tenantScope.WithAuthenticatedPrincipal(context.Background(), principal)

	req := buildRPCRequestCtx(ctx, "role", "handler", nil, time.Second, "orbtr")
	if req.Principal == nil {
		t.Fatal("mismatched principal was silently dropped")
	}
	if got := principalAxis(req.Principal, tenantScope.AxisTransportPinned); got != "foreign-platform" {
		t.Fatalf("principal rewritten to dispatch platform: %q", got)
	}
	if got := req.Context["tenantId"]; got != "orbtr" {
		t.Fatalf("dispatch platform context = %q", got)
	}
}
