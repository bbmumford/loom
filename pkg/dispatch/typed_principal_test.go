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
	rpcscope "github.com/bbmumford/loom/pkg/rpc/scope"
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
	identity := rpcscope.AuthenticatedIdentity{
		Scopes: []rpcscope.ScopeValue{
			{Name: rpcscope.AxisTransportPinned, Value: "p-pinned"},
			{Name: rpcscope.AxisWithinTransport, Value: "p-1"},
		},
		Subject: "user-A",
	}.WithPartition("p-1")
	principal, err := rpcscope.NewAuthenticatedPrincipal(
		identity,
		[]string{"example.plugins.read", "example.plugins.publish"},
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx := rpcscope.WithAuthenticatedPrincipal(context.Background(), principal)

	req := buildRPCRequestCtx(
		ctx,
		"example.plugins",
		"example.plugins.List",
		nil,
		time.Second,
	)

	if req.Principal == nil {
		t.Fatal("typed principal omitted from RPC envelope")
	}
	if got := principalAxis(req.Principal, rpcscope.AxisTransportPinned); got != "p-pinned" {
		t.Fatalf("pinned axis = %q", got)
	}
	if got := principalAxis(req.Principal, rpcscope.AxisWithinTransport); got != "p-1" {
		t.Fatalf("within-transport axis = %q", got)
	}
	if got := req.Principal.Subject; got != "user-A" {
		t.Fatalf("subject = %q", got)
	}
	if got := req.Principal.GetPartition(); got != "p-1" {
		t.Fatalf("partition = %q, want p-1", got)
	}
	if got := req.Principal.Scopes; len(got) != 2 ||
		got[0] != "example.plugins.read" ||
		got[1] != "example.plugins.publish" {
		t.Fatalf("scopes = %v", got)
	}
	if _, ok := req.Context["orgId"]; ok {
		t.Fatalf("principal axis leaked into mutable camel context: %v", req.Context)
	}
	if _, ok := req.Context["org_id"]; ok {
		t.Fatalf("principal axis leaked into mutable snake context: %v", req.Context)
	}
}

// TestBuildRPCRequestCtxCarriesThePinnedAxisVerbatim: the builder projects the
// sealed principal's pinned axis unchanged, and it writes no partition value
// of its own into the mutable request map.
func TestBuildRPCRequestCtxCarriesThePinnedAxisVerbatim(t *testing.T) {
	identity := rpcscope.AuthenticatedIdentity{
		Scopes: []rpcscope.ScopeValue{
			{Name: rpcscope.AxisTransportPinned, Value: "foreign-platform"},
			{Name: rpcscope.AxisWithinTransport, Value: "p-1"},
		},
		Subject: "user-A",
	}
	principal, err := rpcscope.NewAuthenticatedPrincipal(identity, nil)
	if err != nil {
		t.Fatal(err)
	}
	ctx := rpcscope.WithAuthenticatedPrincipal(context.Background(), principal)

	req := buildRPCRequestCtx(ctx, "role", "handler", nil, time.Second)
	if req.Principal == nil {
		t.Fatal("principal was silently dropped")
	}
	if got := principalAxis(req.Principal, rpcscope.AxisTransportPinned); got != "foreign-platform" {
		t.Fatalf("pinned axis rewritten: %q", got)
	}
	if v, ok := req.Context["tenantId"]; ok {
		t.Fatalf("request map carries tenantId=%q; want none", v)
	}
}
