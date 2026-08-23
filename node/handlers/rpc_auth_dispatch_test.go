/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package handlers

import (
	"context"
	"sync/atomic"
	"testing"

	tenantScope "github.com/bbmumford/loom/pkg/rpc/scope"
	"github.com/bbmumford/loom/ports"
)

type resolvedRPCProbe struct {
	name  string
	scope TenantScope
	ran   atomic.Int32
}

func (h *resolvedRPCProbe) Name() string             { return h.name }
func (*resolvedRPCProbe) Role() string               { return "resolved-rpc-test" }
func (*resolvedRPCProbe) RequiresAuth() bool         { return true }
func (*resolvedRPCProbe) AllowedAuthTypes() []string { return []string{"service"} }
func (*resolvedRPCProbe) Scopes() []string           { return []string{"rpc.execute"} }
func (h *resolvedRPCProbe) TenantScope() TenantScope {
	if h.scope == "" {
		return TenantScopeNone
	}
	return h.scope
}
func (*resolvedRPCProbe) AllowedTenants() []string { return nil }
func (h *resolvedRPCProbe) ExecuteRPC(context.Context, *RPCRequest) (*RPCResponse, error) {
	h.ran.Add(1)
	return &RPCResponse{Success: true}, nil
}

type allowingRPCAuth struct {
	calls atomic.Int32
}

func (a *allowingRPCAuth) ValidateExecutionAuth(context.Context, ports.SecureHandler) error {
	a.calls.Add(1)
	return nil
}

func (*allowingRPCAuth) WithTenantID(ctx context.Context, _ string) context.Context {
	return ctx
}

func TestResolvedRPCWithAuthPinsExactRegistrationAcrossReplacement(t *testing.T) {
	registry := NewHandlerRegistry()
	first := &resolvedRPCProbe{name: "rpc.exact"}
	replacement := &resolvedRPCProbe{name: first.name}
	if err := registry.RegisterRPC(first); err != nil {
		t.Fatalf("RegisterRPC(first): %v", err)
	}
	resolved, ok := registry.Resolve(first.name)
	if !ok {
		t.Fatal("Resolve(first) = false")
	}
	if !registry.Unregister(first.name) {
		t.Fatal("Unregister(first) = false")
	}
	if err := registry.RegisterRPC(replacement); err != nil {
		t.Fatalf("RegisterRPC(replacement): %v", err)
	}
	auth := &allowingRPCAuth{}

	resp, err := resolved.DispatchRPCWithAuth(
		context.Background(),
		&RPCRequest{Handler: first.name},
		auth,
	)
	if err != nil || resp == nil || !resp.Success {
		t.Fatalf("captured dispatch = (%+v, %v)", resp, err)
	}
	if got := first.ran.Load(); got != 1 {
		t.Fatalf("captured handler calls = %d, want 1", got)
	}
	if got := replacement.ran.Load(); got != 0 {
		t.Fatalf("replacement stole captured dispatch: calls=%d", got)
	}

	resp, err = registry.DispatchRPCWithAuth(
		context.Background(),
		&RPCRequest{Handler: replacement.name},
		auth,
	)
	if err != nil || resp == nil || !resp.Success {
		t.Fatalf("current dispatch = (%+v, %v)", resp, err)
	}
	if got := replacement.ran.Load(); got != 1 {
		t.Fatalf("replacement handler calls = %d, want 1", got)
	}
	if got := auth.calls.Load(); got != 2 {
		t.Fatalf("auth calls = %d, want 2", got)
	}
}

func TestAuthenticatedRPCScopeIgnoresRequestIdentityHints(t *testing.T) {
	registry := NewHandlerRegistry()
	handler := &resolvedRPCProbe{name: "rpc.org", scope: TenantScopeOrg}
	var events []string
	registry.Use(&taskDispatchMiddleware{name: "scope-mw", events: &events})
	if err := registry.RegisterRPC(handler); err != nil {
		t.Fatalf("RegisterRPC: %v", err)
	}
	auth := &allowingRPCAuth{}
	req := &RPCRequest{
		Handler: handler.name,
		Context: map[string]interface{}{
			"tenantId":  "evil-camel",
			"tenant_id": "evil-snake",
			"orgId":     "evil-camel",
			"org_id":    "evil-snake",
		},
	}

	platformOnly := WithTransportTenant(context.Background(), "orbtr")
	resp, err := registry.DispatchRPCWithAuth(platformOnly, req, auth)
	if err != nil {
		t.Fatalf("map-only DispatchRPCWithAuth error: %v", err)
	}
	if resp == nil || resp.Success || resp.Error == "" {
		t.Fatalf("map-only response=%+v, want tenant restriction", resp)
	}
	if got := handler.ran.Load(); got != 0 {
		t.Fatalf("handler ran on map-only organisation: %d", got)
	}
	if len(events) != 0 {
		t.Fatalf("middleware ran before scope denial: %v", events)
	}

	exact := tenantScope.WithAuthenticatedIdentity(platformOnly, tenantScope.AuthenticatedIdentity{
		PlatformTenantID: "orbtr",
		OrganizationID:   "org-1",
		UserID:           "user-1",
	})
	resp, err = registry.DispatchRPCWithAuth(exact, req, auth)
	if err != nil || resp == nil || !resp.Success {
		t.Fatalf("exact authenticated DispatchRPCWithAuth=(%+v, %v)", resp, err)
	}
	if got := handler.ran.Load(); got != 1 {
		t.Fatalf("handler calls=%d, want 1", got)
	}
	if len(events) != 2 || events[0] != "scope-mw.before" || events[1] != "scope-mw.after" {
		t.Fatalf("middleware events=%v, want before/after", events)
	}
}
