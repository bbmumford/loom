/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ORBTR/aether/rpc/pb"
	"github.com/bbmumford/loom/node/handlers"
	tenantScope "github.com/bbmumford/loom/pkg/rpc/scope"
	"github.com/bbmumford/loom/ports"
)

type customerPrincipalProbe struct {
	calls int32
	got   tenantScope.AuthenticatedPrincipal
}

func (h *customerPrincipalProbe) Name() string               { return "orbtr.test.CustomerOrg" }
func (h *customerPrincipalProbe) Role() string               { return "orbtr.test" }
func (h *customerPrincipalProbe) RequiresAuth() bool         { return false }
func (h *customerPrincipalProbe) AllowedAuthTypes() []string { return nil }
func (h *customerPrincipalProbe) Scopes() []string           { return nil }
func (h *customerPrincipalProbe) TenantScope() handlers.TenantScope {
	return handlers.TenantScopeOrg
}
func (h *customerPrincipalProbe) AllowedTenants() []string { return nil }
func (h *customerPrincipalProbe) ExecuteRPC(
	ctx context.Context,
	_ *handlers.RPCRequest,
) (*handlers.RPCResponse, error) {
	atomic.AddInt32(&h.calls, 1)
	h.got, _ = tenantScope.AuthenticatedPrincipalFromContext(ctx)
	return &handlers.RPCResponse{Success: true}, nil
}

type customerPrincipalValidator struct{}

func (customerPrincipalValidator) ValidateExecutionAuth(
	context.Context,
	ports.SecureHandler,
) error {
	return nil
}
func (customerPrincipalValidator) WithTenantID(ctx context.Context, _ string) context.Context {
	return ctx
}
func (customerPrincipalValidator) WithAuthenticatedPrincipal(
	ctx context.Context,
	principal tenantScope.AuthenticatedPrincipal,
) context.Context {
	return tenantScope.WithAuthenticatedPrincipal(ctx, principal)
}

func customerPrincipalRequest() *pb.RPCRequest {
	return &pb.RPCRequest{
		Id:      "principal-request",
		Handler: "orbtr.test.CustomerOrg",
		Context: map[string]string{
			"tenantId": "orbtr",
			"orgId":    "org-A",
			"userId":   "user-A",
			"scopes":   "org.read org.write",
		},
		Principal: &pb.AuthenticatedPrincipal{
			PlatformTenantId: "orbtr",
			CustomerOrgId:    "org-A",
			UserId:           "user-A",
			Scopes:           []string{"org.read", "org.write"},
		},
	}
}

func TestRPCServerBindsTypedPrincipalFromTenantTransport(t *testing.T) {
	registry := handlers.NewHandlerRegistry()
	probe := &customerPrincipalProbe{}
	if err := registry.RegisterRPC(probe); err != nil {
		t.Fatal(err)
	}
	server := NewRPCServer(registry)
	server.SetAuthValidator(customerPrincipalValidator{})

	ctx := handlers.WithTransportTenant(context.Background(), "orbtr")
	resp := server.handleRequest(ctx, customerPrincipalRequest())

	if !resp.Success {
		t.Fatalf("typed principal request denied: %s", resp.Error)
	}
	if got := atomic.LoadInt32(&probe.calls); got != 1 {
		t.Fatalf("handler calls = %d", got)
	}
	if identity := probe.got.Identity(); identity != (tenantScope.AuthenticatedIdentity{
		PlatformTenantID: "orbtr",
		OrganizationID:   "org-A",
		UserID:           "user-A",
	}) {
		t.Fatalf("handler identity = %#v", identity)
	}
	if got := probe.got.Scopes(); len(got) != 2 ||
		got[0] != "org.read" ||
		got[1] != "org.write" {
		t.Fatalf("handler scopes = %v", got)
	}
}

func TestRPCServerRejectsPrincipalTamperingBeforeHandler(t *testing.T) {
	tests := []struct {
		name      string
		mutate    func(*pb.RPCRequest)
		transport string
		want      string
	}{
		{
			name: "org A principal org B request",
			mutate: func(req *pb.RPCRequest) {
				req.Context["orgId"] = "org-B"
			},
			transport: "orbtr",
			want:      "customer-org candidate mismatch",
		},
		{
			name: "org A principal snake organisation B request",
			mutate: func(req *pb.RPCRequest) {
				req.Context["organization_id"] = "org-B"
			},
			transport: "orbtr",
			want:      "customer-org candidate mismatch",
		},
		{
			name: "platform mismatch",
			mutate: func(req *pb.RPCRequest) {
				req.Principal.PlatformTenantId = "foreign-platform"
			},
			transport: "orbtr",
			want:      "transport/principal platform mismatch",
		},
		{
			name: "snake platform candidate mismatch",
			mutate: func(req *pb.RPCRequest) {
				req.Context["tenant_id"] = "foreign-platform"
			},
			transport: "orbtr",
			want:      "platform candidate mismatch",
		},
		{
			name:      "unbound transport",
			mutate:    func(*pb.RPCRequest) {},
			transport: "default",
			want:      "tenant-bound transport",
		},
		{
			name: "forged scopes",
			mutate: func(req *pb.RPCRequest) {
				req.Context["scopes"] = "org.admin"
			},
			transport: "orbtr",
			want:      "scope candidate mismatch",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			registry := handlers.NewHandlerRegistry()
			probe := &customerPrincipalProbe{}
			if err := registry.RegisterRPC(probe); err != nil {
				t.Fatal(err)
			}
			server := NewRPCServer(registry)
			server.SetAuthValidator(customerPrincipalValidator{})
			req := customerPrincipalRequest()
			tc.mutate(req)

			ctx := handlers.WithTransportTenant(context.Background(), tc.transport)
			resp := server.handleRequest(ctx, req)

			if resp.Success || !strings.Contains(resp.Error, tc.want) {
				t.Fatalf("response = success:%v error:%q, want %q", resp.Success, resp.Error, tc.want)
			}
			if got := atomic.LoadInt32(&probe.calls); got != 0 {
				t.Fatalf("handler ran %d times after principal denial", got)
			}
		})
	}
}

func TestRPCServerMutableContextCannotCreateCustomerPrincipal(t *testing.T) {
	registry := handlers.NewHandlerRegistry()
	probe := &customerPrincipalProbe{}
	if err := registry.RegisterRPC(probe); err != nil {
		t.Fatal(err)
	}
	server := NewRPCServer(registry)
	server.SetAuthValidator(customerPrincipalValidator{})
	req := customerPrincipalRequest()
	req.Principal = nil

	ctx := handlers.WithTransportTenant(context.Background(), "orbtr")
	resp := server.handleRequest(ctx, req)

	if resp.Success || !strings.Contains(resp.Error, "authenticated organisation") {
		t.Fatalf("mutable context gained authority: success:%v error:%q", resp.Success, resp.Error)
	}
	if got := atomic.LoadInt32(&probe.calls); got != 0 {
		t.Fatalf("handler ran %d times without typed principal", got)
	}
}

type principalForwarder struct {
	got *pb.RPCRequest
}

func (f *principalForwarder) Forward(
	_ context.Context,
	req *pb.RPCRequest,
) (*pb.RPCResponse, error) {
	f.got = req
	return &pb.RPCResponse{Id: req.Id, Success: true}, nil
}

func TestRPCServerForwardingPreservesTypedPrincipal(t *testing.T) {
	server := NewRPCServer(handlers.NewHandlerRegistry())
	server.SetAuthValidator(customerPrincipalValidator{})
	forwarder := &principalForwarder{}
	server.SetForwarder(forwarder)

	ctx := handlers.WithTransportTenant(context.Background(), "orbtr")
	resp := server.handleRequest(ctx, customerPrincipalRequest())

	if !resp.Success {
		t.Fatalf("forwarded request failed: %s", resp.Error)
	}
	if forwarder.got == nil || forwarder.got.Principal == nil {
		t.Fatal("forwarder did not receive typed principal")
	}
	if got := forwarder.got.Principal.CustomerOrgId; got != "org-A" {
		t.Fatalf("forwarded customer org = %q", got)
	}
}
