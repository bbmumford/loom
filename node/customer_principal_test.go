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

// customerScopePolicy is a CONSUMER-side reference PresencePolicy admitting a
// customer-org principal. loom no longer contains presence semantics — the
// receive path reconstructs the opaque identity and this policy judges it.
func customerScopePolicy() tenantScope.PresencePolicyFunc {
	return func(declared handlers.TenantScope, id tenantScope.AuthenticatedIdentity) (bool, string) {
		tenant := id.Scope(tenantScope.AxisPlatformTenant)
		switch declared {
		case handlers.TenantScopeNone:
			return true, ""
		case handlers.TenantScopeOrg:
			if tenant == "" {
				return false, "authenticated platform tenant context required"
			}
			if id.Scope(tenantScope.AxisCustomerOrg) == "" {
				return false, "authenticated organisation context required"
			}
			return true, ""
		default:
			if tenant == "" {
				return false, "authenticated platform tenant context required"
			}
			return true, ""
		}
	}
}

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
	registry.SetPresencePolicy(customerScopePolicy())
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
	if identity := probe.got.Identity(); identity.Scope(tenantScope.AxisPlatformTenant) != "orbtr" ||
		identity.Scope(tenantScope.AxisCustomerOrg) != "org-A" ||
		identity.Subject != "user-A" {
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
	registry.SetPresencePolicy(customerScopePolicy())
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

// platformOnlyProbe is a platform-tier-scoped handler (TenantScopeTenant): the
// presence policy admits it only when the platform axis is present. It counts
// invocations so a test can assert it was NOT reached.
type platformOnlyProbe struct{ calls int32 }

func (h *platformOnlyProbe) Name() string                      { return "orbtr.test.PlatformOnly" }
func (h *platformOnlyProbe) Role() string                      { return "orbtr.test" }
func (h *platformOnlyProbe) RequiresAuth() bool                { return false }
func (h *platformOnlyProbe) AllowedAuthTypes() []string        { return nil }
func (h *platformOnlyProbe) Scopes() []string                  { return nil }
func (h *platformOnlyProbe) TenantScope() handlers.TenantScope { return handlers.TenantScopeTenant }
func (h *platformOnlyProbe) AllowedTenants() []string          { return nil }
func (h *platformOnlyProbe) ExecuteRPC(
	ctx context.Context,
	_ *handlers.RPCRequest,
) (*handlers.RPCResponse, error) {
	atomic.AddInt32(&h.calls, 1)
	return &handlers.RPCResponse{Success: true}, nil
}

// stampingTenantValidator's WithTenantID ACTUALLY stamps platform authority (unlike
// customerPrincipalValidator's no-op). It exists so this test can discriminate the
// removed req.Context["tenantId"] fallback: only a validator that stamps on
// WithTenantID makes the fallback's authority-minting observable.
type stampingTenantValidator struct{}

func (stampingTenantValidator) ValidateExecutionAuth(context.Context, ports.SecureHandler) error {
	return nil
}
func (stampingTenantValidator) WithTenantID(ctx context.Context, tenantID string) context.Context {
	return tenantScope.WithAuthenticatedScope(ctx, tenantScope.AxisPlatformTenant, tenantID)
}
func (stampingTenantValidator) WithAuthenticatedPrincipal(
	ctx context.Context,
	principal tenantScope.AuthenticatedPrincipal,
) context.Context {
	return tenantScope.WithAuthenticatedPrincipal(ctx, principal)
}

// TestRPCServerRejectsWireTenantWithoutAuthenticatedTransport is the mutation-
// discriminating proof that node/rpc.go does NOT mint platform authority from the
// mutable req.Context["tenantId"] map. The request carries tenantId ONLY in that map
// (no authenticated transport, no typed Principal), and the validator would stamp
// platform authority if WithTenantID were called. With the fallback removed, nothing
// calls WithTenantID, so the platform axis stays empty and the platform-scoped
// handler is DENIED (calls==0). Restoring the deleted
// `else if tid := req.Context["tenantId"]; tid != "" { WithTenantID }` branch turns
// this RED: WithTenantID stamps the platform tenant and the handler is invoked.
func TestRPCServerRejectsWireTenantWithoutAuthenticatedTransport(t *testing.T) {
	registry := handlers.NewHandlerRegistry()
	registry.SetPresencePolicy(customerScopePolicy())
	probe := &platformOnlyProbe{}
	if err := registry.RegisterRPC(probe); err != nil {
		t.Fatal(err)
	}
	server := NewRPCServer(registry)
	server.SetAuthValidator(stampingTenantValidator{})

	// Deliberately NO handlers.WithTransportTenant: the only tenant signal is the
	// mutable req.Context map, which must never establish authority.
	req := &pb.RPCRequest{
		Id:      "wire-tenant-no-transport",
		Handler: "orbtr.test.PlatformOnly",
		Context: map[string]string{"tenantId": "orbtr"},
	}
	resp := server.handleRequest(context.Background(), req)

	if got := atomic.LoadInt32(&probe.calls); got != 0 {
		t.Fatalf("platform-scoped handler invoked with no authenticated transport — req.Context tenantId minted authority (the removed fallback regressed): calls=%d", got)
	}
	if resp.Success {
		t.Fatalf("request succeeded on a mutable-map tenant with no authenticated transport: %s", resp.Error)
	}
}
