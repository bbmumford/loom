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
	rpcscope "github.com/bbmumford/loom/pkg/rpc/scope"
	"github.com/bbmumford/loom/ports"
)

// customerScopePolicy is a CONSUMER-side reference PresencePolicy admitting a
// customer-org principal. loom no longer contains presence semantics — the
// receive path reconstructs the opaque identity and this policy judges it.
func customerScopePolicy() rpcscope.PresencePolicyFunc {
	return func(declared handlers.Scope, id rpcscope.AuthenticatedIdentity) (bool, string) {
		partition := id.Scope(rpcscope.AxisTransportPinned)
		switch declared {
		case handlers.ScopeNone:
			return true, ""
		case handlers.ScopeWithinPartition:
			if partition == "" {
				return false, "authenticated platform tenant context required"
			}
			if id.Scope(rpcscope.AxisWithinTransport) == "" {
				return false, "authenticated organisation context required"
			}
			return true, ""
		default:
			if partition == "" {
				return false, "authenticated platform tenant context required"
			}
			return true, ""
		}
	}
}

type customerPrincipalProbe struct {
	calls int32
	got   rpcscope.AuthenticatedPrincipal
}

func (h *customerPrincipalProbe) Name() string               { return "orbtr.test.CustomerOrg" }
func (h *customerPrincipalProbe) Role() string               { return "orbtr.test" }
func (h *customerPrincipalProbe) RequiresAuth() bool         { return false }
func (h *customerPrincipalProbe) AllowedAuthTypes() []string { return nil }
func (h *customerPrincipalProbe) Scopes() []string           { return nil }
func (h *customerPrincipalProbe) Scope() handlers.Scope {
	return handlers.ScopeWithinPartition
}
func (h *customerPrincipalProbe) AllowedPartitions() []string { return nil }
func (h *customerPrincipalProbe) ExecuteRPC(
	ctx context.Context,
	_ *handlers.RPCRequest,
) (*handlers.RPCResponse, error) {
	atomic.AddInt32(&h.calls, 1)
	h.got, _ = rpcscope.AuthenticatedPrincipalFromContext(ctx)
	return &handlers.RPCResponse{Success: true}, nil
}

type customerPrincipalValidator struct{}

func (customerPrincipalValidator) ValidateExecutionAuth(
	context.Context,
	ports.SecureHandler,
) error {
	return nil
}
func (customerPrincipalValidator) WithPartitionID(ctx context.Context, _ string) context.Context {
	return ctx
}
func (customerPrincipalValidator) WithAuthenticatedPrincipal(
	ctx context.Context,
	principal rpcscope.AuthenticatedPrincipal,
) context.Context {
	return rpcscope.WithAuthenticatedPrincipal(ctx, principal)
}

// TransportPinnedAxis names the platform-partition axis as the one the RPC server
// gates against the transport partition (ports.TransportAxisPin) — the consumer
// supplies the name; loom never hard-codes it.
func (customerPrincipalValidator) TransportPinnedAxis() string {
	return rpcscope.AxisTransportPinned
}

func customerPrincipalRequest() *pb.RPCRequest {
	return &pb.RPCRequest{
		Id:      "principal-request",
		Handler: "orbtr.test.CustomerOrg",
		Principal: &pb.AuthenticatedPrincipal{
			Subject: "user-A",
			Axes: []*pb.ScopeAxis{
				{Name: rpcscope.AxisTransportPinned, Value: "orbtr"},
				{Name: rpcscope.AxisWithinTransport, Value: "org-A"},
			},
			Scopes: []string{"org.read", "org.write"},
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

	ctx := handlers.WithTransportPartition(context.Background(), "orbtr")
	resp := server.handleRequest(ctx, customerPrincipalRequest())

	if !resp.Success {
		t.Fatalf("typed principal request denied: %s", resp.Error)
	}
	if got := atomic.LoadInt32(&probe.calls); got != 1 {
		t.Fatalf("handler calls = %d", got)
	}
	if identity := probe.got.Identity(); identity.Scope(rpcscope.AxisTransportPinned) != "orbtr" ||
		identity.Scope(rpcscope.AxisWithinTransport) != "org-A" ||
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
			// Forge: a peer bound to transport orbtr presents a principal whose
			// pinned (platform-partition) axis is another partition. The gate rejects it.
			name: "pinned-axis forge",
			mutate: func(req *pb.RPCRequest) {
				req.Principal.Axes[0].Value = "foreign-platform"
			},
			transport: "orbtr",
			want:      "pinned-axis mismatch",
		},
		{
			// Forge by omission: the pinned axis is absent, so its value cannot
			// equal the transport partition.
			name: "pinned-axis absent",
			mutate: func(req *pb.RPCRequest) {
				req.Principal.Axes = req.Principal.Axes[1:] // drop platform_tenant
			},
			transport: "orbtr",
			want:      "pinned-axis mismatch",
		},
		{
			name:      "unbound transport",
			mutate:    func(*pb.RPCRequest) {},
			transport: "default",
			want:      "tenant-bound transport",
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

			ctx := handlers.WithTransportPartition(context.Background(), tc.transport)
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

// TestRPCServerAdmitsWithinTenantCustomerOrg is the no-over-pin proof: only the
// platform-partition axis is transport-pinned, so a principal whose customer-org
// axis is any value WITHIN the transport partition is admitted and round-trips
// unchanged. Pinning the customer-org axis too would deny legitimate
// within-partition multi-org traffic (the availability regression orb's stage-4
// spec warns against).
func TestRPCServerAdmitsWithinTenantCustomerOrg(t *testing.T) {
	registry := handlers.NewHandlerRegistry()
	registry.SetPresencePolicy(customerScopePolicy())
	probe := &customerPrincipalProbe{}
	if err := registry.RegisterRPC(probe); err != nil {
		t.Fatal(err)
	}
	server := NewRPCServer(registry)
	server.SetAuthValidator(customerPrincipalValidator{})

	req := customerPrincipalRequest()
	req.Principal.Axes[1].Value = "org-Z" // a different customer org, same platform tenant

	ctx := handlers.WithTransportPartition(context.Background(), "orbtr")
	resp := server.handleRequest(ctx, req)

	if !resp.Success {
		t.Fatalf("within-tenant customer org denied (over-pin regression): %s", resp.Error)
	}
	if got := probe.got.Identity().Scope(rpcscope.AxisWithinTransport); got != "org-Z" {
		t.Fatalf("customer-org axis round-trip = %q, want org-Z", got)
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

	ctx := handlers.WithTransportPartition(context.Background(), "orbtr")
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

	ctx := handlers.WithTransportPartition(context.Background(), "orbtr")
	resp := server.handleRequest(ctx, customerPrincipalRequest())

	if !resp.Success {
		t.Fatalf("forwarded request failed: %s", resp.Error)
	}
	if forwarder.got == nil || forwarder.got.Principal == nil {
		t.Fatal("forwarder did not receive typed principal")
	}
	if got := forwarder.got.Principal.GetAxes(); len(got) != 2 || got[1].GetValue() != "org-A" {
		t.Fatalf("forwarded customer org axis = %#v", got)
	}
}

// platformOnlyProbe is a platform-tier-scoped handler (ScopePartition): the
// presence policy admits it only when the platform axis is present. It counts
// invocations so a test can assert it was NOT reached.
type platformOnlyProbe struct{ calls int32 }

func (h *platformOnlyProbe) Name() string                { return "orbtr.test.PlatformOnly" }
func (h *platformOnlyProbe) Role() string                { return "orbtr.test" }
func (h *platformOnlyProbe) RequiresAuth() bool          { return false }
func (h *platformOnlyProbe) AllowedAuthTypes() []string  { return nil }
func (h *platformOnlyProbe) Scopes() []string            { return nil }
func (h *platformOnlyProbe) Scope() handlers.Scope       { return handlers.ScopePartition }
func (h *platformOnlyProbe) AllowedPartitions() []string { return nil }
func (h *platformOnlyProbe) ExecuteRPC(
	ctx context.Context,
	_ *handlers.RPCRequest,
) (*handlers.RPCResponse, error) {
	atomic.AddInt32(&h.calls, 1)
	return &handlers.RPCResponse{Success: true}, nil
}

// stampingTenantValidator's WithPartitionID ACTUALLY stamps platform authority (unlike
// customerPrincipalValidator's no-op). It exists so this test can discriminate the
// removed req.Context["tenantId"] fallback: only a validator that stamps on
// WithPartitionID makes the fallback's authority-minting observable.
type stampingTenantValidator struct{}

func (stampingTenantValidator) ValidateExecutionAuth(context.Context, ports.SecureHandler) error {
	return nil
}
func (stampingTenantValidator) WithPartitionID(ctx context.Context, partitionID string) context.Context {
	return rpcscope.WithAuthenticatedScope(ctx, rpcscope.AxisTransportPinned, partitionID)
}
func (stampingTenantValidator) WithAuthenticatedPrincipal(
	ctx context.Context,
	principal rpcscope.AuthenticatedPrincipal,
) context.Context {
	return rpcscope.WithAuthenticatedPrincipal(ctx, principal)
}

// TestRPCServerRejectsWireTenantWithoutAuthenticatedTransport is the mutation-
// discriminating proof that node/rpc.go does NOT mint platform authority from the
// mutable req.Context["tenantId"] map. The request carries tenantId ONLY in that map
// (no authenticated transport, no typed Principal), and the validator would stamp
// platform authority if WithPartitionID were called. With the fallback removed, nothing
// calls WithPartitionID, so the platform axis stays empty and the platform-scoped
// handler is DENIED (calls==0). Restoring the deleted
// `else if tid := req.Context["tenantId"]; tid != "" { WithPartitionID }` branch turns
// this RED: WithPartitionID stamps the platform partition and the handler is invoked.
func TestRPCServerRejectsWireTenantWithoutAuthenticatedTransport(t *testing.T) {
	registry := handlers.NewHandlerRegistry()
	registry.SetPresencePolicy(customerScopePolicy())
	probe := &platformOnlyProbe{}
	if err := registry.RegisterRPC(probe); err != nil {
		t.Fatal(err)
	}
	server := NewRPCServer(registry)
	server.SetAuthValidator(stampingTenantValidator{})

	// Deliberately NO handlers.WithTransportPartition: the only partition signal is the
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
