/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ORBTR/aether/rpc/pb"
	"github.com/bbmumford/loom/node/handlers"
	rpcscope "github.com/bbmumford/loom/pkg/rpc/scope"
	"github.com/bbmumford/loom/ports"
)

// typedPrincipalScopePolicy is a consumer-side PresencePolicy that admits a
// principal carrying a transport-pinned value, and for ScopeWithinPartition a
// within-transport value too. The receive path reconstructs the opaque identity
// and this policy judges it; loom carries no presence semantics of its own.
func typedPrincipalScopePolicy() rpcscope.PresencePolicyFunc {
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

type typedPrincipalProbe struct {
	calls int32
	got   rpcscope.AuthenticatedPrincipal
}

func (h *typedPrincipalProbe) Name() string               { return "orbtr.test.CustomerOrg" }
func (h *typedPrincipalProbe) Role() string               { return "orbtr.test" }
func (h *typedPrincipalProbe) RequiresAuth() bool         { return false }
func (h *typedPrincipalProbe) AllowedAuthTypes() []string { return nil }
func (h *typedPrincipalProbe) Scopes() []string           { return nil }
func (h *typedPrincipalProbe) Scope() handlers.Scope {
	return handlers.ScopeWithinPartition
}
func (h *typedPrincipalProbe) AllowedPartitions() []string { return nil }
func (h *typedPrincipalProbe) ExecuteRPC(
	ctx context.Context,
	_ *handlers.RPCRequest,
) (*handlers.RPCResponse, error) {
	atomic.AddInt32(&h.calls, 1)
	h.got, _ = rpcscope.AuthenticatedPrincipalFromContext(ctx)
	return &handlers.RPCResponse{Success: true}, nil
}

type typedPrincipalValidator struct{}

func (typedPrincipalValidator) ValidateExecutionAuth(
	context.Context,
	ports.SecureHandler,
) error {
	return nil
}
func (typedPrincipalValidator) WithPartitionID(ctx context.Context, _ string) context.Context {
	return ctx
}
func (typedPrincipalValidator) WithAuthenticatedPrincipal(
	ctx context.Context,
	principal rpcscope.AuthenticatedPrincipal,
) context.Context {
	return rpcscope.WithAuthenticatedPrincipal(ctx, principal)
}

// AdmitPrincipal is a consumer admitter with the rule a composition root applies: the
// pinned axis a peer asserts must equal the transport partition it arrived on.
// loom applies no rule of its own; this one lives in the consumer.
func (typedPrincipalValidator) AdmitPrincipal(
	_ context.Context,
	transportPartition string,
	principal rpcscope.AuthenticatedPrincipal,
) error {
	if pinned := principal.Identity().Scope(rpcscope.AxisTransportPinned); pinned != transportPartition {
		return fmt.Errorf("pinned axis %q does not match the transport", pinned)
	}
	return nil
}

func typedPrincipalRequest() *pb.RPCRequest {
	return &pb.RPCRequest{
		Id:      "principal-request",
		Handler: "orbtr.test.CustomerOrg",
		Principal: &pb.AuthenticatedPrincipal{
			Subject: "user-A",
			Axes: []*pb.ScopeAxis{
				{Name: rpcscope.AxisTransportPinned, Value: "orbtr"},
				{Name: rpcscope.AxisWithinTransport, Value: "org-A"},
			},
			Scopes:    []string{"org.read", "org.write"},
			Partition: "org-A",
		},
	}
}

func TestRPCServerBindsTypedPrincipalFromPartitionTransport(t *testing.T) {
	registry := handlers.NewHandlerRegistry()
	registry.SetPresencePolicy(typedPrincipalScopePolicy())
	probe := &typedPrincipalProbe{}
	if err := registry.RegisterRPC(probe); err != nil {
		t.Fatal(err)
	}
	server := NewRPCServer(registry)
	server.SetAuthValidator(typedPrincipalValidator{})

	ctx := handlers.WithTransportPartition(context.Background(), "orbtr")
	resp := server.handleRequest(ctx, typedPrincipalRequest())

	if !resp.Success {
		t.Fatalf("typed principal request denied: %s", resp.Error)
	}
	if got := atomic.LoadInt32(&probe.calls); got != 1 {
		t.Fatalf("handler calls = %d", got)
	}
	if identity := probe.got.Identity(); identity.Scope(rpcscope.AxisTransportPinned) != "orbtr" ||
		identity.Scope(rpcscope.AxisWithinTransport) != "org-A" ||
		identity.Partition() != "org-A" ||
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
			// pinned axis is another partition. The consumer admitter rejects it.
			name: "pinned-axis forge",
			mutate: func(req *pb.RPCRequest) {
				req.Principal.Axes[0].Value = "foreign-platform"
			},
			transport: "orbtr",
			want:      "does not match the transport",
		},
		{
			// Forge by omission: the pinned axis is absent, so its value cannot
			// equal the transport partition.
			name: "pinned-axis absent",
			mutate: func(req *pb.RPCRequest) {
				req.Principal.Axes = req.Principal.Axes[1:] // drop the pinned axis
			},
			transport: "orbtr",
			want:      "does not match the transport",
		},
		{
			name:      "unbound transport",
			mutate:    func(*pb.RPCRequest) {},
			transport: "",
			want:      "partition-bound transport",
		},
		{
			// A partition whose name is "default" is bound like any other, so
			// the consumer admitter judges the principal on it.
			name:      "partition named default",
			mutate:    func(*pb.RPCRequest) {},
			transport: "default",
			want:      "does not match the transport",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			registry := handlers.NewHandlerRegistry()
			probe := &typedPrincipalProbe{}
			if err := registry.RegisterRPC(probe); err != nil {
				t.Fatal(err)
			}
			server := NewRPCServer(registry)
			server.SetAuthValidator(typedPrincipalValidator{})
			req := typedPrincipalRequest()
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

// The no-over-pin proof: only the transport-pinned axis is pinned, so a
// principal whose within-transport axis is any value WITHIN the transport
// partition is admitted and round-trips unchanged. Pinning the
// within-transport axis too would deny legitimate within-partition traffic
// that spans many values of that axis.
func TestRPCServerAdmitsAnyWithinTransportValue(t *testing.T) {
	registry := handlers.NewHandlerRegistry()
	registry.SetPresencePolicy(typedPrincipalScopePolicy())
	probe := &typedPrincipalProbe{}
	if err := registry.RegisterRPC(probe); err != nil {
		t.Fatal(err)
	}
	server := NewRPCServer(registry)
	server.SetAuthValidator(typedPrincipalValidator{})

	req := typedPrincipalRequest()
	req.Principal.Axes[1].Value = "org-Z" // a different within-transport value, same pinned value

	ctx := handlers.WithTransportPartition(context.Background(), "orbtr")
	resp := server.handleRequest(ctx, req)

	if !resp.Success {
		t.Fatalf("within-tenant customer org denied (over-pin regression): %s", resp.Error)
	}
	if got := probe.got.Identity().Scope(rpcscope.AxisWithinTransport); got != "org-Z" {
		t.Fatalf("customer-org axis round-trip = %q, want org-Z", got)
	}
}

func TestRPCServerMutableContextCannotCreatePrincipal(t *testing.T) {
	registry := handlers.NewHandlerRegistry()
	registry.SetPresencePolicy(typedPrincipalScopePolicy())
	probe := &typedPrincipalProbe{}
	if err := registry.RegisterRPC(probe); err != nil {
		t.Fatal(err)
	}
	server := NewRPCServer(registry)
	server.SetAuthValidator(typedPrincipalValidator{})
	req := typedPrincipalRequest()
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
	server.SetAuthValidator(typedPrincipalValidator{})
	forwarder := &principalForwarder{}
	server.SetForwarder(forwarder)

	ctx := handlers.WithTransportPartition(context.Background(), "orbtr")
	resp := server.handleRequest(ctx, typedPrincipalRequest())

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

// stampingPartitionValidator's WithPartitionID stamps the transport-pinned axis,
// where typedPrincipalValidator's returns ctx unchanged. The test below needs a
// validator that stamps, so that a WithPartitionID call made from the
// mutable req.Context map would show up as an admitted handler.
type stampingPartitionValidator struct{}

func (stampingPartitionValidator) ValidateExecutionAuth(context.Context, ports.SecureHandler) error {
	return nil
}
func (stampingPartitionValidator) WithPartitionID(ctx context.Context, partitionID string) context.Context {
	return rpcscope.WithAuthenticatedScope(ctx, rpcscope.AxisTransportPinned, partitionID)
}
func (stampingPartitionValidator) WithAuthenticatedPrincipal(
	ctx context.Context,
	principal rpcscope.AuthenticatedPrincipal,
) context.Context {
	return rpcscope.WithAuthenticatedPrincipal(ctx, principal)
}

// TestRPCServerRejectsWirePartitionWithoutAuthenticatedTransport requires
// node/rpc.go to take no transport partition from the mutable request map. The
// request carries a partition value only in req.Context (no authenticated transport, no
// typed Principal), and stampingPartitionValidator stamps the pinned axis if
// WithPartitionID is called. handleRequest calls it for no map value, so the
// pinned axis stays empty and the partition-scoped handler is denied
// (calls==0). A branch such as
// `else if v := req.Context[key]; v != "" { WithPartitionID }` in
// handleRequest turns this red: the handler is invoked.
func TestRPCServerRejectsWirePartitionWithoutAuthenticatedTransport(t *testing.T) {
	registry := handlers.NewHandlerRegistry()
	registry.SetPresencePolicy(typedPrincipalScopePolicy())
	probe := &platformOnlyProbe{}
	if err := registry.RegisterRPC(probe); err != nil {
		t.Fatal(err)
	}
	server := NewRPCServer(registry)
	server.SetAuthValidator(stampingPartitionValidator{})

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
