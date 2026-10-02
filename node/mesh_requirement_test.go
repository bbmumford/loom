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
)

const testInferenceCap = "orbtr.ai:inference.dispatch"

// requirementProbe declares ScopeAllowlisted, which typedPrincipalScopePolicy admits
// for any principal with a pinned axis, plus an optional Requirement. With a
// Requirement declared, a denial can therefore only come from the
// authorization arm, never from the scope tag.
type requirementProbe struct {
	typedPrincipalProbe
	req *rpcscope.Requirement
}

func (h *requirementProbe) Scope() handlers.Scope { return handlers.ScopeAllowlisted }

func (h *requirementProbe) Requirement() *rpcscope.Requirement { return h.req }

// grantAuthorizer confers a capability exactly when the principal's grants
// list it, the way a consumer's warrant adapter reads principal.Scopes().
type grantAuthorizer struct{}

func (grantAuthorizer) HasCapability(p rpcscope.AuthenticatedPrincipal, capability string) bool {
	for _, g := range p.Scopes() {
		if g == capability {
			return true
		}
	}
	return false
}

// partitionStampingValidator derives the receiver's partition from the
// within-transport axis when it stamps the sealed principal, as a consumer's
// AuthenticatedPrincipalStamper does: loom rebuilds the wire identity with no
// partition, so the nexus arm sees one only after this stamp.
type partitionStampingValidator struct{ typedPrincipalValidator }

func (partitionStampingValidator) WithAuthenticatedPrincipal(
	ctx context.Context,
	principal rpcscope.AuthenticatedPrincipal,
) context.Context {
	identity := principal.Identity()
	stamped, err := rpcscope.NewAuthenticatedPrincipal(
		identity.WithPartition(identity.Scope(rpcscope.AxisWithinTransport)), principal.Scopes())
	if err != nil {
		return rpcscope.WithAuthenticatedPrincipal(ctx, principal)
	}
	return rpcscope.WithAuthenticatedPrincipal(ctx, stamped)
}

func requirementServer(t *testing.T, probe *requirementProbe, authz *rpcscope.Policy) *RPCServer {
	t.Helper()
	registry := handlers.NewHandlerRegistry()
	registry.SetPresencePolicy(typedPrincipalScopePolicy())
	if authz != nil {
		registry.SetAuthzPolicy(authz)
	}
	if err := registry.RegisterRPC(probe); err != nil {
		t.Fatal(err)
	}
	server := NewRPCServer(registry)
	server.SetAuthValidator(partitionStampingValidator{})
	return server
}

func requirementRequest(grants []string, axes ...*pb.ScopeAxis) *pb.RPCRequest {
	req := typedPrincipalRequest()
	req.Principal.Scopes = grants
	if len(axes) > 0 {
		req.Principal.Axes = axes
	}
	return req
}

func sendOverPartitionTransport(server *RPCServer, req *pb.RPCRequest) *pb.RPCResponse {
	ctx := handlers.WithTransportPartition(context.Background(), "orbtr")
	return server.handleRequest(ctx, req)
}

func TestMeshRequirementCapabilityArm(t *testing.T) {
	capability := rpcscope.Capability(testInferenceCap)
	policy := rpcscope.NewPolicy(grantAuthorizer{})

	t.Run("a principal holding the capability is admitted", func(t *testing.T) {
		probe := &requirementProbe{req: &capability}
		resp := sendOverPartitionTransport(requirementServer(t, probe, policy),
			requirementRequest([]string{testInferenceCap}))
		if !resp.Success || atomic.LoadInt32(&probe.calls) != 1 {
			t.Fatalf("granted principal: success=%v calls=%d err=%q", resp.Success, probe.calls, resp.Error)
		}
	})

	t.Run("a principal without the capability is denied before the handler runs", func(t *testing.T) {
		probe := &requirementProbe{req: &capability}
		resp := sendOverPartitionTransport(requirementServer(t, probe, policy),
			requirementRequest([]string{"org.read"}))
		if resp.Success || atomic.LoadInt32(&probe.calls) != 0 {
			t.Fatalf("ungranted principal admitted: success=%v calls=%d", resp.Success, probe.calls)
		}
		if !strings.Contains(resp.Error, "denied by authorization policy") {
			t.Fatalf("error = %q, want the authorization arm's denial", resp.Error)
		}
	})

	// The scope tag alone would admit this principal (ScopeAllowlisted under
	// typedPrincipalScopePolicy); the declared Requirement must still fail closed
	// when no authorization policy is configured.
	t.Run("with no authorization policy the handler is denied, not judged by its tag", func(t *testing.T) {
		probe := &requirementProbe{req: &capability}
		resp := sendOverPartitionTransport(requirementServer(t, probe, nil),
			requirementRequest([]string{testInferenceCap}))
		if resp.Success || atomic.LoadInt32(&probe.calls) != 0 {
			t.Fatalf("admitted with no authorization policy: success=%v calls=%d", resp.Success, probe.calls)
		}
		if !strings.Contains(resp.Error, "no authorization policy is configured") {
			t.Fatalf("error = %q, want the missing-policy denial", resp.Error)
		}
	})
}

func TestMeshRequirementNexusArm(t *testing.T) {
	nexus := rpcscope.Nexus()
	policy := rpcscope.NewPolicy(nil)

	t.Run("an identity with no partition is denied", func(t *testing.T) {
		probe := &requirementProbe{req: &nexus}
		resp := sendOverPartitionTransport(requirementServer(t, probe, policy),
			requirementRequest(nil, &pb.ScopeAxis{Name: rpcscope.AxisTransportPinned, Value: "orbtr"}))
		if resp.Success || atomic.LoadInt32(&probe.calls) != 0 {
			t.Fatalf("partitionless identity admitted: success=%v calls=%d", resp.Success, probe.calls)
		}
	})

	t.Run("an identity with a partition is admitted", func(t *testing.T) {
		probe := &requirementProbe{req: &nexus}
		resp := sendOverPartitionTransport(requirementServer(t, probe, policy), requirementRequest(nil))
		if !resp.Success || atomic.LoadInt32(&probe.calls) != 1 {
			t.Fatalf("partitioned identity: success=%v calls=%d err=%q", resp.Success, probe.calls, resp.Error)
		}
	})
}

// A handler that declares no Requirement is judged by its scope tag exactly as
// before, whether or not an authorization policy is set.
func TestMeshRequirementAbsentKeepsThePresencePath(t *testing.T) {
	probe := &requirementProbe{}
	resp := sendOverPartitionTransport(
		requirementServer(t, probe, rpcscope.NewPolicy(grantAuthorizer{})),
		requirementRequest(nil))
	if !resp.Success || atomic.LoadInt32(&probe.calls) != 1 {
		t.Fatalf("no-Requirement handler: success=%v calls=%d err=%q", resp.Success, probe.calls, resp.Error)
	}
}
