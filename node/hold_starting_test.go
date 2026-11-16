/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/ORBTR/aether/rpc/pb"
	"github.com/bbmumford/loom/pkg/rpc"
	rpcscope "github.com/bbmumford/loom/pkg/rpc/scope"
)

// A held node answers every inbound request, one it registers, one it does not
// and one naming another node as its target, with the stamped starting refusal,
// and runs nothing; once marked ready the same request runs.
func TestAHeldNodeAnswersEveryRequestWithTheStartingRefusal(t *testing.T) {
	server, fqn, runs := localRunServer(t)
	server.registry.HoldUntilReady()
	relay := classRequest("s-3", fqn, "standard")
	relay.TargetNodeId = "another-node"
	for _, req := range []*pb.RPCRequest{
		classRequest("s-1", fqn, "standard"),
		classRequest("s-2", "test.floor.Unregistered", "standard"),
		relay,
	} {
		resp := server.handleRequest(context.Background(), req)
		if resp.Success {
			t.Errorf("%s (%s) succeeded on a held node", req.Id, req.Handler)
			continue
		}
		if got := resp.Metadata["refusal"]; got != "starting" {
			t.Errorf("%s (%s) is answered with refusal kind %q, want \"starting\": %s", req.Id, req.Handler, got, resp.Error)
		}
	}
	if n := runs.Load(); n != 0 {
		t.Fatalf("the handler ran %d time(s) on a held node, want none", n)
	}
	server.registry.MarkReady()
	if resp := server.handleRequest(context.Background(), classRequest("s-4", fqn, "standard")); !resp.Success || runs.Load() != 1 {
		t.Fatalf("once ready the request answered %q with %d run(s), want a success and 1", resp.Error, runs.Load())
	}
}

// A held node claims no in-process call, so rpc.Call goes to the mesh, and its
// dispatcher refuses one that reaches it anyway with the starting refusal.
func TestAHeldNodeLeavesItsInProcessCallsToTheMesh(t *testing.T) {
	server, fqn, runs := localRunServer(t)
	local := &localDispatcherAdapter{registry: server.registry}
	server.registry.HoldUntilReady()
	if local.HasHandler(fqn) {
		t.Fatal("a held node claims an in-process call")
	}
	if _, err := local.DispatchBytes(context.Background(), fqn, nil); !errors.Is(err, rpcscope.ErrStarting) {
		t.Fatalf("a held node's dispatcher answered %v, want the starting refusal", err)
	}
	if runs.Load() != 0 {
		t.Fatal("a held node ran an in-process call")
	}
	server.registry.MarkReady()
	if !local.HasHandler(fqn) {
		t.Fatal("a ready node does not claim the in-process call to a handler it hosts")
	}
}

// loom's own mesh handlers (RegisterInfrastructure: ping, status, hole punching)
// are dispatched on every node and listed on no record.
func TestTheRecordLeavesOutLoomsOwnMeshHandlers(t *testing.T) {
	rt, node := publicationRuntime(t)
	reg := rpc.NewRegistry()
	served(t, rt, reg, "alpha")
	if err := rt.Registry().RegisterInfrastructure(meshOnly{name: "test-mesh-builtin", role: "system"}); err != nil {
		t.Fatalf("RegisterInfrastructure: %v", err)
	}
	if err := rt.PublishRPCHandlersToLAD(context.Background(), reg); err != nil {
		t.Fatalf("PublishRPCHandlersToLAD: %v", err)
	}
	rec := node.last(t)
	if got, want := rec.GetRpcHandlers(), []string{"test.alpha.Op"}; !reflect.DeepEqual(got, want) {
		t.Errorf("the record lists %v, want %v", got, want)
	}
	if got, want := rec.GetCapabilities().GetRoles(), []string{"test.alpha"}; !reflect.DeepEqual(got, want) {
		t.Errorf("the record carries roles %v, want %v", got, want)
	}
}
