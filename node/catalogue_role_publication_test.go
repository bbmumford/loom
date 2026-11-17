/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"testing"
	"time"

	"github.com/bbmumford/loom/pkg/rpc"
	"google.golang.org/protobuf/proto"
)

// A domain an endpoint only calls, listed in its registry's Catalogue, is on the
// role list its peer record publishes, so the role table's role-only lookup and
// the role takeover's coverage count (LookupRoleViaSwarm with no handler) count
// this node, while no handler of it is on the record: a lookup naming the
// operation finds nothing here, and the node is never chosen to answer it.
func TestACatalogueOnlyDomainIsAPublishedRoleWithNoHandler(t *testing.T) {
	run := func(ctx context.Context, req proto.Message) (proto.Message, error) { return req, nil }
	reg := rpc.NewRegistry()
	if err := reg.Register(rpc.Handler{Namespace: "test", Domain: "here", Operation: "Runs", Scope: rpc.ScopeNone, Func: run}); err != nil {
		t.Fatal(err)
	}
	if err := reg.Catalogue().Declare([]rpc.Handler{{Namespace: "test", Domain: "elsewhere", Operation: "Runs", Scope: rpc.ScopeNone}}); err != nil {
		t.Fatalf("Declare: %v", err)
	}
	rt := newPeerPublicationTestRuntime(t)
	if err := rt.PublishRPCHandlersToLAD(context.Background(), reg); err != nil {
		t.Fatalf("PublishRPCHandlersToLAD: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && len(rt.Swarm().RoleTable.Lookup("test.here", "")) == 0 {
		time.Sleep(time.Millisecond)
	}
	if got := len(rt.Swarm().RoleTable.Lookup("test.here", "")); got != 1 {
		t.Fatalf("the served role: %d records, want 1 (the record was not published)", got)
	}
	if got := len(rt.Swarm().RoleTable.Lookup("test.elsewhere", "")); got != 1 {
		t.Errorf("role-only lookup of the listed role: %d records, want 1", got)
	}
	if got := len(rt.Swarm().RoleTable.Lookup("test.elsewhere", "test.elsewhere.Runs")); got != 0 {
		t.Errorf("lookup naming the listed operation: %d records, want 0", got)
	}
	if got := len(rt.LookupRoleViaSwarm("test.elsewhere", "")); got != 1 {
		t.Errorf("the takeover engine's coverage count of the listed role: %d, want 1", got)
	}
}
