/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"reflect"
	"testing"
	"time"

	swarmpb "github.com/bbmumford/swarm/proto/pb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/bbmumford/loom/pkg/rpc"
)

// publisherTestRegistry holds one handler this node executes, one Register
// call that supplied no Func, and one RegisterProxy entry, the three shapes a
// gateway's registry mixes. Returns the registry and the executing FQN, the
// only one the record may carry.
func publisherTestRegistry(t *testing.T) (*rpc.Registry, string, []string) {
	t.Helper()
	echo := func(ctx context.Context, req proto.Message) (proto.Message, error) { return req, nil }
	executing := rpc.Handler{
		Namespace: "test", Domain: "publish", Operation: "Runs",
		Func:     echo,
		Request:  (*wrapperspb.StringValue)(nil),
		Response: (*wrapperspb.StringValue)(nil),
		Scope:    rpc.ScopeNone,
	}
	declared := rpc.Handler{
		Namespace: "test", Domain: "publish", Operation: "Declared",
		Request:  (*wrapperspb.StringValue)(nil),
		Response: (*wrapperspb.StringValue)(nil),
		Scope:    rpc.ScopeNone,
	}
	// RegisterProxy strips Func, so the entry reaches the registry with none
	// although the Types()-shaped input carries one.
	proxied := rpc.Handler{
		Namespace: "test", Domain: "elsewhere", Operation: "Runs",
		Func:     echo,
		Request:  (*wrapperspb.StringValue)(nil),
		Response: (*wrapperspb.StringValue)(nil),
		Scope:    rpc.ScopeNone,
	}
	reg := rpc.NewRegistry()
	for _, h := range []rpc.Handler{executing, declared} {
		if err := reg.Register(h); err != nil {
			t.Fatalf("Register(%s): %v", h.FQN(), err)
		}
	}
	if err := reg.RegisterProxy([]rpc.Handler{proxied}); err != nil {
		t.Fatalf("RegisterProxy: %v", err)
	}
	return reg, executing.FQN(), []string{declared.FQN(), proxied.FQN()}
}

// publishOnce puts on the record only the FQNs whose Func this registry holds.
// The executing FQN stays, or a filter that dropped every handler would pass
// the absent legs alone.
func TestThePeerRecordAdvertisesOnlyHandlersThisNodeExecutes(t *testing.T) {
	reg, executing, notHere := publisherTestRegistry(t)
	node := &capturingPublishNode{stubSwarmNode: &stubSwarmNode{}}
	NewPeerPublisher(node, nil, reg).publishOnce()

	var rec swarmpb.PeerRecord
	if err := proto.Unmarshal(node.body, &rec); err != nil {
		t.Fatalf("published bytes do not decode as a PeerRecord: %v", err)
	}
	if got, want := rec.GetRpcHandlers(), []string{executing}; !reflect.DeepEqual(got, want) {
		t.Fatalf("published rpc_handlers = %v, want %v: %v execute on another node, "+
			"so a record naming them sends their callers here for a forwarding hop", got, want, notHere)
	}
}

// RoleTable.Lookup(role, fqn) is how the forwarder and findNodeForRole pick a
// node for an FQN, and it reads the handler list this node publishes. Through
// the whole pipeline (PublishRPCHandlersToLAD, the signed fleet.peer record,
// the RoleTable projection) a Func-less FQN names no node here, and the
// executing FQN names this one.
func TestAFuncLessFQNIsNotRoutedToThisNode(t *testing.T) {
	reg, executing, notHere := publisherTestRegistry(t)
	rt := newPeerPublicationTestRuntime(t)
	if err := rt.PublishRPCHandlersToLAD(context.Background(), reg); err != nil {
		t.Fatalf("PublishRPCHandlersToLAD: %v", err)
	}

	roleOf := func(fqn string) string {
		h, ok := reg.Get(fqn)
		if !ok {
			t.Fatalf("Get(%s): not registered", fqn)
		}
		return h.Namespace + "." + h.Domain
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && len(rt.Swarm().RoleTable.Lookup(roleOf(executing), executing)) == 0 {
		time.Sleep(time.Millisecond)
	}
	if got := rt.Swarm().RoleTable.Lookup(roleOf(executing), executing); len(got) != 1 {
		t.Fatalf("Lookup(%s) returned %d records, want this node", executing, len(got))
	}
	for _, fqn := range notHere {
		if got := rt.Swarm().RoleTable.Lookup(roleOf(fqn), fqn); len(got) != 0 {
			t.Errorf("Lookup(%s) names %s, which holds no Func for it", fqn, got[0].NodeID)
		}
	}
}
