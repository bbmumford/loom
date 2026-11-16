/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"

	"github.com/bbmumford/loom/node/handlers"
	"github.com/bbmumford/loom/pkg/rpc"
	"github.com/bbmumford/swarm"
	swarmpb "github.com/bbmumford/swarm/proto/pb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// recordingPublishNode keeps every record handed to Publish, or answers every
// Publish with err while err is set.
type recordingPublishNode struct {
	*stubSwarmNode
	mu      sync.Mutex
	err     error
	records []*swarmpb.PeerRecord
}

func (n *recordingPublishNode) Publish(_ swarm.Topic, body []byte) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.err != nil {
		return n.err
	}
	rec := &swarmpb.PeerRecord{}
	if err := proto.Unmarshal(body, rec); err != nil {
		return err
	}
	n.records = append(n.records, rec)
	return nil
}

func (n *recordingPublishNode) fail(err error) {
	n.mu.Lock()
	n.err = err
	n.mu.Unlock()
}

func (n *recordingPublishNode) count() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return len(n.records)
}

func (n *recordingPublishNode) last(t *testing.T) *swarmpb.PeerRecord {
	t.Helper()
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.records) == 0 {
		t.Fatal("no record was published")
	}
	return n.records[len(n.records)-1]
}

// publicationRuntime is a runtime whose swarm integration publishes through a
// recording node, with a publisher whose run loop never starts, so every record
// the test reads is one a call published itself.
func publicationRuntime(t *testing.T) (*Runtime, *recordingPublishNode) {
	t.Helper()
	rt := newPeerPublicationTestRuntime(t)
	si, err := rt.InitSwarm(nil)
	if err != nil {
		t.Fatalf("InitSwarm: %v", err)
	}
	node := &recordingPublishNode{stubSwarmNode: &stubSwarmNode{}}
	si.Publisher = NewPeerPublisher(node, rt, nil)
	return rt, node
}

// served registers each FQN as namespace.domain.Op with a Func on reg and exports
// reg into rt's handler registry.
func served(t *testing.T, rt *Runtime, reg *rpc.Registry, domains ...string) {
	t.Helper()
	for _, domain := range domains {
		if err := reg.Register(rpc.Handler{
			Namespace: "test", Domain: domain, Operation: "Op",
			Func: func(_ context.Context, req proto.Message) (proto.Message, error) {
				return req, nil
			},
			Request:  (*wrapperspb.StringValue)(nil),
			Response: (*wrapperspb.StringValue)(nil),
			Scope:    rpc.ScopeNone,
		}); err != nil {
			t.Fatalf("Register(%s): %v", domain, err)
		}
	}
	if err := reg.ExportToHandlerRegistry(rt.Registry()); err != nil {
		t.Fatalf("ExportToHandlerRegistry: %v", err)
	}
}

// The publication is published by the call itself, with no run loop, and the
// call returns the publication's error.
func TestThePublicationIsSynchronousAndReturnsItsError(t *testing.T) {
	rt, node := publicationRuntime(t)
	reg := rpc.NewRegistry()
	served(t, rt, reg, "alpha")
	if err := rt.PublishRPCHandlersToLAD(context.Background(), reg); err != nil {
		t.Fatalf("PublishRPCHandlersToLAD: %v", err)
	}
	rec := node.last(t)
	if got, want := rec.GetRpcHandlers(), []string{"test.alpha.Op"}; !reflect.DeepEqual(got, want) {
		t.Errorf("the record lists %v, want %v", got, want)
	}
	if got := rec.GetCapabilities().GetRoles(); !reflect.DeepEqual(got, []string{"test.alpha"}) {
		t.Errorf("the record carries roles %v beside its handlers, want [test.alpha]", got)
	}
	refused := errors.New("the swarm refused the record")
	node.fail(refused)
	if err := rt.PublishRPCHandlersToLAD(context.Background(), reg); !errors.Is(err, refused) {
		t.Fatalf("a refused publication returned %v, want the swarm's refusal", err)
	}
}

// A record lists the committed catalogue and nothing registered after it: a
// later tick republishes the commit, a failed commit leaves the previous one in
// force, and a successful one replaces it.
func TestARecordListsOnlyTheCommittedCatalogue(t *testing.T) {
	rt, node := publicationRuntime(t)
	reg := rpc.NewRegistry()
	served(t, rt, reg, "alpha")
	if err := rt.PublishRPCHandlersToLAD(context.Background(), reg); err != nil {
		t.Fatalf("PublishRPCHandlersToLAD: %v", err)
	}
	served(t, rt, reg, "beta")
	rt.Swarm().Publisher.publishOnce()
	if got := node.last(t).GetRpcHandlers(); !reflect.DeepEqual(got, []string{"test.alpha.Op"}) {
		t.Fatalf("a tick after a later registration lists %v, want only the committed [test.alpha.Op]", got)
	}

	node.fail(errors.New("the swarm refused the record"))
	if err := rt.PublishRPCHandlersToLAD(context.Background(), reg); err == nil {
		t.Fatal("a refused publication returned no error")
	}
	node.fail(nil)
	rt.Swarm().Publisher.publishOnce()
	if got := node.last(t).GetRpcHandlers(); !reflect.DeepEqual(got, []string{"test.alpha.Op"}) {
		t.Fatalf("after a refused commit the record lists %v, want the previous [test.alpha.Op]", got)
	}

	if err := rt.PublishRPCHandlersToLAD(context.Background(), reg); err != nil {
		t.Fatalf("PublishRPCHandlersToLAD: %v", err)
	}
	if got, want := node.last(t).GetRpcHandlers(), []string{"test.alpha.Op", "test.beta.Op"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("after a second commit the record lists %v, want %v", got, want)
	}
}

// meshOnly is a handler an endpoint registers on the node's handler registry
// directly, not through an rpc.Registry export.
type meshOnly struct{ name, role string }

func (h meshOnly) Name() string                { return h.name }
func (h meshOnly) Role() string                { return h.role }
func (h meshOnly) RequiresAuth() bool          { return false }
func (h meshOnly) AllowedAuthTypes() []string  { return nil }
func (h meshOnly) Scopes() []string            { return nil }
func (h meshOnly) Scope() handlers.Scope       { return handlers.ScopeNone }
func (h meshOnly) AllowedPartitions() []string { return nil }
func (h meshOnly) ExecuteRPC(context.Context, *handlers.RPCRequest) (*handlers.RPCResponse, error) {
	return &handlers.RPCResponse{Success: true}, nil
}

// The record lists what the node's handler registry dispatches, however it got
// there, and nothing else: an exported handler and one registered directly are
// listed, a proxy is not. Its roles are the registry's beside the catalogue's.
func TestTheRecordListsWhatTheNodeDispatches(t *testing.T) {
	rt, node := publicationRuntime(t)
	reg := rpc.NewRegistry()
	if err := reg.RegisterProxy([]rpc.Handler{{
		Namespace: "test", Domain: "proxied", Operation: "Op",
		Request: (*wrapperspb.StringValue)(nil), Response: (*wrapperspb.StringValue)(nil), Scope: rpc.ScopeNone,
	}}); err != nil {
		t.Fatalf("RegisterProxy: %v", err)
	}
	served(t, rt, reg, "alpha")
	if err := rt.Registry().RegisterRPC(meshOnly{name: "test.native.Op", role: "test.native"}); err != nil {
		t.Fatalf("RegisterRPC: %v", err)
	}
	if err := rt.PublishRPCHandlersToLAD(context.Background(), reg); err != nil {
		t.Fatalf("PublishRPCHandlersToLAD: %v", err)
	}
	rec := node.last(t)
	if got, want := rec.GetRpcHandlers(), []string{"test.alpha.Op", "test.native.Op"}; !reflect.DeepEqual(got, want) {
		t.Errorf("the record lists %v, want %v", got, want)
	}
	if got, want := rec.GetCapabilities().GetRoles(), []string{"test.alpha", "test.native", "test.proxied"}; !reflect.DeepEqual(got, want) {
		t.Errorf("the record carries roles %v, want %v", got, want)
	}
}

// A role set the endpoint declared (SetRoles) is the one a later publication
// advertises, so a role the endpoint withheld is not re-advertised.
func TestADeclaredRoleSetIsTheOneThePublicationAdvertises(t *testing.T) {
	rt, node := publicationRuntime(t)
	reg := rpc.NewRegistry()
	served(t, rt, reg, "alpha", "withheld")
	rt.SetRoles([]string{"test.alpha", "test.topology"})
	before := node.count()
	if err := rt.PublishRPCHandlersToLAD(context.Background(), reg); err != nil {
		t.Fatalf("PublishRPCHandlersToLAD: %v", err)
	}
	if node.count() == before {
		t.Fatal("the publication published no record")
	}
	if got, want := node.last(t).GetCapabilities().GetRoles(), []string{"test.alpha", "test.topology"}; !reflect.DeepEqual(got, want) {
		t.Errorf("the record carries roles %v, want the declared %v", got, want)
	}
}
