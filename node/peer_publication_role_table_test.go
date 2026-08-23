/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"reflect"
	"testing"
	"time"

	"github.com/ORBTR/aether"
	"github.com/bbmumford/loom/pkg/rpc"
)

// TestPublishRPCHandlersToLAD_RoleTableGolden pins the whole role-discovery
// pipeline, not a local replica of either endpoint:
//
//	rpc.Registry -> Runtime.PublishRPCHandlersToLAD -> signed fleet.peer record
//	-> RoleTable projection -> handler-filtered lookup
//
// The exact lists are deliberately registered out of order. This guards both
// identity shape (handler FQNs, not role names) and deterministic publication.
func TestPublishRPCHandlersToLAD_RoleTableGolden(t *testing.T) {
	reg := rpc.NewRegistry()
	for _, handler := range []rpc.Handler{
		{Namespace: "orbtr", Domain: "device", Operation: "List", Scope: rpc.ScopeNone},
		{Namespace: "orbtr", Domain: "audit", Operation: "Write", Scope: rpc.ScopeNone},
		{Namespace: "orbtr", Domain: "device", Operation: "Get", Scope: rpc.ScopeNone},
	} {
		if err := reg.Register(handler); err != nil {
			t.Fatalf("Register(%s): %v", handler.FQN(), err)
		}
	}

	rt := newPeerPublicationTestRuntime(t)
	if err := rt.PublishRPCHandlersToLAD(context.Background(), reg); err != nil {
		t.Fatalf("PublishRPCHandlersToLAD: %v", err)
	}

	wantRoles := []string{"orbtr.audit", "orbtr.device"}
	wantHandlers := []string{
		"orbtr.audit.Write",
		"orbtr.device.Get",
		"orbtr.device.List",
	}

	var gotRoles, gotHandlers []string
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		rec, ok := rt.Swarm().RoleTable.All()[string(rt.identity.NodeID)]
		if ok {
			gotRoles = append([]string(nil), rec.Roles...)
			gotHandlers = handlerFQNs(rec)
			if reflect.DeepEqual(gotRoles, wantRoles) &&
				reflect.DeepEqual(gotHandlers, wantHandlers) {
				break
			}
		}
		time.Sleep(time.Millisecond)
	}
	if !reflect.DeepEqual(gotRoles, wantRoles) {
		t.Fatalf("published roles = %v, want %v", gotRoles, wantRoles)
	}
	if !reflect.DeepEqual(gotHandlers, wantHandlers) {
		t.Fatalf("published handlers = %v, want exact FQNs %v", gotHandlers, wantHandlers)
	}

	if got := rt.Swarm().RoleTable.Lookup("orbtr.device", "orbtr.device.Get"); len(got) != 1 {
		t.Fatalf("handler-filtered lookup returned %d records, want 1", len(got))
	}
	if got := rt.Swarm().RoleTable.Lookup("orbtr.device", "orbtr.device"); len(got) != 0 {
		t.Fatalf("role name masqueraded as handler identity: %v", handlerFQNs(got[0]))
	}
}

func newPeerPublicationTestRuntime(t *testing.T) *Runtime {
	t.Helper()

	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	nodeID, err := aether.NewNodeID(publicKey)
	if err != nil {
		t.Fatalf("NewNodeID: %v", err)
	}
	runtimeCtx, cancel := context.WithCancel(context.Background())
	rt := &Runtime{
		cfg: Config{
			ServiceName: "publication-golden",
		},
		identity: &NodeIdentity{
			NodeID:     nodeID,
			PublicKey:  publicKey,
			PrivateKey: privateKey,
		},
		ctx:    runtimeCtx,
		cancel: cancel,
	}

	t.Cleanup(func() {
		if err := rt.Shutdown(); err != nil {
			t.Errorf("Shutdown: %v", err)
		}
		if rt.swarm == nil {
			return
		}
		rt.swarm.RoleTable.Close()
		rt.swarm.AddressTable.Close()
		rt.swarm.Transport.Stop()
		_ = rt.swarm.Node.Stop()
	})
	return rt
}
