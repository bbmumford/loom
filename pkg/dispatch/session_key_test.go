/*
 * Copyright (c) 2026 ORBTR Pty Ltd. All Rights Reserved.
 */
package dispatch

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/rpc/pb"
)

// servingSession is a peer serving a fixed set of handlers. Every request it
// receives is recorded by handler, and a handler it does not serve is
// answered as the receive path answers one: unsuccessful, never executed.
type servingSession struct {
	rpcMockSession
	node   aether.NodeID
	serves map[string]bool

	mu       sync.Mutex
	received []string
}

func (s *servingSession) RemoteNodeID() aether.NodeID { return s.node }

func (s *servingSession) OpenStream(ctx context.Context, cfg aether.StreamConfig) (aether.Stream, error) {
	return &servingStream{rpcMockStream: rpcMockStream{id: cfg.StreamID}, peer: s}, nil
}

func (s *servingSession) got(handler string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, h := range s.received {
		if h == handler {
			return true
		}
	}
	return false
}

type servingStream struct {
	rpcMockStream
	peer *servingSession
}

func (st *servingStream) Send(ctx context.Context, data []byte) error {
	req, err := pb.UnmarshalRequest(data)
	if err != nil {
		return err
	}
	st.peer.mu.Lock()
	st.peer.received = append(st.peer.received, req.Handler)
	st.peer.mu.Unlock()
	resp := &pb.RPCResponse{Id: req.Id, Success: true}
	if !st.peer.serves[req.Handler] {
		resp = &pb.RPCResponse{Id: req.Id, Error: fmt.Sprintf("unknown handler: %s", req.Handler)}
	}
	st.respData, _ = pb.MarshalResponse(resp)
	return nil
}

// handlerFinder answers FindSession with the peer serving the handler, as
// findNodeForRole does through RoleTable.Lookup(role, handler), and offers no
// route, so every call takes callOnce and its session cache.
type handlerFinder struct {
	peers []*servingSession
}

func (f *handlerFinder) FindSession(ctx context.Context, role, handler string) (aether.Session, error) {
	for _, p := range f.peers {
		if p.serves[handler] {
			return p, nil
		}
	}
	return nil, fmt.Errorf("no peer serves %s", handler)
}
func (f *handlerFinder) FindAnySession() (aether.Session, bool) { return nil, false }
func (f *handlerFinder) CallViaBidi(ctx context.Context, nodeID string, req *pb.RPCRequest) (*pb.RPCResponse, bool, error) {
	return nil, false, nil
}
func (f *handlerFinder) FindRoutes(ctx context.Context, role, handler string) []aether.ProbeRoute {
	return nil
}
func (f *handlerFinder) IsSupersededByUpgrade(aether.Session) bool { return false }

// TestAProvisioningCreateNeverLandsOnAPeerLackingIt: two peers share the role
// orbtr.provisioning and serve different handlers of it. After a call that
// finds and caches the peer serving GetTenant, a create takes the same
// no-route path and is sent only to the peer serving the create.
func TestAProvisioningCreateNeverLandsOnAPeerLackingIt(t *testing.T) {
	const (
		role      = "orbtr.provisioning"
		getTenant = "orbtr.provisioning.GetTenant"
		create    = "orbtr.provisioning.CreatePartitionDatabase"
	)
	io := &servingSession{node: "node-io", serves: map[string]bool{getTenant: true}}
	ai := &servingSession{node: "node-ai", serves: map[string]bool{create: true}}
	caller := NewHWPCaller(&handlerFinder{peers: []*servingSession{io, ai}})
	defer caller.Close()

	if _, err := caller.Call(context.Background(), role, getTenant, nil); err != nil {
		t.Fatalf("GetTenant: %v", err)
	}
	if !io.got(getTenant) {
		t.Fatal("GetTenant did not reach the peer serving it; the fixture routes nothing")
	}
	if _, err := caller.Call(context.Background(), role, create, nil); err != nil {
		t.Fatalf("CreatePartitionDatabase: %v", err)
	}
	if io.got(create) {
		t.Error("the create was sent to the peer serving GetTenant, which lacks it, over the session cached for the role")
	}
	if !ai.got(create) {
		t.Error("the create did not reach the peer serving it")
	}
}
