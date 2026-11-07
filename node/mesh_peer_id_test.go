/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/adapter"
)

// TestDialedPeerID_OnlyADialThatProvesTheKeySharesThePeersRoom: a mesh session a
// dial builds joins the dialled node's per-peer ledgers only over a protocol whose
// dial proves the remote holds that node's key; the TLS dial proves it through the
// seed's join proof and provedTLSDial, and the gRPC dial builds under the ID as a
// stated label.
func TestDialedPeerID_OnlyADialThatProvesTheKeySharesThePeersRoom(t *testing.T) {
	want := map[Protocol]meshPeerID{
		ProtoNoiseUDP:  provedPeerID,
		ProtoQUIC:      provedPeerID,
		ProtoWebSocket: provedPeerID,
		ProtoTLS:       provedPeerID,
		ProtoGRPC:      statedPeerID,
	}
	for _, proto := range protocolOrder {
		w, ok := want[proto]
		if !ok {
			t.Fatalf("protocol %s has no stated expectation", proto)
		}
		if got := dialedPeerID(proto); got != w {
			t.Fatalf("dialedPeerID(%s) = %v, want %v", proto, got, w)
		}
	}
}

// TestMeshSessionFactory_AStatedIDBuildsThroughTheStatedFactory: setupMeshSession
// builds a proved ID's session through the factory that shares the node's per-peer
// ledgers and a stated ID's through the one that gives it ledgers of its own.
func TestMeshSessionFactory_AStatedIDBuildsThroughTheStatedFactory(t *testing.T) {
	ptr := func(f any) uintptr { return reflect.ValueOf(f).Pointer() }
	if ptr(meshSessionFactory(provedPeerID)) != ptr(adapter.NewSessionForProtocol) {
		t.Fatal("a proved ID's session is not built through NewSessionForProtocol")
	}
	if ptr(meshSessionFactory(statedPeerID)) != ptr(adapter.NewSessionForStatedPeer) {
		t.Fatal("a stated ID's session is not built through NewSessionForStatedPeer")
	}
}

// TestProvedTLSDial_ASeedProvingAnotherNodeIsRefused: a TLS dial whose seed proved a
// node other than the one dialled returns no connection and closes the joined conn,
// so DialAndAcceptMesh never files that seed's session under the dialled node's ID;
// a seed proving the dialled node (in any letter case) returns a connection to it.
func TestProvedTLSDial_ASeedProvingAnotherNodeIsRefused(t *testing.T) {
	rt := &Runtime{identity: &NodeIdentity{NodeID: "vl1_local"}}

	ours, theirs := net.Pipe()
	defer theirs.Close()
	conn, err := rt.provedTLSDial("vl1_dialled", "seed.example", &bootstrapDialResult{conn: ours, nodeID: "vl1_other"})
	if err == nil || conn != nil {
		t.Fatalf("a seed proving another node gave conn %v, err %v; want a refusal", conn, err)
	}
	_ = theirs.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, rerr := theirs.Read(make([]byte, 1)); rerr == nil {
		t.Fatal("the refused dial's conn was left open")
	}

	ours2, theirs2 := net.Pipe()
	defer theirs2.Close()
	defer ours2.Close()
	conn, err = rt.provedTLSDial("vl1_dialled", "seed.example", &bootstrapDialResult{conn: ours2, nodeID: "VL1_Dialled"})
	if err != nil {
		t.Fatalf("a seed proving the dialled node was refused: %v", err)
	}
	if got := aether.NormalizeNodeID(string(conn.RemoteNodeID())); got != "vl1_dialled" {
		t.Fatalf("the connection names node %q, want the dialled vl1_dialled", got)
	}
}
