/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"encoding/json"
	"net"
	"path/filepath"
	"testing"
	"time"

	"github.com/ORBTR/aether"
	lad "github.com/bbmumford/ledger"
)

// This test runs the gossip loops themselves on the runtime's delta tracker:
// two whole runtimes, a session between them, and a restart of one over its
// data directory. delta_persist_test.go covers the tracker and its persistence
// without running an exchange.

// startDeltaMeshRuntime starts a whole runtime with durable storage under dir.
// Its VL1 transport has no UDP port, so the runtime builds a connection manager
// and binds no listener, and its bootstrap host answers nothing: it holds no
// session until a test joins it to another runtime with joinDeltaPeers. A
// directory that already holds a node key gives the runtime that identity
// again, which is how a test restarts a generation.
func startDeltaMeshRuntime(t *testing.T, dir string) *Runtime {
	t.Helper()
	rt, err := Initialize(Config{
		ServiceName: "delta-exchange-test",
		DataDir:     dir,
		NetworkKeys: []string{"delta-exchange-test-network-key"},
		VL1:         VL1TransportConfig{Enabled: true},
		LAD:         LADDirectoryConfig{Storage: "local", LocalDir: filepath.Join(dir, "lad"), BootstrapHosts: "127.0.0.1:9"},
	})
	if err != nil {
		t.Fatalf("Initialize over %s: %v", dir, err)
	}
	t.Cleanup(func() { _ = rt.Shutdown() })
	return rt
}

// newDeltaMeshRuntime starts a runtime over a new data directory, which gives
// it a new identity, and returns the runtime with that directory. It starts
// again over another directory while the frame codec cannot carry the identity
// it drew: a full frame header begins with the sender's PeerID, and
// aether.ReadNextFrame reads a first byte that aether.IsShortHeader accepts as
// a short header, so a session with such a runtime ends on the first full
// header it sends and carries no exchange.
//
// TODO: start once, when a full header can no longer be read as a short one.
func newDeltaMeshRuntime(t *testing.T) (*Runtime, string) {
	t.Helper()
	for range 64 {
		dir := t.TempDir()
		rt := startDeltaMeshRuntime(t, dir)
		if peerID := rt.identity.NodeID.ToPeerID(); !aether.IsShortHeader(peerID[0]) {
			return rt, dir
		}
		_ = rt.Shutdown()
	}
	t.Fatal("no new identity had a PeerID the frame codec carries in a full header")
	return nil, ""
}

// joinDeltaPeers opens a loopback TCP connection between two runtimes and runs
// the mesh session lifecycle on each end of it, as the connection manager does
// after a dial and the accept loop does for an incoming connection.
// AcceptMeshConnection then starts the gossip initiator loop on the dialling
// runtime and the responder loop on the accepting one. ProtoWebSocket selects
// the session adapter for a reliable byte stream, which the connection is.
//
// Each end runs on a goroutine its runtime enrols with Runtime.Go, so that
// runtime's Shutdown waits for it. AcceptMeshConnection's cleanup appends a
// peer-state event to the LAD ledger, and Shutdown closes that ledger after
// its wait: on a goroutine the wait does not cover, the append can reach a
// closed ledger, whose broadcast then panics.
func joinDeltaPeers(t *testing.T, dialler, accepter *Runtime) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	near, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	far, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	accepter.Go("test.accept_mesh_session", func() {
		accepter.AcceptIncomingMeshSession(accepter.ctx, far, string(dialler.identity.NodeID), ProtoWebSocket, "", "", "", "")
	})
	dialler.Go("test.dial_mesh_session", func() {
		dialler.DialAndAcceptMesh(dialler.ctx, near, string(accepter.identity.NodeID), "", ProtoWebSocket, "", "", "", dialBorrowsConnectingState)
	})
}

// exchangeOverASession joins two runtimes and returns once the dialling
// runtime's gossip initiator loop has completed its first exchange with the
// accepting one: the instant AcceptMeshConnection's gossip callback records in
// the connection table. The accepting runtime has by then applied every record
// that exchange carried, because the responder applies a request before it
// answers, so a caller reads both caches without waiting.
func exchangeOverASession(t *testing.T, dialler, accepter *Runtime) {
	t.Helper()
	accepterID := string(accepter.identity.NodeID)
	joinDeltaPeers(t, dialler, accepter)
	waitMeshDB(t, 10*time.Second, "the gossip initiator loop to complete an exchange with "+truncID(accepterID), func() bool {
		dialler.connMgr.mu.Lock()
		defer dialler.connMgr.mu.Unlock()
		p := dialler.connMgr.peers[accepterID]
		return p != nil && !p.lastExchange.IsZero()
	})
}

// holdsOpenMeshSession reports whether rt's dispatch table lists a session for
// a peer that has not closed. registerMeshSession keeps such a session and
// turns away a second one of the same grade from the same dialler.
func holdsOpenMeshSession(rt *Runtime, peerID string) bool {
	rt.connMgr.dispatchMu.RLock()
	defer rt.connMgr.dispatchMu.RUnlock()
	session := rt.connMgr.meshSessions[peerID]
	return session != nil && !session.IsClosed()
}

// holdMember puts a member record created at the given instant into rt's
// directory cache, signed with rt's key so the cache's signed-topic check
// admits it. The gossip exchange reads the cache through Dump and DumpSince:
// a full snapshot carries every member, and a delta carries only the members
// created after the watermark it is cut from.
func holdMember(t *testing.T, rt *Runtime, nodeID string, created time.Time) {
	t.Helper()
	body, err := json.Marshal(lad.MemberRecord{NodeID: nodeID, CreatedAt: created})
	if err != nil {
		t.Fatal(err)
	}
	rec := lad.Record{Topic: lad.TopicMember, NodeID: nodeID, Body: body, Timestamp: created}
	lad.SignRecord(&rec, rt.identity.PrivateKey)
	if err := rt.cache.Apply(rec); err != nil {
		t.Fatalf("apply member %s: %v", nodeID, err)
	}
}

func holdsMember(rt *Runtime, nodeID string) bool {
	return rt.cache.HasRecord(lad.TopicMember, ":"+nodeID)
}

// The gossip loops AcceptMeshConnection starts read and advance the runtime's
// own tracker: the initiator loop on the dialling runtime and the responder
// loop on the accepting one each leave a watermark for the other in
// Runtime.deltaTracker. A generation restarted over the same data directory
// then opens its first exchange with the unchanged peer as a delta cut from
// the persisted watermark: a member it holds from before that watermark stays
// behind, and one created after it travels. With no watermark the exchange is
// a full snapshot, which carries a member however old it is.
func TestTheGossipLoopsUseTheRuntimesTrackerAcrossARestart(t *testing.T) {
	t.Setenv(EnvNodeIdentitySeed, "")
	t.Setenv(EnvFlyMachineID, "")
	peer, _ := newDeltaMeshRuntime(t)
	first, dir := newDeltaMeshRuntime(t)
	self, peerID := string(first.identity.NodeID), string(peer.identity.NodeID)
	old := time.Now().Add(-5 * time.Minute)

	holdMember(t, first, "vl1_before_the_first_exchange", old)
	exchangeOverASession(t, first, peer)
	if first.deltaTracker.RecordWatermark(peerID).IsZero() {
		t.Fatal("the initiator loop completed an exchange and left no watermark for the peer in the dialling runtime's tracker")
	}
	waitMeshDB(t, 5*time.Second, "the responder loop to leave a watermark for the dialler in the accepting runtime's tracker", func() bool {
		return !peer.deltaTracker.RecordWatermark(self).IsZero()
	})
	if !holdsMember(peer, "vl1_before_the_first_exchange") {
		t.Fatal("premise wrong: an exchange with no watermark did not carry an old member, " +
			"so an old member missing after the restart would prove nothing")
	}

	if err := first.Shutdown(); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	requirePersistedPeer(t, dir, peerID)
	waitMeshDB(t, 10*time.Second, "the peer to see the stopped runtime's session close", func() bool {
		return !holdsOpenMeshSession(peer, self)
	})

	second := startDeltaMeshRuntime(t, dir)
	if got := string(second.identity.NodeID); got != self {
		t.Fatalf("premise wrong: the restarted runtime is %s, not %s, so the peer does not know it", truncID(got), truncID(self))
	}
	if second.deltaTracker.ShouldFullSync(peerID) {
		t.Fatal("the restarted generation's tracker holds no watermark for the peer to cut a delta from")
	}
	holdMember(t, second, "vl1_before_the_watermark", old)
	holdMember(t, second, "vl1_after_the_watermark", time.Now())
	exchangeOverASession(t, second, peer)

	if !holdsMember(peer, "vl1_after_the_watermark") {
		t.Fatal("the first exchange after the restart did not carry a member created after the watermark")
	}
	if !holdsMember(second, "vl1_before_the_watermark") {
		t.Fatal("premise wrong: the restarted runtime no longer holds the old member, so its absence on the peer proves nothing")
	}
	if holdsMember(peer, "vl1_before_the_watermark") {
		t.Fatal("after a restart the first exchange with an unchanged peer was a full snapshot: " +
			"it carried a member created before the persisted watermark")
	}
}
