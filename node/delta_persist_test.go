/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ORBTR/aether"
	lad "github.com/bbmumford/ledger"
	"github.com/bbmumford/whisper"
)

// These tests cover the delta-watermark persistence backend
// (NewJSONDeltaPersistence, Save and Load) and the runtime's own tracker:
// Runtime.startDeltaTracker gives each runtime generation one DeltaTracker,
// which the gossip loops mesh_connection.go starts read and advance, and
// attaches this backend under the data directory so a restarted node resumes
// delta exchange with a peer whose watermark is still fresh.

func newTestPersistence(t *testing.T) (whisper.DeltaPersistence, string) {
	t.Helper()
	dir := t.TempDir()
	return NewJSONDeltaPersistence(dir), filepath.Join(dir, ".mesh", "watermarks.json")
}

// The first-ever boot: no file yet must be (nil, nil), NOT an error. An error
// here would make a fresh node log a spurious failure on every cold start.
func TestLoadOnAFirstEverBootReturnsNothingAndNoError(t *testing.T) {
	p, _ := newTestPersistence(t)

	states, err := p.Load()
	if err != nil {
		t.Fatalf("Load on a missing file returned %v, want nil — a first boot would "+
			"log a failure that is actually the normal case", err)
	}
	if len(states) != 0 {
		t.Fatalf("Load returned %d states from a missing file", len(states))
	}
}

func TestSaveThenLoadRoundTripsTheWatermarks(t *testing.T) {
	p, _ := newTestPersistence(t)

	want := []whisper.PersistedPeerState{
		{PeerID: "peer-a"},
		{PeerID: "peer-b"},
	}
	if err := p.Save(want); err != nil {
		t.Fatalf("Save: %v", err)
	}

	got, err := p.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(got) != len(want) {
		t.Fatalf("round trip returned %d states, want %d — watermarks are lost across "+
			"a restart, so every peer's first exchange becomes a full snapshot",
			len(got), len(want))
	}
	seen := map[string]bool{}
	for _, s := range got {
		seen[s.PeerID] = true
	}
	for _, w := range want {
		if !seen[w.PeerID] {
			t.Errorf("peer %q did not survive the round trip", w.PeerID)
		}
	}
}

// The constructor creates its directory. Without this, every Save fails at
// CreateTemp — and NewJSONDeltaPersistence deliberately DISCARDS MkdirAll's
// error, so the only signal would be a Save failure much later.
func TestTheConstructorCreatesTheMeshDirectory(t *testing.T) {
	dir := t.TempDir()
	_ = NewJSONDeltaPersistence(dir)

	info, err := os.Stat(filepath.Join(dir, ".mesh"))
	if err != nil {
		t.Fatalf(".mesh directory was not created: %v — MkdirAll's error is discarded "+
			"at :44, so the first symptom would be a Save failure with an unrelated "+
			"message", err)
	}
	if !info.IsDir() {
		t.Fatal(".mesh exists but is not a directory")
	}
}

// 🔴 SAVE IS THE ONE DOC CLAIM IN THIS FILE THAT IS TRUE, AND IT IS WORTH
// PINNING RATHER THAN ASSUMING: "Temp file + rename so a crash mid-write never
// leaves a half-encoded JSON that the next Load can't parse."
//
// The observable consequence of atomicity here is that no .tmp file survives a
// successful Save — a leaked temp per save would accumulate in a directory that
// persists across redeploys.
func TestWatermarkSaveLeavesNoTemporaryFileBehind(t *testing.T) {
	p, path := newTestPersistence(t)

	for i := 0; i < 3; i++ {
		if err := p.Save([]whisper.PersistedPeerState{{PeerID: "peer-a"}}); err != nil {
			t.Fatalf("Save %d: %v", i, err)
		}
	}

	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".tmp" {
			t.Errorf("a temp file survived a successful Save: %s — they accumulate in "+
				"a directory that outlives redeploys", e.Name())
		}
	}
	if len(entries) != 1 {
		t.Errorf("directory holds %d entries after 3 saves, want exactly 1 "+
			"(watermarks.json)", len(entries))
	}
}

// A later Save must REPLACE the previous state, not merge with it. A stale
// watermark for a peer that is gone would keep requesting deltas from a point
// the remote no longer has.
func TestASubsequentSaveReplacesRatherThanAccumulates(t *testing.T) {
	p, _ := newTestPersistence(t)

	if err := p.Save([]whisper.PersistedPeerState{{PeerID: "old-a"}, {PeerID: "old-b"}}); err != nil {
		t.Fatalf("first Save: %v", err)
	}
	if err := p.Save([]whisper.PersistedPeerState{{PeerID: "new-only"}}); err != nil {
		t.Fatalf("second Save: %v", err)
	}

	got, err := p.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(got) != 1 || got[0].PeerID != "new-only" {
		t.Fatalf("after replacing the snapshot Load returned %+v, want exactly "+
			"[new-only] — old watermarks are accumulating", got)
	}
}

// 🔴 A CORRUPTED FILE MUST ERROR, NOT SILENTLY RETURN EMPTY STATE. The doc says
// so: "Corrupted or unreadable files return an error so the caller can log and
// continue with empty state." Silently returning nil would make corruption
// indistinguishable from a first boot — the node would converge with a
// full-snapshot burst and nothing would say why.
func TestACorruptedFileErrorsRatherThanLookingLikeAFirstBoot(t *testing.T) {
	p, path := newTestPersistence(t)
	if err := p.Save([]whisper.PersistedPeerState{{PeerID: "peer-a"}}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := os.WriteFile(path, []byte("{not json at all"), 0o600); err != nil {
		t.Fatalf("corrupting the file: %v", err)
	}

	states, err := p.Load()
	if err == nil {
		t.Fatalf("Load returned no error on a corrupted file (states=%+v) — corruption "+
			"is now indistinguishable from a first boot, and the resulting "+
			"full-snapshot convergence burst has no explanation in the logs", states)
	}
}

// deltaDataDir is a data directory for a runtime whose tracker persists. The
// tracker's final save runs on its own goroutine after the runtime's context
// ends, so removal retries until that write has landed; t.TempDir's single
// RemoveAll would race it. Call it before deltaRuntime, so this cleanup runs
// after the runtime's cancel.
func deltaDataDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "loom-delta-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		deadline := time.Now().Add(3 * time.Second)
		for {
			err := os.RemoveAll(dir)
			if err == nil || time.Now().After(deadline) {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	})
	return dir
}

// deltaRuntime is a runtime generation with durable storage under dir, which
// is what startDeltaTracker attaches persistence for.
func deltaRuntime(t *testing.T, dir string) *Runtime {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return &Runtime{
		cfg:    Config{DataDir: dir, LAD: LADDirectoryConfig{Storage: "local"}},
		ctx:    ctx,
		cancel: cancel,
	}
}

// waitForPersistedPeer polls the watermarks file until it lists peer.
func waitForPersistedPeer(t *testing.T, dir, peer string) {
	t.Helper()
	path := filepath.Join(dir, ".mesh", "watermarks.json")
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if data, err := os.ReadFile(path); err == nil {
			var states []whisper.PersistedPeerState
			if json.Unmarshal(data, &states) == nil {
				for _, s := range states {
					if s.PeerID == peer {
						return
					}
				}
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("%s never listed a watermark for peer %q", path, peer)
}

// A runtime generation that stops saves its watermarks, and the next
// generation over the same data directory resumes delta exchange with a peer
// whose watermark is fresh: its first exchange is a delta from that watermark,
// not a full snapshot.
func TestARestartedRuntimeResumesDeltaExchangeWithAnUnchangedPeer(t *testing.T) {
	dir := deltaDataDir(t)
	first := deltaRuntime(t, dir)
	first.startDeltaTracker()
	for i := 0; i < 3; i++ {
		first.deltaTracker.UpdateWatermark("peer-a", time.Now())
	}
	first.cancel()
	waitForPersistedPeer(t, dir, "peer-a")

	second := deltaRuntime(t, dir)
	second.startDeltaTracker()
	if second.deltaTracker == first.deltaTracker {
		t.Fatal("premise wrong: both generations hold the same tracker, so this proves " +
			"nothing about persistence")
	}
	if second.deltaTracker.ShouldFullSync("peer-a") {
		t.Fatal("after a restart the first exchange with an unchanged peer is a full " +
			"snapshot; the persisted watermark did not reach the new generation's tracker")
	}
	if second.deltaTracker.RecordWatermark("peer-a").IsZero() {
		t.Fatal("after a restart the peer's watermark is zero, so the exchange has no " +
			"delta base")
	}
}

// Two runtimes in one process keep separate watermarks: an exchange one of
// them records with a peer leaves the other with no watermark for it.
func TestTwoRuntimesInOneProcessKeepSeparateWatermarks(t *testing.T) {
	a := deltaRuntime(t, deltaDataDir(t))
	b := deltaRuntime(t, deltaDataDir(t))
	a.startDeltaTracker()
	b.startDeltaTracker()

	a.deltaTracker.UpdateWatermark("peer-x", time.Now())
	if b.deltaTracker == a.deltaTracker {
		t.Fatal("two runtimes in one process hold the same delta tracker")
	}
	if !b.deltaTracker.RecordWatermark("peer-x").IsZero() {
		t.Fatal("runtime b holds a watermark for a peer only runtime a exchanged with")
	}
}

// A persisted watermark older than the declared staleness bound is dropped,
// so the first exchange with that peer is a full snapshot: the peer has
// likely evicted the records the old watermark marks.
func TestAWatermarkPastTheStalenessBoundIsDropped(t *testing.T) {
	dir := deltaDataDir(t)
	stale := time.Now().Add(-(deltaWatermarkMaxAge + 10*time.Minute))
	if err := NewJSONDeltaPersistence(dir).Save([]whisper.PersistedPeerState{
		{PeerID: "peer-old", Watermark: stale, ExchangeCount: 3, LastActivity: stale},
	}); err != nil {
		t.Fatalf("Save: %v", err)
	}

	rt := deltaRuntime(t, dir)
	rt.startDeltaTracker()
	if !rt.deltaTracker.ShouldFullSync("peer-old") {
		t.Fatalf("a watermark %v old resumed a delta; past the %v bound it must be dropped",
			time.Since(stale).Round(time.Minute), deltaWatermarkMaxAge)
	}
}

// A runtime without durable storage writes no watermark file: its tracker is
// in memory only, the same rule the peer store follows.
func TestARuntimeWithoutDurableStorageWritesNoWatermarks(t *testing.T) {
	dir := t.TempDir()
	rt := deltaRuntime(t, dir)
	rt.cfg.LAD.Storage = "memory"
	rt.startDeltaTracker()
	if rt.deltaTracker == nil {
		t.Fatal("a runtime without durable storage has no delta tracker at all")
	}
	rt.deltaTracker.UpdateWatermark("peer-m", time.Now())
	rt.cancel()
	time.Sleep(100 * time.Millisecond)
	if _, err := os.Stat(filepath.Join(dir, ".mesh", "watermarks.json")); err == nil {
		t.Fatal("a runtime on memory storage wrote a watermark file")
	}
}

// deltaMeshRuntime starts a whole runtime with durable storage under dir and a
// noise-UDP listener on loopback, and returns it with the listener's port. Its
// bootstrap host answers nothing, so it holds no session until a test dials
// one with dialDeltaPeer.
func deltaMeshRuntime(t *testing.T, dir string) (*Runtime, int) {
	t.Helper()
	port := freeUDPPort(t)
	rt, err := Initialize(Config{
		ServiceName: "delta-exchange-test",
		DataDir:     dir,
		NetworkKeys: []string{"delta-exchange-test-network-key"},
		VL1:         VL1TransportConfig{Enabled: true, UDPPort: port},
		LAD:         LADDirectoryConfig{Storage: "local", LocalDir: filepath.Join(dir, "lad"), BootstrapHosts: "127.0.0.1:9"},
	})
	if err != nil {
		t.Fatalf("Initialize over %s: %v", dir, err)
	}
	t.Cleanup(func() { _ = rt.Shutdown() })
	return rt, port
}

// dialDeltaPeer opens a noise-UDP session from one runtime to another's
// listener and runs the mesh session lifecycle on it, as the connection
// manager does after a dial. AcceptMeshConnection then starts the gossip
// initiator loop on the dialling runtime and the responder loop on the other.
func dialDeltaPeer(t *testing.T, from, to *Runtime, toPort int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	conn, err := from.dialNoiseUDP(ctx, aether.Target{NodeID: to.identity.NodeID, Address: fmt.Sprintf("127.0.0.1:%d", toPort)})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	bs, ok := installableMeshConn(conn)
	if !ok {
		t.Fatalf("dial returned %T", conn)
	}
	go from.DialAndAcceptMesh(from.ctx, bs.Conn, string(to.identity.NodeID), "", ProtoNoiseUDP, "", "", "", dialBorrowsConnectingState)
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

// dropMeshSession closes the session rt holds for a peer, which is what rt's
// keepalive does once that peer has been silent for its retry budget. A
// noise-UDP peer that stops sends no close, and registerMeshSession keeps an
// open session and refuses a second one of the same grade from the same
// dialler, so without this the peer would refuse a restarted runtime's
// session until the keepalive gave the first one up.
func dropMeshSession(t *testing.T, rt *Runtime, peerID string) {
	t.Helper()
	rt.connMgr.dispatchMu.RLock()
	session := rt.connMgr.meshSessions[peerID]
	rt.connMgr.dispatchMu.RUnlock()
	if session == nil {
		t.Fatalf("the runtime holds no session for %s", truncID(peerID))
	}
	_ = session.Close()
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
	dir := deltaDataDir(t)
	peer, peerPort := deltaMeshRuntime(t, deltaDataDir(t))
	first, _ := deltaMeshRuntime(t, dir)
	self, peerID := string(first.identity.NodeID), string(peer.identity.NodeID)
	old := time.Now().Add(-5 * time.Minute)

	holdMember(t, first, "vl1_before_the_first_exchange", old)
	dialDeltaPeer(t, first, peer, peerPort)
	waitMeshDB(t, 10*time.Second, "the initiator loop to leave a watermark for the peer in the dialling runtime's tracker", func() bool {
		return !first.deltaTracker.RecordWatermark(peerID).IsZero()
	})
	waitMeshDB(t, 10*time.Second, "the responder loop to leave a watermark for the dialler in the accepting runtime's tracker", func() bool {
		return !peer.deltaTracker.RecordWatermark(self).IsZero()
	})
	if !holdsMember(peer, "vl1_before_the_first_exchange") {
		t.Fatal("premise wrong: an exchange with no watermark did not carry an old member, " +
			"so an old member missing after the restart would prove nothing")
	}

	if err := first.Shutdown(); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	waitForPersistedPeer(t, dir, peerID)
	dropMeshSession(t, peer, self)

	second, _ := deltaMeshRuntime(t, dir)
	if got := string(second.identity.NodeID); got != self {
		t.Fatalf("premise wrong: the restarted runtime is %s, not %s, so the peer does not know it", truncID(got), truncID(self))
	}
	if second.deltaTracker.ShouldFullSync(peerID) {
		t.Fatal("the restarted generation's tracker holds no watermark for the peer to cut a delta from")
	}
	holdMember(t, second, "vl1_before_the_watermark", old)
	holdMember(t, second, "vl1_after_the_watermark", time.Now())
	served := peer.deltaTracker.RecordWatermark(self)
	dialDeltaPeer(t, second, peer, peerPort)
	waitMeshDB(t, 10*time.Second, "the peer to serve the restarted generation's first exchange", func() bool {
		return peer.deltaTracker.RecordWatermark(self).After(served)
	})

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
