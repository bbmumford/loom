/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

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
