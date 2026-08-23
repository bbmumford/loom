/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/bbmumford/whisper"
)

// COVERAGE of the delta-watermark persistence backend:
// NewJSONDeltaPersistence (:42), Save (:53), Load (:88) — all at 0.0%.
//
// 🔴 CENSUS FIRST, AND IT IS THE FINDING. The chain is unwired END TO END in
// this repo:
//
//	NewJSONDeltaPersistence   0 non-test callers (loom, _PACKAGES and ORBTR roots)
//	SharedDeltaTracker()      0 non-test callers — only its own declaration
//	AttachPersistence         0 CODE references in loom; it appears ONLY inside the
//	                          comment at core/directory/gossip/stream_gossip.go:30.
//	                          The method does exist upstream (whisper@v0.0.18, 2 hits)
//	                          — loom simply never calls it.
//
// That comment asserts: "The Runtime wires a DeltaPersistence backend onto this
// tracker via AttachPersistence so watermarks survive fly redeploys — without
// persistence, every restart converts every peer's first exchange into a
// full-snapshot, the convergence-burst pattern that historically drained stream
// credit windows."
//
// ⇒ The Runtime does not. The comment names a mitigation for a specific,
// historically-observed production failure, and that mitigation is not attached.
//
// ⚠ BOUND HONESTLY: unlike ConnectionMap, these ARE exported symbols on
// a published module and NewJSONDeltaPersistence returns an interface, so an
// out-of-repo consumer could construct and wire one. This census bounds THIS
// repo. What it does establish regardless is the narrow claim the comment makes:
// loom's own Runtime does not wire it.
//
// The implementation itself is correct — Save is genuinely atomic — so these
// tests characterise a working component that has no producer.

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
