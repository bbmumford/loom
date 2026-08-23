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
)

// COVERAGE of the persisted peer list, 9 functions at 0.0%.
//
// CENSUSED FIRST and it is wired on any node with a durable data dir:
// runtime.go:2683 constructs it, :2688 Loads it and seeds the connection
// manager's scan queue from the result, :2711 starts the periodic save.
// ⇒ this file is a node's ONLY memory of the mesh across a cold restart. If
// it is empty or stale the node has to rediscover everything from bootstrap.
//
// The store is deliberately best-effort — a corrupt file is recreated rather
// than fatal — and the tests below pin which failures are swallowed on
// purpose and which would be silent data loss.

func storeForTest(t *testing.T, self string) (*FilePeerStore, string) {
	t.Helper()
	dir := t.TempDir()
	cfg := DefaultPeerStoreConfig(dir)
	cfg.SaveInterval = 10 * time.Millisecond
	return NewFilePeerStore(self, cfg), cfg.FilePath
}

func peerEntry(id, region string, lastSeen time.Time) PersistedPeer {
	return PersistedPeer{
		NodeID: id, Region: region, LastSeen: lastSeen,
		Addresses: []string{"1.2.3.4:9000"}, Source: "pex",
	}
}

// ── Round trip ──────────────────────────────────────────────────────────────

// The whole point of the file: what a node knew before the restart is what it
// knows after.
func TestPeersSurviveASaveAndLoadRoundTrip(t *testing.T) {
	s, path := storeForTest(t, testNodeIDA)
	s.Add(peerEntry(testNodeIDB, "syd", time.Now()))

	if err := s.Save(); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("no file at %s after Save: %v — the node has no memory of the "+
			"mesh across a restart", path, err)
	}

	// A fresh store over the same file is exactly the cold-restart case.
	reloaded := NewFilePeerStore(testNodeIDA, DefaultPeerStoreConfig(filepath.Dir(path)))
	peers, err := reloaded.Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(peers) != 1 || peers[0].NodeID != testNodeIDB {
		t.Fatalf("Load returned %+v, want the one saved peer — a cold restart "+
			"rediscovers the whole mesh from bootstrap instead", peers)
	}
	if peers[0].Region != "syd" {
		t.Fatalf("Region = %q, want syd — the region hint is what lets the "+
			"first reconnect prefer a near peer", peers[0].Region)
	}
	// Load also populates the in-memory state, or the next Save writes nothing.
	if got := len(reloaded.All()); got != 1 {
		t.Fatalf("All() = %d after Load, want 1 — the next periodic Save would "+
			"overwrite the file with an empty list", got)
	}
}

// 🔴 A NODE MUST NEVER PERSIST ITSELF. A self-entry would be fed straight back
// into the scan queue at runtime.go:2693 and the node would try to dial itself
// on every cold start.
func TestTheStoreRefusesToRecordItsOwnNodeID(t *testing.T) {
	s, _ := storeForTest(t, testNodeIDA)

	s.Add(peerEntry(testNodeIDA, "syd", time.Now())) // ourselves
	s.Add(peerEntry(testNodeIDB, "syd", time.Now()))

	if got := len(s.All()); got != 1 {
		t.Fatalf("All() = %d, want 1 — the store accepted its own node ID", got)
	}
	if err := s.Save(); err != nil {
		t.Fatal(err)
	}

	// And the read side filters it too, so a file written by an older build
	// (or by a different node) cannot reintroduce it.
	s2, path2 := storeForTest(t, testNodeIDA)
	writeRawPeerFile(t, path2, persistedPeerList{Peers: []PersistedPeer{
		peerEntry(testNodeIDA, "syd", time.Now()),
		peerEntry(testNodeIDB, "syd", time.Now()),
	}})
	peers, err := s2.Load()
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range peers {
		if p.NodeID == testNodeIDA {
			t.Fatal("Load returned our own node ID — runtime.go:2693 would " +
				"feed it to the scan queue and the node would dial itself")
		}
	}
}

func writeRawPeerFile(t *testing.T, path string, list persistedPeerList) {
	t.Helper()
	data, err := json.Marshal(list)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
}

// ── Staleness ───────────────────────────────────────────────────────────────

// Entries older than MaxAge are dropped on load. Without this a node
// resurrects a year-old peer list and spends its startup budget dialling
// addresses that stopped existing.
func TestEntriesOlderThanMaxAgeAreDroppedOnLoad(t *testing.T) {
	s, path := storeForTest(t, testNodeIDA)
	fresh := peerEntry(testNodeIDB, "syd", time.Now())
	stale := peerEntry("dd44dd44dd44dd44dd44dd44dd44dd44dd44dd44dd44dd44dd44dd44dd44dd44",
		"iad", time.Now().Add(-48*time.Hour)) // MaxAge is 24h
	writeRawPeerFile(t, path, persistedPeerList{Peers: []PersistedPeer{fresh, stale}})

	peers, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(peers) != 1 || peers[0].NodeID != testNodeIDB {
		t.Fatalf("Load returned %d peers (%+v), want only the fresh one",
			len(peers), peers)
	}
	// The stale entry must not linger in memory either, or the next Save
	// writes it straight back and it never ages out.
	for _, p := range s.All() {
		if p.NodeID == stale.NodeID {
			t.Fatal("a stale entry survived into the in-memory store — the " +
				"next periodic Save re-persists it and it is immortal")
		}
	}
}

// ── Failure handling, and which failures are swallowed on purpose ───────────

// A missing file is the normal first-boot case: no error, no peers.
func TestAMissingFileIsNotAnError(t *testing.T) {
	s, _ := storeForTest(t, testNodeIDA)
	peers, err := s.Load()
	if err != nil {
		t.Fatalf("Load of a non-existent file returned %v — first boot would "+
			"log an error every time", err)
	}
	if peers != nil {
		t.Fatalf("Load returned %+v for a missing file, want nil", peers)
	}
}

// 🔑 A CORRUPT FILE IS SWALLOWED, DELIBERATELY — pinned because it is the one
// place the store loses data silently. The caller (runtime.go:2688) branches
// on err, so returning an error here would log "Failed to load persisted
// peers" and change nothing else; returning nil,nil means the node starts
// with an empty list and the file is recreated on the next save. Best-effort
// cache semantics, and the parse failure IS logged — but a reader should know
// the whole peer list can vanish without the caller ever seeing an error.
func TestACorruptFileIsTreatedAsEmptyRatherThanFatal(t *testing.T) {
	s, path := storeForTest(t, testNodeIDA)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("{not json at all"), 0o644); err != nil {
		t.Fatal(err)
	}

	peers, err := s.Load()
	if err != nil {
		t.Fatalf("Load of a corrupt file returned %v — this pins the CURRENT "+
			"best-effort contract; if it is now an error, runtime.go:2688's "+
			"log line changes and this test should change with it", err)
	}
	if len(peers) != 0 {
		t.Fatalf("Load returned %+v from a corrupt file", peers)
	}
	// The store must still be usable afterwards — a corrupt file cannot leave
	// it wedged, or the node never persists anything again.
	s.Add(peerEntry(testNodeIDB, "syd", time.Now()))
	if err := s.Save(); err != nil {
		t.Fatalf("Save after a corrupt Load: %v", err)
	}
}

// Save is atomic (temp + rename) and must not leave the temp file behind — a
// data dir that accumulates .tmp files eventually fills a small Fly volume.
func TestSaveLeavesNoTemporaryFileBehind(t *testing.T) {
	s, path := storeForTest(t, testNodeIDA)
	s.Add(peerEntry(testNodeIDB, "syd", time.Now()))
	if err := s.Save(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path + ".tmp"); err == nil {
		t.Fatal("the .tmp file survived a successful Save — repeated saves " +
			"would leave one per write on the data volume")
	}
}

// ── Capacity ────────────────────────────────────────────────────────────────

// 🔴 THE CAP EVICTS BY AGE, NOT ARRIVAL. A burst of gossip must not push out
// the long-known peers that are the node's best restart candidates.
func TestPruningEvictsTheOldestPeersNotTheNewestOnes(t *testing.T) {
	s, _ := storeForTest(t, testNodeIDA)

	// One deliberately ancient entry, then fill past the cap with fresh ones.
	const ancient = "ee55ee55ee55ee55ee55ee55ee55ee55ee55ee55ee55ee55ee55ee55ee55ee55"
	s.mu.Lock()
	s.peers[ancient] = peerEntry(ancient, "syd", time.Now().Add(-time.Hour))
	s.mu.Unlock()

	for i := 0; i < peerStoreMaxEntries+5; i++ {
		s.Add(peerEntry(nodeIDf(i), "syd", time.Now()))
	}

	if got := len(s.All()); got > peerStoreMaxEntries {
		t.Fatalf("store holds %d entries, cap is %d — unbounded growth in a "+
			"file written to disk every 5 minutes", got, peerStoreMaxEntries)
	}
	for _, p := range s.All() {
		if p.NodeID == ancient {
			t.Fatal("the OLDEST peer survived pruning past the cap — eviction " +
				"is not ordered by LastSeen, so a gossip burst keeps the " +
				"newest addresses and drops the proven ones")
		}
	}
}

// nodeIDf builds a DISTINCT 64-char node ID per index.
//
// 🙋 My first version was `pad + string(rune('a'+i%26))`, which repeats every
// 26 values — so the loop above inserted 26 keys, never reached the 500 cap,
// pruning never ran, and the test failed claiming the oldest peer "survived
// pruning". The fixture was wrong, not the code. A capacity test whose keys
// collide silently tests nothing at all.
func nodeIDf(i int) string {
	id := fmt.Sprintf("%064x", i+1)
	return id[len(id)-64:]
}

// Add refreshes LastSeen — that is what keeps an actively-seen peer out of the
// eviction path.
func TestAddRefreshesLastSeen(t *testing.T) {
	s, _ := storeForTest(t, testNodeIDA)
	old := time.Now().Add(-time.Hour)
	s.Add(peerEntry(testNodeIDB, "syd", old))

	got := s.All()
	if len(got) != 1 {
		t.Fatalf("All() = %d, want 1", len(got))
	}
	if !got[0].LastSeen.After(old) {
		t.Fatalf("LastSeen = %v, want it refreshed past %v — a peer we are "+
			"actively adding would age out as if we had not seen it",
			got[0].LastSeen, old)
	}
}

func TestRemoveDropsAPeerAndIsSafeForUnknownIDs(t *testing.T) {
	s, _ := storeForTest(t, testNodeIDA)
	s.Add(peerEntry(testNodeIDB, "syd", time.Now()))

	s.Remove(testNodeIDB)
	if got := len(s.All()); got != 0 {
		t.Fatalf("All() = %d after Remove, want 0", got)
	}
	s.Remove("never-added") // must not panic
}

// ── Periodic save ───────────────────────────────────────────────────────────

// The ticker really does write, and cancelling the context performs the final
// save. Both halves matter: without the ticker a long-running node persists
// nothing until shutdown, and without the final save it loses everything
// learned since the last tick.
func TestPeriodicSaveWritesOnTheTickerAndAgainOnCancel(t *testing.T) {
	s, path := storeForTest(t, testNodeIDA) // SaveInterval 10ms
	ctx, cancel := context.WithCancel(context.Background())
	s.Add(peerEntry(testNodeIDB, "syd", time.Now()))

	s.StartPeriodicSave(ctx)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("no file after several save intervals: %v — a long-running "+
			"node persists nothing until it shuts down", err)
	}

	// Learn a new peer, then cancel: the final save must include it.
	const late = "ff66ff66ff66ff66ff66ff66ff66ff66ff66ff66ff66ff66ff66ff66ff66ff66"
	s.Add(peerEntry(late, "iad", time.Now()))
	cancel()

	found := false
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !found {
		data, err := os.ReadFile(path)
		if err == nil {
			var list persistedPeerList
			if json.Unmarshal(data, &list) == nil {
				for _, p := range list.Peers {
					if p.NodeID == late {
						found = true
					}
				}
			}
		}
		if !found {
			time.Sleep(time.Millisecond)
		}
	}
	if !found {
		t.Fatal("the peer learned just before cancellation is missing from the " +
			"file — the final-save-on-shutdown path did not run, and every " +
			"peer discovered since the last tick is lost on restart")
	}
}

func TestDefaultConfigPointsAtTheDataDir(t *testing.T) {
	cfg := DefaultPeerStoreConfig("/var/lib/orbtr")
	if cfg.FilePath != filepath.Join("/var/lib/orbtr", "mesh-peers.json") {
		t.Fatalf("FilePath = %q", cfg.FilePath)
	}
	if cfg.SaveInterval != 5*time.Minute || cfg.MaxAge != 24*time.Hour {
		t.Fatalf("defaults changed: %+v — the 24h MaxAge is what stops a node "+
			"dialling a year-old address list", cfg)
	}
}
