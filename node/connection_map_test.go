/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"fmt"
	"testing"
	"time"
)

// COVERAGE of the gossip connection map: BatchUpdate (:55),
// EvictStale (:120), Snapshot (:164) — all at 0.0% — and pruneIfNeeded (:136)
// at 26.7% with its entire hard-cap branch unexecuted.
//
// 🔴 CENSUS FIRST, AND IT IS THE FINDING. The COMPLETE set of
// non-test references to the live instance is 8 lines:
//
//	peer_connections.go:648   field decl
//	peer_connections.go:1071  mgr.connectionMap = NewConnectionMap()   ← constructed
//	peer_connections.go:1081  mgr.scaler.connectionMap = ...           ← wired
//	connection_scaling.go:74  field decl
//	connection_scaling.go:299 nil guard
//	connection_scaling.go:302 IsHotspot(peerNodeID)                    ← READ
//	connection_scaling.go:303 ConnectionCount(peerNodeID)              ← READ
//	connection_scaling.go:305 MeshAverage()                            ← READ
//
// THREE READS. ZERO WRITES. Update and BatchUpdate are the only ways data can
// enter this map and NEITHER has a production caller, so cm.entries is
// permanently empty in production and IsHotspot returns false at its FIRST
// guard — the hotspot-driven scaling reduction at connection_scaling.go:302 can
// never fire.
//
// The bound is tight rather than a source-grep guess: the only function
// returning *ConnectionMap is the constructor, and the instance is held solely
// in two UNEXPORTED fields, so no out-of-repo caller can reach it either.
//
// These tests therefore pin the type's behaviour as a UNIT — it is correct code
// with no producer — so that whoever wires the gossip writer inherits a
// characterised component rather than an unexercised one.

func TestBatchUpdateInsertsNewPeersAndRefreshesExistingOnes(t *testing.T) {
	cm := NewConnectionMap()
	cm.Update("peer-a", 10, 100)

	cm.BatchUpdate(map[string]int{"peer-a": 25, "peer-b": 7})

	if got, ok := cm.ConnectionCount("peer-a"); !ok || got != 25 {
		t.Errorf("peer-a = (%d,%v), want (25,true) — an existing entry was not refreshed", got, ok)
	}
	if got, ok := cm.ConnectionCount("peer-b"); !ok || got != 7 {
		t.Errorf("peer-b = (%d,%v), want (7,true) — a new entry was not inserted", got, ok)
	}
}

// ⚠ CHARACTERISATION: BatchUpdate leaves Capacity at 0 for a NEWLY created
// entry (:65-69) while Update sets it (:48). That is currently harmless only
// because ConnectionMapEntry.Capacity has no reader anywhere in the repo — so
// the zero participates in no comparison. Pinned so that whoever gives Capacity
// a reader discovers this asymmetry from a test rather than from behaviour.
func TestBatchUpdateLeavesCapacityUnsetForNewEntries(t *testing.T) {
	cm := NewConnectionMap()
	cm.Update("known", 5, 64)
	cm.BatchUpdate(map[string]int{"known": 6, "fresh": 3})

	cm.mu.RLock()
	known, fresh := cm.entries["known"], cm.entries["fresh"]
	cm.mu.RUnlock()

	if known.Capacity != 64 {
		t.Errorf("BatchUpdate clobbered an existing Capacity: got %d, want 64 — the "+
			"preserve-on-update branch is not preserving", known.Capacity)
	}
	if fresh.Capacity != 0 {
		t.Errorf("a BatchUpdate-created entry now has Capacity %d — that is very "+
			"likely an improvement over 0; update this test deliberately and check "+
			"every Capacity reader, because there are none",
			fresh.Capacity)
	}
}

func TestEvictStaleRemovesOnlyEntriesPastMaxAge(t *testing.T) {
	cm := NewConnectionMap()
	cm.Update("fresh", 1, 10)

	// Backdate one entry past maxAge without waiting two minutes.
	cm.mu.Lock()
	cm.entries["old"] = ConnectionMapEntry{
		NodeID: "old", Connections: 99, ReportedAt: time.Now().Add(-3 * time.Minute),
	}
	cm.mu.Unlock()

	if n := cm.EvictStale(); n != 1 {
		t.Fatalf("EvictStale evicted %d, want 1 — a stale peer's count keeps skewing "+
			"MeshAverage and therefore every hotspot verdict", n)
	}
	if _, ok := cm.ConnectionCount("old"); ok {
		t.Error("the stale entry survived eviction")
	}
	if _, ok := cm.ConnectionCount("fresh"); !ok {
		t.Error("EvictStale removed a FRESH entry — it is evicting on the wrong side " +
			"of the comparison")
	}
}

// A stale entry must be invisible to readers even before EvictStale runs —
// otherwise a dead peer's last count steers scaling until the next sweep.
func TestAStaleEntryIsInvisibleToReadersBeforeEvictionRuns(t *testing.T) {
	cm := NewConnectionMap()
	cm.mu.Lock()
	cm.entries["old"] = ConnectionMapEntry{
		NodeID: "old", Connections: 500, ReportedAt: time.Now().Add(-3 * time.Minute),
	}
	cm.mu.Unlock()

	if _, ok := cm.ConnectionCount("old"); ok {
		t.Error("ConnectionCount returned a stale entry as live")
	}
	if avg := cm.MeshAverage(); avg != 0 {
		t.Errorf("MeshAverage = %v with only a stale entry, want 0 — a dead peer's "+
			"count is still setting the mesh-wide mean", avg)
	}
	if cm.IsHotspot("old") {
		t.Error("a stale peer was reported as a hotspot")
	}
}

func TestSnapshotReturnsLiveCountsAndIsACopy(t *testing.T) {
	cm := NewConnectionMap()
	cm.Update("a", 3, 10)
	cm.Update("b", 8, 10)

	snap := cm.Snapshot()
	if len(snap) != 2 || snap["a"] != 3 || snap["b"] != 8 {
		t.Fatalf("Snapshot = %v, want {a:3 b:8}", snap)
	}

	// Mutating the snapshot must not reach the map — it is described as gossip
	// output, so a caller marshalling it must not be able to corrupt live state.
	snap["a"] = 999
	if got, _ := cm.ConnectionCount("a"); got != 3 {
		t.Errorf("mutating the snapshot changed the live map (a = %d) — Snapshot is "+
			"handing out its internal state", got)
	}
}

// 🔴 THE HARD CAP IS UNEXERCISED IN THE REPO (measured: pruneIfNeeded 26.7%,
// with the whole cap branch at count=0). It is the only thing bounding this
// map's memory, and the map is fed from gossip — i.e. from remote input.
func TestTheEntryCapIsEnforcedSoGossipCannotGrowTheMapWithoutBound(t *testing.T) {
	cm := NewConnectionMap()
	for i := 0; i < connMapMaxEntries+50; i++ {
		cm.Update(fmt.Sprintf("peer-%04d", i), i, 100)
	}

	cm.mu.RLock()
	n := len(cm.entries)
	cm.mu.RUnlock()

	if n > connMapMaxEntries {
		t.Fatalf("map holds %d entries, cap is %d — the bound on a gossip-fed map is "+
			"not being enforced, so remote peers determine this node's memory use", n, connMapMaxEntries)
	}
	if n == 0 {
		t.Fatal("the cap evicted EVERYTHING — over-eviction makes MeshAverage 0 and " +
			"silently disables every hotspot verdict")
	}
}

// Characterisation: with no writer, every read is
// the empty-map answer. This is what the scaler sees in production today.
func TestWithNoWriterEveryReadIsTheEmptyMapAnswer(t *testing.T) {
	cm := NewConnectionMap() // exactly what peer_connections.go:1071 builds

	if _, ok := cm.ConnectionCount("any-peer"); ok {
		t.Error("ConnectionCount reported data on a map nothing has written to")
	}
	if avg := cm.MeshAverage(); avg != 0 {
		t.Errorf("MeshAverage = %v on an empty map, want 0", avg)
	}
	if cm.IsHotspot("any-peer") {
		t.Error("IsHotspot returned TRUE on an empty map — the scaler would reduce a " +
			"peer's target on evidence that does not exist")
	}
	if n := len(cm.Snapshot()); n != 0 {
		t.Errorf("Snapshot returned %d entries from an empty map", n)
	}
}
