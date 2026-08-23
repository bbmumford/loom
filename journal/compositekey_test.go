/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package journal

import (
	"context"
	"testing"

	"github.com/bbmumford/loom/ports"
)

// keyedRec builds a record in one of a node's per-key slots.
func keyedRec(topic, node, key string, hlc uint64, body string) ports.Record {
	r := rec(topic, node, hlc, body, false)
	r.Key = key
	return r
}

// Compaction keeps only the newest entry per SLOT and deletes the rest. The
// slot identity must therefore include Key: with composite keys one node
// holds several records on one topic, and a node-only key makes them share a
// slot — so compaction silently deletes durable records that were never
// superseded.
//
// This is permanent loss, not a projection glitch, which is why it is pinned
// here rather than left to the cutover to discover.
func TestCompactionKeepsEveryKeyedSlotOfANode(t *testing.T) {
	j, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	ctx := context.Background()

	// ONE node, ONE topic, THREE distinct per-key slots — none supersedes
	// another. Ascending HLCs so a node-keyed compactor keeps only the last.
	for i, key := range []string{"peer-1", "peer-2", "peer-3"} {
		if _, err := j.Append(ctx, keyedRec("fleet.latency", "nodeA", key, uint64(10+i), "rtt-"+key)); err != nil {
			t.Fatal(err)
		}
	}
	head, err := j.Head(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := j.Compact(ctx, head); err != nil {
		t.Fatal(err)
	}

	ch, err := j.ReplayUntil(ctx, 0, head, nil)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for r := range ch {
		got[r.Key] = string(r.Body)
	}
	for _, key := range []string{"peer-1", "peer-2", "peer-3"} {
		if body, ok := got[key]; !ok {
			t.Errorf("slot key %q was COMPACTED AWAY — it was never superseded; "+
				"durable records are being deleted because slotKey ignores Key", key)
		} else if body != "rtt-"+key {
			t.Errorf("slot key %q body = %q, want %q", key, body, "rtt-"+key)
		}
	}
	if len(got) != 3 {
		t.Errorf("replay yielded %d slots, want 3 (got %v)", len(got), got)
	}
}

// The positive control for the test above: compaction must STILL collapse
// genuine supersessions within one slot. Without this, a slotKey that simply
// never matched anything would also pass.
func TestCompactionStillCollapsesSupersededWritesInOneSlot(t *testing.T) {
	j, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	ctx := context.Background()

	// Same node, same topic, SAME key — later writes supersede earlier ones.
	for i, body := range []string{"v1", "v2", "v3"} {
		if _, err := j.Append(ctx, keyedRec("fleet.latency", "nodeA", "peer-1", uint64(10+i), body)); err != nil {
			t.Fatal(err)
		}
	}
	head, err := j.Head(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := j.Compact(ctx, head); err != nil {
		t.Fatal(err)
	}

	ch, err := j.ReplayUntil(ctx, 0, head, nil)
	if err != nil {
		t.Fatal(err)
	}
	var kept []ports.Record
	for r := range ch {
		kept = append(kept, r)
	}
	if len(kept) != 1 {
		t.Fatalf("compaction kept %d entries for ONE slot, want 1 — compaction is "+
			"no longer collapsing supersessions", len(kept))
	}
	if string(kept[0].Body) != "v3" {
		t.Fatalf("kept body = %q, want the newest (%q)", kept[0].Body, "v3")
	}
}

// The keyless form must be untouched: every record in the fleet today has
// Key == "", and this change must not alter their compaction at all.
func TestCompactionUnchangedForKeylessRecords(t *testing.T) {
	j, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	ctx := context.Background()

	for i, body := range []string{"v1", "v2"} {
		if _, err := j.Append(ctx, rec("fleet.peer", "nodeA", uint64(10+i), body, false)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := j.Append(ctx, rec("fleet.peer", "nodeB", 12, "other", false)); err != nil {
		t.Fatal(err)
	}
	head, err := j.Head(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := j.Compact(ctx, head); err != nil {
		t.Fatal(err)
	}

	ch, err := j.ReplayUntil(ctx, 0, head, nil)
	if err != nil {
		t.Fatal(err)
	}
	byNode := map[ports.NodeID]string{}
	n := 0
	for r := range ch {
		byNode[r.NodeID] = string(r.Body)
		n++
	}
	if n != 2 {
		t.Fatalf("kept %d entries, want 2 (nodeA superseded to v2, nodeB intact)", n)
	}
	if byNode["nodeA"] != "v2" || byNode["nodeB"] != "other" {
		t.Fatalf("keyless compaction changed: %v", byNode)
	}
}
