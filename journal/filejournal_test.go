/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package journal

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bbmumford/loom/ports"
)

func rec(topic, node string, hlc uint64, body string, tomb bool) ports.Record {
	return ports.Record{
		Topic:        ports.Topic(topic),
		NodeID:       ports.NodeID(node),
		HLC:          ports.HLC(hlc),
		Tombstone:    tomb,
		Body:         []byte(body),
		AuthorPubKey: []byte("pub-" + node),
		Signature:    []byte("sig-" + body),
	}
}

func drain(t *testing.T, ch <-chan ports.Record, n int) []ports.Record {
	t.Helper()
	out := make([]ports.Record, 0, n)
	deadline := time.After(3 * time.Second)
	for len(out) < n {
		select {
		case r, ok := <-ch:
			if !ok {
				t.Fatalf("channel closed after %d/%d records", len(out), n)
			}
			out = append(out, r)
		case <-deadline:
			t.Fatalf("timed out after %d/%d records", len(out), n)
		}
	}
	return out
}

func TestAppendReplayRoundTrip(t *testing.T) {
	ctx := context.Background()
	j, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()

	w1, err := j.Append(ctx, rec("t.a", "n1", 100, "one", false))
	if err != nil {
		t.Fatal(err)
	}
	w2, err := j.Append(ctx, rec("t.b", "n2", 200, "two", false))
	if err != nil {
		t.Fatal(err)
	}
	if w1 != 1 || w2 != 2 {
		t.Fatalf("watermarks = %d, %d", w1, w2)
	}

	ctx2, cancel := context.WithCancel(ctx)
	defer cancel()
	ch, err := j.Replay(ctx2, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	got := drain(t, ch, 2)
	if string(got[0].Body) != "one" || string(got[1].Body) != "two" {
		t.Fatalf("replay order: %q, %q", got[0].Body, got[1].Body)
	}
	// Signed fields must round-trip byte-identically.
	if !bytes.Equal(got[0].Signature, []byte("sig-one")) || !bytes.Equal(got[0].AuthorPubKey, []byte("pub-n1")) {
		t.Fatal("signed fields did not round-trip")
	}
}

func TestRestartRecovery(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	j, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.Append(ctx, rec("t.a", "n1", 1, "x", false)); err != nil {
		t.Fatal(err)
	}
	if _, err := j.BatchAppend(ctx, []ports.Record{
		rec("t.a", "n2", 2, "y", false),
		rec("t.a", "n3", 3, "z", false),
	}); err != nil {
		t.Fatal(err)
	}
	j.Close()

	// Reopen: head and contents must be identical to an uninterrupted run.
	j2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer j2.Close()
	head, err := j2.Head(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if head != 3 {
		t.Fatalf("recovered head = %d, want 3", head)
	}
	w4, err := j2.Append(ctx, rec("t.a", "n4", 4, "w", false))
	if err != nil || w4 != 4 {
		t.Fatalf("post-recovery append = %d, %v", w4, err)
	}
}

func TestTornTailTruncated(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	j, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = j.Append(ctx, rec("t.a", "n1", 1, "keep", false))
	j.Close()

	// Simulate a crash mid-append: garbage tail bytes.
	path := filepath.Join(dir, "loom-journal.log")
	f, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	f.Write([]byte{0x00, 0x00, 0x00, 0xFF, 0x01})
	f.Close()

	j2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer j2.Close()
	head, _ := j2.Head(ctx)
	if head != 1 {
		t.Fatalf("head after torn tail = %d, want 1", head)
	}
	// The journal must accept new appends cleanly after truncation.
	if w, err := j2.Append(ctx, rec("t.a", "n2", 2, "next", false)); err != nil || w != 2 {
		t.Fatalf("append after truncation = %d, %v", w, err)
	}
}

func TestReplayHistoryThenLiveNoGapNoDup(t *testing.T) {
	ctx := context.Background()
	j, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()

	for i := 1; i <= 5; i++ {
		if _, err := j.Append(ctx, rec("t.a", "n1", uint64(i), string(rune('a'+i-1)), false)); err != nil {
			t.Fatal(err)
		}
	}
	ctx2, cancel := context.WithCancel(ctx)
	defer cancel()
	ch, err := j.Replay(ctx2, 2, nil) // resume after watermark 2
	if err != nil {
		t.Fatal(err)
	}
	// Concurrent live appends while history replays.
	for i := 6; i <= 8; i++ {
		if _, err := j.Append(ctx, rec("t.a", "n1", uint64(i), string(rune('a'+i-1)), false)); err != nil {
			t.Fatal(err)
		}
	}
	got := drain(t, ch, 6) // watermarks 3..8
	for i, r := range got {
		want := string(rune('a' + 2 + i))
		if string(r.Body) != want {
			t.Fatalf("position %d = %q, want %q (gap or duplicate)", i, r.Body, want)
		}
	}
}

func TestReplayTopicFilter(t *testing.T) {
	ctx := context.Background()
	j, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	_, _ = j.Append(ctx, rec("t.keep", "n1", 1, "k1", false))
	_, _ = j.Append(ctx, rec("t.skip", "n1", 2, "s1", false))
	_, _ = j.Append(ctx, rec("t.keep", "n2", 3, "k2", false))

	ctx2, cancel := context.WithCancel(ctx)
	defer cancel()
	ch, err := j.Replay(ctx2, 0, []ports.Topic{"t.keep"})
	if err != nil {
		t.Fatal(err)
	}
	got := drain(t, ch, 2)
	if string(got[0].Body) != "k1" || string(got[1].Body) != "k2" {
		t.Fatalf("filtered replay: %q, %q", got[0].Body, got[1].Body)
	}
}

func TestCompactRetainsLatestPerSlotAndTombstones(t *testing.T) {
	ctx := context.Background()
	j, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()

	// n1: two versions then final state at w3. n2: live then TOMBSTONE at w4.
	_, _ = j.Append(ctx, rec("t.a", "n1", 1, "n1-old", false))
	_, _ = j.Append(ctx, rec("t.a", "n2", 2, "n2-live", false))
	_, _ = j.Append(ctx, rec("t.a", "n1", 3, "n1-new", false))
	_, _ = j.Append(ctx, rec("t.a", "n2", 4, "", true))
	_, _ = j.Append(ctx, rec("t.a", "n3", 5, "n3-after", false))

	if err := j.Compact(ctx, 4); err != nil {
		t.Fatal(err)
	}

	ctx2, cancel := context.WithCancel(ctx)
	defer cancel()
	ch, err := j.Replay(ctx2, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	got := drain(t, ch, 3)
	// Survivors: n1-new (latest ≤ upTo), the n2 tombstone (latest ≤ upTo —
	// retention must NEVER drop a governing tombstone), n3-after (> upTo).
	// Dropped: n1-old, n2-live.
	if string(got[0].Body) != "n1-new" {
		t.Fatalf("first survivor = %q", got[0].Body)
	}
	if !got[1].Tombstone || got[1].NodeID != "n2" {
		t.Fatalf("second survivor must be n2's tombstone, got %+v", got[1])
	}
	if string(got[2].Body) != "n3-after" {
		t.Fatalf("third survivor = %q", got[2].Body)
	}

	// Journal stays appendable post-compaction with monotonic watermarks.
	w, err := j.Append(ctx, rec("t.a", "n4", 6, "post", false))
	if err != nil || w != 6 {
		t.Fatalf("post-compact append = %d, %v", w, err)
	}
}

func TestSnapshotIsPointInTime(t *testing.T) {
	ctx := context.Background()
	j, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	_, _ = j.Append(ctx, rec("t.a", "n1", 1, "in-snapshot", false))

	snap, err := j.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// An append AFTER the snapshot must not appear in it.
	_, _ = j.Append(ctx, rec("t.a", "n2", 2, "after-snapshot", false))

	// Parse the snapshot stream with the journal framing (bodies are
	// base64 inside JSON — raw byte search would always miss).
	var bodies []string
	data := new(bytes.Buffer)
	if _, err := data.ReadFrom(snap); err != nil {
		t.Fatal(err)
	}
	snap.Close()
	buf := data.Bytes()
	for len(buf) >= 8 {
		length := int(uint32(buf[0])<<24 | uint32(buf[1])<<16 | uint32(buf[2])<<8 | uint32(buf[3]))
		if len(buf) < 8+length {
			break
		}
		var e entry
		if err := jsonUnmarshal(buf[8:8+length], &e); err != nil {
			t.Fatal(err)
		}
		bodies = append(bodies, string(e.R.Body))
		buf = buf[8+length:]
	}
	if len(bodies) != 1 || bodies[0] != "in-snapshot" {
		t.Fatalf("snapshot bodies = %v, want exactly [in-snapshot]", bodies)
	}
}

func jsonUnmarshal(b []byte, v any) error { return json.Unmarshal(b, v) }
