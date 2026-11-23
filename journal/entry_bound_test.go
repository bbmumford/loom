/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
 */

package journal

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

// corruptTail opens a journal holding one record, then appends to its file a header whose length
// passes the bound Open applies, followed by no payload: an entry the journal never wrote, as a
// damaged file holds after Open's scan.
func corruptTail(t *testing.T) (*FileJournal, string) {
	t.Helper()
	dir := t.TempDir()
	j, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { j.Close() })
	if _, err := j.Append(context.Background(), rec("t.a", "n1", 100, "one", false)); err != nil {
		t.Fatal(err)
	}
	matches, err := filepath.Glob(filepath.Join(dir, "*"))
	if err != nil || len(matches) != 1 {
		t.Fatalf("journal files %v, %v; want one", matches, err)
	}
	f, err := os.OpenFile(matches[0], os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	var hdr [8]byte
	binary.BigEndian.PutUint32(hdr[:4], 128<<20)
	if _, err := f.Write(hdr[:]); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return j, matches[0]
}

// allocatedBy reports the bytes the heap allocated while run ran.
func allocatedBy(run func()) uint64 {
	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	run()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// Every reader of the journal file refuses an entry length past the bound Open applies before
// allocating for it: a damaged length costs no more than the bytes around it, on the history
// replay, the bounded replay and the compaction scan alike, and each still answers the entry
// before it.
func TestEveryJournalReaderBoundsAnEntryLength(t *testing.T) {
	ctx := context.Background()
	j, _ := corruptTail(t)
	const limit = 16 << 20

	var got []string
	if n := allocatedBy(func() {
		cctx, cancel := context.WithCancel(ctx)
		ch, err := j.Replay(cctx, 0, nil)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range drain(t, ch, 1) {
			got = append(got, string(r.Body))
		}
		// The history pass reads on past the record it answered; give it the time to reach the
		// damaged length before the measurement ends.
		time.Sleep(300 * time.Millisecond)
		cancel()
	}); n > limit {
		t.Errorf("Replay over a damaged length allocated %d bytes", n)
	}
	if n := allocatedBy(func() {
		ch, err := j.ReplayUntil(ctx, 0, 1, nil)
		if err != nil {
			t.Fatal(err)
		}
		for r := range ch {
			got = append(got, string(r.Body))
		}
	}); n > limit {
		t.Errorf("ReplayUntil over a damaged length allocated %d bytes", n)
	}
	if n := allocatedBy(func() {
		if err := j.Compact(ctx, 0); err != nil {
			t.Fatalf("Compact: %v", err)
		}
	}); n > limit {
		t.Errorf("Compact over a damaged length allocated %d bytes", n)
	}
	if want := []string{"one", "one"}; len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Errorf("replays answered %q, want %q", got, want)
	}
}

// An entry is written only when every reader will read it back: a record whose entry passes the
// bound Open applies is refused, so a journal reopened after it holds every record Append accepted.
func TestAppendRefusesAnEntryNoReaderWouldRead(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	j, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := j.Append(ctx, rec("t.a", "n1", 100, "one", false)); err != nil {
		t.Fatal(err)
	}
	big := rec("t.a", "n1", 101, "", false)
	big.Body = bytes.Repeat([]byte{'x'}, 49<<20) // base64 in the entry's JSON: past 64 MiB
	_, bigErr := j.Append(ctx, big)
	if _, err := j.Append(ctx, rec("t.a", "n1", 102, "after", false)); err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	if bigErr == nil {
		t.Error("Append accepted an entry past the bound every reader applies")
	}
	re, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer re.Close()
	ch, err := re.ReplayUntil(ctx, 0, 100, nil)
	if err != nil {
		t.Fatal(err)
	}
	var bodies []string
	for r := range ch {
		if len(r.Body) < 64 {
			bodies = append(bodies, string(r.Body))
		} else {
			bodies = append(bodies, "<big>")
		}
	}
	if len(bodies) != 2 || bodies[0] != "one" || bodies[1] != "after" {
		t.Fatalf("reopened journal holds %q, want the two accepted records [one after]", bodies)
	}
}
