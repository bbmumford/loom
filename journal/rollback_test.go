/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package journal

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/bbmumford/loom/ports"
)

// COVERAGE of rollbackLocked — measured at 0.0% and reached from FOUR call
// sites (Append ×2, BatchAppend ×2), all of them error paths.
//
// 🛑 WHY IT MATTERS NOW: this journal was wired into every node's startup at
// #M-557. rollbackLocked is the mechanism behind the port's stated guarantee —
// "BatchAppend appends all records atomically (all-or-nothing)" and
// "watermarks are strictly monotonic". Until this test, that guarantee had
// never been executed once.
//
// 🔑 REACHING IT NEEDS A WRITE THAT FAILS WHILE Seek STILL WORKS. Closing the
// file does not do it: Append's first act is j.f.Seek, which then fails and
// returns BEFORE the rollback. A READ-ONLY handle is the shape that works —
// Seek succeeds, Write fails EBADF — and it is not a contrivance: a full disk
// or a revoked mount produces the same ordering.

// breakWrites swaps the journal's file for a read-only handle on the same
// path, and returns a func restoring a writable one.
func breakWrites(t *testing.T, j *FileJournal, path string) func() {
	t.Helper()
	writable := j.f
	ro, err := os.OpenFile(path, os.O_RDONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	j.mu.Lock()
	j.f = ro
	j.mu.Unlock()

	return func() {
		j.mu.Lock()
		j.f = writable
		j.mu.Unlock()
		ro.Close()
	}
}

func TestFailedAppendRollsBackAndLeavesNoWatermarkGap(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "loom-journal.log")
	j, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()

	rec := func(n string) ports.Record {
		return ports.Record{Topic: ports.Topic("fleet.peer"), NodeID: ports.NodeID(n)}
	}

	w1, err := j.Append(ctx, rec("node-a"))
	if err != nil {
		t.Fatal(err)
	}
	if w1 != 1 {
		t.Fatalf("first watermark = %d, want 1", w1)
	}

	restore := breakWrites(t, j, path)
	if _, err := j.Append(ctx, rec("node-b")); err == nil {
		restore()
		t.Fatal("an append to a read-only file SUCCEEDED — the failure this test " +
			"needs did not occur, so the rollback below is not being measured")
	}

	// Head must not have moved: the failed record is not durable.
	if h, err := j.Head(ctx); err != nil || h != w1 {
		t.Fatalf("Head = %d (err %v) after a failed append, want %d — a record "+
			"that never fsynced is being reported as durable", h, err, w1)
	}
	restore()

	// 🔑 THE PROPERTY: the next successful append must take watermark 2, NOT 3.
	// rollbackLocked resets next back to head; without it the failed attempt
	// would consume a watermark and leave a permanent hole in a sequence the
	// port promises is strictly monotonic and gap-free.
	w2, err := j.Append(ctx, rec("node-c"))
	if err != nil {
		t.Fatalf("append after rollback failed: %v", err)
	}
	if w2 != 2 {
		t.Fatalf("watermark after a rolled-back append = %d, want 2 — the failed "+
			"attempt consumed a watermark, so the sequence has a permanent gap "+
			"and any consumer resuming from it will wait forever for a record "+
			"that was never written", w2)
	}

	// And the file must contain exactly the two durable records — the staged
	// bytes of the failed attempt must not survive.
	ch, err := j.ReplayUntil(ctx, 0, w2, nil)
	if err != nil {
		t.Fatal(err)
	}
	var got []ports.NodeID
	for r := range ch {
		got = append(got, r.NodeID)
	}
	if len(got) != 2 || got[0] != "node-a" || got[1] != "node-c" {
		t.Fatalf("replay returned %v, want [node-a node-c] — a partially-staged "+
			"record survived the rollback and is now readable", got)
	}
}

// 🔴 THE ALL-OR-NOTHING GUARANTEE, WHICH IS THE PORT'S OWN WORDING.
//
// A batch that fails midway must leave NOTHING: not the records staged before
// the failure, and not their watermarks. A partial batch is worse than a
// failed one — it is a durable lie about what was accepted.
func TestFailedBatchAppendExposesNoPartialBatch(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	path := filepath.Join(dir, "loom-journal.log")
	j, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()

	rec := func(n string) ports.Record {
		return ports.Record{Topic: ports.Topic("fleet.peer"), NodeID: ports.NodeID(n)}
	}

	base, err := j.Append(ctx, rec("before"))
	if err != nil {
		t.Fatal(err)
	}

	restore := breakWrites(t, j, path)
	_, err = j.BatchAppend(ctx, []ports.Record{rec("b1"), rec("b2"), rec("b3")})
	if err == nil {
		restore()
		t.Fatal("BatchAppend to a read-only file SUCCEEDED — the partial-batch " +
			"case is not being measured")
	}
	restore()

	if h, herr := j.Head(ctx); herr != nil || h != base {
		t.Fatalf("Head = %d (err %v) after a failed batch, want %d", h, herr, base)
	}

	// The next append must be base+1: none of the three staged watermarks
	// may have been consumed.
	after, err := j.Append(ctx, rec("after"))
	if err != nil {
		t.Fatal(err)
	}
	if after != base+1 {
		t.Fatalf("watermark after a failed 3-record batch = %d, want %d — the "+
			"batch consumed %d watermarks it never made durable",
			after, base+1, after-base-1)
	}

	ch, err := j.ReplayUntil(ctx, 0, after, nil)
	if err != nil {
		t.Fatal(err)
	}
	var got []ports.NodeID
	for r := range ch {
		got = append(got, r.NodeID)
	}
	if len(got) != 2 || got[0] != "before" || got[1] != "after" {
		t.Fatalf("replay returned %v, want [before after] — a PARTIAL BATCH "+
			"survived, so the all-or-nothing guarantee is broken and consumers "+
			"can read records that were never acknowledged", got)
	}
}

// An empty batch is a no-op that must not disturb the watermark — the guard
// that returns j.head before any staging happens.
func TestEmptyBatchIsANoOp(t *testing.T) {
	ctx := context.Background()
	j, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()

	w, err := j.Append(ctx, ports.Record{
		Topic: ports.Topic("fleet.peer"), NodeID: ports.NodeID("n1"),
	})
	if err != nil {
		t.Fatal(err)
	}
	got, err := j.BatchAppend(ctx, nil)
	if err != nil {
		t.Fatalf("an empty batch errored: %v", err)
	}
	if got != w {
		t.Fatalf("empty BatchAppend returned %d, want the unchanged head %d", got, w)
	}
}

// 🔴🔴 THE TEST THE MUTATION DEMANDED — AND THE REASON IT IS SEPARATE IS THE
// FINDING.
//
// The two tests above reach rollbackLocked and were still GREEN with BOTH of
// its statements deleted. Not because they are weak assertions, but because
// they hand it NOTHING TO UNDO:
//
//	stageLocked sets `j.next = w` LAST, after both writes succeed.
//
// So a write failure never advances the watermark and never leaves staged
// bytes — the rollback runs and correctly does nothing. Coverage said 100%;
// discrimination was zero. (See feedback_a_mutation_result_needs_its_own_control:
// the mutants were EQUIVALENT for the paths those tests can reach.)
//
// The state where rollbackLocked is load-bearing is a PARTIAL batch: records
// 1..n-1 staged (next advanced, bytes written), record n fails. A read-only
// file cannot produce that — it fails the first write. So the mechanism is
// exercised directly here, with the partial state built explicitly.
func TestRollbackUndoesPartiallyStagedWork(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	j, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()

	rec := func(n string) ports.Record {
		return ports.Record{Topic: ports.Topic("fleet.peer"), NodeID: ports.NodeID(n)}
	}

	durable, err := j.Append(ctx, rec("durable"))
	if err != nil {
		t.Fatal(err)
	}

	// Build the partial-batch state by hand, exactly as BatchAppend does:
	// stage two records WITHOUT fsync, so next advances and bytes land.
	j.mu.Lock()
	startOff, err := j.f.Seek(0, io.SeekCurrent)
	if err != nil {
		j.mu.Unlock()
		t.Fatal(err)
	}
	if _, err := j.stageLocked(rec("staged-1")); err != nil {
		j.mu.Unlock()
		t.Fatal(err)
	}
	if _, err := j.stageLocked(rec("staged-2")); err != nil {
		j.mu.Unlock()
		t.Fatal(err)
	}

	// PREMISE — the whole point. Without this the assertions below are the
	// same no-op the tests above turned out to be.
	if j.next != durable+2 {
		j.mu.Unlock()
		t.Fatalf("premise wrong: next = %d after staging 2, want %d — nothing was "+
			"staged, so the rollback would have nothing to undo", j.next, durable+2)
	}
	endOff, _ := j.f.Seek(0, io.SeekCurrent)
	if endOff <= startOff {
		j.mu.Unlock()
		t.Fatalf("premise wrong: no bytes were staged (%d -> %d)", startOff, endOff)
	}

	j.rollbackLocked(startOff)

	gotNext, gotHead := j.next, j.head
	nowOff, _ := j.f.Seek(0, io.SeekCurrent)
	j.mu.Unlock()

	if gotNext != gotHead {
		t.Fatalf("after rollback next = %d, head = %d — the staged watermarks "+
			"were NOT released, so the next append skips them and the sequence "+
			"the port promises is gap-free has a permanent hole", gotNext, gotHead)
	}
	if nowOff != startOff {
		t.Fatalf("after rollback the file offset is %d, want %d — the write "+
			"position was not rewound", nowOff, startOff)
	}

	// 🔑 SIZE, NOT OFFSET, IS WHAT DETECTS A MISSING TRUNCATE — and my first
	// version of this test checked only the offset, which Seek restores
	// whether or not the truncate ran. Deleting the Truncate left it GREEN.
	//
	// The leftover matters on RESTART: Open runs scanLastValid over the file,
	// so staged-but-never-fsynced bytes past the durable end are exactly what
	// it may resurrect as records nobody acknowledged.
	fi, statErr := os.Stat(filepath.Join(dir, "loom-journal.log"))
	if statErr != nil {
		t.Fatal(statErr)
	}
	if fi.Size() != startOff {
		t.Fatalf("after rollback the file is %d bytes, want %d — %d bytes of "+
			"rolled-back records are still on disk, and Open's scanLastValid "+
			"can recover them as durable on the next restart",
			fi.Size(), startOff, fi.Size()-startOff)
	}

	// End to end: the next real append takes the released watermark, and only
	// the durable record plus it are readable.
	next, err := j.Append(ctx, rec("after"))
	if err != nil {
		t.Fatal(err)
	}
	if next != durable+1 {
		t.Fatalf("watermark after rollback = %d, want %d", next, durable+1)
	}
	ch, err := j.ReplayUntil(ctx, 0, next, nil)
	if err != nil {
		t.Fatal(err)
	}
	var got []ports.NodeID
	for r := range ch {
		got = append(got, r.NodeID)
	}
	if len(got) != 2 || got[0] != "durable" || got[1] != "after" {
		t.Fatalf("replay returned %v, want [durable after] — rolled-back records "+
			"are readable", got)
	}
}

// 🔴 CONVERTING A CLAIM I MADE IN A COMMENT INTO EVIDENCE.
//
// TestRollbackUndoesPartiallyStagedWork asserts that leftover staged bytes
// are "what Open's scanLastValid can recover as durable on the next restart".
// I wrote that as a hazard and did not test it — so here it is, measured.
//
// 🔑 THE MECHANISM: stageLocked writes a STRUCTURALLY VALID entry — correct
// length header, correct CRC32, valid JSON — and the ONLY thing that has not
// happened is the fsync. scanLastValid validates exactly those three things.
// ⇒ ***CRC encodes INTEGRITY, never DURABILITY. A staged-but-never-fsynced
// entry is indistinguishable, on disk, from an acknowledged one.***
//
// That is precisely why rollbackLocked's Truncate is load-bearing rather than
// tidy-up, and why the "no truncate" mutant had to be killed with a SIZE
// assertion (#M-561 ④).
func TestUnfsyncedStagedBytesAreResurrectedOnReopenWithoutRollback(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	j, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	rec := func(n string) ports.Record {
		return ports.Record{Topic: ports.Topic("fleet.peer"), NodeID: ports.NodeID(n)}
	}

	durable, err := j.Append(ctx, rec("acknowledged"))
	if err != nil {
		t.Fatal(err)
	}

	// Stage two records and DO NOT fsync, DO NOT roll back — the state a
	// process crash between staging and fsync leaves behind.
	j.mu.Lock()
	if _, err := j.stageLocked(rec("never-acked-1")); err != nil {
		j.mu.Unlock()
		t.Fatal(err)
	}
	if _, err := j.stageLocked(rec("never-acked-2")); err != nil {
		j.mu.Unlock()
		t.Fatal(err)
	}
	// PREMISE: head still reports only the acknowledged record — these two
	// were never made durable by the journal's own accounting.
	if j.head != durable {
		j.mu.Unlock()
		t.Fatalf("premise wrong: head = %d after staging, want %d (staging must "+
			"not advance head)", j.head, durable)
	}
	j.mu.Unlock()
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}

	// Restart.
	j2, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer j2.Close()

	head2, err := j2.Head(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// 🛑 THE MEASUREMENT. If head2 > durable, the reopened journal has adopted
	// records the writer never acknowledged.
	if head2 == durable {
		t.Fatalf("head after restart = %d, same as the acknowledged head — the "+
			"unfsynced entries were REJECTED. That is stronger than the comment "+
			"in TestRollbackUndoesPartiallyStagedWork claims, and that comment "+
			"should be corrected to say so", head2)
	}
	if head2 != durable+2 {
		t.Fatalf("head after restart = %d, want %d", head2, durable+2)
	}

	// And they are readable — not merely counted.
	ch, err := j2.ReplayUntil(ctx, 0, head2, nil)
	if err != nil {
		t.Fatal(err)
	}
	var got []ports.NodeID
	for r := range ch {
		got = append(got, r.NodeID)
	}
	if len(got) != 3 {
		t.Fatalf("replay after restart returned %d records, want 3: %v", len(got), got)
	}
	t.Logf("MEASURED: a restart adopted %d never-fsynced records as durable (%v) "+
		"— CRC validates integrity, not durability, so rollbackLocked's Truncate "+
		"is the only thing standing between a rolled-back batch and resurrection",
		len(got)-1, got)
}
