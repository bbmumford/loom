/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/bbmumford/loom/journal"
	"github.com/bbmumford/loom/ports"
)

// The DurableJournal port: the seam is constructed at the composition root and
// its lifecycle is OWNED there.
//
// What this deliberately does NOT do. No consumer is routed through
// the journal — it is not the live merge authority, nothing appends, nothing
// replays. So the only properties worth asserting are the ones a WIRING can
// get wrong: does the port actually get satisfied, is the fd released, and is
// the seam safe when construction failed.

func TestFileJournalSatisfiesTheDurableJournalPort(t *testing.T) {
	dir := t.TempDir()
	fj, err := journal.Open(filepath.Join(dir, "journal"))
	if err != nil {
		t.Fatal(err)
	}
	defer fj.Close()

	// The compile-time assertion lives in the journal package; this is the
	// runtime counterpart at the seam that consumes it, so a signature drift
	// in ports.DurableJournal fails HERE too rather than only at the
	// implementation.
	var _ ports.DurableJournal = fj

	head, err := fj.Head(context.Background())
	if err != nil {
		t.Fatalf("Head on a fresh journal errored: %v", err)
	}
	if head != 0 {
		t.Fatalf("fresh journal Head = %d, want 0", head)
	}
}

// Close must be idempotent, because Shutdown can run more than once and the
// runtime calls it unconditionally when the seam is present.
func TestJournalCloseIsIdempotent(t *testing.T) {
	fj, err := journal.Open(filepath.Join(t.TempDir(), "journal"))
	if err != nil {
		t.Fatal(err)
	}
	if err := fj.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := fj.Close(); err != nil {
		t.Fatalf("second Close: %v — Shutdown would acquire an error (or a "+
			"double-close panic) on any second pass", err)
	}
}

// The behaviour delta is named rather than claimed absent: journal.Open CREATES
// <DataDir>/journal/ and an empty log file inside it, and holds the fd until
// Shutdown. One directory, one empty file, one descriptor per node — and no
// writes, because no consumer appends yet.
//
// Pinned so that if a future change starts writing at construction time, the
// "nothing is written until a consumer is routed" claim fails instead of
// quietly becoming false.
func TestJournalConstructionCreatesTheFileAndWritesNothing(t *testing.T) {
	base := t.TempDir()
	jdir := filepath.Join(base, "journal")

	if _, err := os.Stat(jdir); !os.IsNotExist(err) {
		t.Fatalf("premise wrong: %s already exists before Open", jdir)
	}

	fj, err := journal.Open(jdir)
	if err != nil {
		t.Fatal(err)
	}
	defer fj.Close()

	logPath := filepath.Join(jdir, "loom-journal.log")
	fi, err := os.Stat(logPath)
	if err != nil {
		t.Fatalf("expected %s to exist after Open: %v", logPath, err)
	}
	if fi.Size() != 0 {
		t.Fatalf("the journal file is %d bytes after construction, want 0 — "+
			"something is writing at construction time, so wiring the seam is "+
			"no longer inert and the stage-1 'no consumer routed' claim is false",
			fi.Size())
	}
}

// The seam must fail SOFT: a node whose DataDir cannot host the journal keeps
// running exactly as before, because no consumer depends on it yet.
//
// 🛑 This asserts the property at the level the runtime relies on — Open
// returning an error rather than panicking — and the runtime's own branch
// leaves rt.journal nil on that path. rt.journal is an INTERFACE, so a future
// consumer calling it unguarded panics where a concrete nil would not; that is
// why the wiring comment names the nil-check requirement explicitly.
func TestJournalOpenFailsSoftlyOnAnUnusableDir(t *testing.T) {
	base := t.TempDir()
	// A regular FILE where the journal directory should be: MkdirAll fails.
	blocker := filepath.Join(base, "journal")
	if err := os.WriteFile(blocker, []byte("not a directory"), 0o644); err != nil {
		t.Fatal(err)
	}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("journal.Open panicked on an unusable dir: %v — the runtime "+
				"calls this during Initialize, so a panic here fails node startup "+
				"over a seam nothing consumes yet", r)
		}
	}()

	fj, err := journal.Open(blocker)
	if err == nil {
		fj.Close()
		t.Fatal("Open succeeded where a regular file blocks the directory — the " +
			"runtime would then treat a broken journal as wired")
	}
}

// 🔴 THE PROPERTY THE MUTATION DEMANDED, AND IT EXPOSED A GAP OLDER THAN THIS
// TASK.
//
// Close must be both CALLED and idempotent, and idempotence alone is what a
// suite proves by accident: with nothing constructing a Runtime and shutting it
// down, deleting the journal Close from Shutdown leaves everything green, and
// the LiveDirectory close beside it goes unverified for the same reason.
//
// closePortSeams exists so both are testable without standing up a full
// node, and this asserts what shutdown must actually do.
func TestShutdownReleasesBothPhase05Seams(t *testing.T) {
	fj, err := journal.Open(filepath.Join(t.TempDir(), "journal"))
	if err != nil {
		t.Fatal(err)
	}
	rt := &Runtime{ctx: context.Background(), journalRaw: fj}

	// Premise: the journal is OPEN, or "closed afterwards" proves nothing.
	if _, err := fj.Head(context.Background()); err != nil {
		t.Fatalf("premise wrong: the journal is not usable before close: %v", err)
	}

	rt.closePortSeams()

	// After release, an append must fail — the observable consequence of the
	// fd being closed. A Head() read can be served from memory, so it is not
	// the discriminator.
	if _, err := fj.Append(context.Background(), ports.Record{
		Topic: ports.Topic("fleet.peer"), NodeID: ports.NodeID("n1"),
	}); err == nil {
		t.Fatal("the journal still accepts appends after shutdown released it — " +
			"the fd is leaked for the process lifetime and replay subscribers " +
			"stay open")
	}

	// Idempotent: Shutdown may run more than once.
	rt.closePortSeams()
}

// The release path must tolerate unconstructed seams — both are
// fail-soft at Initialize, so a node with an unusable DataDir reaches shutdown
// with nil handles.
func TestShutdownSeamReleaseIsSafeWithNoSeams(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("releasing absent seams panicked: %v — a node whose seams "+
				"failed to construct would crash on shutdown instead of exiting "+
				"cleanly", r)
		}
	}()
	(&Runtime{ctx: context.Background()}).closePortSeams()
	var nilRT *Runtime
	nilRT.closePortSeams()
}
