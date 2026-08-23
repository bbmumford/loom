/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package journal

import (
	"context"
	"encoding/binary"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
)

// A journal written by the PREVIOUS loom must replay correctly under this one.
//
// This is the gate-1 risk and it is not hypothetical: the seven-gate unfreeze
// order (#R-1396 ③) publishes loom FIRST, so the very first thing the new
// binary does in production is open a journal the OLD binary wrote. Adding
// Record.Key changed the on-disk JSON shape; if a pre-Key entry failed to
// decode, Open() would treat it as a torn payload and TRUNCATE the journal at
// that offset — silently discarding durable history at the moment of upgrade.
//
// The payloads below are written byte-for-byte as the current published loom
// writes them: length-prefixed, CRC32'd JSON with NO "Key" member at all.
func TestReplaysJournalWrittenBeforeKeyExisted(t *testing.T) {
	dir := t.TempDir()

	// Exactly the pre-Key encoding: entry{W,R} where R has no "Key" member.
	legacy := []string{
		`{"w":1,"r":{"Topic":"fleet.peer","NodeID":"nodeA","HLC":655360,"Lamport":0,` +
			`"Tombstone":false,"TombstoneReason":"","ExpiresAtUnixMs":0,` +
			`"Body":"djE=","AuthorPubKey":"cHVi","Signature":"c2ln","Observer":null,"BlobCID":""}}`,
		`{"w":2,"r":{"Topic":"fleet.peer","NodeID":"nodeB","HLC":720896,"Lamport":0,` +
			`"Tombstone":false,"TombstoneReason":"","ExpiresAtUnixMs":0,` +
			`"Body":"djI=","AuthorPubKey":"cHVi","Signature":"c2ln","Observer":null,"BlobCID":""}}`,
	}

	f, err := os.Create(filepath.Join(dir, "loom-journal.log"))
	if err != nil {
		t.Fatal(err)
	}
	for _, payload := range legacy {
		var hdr [8]byte
		binary.BigEndian.PutUint32(hdr[:4], uint32(len(payload)))
		binary.BigEndian.PutUint32(hdr[4:], crc32.ChecksumIEEE([]byte(payload)))
		if _, err := f.Write(hdr[:]); err != nil {
			t.Fatal(err)
		}
		if _, err := f.WriteString(payload); err != nil {
			t.Fatal(err)
		}
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	j, err := Open(dir)
	if err != nil {
		t.Fatalf("Open refused a journal written by the previous loom: %v", err)
	}
	defer j.Close()
	ctx := context.Background()

	head, err := j.Head(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if head != 2 {
		t.Fatalf("head = %d, want 2 — the legacy journal was TRUNCATED on open, "+
			"which discards durable history at upgrade", head)
	}

	ch, err := j.ReplayUntil(ctx, 0, head, nil)
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]string{}
	for r := range ch {
		if r.Key != "" {
			t.Errorf("legacy record for %s decoded with Key=%q, want \"\" — a record "+
				"written before the field existed must land in the classical slot",
				r.NodeID, r.Key)
		}
		got[string(r.NodeID)] = string(r.Body)
	}
	if len(got) != 2 || got["nodeA"] != "v1" || got["nodeB"] != "v2" {
		t.Fatalf("legacy replay = %v, want nodeA=v1 nodeB=v2", got)
	}

	// And the upgraded journal must keep working: a new keyed append alongside
	// the legacy entries, both readable.
	if _, err := j.Append(ctx, keyedRec("fleet.latency", "nodeA", "peer-1", 30, "rtt")); err != nil {
		t.Fatal(err)
	}
	head2, err := j.Head(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if head2 != 3 {
		t.Fatalf("head after append = %d, want 3", head2)
	}
	ch2, err := j.ReplayUntil(ctx, 0, head2, nil)
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for range ch2 {
		n++
	}
	if n != 3 {
		t.Fatalf("replay after upgrade yielded %d records, want 3 (2 legacy + 1 keyed)", n)
	}
}
