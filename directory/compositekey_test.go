/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package directory

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"testing"

	"github.com/bbmumford/loom/journal"
	"github.com/bbmumford/loom/ports"
)

// The raw-slot view is keyed by (NodeID, Key). Keyed by NodeID alone, every
// record a node holds on a topic beyond the first is overwritten
// last-writer-wins — silently, with no error and no counter.
func TestProjectionKeepsEveryKeyedSlotOfANode(t *testing.T) {
	dir := t.TempDir()
	j, err := journal.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	ctx := context.Background()

	d, err := NewSwarmDirectory(ctx, j, nil)
	if err != nil {
		t.Fatal(err)
	}

	const topic = ports.Topic("fleet.content")
	for i, key := range []string{"blob-a", "blob-b", "blob-c"} {
		r := ports.Record{
			Topic:  topic,
			NodeID: ports.NodeID("nodeA"),
			Key:    key,
			HLC:    ports.HLC(uint64(10+i) << 16),
			Body:   []byte(key),
		}
		if err := d.Ingest(ctx, r); err != nil {
			t.Fatal(err)
		}
	}

	recs, err := d.RecordsByTopic(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 3 {
		t.Fatalf("projection holds %d slots for one node, want 3 — its keyed "+
			"records are overwriting each other", len(recs))
	}
	// Canonical order is (NodeID, Key), so equal NodeIDs order by Key.
	for i, want := range []string{"blob-a", "blob-b", "blob-c"} {
		if recs[i].Key != want {
			t.Fatalf("RecordsByTopic[%d].Key = %q, want %q — order is not canonical",
				i, recs[i].Key, want)
		}
	}
}

// Fingerprint hashes the record sequence, so that sequence must be stable.
// With several slots per node, ordering by NodeID alone leaves their relative
// order to Go's map iteration and the fingerprint flaps — which would make
// shadow parity (§0.5.3 stage 3) fail at random, or mask a real divergence.
func TestFingerprintStableAcrossManyKeysOfOneNode(t *testing.T) {
	j, err := journal.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	ctx := context.Background()

	d, err := NewSwarmDirectory(ctx, j, nil)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 40; i++ {
		r := ports.Record{
			Topic:  ports.Topic("fleet.content"),
			NodeID: ports.NodeID("nodeA"),
			Key:    string(rune('a'+i%26)) + string(rune('0'+i/26)),
			HLC:    ports.HLC(uint64(10+i) << 16),
			Body:   []byte("v"),
		}
		if err := d.Ingest(ctx, r); err != nil {
			t.Fatal(err)
		}
	}

	first, err := d.Fingerprint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 25; i++ {
		got, err := d.Fingerprint(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if got != first {
			t.Fatalf("fingerprint is not stable: call %d gave %x, first gave %x — "+
				"the record order depends on map iteration", i+2, got, first)
		}
	}
}

// The fingerprint is the shadow gate's cheap comparator (§0.5.3 stage 3:
// "shadow mismatches are observable and fail the phase gate"). It hashes a
// tuple per record, so that tuple must contain every field that makes two
// accepted sets DIFFERENT. Composite keys added such a field. Two directories
// holding genuinely different content — one node's "blob-alpha" versus its
// "blob-beta" — are otherwise identical in (topic, node, hlc, signature), so
// a tuple blind to Key reports PARITY on divergent state, which is the one
// failure mode the shadow phase exists to prevent.
func TestFingerprintDistinguishesDirectoriesDifferingOnlyByKey(t *testing.T) {
	ctx := context.Background()

	build := func(key string) [32]byte {
		j, err := journal.Open(t.TempDir())
		if err != nil {
			t.Fatal(err)
		}
		defer j.Close()
		d, err := NewSwarmDirectory(ctx, j, nil)
		if err != nil {
			t.Fatal(err)
		}
		r := ports.Record{
			Topic:     ports.Topic("fleet.content"),
			NodeID:    ports.NodeID("nodeA"),
			Key:       key,
			HLC:       ports.HLC(10 << 16),
			Body:      []byte("same-body"),
			Signature: []byte("same-signature"),
		}
		if err := d.Ingest(ctx, r); err != nil {
			t.Fatal(err)
		}
		fp, err := d.Fingerprint(ctx)
		if err != nil {
			t.Fatal(err)
		}
		return fp
	}

	if build("blob-alpha") == build("blob-beta") {
		t.Fatal("two directories holding DIFFERENT keyed records fingerprint " +
			"identically — the shadow comparator would report parity on " +
			"divergent state")
	}
}

// The counterpart, and the reason the Key contribution is conditional: every
// record in the fleet today is keyless, and their fingerprint must not move.
// A fingerprint that changed shape for existing records would make every
// live directory disagree with every stored anchor input at once.
func TestKeylessFingerprintIsTheLegacyTuple(t *testing.T) {
	recs := []ports.Record{
		{Topic: "fleet.peers", NodeID: "nodeA", HLC: ports.HLC(10 << 16), Signature: []byte("sig-a")},
		{Topic: "fleet.peers", NodeID: "nodeB", HLC: ports.HLC(11 << 16), Signature: []byte("sig-b")},
	}

	// Independently written legacy oracle: the exact tuple shipped before
	// composite keys existed. Compared against the implementation, not
	// derived from it.
	legacy := func(rs []ports.Record) [32]byte {
		h := sha256.New()
		for _, r := range rs {
			th := sha256.Sum256([]byte(r.Topic))
			h.Write(th[:])
			nh := sha256.Sum256([]byte(r.NodeID))
			h.Write(nh[:])
			var hlcb [8]byte
			binary.BigEndian.PutUint64(hlcb[:], uint64(r.HLC))
			h.Write(hlcb[:])
			sh := sha256.Sum256(r.Signature)
			h.Write(sh[:])
		}
		var out [32]byte
		copy(out[:], h.Sum(nil))
		return out
	}

	if fingerprintRecords(recs) != legacy(recs) {
		t.Fatal("keyless records no longer fingerprint to the pre-composite-key " +
			"tuple — every existing directory and anchor input just diverged")
	}
}

// Keyless records must project exactly as before: one slot per node, later
// HLC replacing earlier. Every record in the fleet today is this shape.
func TestProjectionUnchangedForKeylessRecords(t *testing.T) {
	j, err := journal.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer j.Close()
	ctx := context.Background()

	d, err := NewSwarmDirectory(ctx, j, nil)
	if err != nil {
		t.Fatal(err)
	}
	const topic = ports.Topic("fleet.keyops")
	for i, body := range []string{"v1", "v2"} {
		r := ports.Record{
			Topic:  topic,
			NodeID: ports.NodeID("nodeA"),
			HLC:    ports.HLC(uint64(10+i) << 16),
			Body:   []byte(body),
		}
		if err := d.Ingest(ctx, r); err != nil {
			t.Fatal(err)
		}
	}
	recs, err := d.RecordsByTopic(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != 1 {
		t.Fatalf("keyless projection holds %d slots for one node, want 1", len(recs))
	}
	if string(recs[0].Body) != "v2" {
		t.Fatalf("keyless slot body = %q, want the newer %q", recs[0].Body, "v2")
	}
}

// opaqueDirectory is a LiveDirectory that is not a *SwarmDirectory. Embedding
// the INTERFACE (not the concrete type) forwards every port method without
// promoting the optional handler-enumeration capability — which is exactly
// the shape of any second implementation, including the LAD adapter the
// shadow phase compares against.
type opaqueDirectory struct{ ports.LiveDirectory }

// A phase gate must not pass on a comparison it never performed. The handler
// axis is enumerated from the sides themselves, so a side that cannot
// enumerate silently contributes nothing — and since the shadow phase exists
// to compare the Swarm directory against a DIFFERENT implementation, that is
// the normal case, not an edge case. InParity() reporting true there means
// the gate reports "handlers agree" having compared zero handlers.
func TestComparisonThatCouldNotEnumerateHandlersIsNotParity(t *testing.T) {
	ctx := context.Background()
	idA, pubA, _ := testIdentity(t)

	auth := newTestDirectory(t, nil)
	shadowInner := newTestDirectory(t, nil)
	rec := peerRecord(t, idA, pubA, 100<<16, []string{"auth"}, "svc-a")
	if err := auth.Ingest(ctx, rec); err != nil {
		t.Fatal(err)
	}
	if err := shadowInner.Ingest(ctx, rec); err != nil {
		t.Fatal(err)
	}

	// Control: both sides enumerable, identical state => real parity.
	ctl, err := CompareDirectories(ctx, auth, shadowInner, testTenantID, []string{"auth"})
	if err != nil {
		t.Fatal(err)
	}
	if !ctl.InParity() {
		t.Fatalf("control must be in parity, got %v", ctl.Mismatches)
	}
	if ctl.ComparedHandlers == 0 {
		t.Fatal("control compared zero handlers — the positive case is vacuous")
	}

	// Same state, but the shadow side cannot enumerate its handler index.
	rep, err := CompareDirectories(ctx, auth, opaqueDirectory{shadowInner}, testTenantID, []string{"auth"})
	if err != nil {
		t.Fatal(err)
	}
	if rep.InParity() {
		t.Fatal("a comparison whose handler axis could not be enumerated on one " +
			"side reports parity — the phase gate passes on an unperformed check")
	}
}

// Tenant scoping is a REAL filter, not a decorative parameter (#R-1455 ③).
//
// Before the port went tenant-explicit, SwarmDirectory.Members returned every
// tenant's members to every caller — a cross-tenant read the tenant-implicit
// signature could not even express, so nothing could have caught it. Adding
// the parameter without enforcing it would be worse than leaving it out: the
// signature would promise isolation the code does not provide.
func TestTenantScopingActuallyFilters(t *testing.T) {
	ctx := context.Background()
	d := newTestDirectory(t, nil)

	idA, pubA, _ := testIdentity(t)
	idB, pubB, _ := testIdentity(t)

	// peerRecord tags tenant=hstles; build a second node on another tenant.
	if err := d.Ingest(ctx, peerRecord(t, idA, pubA, 100<<16, []string{"auth"}, "svc-a")); err != nil {
		t.Fatal(err)
	}
	other := peerRecord(t, idB, pubB, 100<<16, []string{"auth"}, "svc-b")
	other.Body = peerBodyWithTenant(t, pubB, []string{"auth"}, "svc-b", "other-tenant")
	if err := d.Ingest(ctx, other); err != nil {
		t.Fatal(err)
	}

	ms, err := d.Members(ctx, testTenantID)
	if err != nil {
		t.Fatal(err)
	}
	if len(ms) != 1 || ms[0].NodeID != idA {
		t.Fatalf("Members(%q) = %d entries %v — want exactly the one node on that "+
			"tenant; the parameter is not being enforced", testTenantID, len(ms), ms)
	}

	// Control: the other tenant's node IS present, so the 1 above is a filter
	// result and not an ingest failure.
	os, err := d.Members(ctx, ports.Tenant("other-tenant"))
	if err != nil {
		t.Fatal(err)
	}
	if len(os) != 1 || os[0].NodeID != idB {
		t.Fatalf("control: Members(\"other-tenant\") = %v, want the second node — "+
			"the first assertion may have passed because ingest dropped it", os)
	}

	// Role and reach indexes must scope too, or the isolation is partial.
	if n, _ := d.NodesByRole(ctx, testTenantID, "auth"); len(n) != 1 || n[0] != idA {
		t.Fatalf("NodesByRole leaked across tenants: %v", n)
	}
	if r, _ := d.Reach(ctx, testTenantID, idB); len(r) != 0 {
		t.Fatalf("Reach returned another tenant's addresses: %v", r)
	}
}
