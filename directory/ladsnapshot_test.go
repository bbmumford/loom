/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package directory

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	lad "github.com/bbmumford/ledger"
	ladcache "github.com/bbmumford/ledger/cache"

	"github.com/bbmumford/loom/ports"
)

// 🛑 THE PHASE-0.5 GATE RUNS THESE, AND THEY WERE AT 0.0% (#M-554 ⑤).
//
// Snapshot/Fingerprint are the deterministic-snapshot primitives
// CompareFingerprints uses to decide whether the shadow directory may take
// authority. An untested Fingerprint does not fail the gate — it PASSES it,
// which is the direction that matters: two directories holding different
// content can hash identically and be certified in parity.
//
// ⚠ SCOPE, CORRECTED AFTER MEASURING RATHER THAN LEFT AS WRITTEN. I claimed
// at #M-554 ⑤ that this would pin the #M-483 property (fingerprintRecords
// being blind to Record.Key). It does not, for two measured reasons:
//
//  1. lad.Record has NO Key field — on this side the key is DERIVED in r2p
//     from the LAD topic, so "two records differing only in Key" is not a
//     state the LAD path can even be put into directly; and
//  2. that property is ALREADY pinned, on the Swarm side, by
//     TestFingerprintDistinguishesDirectoriesDifferingOnlyByKey.
//
// What is genuinely untested here is the LAD-side DERIVATION, covered below.

func ladSnapshotCache(t *testing.T) (*ladcache.DirectoryCache, *LADDirectory) {
	t.Helper()
	c := ladcache.NewDirectoryCache()
	d, err := NewLADDirectory(c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { d.Close() })
	return c, d
}

func applyMember(t *testing.T, c *ladcache.DirectoryCache, nodeID string, ts time.Time) {
	t.Helper()
	body, err := json.Marshal(lad.MemberRecord{
		NodeID: nodeID, CreatedAt: ts,
		Attrs: map[string]string{"serviceName": "svc"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Apply(lad.Record{
		Topic: lad.TopicMember, NodeID: nodeID, Body: body, Timestamp: ts,
	}); err != nil {
		t.Fatal(err)
	}
}

func TestLADSnapshotIsDeterministicAndCarriesAWatermark(t *testing.T) {
	ctx := context.Background()
	c, d := ladSnapshotCache(t)
	ts := time.Now().UTC()
	applyMember(t, c, "node-a", ts)
	applyMember(t, c, "node-b", ts.Add(time.Second))

	snap, err := d.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(snap.Records) == 0 {
		t.Fatal("snapshot carried no records — every assertion below would be vacuous")
	}
	if snap.Watermark == 0 {
		t.Fatal("snapshot watermark is 0 with records present — a resume from " +
			"this snapshot would replay from the beginning")
	}

	// Determinism: the same cache must produce a byte-identical fingerprint and
	// the same record order. A gate that compares fingerprints is worthless if
	// repeated calls disagree.
	snap2, err := d.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Fingerprint != snap2.Fingerprint {
		t.Fatal("two snapshots of an unchanged directory produced different " +
			"fingerprints — the parity gate would report divergence at random")
	}
	if len(snap.Records) != len(snap2.Records) {
		t.Fatalf("record count changed between snapshots: %d vs %d",
			len(snap.Records), len(snap2.Records))
	}
	for i := range snap.Records {
		if snap.Records[i].Topic != snap2.Records[i].Topic ||
			snap.Records[i].NodeID != snap2.Records[i].NodeID {
			t.Fatalf("record order is not canonical at %d: %v/%v vs %v/%v", i,
				snap.Records[i].Topic, snap.Records[i].NodeID,
				snap2.Records[i].Topic, snap2.Records[i].NodeID)
		}
	}

	// Fingerprint() must agree with Snapshot().Fingerprint — they are separate
	// entry points over the same allRecords, and the gate uses both.
	fp, err := d.Fingerprint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if fp != snap.Fingerprint {
		t.Fatal("Fingerprint() and Snapshot().Fingerprint disagree on the same " +
			"directory — CompareFingerprints and the snapshot path would reach " +
			"opposite conclusions")
	}
}

// 🔴 THE LAD-SIDE COMPOSITE-KEY MECHANISM, WHICH IS NOT THE SWARM ONE.
//
// On the Swarm side a record carries its own Key, and
// TestFingerprintDistinguishesDirectoriesDifferingOnlyByKey (compositekey_test.go)
// already pins that. The LAD side is different and untested: THREE distinct LAD
// topics — member, role, reach — collapse onto the SINGLE ports topic
// FleetPeerTopic, distinguished only by a key DERIVED in r2p (ladKeyMember /
// ladKeyRole / ladKeyReach).
//
// ⇒ If that derivation ever collapsed, all three would occupy one slot for a
// node: two of the three would vanish from the snapshot, the fingerprint would
// agree with a directory genuinely missing them, and the gate would certify it.
func TestLADDerivedKeysKeepMemberRoleAndReachInSeparateSlots(t *testing.T) {
	ctx := context.Background()
	c, d := ladSnapshotCache(t)
	ts := time.Unix(1750000000, 0).UTC()
	const node = "node-a"

	applyMember(t, c, node, ts)

	roleBody, err := json.Marshal(lad.RoleRecord{NodeID: node, Roles: []string{"auth"}, Updated: ts})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Apply(lad.Record{Topic: lad.TopicRole, NodeID: node, Body: roleBody, Timestamp: ts}); err != nil {
		t.Fatal(err)
	}
	reachBody, err := json.Marshal(lad.ReachRecord{
		NodeID: node, Addresses: []lad.ReachAddress{{Host: "203.0.113.7", Port: 443, Proto: "wss"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Apply(lad.Record{Topic: lad.TopicReach, NodeID: node, Body: reachBody, Timestamp: ts}); err != nil {
		t.Fatal(err)
	}

	snap, err := d.Snapshot(ctx)
	if err != nil {
		t.Fatal(err)
	}

	keys := map[string]int{}
	for _, r := range snap.Records {
		if r.NodeID == ports.NodeID(node) && r.Topic == FleetPeerTopic {
			keys[r.Key]++
		}
	}
	if len(keys) != 3 {
		t.Fatalf("one node's member/role/reach produced %d distinct keys, want 3: "+
			"%v — the three collapsed into fewer slots, so records are being "+
			"silently dropped from the snapshot the parity gate hashes", len(keys), keys)
	}
	for k, n := range keys {
		if n != 1 {
			t.Errorf("key %q appears %d times for one node, want 1", k, n)
		}
	}
}

// An empty directory must fingerprint deterministically rather than error —
// the gate runs before any records exist.
func TestLADFingerprintOnAnEmptyDirectoryIsStable(t *testing.T) {
	ctx := context.Background()
	_, d := ladSnapshotCache(t)

	fp1, err := d.Fingerprint(ctx)
	if err != nil {
		t.Fatalf("Fingerprint on an empty directory errored: %v", err)
	}
	fp2, err := d.Fingerprint(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if fp1 != fp2 {
		t.Fatal("an empty directory fingerprinted differently twice")
	}

	snap, err := d.Snapshot(ctx)
	if err != nil {
		t.Fatalf("Snapshot on an empty directory errored: %v", err)
	}
	if snap.Watermark != 0 {
		t.Fatalf("empty snapshot watermark = %d, want 0", snap.Watermark)
	}
}

func TestLADLatencyReturnsTheMatchingSampleAndReportsAbsence(t *testing.T) {
	ctx := context.Background()
	c, d := ladSnapshotCache(t)

	now := time.Now().UTC()
	body, err := json.Marshal(lad.LatencyRecord{
		FromNode: "node-a", ToNode: "node-b", RTTMs: 42, MeasuredAt: now,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Apply(lad.Record{
		Topic: lad.TopicLatency, NodeID: "node-a", Body: body, Timestamp: now,
	}); err != nil {
		t.Fatal(err)
	}

	got, ok, err := d.Latency(ctx, ports.NodeID("node-a"), ports.NodeID("node-b"))
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("a stored latency sample was reported absent — routing loses " +
			"every RTT input and treats all peers as equidistant")
	}
	if got.RTTMs != 42 {
		t.Errorf("RTTMs = %v, want 42", got.RTTMs)
	}
	if got.ObservedAtUnixMs != now.UnixMilli() {
		t.Errorf("ObservedAtUnixMs = %d, want %d", got.ObservedAtUnixMs, now.UnixMilli())
	}

	// The absent case must report false rather than a zero-valued sample that
	// reads as "0ms RTT" — the fastest possible peer.
	if _, ok, err := d.Latency(ctx, ports.NodeID("node-a"), ports.NodeID("node-z")); err != nil {
		t.Fatal(err)
	} else if ok {
		t.Fatal("a latency sample was reported for a pair that has none — a " +
			"zero-valued sample reads as 0ms, the best possible score")
	}
}

func TestSplitAttrListTrimsAndDropsEmpties(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"auth,billing", []string{"auth", "billing"}},
		{" auth , billing ", []string{"auth", "billing"}},
		{"auth,,billing", []string{"auth", "billing"}},
		{",", nil},
		{"", nil},
		{"   ", nil},
		{"auth", []string{"auth"}},
	}
	for _, tc := range cases {
		got := splitAttrList(tc.in)
		if len(got) != len(tc.want) {
			t.Errorf("splitAttrList(%q) = %v, want %v", tc.in, got, tc.want)
			continue
		}
		for i := range tc.want {
			if got[i] != tc.want[i] {
				t.Errorf("splitAttrList(%q)[%d] = %q, want %q — an untrimmed or "+
					"empty entry becomes a role/handler name that matches nothing",
					tc.in, i, got[i], tc.want[i])
			}
		}
	}
}
