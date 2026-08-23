/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"encoding/json"
	"testing"
	"time"

	lad "github.com/bbmumford/ledger"
	ladcache "github.com/bbmumford/ledger/cache"
)

// PINNED MEASUREMENT: the anchor snapshot attests to records that
// carry no owner signature — TODAY, in shipping code, independently of the
// loom cutover.
//
// generateSnapshot (runtime.go) does `records := rt.cache.Dump()` and hands
// them straight to anchorGenerator.Generate. This test measures what Dump
// actually yields.
//
// Why it matters: ports.Record's four missing signature-covered fields do NOT
// block the signed anchor-snapshot input, because anchor.Generate
// never verifies a per-record signature and anchor.Verify covers only
// SnapshotContent{Timestamp, Sequence, NodeID, Records} with ONE header
// signature. The anchor's signature is a CONTAINER signature: it attests
// "this node, at this sequence, saw these bytes", not "these records are
// authentic owner facts".
//
// The provenance is not forged or lost in transit — ladcache verifies
// signatures at INGRESS (signedTopicACL -> lad.VerifyRecord) and then stores
// the typed struct, discarding the envelope. So the property is: verified
// once at the wire boundary, NOT preserved for downstream re-verification.
// §0.5.4 requires provenance to survive "snapshot". It does not.
func TestAnchorSnapshotInputCarriesNoOwnerSignature(t *testing.T) {
	c := ladcache.NewDirectoryCache()

	rr := lad.ReachRecord{
		TenantID: "hstles", NodeID: "node-1", Seq: 5,
		Addresses: []lad.ReachAddress{{Host: "1.2.3.4", Port: 443, Proto: "ws"}},
		UpdatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour),
	}
	wire, err := json.Marshal(rr)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Apply(lad.Record{
		Topic: lad.TopicReach, TenantID: "hstles", NodeID: "node-1", Seq: 5,
		Body: wire, Timestamp: time.Now(),
		Signature: []byte("OWNER-SIGNATURE-BYTES"), AuthorPubKey: []byte("OWNER-PUBKEY"),
	}); err != nil {
		t.Fatal(err)
	}

	// EXACTLY what runtime.go's generateSnapshot feeds the anchor.
	dumped := c.Dump()
	if len(dumped) == 0 {
		t.Fatal("Dump() returned nothing — the assertions below would be vacuous")
	}

	for _, r := range dumped {
		if len(r.Signature) != 0 || len(r.AuthorPubKey) != 0 {
			t.Fatalf("the anchor snapshot input now carries an owner signature on %q "+
				"(sig=%d pubkey=%d) — ladcache has gained signature retention. Update "+
				"the §0.5.4 correction and re-evaluate whether the anchor can now make "+
				"a provenance claim rather than a container claim",
				r.Topic, len(r.Signature), len(r.AuthorPubKey))
		}
		// Byte equality against the source, not a property of the output:
		// a faithful re-marshal produces the SAME LENGTH and different bytes,
		// so len() and non-empty checks both pass.
		if string(r.Body) == string(wire) {
			t.Fatalf("Dump() now returns the owner's verbatim body for %q — the "+
				"re-marshal has stopped; update the docs resting on this", r.Topic)
		}
	}
}
