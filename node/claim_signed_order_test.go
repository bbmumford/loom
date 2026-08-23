/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"encoding/json"
	"testing"

	swarm "github.com/bbmumford/swarm"
)

// the design :
//
//	"THE TAKEOVER CLAIM CARRIES BOTH QUALIFIERS IN ONE WIRE RECORD: node
//	 precedence rung and a positive minimum-holder floor. Missing or malformed
//	 qualifiers fail closed for coverage; incompatible floors are surfaced.
//	 Ranking is rung first, then DETERMINISTIC SIGNED CLAIM ORDER; the floor is
//	 a coverage declaration, never a ranking shortcut."
//
// the design: "equal-version tie-breaks use SIGNED BYTES."
//
// 🔴 THE DEFECT THESE TESTS PIN. Before this change rankClaims tie-broke on the
// claimant's NODE ID after rung and HLC. A node chooses its own ID, so it could
// bias which claim won a contested takeover by picking a low-sorting one. The
// signature is verified by swarm before delivery (swarm@v0.0.8 sig.go:95,
// ed25519.Verify over signableBytes) and cannot be steered without the key.
//
// ⚠ THESE TESTS EXIST BECAUSE THE EXISTING SUITE COULD NOT SEE THE CHANGE.
// Every pre-existing rank test builds roleClaim values in-process, where sig is
// empty; both sides are then unsigned and fall through to the node-ID branch,
// so the whole suite stayed green whether or not the signed branch worked. A
// green suite proved nothing about this path until these fixtures existed.

// signedClaim builds an ingested-shaped claim: a rung/HLC plus a signature,
// with the node ID deliberately free to be set ANTI-CORRELATED to the sig.
func signedClaim(nodeID string, rung int, hlc uint64, sig []byte) roleClaim {
	return roleClaim{
		body:       claimBody{Role: "auth", NodeID: nodeID, Rung: rung},
		hlc:        hlc,
		rung:       normaliseRung(rung),
		minHolders: floorUnset,
		sig:        sig,
	}
}

// 🔑 THE ANTI-CORRELATED FIXTURE. The node IDs and the signatures order in
// OPPOSITE directions, so a comparator that still consults the node ID gives a
// different answer from one that consults the signature. A fixture where both
// agree would pass under either implementation and prove nothing — the mistake
// this lane has made five times (sort keys, node IDs, zero-value grades, ctx
// identity, dedup floors).
func TestAtEqualRungAndHLCTheSignatureDecidesNotTheNodeID(t *testing.T) {
	set := map[string]roleClaim{
		// node-a sorts FIRST by ID but its signature sorts LAST.
		"node-a": signedClaim("node-a", 1, 100, []byte{0xFF, 0x01}),
		// node-z sorts LAST by ID but its signature sorts FIRST.
		"node-z": signedClaim("node-z", 1, 100, []byte{0x00, 0x01}),
	}

	got := rankClaims(set, 2)

	if len(got) != 2 {
		t.Fatalf("rankClaims returned %d entries, want 2", len(got))
	}
	if got[0] != "node-z" {
		t.Errorf("winner = %q, want \"node-z\".\n"+
			"Both claims are at rung 1 and HLC 100, so the decision falls to the tie-break. "+
			"node-z holds the lower SIGNATURE (0x0001) and the higher NODE ID; node-a holds "+
			"the higher signature (0xFF01) and the lower node ID. Getting node-a means the "+
			"comparator is still deciding on the claimant-chosen node ID, which lets a node "+
			"win a contested role by renaming itself.", got[0])
	}
}

// 🔴 ABSENT MUST SORT LAST. bytes.Compare ranks an empty signature ahead of
// every real one, so a naive signed comparator would make the ONE claim whose
// origin was never attested the most preferred. That is the same inversion
// rungUnset and floorUnset already guard in this file.
func TestAnUnsignedClaimNeverOutranksASignedOneAtEqualRungAndHLC(t *testing.T) {
	set := map[string]roleClaim{
		// Unsigned AND lowest node ID — wins under both the old comparator and
		// a naive bytes.Compare. It must still lose.
		"node-a": signedClaim("node-a", 1, 100, nil),
		"node-z": signedClaim("node-z", 1, 100, []byte{0xFF, 0xFF}),
	}

	got := rankClaims(set, 2)

	if got[0] != "node-z" {
		t.Errorf("winner = %q, want \"node-z\" — the UNSIGNED claim won.\n"+
			"node-a has no signature and the lowest node ID; node-z is attested but sorts "+
			"last by both empty-bytes comparison and by ID. An unattested claim taking a "+
			"service role over an attested one is the fail-open direction.", got[0])
	}
}

// Rung still dominates the signature: the design says "rung FIRST, then
// deterministic signed claim order". A better signature must not rescue a
// further rung.
func TestRungStillDominatesTheSignature(t *testing.T) {
	set := map[string]roleClaim{
		// Nearer rung, worst possible signature.
		"near": signedClaim("near", 1, 100, []byte{0xFF, 0xFF}),
		// Further rung, best possible signature.
		"far": signedClaim("far", 3, 100, []byte{0x00, 0x00}),
	}

	if got := rankClaims(set, 2); got[0] != "near" {
		t.Errorf("winner = %q, want \"near\" — the signature overtook the rung. "+
			"the design orders rung FIRST; a dormant rung must never win by holding a "+
			"lower-sorting signature.", got[0])
	}
}

// And HLC still sits between them: rung → HLC → signature.
func TestHLCIsStillComparedBeforeTheSignature(t *testing.T) {
	set := map[string]roleClaim{
		"early": signedClaim("early", 1, 100, []byte{0xFF, 0xFF}),
		"late":  signedClaim("late", 1, 300, []byte{0x00, 0x00}),
	}

	if got := rankClaims(set, 2); got[0] != "early" {
		t.Errorf("winner = %q, want \"early\" — the signature overtook the HLC. "+
			"the design makes signed bytes the EQUAL-VERSION tie-break, so the version "+
			"(HLC) is compared first.", got[0])
	}
}

// Two claims that are identical on every ranked key must still order totally
// and stably, or rankClaims is not deterministic and two nodes can disagree
// about the winner.
func TestIdenticallyRankedClaimsStillOrderDeterministically(t *testing.T) {
	build := func() map[string]roleClaim {
		return map[string]roleClaim{
			"node-a": signedClaim("node-a", 1, 100, []byte{0x07}),
			"node-b": signedClaim("node-b", 1, 100, []byte{0x07}),
			"node-c": signedClaim("node-c", 1, 100, []byte{0x07}),
		}
	}
	first := rankClaims(build(), 3)
	for i := 0; i < 12; i++ {
		if got := rankClaims(build(), 3); !equalOrder(got, first) {
			t.Fatalf("rankClaims is not deterministic across map iterations: %v then %v.\n"+
				"Go randomises map range order, so an incomplete comparator surfaces here. "+
				"Two nodes computing different winners from the same claim set is a "+
				"split-brain takeover.", first, got)
		}
	}
}

func equalOrder(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// 🔴 A VALUE SET IS NOT A VALUE DELIVERED. The tests above build() roleClaim
// directly, so none of them proves that onClaimRecord actually carries
// swarm.Record.Sig through to the ranker. Without this, the signature could be
// dropped at ingest and every test above would still pass.
func TestOnClaimRecordCarriesTheRecordSignatureThroughToTheRanker(t *testing.T) {
	e := &TakeoverEngine{claims: map[string]map[string]roleClaim{}}
	body, err := json.Marshal(claimBody{Role: "auth", NodeID: "node-a", Rung: 2, MinHolders: 3})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	sig := []byte{0xDE, 0xAD, 0xBE, 0xEF}

	e.onClaimRecord("auth", swarm.Record{
		NodeID: swarm.NodeID("node-a"),
		HLC:    99,
		Body:   body,
		Sig:    sig,
	})

	got, ok := e.claims["auth"]["node-a"]
	if !ok {
		t.Fatal("claim was not ingested at all")
	}
	if len(got.sig) == 0 {
		t.Fatal("onClaimRecord dropped the record signature — the design signed order is " +
			"unreachable no matter what the comparator does, because the ranker never " +
			"receives the bytes")
	}
	if string(got.sig) != string(sig) {
		t.Errorf("carried signature = %x, want %x", got.sig, sig)
	}

	// 🔑 COPIED, NOT ALIASED. r.Sig belongs to the delivering subscriber; if the
	// claim retained the slice by reference, a later reuse of that buffer would
	// silently re-rank an already-ingested claim.
	sig[0] = 0x00
	if got2 := e.claims["auth"]["node-a"]; got2.sig[0] == 0x00 {
		t.Error("the stored claim ALIASES the record's signature slice — mutating the " +
			"caller's buffer changed a stored claim, so ranking depends on memory the " +
			"subscriber is free to reuse")
	}
}
