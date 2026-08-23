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

// COVERAGE of onClaimRecord's remaining branches (:358, was 75.0%) — the
// tombstone path and the malformed-body path.
//
// 🔑 WHY THIS PAIRS WITH claim_withdraw_test.go. That file tests the node doing
// the withdrawing: withdrawClaim publishes a tombstone. THIS file tests the
// other end — what a PEER does when that tombstone arrives. A withdrawal that
// is published but not honoured on ingest is a withdrawal in name only, and
// neither half proves the other. Measuring both endpoints is the only way to
// measure the edge.
//
// Claim lifecycle only; no secret material is constructed or asserted on.

func ingestFixture() *TakeoverEngine {
	return &TakeoverEngine{claims: map[string]map[string]roleClaim{}}
}

func claimRecord(t *testing.T, nodeID, role string, rung int, hlc uint64) swarm.Record {
	t.Helper()
	body, err := json.Marshal(claimBody{Role: role, NodeID: nodeID, Rung: rung})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return swarm.Record{NodeID: swarm.NodeID(nodeID), HLC: hlc, Body: body, Sig: []byte{0x01}}
}

// 🔴 THE WITHDRAWAL MUST LAND. A tombstone removes that claimant from the set,
// so the next ranking no longer counts a node that has stood down. If it did
// not, a withdrawn node would keep winning the role forever — it publishes a
// tombstone, believes it withdrew, and every peer keeps ranking it first.
func TestATombstoneRemovesTheClaimantFromTheSet(t *testing.T) {
	e := ingestFixture()
	e.onClaimRecord("auth", claimRecord(t, "node-a", "auth", 1, 100))
	e.onClaimRecord("auth", claimRecord(t, "node-b", "auth", 1, 200))

	if got := len(e.claims["auth"]); got != 2 {
		t.Fatalf("fixture: %d claims ingested, want 2", got)
	}

	e.onClaimRecord("auth", swarm.Record{NodeID: swarm.NodeID("node-a"), Tombstone: true})

	if _, still := e.claims["auth"]["node-a"]; still {
		t.Error("a tombstoned claimant is still in the claim set — its withdrawal was " +
			"published but not honoured, so peers keep ranking a node that stood down")
	}
	if _, ok := e.claims["auth"]["node-b"]; !ok {
		t.Error("the tombstone also removed an unrelated claimant")
	}
}

// A tombstone for a claimant that was never seen must be a harmless no-op, not
// a panic or a spurious empty entry. Gossip routinely delivers a tombstone
// before, or without, the record it retracts.
func TestATombstoneForAnUnknownClaimantIsHarmless(t *testing.T) {
	e := ingestFixture()

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("tombstone for an unseen claimant panicked: %v", r)
		}
	}()

	e.onClaimRecord("auth", swarm.Record{NodeID: swarm.NodeID("ghost"), Tombstone: true})

	if n := len(e.claims["auth"]); n != 0 {
		t.Errorf("claim set has %d entries after only a tombstone, want 0 — an unseen "+
			"retraction manufactured a claimant", n)
	}
}

// 🔴 A MALFORMED OR MIS-ROUTED BODY MUST NOT ENTER THE SET. the design requires
// missing or malformed qualifiers to "fail closed for coverage". A record that
// does not parse, or whose body names a DIFFERENT role than the topic it
// arrived on, is not evidence of a claim — admitting it would let a node claim
// a role it never named by publishing to the wrong topic.
func TestMalformedAndMisroutedClaimsAreRejected(t *testing.T) {
	for _, tc := range []struct {
		name string
		rec  swarm.Record
	}{
		{"unparseable body", swarm.Record{
			NodeID: swarm.NodeID("node-a"), Body: []byte("{not json"),
		}},
		{"empty body", swarm.Record{
			NodeID: swarm.NodeID("node-a"), Body: nil,
		}},
		{"body names a different role than the topic", claimRecord(t, "node-a", "billing", 1, 100)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := ingestFixture()

			e.onClaimRecord("auth", tc.rec)

			if n := len(e.claims["auth"]); n != 0 {
				t.Errorf("claim set has %d entries, want 0 — a %s was admitted as a valid "+
					"claim on \"auth\"", n, tc.name)
			}
		})
	}
}

// 🔬 THIS TEST EXISTS BECAUSE A MUTANT SURVIVED. Neutering the
// `err != nil` half of onClaimRecord's guard did not fail anything, because
// every malformed body in the table above leaves Role empty and is caught by
// the `body.Role != role` half instead. The two guards happened to agree, so
// the suite could not tell which one was working.
//
// The case that separates them is a body whose role is VALID and whose
// QUALIFIER is malformed. encoding/json populates fields as it goes and returns
// the type error at the end, so Role is already "auth" when the decode fails —
// the role check passes and only the unmarshal check stands between a corrupt
// record and the claim set.
//
// 🔴 That is the design verbatim: "Missing or malformed qualifiers FAIL CLOSED for
// coverage." A claim admitted here would carry a zero rung — which normalises
// to rungUnset and sorts last, so the immediate effect is mild — but it would
// still count as a live claimant for coverage, letting a corrupt record hold a
// role open against the very floor the design exists to enforce.
func TestAValidRoleWithAMalformedQualifierIsRejected(t *testing.T) {
	// Parses far enough to set Role, then fails on the typed rung field.
	body := []byte(`{"role":"auth","nodeId":"node-a","rung":"three"}`)

	// Guard the premise: the decoder really does populate Role before erroring,
	// otherwise this test would pass for the wrong reason.
	var probe claimBody
	if err := json.Unmarshal(body, &probe); err == nil {
		t.Fatal("fixture is wrong: the body parsed cleanly, so it cannot exercise the " +
			"unmarshal-error guard")
	} else if probe.Role != "auth" {
		t.Fatalf("fixture is wrong: Role = %q after the failed decode, so the role check "+
			"would reject this body and the unmarshal guard stays masked", probe.Role)
	}

	e := ingestFixture()
	e.onClaimRecord("auth", swarm.Record{NodeID: swarm.NodeID("node-a"), Body: body})

	if n := len(e.claims["auth"]); n != 0 {
		t.Errorf("claim set has %d entries, want 0 — a record whose rung failed to decode "+
			"was admitted as a live claimant, so a malformed qualifier holds coverage open "+
			"instead of failing closed (the design)", n)
	}
}

// A later record from the same claimant replaces the earlier one rather than
// accumulating, so the set holds one current claim per node.
func TestARepeatedClaimFromOneNodeReplacesRatherThanAccumulates(t *testing.T) {
	e := ingestFixture()

	e.onClaimRecord("auth", claimRecord(t, "node-a", "auth", 3, 100))
	e.onClaimRecord("auth", claimRecord(t, "node-a", "auth", 1, 200))

	if n := len(e.claims["auth"]); n != 1 {
		t.Fatalf("claim set has %d entries for one claimant, want 1", n)
	}
	got := e.claims["auth"]["node-a"]
	if got.hlc != 200 || got.rung != 1 {
		t.Errorf("stored claim is hlc=%d rung=%d, want the LATER record hlc=200 rung=1 — "+
			"a re-claim at a nearer rung was ignored", got.hlc, got.rung)
	}
}
