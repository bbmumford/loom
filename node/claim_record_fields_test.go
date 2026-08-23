/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/bbmumford/swarm"
)

// Acceptance tests for the single wire-format change that
// freezes claimBody's field set under ME-Q07:
//
//	"DECIDE THE CLAIM RECORD'S FULL FIELD SET ONCE — both are the same missing
//	 idea, and ADDING THEM SEPARATELY MEANS CHANGING THE WIRE FORMAT TWICE."
//
// Both qualifiers travel in one record: Rung (the node class) and MinHolders
// (the cardinality FLOOR).
//
// The boundary these tests exist to defend. Whether any role is genuinely
// exclusive, and if so what fences it, is undecided.
// Carrying the field applies ME-Q07. Giving it RANKING semantics would answer
// ME-Q06 by implementation. So the central assertion here is a NEGATIVE one:
// ranking must be provably unaffected by the floor.

func TestBothClaimQualifiersRoundTripOnTheWire(t *testing.T) {
	in := claimBody{Role: "auth", NodeID: "node-a", AtUnixMs: 1234, Rung: 2, MinHolders: 3}

	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out claimBody
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if out != in {
		t.Errorf("claim body did not survive the round trip:\n got %+v\nwant %+v", out, in)
	}
	// Both qualifiers must be present as named wire keys — a field that
	// serialises under the wrong name is a second wire change waiting to happen.
	for _, key := range []string{`"rung":2`, `"minHolders":3`} {
		if !jsonContains(raw, key) {
			t.Errorf("wire form %s is missing %s", raw, key)
		}
	}
}

// omitempty on both keeps an old peer's record parseable and keeps OUR record
// parseable by an old peer — the compatibility half of doing this once.
func TestAClaimFromAPeerPredatingBothFieldsStillParses(t *testing.T) {
	legacy := []byte(`{"role":"auth","nodeId":"node-old","atUnixMs":99}`)

	var body claimBody
	if err := json.Unmarshal(legacy, &body); err != nil {
		t.Fatalf("a pre-chain claim no longer parses: %v", err)
	}
	if body.Rung != 0 || body.MinHolders != 0 {
		t.Errorf("absent qualifiers did not decode as zero: %+v", body)
	}
	// And both normalise to their explicit "unset" markers rather than to a
	// value that reads as a real declaration.
	if got := normaliseRung(body.Rung); got != rungUnset {
		t.Errorf("normaliseRung(absent) = %d, want rungUnset — an unset rung must sort LAST", got)
	}
	if got := normaliseFloor(body.MinHolders); got != floorUnset {
		t.Errorf("normaliseFloor(absent) = %d, want floorUnset", got)
	}

	// A claim carrying only ONE of the two must also parse — this is exactly
	// the mixed fleet ME-Q07 warns about, and doing both fields at once is what
	// keeps this to a single transitional shape rather than two.
	partial := []byte(`{"role":"auth","nodeId":"node-mid","atUnixMs":99,"rung":2}`)
	var mid claimBody
	if err := json.Unmarshal(partial, &mid); err != nil {
		t.Fatalf("a rung-only claim does not parse: %v", err)
	}
	if mid.Rung != 2 || normaliseFloor(mid.MinHolders) != floorUnset {
		t.Errorf("rung-only claim decoded wrong: %+v", mid)
	}
}

// 🔴 THE FLOOR FAILS CLOSED: a zero or negative floor is recorded as UNSET, not
// as a satisfied floor of zero. "At least 0 holders" is the one reading that
// would let a role go uncovered with nothing noticing.
func TestAZeroOrNegativeFloorIsRecordedAsUnsetNotAsSatisfied(t *testing.T) {
	for _, in := range []int{0, -1, -7} {
		if got := normaliseFloor(in); got != floorUnset {
			t.Errorf("normaliseFloor(%d) = %d, want floorUnset — a floor of zero means \"no "+
				"holders required\", which is never what an operator meant", in, got)
		}
	}
	for _, in := range []int{1, 3, 99} {
		if got := normaliseFloor(in); got != in {
			t.Errorf("normaliseFloor(%d) = %d, want it unchanged", in, got)
		}
	}
}

// 🔑 SILENCE IS NOT CONTRADICTION. A peer that predates the field declares
// nothing, and treating "nothing" as a disagreement would make every mixed
// fleet log a split-brain that does not exist.
func TestFloorDisagreementIgnoresSilenceAndCatchesRealConflict(t *testing.T) {
	for _, tc := range []struct {
		name          string
		local, remote int
		want          bool
	}{
		{"both silent", floorUnset, floorUnset, false},
		{"remote silent (an older peer)", 3, floorUnset, false},
		{"local silent (we are unconfigured)", floorUnset, 3, false},
		{"agreement", 3, 3, false},
		{"real disagreement", 3, 1, true},
		{"real disagreement, other direction", 1, 3, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := claimFloorDisagreement(tc.local, tc.remote); got != tc.want {
				t.Errorf("claimFloorDisagreement(%d, %d) = %v, want %v",
					tc.local, tc.remote, got, tc.want)
			}
		})
	}
}

// 🛑 THE NEGATIVE ASSERTION, AND IT IS THE POINT OF THE WHOLE FILE.
// rankClaims must produce an identical ordering whatever the floors say. If a
// change ever makes the floor rank, ME-Q06 has been answered by implementation
// rather than by @owner, and this test is what catches it.
func TestTheCardinalityFloorDoesNotAffectRanking(t *testing.T) {
	base := map[string]roleClaim{
		"node-a": {body: claimBody{NodeID: "node-a"}, hlc: 300, rung: 1, minHolders: floorUnset},
		"node-z": {body: claimBody{NodeID: "node-z"}, hlc: 100, rung: 2, minHolders: floorUnset},
		"node-m": {body: claimBody{NodeID: "node-m"}, hlc: 200, rung: 1, minHolders: floorUnset},
	}
	want := rankClaims(base, len(base))

	// Same claims, wildly different floors — including values that would
	// reorder the set if any of them were consulted.
	withFloors := map[string]roleClaim{
		"node-a": {body: claimBody{NodeID: "node-a"}, hlc: 300, rung: 1, minHolders: 99},
		"node-z": {body: claimBody{NodeID: "node-z"}, hlc: 100, rung: 2, minHolders: 1},
		"node-m": {body: claimBody{NodeID: "node-m"}, hlc: 200, rung: 1, minHolders: 50},
	}
	got := rankClaims(withFloors, len(withFloors))

	if len(got) != len(want) {
		t.Fatalf("rank length changed: got %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("RANKING CHANGED WITH THE FLOOR SET:\n got %v\nwant %v\n"+
				"ME-Q06 is an OPEN owner decision — whether a role is genuinely exclusive, and "+
				"what fences it. A floor that reorders claimants answers it by implementation.",
				got, want)
		}
	}
}

// 🔴 A VALUE SET IS NOT A VALUE DELIVERED. The tests above prove claimBody CAN
// carry both qualifiers; none of them proves publishClaim actually PUTS them
// there. Mutation confirmed the gap — deleting `MinHolders: e.cfg.MinReplicas`
// from publishClaim survived every test above, because they construct the
// struct themselves rather than travelling the production path.
//
// This walks from the engine's configuration to the bytes handed to the swarm,
// which is the only assertion that closes the gap between the value set and
// the value delivered.
func TestPublishClaimPutsBothQualifiersOnTheActualWireBytes(t *testing.T) {
	pub := &capturingSwarmNode{stubSwarmNode: &stubSwarmNode{}}
	e := &TakeoverEngine{
		rt: &Runtime{
			swarm:    &SwarmIntegration{Node: pub},
			identity: &NodeIdentity{NodeID: "node-a"},
		},
		cfg:       TakeoverConfig{ChainRung: 2, ChainDepth: 4, MinReplicas: 3},
		claimedAt: map[string]time.Time{},
	}

	if err := e.publishClaim("auth"); err != nil {
		t.Fatalf("publishClaim: %v", err)
	}
	if len(pub.published) != 1 {
		t.Fatalf("publishClaim published %d records, want 1", len(pub.published))
	}

	var body claimBody
	if err := json.Unmarshal(pub.published[0], &body); err != nil {
		t.Fatalf("published body does not parse: %v", err)
	}
	if body.Rung != 2 {
		t.Errorf("published Rung = %d, want the configured ChainRung 2 — the class qualifier "+
			"ME-P16 requires is not reaching the wire", body.Rung)
	}
	if body.MinHolders != 3 {
		t.Errorf("published MinHolders = %d, want the configured MinReplicas 3 — the field "+
			"exists on the struct but the publish path is not populating it, so ME-Q07's "+
			"single wire change would be incomplete and need a second one", body.MinHolders)
	}
	if body.Role != "auth" || body.NodeID != "node-a" {
		t.Errorf("published identity fields wrong: %+v", body)
	}
}

// capturingSwarmNode records what Publish was handed, which the package's
// existing stub discards.
type capturingSwarmNode struct {
	*stubSwarmNode
	published [][]byte
}

func (n *capturingSwarmNode) Publish(_ swarm.Topic, body []byte) error {
	n.published = append(n.published, append([]byte(nil), body...))
	return nil
}

func jsonContains(raw []byte, needle string) bool {
	return len(raw) > 0 && bytesIndex(raw, needle) >= 0
}

func bytesIndex(haystack []byte, needle string) int {
	n, h := len(needle), len(haystack)
	for i := 0; i+n <= h; i++ {
		if string(haystack[i:i+n]) == needle {
			return i
		}
	}
	return -1
}
