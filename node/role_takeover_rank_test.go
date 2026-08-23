/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"encoding/json"
	"testing"
)

// COVERAGE of the role-claim ranking decision: rankClaims (:377) and
// roleActivationManager.isActive (:403) — both at 0.0%.
//
// 🔑 rankClaims IS A CONSENSUS FUNCTION WITH NO CONSENSUS ROUND. Every node
// computes it locally over the same gossiped claim set and acts on the result
// without coordinating (evaluateRole:310-313). Determinism is therefore not a
// nicety: if two nodes rank the same set differently, both activate the same
// role, or neither does. These tests pin the exact ordering contract.
//
// CENSUS: rankClaims <- evaluateRole:313 and :339 (both in this file, non-test).
// isActive <- evaluateRole:269 and :288.

func claim(hlc uint64) roleClaim { return roleClaim{hlc: hlc} }

func rankedEquals(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// 🔴 THE ORDERING CONTRACT, stated in the function's own doc: "(HLC, NodeID)
// ascending … earliest claims win; NodeID breaks HLC ties."
//
// ⚠ FIXTURE NOTE, and mutation caught it: my first version used node-a(100),
// node-b(200), node-c(300) — where alphabetical NodeID order HAPPENS to equal
// HLC order. A mutant that compared NodeID FIRST and ignored HLC entirely
// SURVIVED, because every fixture produced the same answer either way. The IDs
// below are deliberately ANTI-correlated with their HLCs: only an HLC-first
// comparator produces `want`, and an ID-first one produces the exact reverse.
func TestRankClaimsOrdersByHLCThenNodeIDAscending(t *testing.T) {
	set := map[string]roleClaim{
		"node-a": claim(300), // alphabetically FIRST, chronologically LAST
		"node-m": claim(200),
		"node-z": claim(100), // alphabetically LAST, chronologically FIRST
	}

	got := rankClaims(set, 3)
	want := []string{"node-z", "node-m", "node-a"}
	if !rankedEquals(got, want) {
		t.Fatalf("rankClaims = %v, want %v — earliest claim must win, and HLC must "+
			"outrank NodeID. Getting [node-a node-m node-z] means the comparator is "+
			"sorting by NodeID and ignoring the claim time entirely", got, want)
	}
}

// The tie-break is what makes the function deterministic across nodes. Two
// claims at the same HLC must resolve by NodeID, or two nodes ranking the same
// set can disagree about the winner and both activate the role.
func TestEqualHLCsAreBrokenByNodeIDNotMapOrder(t *testing.T) {
	set := map[string]roleClaim{
		"node-z": claim(500),
		"node-a": claim(500),
		"node-m": claim(500),
	}

	// Go randomises map iteration order, so repeating this is a real test of
	// determinism rather than a formality.
	want := []string{"node-a", "node-m", "node-z"}
	for i := 0; i < 50; i++ {
		if got := rankClaims(set, 3); !rankedEquals(got, want) {
			t.Fatalf("iteration %d: rankClaims = %v, want %v — the ranking depends on "+
				"map iteration order, so two nodes ranking the SAME claim set can pick "+
				"DIFFERENT winners and both activate the role", i, got, want)
		}
	}
}

// n larger than the set must clamp, not panic — evaluateRole:339 deliberately
// calls it with 1<<30 to count the whole set for a log line.
func TestRankClaimsClampsNBeyondTheSetSize(t *testing.T) {
	set := map[string]roleClaim{"node-a": claim(1), "node-b": claim(2)}

	got := rankClaims(set, 1<<30)
	if len(got) != 2 {
		t.Fatalf("rankClaims(set, 1<<30) returned %d entries, want 2 — evaluateRole:339 "+
			"calls it exactly this way to size the claim set for its log line", len(got))
	}
}

func TestRankClaimsOnAnEmptySetReturnsEmptyNotNilPanic(t *testing.T) {
	if got := rankClaims(map[string]roleClaim{}, 5); len(got) != 0 {
		t.Fatalf("rankClaims(empty) = %v, want no winners", got)
	}
	// A nil map is the state before any claim record has arrived.
	if got := rankClaims(nil, 5); len(got) != 0 {
		t.Fatalf("rankClaims(nil) = %v, want no winners", got)
	}
}

// MaxWinners defaults to MinReplicas, which defaults to 1 (StartTakeover:101-105).
// So in the default configuration this function selects exactly ONE node, and
// the truncation is the whole decision.
func TestOnlyTheFirstNWinAndTheRestAreExcluded(t *testing.T) {
	set := map[string]roleClaim{
		"node-a": claim(100),
		"node-b": claim(200),
		"node-c": claim(300),
	}

	got := rankClaims(set, 1)
	if !rankedEquals(got, []string{"node-a"}) {
		t.Fatalf("rankClaims(set, 1) = %v, want [node-a] — with the default "+
			"MaxWinners=1 this single entry decides who assumes the role", got)
	}
}

// 🔴 CHARACTERISATION, NOT APPROVAL — . THE RANKING IS ASCENDING, SO THE
// LOWEST HLC WINS, AND A LOW HLC IS EXACTLY WHAT A BROKEN OR HOSTILE CLOCK
// PRODUCES.
//
// swarm's HLC packs (unixMillis << 16 | counter), so an honest claim published
// today carries a wall component of ~1.79e12 milliseconds shifted left 16 bits —
// an enormous number. A node whose clock reads the epoch (an unsynchronised
// container is the realistic case, not an attacker) publishes a claim whose HLC
// is near zero and DETERMINISTICALLY TAKES THE ONLY WINNER SLOT on every node,
// every round, forever.
//
// ⚠ The value is signed (swarm/sig.go:50 covers HLC) and the claim map is keyed
// by the AUTHENTICATED record NodeID, so a node cannot forge another's claim —
// one bad node gets one entry. But with MaxWinners=1, one entry is all it takes.
//
// 📌 The asymmetry is the point: swarm HAS a skew defence
// (observerForwardSkewBudget, crdt.go:215) and it bounds how far into the FUTURE
// a timestamp may be, because the CRDT merge is HLC-MAX where high wins. This
// ranking is HLC-MIN where LOW wins, so the existing defence points 180° away
// from the direction that matters here.
//
// This test pins the CURRENT behaviour. If a plausibility floor is added, this
// test should fail and be updated deliberately.
func TestTheLowestHLCWinsIncludingAnImplausibleZero(t *testing.T) {
	const realisticHLC = uint64(1785000000000) << 16 // a genuine 2026 wall clock

	set := map[string]roleClaim{
		"honest-node-1":    claim(realisticHLC),
		"honest-node-2":    claim(realisticHLC + 5),
		"epoch-clock-node": claim(1), // wall component 0 — Jan 1 1970
	}

	got := rankClaims(set, 1)
	if !rankedEquals(got, []string{"epoch-clock-node"}) {
		t.Fatalf("rankClaims = %v, want [epoch-clock-node] — this test PINS a known "+
			"weakness rather than endorsing it. If a lower plausibility "+
			"bound on claim HLC has been added, that is an improvement: update this "+
			"test deliberately and record why", got)
	}

	// The admission twin: with the epoch claim removed, the honest earliest
	// claim wins — so the assertion above is about the ZERO specifically, not
	// about rankClaims being broken for everyone.
	delete(set, "epoch-clock-node")
	if got := rankClaims(set, 1); !rankedEquals(got, []string{"honest-node-1"}) {
		t.Fatalf("without the epoch claim, rankClaims = %v, want [honest-node-1]", got)
	}
}

// ── the design: the ordered failover chain ─────────────────────────────

func rungClaim(rung int, hlc uint64) roleClaim {
	return roleClaim{body: claimBody{Rung: rung}, hlc: hlc, rung: normaliseRung(rung)}
}

// 🔴 RUNG DOMINATES HLC — this is the whole of the design "explicit and ordered".
// A nearer rung must win even when it claims LATER, because the chain
// "descends only as far as needed" (the design). Without this the chain is
// decorative: whoever gossiped first takes the role regardless of position.
func TestANearerRungBeatsAnEarlierClaimFromAFurtherRung(t *testing.T) {
	set := map[string]roleClaim{
		"far-but-first":  rungClaim(3, 100), // claimed FIRST, furthest rung
		"near-but-later": rungClaim(1, 900), // claimed LAST, nearest rung
		"middle":         rungClaim(2, 500),
	}

	got := rankClaims(set, 3)
	want := []string{"near-but-later", "middle", "far-but-first"}
	if !rankedEquals(got, want) {
		t.Fatalf("rankClaims = %v, want %v — rung must outrank HLC. Getting "+
			"[far-but-first …] means the chain order is being ignored and the "+
			"earliest gossiper takes the role regardless of its position", got, want)
	}
}

// …and WITHIN a rung, HLC still decides. The anti-thundering-herd property must
// survive the chain, or every node on the same rung activates together.
func TestWithinOneRungTheEarliestClaimStillWins(t *testing.T) {
	set := map[string]roleClaim{
		"same-rung-late":  rungClaim(2, 900),
		"same-rung-early": rungClaim(2, 100),
	}

	if got := rankClaims(set, 1); !rankedEquals(got, []string{"same-rung-early"}) {
		t.Fatalf("rankClaims = %v, want [same-rung-early] — HLC is no longer the "+
			"tie-break within a rung, so co-rung nodes have no deterministic order", got)
	}
}

// 🔴 THE GUARD THAT MATTERS MOST, AND IT IS THIS CODEBASE'S SIGNATURE DEFECT.
// `Rung` is `omitempty`, so a claim from a node predating the design decodes to 0 —
// and 0 would be the MOST-preferred rung. An un-upgraded node would silently
// outrank every deliberately-placed one.
func TestAClaimWithNoRungSortsLastNotFirst(t *testing.T) {
	set := map[string]roleClaim{
		"legacy-no-rung": rungClaim(0, 100), // claimed FIRST, no rung on the wire
		"declared-rung3": rungClaim(3, 900), // claimed LAST, furthest DECLARED rung
	}

	got := rankClaims(set, 2)
	want := []string{"declared-rung3", "legacy-no-rung"}
	if !rankedEquals(got, want) {
		t.Fatalf("rankClaims = %v, want %v — a claim carrying NO rung is ranking "+
			"as rung 0, i.e. the most preferred. Absence must never outrank a "+
			"declared position", got, want)
	}
}

func TestNormaliseRungFailsClosedOnEveryNonPositiveValue(t *testing.T) {
	for _, in := range []int{0, -1, -999} {
		if got := normaliseRung(in); got != rungUnset {
			t.Errorf("normaliseRung(%d) = %d, want rungUnset (%d) — a nonsensical "+
				"rung is being treated as a preference", in, got, rungUnset)
		}
	}
	for _, in := range []int{1, 2, 50} {
		if got := normaliseRung(in); got != in {
			t.Errorf("normaliseRung(%d) = %d, want %d — a declared rung was altered", in, got, in)
		}
	}
}

// 🔑 rankClaims must normalise defensively: a roleClaim built directly (tests,
// or any future call site that bypasses onClaimRecord) has rung==0, which the
// comparator would otherwise read as the best rung.
func TestRankClaimsNormalisesAnUnnormalisedClaim(t *testing.T) {
	set := map[string]roleClaim{
		// Deliberately NOT via rungClaim: .rung left at the zero value while
		// the wire body declares rung 5.
		"bypassed": {body: claimBody{Rung: 5}, hlc: 100},
		"declared": rungClaim(2, 900),
	}

	got := rankClaims(set, 2)
	if !rankedEquals(got, []string{"declared", "bypassed"}) {
		t.Fatalf("rankClaims = %v, want [declared bypassed] — an un-normalised "+
			"claim reached the comparator with rung 0 and outranked a real rung-2 "+
			"claim", got)
	}
}

// A FLAT chain (every node on the same rung) must behave exactly as it did
// before the design — ChainDepth defaults to 1, so this is the default deployment
// and any change here is a regression for every existing node.
func TestAFlatChainRanksExactlyAsBefore(t *testing.T) {
	set := map[string]roleClaim{
		"node-a": rungClaim(1, 300),
		"node-m": rungClaim(1, 200),
		"node-z": rungClaim(1, 100),
	}

	got := rankClaims(set, 3)
	if !rankedEquals(got, []string{"node-z", "node-m", "node-a"}) {
		t.Fatalf("rankClaims = %v, want [node-z node-m node-a] — a single-rung "+
			"chain no longer reduces to pure (HLC, NodeID) ordering, so the design "+
			"changed behaviour for every node that never configured a chain", got)
	}
}

// 🔴 THE SAFETY DEFAULT, NOW REACHABLE. Mutation proved this clamp was
// untestable while it lived inline in StartTakeover (which needs a live swarm
// Node), so a mutant defaulting an unconfigured node to rung 1 — THE MOST
// PREFERRED — survived. Extracting it made the highest-consequence default in
// the file assertable.
func TestAnUnconfiguredNodeResolvesToTheLastRungNeverTheFirst(t *testing.T) {
	// This table encodes the RANKING RULE, not the shape of any one input.
	// It asserted {0,0} → (1,1): an entirely unconfigured node resolving to rung
	// 1, the MOST preferred class. The guard below it only probed depth=5, so it
	// never reached the depth=0 case where the inversion actually lived. A test
	// written against current behaviour pins that behaviour as correct — the
	// exact rule this lane cites elsewhere, broken here by the lane itself.
	//
	// Corrected semantics: the rung is a GLOBAL CLASS. An undeclared rung
	// publishes ABSENT (0) and sorts last on every peer via rungUnset; a
	// declared rung publishes exactly as declared and is never clamped to the
	// private local depth.
	for _, tc := range []struct {
		name              string
		rung, depth       int
		wantRung, wantDep int
	}{
		{"both unset → undeclared rung, flat chain", 0, 0, 0, 1},
		{"rung unset on a 4-deep chain → undeclared", 0, 4, 0, 4},
		{"negative rung is no declaration at all", -3, 4, 0, 4},
		{"rung past the declared depth is NOT clamped down", 9, 4, 9, 4},
		{"a declared in-range rung is preserved", 2, 4, 2, 4},
		{"the nearest rung is preserved", 1, 4, 1, 4},
		{"negative depth → flat, declaration untouched", 1, -1, 1, 1},
	} {
		gotRung, gotDepth := resolveChainPosition(tc.rung, tc.depth)
		if gotRung != tc.wantRung || gotDepth != tc.wantDep {
			t.Errorf("%s: resolveChainPosition(%d,%d) = (%d,%d), want (%d,%d)",
				tc.name, tc.rung, tc.depth, gotRung, gotDepth, tc.wantRung, tc.wantDep)
		}
	}

	// 🔴 THE CASE THE OLD GUARD COULD NOT REACH. depth=0 is ChainDepth's
	// documented default, so this is the ordinary shape of an unconfigured node
	// — and it is precisely where the old code produced rung 1.
	if r, _ := resolveChainPosition(0, 0); r == 1 {
		t.Fatal("a fully unconfigured node (rung 0, depth 0) resolved to rung 1 — the " +
			"MOST preferred class. This is ChainDepth's default, so every node that " +
			"forgot to configure the chain would outrank every node that configured it " +
			"deliberately, inverting the design \"a dormant rung can never outrank a live one\"")
	}
	if r, _ := resolveChainPosition(0, 5); r == 1 {
		t.Fatal("an unconfigured node on a 5-deep chain resolved to rung 1")
	}
}

// 🔴 CROSS-NODE COMPARABILITY (the design 🔑 note: "a cross-node decision cannot
// be driven by a value only one node can see").
//
// ChainDepth is private — it is not on claimBody, never negotiated, never
// validated against a peer. So two nodes that declare the SAME class must
// publish the SAME rung regardless of what each believes the chain depth to be.
// The old clamp broke exactly this, and no test compared two nodes at all.
func TestTwoNodesDeclaringTheSameClassPublishTheSameRungWhateverTheirPrivateDepth(t *testing.T) {
	for _, depth := range []int{0, 1, 2, 4, 99} {
		got, _ := resolveChainPosition(3, depth)
		if got != 3 {
			t.Errorf("a node declaring class 3 with private depth %d publishes rung %d, "+
				"want 3 — the published class now depends on a value no peer can see, so "+
				"two nodes in the same class are ranked as if they were in different ones",
				depth, got)
		}
	}

	// The concrete inversion the refutation described, stated as an assertion:
	// a deliberately-dormant node must never outrank a deliberately-placed one
	// merely because it left ChainDepth unset.
	dormant, _ := resolveChainPosition(3, 0) // thin app, last rung, depth unset
	placed, _ := resolveChainPosition(2, 4)  // designated thick node, configured
	if normaliseRung(dormant) < normaliseRung(placed) {
		t.Fatalf("a dormant class-3 node with unset depth publishes rung %d and outranks "+
			"a configured class-2 node publishing rung %d — the dormant rung wins",
			dormant, placed)
	}
}

// The rung must survive the wire. `omitempty` means a rung of 0 is dropped —
// which is correct only because an absent rung normalises to LAST, not first.
func TestTheRungSurvivesJSONRoundTrip(t *testing.T) {
	in := claimBody{Role: "r", NodeID: "n", Rung: 3}
	blob, err := json.Marshal(in)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var out claimBody
	if err := json.Unmarshal(blob, &out); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if out.Rung != 3 {
		t.Fatalf("rung did not survive the wire: got %d, want 3 — every claim "+
			"would arrive rung-less and rank last", out.Rung)
	}

	// And the omitempty case decodes to 0, which normalises to LAST.
	var legacy claimBody
	if err := json.Unmarshal([]byte(`{"role":"r","nodeId":"n"}`), &legacy); err != nil {
		t.Fatalf("unmarshal legacy: %v", err)
	}
	if normaliseRung(legacy.Rung) != rungUnset {
		t.Fatal("a legacy claim with no rung field does not normalise to rungUnset")
	}
}

// isActive must be false for a role that was never activated, and must not
// panic on a manager whose map is populated but missing the key. evaluateRole
// consults it twice per pass, including on the "covered" fast path.
func TestIsActiveReportsFalseForAnUnknownRole(t *testing.T) {
	m := &roleActivationManager{active: map[string]bool{"role-a": true}}

	if !m.isActive("role-a") {
		t.Fatal("isActive(role-a) = false for an activated role — evaluateRole would " +
			"withdraw a claim for a role this node is actually carrying")
	}
	if m.isActive("role-never-seen") {
		t.Fatal("isActive returned true for a role that was never activated — " +
			"evaluateRole:288 would skip takeover believing the role is already held")
	}
}
