/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"errors"
	"testing"
	"time"

	lad "github.com/bbmumford/ledger"
	swarm "github.com/bbmumford/swarm"

	"github.com/bbmumford/loom/ports"
	"github.com/bbmumford/loom/secrets"
)

// errTestNotEntitled stands in for whatever the real entitlement gate refuses
// with. evaluateRole only tests err != nil, so the value carries no meaning
// beyond "refused" — and deliberately nothing about WHY (credential fence).
var errTestNotEntitled = errors.New("not entitled to this role")

// errTestPublishRefused stands in for a transient gossip publish failure.
var errTestPublishRefused = errors.New("gossip publish refused")

// COVERAGE of evaluateRole (:443), which was 0.0% — the §1.3/§1.4 state
// machine, and the single largest uncovered surface in this file.
//
// 🔴 WHY THIS IS THE MOST OVERDUE FILE IN THE SLICE. Earlier this lane
// verdicted the design MATCHES, and every conjunct of that verdict was a statement
// about evaluateRole:
//
//	(a) "arms only after the corroboration window"      → :471-480
//	(b) "claims once, then waits for the settle"        → :495-503
//	(c) "a node that does not win withdraws without
//	     activating"                                    → :516-520
//
// evaluateRole had ZERO test coverage when that verdict was published and
// still had zero when the verdict was withdrawn. Three claims about an
// unexercised function — "a passing test is not a tested path", filed by the
// lane that keeps committing it. This file supplies the missing measurement.
//
// WHAT THIS FILE DOES NOT PROVE: StartTakeover has no non-test caller in loom,
// ORBTR or HSTLES, so evaluateRole does not run in any deployment. These tests
// bound the MECHANISM, not the deployment.
// That distinction is exactly what the withdrawn verdict got wrong, so it is
// recorded here rather than left for a reader to infer.
//
// Every test here stops at or before the ranking decision, and the engine's
// opener is deliberately left NIL: the winner path calls opener.Open at :523,
// so a test that ever reached it would panic on the nil rather than quietly
// decrypt. That boundary is enforced by the fixture rather than by care in
// writing each test.

// evalNode counts what reached the mesh, so a test can assert on the claim
// lifecycle rather than on internal maps alone.
type evalNode struct {
	*stubSwarmNode
	published  []swarm.Topic
	tombstoned []swarm.Topic
	pubErr     error // when set, every Publish is refused
}

func (n *evalNode) Publish(t swarm.Topic, _ []byte) error {
	if n.pubErr != nil {
		return n.pubErr
	}
	n.published = append(n.published, t)
	return nil
}

func (n *evalNode) PublishTombstone(t swarm.Topic) error {
	n.tombstoned = append(n.tombstoned, t)
	return nil
}

// loopNode is the concurrent-safe variant used by the run loop test. The
// engine publishes from its own goroutine there, so the observation has to
// cross goroutines through a channel rather than through a slice — reading
// evalNode.published from the test goroutine would be a genuine data race,
// and -race would be right to flag it.
type loopNode struct {
	*stubSwarmNode
	claims chan swarm.Topic
}

func (n *loopNode) Publish(t swarm.Topic, _ []byte) error {
	select {
	case n.claims <- t:
	default: // the test already has what it needs
	}
	return nil
}

// evalFixture builds an engine whose every dependency is inspectable.
// holders is how many nodes the role table reports for the role — the
// "covered" input to the state machine.
func evalFixture(t *testing.T, holders int) (*TakeoverEngine, *evalNode) {
	t.Helper()

	byNode := map[string]lad.RoleRecord{}
	for i := 0; i < holders; i++ {
		id := string(rune('a' + i))
		byNode[id] = lad.RoleRecord{NodeID: id}
	}

	node := &evalNode{stubSwarmNode: &stubSwarmNode{}}
	rt := &Runtime{
		identity: &NodeIdentity{NodeID: "self"},
		swarm: &SwarmIntegration{
			Node:      node,
			RoleTable: &RoleTable{byRole: map[string]map[string]lad.RoleRecord{"auth": byNode}},
		},
	}
	rt.roleActivation = &roleActivationManager{
		rt:          rt,
		activators:  map[string]ports.RoleActivator{"auth": &scriptedActivator{role: "auth", rt: rt}},
		active:      map[string]bool{},
		generations: map[string]*roleGeneration{},
	}

	e := &TakeoverEngine{
		rt: rt,
		// opener stays nil — see the credential fence note above.
		cfg: TakeoverConfig{
			Roles:               []string{"auth"},
			MinReplicas:         2,
			MaxWinners:          1,
			CorroborationWindow: time.Minute,
			ClaimSettle:         time.Minute,
			Entitled:            func(string) error { return nil },
		},
		envelopes:    map[string]*secrets.Envelope{"auth": {Role: "auth"}},
		claims:       map[string]map[string]roleClaim{},
		missingSince: map[string]time.Time{},
		claimedAt:    map[string]time.Time{},
	}
	return e, node
}

// arm drives the engine past the corroboration window without sleeping, by
// backdating the moment the role was first seen missing.
func arm(e *TakeoverEngine, role string) {
	e.missingSince[role] = time.Now().Add(-time.Hour)
}

// ─── covered: the engine must stand down ─────────────────────────────────

// A covered role must not be claimed. This is the outermost fail-safe: if it
// were wrong, every node would race to take over a role that is working.
func TestACoveredRoleIsNeverClaimed(t *testing.T) {
	e, node := evalFixture(t, 2) // holders == MinReplicas
	arm(e, "auth")               // even fully armed

	e.evaluateRole("auth")

	if len(node.published) != 0 {
		t.Errorf("published %d claims for a role that already has %d/%d holders — the "+
			"engine tries to take over a healthy role", len(node.published), 2, e.cfg.MinReplicas)
	}
	if _, still := e.missingSince["auth"]; still {
		t.Error("coverage returned but the armed state survived — the next pass would " +
			"treat a healthy role as having been missing for the whole window and take " +
			"over immediately on the next dip")
	}
	// 🔬 ADDED BECAUSE A MUTANT SURVIVED. Dropping the `hasClaimed`
	// half of the disarm condition — leaving bare `!isActive(role)` — passed
	// every test in this file. The two halves happened to agree everywhere,
	// exactly the masked-conjunction shape that let mutant I3 through in
	// claim_ingest_test.go.
	//
	// The surviving behaviour is a tombstone published on every tick, for every
	// covered role this node does not carry: a steady per-role publish storm on
	// the claim topic asserting the withdrawal of a claim that was never made.
	if len(node.tombstoned) != 0 {
		t.Errorf("published %d tombstones for a covered role this node never claimed — "+
			"withdrawal is firing on the absence of a claim rather than on the presence "+
			"of one, so every tick tombstones every covered role", len(node.tombstoned))
	}
}

// 🔴 THE DISARM-WITHDRAW. When coverage returns while we hold a claim we never
// activated, that claim must be released. Left standing, it counts in every
// peer's ranking forever: a node that is not carrying the role, and no longer
// intends to, keeps winning it.
func TestCoverageReturningWithdrawsAClaimWeNeverActivated(t *testing.T) {
	e, node := evalFixture(t, 2)
	e.claimedAt["auth"] = time.Now()

	e.evaluateRole("auth")

	if len(node.tombstoned) != 1 {
		t.Fatalf("published %d tombstones, want 1 — a pending claim survived the return "+
			"of coverage", len(node.tombstoned))
	}
	if _, still := e.claimedAt["auth"]; still {
		t.Error("claimedAt still holds the role after a successful disarm-withdraw")
	}
}

// 🔴 THE ANTI-FLAP PROPERTY, and the case that separates it from the test
// above. A role we ACTIVELY carry must NOT be handed back just because other
// holders appeared — the comment at :457 calls handback "an operator/coverage-
// pressure decision, not automatic flapping". Automatic handback would make
// two nodes coming up simultaneously each stand down for the other.
//
// 🔬 Note the fixture differs from the previous test by ONE field. That is
// deliberate: `hasClaimed && !isActive` is a conjunction, and a test that only
// ever sees it false-because-inactive cannot tell which half is doing the work.
func TestAnActivelyHeldRoleIsNotHandedBackWhenCoverageReturns(t *testing.T) {
	e, node := evalFixture(t, 2)
	e.claimedAt["auth"] = time.Now()
	e.rt.roleActivation.active["auth"] = true // the only difference

	e.evaluateRole("auth")

	if len(node.tombstoned) != 0 {
		t.Errorf("published %d tombstones for a role this node is actively carrying — "+
			"coverage appearing caused an automatic handback, so two nodes coming up "+
			"together would each stand down for the other", len(node.tombstoned))
	}
}

// ─── under-covered: corroborate before arming ────────────────────────────

// (a) The first sighting of under-coverage must only START the window. Acting
// on one observation makes the engine take over on a single gossip hiccup.
func TestTheFirstSightingOfUnderCoverageOnlyStartsTheWindow(t *testing.T) {
	e, node := evalFixture(t, 0)

	e.evaluateRole("auth")

	if len(node.published) != 0 {
		t.Errorf("claimed on the FIRST under-covered observation (%d publishes) — a single "+
			"gossip hiccup triggers a takeover", len(node.published))
	}
	if _, armed := e.missingSince["auth"]; !armed {
		t.Error("the corroboration window was never started, so the role can never arm " +
			"and takeover never happens at all")
	}
}

// (a) Still inside the window: the engine waits. The window must be measured
// from the FIRST sighting, so a second pass a moment later must not act.
func TestASecondPassInsideTheCorroborationWindowStillWaits(t *testing.T) {
	e, node := evalFixture(t, 0)

	e.evaluateRole("auth") // starts the window
	e.evaluateRole("auth") // moments later

	if len(node.published) != 0 {
		t.Errorf("claimed %d times while still inside the corroboration window", len(node.published))
	}
}

// ─── armed: the gates before claiming ────────────────────────────────────

// (b) Once the window has elapsed the engine claims — exactly ONCE. A second
// pass inside the settle must not republish; re-claiming resets nothing and
// floods the claim topic.
func TestAnArmedRoleIsClaimedExactlyOnceWithinTheSettle(t *testing.T) {
	e, node := evalFixture(t, 0)
	arm(e, "auth")

	e.evaluateRole("auth")
	e.evaluateRole("auth")

	if len(node.published) != 1 {
		t.Fatalf("published %d claims, want exactly 1 — the engine either never claimed "+
			"an armed role or re-claims on every tick", len(node.published))
	}
	if want := swarm.Topic(secrets.ClaimTopic("auth")); node.published[0] != want {
		t.Errorf("claim published to %q, want %q", node.published[0], want)
	}
	if _, ok := e.claimedAt["auth"]; !ok {
		t.Error("claimedAt was not recorded, so the settle can never elapse and this node " +
			"re-claims forever without ever ranking")
	}
}

// 🔴 A CLAIM THAT NEVER REACHED THE MESH MUST NOT BE RECORDED AS MADE, AND
// MUST BE RETRIED. This is the exact mirror of the failed-WITHDRAW
// property, on the other half of the lifecycle.
//
// If publishClaim recorded claimedAt before checking the publish error, this
// node would enter the settle believing it had claimed. When the settle
// elapsed it would rank itself against a claim set that does not contain it,
// lose, and withdraw a claim it never made — an entitled node that silently
// removes itself from a role nobody else is holding, with no retry, because
// the `!hasClaimed` gate has already been passed.
//
// The second assertion is the one that matters. "claimedAt was not set" alone
// is a statement about a field; it does not establish that a retry actually
// happens — "a fallback that EXISTS is not one that FIRES", applied to a path I
// am claiming is good rather than one I am claiming is broken.
func TestAClaimThatFailedToPublishIsNotRecordedAndIsRetried(t *testing.T) {
	e, node := evalFixture(t, 0)
	arm(e, "auth")
	node.pubErr = errTestPublishRefused

	e.evaluateRole("auth")

	if _, recorded := e.claimedAt["auth"]; recorded {
		t.Fatal("claimedAt was recorded for a claim the mesh refused — this node will " +
			"wait out the settle, rank itself against a claim set it is not in, lose, and " +
			"withdraw a claim it never made")
	}

	// The retry: with publishing restored, the very next pass must claim.
	node.pubErr = nil
	e.evaluateRole("auth")

	if len(node.published) != 1 {
		t.Errorf("published %d claims on the pass after the failure recovered, want 1 — "+
			"the claim was never retried, so a transient publish error costs this node "+
			"the role permanently", len(node.published))
	}
}

// 🔴 THE ENTITLEMENT GATE MUST FAIL CLOSED. A node that is not entitled to a
// role must not claim it — a claim from an ineligible node still occupies a
// winner slot in every peer's ranking, so it can starve an eligible node out
// of a role it could actually carry.
func TestAnUnentitledNodeNeverClaims(t *testing.T) {
	e, node := evalFixture(t, 0)
	arm(e, "auth")
	e.cfg.Entitled = func(string) error { return errTestNotEntitled }

	e.evaluateRole("auth")

	if len(node.published) != 0 {
		t.Errorf("an unentitled node published %d claims — it occupies a winner slot it "+
			"can never honour, starving an eligible node", len(node.published))
	}
}

// A node with nothing to activate with must not claim. Both halves matter and
// both are tested, because either alone would let a node win a role it cannot
// bring up — and having won, it holds the slot while the role stays down.
func TestANodeWithNothingToActivateWithNeverClaims(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*TakeoverEngine)
	}{
		{"no activator registered", func(e *TakeoverEngine) {
			e.rt.roleActivation.activators = map[string]ports.RoleActivator{}
		}},
		{"no sealed bundle received", func(e *TakeoverEngine) {
			e.envelopes = map[string]*secrets.Envelope{}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e, node := evalFixture(t, 0)
			arm(e, "auth")
			tc.mutate(e)

			e.evaluateRole("auth")

			if len(node.published) != 0 {
				t.Errorf("claimed with %s — this node can win the role and then fail to "+
					"bring it up, holding the winner slot while the role stays down", tc.name)
			}
		})
	}
}

// A role this node already carries needs no further evaluation — it must not
// re-claim its own live role.
func TestAnAlreadyActiveRoleIsNotReClaimed(t *testing.T) {
	e, node := evalFixture(t, 0)
	arm(e, "auth")
	e.rt.roleActivation.active["auth"] = true

	e.evaluateRole("auth")

	if len(node.published) != 0 {
		t.Errorf("re-claimed a role this node is already carrying (%d publishes)", len(node.published))
	}
}

// ─── the loop that drives all of it ──────────────────────────────────────

// 🔴 THE LAST UNMEASURED LINK. Every test above calls evaluateRole directly.
// That measures the state machine and says NOTHING about whether the engine
// ever calls it — the same gap, one level up, that made the the design
// verdict wrong: a mechanism measured in isolation and assumed to be driven.
//
// run is the only thing that drives evaluateRole, and it is reached only
// through StartTakeover's rt.Go. This test walks tick → evaluateRole → claim
// on the wire, and then walks the teardown: cancelling the runtime context
// must unwind the subscriptions via the deferred stopSubs.
//
// ⚠ SCOPE, STATED RATHER THAN IMPLIED: this proves the ticker drives the state
// machine. It does NOT make the engine reachable in a deployment —
// measured zero non-test callers of StartTakeover in loom, ORBTR and HSTLES,
// and nothing in this file changes that.
func TestTheTickerActuallyDrivesEvaluateRoleAndUnwindsOnContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	node := &loopNode{stubSwarmNode: &stubSwarmNode{}, claims: make(chan swarm.Topic, 4)}
	rt := &Runtime{
		ctx:      ctx,
		identity: &NodeIdentity{NodeID: "self"},
		swarm: &SwarmIntegration{
			Node:      node,
			RoleTable: &RoleTable{byRole: map[string]map[string]lad.RoleRecord{}}, // zero holders
		},
	}
	rt.roleActivation = &roleActivationManager{
		rt:          rt,
		activators:  map[string]ports.RoleActivator{"auth": &scriptedActivator{role: "auth", rt: rt}},
		active:      map[string]bool{},
		generations: map[string]*roleGeneration{},
	}

	e, err := rt.StartTakeover(TakeoverConfig{
		Roles:         []string{"auth"},
		MinReplicas:   2,
		MaxWinners:    1,
		CheckInterval: time.Millisecond,
		// 🔬 POSITIVE, NOT -1. StartTakeover maps any non-positive window to the
		// 60s default (:285), so a "already elapsed" fixture of -1 silently
		// becomes the longest window in the file and the test times out — red
		// for a fixture reason, with nothing wrong in the code it is testing.
		CorroborationWindow: time.Nanosecond,
		ClaimSettle:         time.Minute,
		Entitled:            func(string) error { return nil },
	}, secrets.NewOpener(nil, nil, func() error { return nil }))
	if err != nil {
		t.Fatalf("StartTakeover: %v", err)
	}
	e.mu.Lock()
	e.envelopes["auth"] = &secrets.Envelope{Role: "auth"}
	e.mu.Unlock()

	select {
	case got := <-node.claims:
		if want := swarm.Topic(secrets.ClaimTopic("auth")); got != want {
			t.Errorf("ticker published to %q, want %q", got, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no claim was published within 5s — the ticker never reached evaluateRole, " +
			"so the entire state machine measured in this file is driven by nothing")
	}

	// Teardown: run defers stopSubs, so cancelling the runtime context is the
	// only way the engine releases its subscriptions. There is no Stop method.
	cancel()
	rt.wg.Wait()

	e.mu.Lock()
	remaining := e.unsubs
	e.mu.Unlock()
	if remaining != nil {
		t.Errorf("%d subscriptions survived context cancellation — a stopped engine keeps "+
			"receiving claim records", len(remaining))
	}
}

// ─── ranked: conjunct (c) ───────────────────────────────────────────────────

// A node that does not win calls withdrawClaim and returns without activating.
// This measures that conjunct rather than asserting it from the source.
//
// The peer claims at rung 1; this node is unranked (rungUnset) and therefore
// sorts last. With MaxWinners 1 the peer takes the only slot.
//
// Both halves are asserted. Withdrawing but activating anyway would mean two
// nodes carry a single-winner role while only one of them advertises a claim —
// the split-brain the ranking exists to prevent.
func TestLosingTheRankingWithdrawsTheClaimAndDoesNotActivate(t *testing.T) {
	e, node := evalFixture(t, 0)
	arm(e, "auth")
	e.claimedAt["auth"] = time.Now().Add(-time.Hour) // settle already elapsed
	e.claims["auth"] = map[string]roleClaim{
		"peer-nearer": {hlc: 1, rung: 1},
		"self":        {hlc: 1, rung: rungUnset},
	}

	e.evaluateRole("auth")

	if len(node.tombstoned) != 1 {
		t.Fatalf("published %d tombstones after losing the ranking, want 1 — the losing "+
			"node keeps a live claim standing, so it stays in every peer's ranking "+
			"forever", len(node.tombstoned))
	}
	if e.rt.roleActivation.active["auth"] {
		t.Error("the LOSING node activated the role anyway — two nodes now carry a " +
			"single-winner role, and only one of them advertises a claim")
	}
	if len(node.published) != 0 {
		t.Errorf("published %d further claims after losing", len(node.published))
	}
}

// The mirror, and the case that stops the test above from passing for the
// wrong reason: a winner must NOT withdraw. If withdrawal were unconditional
// the losing-side assertion would still pass, and no node would ever take over.
//
// This test stops at the withdrawal check by design — it asserts what did NOT
// happen (no tombstone), so it never reaches the nil opener at :523. That the
// winner path is unreachable from this file is the credential fence holding,
// not an oversight.
func TestTheWINNERDoesNotWithdrawItsOwnClaim(t *testing.T) {
	e, node := evalFixture(t, 0)
	arm(e, "auth")
	e.claimedAt["auth"] = time.Now().Add(-time.Hour)
	e.claims["auth"] = map[string]roleClaim{
		"self":        {hlc: 1, rung: 1},
		"peer-losing": {hlc: 1, rung: rungUnset},
	}

	defer func() {
		// The winner path reaches opener.Open with a nil opener. Reaching it is
		// the expected outcome for a WINNER; the point of this test is that the
		// engine got there instead of withdrawing.
		_ = recover()
		if len(node.tombstoned) != 0 {
			t.Errorf("the WINNING node withdrew its own claim (%d tombstones) — if "+
				"withdrawal were unconditional no node would ever take over, and the "+
				"losing-side test would pass for entirely the wrong reason",
				len(node.tombstoned))
		}
	}()

	e.evaluateRole("auth")
}
