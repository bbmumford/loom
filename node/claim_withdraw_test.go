/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"errors"
	"testing"
	"time"

	swarm "github.com/bbmumford/swarm"

	"github.com/bbmumford/loom/secrets"
)

// COVERAGE of withdrawClaim (:545) and stopSubs (:330), both at 0.0%.
//
// 🔴 WHY THIS ONE MATTERS MORE THAN ITS LINE COUNT, AND WHY IT IS OVERDUE.
// Earlier this lane verdicted the design as MATCHES, and conjunct (c) —
// "activate only when every higher rung is gone" — rested on the statement
// that "a node that does not win calls withdrawClaim and returns without
// activating". withdrawClaim had ZERO test coverage at the time and still did
// when this file was written. That is a verdict resting on an unexercised
// path: exactly the "a passing test is not a tested path" failure, committed
// by the lane that keeps filing it. These tests supply the missing half.
//
// This file touches the CLAIM
// lifecycle only. It constructs no secret material, opens no envelope, and
// never exercises PublishRoleSecrets or onSecretRecord, which remain declined.

// withdrawNode captures tombstone publications and can be made to fail.
type withdrawNode struct {
	*stubSwarmNode
	tombstoned []swarm.Topic
	err        error
}

func (n *withdrawNode) PublishTombstone(t swarm.Topic) error {
	if n.err != nil {
		return n.err
	}
	n.tombstoned = append(n.tombstoned, t)
	return nil
}

func withdrawFixture(pubErr error) (*TakeoverEngine, *withdrawNode) {
	node := &withdrawNode{stubSwarmNode: &stubSwarmNode{}, err: pubErr}
	e := &TakeoverEngine{
		rt:        &Runtime{swarm: &SwarmIntegration{Node: node}},
		claimedAt: map[string]time.Time{"auth": time.Now()},
	}
	return e, node
}

// The happy path: withdrawing publishes a tombstone on the role's claim topic
// AND forgets the local claim, so a later evaluation does not believe this node
// is still claiming.
func TestWithdrawClaimPublishesATombstoneAndForgetsTheLocalClaim(t *testing.T) {
	e, node := withdrawFixture(nil)

	e.withdrawClaim("auth")

	if len(node.tombstoned) != 1 {
		t.Fatalf("published %d tombstones, want 1 — the mesh never learns the claim is "+
			"withdrawn, so peers keep ranking a claimant that has stood down", len(node.tombstoned))
	}
	if _, still := e.claimedAt["auth"]; still {
		t.Error("claimedAt still holds the role after a SUCCESSFUL withdraw — this node " +
			"believes it is still claiming and will not re-claim when it should")
	}
}

// 🔴 THE FAIL-CLOSED PROPERTY. If the tombstone cannot be published, the local
// claim record must SURVIVE. Clearing it on a failed publish would leave the
// node believing it withdrew while every peer still sees its live claim — the
// node stops defending a role it is still advertising, and no retry path
// exists because the engine has forgotten it ever claimed.
//
// Same shape as the failed-teardown property: the error path must not
// perform the half of the transaction that succeeded nowhere.
func TestAFailedWithdrawKeepsTheLocalClaimSoItCanBeRetried(t *testing.T) {
	boom := errors.New("gossip publish refused")
	e, node := withdrawFixture(boom)

	e.withdrawClaim("auth")

	if len(node.tombstoned) != 0 {
		t.Fatalf("fixture wrong: publish should have failed, got %d tombstones", len(node.tombstoned))
	}
	if _, still := e.claimedAt["auth"]; !still {
		t.Error("a FAILED withdraw cleared claimedAt — the node now believes it has stood " +
			"down while the mesh still sees its claim, and nothing will retry the withdraw")
	}
}

// Withdrawing one role must not disturb another. claimedAt is keyed by role and
// a whole-map clear would silently stand the node down everywhere.
func TestWithdrawingOneRoleLeavesOtherClaimsIntact(t *testing.T) {
	e, _ := withdrawFixture(nil)
	e.claimedAt["relay"] = time.Now()

	e.withdrawClaim("auth")

	if _, ok := e.claimedAt["relay"]; !ok {
		t.Error("withdrawing \"auth\" also dropped the \"relay\" claim — one stand-down " +
			"silently released every role this node holds")
	}
}

// The tombstone must go to the CLAIM topic for that exact role. A tombstone on
// the wrong topic withdraws nothing and, worse, tombstones something else.
func TestTheTombstoneTargetsTheRolesOwnClaimTopic(t *testing.T) {
	e, node := withdrawFixture(nil)

	e.withdrawClaim("auth")

	if len(node.tombstoned) != 1 {
		t.Fatalf("want exactly one tombstone, got %d", len(node.tombstoned))
	}
	got := string(node.tombstoned[0])
	if got == "" {
		t.Fatal("tombstone published to an empty topic")
	}
	// Derived from the same helper production uses, rather than a literal, so
	// the test cannot drift from the real topic scheme.
	if want := string(swarm.Topic(secrets.ClaimTopic("auth"))); got != want {
		t.Errorf("tombstone topic = %q, want %q — the withdrawal is being published "+
			"somewhere other than the role's claim topic", got, want)
	}
}

// stopSubs must run every unsubscribe exactly once and tolerate a nil entry —
// a partially-built subscription list is the normal shape when Start failed
// midway, and a panic there would strand the engine.
func TestStopSubsRunsEveryUnsubscribeAndToleratesNil(t *testing.T) {
	calls := 0
	e := &TakeoverEngine{
		unsubs: []swarm.Unsubscribe{
			func() { calls++ },
			nil, // a subscription that never established
			func() { calls++ },
		},
	}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("stopSubs panicked on a nil unsubscribe: %v — a Start that failed "+
				"midway leaves exactly this shape, so the cleanup path would strand the engine", r)
		}
	}()

	e.stopSubs()

	if calls != 2 {
		t.Errorf("ran %d unsubscribes, want 2 — a leaked subscription keeps delivering "+
			"claim records to a stopped engine", calls)
	}
	if e.unsubs != nil {
		t.Error("stopSubs left the slice populated — a second Stop would run every " +
			"unsubscribe again")
	}
}
