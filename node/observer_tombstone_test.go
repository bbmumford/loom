/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"errors"
	"testing"
	"time"

	"github.com/bbmumford/swarm"

	ladcache "github.com/bbmumford/ledger/cache"
)

// COVERAGE of the observer-tombstone pair: shouldEmitObserverTombstone
// (:1326) and emitPeerTombstone (:1352), both 0.0%.
//
// ⚠ DUPLICATION CHECK: no existing node/ test names either.
//
// 🔴 WHY THIS PAIR IS WORTH MORE THAN ITS LINE COUNT. A tombstone is an
// ATTESTATION: this node telling the whole mesh that a peer is dead. A false
// positive evicts a live peer from the LAD directory and feeds a K-of-N quorum
// that drains the swarm RoleTable and AddressTable. Every branch in the gate is
// therefore a refusal, and each refusal is the mesh declining to accuse a peer
// on insufficient evidence.
//
// ✅ THE CONTRACT BETWEEN THE TWO, MEASURED FIRST: emitPeerTombstone's doc says
// "Callers MUST gate via shouldEmitObserverTombstone before invoking this — the
// helper does no gating of its own." There is exactly ONE non-test caller
// (peer_connections.go:1449) and it IS gated (:1448). The contract holds today;
// these tests are what keep it holding, since nothing in the code enforces it.

// tombstoneNode embeds the package's existing swarm-node stub and overrides
// only the two methods this pair touches, rather than reimplementing ~25.
type tombstoneNode struct {
	*stubSwarmNode
	role      swarm.Role
	published []swarm.NodeID
	pubErr    error
}

func (n *tombstoneNode) SelfRole() swarm.Role { return n.role }

func (n *tombstoneNode) PublishObserverTombstone(_ swarm.Topic, id swarm.NodeID) error {
	if n.pubErr != nil {
		return n.pubErr
	}
	n.published = append(n.published, id)
	return nil
}

func tombstoneFixture(role swarm.Role) (*ConnectionManager, *tombstoneNode, *ladcache.DirectoryCache) {
	node := &tombstoneNode{stubSwarmNode: &stubSwarmNode{}, role: role}
	cache := ladcache.NewDirectoryCache()
	m := &ConnectionManager{
		peers: map[string]*peerConn{},
		rt:    &Runtime{swarm: &SwarmIntegration{Node: node}, cache: cache},
	}
	return m, node, cache
}

// seenAgo plants a gossip-liveness timestamp directly in the cache store, which
// is the only way to test the silence threshold without sleeping for a minute.
func seenAgo(c *ladcache.DirectoryCache, nodeID string, d time.Duration) {
	c.Store().PutGossipSeen(nodeID, time.Now().Add(-d))
}

// 🔴 EVERY REFUSAL PATH. The gate must fail CLOSED on each one independently —
// a node that cannot establish its own authority or the peer's silence must not
// accuse.
func TestTheTombstoneGateRefusesOnEveryKindOfMissingEvidence(t *testing.T) {
	const peer = "peer-1"

	t.Run("no runtime", func(t *testing.T) {
		m := &ConnectionManager{peers: map[string]*peerConn{}}
		if m.shouldEmitObserverTombstone(peer) {
			t.Error("attested with no runtime at all")
		}
	})

	t.Run("no swarm", func(t *testing.T) {
		m, _, _ := tombstoneFixture(swarm.RoleAnchor)
		m.rt.swarm = nil
		if m.shouldEmitObserverTombstone(peer) {
			t.Error("attested with no swarm integration — there is no fabric to publish on")
		}
	})

	t.Run("no swarm node", func(t *testing.T) {
		m, _, _ := tombstoneFixture(swarm.RoleAnchor)
		m.rt.swarm.Node = nil
		if m.shouldEmitObserverTombstone(peer) {
			t.Error("attested with a nil swarm node")
		}
	})

	t.Run("no cache", func(t *testing.T) {
		m, _, _ := tombstoneFixture(swarm.RoleAnchor)
		m.rt.cache = nil
		if m.shouldEmitObserverTombstone(peer) {
			t.Error("attested with no LAD cache — there is no gossip evidence to reason from")
		}
	})
}

// 🔑 ONLY AN ANCHOR MAY ATTEST — AND THE ZERO-VALUE ROLE IS NOT AN ANCHOR.
//
// This is the fail-closed mirror of a defect class this estate has been bitten
// by: a role enum whose unset zero value silently DISABLES a mechanism. Here the
// same zero value silently disables ATTESTATION, which is the correct direction
// — an unconfigured or not-yet-roled node must not accuse peers. Pinned because
// the correctness depends entirely on RoleAnchor not being the zero value.
func TestOnlyAnAnchorAttestsAndTheZeroValueRoleDoesNot(t *testing.T) {
	const peer = "peer-1"

	if swarm.Role(0) == swarm.RoleAnchor {
		t.Fatal("RoleAnchor IS the zero value — every unconfigured node would now attest " +
			"peers dead by default; this gate's fail-closed behaviour has inverted")
	}

	for _, tc := range []struct {
		name string
		role swarm.Role
		want bool
	}{
		{"anchor attests", swarm.RoleAnchor, true},
		{"the unset zero-value role does not", swarm.Role(0), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, _, cache := tombstoneFixture(tc.role)
			seenAgo(cache, peer, 5*time.Minute) // long past the silence threshold

			if got := m.shouldEmitObserverTombstone(peer); got != tc.want {
				t.Errorf("shouldEmitObserverTombstone = %v, want %v", got, tc.want)
			}
		})
	}
}

// 🔴 "NEVER SEEN IN GOSSIP" MUST NOT READ AS "SILENT FOR A LONG TIME".
//
// The zero timestamp is ABSENCE OF EVIDENCE, and the code refuses on it
// deliberately: "A peer we have an aether session to but no gossip from is most
// likely a not-yet-bootstrapped fresh peer." Treating zero as an ordinary
// timestamp would make time.Since(zero) enormous — comfortably past the
// threshold — and every JOINING peer would be attested dead by every anchor it
// connected to.
//
// That is this estate's recurring defect shape (a zero meaning "unknown" used in
// a comparison where zero is an extreme), caught here on the correct side.
func TestAPeerNeverSeenInGossipIsNotAttestedDead(t *testing.T) {
	m, _, cache := tombstoneFixture(swarm.RoleAnchor)

	if got := cache.LastGossipSeen("fresh-peer"); !got.IsZero() {
		t.Fatalf("fixture is wrong: LastGossipSeen = %v, want zero", got)
	}
	if m.shouldEmitObserverTombstone("fresh-peer") {
		t.Error("a peer never seen in gossip was attested DEAD — time.Since(zero) is ~2000 " +
			"years, so without the IsZero guard every JOINING peer is accused by every " +
			"anchor it connects to")
	}
}

// The threshold is a minimum silence, not a maximum: >= attests, < refuses.
func TestTheSilenceThresholdIsAMinimumNotAWindow(t *testing.T) {
	const peer = "peer-1"

	for _, tc := range []struct {
		name string
		ago  time.Duration
		want bool
	}{
		{"well inside the threshold", observerSilenceThreshold / 2, false},
		{"just inside", observerSilenceThreshold - 5*time.Second, false},
		{"comfortably past", observerSilenceThreshold + 5*time.Second, true},
		{"very stale", 10 * observerSilenceThreshold, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, _, cache := tombstoneFixture(swarm.RoleAnchor)
			seenAgo(cache, peer, tc.ago)

			if got := m.shouldEmitObserverTombstone(peer); got != tc.want {
				t.Errorf("silent for %v: shouldEmitObserverTombstone = %v, want %v — a peer "+
					"mid-reconnect must not be accused, and a genuinely dead one must be",
					tc.ago, got, tc.want)
			}
		})
	}
}

// ─── emitPeerTombstone ───────────────────────────────────────────────────

// Both signals must fire: LAD (drained by EvictPeer immediately) and swarm
// (drained by PublishObserverTombstone + K-of-N quorum). Emitting only one
// leaves the two directories disagreeing about whether the peer exists.
func TestEmitPeerTombstoneFiresBothSignals(t *testing.T) {
	m, node, cache := tombstoneFixture(swarm.RoleAnchor)
	const peer = "peer-1"
	seenAgo(cache, peer, 5*time.Minute)

	m.emitPeerTombstone(peer, "keepalive-dead")

	if len(node.published) != 1 || string(node.published[0]) != peer {
		t.Errorf("swarm observer tombstone not published: %v — the RoleTable and AddressTable "+
			"would never drain the peer", node.published)
	}
	if !cache.LastGossipSeen(peer).IsZero() {
		t.Error("EvictPeer did not clear the peer's LAD gossip liveness — the LAD directory " +
			"still reports the peer alive while the swarm side has been told it is dead")
	}
}

// 📌 CHARACTERISATION, AND IT QUALIFIES THE DOC COMMENT.
// emitPeerTombstone's doc says it "Pairs both signals so LAD ... and swarm ...
// converge in lock-step." On the SUCCESS path they do. On the PUBLISH-FAILURE
// path they cannot: EvictPeer has already run when PublishObserverTombstone
// returns an error, and the function logs and returns.
//
// So a failed publish leaves LAD evicted and the swarm side untouched — the two
// directories disagree, which is exactly the state "lock-step" describes
// avoiding. There is no retry and no rollback on that path.
//
// I am pinning the behaviour, not asserting it is wrong: evicting locally on a
// peer you believe dead is defensible even when you cannot tell anyone, and the
// next sweep re-attempts. The doc is what overreaches — "lock-step" is true of
// the happy path only.
func TestAFailedPublishStillLeavesTheLocalEvictionApplied(t *testing.T) {
	m, node, cache := tombstoneFixture(swarm.RoleAnchor)
	node.pubErr = errors.New("swarm publish failed")
	const peer = "peer-1"
	seenAgo(cache, peer, 5*time.Minute)

	m.emitPeerTombstone(peer, "keepalive-dead")

	if len(node.published) != 0 {
		t.Fatalf("fixture is wrong: publish should have failed, got %v", node.published)
	}
	if !cache.LastGossipSeen(peer).IsZero() {
		t.Error("fixture is wrong: EvictPeer should already have run before the publish")
	}
	t.Log("CHARACTERISED: LAD evicted, swarm NOT told — the two directories disagree after a " +
		"failed publish, so the doc's \"lock-step\" holds on the success path only")
}

// A nil runtime must not panic — the manager is constructed before rt is wired.
func TestEmitPeerTombstoneIsSafeWithNoRuntime(t *testing.T) {
	m := &ConnectionManager{peers: map[string]*peerConn{}}
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("emitPeerTombstone panicked with no runtime: %v", r)
		}
	}()
	m.emitPeerTombstone("peer-1", "keepalive-dead")
}
