/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"testing"
	"time"

	lad "github.com/bbmumford/ledger"
	ladcache "github.com/bbmumford/ledger/cache"
)

// 🔑 affinityFixture, and why roleFixture could not be reused.
//
// `peerRoleAffinity` reads `RoleTable.RolesOf`, which returns
// `t.infoBy[nodeID].Roles` — the PEERINFO projection. `findNodeForRole` (which
// roleFixture was built for) reads the ROLE-RECORD index instead. `applyRecord`
// takes both as SEPARATE arguments, so roleFixture's `PeerInfo{}` leaves the
// projection empty and RolesOf returns nil.
//
// ⇒ TWO CONSUMERS, TWO INDEXES, ONE CALL THAT POPULATES THEM INDEPENDENTLY. A
// fixture that fully satisfies one consumer is silently empty for the other,
// which is exactly why every test below opens with a premise check: my first
// version used roleFixture and failed saying "the fixture is not reaching
// peerRoleAffinity", which was true and was the fixture's fault.
func affinityFixture(t *testing.T, role string, nodeIDs ...string) *ConnectionManager {
	t.Helper()
	table, err := NewRoleTable(nil, testNodeIDA)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range nodeIDs {
		table.applyRecord(
			lad.RoleRecord{NodeID: id, Roles: []string{role}, Updated: time.Now()},
			PeerInfo{NodeID: id, Roles: []string{role}},
		)
		if got := table.RolesOf(id); len(got) == 0 {
			t.Fatalf("fixture did not populate RolesOf(%s) — peerRoleAffinity "+
				"reads exactly this and every assertion would be vacuous", id)
		}
	}
	m := registerTestManager()
	m.selfID = testNodeIDA
	m.peers = map[string]*peerConn{}
	m.rt = &Runtime{swarm: &SwarmIntegration{RoleTable: table}, cache: ladcache.NewDirectoryCache()}
	m.rt.connMgr = m
	return m
}

// COVERAGE of role affinity, 7 functions at 0.0%.
//
// CENSUSED FIRST, per symbol. Live on the rebalance path:
//
//	peerRoleAffinity            <- connection_scaling.go:217
//	recordRebalanceObservation  <- connection_scaling.go:218
//	rebalanceTicks.Add(1)       <- connection_scaling.go:359
//	snapshot()                  -> runtime.go:2167-2171, the five
//	                               role_affinity_* MeshMetrics keys
//	InvalidateLocalRoleCache    <- peer_publisher.go:190
//
// ⇒ these tiers decide WHICH PEERS SURVIVE a scale-down, and the counters are
// the only way an operator can see whether the bonus is firing at all.
//
// 🙋 One hypothesis I formed and killed before writing anything: I expected
// `rebalanceTicks` to be a never-incremented field.
// Measured: `connection_scaling.go:359` increments it, and `runtime.go:2167`
// reads it. The chain is live end to end. Recording that because the
// expectation was wrong and the census is what said so.

// The three tiers stack independently, so each combination is a distinct
// scale-down verdict. Bonuses are additive on top of the latency/grade base,
// and the total is what TargetConnections adds.
func TestAffinityTiersStackIndependently(t *testing.T) {
	for _, tc := range []struct {
		name string
		a    roleAffinity
		want int
	}{
		{"no roles at all", roleAffinity{}, 0},
		{"carries a role", roleAffinity{carryAny: true}, 1},
		{"carries and co-regional", roleAffinity{carryAny: true, sameRegion: true}, 2},
		{"unique remote capability too", roleAffinity{carryAny: true, sameRegion: true, dispatchTarget: true}, 3},
		{"dispatch target without co-region", roleAffinity{carryAny: true, dispatchTarget: true}, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.a.total(); got != tc.want {
				t.Fatalf("total() = %d, want %d — the bonus a peer receives at "+
					"rebalance is wrong, so the wrong peer is drained under "+
					"scale-down pressure", got, tc.want)
			}
		})
	}
}

// 🔴 A PEER WITH NO OBSERVED ROLES MUST GET ZERO BONUS, not a partial one.
// The doc is explicit that those cases "collapse to today's behaviour" — a
// non-zero bonus for an unknown peer would preserve connections on the
// strength of information the RoleTable never had.
func TestAPeerWithNoPublishedRolesGetsNoBonus(t *testing.T) {
	m := affinityFixture(t, "auth", testNodeIDB)

	const unknown = "cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33"
	if got := m.peerRoleAffinity(unknown, "syd").total(); got != 0 {
		t.Fatalf("an unobserved peer scored %d — the rebalance is preserving a "+
			"connection on role information that does not exist", got)
	}
}

// A nil RoleTable (dev/minimal builds) must be a zero bonus, not a panic:
// peerRoleAffinity runs on the Rebalance loop of every node.
func TestAffinityIsZeroRatherThanPanickingWithoutASwarm(t *testing.T) {
	m := registerTestManager()
	if got := m.peerRoleAffinity(testNodeIDB, "syd").total(); got != 0 {
		t.Fatalf("total() = %d with no swarm wired, want 0", got)
	}
	var nilMgr *ConnectionManager
	if got := nilMgr.peerRoleAffinity(testNodeIDB, "syd").total(); got != 0 {
		t.Fatalf("total() = %d on a nil manager, want 0 — and it must not "+
			"panic, because the guard exists precisely for that", got)
	}
}

// 🔑 THE dispatchTarget TIER IS THE ONE WITH REAL CONSEQUENCES: a peer holding
// a role we do NOT serve is unique remote capability, and losing it costs the
// mesh a capability rather than a redundant path. A peer whose roles we all
// serve locally must NOT get the tier.
func TestDispatchTargetFiresOnlyForRolesWeDoNotServeLocally(t *testing.T) {
	m := affinityFixture(t, "billing", testNodeIDB)
	m.selfRegion = "syd"

	// We serve nothing locally → the peer's "billing" is unique capability.
	m.rt.cfg.Roles = nil
	m.InvalidateLocalRoleCache()
	unique := m.peerRoleAffinity(testNodeIDB, "syd")
	if !unique.carryAny {
		t.Fatal("carryAny did not fire for a peer with an observed role — the " +
			"RoleTable fixture is not reaching peerRoleAffinity and the rest of " +
			"this test is vacuous")
	}
	if !unique.dispatchTarget {
		t.Fatal("dispatchTarget did not fire for a role we do not serve — " +
			"unique remote capability ranks the same as a redundant peer, so " +
			"scale-down can drop the only node serving a role")
	}

	// Now we serve "billing" ourselves → the peer is redundancy, not capability.
	m.rt.cfg.Roles = []string{"billing"}
	m.InvalidateLocalRoleCache()
	redundant := m.peerRoleAffinity(testNodeIDB, "syd")
	if redundant.dispatchTarget {
		t.Fatal("dispatchTarget fired for a role we serve locally — every " +
			"redundant peer now outranks peers with unique capability")
	}
	if !redundant.carryAny {
		t.Fatal("carryAny stopped firing when we began serving the role too")
	}
	if redundant.total() >= unique.total() {
		t.Fatalf("a redundant peer scored %d against %d for unique capability — "+
			"the tier is not changing the verdict",
			redundant.total(), unique.total())
	}
}

// sameRegion requires a NON-EMPTY match. An unknown region must not accidentally
// equal an unknown local region and hand out a co-region bonus to everyone.
func TestSameRegionRequiresBothRegionsToBeKnownAndEqual(t *testing.T) {
	m := affinityFixture(t, "auth", testNodeIDB)

	m.selfRegion = "syd"
	if got := m.peerRoleAffinity(testNodeIDB, "syd"); !got.sameRegion {
		t.Fatal("sameRegion did not fire for a co-regional peer")
	}
	if got := m.peerRoleAffinity(testNodeIDB, "iad"); got.sameRegion {
		t.Fatal("sameRegion fired across regions")
	}
	if got := m.peerRoleAffinity(testNodeIDB, ""); got.sameRegion {
		t.Fatal("sameRegion fired for a peer with an UNKNOWN region — an " +
			"unknown region is not a match, and treating it as one gives the " +
			"intra-region bonus to every peer we have no region for")
	}

	// 🔴 The empty==empty trap: with no local region either, an unknown peer
	// region must still not match.
	m.selfRegion = ""
	m.InvalidateLocalRoleCache()
	if got := m.peerRoleAffinity(testNodeIDB, ""); got.sameRegion {
		t.Fatal("two UNKNOWN regions compared equal — every peer would receive " +
			"the co-region bonus on a node whose own region is unset, which is " +
			"exactly the state a misconfigured deployment is in")
	}
}

// ── The local-role cache ────────────────────────────────────────────────────

// 🔴 THE CACHE MUST BE INVALIDATED WHEN LOCAL ROLES CHANGE, or affinity keeps
// scoring against a role set the node no longer publishes. peer_publisher.go:190
// is the one caller, and this is what it buys.
func TestInvalidatingTheCacheMakesTheNextLookupSeeNewLocalRoles(t *testing.T) {
	m := affinityFixture(t, "billing", testNodeIDB)
	m.selfRegion = "syd"

	m.rt.cfg.Roles = nil
	m.InvalidateLocalRoleCache()
	if !m.peerRoleAffinity(testNodeIDB, "syd").dispatchTarget {
		t.Fatal("premise wrong: dispatchTarget should fire before we serve the role")
	}

	// Take on the role locally WITHOUT invalidating: the cached set is stale,
	// so the verdict must not change yet. This pins that the cache is real.
	m.rt.cfg.Roles = []string{"billing"}
	if !m.peerRoleAffinity(testNodeIDB, "syd").dispatchTarget {
		t.Fatal("the verdict changed without an invalidation — the cache is " +
			"not caching, so every per-peer computation rebuilds the local role " +
			"set on the rebalance hot path")
	}

	m.InvalidateLocalRoleCache()
	if m.peerRoleAffinity(testNodeIDB, "syd").dispatchTarget {
		t.Fatal("dispatchTarget still fires after invalidation — the cache is " +
			"never rebuilt, so affinity scores against the role set the node " +
			"had at boot forever")
	}
}

// localRoleSet must return an empty set, never nil, and must drop empty role
// strings — an empty role in the set would make every peer's role "served".
func TestLocalRoleSetIsEmptyNotNilAndDropsBlankRoles(t *testing.T) {
	m := affinityFixture(t, "auth", testNodeIDB)

	m.rt.cfg.Roles = []string{"anchor", "", "auth"}
	m.InvalidateLocalRoleCache()

	set := m.localRoleSet()
	if set == nil {
		t.Fatal("localRoleSet returned nil — the caller indexes it directly")
	}
	if _, blank := set[""]; blank {
		t.Fatal("the empty role made it into the local set — it would then " +
			"'serve' nothing while still occupying the map, and a peer " +
			"publishing an empty role would read as redundant")
	}
	if len(set) != 2 {
		t.Fatalf("local role set = %v, want the two non-empty roles", set)
	}

	var nilMgr *ConnectionManager
	if got := nilMgr.localRoleSet(); got == nil {
		t.Fatal("localRoleSet on a nil manager returned nil rather than an " +
			"empty set — the caller would index a nil map")
	}
}

// ── Telemetry ───────────────────────────────────────────────────────────────

// The five counters are the only operator-visible evidence that the bonus is
// firing. Each must count its own tier and nothing else.
func TestTelemetryCountsEachTierSeparatelyAndSumsTheBonus(t *testing.T) {
	var tel roleAffinityTelemetry

	tel.recordRebalanceObservation(roleAffinity{carryAny: true})
	tel.recordRebalanceObservation(roleAffinity{carryAny: true, sameRegion: true})
	tel.recordRebalanceObservation(roleAffinity{carryAny: true, sameRegion: true, dispatchTarget: true})
	tel.recordRebalanceObservation(roleAffinity{}) // no tiers: must count nothing

	_, carryAny, sameRegion, dispatch, totalBonus := tel.snapshot()
	if carryAny != 3 {
		t.Errorf("carryAnyHits = %d, want 3", carryAny)
	}
	if sameRegion != 2 {
		t.Errorf("sameRegionHits = %d, want 2 — the tiers are not counted "+
			"independently", sameRegion)
	}
	if dispatch != 1 {
		t.Errorf("dispatchHits = %d, want 1", dispatch)
	}
	if totalBonus != 1+2+3 {
		t.Errorf("totalBonus = %d, want 6 — the aggregate an operator reads to "+
			"quantify the feature's effect is wrong", totalBonus)
	}
}

// rebalanceTicks is incremented by the scaler (connection_scaling.go:359), NOT
// by recordRebalanceObservation — so a per-peer observation must not inflate
// the per-tick denominator. Getting this wrong makes the hit RATIO meaningless.
func TestPerPeerObservationsDoNotIncrementTheTickCounter(t *testing.T) {
	var tel roleAffinityTelemetry
	for i := 0; i < 5; i++ {
		tel.recordRebalanceObservation(roleAffinity{carryAny: true})
	}

	ticks, carryAny, _, _, _ := tel.snapshot()
	if ticks != 0 {
		t.Fatalf("rebalanceTicks = %d after 5 per-peer observations, want 0 — "+
			"the tick counter is the DENOMINATOR for the hit ratios, and "+
			"counting peers in it makes every ratio wrong", ticks)
	}
	if carryAny != 5 {
		t.Fatalf("carryAnyHits = %d, want 5", carryAny)
	}

	tel.rebalanceTicks.Add(1) // what the scaler does, once per tick
	if ticks, _, _, _, _ := tel.snapshot(); ticks != 1 {
		t.Fatalf("rebalanceTicks = %d after one scaler increment, want 1", ticks)
	}
}

// 🔴 THE PUBLISHER PATH, AND A MUTANT IS WHY THIS TEST EXISTS.
//
// publishedRolesSnapshot has two arms: it returns `pub.Roles()` DIRECTLY when a
// PeerPublisher is wired and non-empty, and only the `rt.cfg.Roles` FALLBACK
// filters blank strings. So localRoleSet's own `r != ""` check is redundant on
// the fallback arm and LOAD-BEARING on the publisher arm.
//
// My first test suite only drove the fallback, so the mutant that deletes
// localRoleSet's blank filter SURVIVED — not because it is equivalent, but
// because my tests never reached the arm where it matters. That is the same
// scope error, caught here by a mutant instead of by a reader.
func TestABlankRoleFromThePublisherIsStillDroppedFromTheLocalSet(t *testing.T) {
	m := affinityFixture(t, "billing", testNodeIDB)
	m.selfRegion = "syd"

	// A publisher whose advertised set contains a blank. Set the field
	// directly: SetRoles calls PublishNow, which needs a live swarm node.
	m.rt.swarm.Publisher = &PeerPublisher{roles: []string{"anchor", "", "auth"}}
	m.rt.cfg.Roles = nil // force the publisher arm
	m.InvalidateLocalRoleCache()

	set := m.localRoleSet()
	if _, blank := set[""]; blank {
		t.Fatal("a blank role from the PUBLISHER reached the local role set — " +
			"peerRoleAffinity then treats a peer publishing an empty role as " +
			"redundant capability rather than unique, and localRoleSet's blank " +
			"filter is the only thing standing between the two")
	}
	if len(set) != 2 {
		t.Fatalf("local set = %v, want exactly the two non-blank publisher roles", set)
	}

	// And the publisher arm must WIN over cfg.Roles when non-empty — otherwise
	// a role added at runtime (anchor at boot) never affects affinity.
	m.rt.cfg.Roles = []string{"billing"}
	m.InvalidateLocalRoleCache()
	if _, fromCfg := m.localRoleSet()["billing"]; fromCfg {
		t.Fatal("cfg.Roles leaked into the set while a non-empty publisher was " +
			"wired — the publisher is the source of truth for the actively " +
			"advertised set, and mixing them scores affinity against roles this " +
			"node is not advertising")
	}
}

// The fallback arm: with no publisher, cfg.Roles is the source. Pinned so the
// precedence in the test above cannot be satisfied by ignoring cfg entirely.
func TestWithNoPublisherTheConfiguredRolesAreUsed(t *testing.T) {
	m := affinityFixture(t, "billing", testNodeIDB)
	m.rt.swarm.Publisher = nil
	m.rt.cfg.Roles = []string{"billing"}
	m.InvalidateLocalRoleCache()

	if _, ok := m.localRoleSet()["billing"]; !ok {
		t.Fatal("cfg.Roles was ignored with no publisher wired — a node in the " +
			"pre-PublishRPCHandlersToLAD boot phase scores every peer as unique " +
			"capability, inflating the bonus for all of them")
	}
	var nilRT *Runtime
	if got := nilRT.publishedRolesSnapshot(); got != nil {
		t.Fatalf("publishedRolesSnapshot on a nil Runtime = %v, want nil", got)
	}
}
