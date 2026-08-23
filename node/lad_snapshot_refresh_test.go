/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	lad "github.com/bbmumford/ledger"
	ladcache "github.com/bbmumford/ledger/cache"
)

// Covers LADSnapshotCache.refresh and .loop.
//
// These need a live *ladcache.DirectoryCache rather than a fake, and
// `ladcache.NewDirectoryCache()` takes no arguments and returns an in-memory
// cache, so the fixture below is a single constructor call.
//
// This is the read path underneath everything: refresh pulls all four LAD
// layers concurrently under a per-call timeout and publishes one immutable
// snapshot, which HealthEvaluator and the /mesh-* handlers then read
// lock-free.

func snapshotCacheWith(t *testing.T, records ...lad.Record) (*LADSnapshotCache, *ladcache.DirectoryCache) {
	t.Helper()
	dir := ladcache.NewDirectoryCache()
	for _, r := range records {
		if err := dir.Apply(r); err != nil {
			t.Fatalf("Apply(%s) rejected: %v — the fixture never reached the "+
				"cache and every assertion below would be vacuous", r.Topic, err)
		}
	}
	c := NewLADSnapshotCache(dir, LADSnapshotCacheConfig{
		RefreshInterval: 5 * time.Millisecond, PerCallTimeout: time.Second,
	})
	t.Cleanup(c.Stop)
	return c, dir
}

func memberRecord(t *testing.T, nodeID, svc string) lad.Record {
	t.Helper()
	body, err := json.Marshal(lad.MemberRecord{
		NodeID: nodeID, Attrs: map[string]string{"serviceName": svc},
	})
	if err != nil {
		t.Fatal(err)
	}
	return lad.Record{Topic: lad.TopicMember, NodeID: nodeID, Body: body, Timestamp: time.Now()}
}

func latencyRecord(t *testing.T, from, to string) lad.Record {
	t.Helper()
	body, err := json.Marshal(lad.LatencyRecord{
		FromNode: from, ToNode: to, RTTMs: 12, Transport: "websocket",
		MeasuredAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	return lad.Record{Topic: lad.TopicLatency, NodeID: from, Body: body, Timestamp: time.Now()}
}

// The Warm transition is the contract. Before a successful refresh the
// snapshot must report Warm=false; after one it must report true. That flag is
// the only thing letting a consumer distinguish "the mesh is empty" from "we
// have not looked yet", and HealthEvaluator reads it for exactly that.
func TestRefreshTurnsTheSnapshotWarmAndPublishesTheLayers(t *testing.T) {
	c, _ := snapshotCacheWith(t,
		memberRecord(t, testNodeIDB, "auth.hstles.com"),
		latencyRecord(t, testNodeIDA, testNodeIDB),
	)

	before := c.Snapshot()
	if before.Warm {
		t.Fatal("premise wrong: the bootstrap snapshot is already Warm, so the " +
			"transition below cannot be observed")
	}

	c.refresh()

	after := c.Snapshot()
	if after == before {
		t.Fatal("refresh() did not publish a new snapshot — readers keep the " +
			"bootstrap forever and every observability surface reports an " +
			"empty mesh")
	}
	if !after.Warm {
		t.Fatalf("Warm is still false after a successful refresh (errors: %v) — "+
			"consumers cannot distinguish an empty mesh from an unread one",
			after.Errors)
	}
	if len(after.Errors) != 0 {
		t.Fatalf("refresh reported per-method errors against an in-memory "+
			"directory: %v", after.Errors)
	}
	if len(after.Members) != 1 || after.Members[0].NodeID != testNodeIDB {
		t.Fatalf("Members = %+v, want the one applied member — Layer 1 is not "+
			"reaching the snapshot", after.Members)
	}
	if len(after.Latency) != 1 {
		t.Fatalf("Latency = %+v, want the one applied edge — Layer 4's "+
			"mesh-wide half is not reaching the snapshot", after.Latency)
	}
	if after.BuiltAt.Before(before.BuiltAt) {
		t.Fatal("BuiltAt went backwards across a refresh")
	}
	if after.BuildDuration <= 0 {
		t.Fatal("BuildDuration is not positive — the only signal an operator " +
			"has for how long a directory read is taking is dead")
	}
}

// Gossip liveness (Layer 2) is a map, and a nil map on a warm snapshot is
// indistinguishable from "nobody is alive" to health_evaluator.go's range.
//
// 🙋 My first version of this test applied a MEMBER record and expected
// liveness to follow. It does not, and the code is right: `lastGossipAt` is
// written only by the explicit `RecordGossipSeen(nodeID)` — holding a record
// ABOUT a node is not the same as having HEARD FROM it, which is exactly the
// distinction Layer 1 and Layer 2 exist to keep apart. The fixture was wrong,
// not the code; fifth such case this session.
func TestRefreshPopulatesGossipLivenessForNodesSeenInGossip(t *testing.T) {
	c, dir := snapshotCacheWith(t, memberRecord(t, testNodeIDB, "auth.hstles.com"))
	dir.RecordGossipSeen(testNodeIDB)
	c.refresh()

	snap := c.Snapshot()
	if snap.GossipLiveness == nil {
		t.Fatal("GossipLiveness is nil on a WARM snapshot — Layer 2 reads as " +
			"'nobody has been seen', which is how a healthy mesh gets reported " +
			"as degraded (health_evaluator.go's gossipAlive map stays empty)")
	}
	if _, ok := snap.GossipLiveness[testNodeIDB]; !ok {
		t.Fatalf("no liveness entry for the applied member (%d entries) — a "+
			"node that just gossiped reads as silent",
			len(snap.GossipLiveness))
	}
}

// 🔑 SNAPSHOTS ARE IMMUTABLE AND PUBLISHED BY POINTER SWAP. A reader holding
// the old pointer must keep seeing the old contents — the type's own doc says
// readers "must never mutate" because the slices are shared by reference, and
// that only holds if refresh publishes a NEW snapshot rather than editing one.
func TestARefreshDoesNotMutateAPreviouslyHandedOutSnapshot(t *testing.T) {
	c, dir := snapshotCacheWith(t, memberRecord(t, testNodeIDB, "auth.hstles.com"))
	c.refresh()

	held := c.Snapshot()
	heldMembers := len(held.Members)

	if err := dir.Apply(memberRecord(t, testNodeIDA, "billing.hstles.com")); err != nil {
		t.Fatal(err)
	}
	c.refresh()

	if got := len(held.Members); got != heldMembers {
		t.Fatalf("the snapshot a reader was already holding changed from %d to "+
			"%d members — refresh is mutating published state, so a consumer "+
			"iterating it races the refresh goroutine", heldMembers, got)
	}
	if got := len(c.Snapshot().Members); got != heldMembers+1 {
		t.Fatalf("the NEW snapshot has %d members, want %d — the second "+
			"refresh did not pick up the applied record",
			got, heldMembers+1)
	}
}

// Age must track the published snapshot, not the cache: after a refresh the
// age restarts. This is what a staleness check on /mesh-status reads.
func TestAgeRestartsAfterEachRefresh(t *testing.T) {
	c, _ := snapshotCacheWith(t, memberRecord(t, testNodeIDB, "auth.hstles.com"))
	c.refresh()
	time.Sleep(15 * time.Millisecond)

	aged := c.Snapshot().Age()
	if aged < 10*time.Millisecond {
		t.Fatalf("Age() = %v after a 15ms sleep — the staleness signal is not "+
			"advancing", aged)
	}

	c.refresh()
	if fresh := c.Snapshot().Age(); fresh >= aged {
		t.Fatalf("Age() = %v after a fresh refresh, was %v before — a stale "+
			"snapshot never reads as refreshed", fresh, aged)
	}
}

// 🔴 loop MUST REFRESH IMMEDIATELY, not on the first tick. With the live 10s
// interval, waiting for the ticker would leave every observability surface
// reporting an empty mesh for the node's first ten seconds.
func TestStartRefreshesImmediatelyRatherThanWaitingForTheFirstTick(t *testing.T) {
	dir := ladcache.NewDirectoryCache()
	if err := dir.Apply(memberRecord(t, testNodeIDB, "auth.hstles.com")); err != nil {
		t.Fatal(err)
	}
	// An interval far longer than the test: only an immediate first refresh
	// can make this snapshot warm.
	c := NewLADSnapshotCache(dir, LADSnapshotCacheConfig{
		RefreshInterval: time.Hour, PerCallTimeout: time.Second,
	})
	defer c.Stop()

	c.Start()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !c.Snapshot().Warm {
		time.Sleep(time.Millisecond)
	}
	if !c.Snapshot().Warm {
		t.Fatal("the snapshot was still cold two seconds after Start() with a " +
			"one-hour interval — loop() is waiting for its first tick, so a " +
			"real node would report an empty mesh for its first 10 seconds")
	}
}

// The push path: a topic change on the directory must wake the loop, so a new
// record shows up without waiting a full interval. The subscriber is wired in
// the constructor and coalesces bursts through a size-1 channel.
func TestATopicChangeWakesTheLoopWithoutWaitingForTheTicker(t *testing.T) {
	dir := ladcache.NewDirectoryCache()
	c := NewLADSnapshotCache(dir, LADSnapshotCacheConfig{
		RefreshInterval: time.Hour, PerCallTimeout: time.Second,
	})
	defer c.Stop()
	c.Start()

	// Wait out the immediate first refresh so the next change is the only
	// thing that can move the member count.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !c.Snapshot().Warm {
		time.Sleep(time.Millisecond)
	}
	if !c.Snapshot().Warm {
		t.Fatal("premise wrong: never warmed, so a push cannot be distinguished")
	}

	if err := dir.Apply(memberRecord(t, testNodeIDB, "auth.hstles.com")); err != nil {
		t.Fatal(err)
	}

	// The loop coalesces for snapshotCoalesceWindow (500ms) before refreshing.
	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if len(c.Snapshot().Members) == 1 {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("the applied member never reached the snapshot (%d members) — the "+
		"push path is dead and every change waits for the 10s ticker, which "+
		"the ticker's own doc calls a safety net rather than the mechanism",
		len(c.Snapshot().Members))
}

// 🔑 STICKY-ON-EMPTY, AND IT IS ANOTHER ABSENT-vs-MEASURED DECISION.
//
// refresh keeps the PREVIOUS Latency/GossipLiveness when the directory returns
// an empty set (lad_snapshot.go:383-391), because "the directory's internal
// mutex stalled and gave me nothing" must not blank Layer 2 and Layer 4. The
// comment says so explicitly: a transient empty return "blanked the previous
// Latency/Liveness instead of staying sticky".
//
// ⚠ The cost of that choice, stated rather than assumed: a node whose gossip
// peers genuinely ALL go silent keeps its last liveness map indefinitely — the
// same frozen-value hazard MESH-G04 fixed in the reputation tracker by clearing
// on an empty window. The two subsystems resolve the identical trade in
// OPPOSITE directions, and both are defensible: liveness feeds a health verdict
// where a false "everyone is gone" pages, whereas reputation feeds a ranking
// where a stale score silently mis-orders. Pinned here so the asymmetry is a
// recorded decision rather than a discrepancy someone later "fixes".
func TestEmptyLivenessAndLatencyStayStickyRatherThanBlanking(t *testing.T) {
	c, dir := snapshotCacheWith(t, latencyRecord(t, testNodeIDA, testNodeIDB))
	dir.RecordGossipSeen(testNodeIDB)
	c.refresh()

	warm := c.Snapshot()
	if len(warm.Latency) == 0 || len(warm.GossipLiveness) == 0 {
		t.Fatalf("premise wrong: nothing to go sticky (latency=%d liveness=%d)",
			len(warm.Latency), len(warm.GossipLiveness))
	}

	// A fresh cache with no records at all is what a stalled/empty directory
	// return looks like to refresh: swap it in and refresh again.
	// Safe to write c.directory directly ONLY because snapshotCacheWith does not
	// call Start: there is no loop goroutine to race. If the helper ever starts
	// one, this write becomes a data race and -race will say so.
	c.directory = ladcache.NewDirectoryCache()
	c.refresh()

	after := c.Snapshot()
	if len(after.Latency) != len(warm.Latency) {
		t.Fatalf("Latency went from %d to %d on an empty directory return — a "+
			"transient stall now blanks Layer 4, and every peer reads as "+
			"ungraded until the next successful refresh",
			len(warm.Latency), len(after.Latency))
	}
	if len(after.GossipLiveness) != len(warm.GossipLiveness) {
		t.Fatalf("GossipLiveness went from %d to %d on an empty directory "+
			"return — Layer 2 blanks and every gossip-alive service reads as "+
			"degraded", len(warm.GossipLiveness), len(after.GossipLiveness))
	}
}

// ── The per-method ERROR path ───────────────────────────────────────────────
//
// Reaching it needs no failing CacheStore: a 1ns PerCallTimeout expires before
// any of the four reads can complete, so all four take their error branch with
// a real timeout rather than a faked one. Measured, first attempt, all four:
//
//	Roles / Reach / Members: context deadline exceeded
//	Latency/GossipLiveness:  timeout after 1ns
//
// 🙋 Seventh cost estimate of the session that was wrong. The pattern is now
// unmistakable: I describe a dependency instead of reading it.

func timingOutCache(t *testing.T, dir *ladcache.DirectoryCache) *LADSnapshotCache {
	t.Helper()
	c := NewLADSnapshotCache(dir, LADSnapshotCacheConfig{
		RefreshInterval: time.Hour, PerCallTimeout: time.Nanosecond,
	})
	t.Cleanup(c.Stop)
	return c
}

// Every failure must be REPORTED, not swallowed. Errors is the only channel by
// which a total directory failure is distinguishable from an empty mesh.
//
// 🙋 MY FIRST VERSION OF THIS TEST WAS FLAKY, which is worse than wrong — it
// asserted that ALL FOUR reader groups error in a SINGLE refresh. At a 1ns
// timeout that is a race: `Members` occasionally completes inside the window,
// and a 40-iteration stress run reproduced it (Roles/Reach/Latency errored,
// Members did not). A flaky test erodes trust in the whole suite, so the
// assertion is now deterministic: every refresh must record at least one
// labelled error, and all four groups must be observed ACROSS bounded retries
// rather than simultaneously.
func TestEveryFailedDirectoryReadIsRecordedInErrors(t *testing.T) {
	c := timingOutCache(t, ladcache.NewDirectoryCache())

	seen := map[string]bool{}
	groups := []string{"Members", "Reach", "Roles", "Latency/GossipLiveness"}

	for attempt := 0; attempt < 50 && len(seen) < len(groups); attempt++ {
		c.refresh()
		snap := c.Snapshot()

		// DETERMINISTIC, every attempt: a refresh where reads time out must
		// record at least one error, and every error must name its reader.
		if len(snap.Errors) == 0 {
			t.Fatalf("attempt %d recorded NO errors with a 1ns per-call timeout "+
				"— a total directory failure is then indistinguishable from an "+
				"empty mesh", attempt)
		}
		for _, e := range snap.Errors {
			txt := e.Error()
			labelled := false
			for _, g := range groups {
				if strings.HasPrefix(txt, g+":") {
					seen[g] = true
					labelled = true
				}
			}
			if !labelled {
				t.Fatalf("unlabelled error %q — an operator cannot tell which "+
					"layer is broken", txt)
			}
		}
	}

	// AGGREGATE: each of the four readers must be individually capable of
	// reporting. A reader that never appears across 50 attempts is not racing,
	// it is silent.
	for _, g := range groups {
		if !seen[g] {
			t.Errorf("%s never reported an error across 50 timed-out refreshes "+
				"— that reader fails silently and its layer's staleness is "+
				"invisible", g)
		}
	}
}

// Sticky-on-error: a failed read must inherit the PREVIOUS snapshot's value
// for its slot rather than blank it. This is a different code path from
// sticky-on-empty — an `err != nil` branch per reader, versus a
// `len(...) == 0` check at the end.
func TestAFailedReadInheritsThePreviousSnapshotsValue(t *testing.T) {
	dir := ladcache.NewDirectoryCache()
	if err := dir.Apply(memberRecord(t, testNodeIDB, "auth.hstles.com")); err != nil {
		t.Fatal(err)
	}
	if err := dir.Apply(latencyRecord(t, testNodeIDA, testNodeIDB)); err != nil {
		t.Fatal(err)
	}
	dir.RecordGossipSeen(testNodeIDB)

	// One good refresh to establish the values that must survive.
	c := NewLADSnapshotCache(dir, LADSnapshotCacheConfig{
		RefreshInterval: time.Hour, PerCallTimeout: time.Second,
	})
	t.Cleanup(c.Stop)
	c.refresh()
	good := c.Snapshot()
	if len(good.Members) != 1 || len(good.Latency) != 1 || len(good.GossipLiveness) != 1 {
		t.Fatalf("premise wrong: nothing to inherit (members=%d latency=%d "+
			"liveness=%d)", len(good.Members), len(good.Latency),
			len(good.GossipLiveness))
	}

	// Now make every read time out. No loop is running, so writing cfg is safe.
	c.cfg.PerCallTimeout = time.Nanosecond
	c.refresh()

	after := c.Snapshot()
	if len(after.Errors) == 0 {
		t.Fatal("premise wrong: the reads did not fail, so inheritance is untested")
	}
	if len(after.Members) != len(good.Members) {
		t.Fatalf("Members went from %d to %d when the read FAILED — a transient "+
			"directory stall blanks Layer 1, and every service drops out of the "+
			"health evaluation until the next successful refresh",
			len(good.Members), len(after.Members))
	}
	if len(after.Latency) != len(good.Latency) {
		t.Fatalf("Latency went from %d to %d on a failed read", len(good.Latency),
			len(after.Latency))
	}
	if len(after.GossipLiveness) != len(good.GossipLiveness) {
		t.Fatalf("GossipLiveness went from %d to %d on a failed read",
			len(good.GossipLiveness), len(after.GossipLiveness))
	}
}

// Warm goes TRUE even when every read failed, which is the documented
// contract: LADSnapshot's own doc directs callers to check .Warm to
// distinguish bootstrap (no successful refresh yet) from a refresh that
// completed but had per-method errors. So Warm means "a refresh RAN" and
// Errors carries whether it learned anything; both fields are needed to read
// the state.
//
// LADSnapshot.Warm and LADSnapshot.Errors have zero non-test readers, while
// snap.Members readers do resolve. On a FIRST refresh that fails entirely,
// prev is the empty bootstrap, so nothing is inherited, Warm is true and
// Errors is discarded — health_evaluator.go then reports every service
// unreachable while holding a snapshot that records, in fields nobody reads,
// that it learned nothing.
func TestWarmBecomesTrueEvenWhenEveryReadFailed(t *testing.T) {
	c := timingOutCache(t, ladcache.NewDirectoryCache())
	c.refresh()

	snap := c.Snapshot()
	if !snap.Warm {
		t.Fatal("Warm is false after a completed-but-failed refresh — this test " +
			"pins the contract LADSnapshot's doc states, that Warm means 'a " +
			"refresh ran'; if it now means 'a refresh succeeded', that is an " +
			"improvement and this test should be rewritten to match")
	}
	if len(snap.Errors) == 0 {
		t.Fatal("premise wrong: no errors, so this is not the failed case")
	}
	if len(snap.Members) != 0 {
		t.Fatalf("%d members from a directory that never answered", len(snap.Members))
	}
}
