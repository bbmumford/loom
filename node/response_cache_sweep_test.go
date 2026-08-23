/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"fmt"
	"testing"
	"time"

	"github.com/ORBTR/aether/rpc/pb"
)

// COVERAGE of sweepLocked (response_cache.go:159) and StatsDetailed (:135),
// both at 0.0%, plus the  high-water shed and the  clone
// property in Get/Put.
//
// 🔑 EVERY FIXTURE HERE BUILDS THE CACHE BY STRUCT LITERAL, NOT NewResponseCache.
// The constructor unconditionally starts a cleanupLoop goroutine that this
// package has no way to stop (see the two findings below), so calling it in a
// test leaks a goroutine per test AND lets a background sweep race the
// assertions. Constructing the struct directly exercises exactly the code under
// test with no concurrent mutator.
//
// ── TWO FINDINGS RECORDED HERE, NEITHER FIXED IN THIS SLICE ──────────────
//
// 🔴 (1) the coding contract: ResponseCache IS BACKGROUND WORK WITH NO LIFECYCLE.
// NewResponseCache (:47) unconditionally runs `go rc.cleanupLoop`, and
// cleanupLoop (:148) is `for range ticker.C` with no stop channel, no ctx and
// no exit — so `defer ticker.Stop()` never runs either. Measured: the complete
// method set on ResponseCache is Get, Put, Stats, StatsDetailed, cleanupLoop,
// sweepLocked. There is NO Stop or Close. the coding contract requires "explicit
// Start(ctx)/Close or an equivalent lifecycle ... and no work after close".
//
// ⚠ NOT FIXED HERE, DELIBERATELY. The owner would be RPCServer, which
// constructs the cache at rpc.go:354 — and RPCServer has no Stop, Close or
// Shutdown either (measured). Adding ResponseCache.Stop() with nothing to call
// it would manufacture exactly the REGISTERED-but-not-REACHABLE shape this lane
// has filed twice today. The real fix is an RPCServer lifecycle, which is a
// design decision for @R/DESIGN, not a test lane's to invent.
//
// 🔴 (2) A NON-POSITIVE TTL PANICS IN A GOROUTINE THE CALLER CANNOT RECOVER.
// cleanupLoop does time.NewTicker(rc.ttl / 2), and time.NewTicker panics when
// d <= 0. Integer division makes that true for any ttl <= 1ns, including 0.
// The panic happens on the goroutine started by the constructor, so the
// caller's recover() cannot catch it: the process dies.
//
// ⚠ REACHABILITY, MEASURED: the ONLY production call site is rpc.go:354, which
// passes a hard-coded 10*time.Second. So this is NOT live today — it is an
// exported-constructor robustness hole, the same category as the Stop
// double-close fixed earlier: "no current caller does it" is a
// property of today's callers, not of the API. Recorded, not fixed, because
// the fix (clamp or reject) is a contract choice.

func sweepFixture(ttl time.Duration) *ResponseCache {
	return &ResponseCache{entries: map[string]*cachedResponse{}, ttl: ttl}
}

func (rc *ResponseCache) putAged(id string, age time.Duration) {
	rc.entries[id] = &cachedResponse{
		response:  &pb.RPCResponse{Id: id, Success: true},
		createdAt: time.Now().Add(-age),
	}
}

// 🔬 ANTI-CORRELATED BY CONSTRUCTION: one entry past the TTL and one inside it.
// A sweep that dropped everything and a sweep that dropped nothing both pass a
// single-entry test; only a mixed set distinguishes a working predicate.
func TestSweepDropsOnlyTheExpiredEntries(t *testing.T) {
	rc := sweepFixture(time.Minute)
	rc.putAged("expired", 2*time.Minute)
	rc.putAged("fresh", time.Second)

	rc.sweepLocked(time.Now())

	if _, still := rc.entries["expired"]; still {
		t.Error("an entry past its TTL survived the sweep — the cache grows without bound " +
			"and stale responses keep being served to dedup lookups")
	}
	if _, ok := rc.entries["fresh"]; !ok {
		t.Error("the sweep dropped an entry still inside its TTL — a live dedup entry is " +
			"gone, so a duplicate probe re-executes a handler that already ran")
	}
}

// The TTL boundary is exclusive: `now.Sub(createdAt) > ttl`. An entry exactly
// at the TTL is still live. Off-by-one here silently halves or extends the
// dedup window.
func TestAnEntryExactlyAtTheTTLIsNotYetExpired(t *testing.T) {
	rc := sweepFixture(time.Minute)
	base := time.Now()
	rc.entries["edge"] = &cachedResponse{
		response:  &pb.RPCResponse{Id: "edge"},
		createdAt: base.Add(-time.Minute), // exactly ttl old
	}

	rc.sweepLocked(base)

	if _, ok := rc.entries["edge"]; !ok {
		t.Error("an entry exactly at the TTL was swept — the comparison is >= where the " +
			"code and its callers assume >, shortening every dedup window by one tick")
	}
}

// 🔴 THE SWEEP COUNTER MUST TICK EVEN WHEN NOTHING IS FREED, and the eviction
// counter must NOT. StatsDetailed exists to "spot runaway-growth conditions"
// (:134): the diagnostic signal for runaway growth is precisely
// "sweeps climbing while evictions stays flat and size stays high". Conflating
// the two counters destroys the only signal the dashboard has.
func TestSweepsCountEvenWhenNothingIsEvicted(t *testing.T) {
	rc := sweepFixture(time.Minute)
	rc.putAged("fresh", time.Second)

	rc.sweepLocked(time.Now())

	_, _, sweeps, evictions, _, size := rc.StatsDetailed()
	if sweeps != 1 {
		t.Errorf("sweeps = %d after one sweep, want 1 — a sweep that frees nothing is not "+
			"recorded, so the runaway-growth signal (sweeps rising, evictions flat) cannot "+
			"be seen", sweeps)
	}
	if evictions != 0 {
		t.Errorf("evictions = %d when nothing expired, want 0 — evictions is being "+
			"incremented per sweep rather than per entry, which makes it useless as a "+
			"growth signal", evictions)
	}
	if size != 1 {
		t.Errorf("size = %d, want 1", size)
	}
}

func TestSweepCountsEveryEvictedEntry(t *testing.T) {
	rc := sweepFixture(time.Minute)
	for i := 0; i < 5; i++ {
		rc.putAged(fmt.Sprintf("old-%d", i), 2*time.Minute)
	}

	rc.sweepLocked(time.Now())

	if _, _, _, evictions, _, size := rc.StatsDetailed(); evictions != 5 || size != 0 {
		t.Errorf("evictions=%d size=%d after expiring 5 entries, want 5 and 0",
			evictions, size)
	}
}

// 🔴 : THE HARD CAP MUST HOLD WHEN THE SWEEP FREES NOTHING. Under
// sustained unique-ID traffic faster than the TTL nothing is ever expired, so
// a TTL-only sweep frees zero and the map grows unbounded. The shed is the only
// thing bounding memory in that regime.
//
// 🔬 THE FIXTURE IS ALL-FRESH ON PURPOSE. With any expired entries the TTL
// sweep alone would free space and the test would pass without the shed ever
// running — the shed's whole reason for existing is the case where the sweep
// recovers nothing.
func TestTheHighWaterShedBoundsTheCacheWhenNothingIsExpired(t *testing.T) {
	rc := sweepFixture(time.Hour) // nothing can expire
	for i := 0; i < ResponseCacheHighWaterMark; i++ {
		rc.putAged(fmt.Sprintf("live-%d", i), time.Second)
	}

	rc.Put("one-more", &pb.RPCResponse{Id: "one-more", Success: true})

	_, _, _, evictions, highMark, size := rc.StatsDetailed()
	if highMark != 1 {
		t.Errorf("highMark = %d, want 1 — the high-water mark did not trip at %d entries",
			highMark, ResponseCacheHighWaterMark)
	}
	if size >= ResponseCacheHighWaterMark {
		t.Errorf("size = %d after the shed, want it below the %d mark — the TTL sweep "+
			"freed nothing (by construction) and the shed did not fire, so sustained "+
			"unique-ID traffic grows this map without bound", size, ResponseCacheHighWaterMark)
	}
	if evictions == 0 {
		t.Error("the shed dropped entries without counting them as evictions — memory " +
			"pressure is invisible to StatsDetailed")
	}
}

// 🔴 , THE READ SIDE. Get returns a clone because callers overwrite
// resp.Id for correlation. Handing out the shared pointer let concurrent hits
// race on that write and corrupted the cached Id for later dedup lookups.
//
// The second Get is the discriminating step: asserting only that the two
// pointers differ would also pass if Put had stored a clone and Get returned
// the original.
func TestGetHandsBackACloneSoCallersCannotCorruptTheCache(t *testing.T) {
	rc := sweepFixture(time.Minute)
	rc.Put("req-1", &pb.RPCResponse{Id: "req-1", Success: true, Payload: []byte("body")})

	first := rc.Get("req-1")
	if first == nil {
		t.Fatal("fixture wrong: the entry was not cached")
	}
	first.Id = "correlation-id-overwritten-by-caller"

	second := rc.Get("req-1")
	if second == nil {
		t.Fatal("the entry vanished after a caller mutated its copy")
	}
	if second.Id != "req-1" {
		t.Errorf("cached Id is now %q — a caller's correlation-id overwrite reached the "+
			"cached copy, so every later dedup lookup for req-1 returns a response "+
			"stamped with someone else's id", second.Id)
	}
}

// The write side of the same property: mutating the response AFTER Put must not
// change what was cached. Both halves are needed — a clone on only one side
// still leaves a shared object on the other.
func TestPutStoresACloneSoLaterCallerMutationsDoNotLeak(t *testing.T) {
	rc := sweepFixture(time.Minute)
	resp := &pb.RPCResponse{Id: "req-1", Success: true}

	rc.Put("req-1", resp)
	resp.Id = "mutated-after-put"
	resp.Success = false

	got := rc.Get("req-1")
	if got == nil {
		t.Fatal("entry missing")
	}
	if got.Id != "req-1" || !got.Success {
		t.Errorf("cached response is Id=%q Success=%v — Put stored the caller's pointer, "+
			"so a response mutated after caching is what later probes receive",
			got.Id, got.Success)
	}
}

// An expired entry must read as a miss, not a stale hit, and must count as one.
func TestGetOnAnExpiredEntryIsAMissNotAStaleHit(t *testing.T) {
	rc := sweepFixture(time.Minute)
	rc.putAged("stale", 2*time.Minute)

	if got := rc.Get("stale"); got != nil {
		t.Error("an expired entry was served as a cache hit — a response from a previous " +
			"request generation is returned for a fresh probe")
	}
	if hits, misses := rc.Stats(); hits != 0 || misses != 1 {
		t.Errorf("hits=%d misses=%d, want 0 and 1 — an expired read was counted as a hit, "+
			"so the hit rate hides the staleness", hits, misses)
	}
}

// Put must reject inputs it cannot key or store, rather than caching a nil
// response that a later Get would hand back as a successful dedup hit.
func TestPutRejectsAnEmptyIDOrNilResponse(t *testing.T) {
	rc := sweepFixture(time.Minute)

	rc.Put("", &pb.RPCResponse{Id: "x"})
	rc.Put("req-1", nil)

	if n := len(rc.entries); n != 0 {
		t.Errorf("%d entries cached from an empty id / nil response, want 0", n)
	}
}
