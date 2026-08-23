/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"testing"
	"time"
)

// Covers the LAD snapshot cache.
//
// `NewLADSnapshotCache` has two non-test callers, health_evaluator.go and
// runtime.go, so this cache is the snapshot source underneath the 4-layer
// HealthEvaluator. This file and health_evaluator_test.go therefore cover a
// producer and its consumer rather than two endpoints of an untested edge.
//
// 🔑 THE CONTRACT THIS FILE PINS IS THE COLD-BOOT ONE, and it is the half a
// test can reach without a live DirectoryCache: Snapshot() must be non-nil
// from t=0, must be honestly marked NOT WARM until a refresh succeeds, and a
// nil directory must be a permanent, safe, loop-free bootstrap state. Every
// observability read in the node goes through this, and `evaluate()` in
// health_evaluator.go dereferences the result without a nil check.

// 🔴 NON-NIL FROM t=0. health_evaluator.go's evaluate() does `snap :=
// e.snapshot()` and immediately ranges over snap.Members — a nil return is an
// unrecovered panic in the evaluation goroutine.
func TestSnapshotIsNonNilBeforeAnyRefresh(t *testing.T) {
	c := NewLADSnapshotCache(nil, LADSnapshotCacheConfig{})

	snap := c.Snapshot()
	if snap == nil {
		t.Fatal("Snapshot() returned nil before Start — evaluate() ranges over " +
			"snap.Members with no nil check and would panic in the evaluation " +
			"goroutine, taking the endpoint down")
	}
	if snap.Warm {
		t.Fatal("the bootstrap snapshot claims to be WARM before any refresh " +
			"has run — a consumer cannot then tell 'the mesh is empty' from " +
			"'we have not looked yet', which is the whole point of the flag")
	}
	if snap.BuiltAt.IsZero() {
		t.Fatal("BuiltAt is zero on the bootstrap snapshot, so Age() reports 0 " +
			"forever and a staleness check can never fire")
	}
	// Empty, not nil-hostile: ranging over these must be safe.
	_ = len(snap.Members) + len(snap.Reach) + len(snap.Roles) + len(snap.Latency)
	for range snap.GossipLiveness {
		t.Fatal("the bootstrap snapshot carries gossip liveness it never measured")
	}
}

// A nil directory is a supported, permanent bootstrap state: no goroutine, no
// panic, and Stop must not hang waiting for a loop that was never started.
func TestANilDirectoryYieldsAPermanentBootstrapSnapshotAndNoLoop(t *testing.T) {
	c := NewLADSnapshotCache(nil, LADSnapshotCacheConfig{RefreshInterval: time.Millisecond})

	c.Start()
	first := c.Snapshot()
	time.Sleep(20 * time.Millisecond) // several refresh intervals

	if got := c.Snapshot(); got != first {
		t.Fatal("the snapshot pointer changed with a nil directory — a refresh " +
			"loop is running against no directory at all")
	}

	done := make(chan struct{})
	go func() { c.Stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop() hung with a nil directory — it is waiting on a " +
			"WaitGroup for a goroutine Start() deliberately never spawned, so " +
			"shutdown deadlocks on any minimal build")
	}
}

// 🔑 Start and Stop are both once-guarded. Pinned because the sibling
// SessionHealthMonitor.Stop does a bare `close(m.stopChan)` with no
// sync.Once — a second call there panics on a closed channel. This one is the
// correct template, and it is 200 lines away.
func TestStartAndStopAreIdempotent(t *testing.T) {
	c := NewLADSnapshotCache(nil, LADSnapshotCacheConfig{})

	c.Start()
	c.Start() // must not spawn a second loop or panic

	c.Stop()
	c.Stop() // must not panic on an already-closed channel

	if c.Snapshot() == nil {
		t.Fatal("Snapshot() went nil after Stop — a late observability read " +
			"during shutdown would panic")
	}
}

// ── Age ─────────────────────────────────────────────────────────────────────

// Age is what a staleness check reads. Its two guard clauses both mean "I
// cannot tell you", and both answer 0 — the safe direction here, because a
// large fabricated age would make a fresh node look stale.
func TestAgeAnswersZeroWhenItCannotKnowAndTracksTimeWhenItCan(t *testing.T) {
	var nilSnap *LADSnapshot
	if got := nilSnap.Age(); got != 0 {
		t.Fatalf("(*LADSnapshot)(nil).Age() = %v, want 0 — and it must not "+
			"panic: a consumer holding a nil snapshot is exactly who calls this",
			got)
	}

	if got := (&LADSnapshot{}).Age(); got != 0 {
		t.Fatalf("Age() on a zero BuiltAt = %v, want 0 — time.Since(zeroTime) "+
			"is ~292 years and would read as catastrophically stale on a node "+
			"that simply has not built a snapshot yet", got)
	}

	old := &LADSnapshot{BuiltAt: time.Now().Add(-time.Hour)}
	if got := old.Age(); got < 59*time.Minute || got > 61*time.Minute {
		t.Fatalf("Age() = %v for a snapshot built an hour ago", got)
	}
}

// ── Config repair ───────────────────────────────────────────────────────────

// The defaults are the live values: runtime.go:695 and health_evaluator.go:158
// both construct with DefaultLADSnapshotCacheConfig().
func TestDefaultConfigMatchesTheDocumentedTuning(t *testing.T) {
	cfg := DefaultLADSnapshotCacheConfig()
	if cfg.RefreshInterval != 10*time.Second {
		t.Fatalf("RefreshInterval = %v, want 10s — the doc's rationale is that "+
			"10s is faster than the evaluator's old 30s tick", cfg.RefreshInterval)
	}
	if cfg.PerCallTimeout != 3*time.Second {
		t.Fatalf("PerCallTimeout = %v, want 3s — the doc's rationale is that 3s "+
			"is well above a warm directory's p99 (~50-200ms) but short enough "+
			"that a stuck call does not delay the next refresh past one tick",
			cfg.PerCallTimeout)
	}
}

// 🔴 A ZERO OR NEGATIVE INTERVAL MUST BE REPAIRED, NOT HONOURED. A zero
// RefreshInterval reaches time.NewTicker, which PANICS on a non-positive
// duration — in the loop goroutine, unrecovered.
func TestNonPositiveConfigValuesAreReplacedWithDefaults(t *testing.T) {
	for _, cfg := range []LADSnapshotCacheConfig{
		{},                              // both zero
		{RefreshInterval: -time.Second}, // negative interval
		{PerCallTimeout: -time.Second},  // negative timeout
		{RefreshInterval: -1, PerCallTimeout: -1}, // both negative
	} {
		c := NewLADSnapshotCache(nil, cfg)
		if c.cfg.RefreshInterval <= 0 {
			t.Fatalf("RefreshInterval = %v from %+v — time.NewTicker panics on "+
				"a non-positive duration, inside the loop goroutine, so the "+
				"whole endpoint dies at Start()", c.cfg.RefreshInterval, cfg)
		}
		if c.cfg.PerCallTimeout <= 0 {
			t.Fatalf("PerCallTimeout = %v from %+v — a non-positive timeout "+
				"makes every context.WithTimeout expire instantly, so every "+
				"directory call fails and the snapshot never becomes warm",
				c.cfg.PerCallTimeout, cfg)
		}
	}

	// A caller's explicit values must survive: repair must not clobber them.
	c := NewLADSnapshotCache(nil, LADSnapshotCacheConfig{
		RefreshInterval: 250 * time.Millisecond, PerCallTimeout: 100 * time.Millisecond,
	})
	if c.cfg.RefreshInterval != 250*time.Millisecond || c.cfg.PerCallTimeout != 100*time.Millisecond {
		t.Fatalf("explicit config was overwritten: %+v", c.cfg)
	}
}
