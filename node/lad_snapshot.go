/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

// Package node — LAD snapshot cache.
//
// Observability handlers (HealthEvaluator, /mesh-debug, /mesh-topology,
// /reach-state, /domain-health) historically called directly into the LAD
// directory (Members, Reach, Roles, Latency, GossipLiveness) on each
// request. On cold-boot, when the directory's gossip cache hasn't yet
// settled, every one of those calls can block for hundreds of
// milliseconds to seconds while waiting for the gossip-bloom full-sync to
// fill. Multiple handlers stack behind the same slow directory mutex,
// goroutines starve, and remote peers — which time response latency on
// their own dispatched RPCs — fire GOAWAY frames at us with "abuse
// threshold stalled=Ns". The downstream effects are session churn,
// noise-UDP getting torn down before it stabilises, and a feedback loop
// where the slow node poisons the wider mesh's transport mix.
//
// LADSnapshotCache breaks the cycle. A single background goroutine
// refreshes a typed snapshot of all directory state on a short interval
// (default 10s), with per-call timeouts so one slow method can't stall
// the others. Readers swap-in the latest snapshot via atomic.Pointer —
// O(1), lock-free. The request path never touches the directory.
//
// Design properties:
// - Lock-free reads via atomic.Pointer[LADSnapshot]. Snapshots are
// immutable after publish.
// - Per-LAD-call timeout so one slow method (Reach, typically) doesn't
// drag down the others. Each method runs in parallel.
// - Warming state: the initial snapshot is published immediately with
// Warm=false so first-request handlers can distinguish "still loading"
// from "directory empty". After the first successful refresh,
// Warm=true.
// - Sticky-on-error semantics: if a refresh fails for a method, that
// method's slot retains the PREVIOUS snapshot's value rather than
// reverting to nil. This keeps observability working through transient
// directory blips. Errors per method are surfaced via Errors slice.
// - Self-reporting health: Snapshot() includes BuildDuration and a Warm
// flag, plus per-method age tracking so handlers can show staleness
// in the UI.
package node

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	lad "github.com/bbmumford/ledger"
	ladcache "github.com/bbmumford/ledger/cache"
)

// LADSnapshot is an immutable snapshot of LAD directory state.
//
// Created and published only by LADSnapshotCache.refresh. Readers must
// never mutate the slices/maps — they are shared by reference across
// concurrent readers.
type LADSnapshot struct {
	// Mesh membership (Layer 1)
	Members []lad.MemberRecord

	// Published reach addresses (Layer 3)
	Reach []lad.ReachRecord

	// Role advertisements (per-node FQN-derived roles)
	Roles []lad.RoleRecord

	// Mesh-wide latency / connection edges (Layer 4)
	Latency []lad.LatencyRecord

	// nodeID → last-seen gossip timestamp (Layer 2)
	GossipLiveness map[string]time.Time

	// Snapshot metadata
	BuiltAt       time.Time     // when this snapshot was finalised
	BuildDuration time.Duration // total wall-clock time the refresh took
	Warm          bool          // false on the bootstrap snapshot before any successful refresh
	Errors        []error       // per-method errors from the most recent refresh, if any
}

// Age returns how old the snapshot is at call time.
func (s *LADSnapshot) Age() time.Duration {
	if s == nil || s.BuiltAt.IsZero() {
		return 0
	}
	return time.Since(s.BuiltAt)
}

// LADSnapshotCacheConfig configures a LADSnapshotCache.
type LADSnapshotCacheConfig struct {
	// RefreshInterval is how often the background loop wakes to pull fresh
	// state from the directory. Default 10s.
	RefreshInterval time.Duration

	// PerCallTimeout bounds each individual directory method invocation.
	// Default 3s. A method exceeding this contributes to Errors and the
	// snapshot retains the previous value for that slot.
	PerCallTimeout time.Duration
}

// DefaultLADSnapshotCacheConfig returns sensible defaults: refresh every
// 10s with a 3s per-method timeout. Tuning rationale:
// - 10s refresh is faster than the previous 30s evaluator tick — gives
// observability handlers fresher data without hammering the directory.
// - 3s per-method timeout is well above the typical p99 of these calls
// when the directory is warm (~50–200ms), but short enough that a
// stuck call doesn't delay the next refresh past one tick.
func DefaultLADSnapshotCacheConfig() LADSnapshotCacheConfig {
	return LADSnapshotCacheConfig{
		RefreshInterval: 10 * time.Second,
		PerCallTimeout:  3 * time.Second,
	}
}

// LADSnapshotCache owns a refreshing snapshot of LAD directory state.
//
// Concurrency model: a single background goroutine performs all directory
// I/O. Snapshots are published via atomic.Pointer so readers are
// lock-free. The cache is safe to construct early in boot — the initial
// (Warm=false) snapshot is published before the goroutine starts, so
// Snapshot() is non-nil from t=0.
//
// Push-update path: in addition to the periodic ticker, the snapshot
// subscribes to topic-change events from the underlying
// DirectoryCache. Every successful ledger.Append on any topic delivers
// a record to the subscriber, which fires a non-blocking send into
// notifyCh. The loop coalesces bursts inside a short window so a flood
// of appends (e.g. snapshot replay at boot) triggers exactly one
// refresh, not one per record. The 10s ticker stays as a safety net in
// case the subscriber path drops a notification under back-pressure.
type LADSnapshotCache struct {
	directory *ladcache.DirectoryCache
	cfg       LADSnapshotCacheConfig

	current atomic.Pointer[LADSnapshot]

	// notifyCh receives a signal whenever the underlying DirectoryCache
	// applies a new record on a subscribed topic. Buffered=1 so a burst
	// of appends collapses into a single wakeup. Drain inside the loop's
	// coalesce window so multiple notifications within 500ms produce
	// exactly one refresh.
	notifyCh chan struct{}

	startOnce sync.Once
	stopOnce  sync.Once
	stopCh    chan struct{}
	wg        sync.WaitGroup
}

// NewLADSnapshotCache constructs a cache. The cache is safe to use
// immediately — Snapshot() returns a warm=false bootstrap snapshot until
// the first refresh completes. Call Start() once node initialisation is
// past the point where the directory is wired but BEFORE other components
// begin issuing observability reads.
//
// directory may be nil (useful for tests / minimal builds); in that case
// Snapshot() returns a permanent warm=false snapshot with empty slices.
func NewLADSnapshotCache(directory *ladcache.DirectoryCache, cfg LADSnapshotCacheConfig) *LADSnapshotCache {
	if cfg.RefreshInterval <= 0 {
		cfg.RefreshInterval = DefaultLADSnapshotCacheConfig().RefreshInterval
	}
	if cfg.PerCallTimeout <= 0 {
		cfg.PerCallTimeout = DefaultLADSnapshotCacheConfig().PerCallTimeout
	}
	c := &LADSnapshotCache{
		directory: directory,
		cfg:       cfg,
		stopCh:    make(chan struct{}),
		notifyCh:  make(chan struct{}, 1),
	}
	c.current.Store(&LADSnapshot{
		Members:        nil,
		Reach:          nil,
		Roles:          nil,
		Latency:        nil,
		GossipLiveness: nil,
		BuiltAt:        time.Now(),
		Warm:           false,
	})

	// Subscribe to every topic that contributes to the snapshot. The
	// handler is a non-blocking send into notifyCh — the loop's
	// coalesce window squashes bursts into a single refresh.
	// DirectoryCache invokes subscribers synchronously inside Apply()
	// under c.mu, so the handler MUST NOT block (the channel send is
	// guarded by a default branch).
	if directory != nil {
		notify := func(_ lad.Record) {
			select {
			case c.notifyCh <- struct{}{}:
			default:
				// already pending — coalesce
			}
		}
		directory.Subscribe(lad.TopicReach, notify)
		directory.Subscribe(lad.TopicMember, notify)
		directory.Subscribe(lad.TopicRole, notify)
		directory.Subscribe(lad.TopicLatency, notify)
	}
	return c
}

// Start kicks off the background refresh loop. Idempotent.
func (c *LADSnapshotCache) Start() {
	c.startOnce.Do(func() {
		if c.directory == nil {
			return // nil directory: bootstrap snapshot is permanent, no loop needed
		}
		c.wg.Add(1)
		go c.loop()
	})
}

// Stop halts the refresh loop and waits for it to drain. Idempotent.
func (c *LADSnapshotCache) Stop() {
	c.stopOnce.Do(func() {
		close(c.stopCh)
		c.wg.Wait()
	})
}

// Snapshot returns the latest published snapshot. Always non-nil; check
// .Warm to distinguish bootstrap (no successful refresh yet) from a
// refresh that completed but had per-method errors.
func (c *LADSnapshotCache) Snapshot() *LADSnapshot {
	return c.current.Load()
}

// loop runs the periodic refresh. The first refresh fires immediately so
// the snapshot becomes warm as soon as the directory has any data.
//
// Push-update path: refresh fires on EITHER the periodic ticker OR a
// topic-change notification from the DirectoryCache subscriber, with a
// 500ms coalesce window after each notification to batch bursts (a
// snapshot replay at boot emits hundreds of records inside a few ms;
// without coalescing this would trigger hundreds of refreshes).
//
// The ticker stays as a safety net — if a notification is ever dropped
// under back-pressure, the next ticker firing closes the gap. Under
// normal load the ticker is mostly a no-op because every change is
// already covered by a push.
const snapshotCoalesceWindow = 500 * time.Millisecond

func (c *LADSnapshotCache) loop() {
	defer c.wg.Done()
	c.refresh()
	ticker := time.NewTicker(c.cfg.RefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.refresh()
		case <-c.notifyCh:
			// Coalesce: wait briefly so a burst of appends produces
			// exactly one refresh. Drain any additional notifications
			// arriving during the window — the next select wakes
			// immediately if the channel has been re-armed, but the
			// refresh that follows already covers those records.
			coalesceTimer := time.NewTimer(snapshotCoalesceWindow)
		drain:
			for {
				select {
				case <-c.notifyCh:
					// keep coalescing
				case <-coalesceTimer.C:
					break drain
				case <-c.stopCh:
					coalesceTimer.Stop()
					return
				}
			}
			c.refresh()
		case <-c.stopCh:
			return
		}
	}
}

// refresh pulls fresh state from the directory and atomically swaps in a
// new snapshot. Each of the four LAD methods runs in its own goroutine
// with the per-call timeout so a slow method doesn't block the others;
// per-method failures inherit the previous snapshot's value for that slot
// (sticky-on-error).
func (c *LADSnapshotCache) refresh() {
	start := time.Now()
	prev := c.current.Load()

	var (
		members  []lad.MemberRecord
		reach    []lad.ReachRecord
		roles    []lad.RoleRecord
		latency  []lad.LatencyRecord
		liveness map[string]time.Time
		errs     []error
		errsMu   sync.Mutex
	)

	addErr := func(name string, err error) {
		errsMu.Lock()
		defer errsMu.Unlock()
		errs = append(errs, fmt.Errorf("%s: %w", name, err))
	}

	var wg sync.WaitGroup
	wg.Add(4)

	// Members (Layer 1) — takes context
	go func() {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), c.cfg.PerCallTimeout)
		defer cancel()
		m, err := c.directory.Members(ctx, "")
		if err != nil {
			addErr("Members", err)
			if prev != nil {
				members = prev.Members
			}
			return
		}
		members = m
	}()

	// Reach (Layer 3) — takes context
	go func() {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), c.cfg.PerCallTimeout)
		defer cancel()
		r, err := c.directory.Reach(ctx, "", ladcache.ReachQuery{})
		if err != nil {
			addErr("Reach", err)
			if prev != nil {
				reach = prev.Reach
			}
			return
		}
		reach = r
	}()

	// Roles — takes context
	go func() {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), c.cfg.PerCallTimeout)
		defer cancel()
		r, err := c.directory.Roles(ctx, "", ladcache.RoleQuery{})
		if err != nil {
			addErr("Roles", err)
			if prev != nil {
				roles = prev.Roles
			}
			return
		}
		roles = r
	}()

	// Latency + GossipLiveness — the directory methods take no context, so we
	// bound them ourselves. Without a timeout, an internal-mutex stall
	// here hangs wg.Wait() and stops ALL snapshot refreshes; and a transient
	// empty return blanked the previous Latency/Liveness instead of staying
	// sticky like the context-aware readers above.
	go func() {
		defer wg.Done()
		type llResult struct {
			latency  []lad.LatencyRecord
			liveness map[string]time.Time
		}
		done := make(chan llResult, 1)
		go func() {
			done <- llResult{
				latency:  c.directory.Latency(""),
				liveness: c.directory.GossipLiveness(),
			}
		}()
		select {
		case res := <-done:
			latency = res.latency
			liveness = res.liveness
		case <-time.After(c.cfg.PerCallTimeout):
			addErr("Latency/GossipLiveness", fmt.Errorf("timeout after %v", c.cfg.PerCallTimeout))
		}
		// Sticky: keep the previous values rather than blanking on a slow/empty return.
		if prev != nil {
			if len(latency) == 0 {
				latency = prev.Latency
			}
			if len(liveness) == 0 {
				liveness = prev.GossipLiveness
			}
		}
	}()

	wg.Wait()

	snap := &LADSnapshot{
		Members:        members,
		Reach:          reach,
		Roles:          roles,
		Latency:        latency,
		GossipLiveness: liveness,
		BuiltAt:        time.Now(),
		BuildDuration:  time.Since(start),
		Warm:           true,
		Errors:         errs,
	}
	c.current.Store(snap)
}
