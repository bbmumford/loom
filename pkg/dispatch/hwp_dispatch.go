/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package dispatch

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/rpc/pb"
	rpcscope "github.com/bbmumford/loom/pkg/rpc/scope"
	"github.com/bbmumford/loom/pkg/trace"
	"golang.org/x/sync/singleflight"
	"google.golang.org/protobuf/proto"
)

// ────────────────────────────────────────────────────────────────────────────
// Session health tracking
// ────────────────────────────────────────────────────────────────────────────

// cachedSession wraps an HWP session with health tracking, idle eviction,
// and a pre-opened stream pool. Time fields use atomic int64 (UnixNano)
// to avoid data races without a per-session mutex.
type cachedSession struct {
	session             aether.Session
	role                string
	lastUsedNano        int64 // atomic — UnixNano
	lastSuccessNano     int64 // atomic — UnixNano
	consecutiveTimeouts int32 // atomic
	pool                *StreamPool
}

// isHealthy returns true if the session is alive and responsive: closed or 3+
// consecutive timeouts is unhealthy, recovering after 30 seconds so churn
// never blacklists a session permanently.
func (cs *cachedSession) isHealthy() bool {
	if cs.session.IsClosed() {
		return false
	}
	timeouts := atomic.LoadInt32(&cs.consecutiveTimeouts)
	if timeouts < 3 {
		return true
	}
	// Recovery window: allow retry after 30s without success.
	// Zero lastSuccessNano (never succeeded) also passes — prevents
	// permanent blacklisting of fresh sessions.
	lastSuccess := time.Unix(0, atomic.LoadInt64(&cs.lastSuccessNano))
	return time.Since(lastSuccess) > 30*time.Second
}

func (cs *cachedSession) recordSuccess() {
	atomic.StoreInt32(&cs.consecutiveTimeouts, 0)
	now := time.Now().UnixNano()
	atomic.StoreInt64(&cs.lastSuccessNano, now)
	atomic.StoreInt64(&cs.lastUsedNano, now)
}

func (cs *cachedSession) recordTimeout() {
	atomic.AddInt32(&cs.consecutiveTimeouts, 1)
	atomic.StoreInt64(&cs.lastUsedNano, time.Now().UnixNano())
}

func (cs *cachedSession) touchUsed() {
	atomic.StoreInt64(&cs.lastUsedNano, time.Now().UnixNano())
}

func (cs *cachedSession) idleSince() time.Duration {
	nano := atomic.LoadInt64(&cs.lastUsedNano)
	if nano == 0 {
		return 0
	}
	return time.Since(time.Unix(0, nano))
}

// ────────────────────────────────────────────────────────────────────────────
// HWPCaller — resilient RPC dispatch over HWP sessions
// ────────────────────────────────────────────────────────────────────────────

// HWPCaller implements the Caller interface using native HWP sessions.
// Each RPC call opens a dynamic stream (from pool or fresh) to avoid
// contention with well-known streams (0-3) managed by AcceptHWPConnection.
//
// Resilience model:
//   - 5-minute session cache with 30s eviction tick
//   - Per-session health tracking (consecutive timeouts + recovery window)
//   - One circuit per capability, destination node and session generation
//     (open/half-open/closed, 5 availability failures → trip, one leased
//     half-open probe)
//   - 2-attempt retry: evict dead session, find fresh session, retry once
//   - Pre-opened stream pool (2 per session, background refill under the
//     caller's lifecycle context and the session's lifetime)
//   - Deadline propagation via RPCRequest.Timeout
//   - Collision-resistant logical request IDs (newRequestID)
//   - Per-role RPC metrics (calls, latency, timeouts, circuit trips)
//   - Load-aware routing via LAD latency records
//   - Forwarding sessions NOT cached (prevents lock-in)
type HWPCaller struct {
	mu       sync.RWMutex
	sessions map[string]*cachedSession // role → cached session
	finder   SessionFinder
	// findGroup coalesces concurrent getOrFindSession callers for the
	// same role onto one in-flight result, so one cache miss runs one
	// FindSession and one StreamPool fill instead of N that overwrite
	// each other.
	findGroup singleflight.Group

	stopCh chan struct{}
	// lifeCtx is the caller's lifecycle context: pool fills and refills run
	// under it, and Close cancels it, so no fill outlives the caller.
	lifeCtx    context.Context
	lifeCancel context.CancelFunc

	bmu      sync.RWMutex                   // separate lock for breakers (reduces contention)
	breakers map[breakerKey]*CircuitBreaker // one circuit per capability, node and session generation
	// breakerReset is the open period of each circuit before a half-open
	// probe; NewHWPCaller sets it to callerBreakerReset.
	breakerReset time.Duration
	metrics      *RPCMetrics

	// routeCache memoizes the result of finder.FindRoutes per
	// (role, method) for routeCacheTTL. FindRoutes walks the latency +
	// directory caches on every call, costing 50-200µs per warm-path
	// RPC; memoization eliminates that on the second-call-within-TTL
	// path. Entries are invalidated on a probe failure so a stale
	// route doesn't survive a downed peer.
	rmu        sync.RWMutex
	routeCache map[string]*cachedRoutes

	// resultCache stores idempotent-read responses keyed by
	// (fqn, sha256(payload)). Handlers opt in via Handler.CacheableForMs
	// or callers via CallCacheable. Invalidation is by TTL only — the
	// receiver-side ResponseCache continues to dedupe in-flight retries.
	cmu         sync.RWMutex
	resultCache map[string]*cachedResult

	// cacheableTTLs maps FQN ("role/method") to its caller-side cache
	// TTL. Populated by SetCacheableHandlers from the dispatch config.
	// Call() checks this map and auto-caches matching responses so
	// callers don't have to know which methods are safe to cache.
	ttlMu        sync.RWMutex
	cacheableTTL map[string]time.Duration
}

// cachedRoutes is the memoized output of finder.FindRoutes plus its
// expiry stamp.
type cachedRoutes struct {
	routes  []aether.ProbeRoute
	expires time.Time
}

// cachedResult is one entry of HWPCaller.resultCache.
type cachedResult struct {
	payload []byte
	expires time.Time
}

// routeCacheTTL bounds how long a memoized FindRoutes result is reused.
// The directory cache freshness ticks every few seconds; 5s sits inside
// that window so we don't pin a route past a real topology change.
const routeCacheTTL = 5 * time.Second

// defaultProbeTimeout caps a single BidiRPC.Call. Applied by both the
// parallel-probe path in Call (every fan-out probe goroutine) and the warm
// callOnce path (every cached-session RPC), each through context.WithTimeout,
// which keeps a stricter caller deadline and clamps a longer or absent one.
// Without this cap a wedged bidi stream blocks dispatch until an edge proxy's
// request timeout, since BidiRPC.Call selects on ctx.Done() AND b.done —
// neither fires when the stream is flow-control-deadlocked but technically
// still open. 10s gives healthy probes ample headroom (typical mesh RPC RTT
// < 50 ms) while bounding the wedge to one timeout instead of one edge
// timeout.
const defaultProbeTimeout = 10 * time.Second

// bidiProbeTimeout is the inner cap on a single BidiRPC attempt inside the
// parallel-probe goroutine. Tighter than defaultProbeTimeout so that, when
// bidi is wedged, the dynamic-stream fallback at the bottom of the goroutine
// still has a usable budget against probeCtx. Without this carve-out the
// dynamic-stream fallback would run on an already-expired ctx and could never
// succeed.
// 3 s covers healthy bidis (typical < 50 ms) with ~60x headroom; the
// remaining 7 s of probeCtx are available for the dynamic-stream attempt.
const bidiProbeTimeout = 3 * time.Second

// SessionFinder resolves a role + handler to an HWPSession.
// Implemented by the ConnectionManager which tracks all peer sessions.
type SessionFinder interface {
	FindSession(ctx context.Context, role, handler string) (aether.Session, error)
	FindAnySession() (aether.Session, bool)
	// CallViaBidi sends an RPC via BidiRPC (Stream 1) if available for the node.
	// Returns (nil, false, nil) if no BidiRPC channel exists for this node.
	//
	// The finder picks the session the request leaves on, and HWPCaller never
	// sees it, so the finder runs the grade gate on that session
	// (CheckRouteGrade with req.Context["opClass"]) and returns the refusal as
	// (nil, true, err). CallNode has no other gate on its route's grade.
	CallViaBidi(ctx context.Context, nodeID string, req *pb.RPCRequest) (*pb.RPCResponse, bool, error)
	// FindRoutes computes up to 3 routes to a handler's role.
	// Each route has a first-hop session + remaining route list.
	FindRoutes(ctx context.Context, role, handler string) []aether.ProbeRoute
	// IsSupersededByUpgrade reports whether the given session has been
	// replaced by a higher-grade session for the same peer. Used by the
	// HWPCaller eviction loop to drop stale cached sessions so the next
	// dispatch picks the upgraded one.
	IsSupersededByUpgrade(session aether.Session) bool
}

const (
	callerMaxIdle      = 5 * time.Minute  // session cache TTL
	callerMaintainTick = 30 * time.Second // background eviction + pool refill interval
	callerRequestTTL   = 30 * time.Second // per-request timeout
	callerPoolSize     = 2                // pre-opened streams per session

	callerBreakerThreshold = 5                // availability failures that open a circuit
	callerBreakerWindow    = 60 * time.Second // window the failures must fall in
	callerBreakerReset     = 30 * time.Second // open period before a half-open probe
)

// requestIDCounter numbers the requests this process mints. It is only the
// trace suffix of a request id: it starts at zero in every process, so an id
// built from it alone collides across processes.
var requestIDCounter uint64

// newRequestID mints a logical request id: 128 random bits in hex, then this
// process's counter in base 36 as a trace suffix. The receiving RPCServer
// keys its dedup cache on the id, and the parallel probes of one call share
// it, so it must not collide with an id another process minted.
func newRequestID() string {
	var b [16]byte
	_, _ = rand.Read(b[:]) // crypto/rand.Read does not fail on the platforms Go supports
	return hex.EncodeToString(b[:]) + "-" + strconv.FormatUint(atomic.AddUint64(&requestIDCounter, 1), 36)
}

// breakerKey is the failure domain of one circuit: the exact capability (the
// handler FQN the call names), the destination node, and the session
// generation, the ConnectionID aether assigns each session at creation. A
// failing capability or a dead session therefore opens its own circuit and
// never another capability's or the replacement session's.
type breakerKey struct {
	capability string
	node       string
	generation aether.ConnectionID
}

// NewHWPCaller creates a native HWP dispatch caller with background maintenance.
func NewHWPCaller(finder SessionFinder) *HWPCaller {
	lifeCtx, lifeCancel := context.WithCancel(context.Background())
	c := &HWPCaller{
		sessions:     make(map[string]*cachedSession),
		finder:       finder,
		stopCh:       make(chan struct{}),
		lifeCtx:      lifeCtx,
		lifeCancel:   lifeCancel,
		breakers:     make(map[breakerKey]*CircuitBreaker),
		breakerReset: callerBreakerReset,
		metrics:      NewRPCMetrics(),
		routeCache:   make(map[string]*cachedRoutes),
		resultCache:  make(map[string]*cachedResult),
		cacheableTTL: make(map[string]time.Duration),
	}
	go c.maintain()
	return c
}

// SetCacheableHandlers installs the FQN → TTL map from the dispatch
// config. Subsequent Call(role, method) invocations automatically cache
// successful responses when the FQN "role/method" is present in this
// map. Idempotent — re-setting replaces the whole map atomically.
func (c *HWPCaller) SetCacheableHandlers(ttls map[string]time.Duration) {
	cp := make(map[string]time.Duration, len(ttls))
	for k, v := range ttls {
		if v <= 0 {
			continue
		}
		cp[k] = v
	}
	c.ttlMu.Lock()
	c.cacheableTTL = cp
	c.ttlMu.Unlock()
}

// CacheableTTL returns the configured cache TTL for a fully-qualified
// method ("role/method"), or 0 if not configured.
func (c *HWPCaller) CacheableTTL(fqn string) time.Duration {
	c.ttlMu.RLock()
	defer c.ttlMu.RUnlock()
	return c.cacheableTTL[fqn]
}

// findRoutesCached returns the memoized routes for (role, method) when
// fresh, otherwise falls through to finder.FindRoutes and populates the
// cache. The cache is invalidated explicitly on probe failure via
// invalidateRouteCache so a downed peer doesn't keep returning stale
// routes for the rest of the TTL.
func (c *HWPCaller) findRoutesCached(ctx context.Context, role, method string) []aether.ProbeRoute {
	key := role + "\x00" + method
	now := time.Now()

	c.rmu.RLock()
	cr, ok := c.routeCache[key]
	c.rmu.RUnlock()
	if ok && now.Before(cr.expires) {
		return cr.routes
	}

	routes := c.finder.FindRoutes(ctx, role, method)
	if len(routes) > 0 {
		// Deterministic ordering by (NodeID, hop count) so every node
		// converges on the same probe order; otherwise hash-iteration
		// inside FindRoutes can yield different winners per node and
		// the receiver-side dedup cache misses more than it should.
		sortProbeRoutes(routes)
		c.rmu.Lock()
		c.routeCache[key] = &cachedRoutes{
			routes:  routes,
			expires: now.Add(routeCacheTTL),
		}
		c.rmu.Unlock()
	}
	return routes
}

// sortProbeRoutes orders routes by (RouteList length asc, NodeID asc)
// so shorter paths probe first and ties are deterministic across nodes.
func sortProbeRoutes(routes []aether.ProbeRoute) {
	sort.SliceStable(routes, func(i, j int) bool {
		li, lj := len(routes[i].RouteList), len(routes[j].RouteList)
		if li != lj {
			return li < lj
		}
		return routes[i].NodeID < routes[j].NodeID
	})
}

// invalidateRouteCache drops the cached routes for (role, method).
// Called when every probe in a Call fails, so the next dispatch
// recomputes from a fresh directory snapshot.
func (c *HWPCaller) invalidateRouteCache(role, method string) {
	key := role + "\x00" + method
	c.rmu.Lock()
	delete(c.routeCache, key)
	c.rmu.Unlock()
}

// LookupCachedResult returns a cached response for (fqn, payload) when
// fresh, or nil. Callers consult this before issuing a dispatch.
func (c *HWPCaller) LookupCachedResult(fqn string, payload []byte) []byte {
	key := resultCacheKey(fqn, payload)

	c.cmu.RLock()
	entry, ok := c.resultCache[key]
	c.cmu.RUnlock()
	if !ok {
		return nil
	}
	if time.Now().After(entry.expires) {
		c.cmu.Lock()
		delete(c.resultCache, key)
		c.cmu.Unlock()
		return nil
	}
	return entry.payload
}

// StoreCachedResult records a successful response for the given TTL.
// ttl == 0 disables caching (no-op so callers can pass zero for non-
// idempotent ops without branching).
func (c *HWPCaller) StoreCachedResult(fqn string, payload, response []byte, ttl time.Duration) {
	if ttl <= 0 {
		return
	}
	key := resultCacheKey(fqn, payload)
	cp := make([]byte, len(response))
	copy(cp, response)

	c.cmu.Lock()
	c.resultCache[key] = &cachedResult{
		payload: cp,
		expires: time.Now().Add(ttl),
	}
	c.cmu.Unlock()
}

// InvalidateCachedResult drops a cached response. Use when an upstream
// signal (peer record HLC change, explicit invalidation message) shows
// the result is stale.
func (c *HWPCaller) InvalidateCachedResult(fqn string, payload []byte) {
	key := resultCacheKey(fqn, payload)
	c.cmu.Lock()
	delete(c.resultCache, key)
	c.cmu.Unlock()
}

// resultCacheKey derives a stable string key for the cache. SHA-256 of
// the payload keeps the key short and lookup time independent of
// payload size; the FQN prefix prevents cross-method collisions.
func resultCacheKey(fqn string, payload []byte) string {
	sum := sha256.Sum256(payload)
	return fqn + "\x00" + string(sum[:])
}

// maintain runs background eviction and stream pool refill.
func (c *HWPCaller) maintain() {
	ticker := time.NewTicker(callerMaintainTick)
	defer ticker.Stop()
	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			c.evictIdle()
			c.refillPools()
			c.evictExpiredCaches()
			c.evictIdleBreakers()
		}
	}
}

// evictIdleBreakers drops circuits that carry no state a fresh one would not
// (CircuitBreaker.idle). Circuits are keyed by session generation, so without
// this every replaced session would leave its circuits in the map for good.
func (c *HWPCaller) evictIdleBreakers() {
	now := time.Now()
	c.bmu.Lock()
	defer c.bmu.Unlock()
	for k, cb := range c.breakers {
		if cb.idle(now) {
			delete(c.breakers, k)
		}
	}
}

// evictExpiredCaches walks the route + result caches and drops expired
// entries. Without this, idle keys would only be reclaimed on lookup,
// which permanently leaks memory for one-shot RPCs that are never
// recalled.
func (c *HWPCaller) evictExpiredCaches() {
	now := time.Now()

	c.rmu.Lock()
	for k, v := range c.routeCache {
		if now.After(v.expires) {
			delete(c.routeCache, k)
		}
	}
	c.rmu.Unlock()

	c.cmu.Lock()
	for k, v := range c.resultCache {
		if now.After(v.expires) {
			delete(c.resultCache, k)
		}
	}
	c.cmu.Unlock()
}

// evictIdle removes sessions unused for longer than callerMaxIdle OR
// superseded by a higher-grade session for the same peer (grade upgrade
// invalidation). Without the upgrade check, a cross-region
// WS session cached on cold-start would keep being used for the full
// 5-minute idle TTL even after a same-region Noise-UDP session opened
// 30 seconds later.
func (c *HWPCaller) evictIdle() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for role, cs := range c.sessions {
		switch {
		case cs.session.IsClosed():
		case cs.idleSince() > callerMaxIdle:
		case c.finder != nil && c.finder.IsSupersededByUpgrade(cs.session):
			// Higher-grade session now available for this peer —
			// drop the cache so the next dispatch picks the upgrade.
		default:
			continue
		}
		if cs.pool != nil {
			cs.pool.Drain()
		}
		delete(c.sessions, role)
	}
}

// refillPools tops up stream pools for all healthy cached sessions.
//
// `isHealthy()` returns true unless the session is closed or has hit
// the consecutive-timeout ceiling (3+ with no recovery). That's a loose
// gate: even one timeout indicates the path is degraded right now, and
// minting fresh streams on top of a degraded session just pre-creates
// streams that will be dispatched onto a wedged transport. Tighten the
// refill predicate so a session with ANY pending failures (>= 1 timeout
// since the last success) skips this cycle; the next successful RPC
// resets the counter and re-enables refill.
func (c *HWPCaller) refillPools() {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, cs := range c.sessions {
		if cs.pool == nil || !cs.isHealthy() || cs.pool.Len() >= callerPoolSize {
			continue
		}
		// Skip refill on degraded sessions: any in-flight failure since
		// last success means we'd be priming streams that will fail.
		if atomic.LoadInt32(&cs.consecutiveTimeouts) > 0 {
			continue
		}
		cs.pool.Fill(c.lifeCtx, cs.session)
	}
}

// Call sends an RPC to the mesh node serving the given role.
// Computes up to 3 routes via LAD and probes them in parallel.
// First response wins. Each probe is governed by the circuit for (method,
// first-hop node, first-hop session generation). When the FQN
// "role/method" appears in the cacheable-handlers config the response
// is served from / stored in the caller-side cache automatically.
//
// The call leaves only on a first hop that grades at or above the floor of
// its operation class (the WithOpClass hint; none is Standard). With no such
// hop, or with a class the gate does not declare, it returns a
// *GradeTooLowError before anything is sent.
func (c *HWPCaller) Call(ctx context.Context, role, method string, payload []byte) ([]byte, error) {
	start := time.Now()

	// The grade gate, before any path and before the result cache: the class
	// this call names sets the lowest grade it may leave on, and a class the
	// gate does not declare is refused whatever is cached or connected.
	opClass := opClassFromContext(ctx)
	floor, err := classFloor(method, opClass)
	if err != nil {
		c.metrics.RecordCall(role, time.Since(start), false)
		return nil, fmt.Errorf("dispatch %s/%s: %w", role, method, err)
	}

	// Caller-side cache check for configured idempotent reads. Hot
	// callees (declared in dispatch.cacheableHandlers config) bypass
	// the mesh entirely when a fresh entry exists.
	fqn := role + "/" + method
	ttl := c.CacheableTTL(fqn)
	if ttl > 0 {
		if cached := c.LookupCachedResult(fqn, payload); cached != nil {
			c.metrics.RecordCall(role, time.Since(start), true)
			return cached, nil
		}
	}

	// Memoized FindRoutes lookup: the directory cache walk costs
	// 50-200µs on the warm path; a 5s TTL keeps it inside the freshness
	// window of the underlying directory snapshot.
	routes := c.findRoutesCached(ctx, role, method)
	if len(routes) == 0 {
		// Fallback: sequential callOnce (no routes means LAD has no data)
		resp, cb, err := c.callOnce(ctx, role, method, payload)
		if err == nil {
			if cb != nil {
				cb.RecordSuccess()
			}
			c.metrics.RecordCall(role, time.Since(start), true)
			if ttl > 0 {
				c.StoreCachedResult(fqn, payload, resp, ttl)
			}
			return resp, nil
		}
		c.metrics.RecordCall(role, time.Since(start), false)
		return nil, fmt.Errorf("dispatch %s/%s: %w", role, method, err)
	}

	// Only routes whose first hop grades at or above the floor are probed.
	// With none left the call is refused with the best grade on offer and the
	// floor it missed, before a circuit is leased or anything is sent.
	routes, best := routesAtOrAbove(routes, floor)
	if len(routes) == 0 {
		c.metrics.RecordCall(role, time.Since(start), false)
		return nil, fmt.Errorf("dispatch %s/%s: %w", role, method,
			&GradeTooLowError{Handler: method, Class: opClass, Current: best, Required: floor})
	}

	// Build base request
	var remaining time.Duration
	if deadline, ok := ctx.Deadline(); ok {
		remaining = time.Until(deadline)
	}
	baseReq := buildRPCRequestCtx(ctx, role, method, payload, remaining)

	// Set absolute deadline for per-hop TTL
	if remaining > 0 {
		baseReq.Deadline = time.Now().Add(remaining).UnixNano()
	}

	// Launch parallel probes — one per route
	type probeResult struct {
		payload        []byte
		nodeID         string
		err            error
		cb             *CircuitBreaker
		breakerHandled bool // true if the probe already settled its circuit, or the circuit refused it
	}
	resultCh := make(chan probeResult, len(routes))

	// Probe context: always capped at defaultProbeTimeout; WithTimeout keeps
	// a stricter caller deadline and clamps a longer or absent one, so a
	// wedged session can't hang the caller until an edge proxy's request
	// timeout. bidi.Call selects on ctx.Done() AND b.done — if the bidi's
	// underlying stream is stuck but not yet closed (e.g. flow-control
	// deadlock with inflight>0 + zero ACK progress for minutes), neither
	// channel fires and Call blocks until ctx.Done(). Without a deadline
	// that's effectively forever from dispatch's view.
	//
	// An HTTP handler context from r.Context() carries no deadline by
	// default. Capping per-call at 10 s gives healthy probes ample time
	// (typical mesh RPC RTT is < 50 ms) and forces dispatch off a wedged
	// path within one timeout instead of one edge timeout. A probe must
	// fail fast on a wedged path even if the caller is patient: a longer
	// caller deadline (e.g. a 15 s gateway timeout) must not keep the 10 s
	// probe cap from firing.
	probeCtx, probeCancel := context.WithTimeout(ctx, defaultProbeTimeout)
	defer probeCancel()

	for i, route := range routes {
		go func(idx int, r aether.ProbeRoute) {
			// The circuit for this capability over this first hop. Its own
			// refusal is not an availability failure, so it is reported as
			// already handled and never recorded against the circuit.
			cb := c.getBreaker(method, r.NodeID, r.Session)
			if !cb.Allow() {
				resultCh <- probeResult{nodeID: r.NodeID, err: ErrCircuitOpen, cb: cb, breakerHandled: true}
				return
			}

			// Clone request with route-specific fields
			req := proto.Clone(baseReq).(*pb.RPCRequest)
			req.RequestNonce = fmt.Sprintf("%s-%d", baseReq.Id, idx)
			req.TargetNodeId = r.TargetNodeID
			req.RouteList = r.RouteList

			// Try BidiRPC first, then dynamic stream. Bidi runs against a
			// TIGHT inner cap (bidiProbeTimeout) carved out of probeCtx so
			// the dynamic-stream fallback below still has a usable budget
			// when bidi expires it. Without this carve-out, a wedged bidi
			// would consume the full probeCtx and the fallback would run on
			// an already-expired ctx — defeating the fallthrough entirely.
			bidiCtx, bidiCancel := context.WithTimeout(probeCtx, bidiProbeTimeout)
			resp, ok, err := c.finder.CallViaBidi(bidiCtx, r.NodeID, req)
			bidiCancel()
			if ok {
				if err != nil {
					// The finder refused the session it would have sent on.
					// A refusal is not the path's availability, so the
					// circuit gets no verdict, and the dynamic stream below
					// is not tried for a route the gate just refused.
					if errors.Is(err, ErrGradeTooLow) {
						cb.Release()
						resultCh <- probeResult{nodeID: r.NodeID, err: err, cb: cb, breakerHandled: true}
						return
					}
					// Inner-deadline-only fallthrough: only the bidi cap
					// (or the parent probeCtx caused by another probe's
					// quick success cancelling everyone) fired. If the
					// caller's ctx is still healthy AND probeCtx has time
					// left, try the dynamic-stream fallback.
					if !errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
						resultCh <- probeResult{nodeID: r.NodeID, err: err, cb: cb}
						return
					}
					// fall through to dynamic stream below using probeCtx
				} else {
					cb.RecordSuccess() // transport worked regardless of app result
					if !resp.Success {
						resultCh <- probeResult{nodeID: r.NodeID, err: responseError(method, resp), cb: cb, breakerHandled: true}
						return
					}
					resultCh <- probeResult{payload: resp.Payload, nodeID: r.NodeID, cb: cb, breakerHandled: true}
					return
				}
			}

			// Dynamic stream fallback — uses the broader probeCtx (still
			// bounded by defaultProbeTimeout) so any time left after the
			// bidi attempt is available here.
			if r.Session == nil || r.Session.IsClosed() {
				resultCh <- probeResult{nodeID: r.NodeID, err: fmt.Errorf("session closed"), cb: cb}
				return
			}
			reqData, _ := pb.MarshalRequest(req)
			stream, err := c.openDynamicStream(probeCtx, r.Session, priorityForOpClass(req.Context["opClass"]))
			if err != nil {
				resultCh <- probeResult{nodeID: r.NodeID, err: err, cb: cb}
				return
			}
			defer releaseDynamicStream(r.Session, stream)
			if err := stream.Send(probeCtx, reqData); err != nil {
				resultCh <- probeResult{nodeID: r.NodeID, err: err, cb: cb}
				return
			}
			respData, err := stream.Receive(probeCtx)
			if err != nil {
				resultCh <- probeResult{nodeID: r.NodeID, err: err, cb: cb}
				return
			}
			dynResp, err := pb.UnmarshalResponse(respData)
			if err != nil {
				resultCh <- probeResult{nodeID: r.NodeID, err: err, cb: cb}
				return
			}
			cb.RecordSuccess() // transport worked regardless of app result
			if !dynResp.Success {
				resultCh <- probeResult{nodeID: r.NodeID, err: responseError(method, dynResp), cb: cb, breakerHandled: true}
				return
			}
			resultCh <- probeResult{payload: dynResp.Payload, nodeID: r.NodeID, cb: cb, breakerHandled: true}
		}(i, route)
	}

	// First success wins
	var lastErr error
	for i := 0; i < len(routes); i++ {
		select {
		case result := <-resultCh:
			if result.err == nil {
				probeCancel() // cancel remaining probes
				c.metrics.RecordCall(role, time.Since(start), true)
				// Stash for the configured TTL so the next identical
				// call gets the 0-RTT path.
				if ttl > 0 {
					c.StoreCachedResult(fqn, payload, result.payload, ttl)
				}
				return result.payload, nil
			}
			lastErr = result.err
			if !result.breakerHandled && result.cb != nil {
				// An error after the caller's context ended is the
				// caller's cancellation, not the path's availability:
				// it releases a half-open lease instead of counting.
				if ctx.Err() != nil {
					result.cb.Release()
				} else {
					result.cb.RecordFailure()
				}
			}
		case <-ctx.Done():
			c.metrics.RecordCall(role, time.Since(start), false)
			return nil, fmt.Errorf("dispatch %s/%s: %w", role, method, ctx.Err())
		}
	}

	// Every probe failed — drop the memoized route set so the next
	// dispatch recomputes from a fresh directory snapshot. Without this
	// a downed peer would keep being probed for routeCacheTTL.
	c.invalidateRouteCache(role, method)
	c.metrics.RecordCall(role, time.Since(start), false)
	// If the last error was a HandlerError (handler-side application error),
	// propagate that classification so the bridge maps it to 4xx instead of
	// the transport-failure default 502. Deterministic handler errors don't
	// belong in the same bucket as transport drops.
	if errors.Is(lastErr, ErrHandlerError) {
		return nil, fmt.Errorf("dispatch %s/%s: %w", role, method, lastErr)
	}
	// A typed route refusal stays typed for the same reason: the caller tests
	// it with errors.Is, which the flattened text below would defeat.
	if isRouteRefusal(lastErr) {
		return nil, fmt.Errorf("dispatch %s/%s: all %d probes failed: %w", role, method, len(routes), lastErr)
	}
	return nil, fmt.Errorf("dispatch %s/%s: all %d probes failed: %v", role, method, len(routes), lastErr)
}

// CallCacheable wraps Call with the caller-side result cache for
// idempotent reads. If a fresh entry exists for (role+method, payload)
// it returns immediately (0-RTT local hit); otherwise it dispatches via
// Call and records the response under ttl. Handlers / callers that
// know a method is non-idempotent must use Call directly so retries
// don't return stale data.
func (c *HWPCaller) CallCacheable(ctx context.Context, role, method string, payload []byte, ttl time.Duration) ([]byte, error) {
	fqn := role + "/" + method
	if cached := c.LookupCachedResult(fqn, payload); cached != nil {
		c.metrics.RecordCall(role, 0, true)
		return cached, nil
	}
	resp, err := c.Call(ctx, role, method, payload)
	if err != nil {
		return nil, err
	}
	c.StoreCachedResult(fqn, payload, resp, ttl)
	return resp, nil
}

// callOnce performs a single RPC attempt: find session → get stream → send → receive.
// Returns the response payload, the circuit that governed the attempt (nil
// when no session was found or the grade gate refused the one found), and any
// error. The circuit is the one for (method, the session's node, the
// session's generation).
func (c *HWPCaller) callOnce(ctx context.Context, role, method string, payload []byte) ([]byte, *CircuitBreaker, error) {
	session, cs, err := c.getOrFindSession(ctx, role, method)
	if err != nil {
		return nil, nil, fmt.Errorf("find session: %w", err)
	}

	// The grade gate on the one session this attempt would send on, before the
	// circuit is asked, so a refusal never takes a half-open lease.
	if err := CheckRouteGrade(method, opClassFromContext(ctx), session); err != nil {
		return nil, nil, err
	}

	nodeID := string(session.RemoteNodeID())

	cb := c.getBreaker(method, nodeID, session)
	if !cb.Allow() {
		c.metrics.RecordCircuitTrip(role)
		return nil, nil, fmt.Errorf("dispatch %s/%s: %w (node %s)", role, method, ErrCircuitOpen, shortNodeID(nodeID))
	}

	// Pre-flight: skip obviously dead sessions
	if session.IsClosed() {
		cb.Release()
		return nil, cb, fmt.Errorf("session closed before send")
	}

	// Try BidiRPC first (Stream 1) — avoids dynamic stream overhead.
	//
	// Bidi context: always capped at defaultProbeTimeout; WithTimeout keeps
	// a stricter caller deadline and clamps a longer or absent one. The
	// parallel-probe path in Call installs the same cap and the rationale
	// applies identically here: BidiRPC.Call selects on ctx.Done() AND
	// b.done — if the bidi's underlying stream is wedged (flow-control
	// deadlock, inflight>0, zero ACK progress for minutes) but not closed,
	// neither channel fires and Call blocks until ctx.Done(). An HTTP
	// handler context from r.Context() has no deadline by default, so
	// without this cap a gateway hangs every cached-session RPC until an
	// edge proxy's request timeout.
	if c.finder != nil {
		// Always cap bidi at defaultProbeTimeout. WithTimeout returns
		// MIN(parent_deadline, now + timeout) — a stricter caller deadline
		// is preserved; a longer one (e.g. a 15 s gateway timeout) gets
		// clamped to give the dispatch caller a chance to react to a wedged
		// bidi within 10s instead of riding the full gateway deadline.
		bidiCtx, cancel := context.WithTimeout(ctx, defaultProbeTimeout)
		defer cancel()
		var remaining time.Duration
		if deadline, ok := bidiCtx.Deadline(); ok {
			remaining = time.Until(deadline)
		}
		gradeTimeout := rpcTimeoutForProtocol(session.Protocol())
		bidiReq := buildRPCRequestCtx(bidiCtx, role, method, payload, remaining, gradeTimeout)
		if resp, ok, err := c.finder.CallViaBidi(bidiCtx, nodeID, bidiReq); ok {
			if errors.Is(err, ErrGradeTooLow) {
				// The finder refused the session it would have sent on: a
				// refusal, not the path's availability.
				cb.Release()
				return nil, cb, err
			}
			if err != nil {
				// Inner-deadline-only: the 10s defaultProbeTimeout cap
				// fired but the caller's ctx is still healthy. Fall
				// through to the dynamic-stream path rather than hard-
				// failing the dispatch. Without this check the cap
				// would be a hard ceiling instead of a recovery trigger,
				// and a wedged bidi would fail every dispatch it carries.
				if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
					// no breaker failure here — the fallback attempt
					// will record its own success/failure
				} else if ctx.Err() != nil {
					// The caller's context ended: its cancellation is
					// not the path's availability.
					cb.Release()
					return nil, cb, fmt.Errorf("bidi: %w", err)
				} else {
					if cs != nil {
						cs.recordTimeout()
					}
					cb.RecordFailure()
					return nil, cb, fmt.Errorf("bidi: %w", err)
				}
			} else {
				if cs != nil {
					cs.recordSuccess()
				}
				cb.RecordSuccess()
				if !resp.Success {
					return nil, cb, responseError(method, resp)
				}
				return resp.Payload, cb, nil
			}
		}
	}

	// Fallback: dynamic stream from pool (or open fresh). Pull the
	// op_class hint from ctx so callOnce (warm path) gets the same
	// priority-routing as the parallel-probe path. Without this the warm
	// path would always ship at Priority=128 regardless of the caller's
	// intent.
	var stream aether.Stream
	if cs != nil && cs.pool != nil {
		stream, err = cs.pool.Get(ctx, session)
	} else {
		stream, err = c.openDynamicStream(ctx, session, priorityForOpClass(opClassFromContext(ctx)))
	}
	if err != nil {
		if cs != nil {
			cs.recordTimeout()
		}
		return nil, cb, fmt.Errorf("open stream: %w", err)
	}
	defer releaseDynamicStream(session, stream)

	// Build request with grade-aware deadline propagation
	var remaining time.Duration
	if deadline, ok := ctx.Deadline(); ok {
		remaining = time.Until(deadline)
	}
	rpcTimeout := rpcTimeoutForProtocol(session.Protocol())
	req := buildRPCRequestCtx(ctx, role, method, payload, remaining, rpcTimeout)
	reqData, err := pb.MarshalRequest(req)
	if err != nil {
		return nil, cb, fmt.Errorf("marshal: %w", err)
	}
	rpcCtx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	// Send request
	if err := stream.Send(rpcCtx, reqData); err != nil {
		if cs != nil {
			cs.recordTimeout()
		}
		c.metrics.RecordTimeout(role)
		return nil, cb, fmt.Errorf("send: %w", err)
	}

	// Receive response
	respData, err := stream.Receive(rpcCtx)
	if err != nil {
		if cs != nil {
			cs.recordTimeout()
		}
		c.metrics.RecordTimeout(role)
		return nil, cb, fmt.Errorf("receive: %w", err)
	}

	// Decode response
	resp, err := pb.UnmarshalResponse(respData)
	if err != nil {
		return nil, cb, fmt.Errorf("unmarshal: %w", err)
	}
	if !resp.Success {
		// Application-level error — session is healthy, request was processed
		if cs != nil {
			cs.recordSuccess()
		}
		return nil, cb, responseError(method, resp)
	}

	// Success
	if cs != nil {
		cs.recordSuccess()
	}
	return resp.Payload, cb, nil
}

// buildRPCRequest creates an RPCRequest with deadline propagation and unique ID.
// gradeTimeout overrides the default if positive (from session grade).
func buildRPCRequest(role, method string, payload []byte, remaining time.Duration, gradeTimeout ...time.Duration) *pb.RPCRequest {
	return buildRPCRequestCtx(nil, role, method, payload, remaining, gradeTimeout...)
}

// CallNode implements TargetedCaller: dispatch to ONE named node, or fail.
//
// This is deliberately NOT a variant of Call. Call resolves a role to up to
// three routes, probes them in parallel, and falls back to a dynamic stream —
// every one of those behaviours is correct for role-addressed dispatch and
// wrong here. A targeted call that quietly reached a different node serving the
// same role would be worse than no targeting at all, because the caller would
// believe the target was honoured.
//
// The fail-closed conversion is the whole point. finder.CallViaBidi reports
// "no bidi channel for this node" as an ordinary (nil, false, nil) — a signal
// its other callers (see the probe loop and callOnce above) correctly read as
// "take the untargeted arm". Here that same signal MUST become an error, or the
// guarantee this method exists to provide evaporates exactly when it matters:
// a node that has just been placed is precisely the node with no bidi yet.
//
// Deliberate omission: this path does NOT record circuit-breaker state. The
// circuits are driven by the role-dispatch probe loop's and callOnce's own
// success/failure accounting; having a second, differently-shaped caller
// mutate them would corrupt that signal. A targeted call that fails returns the
// error to its caller and lets the placement layer decide.
func (c *HWPCaller) CallNode(ctx context.Context, nodeID, role, method string, payload []byte) ([]byte, error) {
	if c == nil || c.finder == nil {
		return nil, fmt.Errorf("dispatch %s/%s: no session finder: %w", role, method, ErrNoRouteToNode)
	}
	if nodeID == "" {
		return nil, fmt.Errorf("dispatch %s/%s: empty nodeID: %w", role, method, ErrNoRouteToNode)
	}
	// An undeclared class is refused here, before the finder is asked. The
	// floor itself is the finder's to hold, in CallViaBidi, because only it
	// knows which session the named node is reached on.
	if _, err := classFloor(method, opClassFromContext(ctx)); err != nil {
		return nil, fmt.Errorf("dispatch %s/%s to node %s: %w", role, method, nodeID, err)
	}

	var remaining time.Duration
	if deadline, ok := ctx.Deadline(); ok {
		remaining = time.Until(deadline)
	}
	req := buildRPCRequestCtx(ctx, role, method, payload, remaining)
	if remaining > 0 {
		req.Deadline = time.Now().Add(remaining).UnixNano()
	}

	resp, ok, err := c.finder.CallViaBidi(ctx, nodeID, req)
	if !ok {
		// The load-bearing line. CallViaBidi returns ok=false for "no mesh
		// session", "no bidi registered" and "bidi not alive" — all with a nil
		// error. Returning anything other than a failure here would silently
		// hand the request to role resolution.
		return nil, fmt.Errorf("dispatch %s/%s to node %s: %w", role, method, nodeID, ErrNoRouteToNode)
	}
	if err != nil {
		return nil, fmt.Errorf("dispatch %s/%s to node %s: %w", role, method, nodeID, err)
	}
	if resp == nil {
		return nil, fmt.Errorf("dispatch %s/%s to node %s: nil response: %w", role, method, nodeID, ErrNoRouteToNode)
	}
	if !resp.Success {
		return nil, responseError(method, resp)
	}
	return resp.Payload, nil
}

// buildRPCRequestCtx is the ctx-aware variant of buildRPCRequest. It copies
// the opClass hint (WithOpClass) to req.Context["opClass"], which the
// receiver's stream priority reads, and projects the caller's sealed
// principal onto req.Principal.
func buildRPCRequestCtx(ctx context.Context, role, method string, payload []byte, remaining time.Duration, gradeTimeout ...time.Duration) *pb.RPCRequest {
	timeout := callerRequestTTL
	if len(gradeTimeout) > 0 && gradeTimeout[0] > 0 {
		timeout = gradeTimeout[0]
	}
	if remaining > 0 && remaining < timeout {
		timeout = remaining
	}
	id := newRequestID()
	rpcCtx := map[string]string{"role": role}
	if oc := opClassFromContext(ctx); oc != "" {
		rpcCtx["opClass"] = oc
	}
	var wirePrincipal *pb.AuthenticatedPrincipal
	if principal, ok := rpcscope.AuthenticatedPrincipalFromContext(ctx); ok {
		// Project loom's opaque identity onto the wire generically: every
		// established axis travels as a ScopeAxis (aether names none), the
		// subject as Subject, the grants as Scopes and the partition as
		// Partition. The receiving node's ports.PrincipalAdmitter decides
		// whether it accepts what this node asserts over that transport.
		identity := principal.Identity()
		axes := make([]*pb.ScopeAxis, 0, len(identity.Scopes))
		for i := range identity.Scopes {
			axes = append(axes, &pb.ScopeAxis{
				Name:  identity.Scopes[i].Name,
				Value: identity.Scopes[i].Value,
			})
		}
		wirePrincipal = &pb.AuthenticatedPrincipal{
			Subject:   identity.Subject,
			Axes:      axes,
			Scopes:    principal.Scopes(),
			Partition: identity.Partition(),
		}
	}
	return &pb.RPCRequest{
		Id:        id,
		Handler:   method,
		Payload:   payload,
		Context:   rpcCtx,
		TimeoutNs: int64(timeout),
		TraceId:   trace.FromContext(ctx),
		Principal: wirePrincipal,
	}
}

// getOrFindSession returns a cached healthy session or finds a new one.
//
// Singleflight per role: when N concurrent calls hit the same cache miss
// for a role, only ONE invokes c.finder.FindSession + builds the pool;
// the others wait on the same in-flight result. Without it a thundering
// herd of cache-miss dispatches would each walk the directory and
// overwrite the cache, orphaning the others' StreamPool fills.
func (c *HWPCaller) getOrFindSession(ctx context.Context, role, handler string) (aether.Session, *cachedSession, error) {
	// Fast path: cache hit on a healthy session.
	c.mu.RLock()
	if cs, ok := c.sessions[role]; ok && cs.isHealthy() {
		cs.touchUsed()
		c.mu.RUnlock()
		return cs.session, cs, nil
	}
	c.mu.RUnlock()

	// Slow path: singleflight the FindSession round. Coalesce concurrent
	// callers for the same role onto one in-flight result.
	v, err, _ := c.findGroup.Do(role, func() (any, error) {
		// Re-check cache under the singleflight lock — a sibling caller
		// may have populated the cache between our cache-miss above and
		// our singleflight admission.
		c.mu.RLock()
		if cs, ok := c.sessions[role]; ok && cs.isHealthy() {
			cs.touchUsed()
			c.mu.RUnlock()
			return findResult{session: cs.session, cached: cs}, nil
		}
		c.mu.RUnlock()

		session, err := c.finder.FindSession(ctx, role, handler)
		if err != nil {
			// Fallback: try any available session for forwarding. NOT
			// cached because caching forwarding sessions prevents
			// switching to direct when one becomes available later.
			if anySession, ok := c.finder.FindAnySession(); ok && !anySession.IsClosed() {
				return findResult{session: anySession, cached: nil}, nil
			}
			return findResult{}, err
		}

		cs := &cachedSession{
			session:      session,
			role:         role,
			lastUsedNano: time.Now().UnixNano(),
			pool:         NewStreamPool(callerPoolSize),
		}
		cs.pool.Fill(c.lifeCtx, session)
		c.mu.Lock()
		c.sessions[role] = cs
		c.mu.Unlock()
		return findResult{session: session, cached: cs}, nil
	})
	if err != nil {
		return nil, nil, err
	}
	res := v.(findResult)
	return res.session, res.cached, nil
}

// findResult is the singleflight payload from getOrFindSession.
// cached is nil when the session is a forwarding fallback that mustn't
// be cached at the role-tier.
type findResult struct {
	session aether.Session
	cached  *cachedSession
}

// openDynamicStream opens a fresh stream (ID 10+) for this RPC call.
// Used as fallback when no stream pool is available. The priority arg
// lets callers map req.Context["opClass"] to the right band; at a fixed
// Priority=128, a bulk file-transfer RPC and a control-plane RPC would
// share the same WDRR weight and a large bulk payload could head-of-line
// block control traffic.
//
// If priority is 0, defaults to 128 (Standard).
//
// IDs come from the session's table in streamid.go, which StreamPool
// shares, so the two never open the same ID and neither leaves the
// dynamic range below the application band.
func (c *HWPCaller) openDynamicStream(ctx context.Context, session aether.Session, priority uint8) (aether.Stream, error) {
	if priority == 0 {
		priority = 128
	}
	return openDynamicStreamOn(ctx, session, aether.StreamConfig{
		Reliability: aether.ReliableOrdered,
		Priority:    priority,
	})
}

// opClassContextKey is the unexported type used as the context-value
// key for opClass. Callers use WithOpClass / opClassFromContext so
// both the dispatch warm path (callOnce) and the parallel-probe path
// pick up the same hint without needing different argument shapes.
type opClassContextKey struct{}

// WithOpClass returns a context that carries the operation-class hint
// used by the dispatch path to pick stream priority. Empty class is a
// no-op (same as Standard). callOnce (the warm/cached-session path, the
// dominant code path in steady state) reads it; without it that path would
// ship at priority=128 even when realtime / bulk classes were specified.
func WithOpClass(ctx context.Context, opClass string) context.Context {
	if opClass == "" {
		return ctx
	}
	return context.WithValue(ctx, opClassContextKey{}, opClass)
}

// opClassFromContext extracts the opClass hint set by WithOpClass.
// Returns "" when not set.
func opClassFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if v, ok := ctx.Value(opClassContextKey{}).(string); ok {
		return v
	}
	return ""
}

// priorityForOpClass maps req.Context["opClass"] to a WDRR Priority
// value. The mapping mirrors the aether stream-class hierarchy:
//   - critical/realtime → high priority (240)
//   - standard          → middle (128, the default)
//   - bulk              → low (64)
func priorityForOpClass(opClass string) uint8 {
	switch opClass {
	case "critical", "realtime":
		return 240
	case "bulk":
		return 64
	default:
		return 128
	}
}

// getBreaker returns the circuit for one capability over one node's session,
// creating it if needed. sess may be nil, which keys the zero generation.
func (c *HWPCaller) getBreaker(capability, nodeID string, sess aether.Session) *CircuitBreaker {
	key := breakerKey{capability: capability, node: nodeID}
	if sess != nil {
		key.generation = sess.ConnectionID()
	}
	// Fast path: read lock
	c.bmu.RLock()
	cb, ok := c.breakers[key]
	c.bmu.RUnlock()
	if ok {
		return cb
	}
	// Slow path: create new breaker
	c.bmu.Lock()
	defer c.bmu.Unlock()
	// Double-check after acquiring write lock
	if cb, ok = c.breakers[key]; ok {
		return cb
	}
	cb = NewCircuitBreaker(callerBreakerThreshold, callerBreakerWindow, c.breakerReset)
	c.breakers[key] = cb
	return cb
}

// shortNodeID is the first 12 characters of a node id, or all of a shorter one.
func shortNodeID(id string) string {
	if len(id) > 12 {
		return id[:12]
	}
	return id
}

// Metrics returns the RPC metrics for monitoring endpoints.
func (c *HWPCaller) Metrics() *RPCMetrics {
	return c.metrics
}

// Close shuts down the caller, stops maintenance, and clears the cache. The
// lifecycle context is cancelled before the session lock is taken, because
// refillPools holds that lock's read side across a pool fill, and the
// cancellation is what ends a fill blocked on a stalled session.
func (c *HWPCaller) Close() {
	close(c.stopCh)
	c.lifeCancel()
	c.mu.Lock()
	defer c.mu.Unlock()
	for role, cs := range c.sessions {
		if cs.pool != nil {
			cs.pool.Drain()
		}
		delete(c.sessions, role)
	}
}

// Ensure HWPCaller implements Caller at compile time.
var _ Caller = (*HWPCaller)(nil)

// rpcTimeoutForProtocol returns the RPC timeout for a protocol from its
// capabilities. It is defined in this package because node depends on this
// package through pkg/rpc, so reading a node timeout here would be an import
// cycle.
func rpcTimeoutForProtocol(proto aether.Protocol) time.Duration {
	caps := proto.Capabilities()
	if !caps.NativeReliability && caps.NativeEncryption {
		return 5 * time.Second // Grade A (Noise)
	}
	if caps.NativeMux {
		return 10 * time.Second // Grade B (QUIC/gRPC)
	}
	if caps.NativeReliability {
		return 30 * time.Second // Grade C (WS/TCP)
	}
	return 5 * time.Second // Grade F
}
