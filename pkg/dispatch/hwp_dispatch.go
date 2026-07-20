/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package dispatch

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ORBTR/aether/rpc/pb"
	"github.com/ORBTR/aether"
	"github.com/bbmumford/loom/pkg/trace"
	"golang.org/x/sync/singleflight"
	"google.golang.org/protobuf/proto"
)

// ────────────────────────────────────────────────────────────────────────────
// Session health tracking (mirrors old DispatchMux health model)
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

// isHealthy returns true if the session is alive and responsive.
// Matches DispatchMux logic: closed=unhealthy, 3+ consecutive timeouts=unhealthy,
// but recovers after 30 seconds (prevents permanent blacklisting during churn).
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
// Resilience model (matches and exceeds old DispatchMux/SessionPool):
//   - 5-minute session cache with 30s eviction tick
//   - Per-session health tracking (consecutive timeouts + recovery window)
//   - Per-role circuit breaker (open/half-open/closed, 5 failures → trip)
//   - 2-attempt retry: evict dead session, find fresh session, retry once
//   - Pre-opened stream pool (2 per session, background refill)
//   - Deadline propagation via RPCRequest.Timeout
//   - Unique request IDs for tracing and future multiplexing
//   - Per-role RPC metrics (calls, latency, timeouts, circuit trips)
//   - Load-aware routing via LAD latency records
//   - Forwarding sessions NOT cached (prevents lock-in)
type HWPCaller struct {
	mu             sync.RWMutex
	sessions       map[string]*cachedSession // role → cached session
	finder         SessionFinder
	// findGroup coalesces concurrent getOrFindSession callers for the
	// same role onto one in-flight result. Without this, a cache miss
	// for a hot role spawned N parallel FindSession calls that all
	// walked the directory/LAD then raced to overwrite the cache —
	// each loser leaked its StreamPool fill. M-FindSession-Stampede fix.
	findGroup      singleflight.Group
	// (removed: per-caller streamID counter — replaced by session-scoped
	// counter in streamid.go.nextDynamicStreamID to prevent collisions
	// between HWPCaller and StreamPool allocators sharing the same
	// session. H-Dispatch-StreamIDCollision fix.)
	stopCh         chan struct{}
	bmu            sync.RWMutex               // separate lock for breakers (reduces contention)
	breakers       map[string]*CircuitBreaker  // nodeID → breaker (per-node, not per-role)
	metrics        *RPCMetrics
	platformTenant string // HSTLES platform tenant (e.g., "orbtr", "hstles") — injected into every RPC context

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

// defaultProbeTimeout caps a single BidiRPC.Call when the caller's
// context has no deadline. Applied by both the parallel-probe path in
// Call (every fan-out probe goroutine) and the warm callOnce path
// (every cached-session RPC). Without this cap a wedged bidi stream
// blocks dispatch until Fly's edge timeout (~60s), since BidiRPC.Call
// selects on ctx.Done() AND b.done — neither fires when the stream is
// flow-control-deadlocked but technically still open. 10s gives healthy
// probes ample headroom (typical mesh RPC RTT < 50 ms) while bounding
// the wedge to one timeout instead of one edge timeout.
const defaultProbeTimeout = 10 * time.Second

// bidiProbeTimeout is the inner cap on a single BidiRPC attempt inside the
// parallel-probe goroutine. Tighter than defaultProbeTimeout so that, when
// bidi is wedged, the dynamic-stream fallback at the bottom of the goroutine
// still has a usable budget against probeCtx. Without this carve-out the
// fallback R2 added ran on an already-expired ctx and could never succeed.
// 3 s covers healthy bidis (typical < 50 ms) with ~60x headroom; the
// remaining 7 s of probeCtx are available for the dynamic-stream attempt.
const bidiProbeTimeout = 3 * time.Second

// SetPlatformTenant sets the platform tenant ID that is propagated in every
// outgoing RPC context map. This ensures HSTLES handlers with TenantScope
// enforcement accept the call and scope it to the correct platform tenant.
//
// The platform tenant is the HSTLES-level app identity (e.g., "orbtr"),
// NOT the end-user's org ID. Set once at startup from cfg.Apps.App.TenantID.
func (c *HWPCaller) SetPlatformTenant(tenantID string) {
	c.platformTenant = tenantID
}

// SessionFinder resolves a role + handler to an HWPSession.
// Implemented by the ConnectionManager which tracks all peer sessions.
type SessionFinder interface {
	FindSession(ctx context.Context, role, handler string) (aether.Session, error)
	FindAnySession() (aether.Session, bool)
	// CallViaBidi sends an RPC via BidiRPC (Stream 1) if available for the node.
	// Returns (nil, false, nil) if no BidiRPC channel exists for this node.
	CallViaBidi(ctx context.Context, nodeID string, req *pb.RPCRequest) (*pb.RPCResponse, bool, error)
	// FindRoutes computes up to 3 routes to a handler's role.
	// Each route has a first-hop session + remaining route list.
	FindRoutes(ctx context.Context, role, handler string) []aether.ProbeRoute
	// IsSupersededByUpgrade reports whether the given session has been
	// replaced by a higher-grade session for the same peer. Used by the
	// HWPCaller eviction loop to drop stale cached sessions so the next
	// dispatch picks the upgraded one — L3 #7 fix.
	IsSupersededByUpgrade(session aether.Session) bool
}

const (
	callerMaxIdle      = 5 * time.Minute  // session cache TTL (matches old SessionPool)
	callerMaintainTick = 30 * time.Second // background eviction + pool refill interval
	callerRequestTTL   = 30 * time.Second // per-request timeout (matches old DispatchMux)
	callerPoolSize     = 2                // pre-opened streams per session
)

// requestIDCounter generates unique IDs across all HWPCaller instances.
var requestIDCounter uint64

// NewHWPCaller creates a native HWP dispatch caller with background maintenance.
func NewHWPCaller(finder SessionFinder) *HWPCaller {
	c := &HWPCaller{
		sessions:     make(map[string]*cachedSession),
		finder:       finder,
		stopCh:       make(chan struct{}),
		breakers:     make(map[string]*CircuitBreaker),
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
// invalidation — L3 #7). Without the upgrade check, a cross-region
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
		cs.pool.Fill(cs.session)
	}
}

// Call sends an RPC to the mesh node serving the given role.
// Computes up to 3 routes via LAD and probes them in parallel.
// First response wins. Circuit breakers are per-node. When the FQN
// "role/method" appears in the cacheable-handlers config the response
// is served from / stored in the caller-side cache automatically.
func (c *HWPCaller) Call(ctx context.Context, role, method string, payload []byte) ([]byte, error) {
	start := time.Now()

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
		resp, nodeID, err := c.callOnce(ctx, role, method, payload)
		if err == nil {
			if nodeID != "" {
				c.getBreaker(nodeID).RecordSuccess()
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

	// Build base request
	var remaining time.Duration
	if deadline, ok := ctx.Deadline(); ok {
		remaining = time.Until(deadline)
	}
	baseReq := buildRPCRequestCtx(ctx, role, method, payload, remaining, c.platformTenant)

	// Set absolute deadline for per-hop TTL
	if remaining > 0 {
		baseReq.Deadline = time.Now().Add(remaining).UnixNano()
	}

	// Launch parallel probes — one per route
	type probeResult struct {
		payload        []byte
		nodeID         string
		err            error
		breakerHandled bool // true if probe already recorded success/failure on breaker
	}
	resultCh := make(chan probeResult, len(routes))

	// Probe context: caller's ctx, unless it has no deadline — in which
	// case cap at defaultProbeTimeout so a wedged session can't hang the
	// caller for Fly's full 60 s edge timeout (which surfaces to the
	// user as a 502). bidi.Call selects on ctx.Done() AND b.done — if
	// the bidi's underlying stream is stuck but not yet closed (e.g.
	// flow-control deadlock with inflight>0 + zero ACK progress for
	// minutes — observed on stream 1 with progressAge=28m in fleet),
	// neither channel fires and Call blocks until ctx.Done(). Without
	// a deadline that's effectively forever from dispatch's view.
	//
	// HTTP handler contexts (the OAuth callback path) never carry a
	// deadline — Go's net/http defaults to a per-server cap which Fly's
	// edge supersedes well before app.orbtr.io's handler is willing to
	// stop. Capping per-call at 10 s gives healthy probes ample time
	// (typical mesh RPC RTT is < 50 ms) and forces dispatch off a
	// wedged path within one timeout instead of one edge timeout.
	// Always cap probes at defaultProbeTimeout. WithTimeout returns
	// MIN(parent_deadline, now + timeout) per the context contract — a
	// caller with a stricter deadline retains it, while a caller with no
	// deadline OR a deadline LONGER than defaultProbeTimeout gets clamped
	// down. The point is "a probe must fail fast on a wedged path even if
	// the caller is patient" — e.g. the 15s RequestTimeout middleware on
	// /rpc/{handler} should NOT prevent the 10s probe cap from firing.
	probeCtx, probeCancel := context.WithTimeout(ctx, defaultProbeTimeout)
	defer probeCancel()

	for i, route := range routes {
		go func(idx int, r aether.ProbeRoute) {
			// Per-node circuit breaker check
			cb := c.getBreaker(r.NodeID)
			if !cb.Allow() {
				resultCh <- probeResult{nodeID: r.NodeID, err: ErrCircuitOpen}
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
			// consumed the full probeCtx and the fallback ran on an already-
			// expired ctx — defeating the very fallthrough R2 wired in.
			bidiCtx, bidiCancel := context.WithTimeout(probeCtx, bidiProbeTimeout)
			resp, ok, err := c.finder.CallViaBidi(bidiCtx, r.NodeID, req)
			bidiCancel()
			if ok {
				if err != nil {
					// Inner-deadline-only fallthrough: only the bidi cap
					// (or the parent probeCtx caused by another probe's
					// quick success cancelling everyone) fired. If the
					// caller's ctx is still healthy AND probeCtx has time
					// left, try the dynamic-stream fallback.
					if !errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
						resultCh <- probeResult{nodeID: r.NodeID, err: err}
						return
					}
					// fall through to dynamic stream below using probeCtx
				} else {
					cb.RecordSuccess() // transport worked regardless of app result
					if !resp.Success {
						resultCh <- probeResult{nodeID: r.NodeID, err: NewHandlerError(resp.Error), breakerHandled: true}
						return
					}
					resultCh <- probeResult{payload: resp.Payload, nodeID: r.NodeID, breakerHandled: true}
					return
				}
			}

			// Dynamic stream fallback — uses the broader probeCtx (still
			// bounded by defaultProbeTimeout) so any time left after the
			// bidi attempt is available here.
			if r.Session == nil || r.Session.IsClosed() {
				resultCh <- probeResult{nodeID: r.NodeID, err: fmt.Errorf("session closed")}
				return
			}
			reqData, _ := pb.MarshalRequest(req)
			stream, err := c.openDynamicStream(probeCtx, r.Session, priorityForOpClass(req.Context["opClass"]))
			if err != nil {
				resultCh <- probeResult{nodeID: r.NodeID, err: err}
				return
			}
			if err := stream.Send(probeCtx, reqData); err != nil {
				resultCh <- probeResult{nodeID: r.NodeID, err: err}
				return
			}
			respData, err := stream.Receive(probeCtx)
			if err != nil {
				resultCh <- probeResult{nodeID: r.NodeID, err: err}
				return
			}
			dynResp, err := pb.UnmarshalResponse(respData)
			if err != nil {
				resultCh <- probeResult{nodeID: r.NodeID, err: err}
				return
			}
			cb.RecordSuccess() // transport worked regardless of app result
			if !dynResp.Success {
				resultCh <- probeResult{nodeID: r.NodeID, err: NewHandlerError(dynResp.Error), breakerHandled: true}
				return
			}
			resultCh <- probeResult{payload: dynResp.Payload, nodeID: r.NodeID, breakerHandled: true}
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
			if !result.breakerHandled {
				c.getBreaker(result.nodeID).RecordFailure()
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
// Returns the response payload, the target nodeID (for per-node circuit breaker), and any error.
func (c *HWPCaller) callOnce(ctx context.Context, role, method string, payload []byte) ([]byte, string, error) {
	session, cs, err := c.getOrFindSession(ctx, role, method)
	if err != nil {
		return nil, "", fmt.Errorf("find session: %w", err)
	}

	nodeID := string(session.RemoteNodeID())

	// Per-node circuit breaker check (after session discovery)
	cb := c.getBreaker(nodeID)
	if !cb.Allow() {
		c.metrics.RecordCircuitTrip(role)
		return nil, nodeID, fmt.Errorf("dispatch %s/%s: %w (node %s)", role, method, ErrCircuitOpen, nodeID[:12])
	}

	// Pre-flight: skip obviously dead sessions
	if session.IsClosed() {
		return nil, nodeID, fmt.Errorf("session closed before send")
	}

	// Try BidiRPC first (Stream 1) — avoids dynamic stream overhead.
	//
	// Cap the bidi call at defaultProbeTimeout when the caller's ctx has
	// no deadline. The parallel-probe path in Call (above, ~lines 487-506)
	// installs the same cap and the rationale applies identically here:
	// BidiRPC.Call selects on ctx.Done() AND b.done — if the bidi's
	// underlying stream is wedged (flow-control deadlock, inflight>0,
	// zero ACK progress for minutes — observed on stream 1 with
	// progressAge=28m in fleet) but not closed, neither channel fires
	// and Call blocks until ctx.Done(). HTTP handler contexts from
	// r.Context() have no deadline by default, so without this cap the
	// gateway hangs every cached-session RPC until Fly's edge timeout
	// (~60s), surfacing as a 502 to the user.
	if c.finder != nil {
		// Always cap bidi at defaultProbeTimeout. WithTimeout returns
		// MIN(parent_deadline, now + timeout) — a stricter caller deadline
		// is preserved; a longer one (e.g. /rpc/{handler}'s 15s gateway
		// timeout) gets clamped to give the dispatch caller a chance to
		// react to a wedged bidi within 10s instead of riding the full
		// gateway deadline.
		bidiCtx, cancel := context.WithTimeout(ctx, defaultProbeTimeout)
		defer cancel()
		var remaining time.Duration
		if deadline, ok := bidiCtx.Deadline(); ok {
			remaining = time.Until(deadline)
		}
		gradeTimeout := rpcTimeoutForProtocol(session.Protocol())
		bidiReq := buildRPCRequestCtx(bidiCtx, role, method, payload, remaining, c.platformTenant, gradeTimeout)
		if resp, ok, err := c.finder.CallViaBidi(bidiCtx, nodeID, bidiReq); ok {
			if err != nil {
				// Inner-deadline-only: the 10s defaultProbeTimeout cap
				// fired but the caller's ctx is still healthy. Fall
				// through to the dynamic-stream path rather than hard-
				// failing the dispatch. Without this check the cap
				// became a hard ceiling instead of a recovery trigger
				// (the failure mode that produced 0/20 dispatch success
				// in v0.0.337 + the residual R2 wedge after v0.0.338).
				if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
					// no breaker failure here — the fallback attempt
					// will record its own success/failure
				} else {
					if cs != nil {
						cs.recordTimeout()
					}
					cb.RecordFailure()
					return nil, nodeID, fmt.Errorf("bidi: %w", err)
				}
			} else {
				if cs != nil {
					cs.recordSuccess()
				}
				cb.RecordSuccess()
				if !resp.Success {
					return nil, nodeID, NewHandlerError(resp.Error)
				}
				return resp.Payload, nodeID, nil
			}
		}
	}

	// Fallback: dynamic stream from pool (or open fresh). Pull the
	// op_class hint from ctx so callOnce (warm path) gets the same
	// priority-routing as the parallel-probe path. Audit H7 — without
	// this the warm path always shipped at Priority=128 regardless
	// of the caller's intent.
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
		return nil, nodeID, fmt.Errorf("open stream: %w", err)
	}

	// Build request with grade-aware deadline propagation
	var remaining time.Duration
	if deadline, ok := ctx.Deadline(); ok {
		remaining = time.Until(deadline)
	}
	rpcTimeout := rpcTimeoutForProtocol(session.Protocol())
	req := buildRPCRequestCtx(ctx, role, method, payload, remaining, c.platformTenant, rpcTimeout)
	reqData, err := pb.MarshalRequest(req)
	if err != nil {
		return nil, nodeID, fmt.Errorf("marshal: %w", err)
	}
	rpcCtx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()

	// Send request
	if err := stream.Send(rpcCtx, reqData); err != nil {
		if cs != nil {
			cs.recordTimeout()
		}
		c.metrics.RecordTimeout(role)
		return nil, nodeID, fmt.Errorf("send: %w", err)
	}

	// Receive response
	respData, err := stream.Receive(rpcCtx)
	if err != nil {
		if cs != nil {
			cs.recordTimeout()
		}
		c.metrics.RecordTimeout(role)
		return nil, nodeID, fmt.Errorf("receive: %w", err)
	}

	// Decode response
	resp, err := pb.UnmarshalResponse(respData)
	if err != nil {
		return nil, nodeID, fmt.Errorf("unmarshal: %w", err)
	}
	if !resp.Success {
		// Application-level error — session is healthy, request was processed
		if cs != nil {
			cs.recordSuccess()
		}
		return nil, nodeID, NewHandlerError(resp.Error)
	}

	// Success
	if cs != nil {
		cs.recordSuccess()
	}
	return resp.Payload, nodeID, nil
}

// buildRPCRequest creates an RPCRequest with deadline propagation and unique ID.
// platformTenant is injected as "tenantId" in the RPC context map so receiving
// handlers can enforce TenantScope (e.g., HSTLES identity handlers require tenant context).
// gradeTimeout overrides the default if positive (from session grade).
func buildRPCRequest(role, method string, payload []byte, remaining time.Duration, platformTenant string, gradeTimeout ...time.Duration) *pb.RPCRequest {
	return buildRPCRequestCtx(nil, role, method, payload, remaining, platformTenant, gradeTimeout...)
}

// buildRPCRequestCtx is the ctx-aware variant. Use it when the caller
// has a Go context carrying opClass via WithOpClass — the hint gets
// propagated to req.Context["opClass"] so the receiver's stream
// priority + scheduling matches the sender's intent.
func buildRPCRequestCtx(ctx context.Context, role, method string, payload []byte, remaining time.Duration, platformTenant string, gradeTimeout ...time.Duration) *pb.RPCRequest {
	timeout := callerRequestTTL
	if len(gradeTimeout) > 0 && gradeTimeout[0] > 0 {
		timeout = gradeTimeout[0]
	}
	if remaining > 0 && remaining < timeout {
		timeout = remaining
	}
	id := strconv.FormatUint(atomic.AddUint64(&requestIDCounter, 1), 36)
	rpcCtx := map[string]string{"role": role}
	if platformTenant != "" {
		rpcCtx["tenantId"] = platformTenant
	}
	if oc := opClassFromContext(ctx); oc != "" {
		rpcCtx["opClass"] = oc
	}
	return &pb.RPCRequest{
		Id:        id,
		Handler:   method,
		Payload:   payload,
		Context:   rpcCtx,
		TimeoutNs: int64(timeout),
		TraceId:   trace.FromContext(ctx),
	}
}

// getOrFindSession returns a cached healthy session or finds a new one.
//
// Singleflight per role: when N concurrent calls hit the same cache miss
// for a role, only ONE invokes c.finder.FindSession + builds the pool;
// the others wait on the same in-flight result. Was the
// M-FindSession-Stampede finding — a thundering herd of cache-miss
// dispatches each independently walked the directory and overwrote
// the cache, orphaning the others' StreamPool fills.
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
		cs.pool.Fill(session)
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
// lets callers map req.Context["opClass"] to the right band — L3 #10
// fix; before this, every dynamic stream rode Priority=128 regardless
// of class, so a bulk file-transfer RPC and a control-plane RPC shared
// the same WDRR weight and a large bulk payload could head-of-line
// block control traffic.
//
// If priority is 0, defaults to 128 (Standard).
//
// IDs come from the session-scoped counter in streamid.go — shared
// with StreamPool to prevent collisions between the two allocators on
// the same session (was H-Dispatch-StreamIDCollision).
func (c *HWPCaller) openDynamicStream(ctx context.Context, session aether.Session, priority uint8) (aether.Stream, error) {
	if priority == 0 {
		priority = 128
	}
	return session.OpenStream(ctx, aether.StreamConfig{
		StreamID:    nextDynamicStreamID(session),
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
// no-op (same as Standard). Audit H7 — without this, callOnce
// (the warm/cached-session path, which is the dominant code path in
// steady state) hardcoded priority=128 even when realtime / bulk
// classes were specified.
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

// getBreaker returns the per-node circuit breaker, creating one if needed.
// Keyed by nodeID so one bad node doesn't block healthy nodes serving the same role.
func (c *HWPCaller) getBreaker(nodeID string) *CircuitBreaker {
	// Fast path: read lock
	c.bmu.RLock()
	cb, ok := c.breakers[nodeID]
	c.bmu.RUnlock()
	if ok {
		return cb
	}
	// Slow path: create new breaker
	c.bmu.Lock()
	defer c.bmu.Unlock()
	// Double-check after acquiring write lock
	if cb, ok = c.breakers[nodeID]; ok {
		return cb
	}
	cb = NewCircuitBreaker(5, 60*time.Second, 30*time.Second)
	c.breakers[nodeID] = cb
	return cb
}

// Metrics returns the RPC metrics for monitoring endpoints.
func (c *HWPCaller) Metrics() *RPCMetrics {
	return c.metrics
}

// Close shuts down the caller, stops maintenance, and clears the cache.
func (c *HWPCaller) Close() {
	close(c.stopCh)
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

// rpcTimeoutForProtocol returns the RPC timeout for a given protocol.
// Inlined from the removed Grade system to avoid importing mesh/node.
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
