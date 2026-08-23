/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/ORBTR/aether/rpc/pb"
	"google.golang.org/protobuf/proto"
)

// ResponseCacheHighWaterMark is the safety-net size at which Put triggers an
// opportunistic TTL-sweep on the writer's path, in addition to the periodic
// cleanup goroutine. Sized well above normal operation (the fleet peaks at
// ~a few hundred in-flight RPCs across all peers) so typical workloads never
// trigger it — it exists purely to bound runaway growth between scheduled
// sweeps under burst or adversarial conditions.
const ResponseCacheHighWaterMark = 50_000

// ResponseCache stores recent RPC responses keyed by request ID for dedup.
// When parallel probes arrive for the same request, the second probe gets
// the cached response instead of re-executing the handler.
type ResponseCache struct {
	mu      sync.RWMutex
	entries map[string]*cachedResponse
	ttl     time.Duration

	// Metrics
	hits      int64 // atomic
	misses    int64 // atomic
	sweeps    int64 // atomic — periodic + opportunistic combined
	evictions int64 // atomic — total entries dropped by TTL
	highMark  int64 // atomic — times the high-water-mark triggered a sweep
}

type cachedResponse struct {
	response  *pb.RPCResponse
	createdAt time.Time
}

// NewResponseCache creates a response cache with the given TTL.
// Starts a background cleanup goroutine that runs every ttl/2.
func NewResponseCache(ttl time.Duration) *ResponseCache {
	rc := &ResponseCache{
		entries: make(map[string]*cachedResponse),
		ttl:     ttl,
	}
	go rc.cleanupLoop()
	return rc
}

// Get returns a cached response for the given request ID, or nil if not found/expired.
func (rc *ResponseCache) Get(requestID string) *pb.RPCResponse {
	rc.mu.RLock()
	entry, ok := rc.entries[requestID]
	rc.mu.RUnlock()

	if !ok || time.Since(entry.createdAt) > rc.ttl {
		atomic.AddInt64(&rc.misses, 1)
		return nil
	}
	atomic.AddInt64(&rc.hits, 1)
	// Return a shallow clone. Callers (bidi / ServeMeshStream)
	// overwrite resp.Id for correlation; handing out the shared cached pointer
	// let concurrent hits race on that write and corrupted the cached Id for
	// later dedup lookups. Payload is immutable post-cache, so sharing it is fine.
	return cloneRPCResponse(entry.response)
}

// cloneRPCResponse returns a deep copy safe for a caller to mutate (e.g.
// resp.Id) without touching the cached original. proto.Clone is used because
// RPCResponse embeds a protoimpl.MessageState (with a sync.Mutex) that must not
// be copied by value.
func cloneRPCResponse(r *pb.RPCResponse) *pb.RPCResponse {
	if r == nil {
		return nil
	}
	return proto.Clone(r).(*pb.RPCResponse)
}

// Put stores a response for the given request ID.
//
// Safety net: when the map is over ResponseCacheHighWaterMark, Put runs an
// inline TTL sweep before inserting. This never drops live (non-expired)
// entries — it just expires what the periodic cleanup would have expired
// soon anyway, so the cache can't balloon between scheduled sweeps. No
// capability is lost: a hot path will continue to insert fresh entries at
// full rate, and reads of live entries are unaffected.
func (rc *ResponseCache) Put(requestID string, resp *pb.RPCResponse) {
	if requestID == "" || resp == nil {
		return
	}
	rc.mu.Lock()
	if len(rc.entries) >= ResponseCacheHighWaterMark {
		rc.sweepLocked(time.Now())
		atomic.AddInt64(&rc.highMark, 1)
		// SweepLocked only drops TTL-expired entries. Under sustained
		// unique-id traffic faster than the TTL nothing is expired, so the map
		// would grow past the mark unbounded. When the sweep frees nothing and
		// we're still at the ceiling, shed ~10% of arbitrary entries to enforce a
		// hard cap — dedup is best-effort, so dropping a few cached responses
		// only costs a possible re-execution, never correctness.
		if len(rc.entries) >= ResponseCacheHighWaterMark {
			toDrop := ResponseCacheHighWaterMark/10 + 1
			for id := range rc.entries {
				delete(rc.entries, id)
				atomic.AddInt64(&rc.evictions, 1)
				if toDrop--; toDrop <= 0 {
					break
				}
			}
		}
	}
	// Store a clone so a caller mutating the returned response (e.g.
	// resp.Id = req.Id before marshalling) can't race a concurrent Get or
	// corrupt the cached copy.
	rc.entries[requestID] = &cachedResponse{
		response:  cloneRPCResponse(resp),
		createdAt: time.Now(),
	}
	rc.mu.Unlock()
}

// Stats returns cache hit/miss counters.
func (rc *ResponseCache) Stats() (hits, misses int64) {
	return atomic.LoadInt64(&rc.hits), atomic.LoadInt64(&rc.misses)
}

// StatsDetailed returns full counters including sweep + eviction + high-water-mark trip counts.
// Useful for dashboards / health endpoints to spot runaway-growth conditions.
func (rc *ResponseCache) StatsDetailed() (hits, misses, sweeps, evictions, highMark int64, size int) {
	rc.mu.RLock()
	size = len(rc.entries)
	rc.mu.RUnlock()
	return atomic.LoadInt64(&rc.hits),
		atomic.LoadInt64(&rc.misses),
		atomic.LoadInt64(&rc.sweeps),
		atomic.LoadInt64(&rc.evictions),
		atomic.LoadInt64(&rc.highMark),
		size
}

// cleanupLoop removes expired entries every ttl/2.
func (rc *ResponseCache) cleanupLoop() {
	ticker := time.NewTicker(rc.ttl / 2)
	defer ticker.Stop()
	for range ticker.C {
		rc.mu.Lock()
		rc.sweepLocked(time.Now())
		rc.mu.Unlock()
	}
}

// sweepLocked drops TTL-expired entries. Caller must hold rc.mu.
func (rc *ResponseCache) sweepLocked(now time.Time) {
	var evicted int64
	for id, entry := range rc.entries {
		if now.Sub(entry.createdAt) > rc.ttl {
			delete(rc.entries, id)
			evicted++
		}
	}
	atomic.AddInt64(&rc.sweeps, 1)
	if evicted > 0 {
		atomic.AddInt64(&rc.evictions, evicted)
	}
}
