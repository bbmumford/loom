/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ORBTR/aether/rpc/pb"
	"github.com/ORBTR/aether"
	aethermetrics "github.com/ORBTR/aether/metrics"
	"github.com/bbmumford/loom/internal/securityctx"
	"github.com/bbmumford/loom/node/handlers"
	"github.com/bbmumford/loom/pkg/rpc"
	"github.com/bbmumford/loom/pkg/trace"
	"github.com/bbmumford/loom/ports"
)

// RPCForwarder finds and sends RPCs to the correct handler node when
// the current node does not handle the requested method locally.
type RPCForwarder interface {
	Forward(ctx context.Context, req *pb.RPCRequest) (*pb.RPCResponse, error)
}

// LoadAdvisor provides load and grade information for routing decisions.
// Implemented by Runtime — RPCServer depends on the interface, not the concrete type.
type LoadAdvisor interface {
	LocalLoad() int32
	PeerDispatchHealth() float64
	BestGradeToHandler(handler string) Grade
}

// rpcMetrics tracks per-handler RPC statistics.
type rpcMetrics struct {
	mu       sync.RWMutex
	handlers map[string]*handlerStats
}

type handlerStats struct {
	totalCalls    int64
	totalErrors   int64
	totalForwards int64
	lastLatency   time.Duration
}

func newRPCMetrics() *rpcMetrics {
	return &rpcMetrics{handlers: make(map[string]*handlerStats)}
}

func (m *rpcMetrics) getOrCreate(handler string) *handlerStats {
	m.mu.RLock()
	s, ok := m.handlers[handler]
	m.mu.RUnlock()
	if ok {
		return s
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if s, ok := m.handlers[handler]; ok {
		return s
	}
	s = &handlerStats{}
	m.handlers[handler] = s
	return s
}

func (m *rpcMetrics) RecordLocal(handler string, success bool, latency time.Duration) {
	s := m.getOrCreate(handler)
	atomic.AddInt64(&s.totalCalls, 1)
	if !success {
		atomic.AddInt64(&s.totalErrors, 1)
	}
	s.lastLatency = latency
}

func (m *rpcMetrics) RecordForward(handler string, latency time.Duration) {
	s := m.getOrCreate(handler)
	atomic.AddInt64(&s.totalForwards, 1)
	s.lastLatency = latency
}

func (m *rpcMetrics) RecordForwardFail(handler string, latency time.Duration) {
	s := m.getOrCreate(handler)
	atomic.AddInt64(&s.totalForwards, 1)
	atomic.AddInt64(&s.totalErrors, 1)
	s.lastLatency = latency
}

// Stats returns a snapshot of per-handler metrics for monitoring.
func (m *rpcMetrics) Stats() map[string]map[string]int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make(map[string]map[string]int64, len(m.handlers))
	for name, s := range m.handlers {
		result[name] = map[string]int64{
			"calls":    atomic.LoadInt64(&s.totalCalls),
			"errors":   atomic.LoadInt64(&s.totalErrors),
			"forwards": atomic.LoadInt64(&s.totalForwards),
		}
	}
	return result
}

// dispatchLatencyRegistry holds per-handler DurationHist samples for OBS-6
// (dispatch_handler_latency_ms). Wraps a sync.Map<string,*DurationHist> with
// a double-checked load so the hot path is a single sync.Map.Load on the
// common case (handler already registered) and only takes the LoadOrStore
// slow path on first sight of a handler name.
//
// Cardinality is bounded by the handler registry — each tenant.<domain>.<Op>
// FQN gets exactly one entry, and there is no per-call or per-tenant key
// component, so the map size stays at O(handlers) regardless of traffic.
type dispatchLatencyRegistry struct {
	hists sync.Map // map[string]*aethermetrics.DurationHist
}

// Record appends a sample for the given handler FQN.
// success is currently unused (we surface a single histogram per handler);
// it's accepted to keep the call site self-documenting in case we later
// want to split success-vs-error percentiles.
func (r *dispatchLatencyRegistry) Record(handler string, d time.Duration, success bool) {
	_ = success
	h, ok := r.hists.Load(handler)
	if !ok {
		fresh := &aethermetrics.DurationHist{}
		actual, _ := r.hists.LoadOrStore(handler, fresh)
		h = actual
	}
	h.(*aethermetrics.DurationHist).Record(d)
}

// dispatchLatencySnapshot is a per-handler readout used when surfacing the
// top-N entries in MeshMetrics.
type dispatchLatencySnapshot struct {
	Handler string
	Count   int
	P50US   int64
	P99US   int64
}

// TopN returns up to n handler snapshots sorted by sample count, descending.
// "Most-recently-active" is approximated by sample count: the DurationHist
// ring keeps the last 256 samples, so a handler that has been quiet will
// have a small Count() while busy handlers saturate at 256. Stable enough
// for operator-facing top-N display without adding a per-record timestamp.
func (r *dispatchLatencyRegistry) TopN(n int) []dispatchLatencySnapshot {
	if n <= 0 {
		return nil
	}
	var all []dispatchLatencySnapshot
	r.hists.Range(func(k, v interface{}) bool {
		name, _ := k.(string)
		hist, _ := v.(*aethermetrics.DurationHist)
		if hist == nil {
			return true
		}
		c := hist.Count()
		if c == 0 {
			return true
		}
		p50, _, p99 := hist.PercentileSnapshot()
		all = append(all, dispatchLatencySnapshot{
			Handler: name,
			Count:   c,
			P50US:   p50,
			P99US:   p99,
		})
		return true
	})
	sort.Slice(all, func(i, j int) bool {
		return all[i].Count > all[j].Count
	})
	if len(all) > n {
		all = all[:n]
	}
	return all
}

// bidiLatencyRegistry holds per-(transport, scope) DurationHist samples for
// OBS-7 bidirpc_call_latency_ms — the full client-side BidiRPC.Call
// round-trip (marshal → send → wait → correlate). Pairs with OBS-6
// (dispatch_handler_latency_ms): OBS-6 measures the responder's pure
// handler execution; OBS-7 measures the wire + queue time on top, so the
// gap between the two surfaces network or scheduler stalls vs handler
// work.
//
// Tag key is "<transport>|<scope>" — e.g. "noise-udp|same-origin",
// "ws|cross-org". Cardinality is bounded by the product of the two
// label dimensions (~3 transports × 2 scopes = 6 buckets max), so the
// map never grows unbounded regardless of peer count.
type bidiLatencyRegistry struct {
	hists sync.Map // map[string]*aethermetrics.DurationHist
}

// Record appends a sample for the given (transport, scope) pair. Empty
// strings on either label are tolerated — they fall into a single
// "unknown" bucket so test bidis or ad-hoc constructions without peer
// context don't poison the labelled histograms. Negative or zero
// durations are dropped by DurationHist itself.
func (r *bidiLatencyRegistry) Record(transport, scope string, d time.Duration) {
	if transport == "" {
		transport = "unknown"
	}
	if scope == "" {
		scope = "unknown"
	}
	key := transport + "|" + scope
	h, ok := r.hists.Load(key)
	if !ok {
		fresh := &aethermetrics.DurationHist{}
		actual, _ := r.hists.LoadOrStore(key, fresh)
		h = actual
	}
	h.(*aethermetrics.DurationHist).Record(d)
}

// bidiLatencySnapshot is a per-(transport, scope) readout used when
// surfacing OBS-7 in MeshMetrics.
type bidiLatencySnapshot struct {
	Transport string
	Scope     string
	Count     int
	P50US     int64
	P99US     int64
}

// Snapshots returns one entry per active (transport, scope) bucket. Order
// is unspecified — callers that care about display order should sort by
// transport/scope themselves. Returns nil if no samples have been
// recorded yet. Excludes buckets with zero samples so a freshly-started
// node doesn't surface placeholder zeros.
func (r *bidiLatencyRegistry) Snapshots() []bidiLatencySnapshot {
	var all []bidiLatencySnapshot
	r.hists.Range(func(k, v interface{}) bool {
		key, _ := k.(string)
		hist, _ := v.(*aethermetrics.DurationHist)
		if hist == nil {
			return true
		}
		c := hist.Count()
		if c == 0 {
			return true
		}
		// Split "<transport>|<scope>" — invalid keys go to (unknown, unknown).
		transport, scope := "unknown", "unknown"
		for i := 0; i < len(key); i++ {
			if key[i] == '|' {
				transport = key[:i]
				scope = key[i+1:]
				break
			}
		}
		p50, _, p99 := hist.PercentileSnapshot()
		all = append(all, bidiLatencySnapshot{
			Transport: transport,
			Scope:     scope,
			Count:     c,
			P50US:     p50,
			P99US:     p99,
		})
		return true
	})
	return all
}

// Context keys for RPC request enrichment
type rpcContextKey string

const (
	rpcNodeIDKey  rpcContextKey = "rpc.nodeID"
	rpcRegionKey  rpcContextKey = "rpc.region"
	rpcServiceKey rpcContextKey = "rpc.service"
	rpcHopsKey    rpcContextKey = "rpc.hops"
)

// RPCServer handles incoming RPC connections using binary TLV wire format.
// Dispatches through HandlerRegistry with intelligent load-aware forwarding.
type RPCServer struct {
	registry       *handlers.HandlerRegistry
	forwarder      RPCForwarder
	logger         *log.Logger
	startTime      time.Time
	metrics        *rpcMetrics
	loadAdvisor    LoadAdvisor
	localID        string
	region         string
	serviceName    string
	activeHandlers int32 // atomic — concurrent handler goroutines
	responseCache  *ResponseCache // dedup cache for parallel probes

	// authValidator stamps wire-envelope tenant IDs onto the platform-
	// canonical context key (convention (b) in handleRequest). Injected
	// from Config.AuthValidator; defaults to the loom-local securityctx
	// implementation, which HSTLES builds must override so domain
	// handlers' helpers.ExtractTenantID sees the value.
	authValidator ports.AuthValidator

	// dispatchLatency records per-handler registry.Dispatch wall-clock
	// duration (OBS-6). Updated in executeLocal so it covers both the
	// non-bidi path (handleRequest → executeLocal) AND the bidi path
	// (handleIncomingRequest → server.handleRequest → executeLocal).
	dispatchLatency dispatchLatencyRegistry

	// bidiLatency records the full BidiRPC.Call client-side round-trip
	// (OBS-7), tagged by (transport, scope). Lives on RPCServer (not on
	// individual BidiRPC instances) so all of a node's bidi sessions
	// aggregate into a single histogram per tag — operator dashboards
	// see one p50/p99 per (transport, scope), not per peer. Updated
	// from BidiRPC.Call's defer.
	bidiLatency bidiLatencyRegistry

	// bidiPhase{Marshal,Send,Wait} decompose the OBS-7 aggregate into
	// the three contiguous wall-clock phases inside BidiRPC.Call:
	//   marshal = pb.MarshalRequest + Type-prefix concat
	//   send    = stream.Send (wire write + scheduler enqueue + park)
	//   wait    = pending-channel select waiting on the response demux
	// Sum approximation: bidiLatency ≈ marshal + send + wait (modulo
	// the inflight gauge / pending-map mutex overhead, all sub-µs).
	// Decomposition lets operators attribute a 5ms p50 same-origin
	// bidirpc round-trip to one of: codec cost, transport stall, or
	// peer-side dispatch + reply delay. Tagged by the same (transport,
	// scope) labels as bidiLatency.
	bidiPhaseMarshal bidiLatencyRegistry
	bidiPhaseSend    bidiLatencyRegistry
	bidiPhaseWait    bidiLatencyRegistry

	// bidiTimeout records DeadlineExceeded exits separately from the
	// success-path bidiLatency histogram. Without this split, every
	// caller-side timeout (default callerRequestTTL = 30s) injects a
	// 30,000ms sample into the bidiLatency histogram, pinning p99 at
	// the exact deadline value and making the histogram unreadable for
	// real successful-call latency. Per-tag tag = (transport, scope).
	// See workflow wd4zasivv synthesis.
	bidiTimeout bidiLatencyRegistry
}

// NewRPCServer creates a new RPC server backed by a HandlerRegistry.
func NewRPCServer(registry *handlers.HandlerRegistry) *RPCServer {
	return &RPCServer{
		registry:      registry,
		logger:        log.Default(),
		startTime:     time.Now(),
		metrics:       newRPCMetrics(),
		responseCache: NewResponseCache(10 * time.Second),
		authValidator: securityctx.Default(),
	}
}

// SetAuthValidator injects the platform auth validator (Config.AuthValidator).
// Must be called before serving; nil is ignored (keeps the fail-closed
// loom-local default).
func (s *RPCServer) SetAuthValidator(v ports.AuthValidator) {
	if v != nil {
		s.authValidator = v
	}
}

// SetForwarder configures mesh-routed RPC forwarding.
func (s *RPCServer) SetForwarder(f RPCForwarder) {
	s.forwarder = f
}

// SetLoadAdvisor configures load-aware routing.
func (s *RPCServer) SetLoadAdvisor(la LoadAdvisor) {
	s.loadAdvisor = la
}

// SetNodeInfo configures context enrichment fields.
func (s *RPCServer) SetNodeInfo(nodeID, region, serviceName string) {
	s.localID = nodeID
	s.region = region
	s.serviceName = serviceName
}

// Metrics returns the per-handler RPC metrics for monitoring APIs.
func (s *RPCServer) Metrics() *rpcMetrics {
	return s.metrics
}

// ActiveHandlers returns the number of concurrent handler goroutines.
func (s *RPCServer) ActiveHandlers() int32 {
	return atomic.LoadInt32(&s.activeHandlers)
}

// DedupStats returns response cache hit/miss counters for observability.
func (s *RPCServer) DedupStats() (hits, misses int64) {
	if s.responseCache != nil {
		return s.responseCache.Stats()
	}
	return 0, 0
}

// DispatchLatencyTopN returns up to n per-handler dispatch latency snapshots,
// sorted by sample count descending. Powers the OBS-6
// dispatch_handler_latency_ms_p50/p99_<handler> keys in Runtime.MeshMetrics
// without surfacing a key per handler in a registry that may hold 100+
// entries. Returns nil if no samples have been recorded yet.
func (s *RPCServer) DispatchLatencyTopN(n int) []dispatchLatencySnapshot {
	return s.dispatchLatency.TopN(n)
}

// RecordBidiLatency appends a sample to the OBS-7 bidi-RPC latency
// histogram. Called from BidiRPC.Call's defer with the full round-trip
// duration. Exposed (vs an internal method) because BidiRPC is in the
// same package but the call sequence is one tight defer at the top of
// Call — keeping the registry access behind a named method makes the
// intent explicit at the instrumentation site.
func (s *RPCServer) RecordBidiLatency(transport, scope string, d time.Duration) {
	s.bidiLatency.Record(transport, scope, d)
}

// RecordBidiPhaseMarshal records the time inside BidiRPC.Call spent on
// pb.MarshalRequest + type-prefix concat. Decomposes the OBS-7 aggregate
// so the operator can distinguish "codec cost" from "wire/wait cost".
func (s *RPCServer) RecordBidiPhaseMarshal(transport, scope string, d time.Duration) {
	s.bidiPhaseMarshal.Record(transport, scope, d)
}

// RecordBidiPhaseSend records the time inside BidiRPC.Call spent on
// stream.Send (wire write + scheduler enqueue + park). Elevated p50
// here vs marshal/wait fingerprints transport-level backpressure.
func (s *RPCServer) RecordBidiPhaseSend(transport, scope string, d time.Duration) {
	s.bidiPhaseSend.Record(transport, scope, d)
}

// RecordBidiPhaseWait records the time inside BidiRPC.Call spent
// blocked on the pending-channel demux waiting for the correlated
// response. Elevated p50 here vs marshal/send fingerprints peer-side
// dispatch + reply delay (or a wedged stream).
func (s *RPCServer) RecordBidiPhaseWait(transport, scope string, d time.Duration) {
	s.bidiPhaseWait.Record(transport, scope, d)
}

// RecordBidiTimeout records a DeadlineExceeded exit from BidiRPC.Call.
// Split from RecordBidiLatency so the success-path histogram is not
// pinned at the 30s callerRequestTTL by timed-out calls. Surfaced as
// bidirpc_timeout_<p50|p99|count>_<transport>_<scope>.
func (s *RPCServer) RecordBidiTimeout(transport, scope string, d time.Duration) {
	s.bidiTimeout.Record(transport, scope, d)
}

// BidiTimeoutSnapshots returns one entry per (transport, scope) bucket
// for the timeout histogram. Used by Runtime.MeshMetrics to surface the
// timeout series alongside bidiLatency.
func (s *RPCServer) BidiTimeoutSnapshots() []bidiLatencySnapshot {
	return s.bidiTimeout.Snapshots()
}

// BidiPhaseSnapshots returns one entry per phase per (transport, scope)
// bucket. Used by Runtime.MeshMetrics to surface the OBS-7 phase
// decomposition keys. The returned slice's "phase" field is one of
// "marshal", "send", "wait" — surface key is built as
// bidirpc_phase_<phase>_<p50|p99|count>_<transport>_<scope>.
func (s *RPCServer) BidiPhaseSnapshots() (marshal, send, wait []bidiLatencySnapshot) {
	return s.bidiPhaseMarshal.Snapshots(), s.bidiPhaseSend.Snapshots(), s.bidiPhaseWait.Snapshots()
}

// BidiLatencySnapshots returns one entry per (transport, scope) bucket
// that has recorded samples. Powers the OBS-7
// bidirpc_latency_ms_p50/p99_<transport>_<scope> keys in
// Runtime.MeshMetrics. Cardinality is naturally bounded (~6 buckets max:
// 3 transports × 2 scopes) so no top-N truncation is needed. Returns
// nil if no samples have been recorded yet.
func (s *RPCServer) BidiLatencySnapshots() []bidiLatencySnapshot {
	return s.bidiLatency.Snapshots()
}

// RegisterRPC registers a handler via the HandlerRegistry.
func (s *RPCServer) RegisterRPC(h handlers.RPCHandler) error {
	return s.registry.RegisterRPC(h)
}

// shouldForwardWithContext makes the grade-aware load-based forwarding decision.
func (s *RPCServer) shouldForwardWithContext(req *pb.RPCRequest) bool {
	if s.loadAdvisor == nil {
		return false
	}
	active := atomic.LoadInt32(&s.activeHandlers)
	peerHealth := s.loadAdvisor.PeerDispatchHealth()

	if req.Hops >= pb.MaxRPCHops-1 {
		return false // prevent forwarding loops
	}
	if peerHealth < 0.3 {
		return false // peers mostly dead
	}
	if active > 50 {
		return true // local overloaded
	}
	bestGrade := s.loadAdvisor.BestGradeToHandler(req.Handler)
	if bestGrade >= GradeA && active > 20 {
		return true // Grade A path + moderate load
	}
	if bestGrade >= GradeB && active > 35 {
		return true // Grade B path + higher load
	}
	return false
}

// ServeSession handles an incoming VL1 session using binary TLV wire format.
// Wire format: pb.MarshalRequest() → session.Send() (matches rpc/client/client.go).
func (s *RPCServer) ServeSession(ctx context.Context, session aether.Connection) error {
	s.logger.Printf("[RPC] Serving session from %s", session.RemoteNodeID().Short())

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		req, err := s.readMessage(ctx, session)
		if err != nil {
			if err == io.EOF {
				s.logger.Printf("[RPC] Session closed by remote node %s", session.RemoteNodeID().Short())
				return nil
			}
			return fmt.Errorf("read message: %w", err)
		}

		resp := s.handleRequest(ctx, req)
		if err := s.writeMessage(ctx, session, resp); err != nil {
			return fmt.Errorf("write response: %w", err)
		}
	}
}

// handleRequest processes a binary RPC request with intelligent dispatch.
// Dispatch chain: timeout enforcement → context enrichment → load-aware local/forward decision → error.
func (s *RPCServer) handleRequest(ctx context.Context, req *pb.RPCRequest) *pb.RPCResponse {
	startTime := time.Now()

	// 0a. Dedup check — return cached response for parallel probes
	if req.Id != "" && s.responseCache != nil {
		if cached := s.responseCache.Get(req.Id); cached != nil {
			return cached
		}
	}

	// 0b. Absolute deadline check — reject if already expired
	if req.Deadline > 0 && time.Now().UnixNano() > req.Deadline {
		return &pb.RPCResponse{
			Id:      req.Id,
			Success: false,
			Error:   "request deadline exceeded at hop",
			LatencyNs: int64(time.Since(startTime)),
		}
	}

	// 1. Timeout enforcement — use propagated deadline from caller.
	// req.TimeoutNs is nanoseconds (int64), set by AetherCaller's
	// buildRPCRequest from the caller's context deadline.
	if req.TimeoutNs > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Duration(req.TimeoutNs))
		defer cancel()
	}

	// 2. Context enrichment — add local node info for handler use
	if s.localID != "" {
		ctx = context.WithValue(ctx, rpcNodeIDKey, s.localID)
		ctx = context.WithValue(ctx, rpcRegionKey, s.region)
		ctx = context.WithValue(ctx, rpcServiceKey, s.serviceName)
		ctx = context.WithValue(ctx, rpcHopsKey, int(req.Hops))
	}

	// 2b. Lift wire-envelope identity values onto platform-canonical
	// context keys. HSTLES has two coexisting conventions:
	//   (a) req.Context map — used by validateTenantScope &
	//       handlers.GetTenantIDFromContext.
	//   (b) the platform's typed tenant ctx key — used by domain
	//       handlers like identity.EnsureIdentity.
	// The wire side populates (a) via HWPCaller.platformTenant; this
	// lift makes (b) see the same value so a nested rpc.Call from a
	// handler (e.g., CompleteOAuth → EnsureIdentity) carries tenant
	// through. The key written is whatever Config.AuthValidator's
	// WithTenantID writes — HSTLES injects a validator delegating to
	// security/helpers so its unexported key is the one stamped.
	// Long-term: collapse the two surfaces into one.
	if tid := req.Context["tenantId"]; tid != "" {
		ctx = s.authValidator.WithTenantID(ctx, tid)
	}
	// Propagate the e2e trace ID from the wire envelope to ctx so this
	// node's handler logs, nested rpc.Call sites, and downstream-only
	// log helpers (trace.Tag) see the same ID the gateway started with.
	if req.TraceId != "" {
		ctx = trace.WithID(ctx, req.TraceId)
	}

	// #K-32: lift the caller's wire-propagated userId + scope-list onto ctx
	// via the injected validator (optional ports.ScopeStamper) so scope
	// enforcement (a handler's RequiredScopes) and userId-scoped handlers see
	// the authenticated principal that crossed the mesh hop.
	// buildRPCRequestCtx selectively serialized these into req.Context; a
	// validator that does not implement ScopeStamper simply ignores them —
	// the safe, non-breaking default (mesh scope enforcement stays closed
	// until the endpoint validator adopts it). Mirrors the tenantId lift
	// above: identity flows through the injected validator, never a
	// loom-local key an HSTLES build would not read.
	if ss, ok := s.authValidator.(ports.ScopeStamper); ok {
		scopes := rpc.ParseScopes(req.Context["scopes"])
		if uid := req.Context["userId"]; uid != "" || len(scopes) > 0 {
			ctx = ss.WithWireIdentity(ctx, uid, scopes)
		}
	}

	// 3. Intelligent dispatch: local vs forward decision
	var hasLocal bool
	if s.registry != nil {
		_, hasLocal = s.registry.GetMeta(req.Handler)
	}
	canForward := s.forwarder != nil && req.Hops < pb.MaxRPCHops

	// If both paths available, check if we should forward instead of local
	//
	// Audit L14: the forwarded response is NOT cached in s.responseCache.
	// Intentional — when this node forwards, it's acting as a proxy hop;
	// the canonical responder is the node that actually executes the
	// handler. That node's own responseCache stores the successful
	// result. Caching the forwarded copy here would (a) double-cache
	// for the network and (b) tie the proxy's cache TTL to upstream
	// state it doesn't own.
	if hasLocal && canForward && s.shouldForwardWithContext(req) {
		req.Hops++
		resp, err := s.forwarder.Forward(ctx, req)
		if err == nil {
			s.metrics.RecordForward(req.Handler, time.Since(startTime))
			return resp
		}
		// Forwarding failed — fall back to local execution
		req.Hops-- // restore for local execution
	}

	// Execute locally if handler registered
	if hasLocal {
		resp := s.executeLocal(ctx, req, startTime)
		s.metrics.RecordLocal(req.Handler, resp.Success, time.Since(startTime))
		// Cascade D fix (L3 #5+#13): cache only successful responses.
		// Caching failures (e.g., "handler not found" returned because
		// of a hot-reload race) poisons subsequent parallel-probe
		// lookups for 10 s — even when a different valid target
		// exists in the mesh. Treating failures as cacheable
		// effectively rate-limits recovery to the cache TTL.
		if req.Id != "" && s.responseCache != nil && resp != nil && resp.Success {
			s.responseCache.Put(req.Id, resp)
		}
		return resp
	}

	// If this node was the intended target but doesn't have the handler,
	// fail immediately instead of re-forwarding (prevents routing loops).
	if req.TargetNodeId != "" && s.localID != "" && req.TargetNodeId == s.localID {
		return &pb.RPCResponse{
			Id:      req.Id,
			Success: false,
			Error:   fmt.Sprintf("handler %s not found on target node", req.Handler),
			LatencyNs: int64(time.Since(startTime)),
		}
	}

	// Forward to a peer that handles it
	if canForward {
		req.Hops++
		resp, err := s.forwarder.Forward(ctx, req)
		if err != nil {
			s.metrics.RecordForwardFail(req.Handler, time.Since(startTime))
			return &pb.RPCResponse{
				Id:      req.Id,
				Success: false,
				Error:   fmt.Sprintf("forward %s failed (hop %d): %v", req.Handler, req.Hops, err),
				LatencyNs: int64(time.Since(startTime)),
			}
		}
		s.metrics.RecordForward(req.Handler, time.Since(startTime))
		return resp
	}

	// No handler found anywhere
	return &pb.RPCResponse{
		Id:      req.Id,
		Success: false,
		Error:   fmt.Sprintf("unknown handler: %s (hops: %d)", req.Handler, req.Hops),
		LatencyNs: int64(time.Since(startTime)),
	}
}

// executeLocal dispatches an RPC to the local HandlerRegistry with active handler tracking.
func (s *RPCServer) executeLocal(ctx context.Context, req *pb.RPCRequest, startTime time.Time) *pb.RPCResponse {
	atomic.AddInt32(&s.activeHandlers, 1)
	defer atomic.AddInt32(&s.activeHandlers, -1)

	handlerReq := &handlers.RPCRequest{
		ID:        req.Id,
		Handler:   req.Handler,
		Payload:   req.Payload,
		Timeout:   time.Duration(req.TimeoutNs),
		SessionID: req.SessionId,
		TraceID:   req.TraceId,
	}
	if req.Context != nil {
		handlerReq.Context = make(map[string]interface{})
		for k, v := range req.Context {
			handlerReq.Context[k] = v
		}
	}

	// OBS-6 dispatch_handler_latency_ms: time the pure handler logic
	// (validateTenantScope + middleware Before + ExecuteRPC + middleware
	// After), excluding network decode/encode which happens in the
	// caller. This is the canonical instrumentation site for inbound
	// dispatch — both the non-bidi path (handleRequest at rpc.go:243)
	// and the bidi path (handleIncomingRequest at bidi_rpc.go:299, which
	// forwards through server.handleRequest) terminate here.
	t0 := time.Now()
	resp, err := s.registry.Dispatch(ctx, handlerReq)
	s.dispatchLatency.Record(req.Handler, time.Since(t0), err == nil && resp != nil && resp.Success)
	if err != nil {
		return &pb.RPCResponse{
			Id:      req.Id,
			Success: false,
			Error:   err.Error(),
			LatencyNs: int64(time.Since(startTime)),
		}
	}
	// MESH-D02: Dispatch can return (nil, nil) — e.g. a custom RPCHandler or an
	// After middleware that returns no response and no error. The raw
	// ServeSession path (a bare goroutine with no recover) would then panic on
	// the resp.* dereferences below and crash the process. Fail closed instead.
	if resp == nil {
		return &pb.RPCResponse{
			Id:        req.Id,
			Success:   false,
			Error:     "nil dispatch response",
			LatencyNs: int64(time.Since(startTime)),
		}
	}
	return &pb.RPCResponse{
		Id:      req.Id,
		Success: resp.Success,
		Payload: resp.Payload,
		Error:   resp.Error,
		LatencyNs: int64(resp.Latency),
	}
}

// readMessage reads a binary TLV message from the session.
func (s *RPCServer) readMessage(ctx context.Context, session aether.Connection) (*pb.RPCRequest, error) {
	body, err := session.Receive(ctx)
	if err != nil {
		return nil, err
	}

	req, err := pb.UnmarshalRequest(body)
	if err != nil {
		return nil, fmt.Errorf("unmarshal binary request: %w", err)
	}
	return req, nil
}

// writeMessage writes a binary TLV message to the session.
func (s *RPCServer) writeMessage(ctx context.Context, session aether.Connection, resp *pb.RPCResponse) error {
	body, err := pb.MarshalResponse(resp)
	if err != nil {
		return fmt.Errorf("marshal binary response: %w", err)
	}
	return session.Send(ctx, body)
}

// HandleIncomingSessions processes incoming VL1 sessions with RPC protocol.
// Backward-compat: uses "default" as the transport tenant for all sessions.
func (s *RPCServer) HandleIncomingSessions(ctx context.Context, listener aether.Listener) {
	s.HandleIncomingSessionsForTenant(ctx, listener, "default")
}

// HandleIncomingSessionsForTenant processes incoming sessions, resolving the
// transport-level tenant ID for each. The fallbackTenantID is used for sessions
// that don't carry an embedded tenant (e.g., dedicated single-tenant transports).
// For shared preamble transports, the tenant ID is extracted from the session.
func (s *RPCServer) HandleIncomingSessionsForTenant(ctx context.Context, listener aether.Listener, fallbackTenantID string) {
	s.logger.Printf("[RPC] Listening for incoming mesh connections (tenant fallback: %s)...", fallbackTenantID)

	for {
		select {
		case <-ctx.Done():
			s.logger.Println("[RPC] Incoming connection handler stopped")
			return
		default:
			session, err := listener.Accept(ctx)
			if err != nil {
				if err != context.Canceled && err != io.EOF {
					s.logger.Printf("[RPC] Accept error: %v", err)
				}
				return
			}

			tenantID := fallbackTenantID
			if tas, ok := session.(aether.TenantAwareSession); ok {
				if tid := tas.ScopeID(); tid != "" {
					tenantID = tid
				}
			}

			sessionCtx := handlers.WithTransportTenant(ctx, tenantID)

			// Spawn via safeGo so a panic anywhere inside ServeSession
			// (e.g. a handler reached via the non-bidi path, before
			// Stream 1 wires up) doesn't take down the entire node
			// process. The bidi recovery only protects the BidiRPC
			// path; this is the outer defence (M-ServeSession-NoRecover).
			func(sess aether.Connection, sCtx context.Context) {
				safeGo("rpc.ServeSession", func() {
					defer sess.Close()

					s.logger.Printf("[RPC] Accepted connection from %s (tenant: %s)",
						sess.RemoteNodeID().Short(), tenantID)

					if err := s.ServeSession(sCtx, sess); err != nil {
						if err != context.Canceled && err != io.EOF {
							s.logger.Printf("[RPC] Session error from %s: %v", sess.RemoteNodeID().Short(), err)
						}
					}

					s.logger.Printf("[RPC] Session closed from %s", sess.RemoteNodeID().Short())
				})
			}(session, sessionCtx)
		}
	}
}

// ---------------------------------------------------------------------------
// Built-in RPC Handlers (registered via HandlerRegistry)
// ---------------------------------------------------------------------------

// PingRPCHandler implements handlers.RPCHandler for ping requests.
// Echoes the payload back with a JSON response containing a timestamp.
type PingRPCHandler struct{}

func (h *PingRPCHandler) Name() string                         { return "ping" }
func (h *PingRPCHandler) Role() string                         { return "system" }
func (h *PingRPCHandler) RequiresAuth() bool                   { return false }
func (h *PingRPCHandler) AllowedAuthTypes() []string           { return nil }
func (h *PingRPCHandler) Scopes() []string                     { return nil }
func (h *PingRPCHandler) TenantScope() handlers.TenantScope    { return "" }
func (h *PingRPCHandler) AllowedTenants() []string             { return nil }

func (h *PingRPCHandler) ExecuteRPC(ctx context.Context, req *handlers.RPCRequest) (*handlers.RPCResponse, error) {
	type PingRequest struct {
		Message string `json:"message"`
	}
	type PingResponse struct {
		Message   string    `json:"message"`
		Timestamp time.Time `json:"timestamp"`
	}

	var pingReq PingRequest
	if len(req.Payload) > 0 {
		_ = json.Unmarshal(req.Payload, &pingReq)
	}

	resp := PingResponse{
		Message:   fmt.Sprintf("pong: %s", pingReq.Message),
		Timestamp: time.Now(),
	}

	payload, _ := json.Marshal(resp)
	return &handlers.RPCResponse{
		Success: true,
		Payload: payload,
	}, nil
}

// StatusRPCHandler returns node status information.
type StatusRPCHandler struct {
	identity  *NodeIdentity
	startTime time.Time
}

func (h *StatusRPCHandler) Name() string                         { return "status" }
func (h *StatusRPCHandler) Role() string                         { return "system" }
func (h *StatusRPCHandler) RequiresAuth() bool                   { return false }
func (h *StatusRPCHandler) AllowedAuthTypes() []string           { return nil }
func (h *StatusRPCHandler) Scopes() []string                     { return nil }
func (h *StatusRPCHandler) TenantScope() handlers.TenantScope    { return "" }
func (h *StatusRPCHandler) AllowedTenants() []string             { return nil }

func (h *StatusRPCHandler) ExecuteRPC(ctx context.Context, req *handlers.RPCRequest) (*handlers.RPCResponse, error) {
	type StatusResponse struct {
		NodeID    string    `json:"nodeId"`
		Uptime    string    `json:"uptime"`
		Timestamp time.Time `json:"timestamp"`
		Status    string    `json:"status"`
	}

	resp := StatusResponse{
		NodeID:    h.identity.String(),
		Uptime:    time.Since(h.startTime).String(),
		Timestamp: time.Now(),
		Status:    "healthy",
	}

	payload, _ := json.Marshal(resp)
	return &handlers.RPCResponse{
		Success: true,
		Payload: payload,
	}, nil
}
