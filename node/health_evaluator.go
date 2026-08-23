/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"sync"
	"time"

	lad "github.com/bbmumford/ledger"
	ladcache "github.com/bbmumford/ledger/cache"
)

// ---------------------------------------------------------------------------
// ServiceHealthReport — the unified health output.
// ---------------------------------------------------------------------------

// ServiceHealthReport holds the evaluated health of a single service.
type ServiceHealthReport struct {
	ServiceName    string    `json:"serviceName"`
	MeshStatus     string    `json:"meshStatus"`     // "healthy", "degraded", "connecting", "unreachable"
	HTTPStatus     string    `json:"httpStatus"`     // "operational", "degraded", etc.
	CombinedStatus string    `json:"combinedStatus"` // final display status
	BestTransport  string    `json:"bestTransport"`  // "A", "B", "C", "D", "gossip", "F"
	Connections    int       `json:"connections"`    // direct connection count
	Detail         string    `json:"detail"`         // human-readable explanation
	EvaluatedAt    time.Time `json:"evaluatedAt"`
}

// ---------------------------------------------------------------------------
// HealthEvaluator — the unified interface.
// ---------------------------------------------------------------------------

// HealthEvaluator computes and caches service health from mesh + HTTP data.
type HealthEvaluator interface {
	ServiceHealth(name string) *ServiceHealthReport
	AllServiceHealth() []*ServiceHealthReport
	MeshStatus(serviceName string) string
	LastEvaluation() time.Time
	Start()
	Stop()
}

// ---------------------------------------------------------------------------
// Sources — abstracted inputs to the evaluator.
// ---------------------------------------------------------------------------

// HTTPStatusSource returns service name → last HTTP check status.
type HTTPStatusSource func() map[string]string

// GossipLivenessSource returns nodeID → last gossip seen time.
type GossipLivenessSource func() map[string]time.Time

// HealthEvaluatorConfig holds configuration for the evaluator.
type HealthEvaluatorConfig struct {
	CacheTTL     time.Duration
	EvalInterval time.Duration
}

// DefaultHealthEvaluatorConfig returns sensible defaults.
func DefaultHealthEvaluatorConfig() HealthEvaluatorConfig {
	return HealthEvaluatorConfig{
		CacheTTL:     30 * time.Second,
		EvalInterval: 30 * time.Second,
	}
}

// ---------------------------------------------------------------------------
// meshHealthEvaluator — proper 4-layer implementation.
//
// Layer 1: LAD membership — is the service registered in the mesh directory?
// Layer 2: Gossip liveness — has the service been seen in gossip recently (<5 min)?
// Layer 3: Reach records — has the service published network addresses?
// Layer 4: Transport connectivity — what's the best connection grade?
//
// ConnectionReporter provides Layer 4 (direct connections from this node).
// LAD cache provides Layers 1-3 (mesh-wide view via gossip propagation).
// HTTP source provides the heartbeat check results.
// ---------------------------------------------------------------------------

// snapshotSource abstracts the supply of LAD snapshots so the evaluator
// can read pre-refreshed state without ever touching the directory
// directly on the request path. The default implementation reads from
// LADSnapshotCache.Snapshot(); tests can supply a fixture.
type snapshotSource func() *LADSnapshot

type meshHealthEvaluator struct {
	mu         sync.RWMutex
	reporter   ConnectionReporter
	httpSource HTTPStatusSource
	directory  *ladcache.DirectoryCache // retained only for legacy callers that supplied it; eval reads via snapshot
	snapshot   snapshotSource           // source of LAD state — snapshot cache fronting the directory
	nodeToSvc  func() map[string]string
	cache      map[string]*ServiceHealthReport
	cacheTTL   time.Duration
	lastEval   time.Time
	evalTicker time.Duration
	stopCh     chan struct{}
	stopOnce   sync.Once // Stop must be idempotent; a second close would panic
	wg         sync.WaitGroup
}

// NewHealthEvaluator creates a unified health evaluator with full 4-layer mesh checks.
//
// Parameters:
//   - reporter: ConnectionReporter for Layer 4 (direct transport connectivity)
//   - httpSource: function returning service name → HTTP status (may be nil)
//   - nodeToSvc: function returning nodeID → service name from LAD
//   - directory: LAD DirectoryCache for Layers 1-3 (membership, gossip liveness, reach). May be nil.
//   - cfg: evaluator configuration
//
// Evaluation reads LAD state from a snapshot fronting the directory so
// the evaluator goroutine never blocks on the directory's internal mutex.
// If directory is non-nil the evaluator constructs an internal
// LADSnapshotCache and starts it; callers that already have a shared
// cache (e.g. Runtime.LADSnapshotCache()) should use
// NewHealthEvaluatorWithSnapshot instead so all components see the same
// snapshot generation.
func NewHealthEvaluator(
	reporter ConnectionReporter,
	httpSource HTTPStatusSource,
	nodeToSvc func() map[string]string,
	directory *ladcache.DirectoryCache,
	cfg HealthEvaluatorConfig,
) HealthEvaluator {
	if reporter == nil {
		reporter = NilConnectionReporter{}
	}
	if nodeToSvc == nil {
		// evaluate() calls this on its first line with no guard, and it runs
		// in the goroutine Start() spawns — so a nil resolver is not a
		// degraded evaluation, it is an unrecovered panic that takes the
		// endpoint process down. Every other input here is already defaulted;
		// this one was the exception. An empty mapping is what a caller
		// supplying nil is asking for.
		nodeToSvc = func() map[string]string { return nil }
	}
	if cfg.CacheTTL == 0 {
		cfg.CacheTTL = 30 * time.Second
	}
	if cfg.EvalInterval == 0 {
		cfg.EvalInterval = 30 * time.Second
	}
	e := &meshHealthEvaluator{
		reporter:   reporter,
		httpSource: httpSource,
		directory:  directory,
		nodeToSvc:  nodeToSvc,
		cache:      make(map[string]*ServiceHealthReport),
		cacheTTL:   cfg.CacheTTL,
		evalTicker: cfg.EvalInterval,
		stopCh:     make(chan struct{}),
	}
	if directory != nil {
		// Spin up a private snapshot cache. Endpoints that share one
		// across components (Runtime + evaluator + monitoring handlers)
		// should call NewHealthEvaluatorWithSnapshot instead.
		sc := NewLADSnapshotCache(directory, DefaultLADSnapshotCacheConfig())
		sc.Start()
		e.snapshot = sc.Snapshot
	} else {
		e.snapshot = func() *LADSnapshot { return &LADSnapshot{BuiltAt: time.Now()} }
	}
	return e
}

// NewHealthEvaluatorWithSnapshot is the preferred constructor when the
// Runtime already owns a LADSnapshotCache. Multiple consumers share one
// snapshot generation, so evaluator and the /mesh-* HTTP handlers always
// agree on the LAD view at any given instant.
func NewHealthEvaluatorWithSnapshot(
	reporter ConnectionReporter,
	httpSource HTTPStatusSource,
	nodeToSvc func() map[string]string,
	snapshot snapshotSource,
	cfg HealthEvaluatorConfig,
) HealthEvaluator {
	if reporter == nil {
		reporter = NilConnectionReporter{}
	}
	if cfg.CacheTTL == 0 {
		cfg.CacheTTL = 30 * time.Second
	}
	if cfg.EvalInterval == 0 {
		cfg.EvalInterval = 30 * time.Second
	}
	if snapshot == nil {
		snapshot = func() *LADSnapshot { return &LADSnapshot{BuiltAt: time.Now()} }
	}
	if nodeToSvc == nil {
		// See NewHealthEvaluator: unguarded in evaluate(), called from the
		// Start() goroutine, so a nil here is a process-level panic.
		nodeToSvc = func() map[string]string { return nil }
	}
	return &meshHealthEvaluator{
		reporter:   reporter,
		httpSource: httpSource,
		snapshot:   snapshot,
		nodeToSvc:  nodeToSvc,
		cache:      make(map[string]*ServiceHealthReport),
		cacheTTL:   cfg.CacheTTL,
		evalTicker: cfg.EvalInterval,
		stopCh:     make(chan struct{}),
	}
}

// Start begins the periodic evaluation loop.
func (e *meshHealthEvaluator) Start() {
	e.wg.Add(1)
	go func() {
		defer e.wg.Done()
		e.evaluate()
		ticker := time.NewTicker(e.evalTicker)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				e.evaluate()
			case <-e.stopCh:
				return
			}
		}
	}()
}

// Stop halts the evaluation loop. Idempotent.
//
// 🔴 The sync.Once is load-bearing — a bare close(e.stopCh) panics with "close
// of closed channel" on a second call. See the fuller note on
// SelfHealthMonitor.Stop.
func (e *meshHealthEvaluator) Stop() {
	e.stopOnce.Do(func() {
		close(e.stopCh)
		e.wg.Wait()
	})
}

// ServiceHealth returns the cached health for a service.
func (e *meshHealthEvaluator) ServiceHealth(name string) *ServiceHealthReport {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if h, ok := e.cache[name]; ok && time.Since(h.EvaluatedAt) < e.cacheTTL {
		return h
	}
	return nil
}

// AllServiceHealth returns cached health for all known services.
func (e *meshHealthEvaluator) AllServiceHealth() []*ServiceHealthReport {
	e.mu.RLock()
	defer e.mu.RUnlock()
	result := make([]*ServiceHealthReport, 0, len(e.cache))
	for _, v := range e.cache {
		result = append(result, v)
	}
	return result
}

// MeshStatus implements the monitoring service's MeshStatusProvider interface.
func (e *meshHealthEvaluator) MeshStatus(serviceName string) string {
	if h := e.ServiceHealth(serviceName); h != nil {
		return h.MeshStatus
	}
	return "unreachable"
}

// LastEvaluation returns when the evaluator last ran.
func (e *meshHealthEvaluator) LastEvaluation() time.Time {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.lastEval
}

// evaluate performs a full 4-layer health evaluation across all services.
// Reads LAD state from the snapshot cache so this hot loop never blocks
// on the directory's internal mutex; the snapshot is refreshed by a
// dedicated goroutine on its own cadence (default 10s) and may not be
// warm yet on cold-boot, in which case .Warm is false and all slices are
// empty — the evaluator degrades gracefully (services appear unreachable
// until the first snapshot lands, then converge as gossip propagates).
func (e *meshHealthEvaluator) evaluate() {
	now := time.Now()
	nodeToSvc := e.nodeToSvc()
	snap := e.snapshot()

	// ── Layer 1: LAD Membership ──────────────────────────────────────────
	// Which services exist in the mesh directory?
	memberServices := make(map[string]bool)             // service → registered
	serviceNodes := make(map[string][]lad.MemberRecord) // service → member records
	for _, m := range snap.Members {
		svcName := m.Attrs["serviceName"]
		if svcName == "" {
			svcName = nodeToSvc[m.NodeID]
		}
		if svcName != "" {
			memberServices[svcName] = true
			serviceNodes[svcName] = append(serviceNodes[svcName], m)
		}
	}

	// ── Layer 2: Gossip Liveness ─────────────────────────────────────────
	// Has each node been seen in gossip within the last 5 minutes?
	gossipAlive := make(map[string]bool) // service → has live gossip
	for nodeID, lastSeen := range snap.GossipLiveness {
		if time.Since(lastSeen) < 5*time.Minute {
			if svcName := nodeToSvc[nodeID]; svcName != "" {
				gossipAlive[svcName] = true
			}
		}
	}

	// ── Layer 3: Reach Records ───────────────────────────────────────────
	// Has the service published network addresses (it's online and advertising)?
	hasReach := make(map[string]bool) // service → has reach records
	for _, r := range snap.Reach {
		if svcName := nodeToSvc[r.NodeID]; svcName != "" {
			hasReach[svcName] = true
		}
	}

	// ── Layer 4: Transport Connectivity ──────────────────────────────────
	// Best connection grade to each service from ANY node in the mesh.
	// Uses both local ConnectionReporter (authoritative for our connections)
	// and LAD latency records (mesh-wide view of all connections).
	svcBestGrade := make(map[string]Grade)
	svcConnected := make(map[string]bool)
	svcConnCount := make(map[string]int)

	// Local connections (ConnectionReporter)
	for _, ci := range e.reporter.ActiveConnections() {
		svcName := nodeToSvc[ci.PeerNodeID]
		if svcName == "" {
			continue
		}
		svcConnected[svcName] = true
		svcConnCount[svcName] += ci.ConnCount
		if best, ok := svcBestGrade[svcName]; !ok || ci.Grade > best {
			svcBestGrade[svcName] = ci.Grade
		}
	}

	// Mesh-wide connections (LAD latency records) — includes Grade A/B
	// connections from other nodes that we may not have directly.
	for _, lat := range snap.Latency {
		for _, nodeID := range []string{lat.FromNode, lat.ToNode} {
			svcName := nodeToSvc[nodeID]
			if svcName == "" {
				continue
			}
			// lat.Transport is a peer-supplied label off a gossiped latency
			// record. ParseProtocol discards its ok flag and returns
			// ProtoNoiseUDP for anything it does not recognise, which
			// GradeForProtocol scores GradeA — the best grade — so an
			// unrecognised label outranks every correctly-labelled transport in
			// the max-selection below. This node publishes "unknown" itself for
			// a transport it could not name (runtime.go:4656), so the label that
			// grades best is one that carries no information at all.
			//
			// Strict parse, fail closed: an unrecognised label grades F and
			// cannot win the selection. Liveness is unaffected — svcConnected is
			// still set, because a record existing is evidence of an edge
			// regardless of how its transport is spelled.
			proto, known := ParseProtocolStrict(lat.Transport)
			latGrade := GradeF
			if known {
				latGrade = GradeForProtocol(proto)
			}
			if best, ok := svcBestGrade[svcName]; !ok || latGrade > best {
				svcBestGrade[svcName] = latGrade
			}
			svcConnected[svcName] = true
		}
	}

	// ── HTTP Status ──────────────────────────────────────────────────────
	httpStatus := make(map[string]string)
	if e.httpSource != nil {
		httpStatus = e.httpSource()
	}

	// ── Compute Combined Status ──────────────────────────────────────────
	// Collect ALL known services from all sources
	allServices := make(map[string]bool)
	for svc := range memberServices {
		allServices[svc] = true
	}
	for svc := range gossipAlive {
		allServices[svc] = true
	}
	for svc := range svcConnected {
		allServices[svc] = true
	}
	for svc := range httpStatus {
		allServices[svc] = true
	}

	newCache := make(map[string]*ServiceHealthReport)

	for svc := range allServices {
		grade, hasGrade := svcBestGrade[svc]
		connected := svcConnected[svc]
		connCount := svcConnCount[svc]
		gossipOK := gossipAlive[svc]
		reachOK := hasReach[svc]
		memberOK := memberServices[svc]
		httpSt := httpStatus[svc]
		httpOK := httpSt == "operational" || httpSt == ""

		// ── Mesh status from 4-layer evaluation ──────────────────────
		//
		// Layer 4 (direct connection) > Layer 2+3 (gossip + reach) > Layer 2 (gossip only)
		//
		//   Grade A-C + connected          → "healthy" (direct, RPC-capable)
		//   Gossip alive + reach records   → "healthy" (mesh-reachable, no direct conn)
		//   Gossip alive + no reach        → "degraded" (alive but not advertising)
		//   Reach records + no gossip      → "connecting" (addresses published, not seen yet)
		//   Member only                    → "connecting" (registered but no activity)
		//   Nothing                        → "unreachable"
		//
		// Grade C (WebSocket/TLS) is healthy because RPCs work reliably over it.
		// Cross-org nodes can ONLY use Grade C — marking them degraded is misleading.

		meshSt := "unreachable"
		gradeLabel := "F"

		if hasGrade && connected {
			// Layer 4: direct transport connection — any grade is healthy
			gradeLabel = grade.String()
			meshSt = "healthy"
		} else if gossipOK && reachOK {
			// Layers 2+3: gossip alive + reach records = healthy via mesh
			meshSt = "healthy"
			gradeLabel = "gossip"
		} else if gossipOK {
			// Layer 2 only: gossip alive but no reach = degraded
			meshSt = "degraded"
			gradeLabel = "gossip"
		} else if reachOK {
			// Layer 3 only: has addresses but no gossip = connecting
			meshSt = "connecting"
			gradeLabel = "reach"
		} else if memberOK {
			// Layer 1 only: registered but no activity = connecting
			meshSt = "connecting"
			gradeLabel = "member"
		}

		// ── Combined status: mesh + HTTP ─────────────────────────────
		var status, detail string
		switch {
		case meshSt == "healthy" && httpOK:
			status = "healthy"
			detail = "Mesh connected (" + gradeLabel + "), HTTP responsive"
		case meshSt == "healthy" && !httpOK:
			status = "degraded"
			detail = "Mesh connected (" + gradeLabel + ") but HTTP " + httpStatusLabel(httpSt)
		case meshSt == "degraded" && httpOK:
			status = "degraded"
			detail = "HTTP responsive, mesh degraded (" + gradeLabel + ")"
		case meshSt == "degraded" && !httpOK:
			status = "degraded"
			detail = "Mesh degraded (" + gradeLabel + "), HTTP " + httpStatusLabel(httpSt)
		case meshSt == "connecting":
			status = "connecting"
			detail = "Mesh " + meshSt + " (" + gradeLabel + ")"
			if !httpOK && httpSt != "" {
				detail += ", HTTP " + httpStatusLabel(httpSt)
			}
		case meshSt == "unreachable" && httpOK:
			status = "degraded"
			detail = "HTTP responsive but mesh unreachable"
		case meshSt == "unreachable" && !httpOK:
			status = "unreachable"
			detail = "Mesh unreachable, HTTP " + httpStatusLabel(httpSt)
		default:
			status = "unreachable"
			detail = "No mesh or HTTP data"
		}

		newCache[svc] = &ServiceHealthReport{
			ServiceName:    svc,
			MeshStatus:     meshSt,
			HTTPStatus:     httpSt,
			CombinedStatus: status,
			BestTransport:  gradeLabel,
			Connections:    connCount,
			Detail:         detail,
			EvaluatedAt:    now,
		}
	}

	// Swap cache
	e.mu.Lock()
	e.cache = newCache
	e.lastEval = now
	e.mu.Unlock()

	dbgNodeHealth.Printf("Evaluated %d services (L1:%d members, L2:%d gossip-alive, L3:%d reachable, L4:%d connected)",
		len(newCache), len(memberServices), len(gossipAlive), len(hasReach), len(svcConnected))
}

// httpStatusLabel returns a human-readable label for HTTP check status values.
func httpStatusLabel(s string) string {
	switch s {
	case "operational":
		return "responsive"
	case "degraded":
		return "slow"
	case "partial_outage":
		return "intermittent"
	case "major_outage":
		return "down"
	case "":
		return "not monitored"
	default:
		return s
	}
}

// Compile-time interface check.
var _ HealthEvaluator = (*meshHealthEvaluator)(nil)
