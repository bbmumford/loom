/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package gateway

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	task "github.com/bbmumford/loom/core/task"
	bidding "github.com/bbmumford/loom/core/task/bidding"
	queue "github.com/bbmumford/loom/core/task/queue"
	"github.com/bbmumford/loom/ports"
)

// Clock abstracts time for testing.
type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// OwnerLimiter implements fairness over immutable authenticated owners.
type OwnerLimiter interface {
	Allow(now time.Time, owner ports.ExecutionOwnerKey) bool
}

type noopLimiter struct{}

func (noopLimiter) Allow(time.Time, ports.ExecutionOwnerKey) bool { return true }

// GatewayConfig configures offer/bid/assignment behaviour.
type GatewayConfig struct {
	ID                     string
	QueuePolicy            queue.QueuePolicy
	BidPolicy              bidding.BidPolicy
	LeaseDuration          time.Duration
	OfferTTL               time.Duration
	MaxInFlightOffers      int
	MaxQueueDepth          int
	MaxOutstandingPerOwner int
	MaxAttempts            int
	RequeueDelay           time.Duration
	LeaseCheckInterval     time.Duration
}

// Gateway coordinates task scheduling for a mesh role.
type Gateway struct {
	id      string
	queue   *queue.TaskQueue
	config  GatewayConfig
	clock   Clock
	limiter OwnerLimiter
	owner   ports.ExecutionPrincipalReader

	offers      chan task.TaskOffer
	assignments chan task.Assignment
	bids        chan task.Bid
	progress    chan task.Progress
	results     chan task.ResultReport
	signalCh    chan struct{}

	mu       sync.Mutex
	open     map[string]*offerState
	inFlight map[string]*assignmentState
	lastWin  map[string]time.Time
	// Queue, offer and assignment state use a gateway-owned deep snapshot.
	// submitted pointers are retained only while admitted so duplicate
	// external enqueue is rejected and StatusOf can synchronize reads.
	admissionsBySubmitted map[*task.Task]*taskAdmission
	admissionsByTask      map[*task.Task]*taskAdmission
	admissionsByID        map[string]*taskAdmission
	ownerOutstanding      map[ports.ExecutionOwnerKey]int
	closing               chan struct{}
	stopOnce              sync.Once // MESH-H08: makes Stop idempotent (close(g.closing) once)
	stopped               bool
}

type offerState struct {
	task      *task.Task
	offer     task.TaskOffer
	createdAt time.Time
	deadline  time.Time
}

type assignmentState struct {
	task       *task.Task
	assignment task.Assignment
	deadline   time.Time
	attempt    int
}

type taskAdmission struct {
	submitted      *task.Task
	current        *task.Task
	principal      ports.ExecutionPrincipal
	owner          ports.ExecutionOwnerKey
	pinnedID       string
	pinnedTenantID string
}

// GatewayOption allows optional configuration injection.
type GatewayOption func(*Gateway)

// WithLimiter injects a custom authenticated-owner limiter.
func WithLimiter(l OwnerLimiter) GatewayOption {
	return func(g *Gateway) { g.limiter = l }
}

// WithClock injects a custom clock implementation.
func WithClock(clock Clock) GatewayOption {
	return func(g *Gateway) { g.clock = clock }
}

// WithExecutionPrincipalReader injects the product-owned authority seam used
// by EnqueueAuthorized. The reader must obtain an immutable principal from a
// server-private context key established by authenticated transport/session
// or an explicit service root; no Task field is passed to it.
func WithExecutionPrincipalReader(reader ports.ExecutionPrincipalReader) GatewayOption {
	return func(g *Gateway) { g.owner = reader }
}

// NewGateway constructs a gateway with sane defaults.
func NewGateway(cfg GatewayConfig, opts ...GatewayOption) (*Gateway, error) {
	if cfg.QueuePolicy.Weights.Priority == 0 && cfg.QueuePolicy.Weights.Deadline == 0 && cfg.QueuePolicy.Weights.Age == 0 {
		cfg.QueuePolicy = queue.DefaultQueuePolicy()
	}
	if cfg.BidPolicy.Weights.Capacity == 0 && cfg.BidPolicy.Weights.ETA == 0 {
		cfg.BidPolicy = bidding.DefaultBidPolicy()
	}
	if cfg.LeaseDuration <= 0 {
		cfg.LeaseDuration = 30 * time.Second
	}
	if cfg.OfferTTL <= 0 {
		cfg.OfferTTL = 5 * time.Second
	}
	if cfg.MaxInFlightOffers <= 0 {
		cfg.MaxInFlightOffers = 32
	}
	if cfg.MaxQueueDepth <= 0 {
		cfg.MaxQueueDepth = queue.DefaultMaxQueueDepth
	}
	if cfg.MaxOutstandingPerOwner <= 0 {
		cfg.MaxOutstandingPerOwner = 64
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 3
	}
	if cfg.RequeueDelay <= 0 {
		cfg.RequeueDelay = 500 * time.Millisecond
	}
	if cfg.LeaseCheckInterval <= 0 {
		cfg.LeaseCheckInterval = 1 * time.Second
	}
	if cfg.ID == "" {
		cfg.ID = uuid.NewString()
	}

	g := &Gateway{
		id:                    cfg.ID,
		queue:                 queue.NewTaskQueue(cfg.QueuePolicy),
		config:                cfg,
		clock:                 realClock{},
		limiter:               noopLimiter{},
		offers:                make(chan task.TaskOffer, cfg.MaxInFlightOffers),
		assignments:           make(chan task.Assignment, cfg.MaxInFlightOffers),
		bids:                  make(chan task.Bid, 128),
		progress:              make(chan task.Progress, 128),
		results:               make(chan task.ResultReport, 128),
		signalCh:              make(chan struct{}, 1),
		open:                  make(map[string]*offerState),
		inFlight:              make(map[string]*assignmentState),
		lastWin:               make(map[string]time.Time),
		admissionsBySubmitted: make(map[*task.Task]*taskAdmission),
		admissionsByTask:      make(map[*task.Task]*taskAdmission),
		admissionsByID:        make(map[string]*taskAdmission),
		ownerOutstanding:      make(map[ports.ExecutionOwnerKey]int),
		closing:               make(chan struct{}),
	}
	for _, opt := range opts {
		opt(g)
	}
	if g.clock == nil {
		g.clock = realClock{}
	}
	if g.limiter == nil {
		g.limiter = noopLimiter{}
	}
	g.queue.SetMaxDepth(cfg.MaxQueueDepth)
	return g, nil
}

// Start launches background workers that manage offers, leases, bids, and results.
func (g *Gateway) Start(ctx context.Context) {
	go g.runOfferPump(ctx)
	go g.runBidLoop(ctx)
	go g.runResultLoop(ctx)
	go g.runLeaseWatcher(ctx)
	go func() {
		select {
		case <-ctx.Done():
			g.Stop()
		case <-g.closing:
		}
	}()
}

// Stop closes internal channels once workers drain. MESH-H08: idempotent — a
// second Stop previously panicked with "close of closed channel".
func (g *Gateway) Stop() {
	g.stopOnce.Do(func() {
		g.mu.Lock()
		g.stopped = true
		clear(g.admissionsBySubmitted)
		clear(g.admissionsByTask)
		clear(g.admissionsByID)
		clear(g.ownerOutstanding)
		g.mu.Unlock()
		close(g.closing)
	})
}

// Enqueue inserts a task into the gateway queue.
//
// Enqueue is the compatibility, body-only path. It deliberately does not bind
// Task.TenantID as authority, so an executor started without an independently
// established principal will reject the task before auth, middleware, or the
// handler runs. Authenticated producers should use EnqueueAuthorized.
func (g *Gateway) Enqueue(t *task.Task) error {
	return g.enqueue(t, nil, false)
}

// EnqueueAuthorized admits a task only when the injected product reader can
// read a server-established principal carrying a nonzero opaque owner key.
// The principal is retained privately against this exact task pointer for
// execution-time re-authorization; it is never indexed by Task.ID and no
// mutable Task field participates in owner construction.
func (g *Gateway) EnqueueAuthorized(ctx context.Context, t *task.Task) error {
	if t == nil {
		return errors.New("task: enqueue nil task")
	}
	if g.owner == nil {
		return errors.New("task: authorized enqueue requires an execution principal reader")
	}
	principal, ok := g.owner.ExecutionPrincipal(ctx)
	if !ok || principal == nil || !principal.OwnerKey().Valid() {
		return errors.New("task: authorized enqueue requires an established execution principal")
	}
	return g.enqueue(t, principal, true)
}

func (g *Gateway) enqueue(t *task.Task, principal ports.ExecutionPrincipal, bind bool) error {
	if t == nil {
		return errors.New("task: enqueue nil task")
	}
	if t.ID == "" {
		return errors.New("task: enqueue requires a nonempty task ID")
	}
	now := g.clock.Now()
	var owner ports.ExecutionOwnerKey
	if bind && principal != nil {
		owner = principal.OwnerKey()
		if !g.limiter.Allow(now, owner) {
			return fmt.Errorf("task: execution owner %s rate limited", owner.Fingerprint())
		}
	}

	owned := t.Clone()
	if owned == nil {
		return errors.New("task: could not snapshot admitted task")
	}
	if owned.CreatedAt.IsZero() {
		owned.CreatedAt = now
	}
	owned.EnqueuedAt = now
	owned.Status = task.TaskStatusQueued

	g.mu.Lock()
	if g.stopped {
		g.mu.Unlock()
		return errors.New("task: gateway stopped")
	}
	if _, exists := g.admissionsBySubmitted[t]; exists {
		g.mu.Unlock()
		return errors.New("task: task pointer is already admitted")
	}
	if _, exists := g.admissionsByTask[t]; exists {
		g.mu.Unlock()
		return errors.New("task: gateway-owned task cannot be externally re-enqueued")
	}
	if _, exists := g.admissionsByID[owned.ID]; exists {
		g.mu.Unlock()
		return errors.New("task: task ID is already admitted")
	}
	if bind {
		if g.ownerOutstanding[owner] >= g.config.MaxOutstandingPerOwner {
			g.mu.Unlock()
			return fmt.Errorf(
				"task: execution owner %s outstanding limit reached",
				owner.Fingerprint(),
			)
		}
		g.ownerOutstanding[owner]++
	}

	admission := &taskAdmission{
		submitted:      t,
		current:        owned,
		principal:      principal,
		owner:          owner,
		pinnedID:       owned.ID,
		pinnedTenantID: owned.TenantID,
	}
	g.admissionsBySubmitted[t] = admission
	g.admissionsByTask[owned] = admission
	g.admissionsByID[owned.ID] = admission
	if _, accepted := g.queue.Enqueue(owned); !accepted {
		owned.Status = task.TaskStatusFailed
		g.releaseAdmissionLocked(owned)
		g.mu.Unlock()
		return errors.New("task: gateway queue is full")
	}
	g.mu.Unlock()

	g.signalOfferPump()
	return nil
}

// EstablishedExecutionOwner returns only the opaque owner key bound to the
// exact admitted task pointer. It is not an ID lookup: a decoded copy or
// same-ID replacement is intentionally unbound.
func (g *Gateway) EstablishedExecutionOwner(t *task.Task) (ports.ExecutionOwnerKey, bool) {
	if t == nil {
		return ports.ExecutionOwnerKey{}, false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	admission, ok := g.admissionForPointerLocked(t)
	if !ok || admission.principal == nil || !admission.owner.Valid() {
		return ports.ExecutionOwnerKey{}, false
	}
	return admission.owner, true
}

// AuthorizeAssignment re-runs the product principal's current execution
// authorization for the exact admitted task pointer. It returns a context
// populated only through product-private keys plus the immutable owner key for
// an independent executor-side equality check.
func (g *Gateway) AuthorizeAssignment(
	ctx context.Context,
	t *task.Task,
) (context.Context, ports.ExecutionOwnerKey, func(), error) {
	if t == nil {
		return ctx, ports.ExecutionOwnerKey{}, nil, errors.New("task: assignment task is nil")
	}
	g.mu.Lock()
	admission, ok := g.admissionsByTask[t]
	g.mu.Unlock()
	if !ok || admission.principal == nil || !admission.owner.Valid() {
		return ctx, ports.ExecutionOwnerKey{}, nil, errors.New("task: assignment has no established execution principal")
	}
	if t.ID != admission.pinnedID || t.TenantID != admission.pinnedTenantID {
		return ctx, ports.ExecutionOwnerKey{}, nil, errors.New("task: assignment identity changed after admission")
	}
	owner := admission.owner
	authorized, release, err := admission.principal.AuthorizeExecution(ctx)
	if err != nil {
		return ctx, ports.ExecutionOwnerKey{}, nil, fmt.Errorf("task: execution principal rejected assignment: %w", err)
	}
	if authorized == nil || release == nil {
		if release != nil {
			release()
		}
		return ctx, ports.ExecutionOwnerKey{}, nil, errors.New("task: execution principal returned an incomplete authorization lease")
	}
	if admission.principal.OwnerKey() != owner {
		release()
		return ctx, ports.ExecutionOwnerKey{}, nil, errors.New("task: execution principal owner changed during authorization")
	}

	// AuthorizeExecution is product code and may block while policy or
	// revocation is resolved. Re-linearize against the gateway after it
	// returns: Stop, terminal settlement, retry transfer, or any replacement
	// that occurred in the meantime invalidates this authorization result.
	g.mu.Lock()
	current := g.admissionsByTask[t]
	stopped := g.stopped
	sameAdmission := current == admission && admission.current == t
	identityChanged := t.ID != admission.pinnedID || t.TenantID != admission.pinnedTenantID
	g.mu.Unlock()
	if stopped {
		release()
		return ctx, ports.ExecutionOwnerKey{}, nil, errors.New("task: gateway stopped during assignment authorization")
	}
	if !sameAdmission {
		release()
		return ctx, ports.ExecutionOwnerKey{}, nil, errors.New("task: assignment admission changed during authorization")
	}
	if identityChanged || admission.owner != owner || admission.principal.OwnerKey() != owner {
		release()
		return ctx, ports.ExecutionOwnerKey{}, nil, errors.New("task: assignment identity changed during authorization")
	}
	return authorized, owner, release, nil
}

// Offers exposes the outbound offer stream.
func (g *Gateway) Offers() <-chan task.TaskOffer { return g.offers }

// Assignments exposes issued assignments.
func (g *Gateway) Assignments() <-chan task.Assignment { return g.assignments }

// BidSink returns a channel executors can push bids into.
func (g *Gateway) BidSink() chan<- task.Bid { return g.bids }

// ProgressSink returns channel for progress reports (not yet persisted, but kept for future use).
func (g *Gateway) ProgressSink() chan<- task.Progress { return g.progress }

// ResultSink returns channel for result reports.
func (g *Gateway) ResultSink() chan<- task.ResultReport { return g.results }

// StatusOf reports the current status of a task the caller submitted.
//
// 🛑 THIS IS THE ONLY SAFE WAY TO READ A SUBMITTED TASK. Enqueue takes a
// *task.Task and the gateway's result/expiry loops then mutate its Status
// under g.mu (see handleResult, expireLeases) — on their own goroutines. The
// caller still holds that pointer, so reading `t.Status` directly is a data
// race, and it is one the caller cannot fix from outside because g.mu is
// unexported. Measured: `core/task/transport` did exactly that and the race
// detector caught it 1 run in 10.
//
// Reading through this accessor establishes the happens-before edge with
// those writes. It deliberately takes the task pointer rather than an ID:
// handleResult DELETES the task from g.inFlight before writing its final
// Status, so an id-keyed lookup could never observe a completed task.
//
// After Enqueue, treat the task's mutable fields as owned by the gateway.
func (g *Gateway) StatusOf(t *task.Task) task.TaskStatus {
	if t == nil {
		return ""
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if admission, ok := g.admissionForPointerLocked(t); ok && admission.current != nil {
		return admission.current.Status
	}
	return t.Status
}

func (g *Gateway) admissionForPointerLocked(t *task.Task) (*taskAdmission, bool) {
	if admission, ok := g.admissionsByTask[t]; ok {
		return admission, true
	}
	admission, ok := g.admissionsBySubmitted[t]
	return admission, ok
}

func (g *Gateway) signalOfferPump() {
	select {
	case g.signalCh <- struct{}{}:
	default:
	}
}

func (g *Gateway) runOfferPump(ctx context.Context) {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			close(g.offers)
			return
		case <-g.closing:
			close(g.offers)
			return
		case <-ticker.C:
			g.produceOffers()
		case <-g.signalCh:
			g.produceOffers()
		}
	}
}

func (g *Gateway) produceOffers() {
	g.mu.Lock()
	defer g.mu.Unlock()

	for len(g.open) < g.config.MaxInFlightOffers {
		t := g.queue.Pop()
		if t == nil {
			break
		}
		now := g.clock.Now()
		offer := task.TaskOffer{
			// Offer consumers need the immutable ID and body to decide whether
			// to bid, not mutation access to the gateway-owned task.
			Task:       t.Clone(),
			IssuedAt:   now,
			ExpiresAt:  now.Add(g.config.OfferTTL),
			OfferToken: uuid.NewString(),
		}
		t.Status = task.TaskStatusOffered
		t.OfferedAt = now
		g.open[t.ID] = &offerState{
			task:      t,
			offer:     offer,
			createdAt: now,
			deadline:  offer.ExpiresAt,
		}
		select {
		case g.offers <- offer:
		default:
			// If downstream is slow, requeue once and break to avoid busy loop.
			if _, accepted := g.queue.Enqueue(t); !accepted {
				t.Status = task.TaskStatusFailed
				g.releaseAdmissionLocked(t)
			}
			delete(g.open, t.ID)
			return
		}
	}
	// Re-enqueue expired offers
	now := g.clock.Now()
	for id, state := range g.open {
		if now.After(state.deadline) {
			delete(g.open, id)
			g.scheduleRetryLocked(state.task)
		}
	}
}

func (g *Gateway) runBidLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-g.closing:
			return
		case bid := <-g.bids:
			g.handleBid(bid)
		}
	}
}

func (g *Gateway) handleBid(bid task.Bid) {
	g.mu.Lock()
	defer g.mu.Unlock()
	state, ok := g.open[bid.TaskID]
	if !ok {
		return
	}
	now := g.clock.Now()
	recentWin := false
	if ts, ok := g.lastWin[bid.NodeID]; ok {
		recentWin = now.Sub(ts) < g.config.BidPolicy.MaxRecentWindow
	}
	if bid.ReceivedAt.IsZero() {
		bid.ReceivedAt = now
	}
	score := g.config.BidPolicy.Evaluate(bid, now, recentWin)
	if score < g.config.BidPolicy.MinScore {
		return
	}
	// First sufficient bid wins immediately.
	g.issueAssignmentLocked(state, &bid)
}

func (g *Gateway) issueAssignmentLocked(state *offerState, bid *task.Bid) {
	t := state.task
	delete(g.open, t.ID)
	now := g.clock.Now()
	fence := uuid.NewString()
	assignment := task.Assignment{
		Task:          t,
		TaskID:        t.ID,
		Primary:       bid.NodeID,
		Fence:         fence,
		LeaseDuration: g.config.LeaseDuration,
		IssuedAt:      now,
	}
	state.task.Status = task.TaskStatusAssigned
	g.inFlight[t.ID] = &assignmentState{
		task:       t,
		assignment: assignment,
		deadline:   now.Add(g.config.LeaseDuration),
		attempt:    t.Attempt,
	}
	g.lastWin[bid.NodeID] = now
	// MESH-H05: prune lastWin entries older than the recent-win window so the map
	// can't accumulate one permanent entry per winning node across fleet churn.
	// Only entries within MaxRecentWindow affect handleBid's recentWin decision.
	if win := g.config.BidPolicy.MaxRecentWindow; win > 0 {
		for nodeID, ts := range g.lastWin {
			if now.Sub(ts) > win {
				delete(g.lastWin, nodeID)
			}
		}
	}
	select {
	case g.assignments <- assignment:
	default:
		// If assignments channel is blocked, requeue task to avoid starvation.
		delete(g.inFlight, t.ID)
		g.scheduleRetryLocked(t)
	}
}

func (g *Gateway) runResultLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-g.closing:
			return
		case res := <-g.results:
			g.handleResult(res.Result)
		case <-g.progress:
			// Progress currently advisory; hook for future metrics sink.
		}
	}
}

func (g *Gateway) handleResult(result task.Result) {
	g.mu.Lock()
	defer g.mu.Unlock()
	state, ok := g.inFlight[result.TaskID]
	if !ok {
		return
	}
	if result.Fence != state.assignment.Fence {
		return
	}
	delete(g.inFlight, result.TaskID)
	switch result.Status {
	case task.ResultStatusOK:
		state.task.Status = task.TaskStatusCompleted
		g.releaseAdmissionLocked(state.task)
	case task.ResultStatusRejected:
		// Authorization and tenant failures are deterministic for this admitted
		// object. Retrying them cannot create authority and only amplifies load.
		state.task.Status = task.TaskStatusFailed
		g.releaseAdmissionLocked(state.task)
	default:
		state.task.Status = task.TaskStatusFailed
		// MESH-H09: use the same retry budget as produceOffers / expireLeases
		// (clone.Attempt <= MaxAttempts). state.task.Attempt+1 is what
		// CloneForRetry sets clone.Attempt to, so `+1 <= MaxAttempts` matches;
		// the previous `+1 < MaxAttempts` stopped one attempt early, so a task
		// got a different number of retries depending on whether it failed
		// (here) vs timed out (expireLeases).
		if state.task.Attempt+1 <= g.config.MaxAttempts {
			g.scheduleRetryLocked(state.task)
		} else {
			g.releaseAdmissionLocked(state.task)
		}
	}
}

func (g *Gateway) runLeaseWatcher(ctx context.Context) {
	ticker := time.NewTicker(g.config.LeaseCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-g.closing:
			return
		case <-ticker.C:
			g.expireLeases()
		}
	}
}

func (g *Gateway) expireLeases() {
	g.mu.Lock()
	defer g.mu.Unlock()
	now := g.clock.Now()
	for id, state := range g.inFlight {
		if now.After(state.deadline) {
			delete(g.inFlight, id)
			g.scheduleRetryLocked(state.task)
		}
	}
}

// scheduleRetryLocked clones a gateway-owned task and transfers its private
// admission record to that exact clone. It never re-enters the public enqueue
// path, so a retry cannot impersonate a fresh producer admission.
func (g *Gateway) scheduleRetryLocked(original *task.Task) {
	clone := original.CloneForRetry()
	if clone == nil || clone.Attempt > g.config.MaxAttempts {
		g.releaseAdmissionLocked(original)
		return
	}
	admission, ok := g.admissionsByTask[original]
	if !ok {
		return
	}
	delete(g.admissionsByTask, original)
	admission.current = clone
	admission.pinnedID = clone.ID
	admission.pinnedTenantID = clone.TenantID
	g.admissionsByTask[clone] = admission
	time.AfterFunc(g.config.RequeueDelay, func() {
		g.mu.Lock()
		defer g.mu.Unlock()
		if g.stopped {
			clone.Status = task.TaskStatusCanceled
			g.releaseAdmissionLocked(clone)
			return
		}
		clone.EnqueuedAt = g.clock.Now()
		clone.Status = task.TaskStatusQueued
		if _, accepted := g.queue.Enqueue(clone); !accepted {
			clone.Status = task.TaskStatusFailed
			g.releaseAdmissionLocked(clone)
			return
		}
		g.signalOfferPump()
	})
}

func (g *Gateway) releaseAdmissionLocked(t *task.Task) {
	admission, ok := g.admissionsByTask[t]
	if !ok {
		return
	}
	delete(g.admissionsByTask, t)
	delete(g.admissionsBySubmitted, admission.submitted)
	delete(g.admissionsByID, admission.pinnedID)
	if admission.submitted != nil {
		// This mirror is not scheduler authority; StatusOf remains the only
		// synchronized read while the task is active.
		admission.submitted.Status = t.Status
	}
	if admission.owner.Valid() {
		if remaining := g.ownerOutstanding[admission.owner] - 1; remaining > 0 {
			g.ownerOutstanding[admission.owner] = remaining
		} else {
			delete(g.ownerOutstanding, admission.owner)
		}
	}
}
