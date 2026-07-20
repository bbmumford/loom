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
)

// Clock abstracts time for testing.
type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// TenantLimiter implements tenant fairness (token bucket, etc.).
type TenantLimiter interface {
	Allow(now time.Time, tenant string) bool
}

type noopLimiter struct{}

func (noopLimiter) Allow(time.Time, string) bool { return true }

// GatewayConfig configures offer/bid/assignment behaviour.
type GatewayConfig struct {
	ID                 string
	QueuePolicy        queue.QueuePolicy
	BidPolicy          bidding.BidPolicy
	LeaseDuration      time.Duration
	OfferTTL           time.Duration
	MaxInFlightOffers  int
	MaxAttempts        int
	RequeueDelay       time.Duration
	LeaseCheckInterval time.Duration
}

// Gateway coordinates task scheduling for a mesh role.
type Gateway struct {
	id      string
	queue   *queue.TaskQueue
	config  GatewayConfig
	clock   Clock
	limiter TenantLimiter

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
	closing  chan struct{}
	stopOnce sync.Once // MESH-H08: makes Stop idempotent (close(g.closing) once)
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

// GatewayOption allows optional configuration injection.
type GatewayOption func(*Gateway)

// WithLimiter injects a custom tenant limiter.
func WithLimiter(l TenantLimiter) GatewayOption {
	return func(g *Gateway) { g.limiter = l }
}

// WithClock injects a custom clock implementation.
func WithClock(clock Clock) GatewayOption {
	return func(g *Gateway) { g.clock = clock }
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
		id:          cfg.ID,
		queue:       queue.NewTaskQueue(cfg.QueuePolicy),
		config:      cfg,
		clock:       realClock{},
		limiter:     noopLimiter{},
		offers:      make(chan task.TaskOffer, cfg.MaxInFlightOffers),
		assignments: make(chan task.Assignment, cfg.MaxInFlightOffers),
		bids:        make(chan task.Bid, 128),
		progress:    make(chan task.Progress, 128),
		results:     make(chan task.ResultReport, 128),
		signalCh:    make(chan struct{}, 1),
		open:        make(map[string]*offerState),
		inFlight:    make(map[string]*assignmentState),
		lastWin:     make(map[string]time.Time),
		closing:     make(chan struct{}),
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
	return g, nil
}

// Start launches background workers that manage offers, leases, bids, and results.
func (g *Gateway) Start(ctx context.Context) {
	go g.runOfferPump(ctx)
	go g.runBidLoop(ctx)
	go g.runResultLoop(ctx)
	go g.runLeaseWatcher(ctx)
}

// Stop closes internal channels once workers drain. MESH-H08: idempotent — a
// second Stop previously panicked with "close of closed channel".
func (g *Gateway) Stop() {
	g.stopOnce.Do(func() {
		close(g.closing)
	})
}

// Enqueue inserts a task into the gateway queue.
func (g *Gateway) Enqueue(t *task.Task) error {
	if t == nil {
		return errors.New("task: enqueue nil task")
	}
	now := g.clock.Now()
	if t.CreatedAt.IsZero() {
		t.CreatedAt = now
	}
	if !g.limiter.Allow(now, t.TenantID) {
		return fmt.Errorf("task: tenant %s rate limited", t.TenantID)
	}
	t.EnqueuedAt = now
	t.Status = task.TaskStatusQueued
	g.queue.Enqueue(t)
	g.signalOfferPump()
	return nil
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
			Task:       t,
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
			g.queue.Enqueue(t)
			delete(g.open, t.ID)
			return
		}
	}
	// Re-enqueue expired offers
	now := g.clock.Now()
	for id, state := range g.open {
		if now.After(state.deadline) {
			delete(g.open, id)
			clone := state.task.CloneForRetry()
			if clone.Attempt <= g.config.MaxAttempts {
				time.AfterFunc(g.config.RequeueDelay, func() { _ = g.Enqueue(clone) })
			}
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
		clone := t.CloneForRetry()
		time.AfterFunc(g.config.RequeueDelay, func() { _ = g.Enqueue(clone) })
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
	default:
		state.task.Status = task.TaskStatusFailed
		// MESH-H09: use the same retry budget as produceOffers / expireLeases
		// (clone.Attempt <= MaxAttempts). state.task.Attempt+1 is what
		// CloneForRetry sets clone.Attempt to, so `+1 <= MaxAttempts` matches;
		// the previous `+1 < MaxAttempts` stopped one attempt early, so a task
		// got a different number of retries depending on whether it failed
		// (here) vs timed out (expireLeases).
		if state.task.Attempt+1 <= g.config.MaxAttempts {
			clone := state.task.CloneForRetry()
			time.AfterFunc(g.config.RequeueDelay, func() { _ = g.Enqueue(clone) })
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
			clone := state.task.CloneForRetry()
			if clone.Attempt <= g.config.MaxAttempts {
				time.AfterFunc(g.config.RequeueDelay, func() { _ = g.Enqueue(clone) })
			}
		}
	}
}
