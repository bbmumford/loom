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
	taskstore "github.com/bbmumford/loom/core/task/store"
	"github.com/bbmumford/loom/ports"
)

// Clock abstracts time for testing.
type Clock interface {
	Now() time.Time
}

type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

// PartitionLimiter implements fairness over immutable authenticated partitions.
type PartitionLimiter interface {
	Allow(now time.Time, partitionKey ports.ExecutionPartitionKey) bool
}

type noopLimiter struct{}

func (noopLimiter) Allow(time.Time, ports.ExecutionPartitionKey) bool { return true }

// GatewayConfig configures offer/bid/assignment behaviour.
type GatewayConfig struct {
	ID                         string
	QueuePolicy                queue.QueuePolicy
	BidPolicy                  bidding.BidPolicy
	LeaseDuration              time.Duration
	OfferTTL                   time.Duration
	MaxInFlightOffers          int
	MaxQueueDepth              int
	MaxOutstandingPerPartition int
	MaxAttempts                int
	RequeueDelay               time.Duration
	LeaseCheckInterval         time.Duration
	// BidWindow is how long an open offer collects bids before its award rule
	// decides over all of them. Zero decides on each bid as it arrives. A window
	// longer than OfferTTL is cut to it, since an offer takes no bid after it
	// expires.
	BidWindow time.Duration
	// TakeoverWindow is how long after a lease expires unrenewed its task is
	// offered again for a takeover: each lease version's TakeoverUntil is its
	// expiry plus this window, a winner inside it takes the task over under
	// the store's Award with Supersedes, and a task no takeover won by then
	// ends as OutcomeReaped. Zero releases an expired lease at once and
	// retries the task as a new attempt.
	TakeoverWindow time.Duration
	// MaxWait ends a task still waiting for an award this long after its
	// admission (OutcomeAbandoned, CauseMaxWaitElapsed). Zero sets no limit.
	MaxWait time.Duration
	// WaitSweepInterval is how often every waiting task is swept: MaxWait is
	// checked and the store's KeepWaiting called. Zero takes LeaseCheckInterval.
	WaitSweepInterval time.Duration
}

// Gateway coordinates task scheduling for a mesh role.
type Gateway struct {
	id         string
	queue      *queue.TaskQueue
	config     GatewayConfig
	clock      Clock
	limiter    PartitionLimiter
	principals ports.ExecutionPrincipalReader
	// award decides an offer's winner (WithAwardRule; bidding.ScoreRule over
	// the config's BidPolicy when none is given), and recordAward, when set,
	// receives every decision it makes.
	award       bidding.AwardRule
	recordAward AwardRecorder
	// store owns every admission, award, lease version and outcome
	// (WithStore; a MemoryStore when none is given); signer signs each lease
	// version inside the store's transaction and verifier admits a completion's
	// receipt (WithLeaseSigner, WithReceiptVerifier; either may be nil).
	store    taskstore.Store
	signer   taskstore.LeaseSigner
	verifier taskstore.ReceiptVerifier
	// storeCtx is the context every store call runs under: Start's context,
	// or Background before Start.
	storeCtx context.Context

	offers      chan task.TaskOffer
	renewals    chan task.Renewal
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
	partitionOutstanding  map[ports.ExecutionPartitionKey]int
	closing               chan struct{}
	stopOnce              sync.Once // makes Stop idempotent (close(g.closing) once)
	stopped               bool
}

type offerState struct {
	task      *task.Task
	offer     task.TaskOffer
	createdAt time.Time
	deadline  time.Time
	// windowEnd is when the award rule first decides; bids holds the latest bid
	// from each node, in the order the nodes first bid; undecided is set when a
	// bid arrived since the rule last decided.
	windowEnd time.Time
	bids      []task.Bid
	undecided bool
}

// AwardRecorder receives each award decision the gateway's rule makes, with the
// offer it decided, after the gateway's own state reflects it. It runs on the
// gateway's goroutine and must not block.
type AwardRecorder func(offer task.TaskOffer, decision bidding.AwardDecision)

// assignmentState is a task under lease: lease is the current version the
// store committed (nil only for a state built without the store), and
// deadline its expiry.
type assignmentState struct {
	task       *task.Task
	assignment task.Assignment
	deadline   time.Time
	attempt    int
	lease      *task.Lease
}

// taskAdmission is the gateway's record of one admitted task. admittedAt
// starts its MaxWait. supersedes, while set, is the lapsed lease a takeover
// must replace, and takeoverUntil the end of that lease's window.
type taskAdmission struct {
	submitted         *task.Task
	current           *task.Task
	principal         ports.ExecutionPrincipal
	partitionKey      ports.ExecutionPartitionKey
	pinnedID          string
	pinnedPartitionID string
	admittedAt        time.Time
	supersedes        *task.LeaseIdentity
	takeoverUntil     time.Time
}

// GatewayOption allows optional configuration injection.
type GatewayOption func(*Gateway)

// WithLimiter injects a custom authenticated-partition limiter.
func WithLimiter(l PartitionLimiter) GatewayOption {
	return func(g *Gateway) { g.limiter = l }
}

// WithClock injects a custom clock implementation.
func WithClock(clock Clock) GatewayOption {
	return func(g *Gateway) { g.clock = clock }
}

// WithAwardRule injects the rule that decides an offer's winner over the bids
// its window collected. A job kind supplies its own; without one the gateway
// uses bidding.ScoreRule over the config's BidPolicy.
func WithAwardRule(rule bidding.AwardRule) GatewayOption {
	return func(g *Gateway) { g.award = rule }
}

// WithAwardRecorder injects the recipient of every award decision, so the
// winner, the rule, the candidates and the ambiguity of each award are kept by
// the consumer that answers for the award.
func WithAwardRecorder(record AwardRecorder) GatewayOption {
	return func(g *Gateway) { g.recordAward = record }
}

// WithStore makes store the gateway's durable state. A nil store keeps the
// MemoryStore the gateway builds.
func WithStore(store taskstore.Store) GatewayOption {
	return func(g *Gateway) {
		if store != nil {
			g.store = store
		}
	}
}

// WithLeaseSigner sets the signer each lease version is signed by inside the
// store's Award and Renew.
func WithLeaseSigner(signer taskstore.LeaseSigner) GatewayOption {
	return func(g *Gateway) { g.signer = signer }
}

// WithReceiptVerifier sets the verifier an OK result's receipt must pass
// before the task completes; a refused receipt fails the attempt.
func WithReceiptVerifier(verifier taskstore.ReceiptVerifier) GatewayOption {
	return func(g *Gateway) { g.verifier = verifier }
}

// WithExecutionPrincipalReader injects the product-owned authority seam used
// by EnqueueAuthorized. The reader must obtain an immutable principal from a
// server-private context key established by authenticated transport/session
// or an explicit service root; no Task field is passed to it.
func WithExecutionPrincipalReader(reader ports.ExecutionPrincipalReader) GatewayOption {
	return func(g *Gateway) { g.principals = reader }
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
	if cfg.MaxOutstandingPerPartition <= 0 {
		cfg.MaxOutstandingPerPartition = 64
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
	if cfg.BidWindow < 0 {
		cfg.BidWindow = 0
	}
	if cfg.BidWindow > cfg.OfferTTL {
		cfg.BidWindow = cfg.OfferTTL
	}
	if cfg.TakeoverWindow < 0 {
		cfg.TakeoverWindow = 0
	}
	if cfg.MaxWait < 0 {
		cfg.MaxWait = 0
	}
	if cfg.WaitSweepInterval <= 0 {
		cfg.WaitSweepInterval = cfg.LeaseCheckInterval
	}

	g := &Gateway{
		id:                    cfg.ID,
		queue:                 queue.NewTaskQueue(cfg.QueuePolicy),
		config:                cfg,
		clock:                 realClock{},
		limiter:               noopLimiter{},
		store:                 taskstore.NewMemoryStore(),
		storeCtx:              context.Background(),
		offers:                make(chan task.TaskOffer, cfg.MaxInFlightOffers),
		renewals:              make(chan task.Renewal, 128),
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
		partitionOutstanding:  make(map[ports.ExecutionPartitionKey]int),
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
	if g.award == nil {
		g.award = bidding.ScoreRule{Policy: cfg.BidPolicy}
	}
	g.queue.SetMaxDepth(cfg.MaxQueueDepth)
	return g, nil
}

// Start launches background workers that manage offers, leases, bids, and results.
//
// Start first rehydrates from the store's Load: a waiting task is queued for
// its next offer, a task holding an unexpired lease is held in flight to that
// lease's expiry and not awarded again, a lapsed lease inside its takeover
// window is offered for a takeover, and one past it ends as OutcomeReaped. A
// Load failure is returned and starts nothing.
func (g *Gateway) Start(ctx context.Context) error {
	g.mu.Lock()
	g.storeCtx = ctx
	err := g.rehydrateLocked(ctx)
	g.mu.Unlock()
	if err != nil {
		return err
	}
	g.signalOfferPump()
	go g.runOfferPump(ctx)
	go g.runBidLoop(ctx)
	go g.runResultLoop(ctx)
	go g.runLeaseWatcher(ctx)
	go g.runWaitSweeper(ctx)
	go func() {
		select {
		case <-ctx.Done():
			g.Stop()
		case <-g.closing:
		}
	}()
	return nil
}

// rehydrateLocked admits into the gateway's memory every task the store holds
// unended that the gateway does not already hold (a task enqueued before
// Start is in both).
func (g *Gateway) rehydrateLocked(ctx context.Context) error {
	records, err := g.store.Load(ctx)
	if err != nil {
		return fmt.Errorf("task: rehydrate from the store: %w", err)
	}
	now := g.clock.Now()
	for _, rec := range records {
		if rec.Task == nil || rec.Task.ID == "" {
			continue
		}
		if _, held := g.admissionsByID[rec.Task.ID]; held {
			continue
		}
		owned := rec.Task.Clone()
		admission := &taskAdmission{
			current:           owned,
			principal:         rec.Principal,
			pinnedID:          owned.ID,
			pinnedPartitionID: owned.Partition,
			admittedAt:        rec.AdmittedAt,
		}
		if rec.Principal != nil {
			admission.partitionKey = rec.Principal.PartitionKey()
			g.partitionOutstanding[admission.partitionKey]++
		}
		g.admissionsByTask[owned] = admission
		g.admissionsByID[owned.ID] = admission
		switch {
		case rec.State == taskstore.StateLeased && rec.Lease != nil && !now.After(rec.Lease.Terms.ExpiresAt):
			owned.Status = task.TaskStatusAssigned
			g.inFlight[owned.ID] = &assignmentState{
				task:       owned,
				assignment: g.assignmentFor(owned, rec.Lease),
				deadline:   rec.Lease.Terms.ExpiresAt,
				attempt:    owned.Attempt,
				lease:      rec.Lease,
			}
		case rec.State == taskstore.StateLeased && rec.Lease != nil:
			id := rec.Lease.Identity()
			if now.After(rec.Lease.Terms.TakeoverUntil) {
				g.finishLocked(owned, &id, taskstore.OutcomeReaped, taskstore.CauseTakeoverWindowElapsed, nil)
				continue
			}
			admission.supersedes, admission.takeoverUntil = &id, rec.Lease.Terms.TakeoverUntil
			g.requeueLocked(owned)
		default:
			g.requeueLocked(owned)
		}
	}
	return nil
}

// requeueLocked puts an admitted task back on the queue for its next offer,
// or ends it as cancelled when the queue refuses it.
func (g *Gateway) requeueLocked(t *task.Task) {
	t.EnqueuedAt = g.clock.Now()
	t.Status = task.TaskStatusQueued
	if _, accepted := g.queue.Enqueue(t); !accepted {
		g.finishLocked(t, g.currentLeaseLocked(t), taskstore.OutcomeCancelled, taskstore.CauseQueueFull, nil)
	}
}

// currentLeaseLocked is the identity of the lease t's admission must replace
// on a takeover, the one Finish of a task between leases names.
func (g *Gateway) currentLeaseLocked(t *task.Task) *task.LeaseIdentity {
	if a, ok := g.admissionsByTask[t]; ok && a.supersedes != nil {
		id := *a.supersedes
		return &id
	}
	return nil
}

// finishLocked records t's terminal outcome in the store and releases its
// admission. The store refusing it (a lost compare-and-set) still releases
// the gateway's admission: the store's own record is the outcome that stands.
func (g *Gateway) finishLocked(t *task.Task, lease *task.LeaseIdentity, outcome taskstore.Outcome, cause string, receipt []byte) {
	_ = g.store.Finish(g.storeCtx, taskstore.Terminal{
		TaskID: t.ID, Lease: lease, Outcome: outcome, Cause: cause, Receipt: receipt, At: g.clock.Now(),
	})
	switch outcome {
	case taskstore.OutcomeCompleted:
		t.Status = task.TaskStatusCompleted
	case taskstore.OutcomeCancelled:
		t.Status = task.TaskStatusCanceled
	default:
		t.Status = task.TaskStatusFailed
	}
	if state, ok := g.open[t.ID]; ok && state.task == t {
		delete(g.open, t.ID)
	}
	g.releaseAdmissionLocked(t)
}

// assignmentFor is the assignment that carries lease to its holder.
func (g *Gateway) assignmentFor(t *task.Task, lease *task.Lease) task.Assignment {
	return task.Assignment{
		Task:          t,
		TaskID:        t.ID,
		Primary:       lease.Terms.NodeID,
		Fence:         lease.Identity().Fence(),
		Lease:         lease.Clone(),
		LeaseDuration: lease.Terms.ExpiresAt.Sub(lease.Terms.IssuedAt),
		IssuedAt:      lease.Terms.IssuedAt,
	}
}

// Stop closes internal channels once workers drain. Idempotent: stopOnce
// guards close(g.closing), which would panic with "close of closed channel"
// on a second call, and Start's ctx watcher calls Stop too.
func (g *Gateway) Stop() {
	g.stopOnce.Do(func() {
		g.mu.Lock()
		g.stopped = true
		clear(g.admissionsBySubmitted)
		clear(g.admissionsByTask)
		clear(g.admissionsByID)
		clear(g.partitionOutstanding)
		g.mu.Unlock()
		close(g.closing)
	})
}

// Enqueue inserts a task into the gateway queue.
//
// Enqueue is the compatibility, body-only path. It deliberately does not bind
// Task.Partition as authority, so an executor started without an independently
// established principal will reject the task before auth, middleware, or the
// handler runs. Authenticated producers should use EnqueueAuthorized.
func (g *Gateway) Enqueue(t *task.Task) error {
	return g.enqueue(t, nil, false)
}

// EnqueueAuthorized admits a task only when the injected product reader can
// read a server-established principal carrying a nonzero opaque partition key.
// The principal is retained privately against this exact task pointer for
// execution-time re-authorization; it is never indexed by Task.ID and no
// mutable Task field participates in partition-key construction.
func (g *Gateway) EnqueueAuthorized(ctx context.Context, t *task.Task) error {
	if t == nil {
		return errors.New("task: enqueue nil task")
	}
	if g.principals == nil {
		return errors.New("task: authorized enqueue requires an execution principal reader")
	}
	principal, ok := g.principals.ExecutionPrincipal(ctx)
	if !ok || principal == nil || !principal.PartitionKey().Valid() {
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
	var partitionKey ports.ExecutionPartitionKey
	if bind && principal != nil {
		partitionKey = principal.PartitionKey()
		if !g.limiter.Allow(now, partitionKey) {
			return fmt.Errorf("task: execution partition %s rate limited", partitionKey.Fingerprint())
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
		if g.partitionOutstanding[partitionKey] >= g.config.MaxOutstandingPerPartition {
			g.mu.Unlock()
			return fmt.Errorf(
				"task: execution partition %s outstanding limit reached",
				partitionKey.Fingerprint(),
			)
		}
	}
	// The queue is checked before the store admits anything, so a refused
	// producer leaves no durable admission and may enqueue the same id again.
	if g.queue.Len() >= g.config.MaxQueueDepth {
		g.mu.Unlock()
		return errors.New("task: gateway queue is full")
	}
	var stored ports.ExecutionPrincipal
	if bind {
		stored = principal
	}
	if err := g.store.Admit(g.storeCtx, taskstore.Record{
		Task: owned, Principal: stored, AdmittedAt: now, State: taskstore.StateWaiting,
	}); err != nil {
		g.mu.Unlock()
		return fmt.Errorf("task: admit: %w", err)
	}
	if bind {
		g.partitionOutstanding[partitionKey]++
	}

	admission := &taskAdmission{
		submitted:         t,
		current:           owned,
		principal:         principal,
		partitionKey:      partitionKey,
		pinnedID:          owned.ID,
		pinnedPartitionID: owned.Partition,
		admittedAt:        now,
	}
	g.admissionsBySubmitted[t] = admission
	g.admissionsByTask[owned] = admission
	g.admissionsByID[owned.ID] = admission
	if _, accepted := g.queue.Enqueue(owned); !accepted {
		_ = g.store.Finish(g.storeCtx, taskstore.Terminal{TaskID: owned.ID, Outcome: taskstore.OutcomeCancelled,
			Cause: taskstore.CauseQueueFull, At: now})
		owned.Status = task.TaskStatusFailed
		g.releaseAdmissionLocked(owned)
		g.mu.Unlock()
		return errors.New("task: gateway queue is full")
	}
	g.mu.Unlock()

	g.signalOfferPump()
	return nil
}

// EstablishedExecutionPartition returns only the opaque partition key bound to the
// exact admitted task pointer. It is not an ID lookup: a decoded copy or
// same-ID replacement is intentionally unbound.
func (g *Gateway) EstablishedExecutionPartition(t *task.Task) (ports.ExecutionPartitionKey, bool) {
	if t == nil {
		return ports.ExecutionPartitionKey{}, false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	admission, ok := g.admissionForPointerLocked(t)
	if !ok || admission.principal == nil || !admission.partitionKey.Valid() {
		return ports.ExecutionPartitionKey{}, false
	}
	return admission.partitionKey, true
}

// AuthorizeAssignment re-runs the product principal's current execution
// authorization for the exact admitted task pointer. It returns a context
// populated only through product-private keys plus the immutable partition key for
// an independent executor-side equality check.
func (g *Gateway) AuthorizeAssignment(
	ctx context.Context,
	t *task.Task,
) (context.Context, ports.ExecutionPartitionKey, func(), error) {
	if t == nil {
		return ctx, ports.ExecutionPartitionKey{}, nil, errors.New("task: assignment task is nil")
	}
	g.mu.Lock()
	admission, ok := g.admissionsByTask[t]
	g.mu.Unlock()
	if !ok || admission.principal == nil || !admission.partitionKey.Valid() {
		return ctx, ports.ExecutionPartitionKey{}, nil, errors.New("task: assignment has no established execution principal")
	}
	if t.ID != admission.pinnedID || t.Partition != admission.pinnedPartitionID {
		return ctx, ports.ExecutionPartitionKey{}, nil, errors.New("task: assignment identity changed after admission")
	}
	partitionKey := admission.partitionKey
	authorized, release, err := admission.principal.AuthorizeExecution(ctx)
	if err != nil {
		return ctx, ports.ExecutionPartitionKey{}, nil, fmt.Errorf("task: execution principal rejected assignment: %w", err)
	}
	if authorized == nil || release == nil {
		if release != nil {
			release()
		}
		return ctx, ports.ExecutionPartitionKey{}, nil, errors.New("task: execution principal returned an incomplete authorization lease")
	}
	if admission.principal.PartitionKey() != partitionKey {
		release()
		return ctx, ports.ExecutionPartitionKey{}, nil, errors.New("task: execution principal partition changed during authorization")
	}

	// AuthorizeExecution is product code and may block while policy or
	// revocation is resolved. Re-linearize against the gateway after it
	// returns: Stop, terminal settlement, retry transfer, or any replacement
	// that occurred in the meantime invalidates this authorization result.
	g.mu.Lock()
	current := g.admissionsByTask[t]
	stopped := g.stopped
	sameAdmission := current == admission && admission.current == t
	identityChanged := t.ID != admission.pinnedID || t.Partition != admission.pinnedPartitionID
	g.mu.Unlock()
	if stopped {
		release()
		return ctx, ports.ExecutionPartitionKey{}, nil, errors.New("task: gateway stopped during assignment authorization")
	}
	if !sameAdmission {
		release()
		return ctx, ports.ExecutionPartitionKey{}, nil, errors.New("task: assignment admission changed during authorization")
	}
	if identityChanged || admission.partitionKey != partitionKey || admission.principal.PartitionKey() != partitionKey {
		release()
		return ctx, ports.ExecutionPartitionKey{}, nil, errors.New("task: assignment identity changed during authorization")
	}
	return authorized, partitionKey, release, nil
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
	var decided []awardNotice
	g.mu.Lock()
	defer func() {
		g.mu.Unlock()
		g.notifyAwards(decided)
	}()

	for len(g.open) < g.config.MaxInFlightOffers {
		t := g.queue.Pop()
		if t == nil {
			break
		}
		if _, admitted := g.admissionsByTask[t]; !admitted {
			// ended while it waited in the queue (MaxWait, a takeover window,
			// the store ending its wait)
			continue
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
			windowEnd: now.Add(g.config.BidWindow),
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
	// Re-enqueue expired offers; decide the offers whose bid window has closed
	// over bids the rule has not yet seen.
	now := g.clock.Now()
	for id, state := range g.open {
		if now.After(state.deadline) {
			// No award: the offer lapses and the task waits for its next
			// offer, bounded by MaxWait and the store's KeepWaiting.
			delete(g.open, id)
			g.requeueLocked(state.task)
			continue
		}
		if state.undecided && !now.Before(state.windowEnd) {
			decided = append(decided, g.decideLocked(state, now))
		}
	}
}

// awardNotice is one decision for the recorder, taken under the gateway's lock
// and delivered after it is released.
type awardNotice struct {
	offer    task.TaskOffer
	decision bidding.AwardDecision
}

// notifyAwards hands each decision to the recorder, outside the gateway's lock.
func (g *Gateway) notifyAwards(notices []awardNotice) {
	if g.recordAward == nil {
		return
	}
	for _, n := range notices {
		g.recordAward(n.offer, n.decision)
	}
}

// decideLocked asks the award rule over state's bids and, when it names a
// winner that bid on the offer, issues that winner's assignment. A rule error
// or a winner the window never collected leaves the offer open, to be decided
// again when another bid arrives and to lapse at its deadline otherwise.
func (g *Gateway) decideLocked(state *offerState, now time.Time) awardNotice {
	state.undecided = false
	bids := append([]task.Bid(nil), state.bids...)
	decision, err := g.award.Award(state.offer, bids, bidding.AwardContext{
		Now: now,
		RecentWinner: func(node string) bool {
			ts, ok := g.lastWin[node]
			return ok && now.Sub(ts) < g.config.BidPolicy.MaxRecentWindow
		},
	})
	if decision.Candidates == nil {
		decision.Candidates = bids
	}
	switch {
	case err != nil:
		decision.Winner = nil
		decision.Reason = err.Error()
	case decision.Winner != nil:
		winner, ok := collectedBid(bids, *decision.Winner)
		if !ok {
			decision.Winner = nil
			decision.Reason = "the award rule named a bid the offer did not collect"
			break
		}
		g.issueAssignmentLocked(state, &winner)
	}
	return awardNotice{offer: state.offer, decision: decision}
}

// collectedBid is the bid in bids from want's node. The gateway assigns that
// collected bid rather than the rule's copy, so a rule cannot award a node, a
// task or terms no bid in the window carried.
func collectedBid(bids []task.Bid, want task.Bid) (task.Bid, bool) {
	for _, b := range bids {
		if b.NodeID == want.NodeID && b.TaskID == want.TaskID {
			return b, true
		}
	}
	return task.Bid{}, false
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

// handleBid records bid on its task's open offer, the latest bid from each node
// replacing that node's earlier one. A bid for no open offer, from no node, or
// naming another offer of the task is dropped. Once the offer's window has
// closed, each new bid is decided at once over everything collected.
func (g *Gateway) handleBid(bid task.Bid) {
	var decided []awardNotice
	g.mu.Lock()
	defer func() {
		g.mu.Unlock()
		g.notifyAwards(decided)
	}()
	state, ok := g.open[bid.TaskID]
	if !ok || bid.NodeID == "" {
		return
	}
	if bid.OfferToken != "" && bid.OfferToken != state.offer.OfferToken {
		return
	}
	now := g.clock.Now()
	if now.After(state.deadline) {
		return
	}
	if bid.ReceivedAt.IsZero() {
		bid.ReceivedAt = now
	}
	replaced := false
	for i := range state.bids {
		if state.bids[i].NodeID == bid.NodeID {
			state.bids[i] = bid
			replaced = true
			break
		}
	}
	if !replaced {
		state.bids = append(state.bids, bid)
	}
	state.undecided = true
	if !now.Before(state.windowEnd) {
		decided = append(decided, g.decideLocked(state, now))
	}
}

func (g *Gateway) issueAssignmentLocked(state *offerState, bid *task.Bid) {
	t := state.task
	delete(g.open, t.ID)
	now := g.clock.Now()
	admission, admitted := g.admissionsByTask[t]
	if !admitted {
		return
	}
	expires := now.Add(g.config.LeaseDuration)
	lease, err := g.store.Award(g.storeCtx, taskstore.Award{
		TaskID: t.ID, NodeID: bid.NodeID, Attempt: t.Attempt, At: now, ExpiresAt: expires,
		TakeoverUntil: expires.Add(g.config.TakeoverWindow), Supersedes: admission.supersedes,
	}, g.signer)
	if err != nil {
		// The award committed nothing. A store that answers the task ended,
		// unknown, or a takeover stale ends the gateway's admission; any other
		// failure (a signing failure among them) offers the task again.
		if errors.Is(err, taskstore.ErrEnded) || errors.Is(err, taskstore.ErrUnknownTask) || errors.Is(err, taskstore.ErrStale) {
			t.Status = task.TaskStatusFailed
			g.releaseAdmissionLocked(t)
			return
		}
		g.requeueLocked(t)
		return
	}
	admission.supersedes, admission.takeoverUntil = nil, time.Time{}
	assignment := g.assignmentFor(t, lease)
	state.task.Status = task.TaskStatusAssigned
	g.inFlight[t.ID] = &assignmentState{
		task:       t,
		assignment: assignment,
		deadline:   lease.Terms.ExpiresAt,
		attempt:    t.Attempt,
		lease:      lease,
	}
	g.lastWin[bid.NodeID] = now
	// Prune lastWin entries older than the recent-win window so the map
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
		// The assignments channel is blocked, so the winner never receives its
		// lease: the lease is released in the store and the task offered again.
		delete(g.inFlight, t.ID)
		id := lease.Identity()
		if err := g.store.Release(g.storeCtx, taskstore.Release{TaskID: t.ID, Lease: &id, At: now,
			Cause: taskstore.CauseLeaseExpired}); err != nil {
			t.Status = task.TaskStatusFailed
			g.releaseAdmissionLocked(t)
			return
		}
		g.requeueLocked(t)
	}
}

// RenewalSink accepts lease holders' renewals.
func (g *Gateway) RenewalSink() chan<- task.Renewal { return g.renewals }

// handleRenewal renews the lease version a holder names through the store,
// to a fresh expiry and takeover window, and sends the new version on the
// assignments channel. A renewal from another node, of a superseded version,
// or of an expired lease changes nothing.
func (g *Gateway) handleRenewal(rn task.Renewal) {
	g.mu.Lock()
	defer g.mu.Unlock()
	state, ok := g.inFlight[rn.TaskID]
	if !ok || state.lease == nil || rn.Fence != state.assignment.Fence || rn.NodeID != state.assignment.Primary {
		return
	}
	now := g.clock.Now()
	if now.After(state.deadline) {
		return
	}
	expires := now.Add(g.config.LeaseDuration)
	lease, err := g.store.Renew(g.storeCtx, taskstore.Renewal{
		Lease: state.lease.Identity(), At: now, ExpiresAt: expires, TakeoverUntil: expires.Add(g.config.TakeoverWindow),
	}, g.signer)
	if err != nil {
		return
	}
	state.lease = lease
	state.deadline = lease.Terms.ExpiresAt
	state.assignment = g.assignmentFor(state.task, lease)
	select {
	case g.assignments <- state.assignment:
	default:
		// The holder keeps the previous version until its next renewal.
	}
}

// leaseOf is the identity of the lease version state holds, nil for a state
// built without the store.
func leaseOf(state *assignmentState) *task.LeaseIdentity {
	if state.lease == nil {
		return nil
	}
	id := state.lease.Identity()
	return &id
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
		case rn := <-g.renewals:
			g.handleRenewal(rn)
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
	if result.NodeID != "" && result.NodeID != state.assignment.Primary {
		return
	}
	lease := leaseOf(state)
	delete(g.inFlight, result.TaskID)
	switch result.Status {
	case task.ResultStatusOK:
		if g.verifier != nil {
			if state.lease == nil || g.verifier.VerifyReceipt(g.storeCtx, *state.lease, result) != nil {
				g.failAttemptLocked(state, lease, taskstore.CauseReceiptRefused)
				return
			}
		}
		g.finishLocked(state.task, lease, taskstore.OutcomeCompleted, "", result.Receipt)
	case task.ResultStatusRejected:
		// Authorization and partition failures are deterministic for this admitted
		// object. Retrying them cannot create authority and only amplifies load.
		g.finishLocked(state.task, lease, taskstore.OutcomeRejected, taskstore.CauseExecutorRejected, nil)
	default:
		g.failAttemptLocked(state, lease, taskstore.CauseAttemptFailed)
	}
}

// failAttemptLocked ends one attempt: the lease is released in the store and
// the task retried as its next attempt while MaxAttempts allows, and finished
// as OutcomeFailed once it does not. state.task.Attempt+1 is what
// CloneForRetry sets the retry's Attempt to, so the bound is the one
// scheduleRetryLocked applies.
func (g *Gateway) failAttemptLocked(state *assignmentState, lease *task.LeaseIdentity, cause string) {
	if state.task.Attempt+1 > g.config.MaxAttempts {
		g.finishLocked(state.task, lease, taskstore.OutcomeFailed, taskstore.CauseAttemptsExhausted, nil)
		return
	}
	state.task.Status = task.TaskStatusFailed
	if err := g.store.Release(g.storeCtx, taskstore.Release{TaskID: state.task.ID, Lease: lease,
		At: g.clock.Now(), Cause: cause}); err != nil {
		g.releaseAdmissionLocked(state.task)
		return
	}
	g.scheduleRetryLocked(state.task)
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
		if !now.After(state.deadline) {
			continue
		}
		delete(g.inFlight, id)
		lease := leaseOf(state)
		if lease != nil && g.config.TakeoverWindow > 0 {
			// The holder stopped renewing: the task is offered again for a
			// takeover of this lease until its TakeoverUntil.
			admission, ok := g.admissionsByTask[state.task]
			if !ok {
				continue
			}
			admission.supersedes, admission.takeoverUntil = lease, state.lease.Terms.TakeoverUntil
			g.requeueLocked(state.task)
			continue
		}
		if err := g.store.Release(g.storeCtx, taskstore.Release{TaskID: state.task.ID, Lease: lease, At: now,
			Cause: taskstore.CauseLeaseExpired}); err != nil {
			state.task.Status = task.TaskStatusFailed
			g.releaseAdmissionLocked(state.task)
			continue
		}
		g.scheduleRetryLocked(state.task)
	}
	for t, admission := range g.admissionsByTask {
		if admission.supersedes == nil || !now.After(admission.takeoverUntil) {
			continue
		}
		if _, leased := g.inFlight[t.ID]; leased {
			continue
		}
		lapsed := *admission.supersedes
		g.finishLocked(t, &lapsed, taskstore.OutcomeReaped, taskstore.CauseTakeoverWindowElapsed, nil)
	}
}

func (g *Gateway) runWaitSweeper(ctx context.Context) {
	ticker := time.NewTicker(g.config.WaitSweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-g.closing:
			return
		case <-ticker.C:
			g.sweepWaiting()
		}
	}
}

// sweepWaiting visits every admitted task that holds no lease and is not
// awaiting a takeover: one waiting past MaxWait ends as OutcomeAbandoned
// with CauseMaxWaitElapsed; every other is put to the store's KeepWaiting,
// which keeps it (renewing what the job kind holds for it) or ends the wait
// with its own cause. The sweep runs on its own ticker, so a waiting task is
// visited whether or not an offer of it is open.
func (g *Gateway) sweepWaiting() {
	g.mu.Lock()
	defer g.mu.Unlock()
	now := g.clock.Now()
	for t, admission := range g.admissionsByTask {
		if admission.supersedes != nil {
			continue
		}
		if _, leased := g.inFlight[t.ID]; leased {
			continue
		}
		if g.config.MaxWait > 0 && now.Sub(admission.admittedAt) > g.config.MaxWait {
			g.finishLocked(t, nil, taskstore.OutcomeAbandoned, taskstore.CauseMaxWaitElapsed, nil)
			continue
		}
		verdict, err := g.store.KeepWaiting(g.storeCtx, t.ID, now)
		if err != nil {
			if errors.Is(err, taskstore.ErrEnded) || errors.Is(err, taskstore.ErrUnknownTask) {
				t.Status = task.TaskStatusFailed
				g.releaseAdmissionLocked(t)
			}
			continue
		}
		if !verdict.Keep {
			g.finishLocked(t, nil, taskstore.OutcomeAbandoned, verdict.Cause, nil)
		}
	}
}

// scheduleRetryLocked clones a gateway-owned task and transfers its private
// admission record to that exact clone. It never re-enters the public enqueue
// path, so a retry cannot impersonate a fresh producer admission.
func (g *Gateway) scheduleRetryLocked(original *task.Task) {
	clone := original.CloneForRetry()
	if clone == nil || clone.Attempt > g.config.MaxAttempts {
		g.finishLocked(original, nil, taskstore.OutcomeFailed, taskstore.CauseAttemptsExhausted, nil)
		return
	}
	admission, ok := g.admissionsByTask[original]
	if !ok {
		return
	}
	delete(g.admissionsByTask, original)
	admission.current = clone
	admission.pinnedID = clone.ID
	admission.pinnedPartitionID = clone.Partition
	g.admissionsByTask[clone] = admission
	time.AfterFunc(g.config.RequeueDelay, func() {
		g.mu.Lock()
		defer g.mu.Unlock()
		if g.stopped {
			clone.Status = task.TaskStatusCanceled
			g.releaseAdmissionLocked(clone)
			return
		}
		if _, admitted := g.admissionsByTask[clone]; !admitted {
			return
		}
		g.requeueLocked(clone)
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
	if admission.partitionKey.Valid() {
		if remaining := g.partitionOutstanding[admission.partitionKey] - 1; remaining > 0 {
			g.partitionOutstanding[admission.partitionKey] = remaining
		} else {
			delete(g.partitionOutstanding, admission.partitionKey)
		}
	}
}
