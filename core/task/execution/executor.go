/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package execution

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	meshtask "github.com/bbmumford/loom/core/task"
	"github.com/bbmumford/loom/core/task/gateway"
	"github.com/bbmumford/loom/internal/securityctx"
	"github.com/bbmumford/loom/node/handlers"
	"github.com/bbmumford/loom/ports"
)

// Executor listens to Gateway offers, bids on tasks, executes assignments,
// and returns results. It acts as the bridge between the async task system
// and synchronous RPC handler execution.
type Executor struct {
	nodeID       string                    // This executor's node ID
	gateway      *gateway.Gateway          // Task gateway to interact with
	registry     *handlers.HandlerRegistry // RPC handler registry (shared with Bridge)
	roles        []string                  // Roles this executor can handle
	capacityFunc func() float64            // Dynamic capacity evaluation (0.0-1.0)
	auth         ports.AuthValidator       // Pre-execution auth gate (fail-closed default)

	// Bidding configuration
	bidTimeout    time.Duration // How long to wait before bidding
	maxConcurrent int           // Maximum concurrent task executions

	// Internal state
	mu       sync.Mutex
	active   map[string]*executionState
	shutdown chan struct{}
}

// executionState tracks an in-progress task execution
type executionState struct {
	assignment meshtask.Assignment
	startedAt  time.Time
	cancel     context.CancelFunc
}

// ExecutorConfig configures task executor behavior
type ExecutorConfig struct {
	NodeID        string         // Executor's unique identifier
	Roles         []string       // Roles this executor handles (e.g., ["identity", "auth"])
	CapacityFunc  func() float64 // Returns current capacity (0.0-1.0)
	BidTimeout    time.Duration  // Delay before sending bid (default: 50ms)
	MaxConcurrent int            // Max parallel executions (default: 10)

	// AuthValidator gates handler execution (RequiresAuth / auth types /
	// scopes) before a task runs. nil → loom-local fail-closed validator,
	// which denies every RequiresAuth handler because nothing writes its
	// context keys. Platform builds (HSTLES) must inject a validator
	// delegating to their real security helpers.
	AuthValidator ports.AuthValidator
}

// DefaultExecutorConfig returns sensible defaults
func DefaultExecutorConfig() ExecutorConfig {
	return ExecutorConfig{
		NodeID:        "executor-default",
		Roles:         []string{},
		CapacityFunc:  func() float64 { return 0.8 }, // 80% capacity
		BidTimeout:    50 * time.Millisecond,
		MaxConcurrent: 10,
	}
}

// NewExecutor creates a new task executor
func NewExecutor(gateway *gateway.Gateway, registry *handlers.HandlerRegistry, cfg ExecutorConfig) (*Executor, error) {
	if gateway == nil {
		return nil, fmt.Errorf("executor: gateway is nil")
	}
	if registry == nil {
		return nil, fmt.Errorf("executor: handler registry is nil")
	}
	if cfg.NodeID == "" {
		cfg.NodeID = DefaultExecutorConfig().NodeID
	}
	if cfg.CapacityFunc == nil {
		cfg.CapacityFunc = DefaultExecutorConfig().CapacityFunc
	}
	if cfg.BidTimeout == 0 {
		cfg.BidTimeout = DefaultExecutorConfig().BidTimeout
	}
	if cfg.MaxConcurrent == 0 {
		cfg.MaxConcurrent = DefaultExecutorConfig().MaxConcurrent
	}
	if cfg.AuthValidator == nil {
		cfg.AuthValidator = securityctx.Default()
	}

	return &Executor{
		nodeID:        cfg.NodeID,
		gateway:       gateway,
		registry:      registry,
		roles:         cfg.Roles,
		capacityFunc:  cfg.CapacityFunc,
		bidTimeout:    cfg.BidTimeout,
		maxConcurrent: cfg.MaxConcurrent,
		auth:          cfg.AuthValidator,
		active:        make(map[string]*executionState),
		shutdown:      make(chan struct{}),
	}, nil
}

// Start begins listening to offers and assignments
// This should be called in a goroutine
func (e *Executor) Start(ctx context.Context) {
	log.Printf("[TaskExecutor] Starting executor: %s (roles: %v)", e.nodeID, e.roles)

	var wg sync.WaitGroup

	// Goroutine 1: Listen to offers and send bids
	wg.Add(1)
	go func() {
		defer wg.Done()
		e.runOfferLoop(ctx)
	}()

	// Goroutine 2: Listen to assignments and execute tasks
	wg.Add(1)
	go func() {
		defer wg.Done()
		e.runAssignmentLoop(ctx)
	}()

	// Wait for context cancellation
	<-ctx.Done()
	close(e.shutdown)

	// Wait for goroutines to finish
	wg.Wait()
	log.Printf("[TaskExecutor] Executor stopped: %s", e.nodeID)
}

// runOfferLoop listens to task offers and sends bids
func (e *Executor) runOfferLoop(ctx context.Context) {
	offers := e.gateway.Offers()

	for {
		select {
		case <-ctx.Done():
			return
		case <-e.shutdown:
			return
		case offer, ok := <-offers:
			if !ok {
				log.Printf("[TaskExecutor] Offer channel closed")
				return
			}

			// Evaluate if we should bid on this task
			if e.shouldBid(offer) {
				go e.sendBid(ctx, offer)
			}
		}
	}
}

// shouldBid determines if this executor should bid on the offered task
func (e *Executor) shouldBid(offer meshtask.TaskOffer) bool {
	// Check if task role matches our capabilities
	roleMatch := false
	for _, role := range e.roles {
		if offer.Task.Role == role {
			roleMatch = true
			break
		}
	}
	if !roleMatch {
		return false
	}

	// Check if we have capacity
	e.mu.Lock()
	currentLoad := len(e.active)
	e.mu.Unlock()

	if currentLoad >= e.maxConcurrent {
		dbgTaskExec.Printf("At capacity, skipping task %s", offer.Task.ID)
		return false
	}

	// Check if we have the handler
	handlerName := e.getHandlerName(offer.Task)
	if _, ok := e.registry.GetMeta(handlerName); !ok {
		dbgTaskExec.Printf("No handler for %s, skipping task %s", handlerName, offer.Task.ID)
		return false
	}

	return true
}

// getHandlerName extracts the RPC handler name from task metadata
// Format: task.Tags[0] should contain handler name (e.g., "identity.getUser")
func (e *Executor) getHandlerName(task *meshtask.Task) string {
	if len(task.Tags) > 0 {
		return task.Tags[0]
	}
	// Fallback: role.type (e.g., "identity.getUser")
	return fmt.Sprintf("%s.%s", task.Role, task.Type)
}

// sendBid creates and sends a bid for the task offer
func (e *Executor) sendBid(ctx context.Context, offer meshtask.TaskOffer) {
	// Small delay to avoid thundering herd
	select {
	case <-time.After(e.bidTimeout):
	case <-ctx.Done():
		return
	case <-e.shutdown:
		return
	}

	// Calculate our bid
	capacity := e.capacityFunc()
	eta := e.estimateETA(offer.Task)

	bid := meshtask.Bid{
		TaskID:        offer.Task.ID,
		NodeID:        e.nodeID,
		Score:         0.0, // Gateway will calculate score
		ETA:           eta,
		CapacityScore: capacity,
		RoleVersion:   "1.0", // Could be dynamic based on handler version
		ReceivedAt:    time.Now(),
	}

	// Send bid to gateway
	select {
	case e.gateway.BidSink() <- bid:
		dbgTaskExec.Printf("Bid sent for task %s (capacity: %.2f, ETA: %s)",
			offer.Task.ID, capacity, eta)
	case <-ctx.Done():
		return
	case <-e.shutdown:
		return
	default:
		dbgTaskExec.Printf("Bid channel full, skipping task %s", offer.Task.ID)
	}
}

// estimateETA estimates how long the task will take based on SLO
func (e *Executor) estimateETA(task *meshtask.Task) time.Duration {
	// Simple estimation: use task SLO latency as baseline
	// In production, this could use historical execution times
	if task.SLO.Latency > 0 {
		return task.SLO.Latency / 2 // Estimate half the SLO
	}
	return 1 * time.Second // Default estimate
}

// runAssignmentLoop listens to assignments and executes tasks
func (e *Executor) runAssignmentLoop(ctx context.Context) {
	assignments := e.gateway.Assignments()

	for {
		select {
		case <-ctx.Done():
			return
		case <-e.shutdown:
			return
		case assignment, ok := <-assignments:
			if !ok {
				log.Printf("[TaskExecutor] Assignment channel closed")
				return
			}

			// Check if this assignment is for us
			if assignment.Primary != e.nodeID {
				continue
			}

			dbgTaskExec.Printf("Received assignment: %s (fence: %s)",
				assignment.TaskID, assignment.Fence)

			// Execute task in goroutine
			go e.executeAssignment(ctx, assignment)
		}
	}
}

// executeAssignment executes an assigned task and sends the result
func (e *Executor) executeAssignment(ctx context.Context, assignment meshtask.Assignment) {
	// MESH-H07: reject a nil Task before spawning execution. A malformed/remote
	// assignment with Task==nil would deref task.Tags/task.Role in getHandlerName
	// and panic in this goroutine with no recover, crashing the whole process.
	if assignment.Task == nil {
		log.Printf("[TaskExecutor] rejecting assignment %s with nil Task", assignment.TaskID)
		return
	}

	// Re-authorize the product-owned principal retained against this exact
	// task pointer. This is deliberately not derivable from Task.ID,
	// Task.TenantID, tags or payload. The product principal repopulates its
	// private identity context and may reject current revocation/policy.
	execBase, expectedOwner, releaseAuthority, authorityErr := e.gateway.AuthorizeAssignment(ctx, assignment.Task)
	if releaseAuthority != nil {
		defer releaseAuthority()
	}
	if authorityErr == nil {
		reader, ok := e.auth.(ports.ExecutionPrincipalReader)
		if !ok {
			authorityErr = fmt.Errorf("auth validator cannot read an execution principal")
		} else {
			principal, established := reader.ExecutionPrincipal(execBase)
			if !established || principal == nil || !principal.OwnerKey().Valid() {
				authorityErr = fmt.Errorf("authorized context has no execution principal")
			} else if principal.OwnerKey() != expectedOwner {
				authorityErr = fmt.Errorf("authorized context owner does not match gateway admission")
			}
		}
	}

	// Create execution context with timeout.
	execCtx, cancel := context.WithTimeout(execBase, assignment.LeaseDuration)
	defer cancel()

	// Track execution. MESH-H01: capture startedAt in a local so the Stats
	// build below reads it WITHOUT touching e.active — reading the map there
	// unlocked, while other executeAssignment goroutines insert/delete under
	// e.mu, triggered Go's `concurrent map read and map write` fatal throw
	// (MaxConcurrent defaults to 10, so two in-flight tasks are enough).
	startedAt := time.Now()
	e.mu.Lock()
	e.active[assignment.TaskID] = &executionState{
		assignment: assignment,
		startedAt:  startedAt,
		cancel:     cancel,
	}
	e.mu.Unlock()

	// Cleanup on completion
	defer func() {
		e.mu.Lock()
		delete(e.active, assignment.TaskID)
		e.mu.Unlock()
	}()

	// Execute the task only after exact-pointer authority revalidation.
	var result meshtask.Result
	if authorityErr != nil {
		result = e.rejectedTaskResult(
			assignment.TaskID,
			assignment.Fence,
			"task authority rejected: "+authorityErr.Error(),
		)
	} else {
		result = e.executeTask(execCtx, assignment.Task, assignment.Fence)
	}

	// Send result to gateway
	report := meshtask.ResultReport{
		Result: result,
		Stats: map[string]any{
			"executor_id": e.nodeID,
			"duration_ms": time.Since(startedAt).Milliseconds(), // MESH-H01: local, not the shared map
		},
	}

	select {
	case e.gateway.ResultSink() <- report:
		dbgTaskExec.Printf("Result sent for task %s (status: %s)",
			assignment.TaskID, result.Status)
	case <-ctx.Done():
		log.Printf("[TaskExecutor] Context canceled, result not sent for task %s", assignment.TaskID)
	case <-e.shutdown:
		log.Printf("[TaskExecutor] Executor shutdown, result not sent for task %s", assignment.TaskID)
	}
}

// executeTask executes the actual task logic using Task execution path
func (e *Executor) executeTask(ctx context.Context, task *meshtask.Task, fence string) meshtask.Result {
	// Get handler name
	handlerName := e.getHandlerName(task)

	// Require the opaque product principal established by the gateway's
	// execution-time authorization. Task fields are mutable body claims and
	// never mint or select this owner.
	ownerReader, ok := e.auth.(ports.ExecutionPrincipalReader)
	if !ok {
		return e.rejectedTaskResult(task.ID, fence,
			"task owner rejected: auth validator cannot read an execution principal")
	}
	principal, ok := ownerReader.ExecutionPrincipal(ctx)
	if !ok || principal == nil || !principal.OwnerKey().Valid() {
		return e.rejectedTaskResult(task.ID, fence,
			"task owner rejected: established execution principal is missing")
	}

	metadata := map[string]interface{}{}
	sessionID := ""
	if tenantReader, ok := e.auth.(ports.TenantPrincipalReader); ok {
		if platformTenant, established := tenantReader.ExecutionTenantID(ctx); established &&
			platformTenant != "" {
			metadata["tenant_id"] = platformTenant
			sessionID = platformTenant
		}
	}

	// Convert mesh task to node task
	nodeTask := &handlers.Task{
		ID:          task.ID,
		Handler:     handlerName,
		Payload:     task.Payload,
		Priority:    int(task.SLO.Priority),
		Deadline:    time.Now().Add(task.SLO.Latency),
		Idempotency: task.Idempotency,
		Metadata:    metadata,
		CreatedAt:   task.CreatedAt,
		SessionID:   sessionID,
		TraceID:     task.ID, // Use task ID for tracing
	}

	// Execute via the same auth + tenant + middleware lifecycle as compose.
	dbgTaskExec.Printf("Executing handler: %s for task %s", handlerName, task.ID)
	taskResult, err := e.registry.DispatchTaskWithAuth(ctx, nodeTask, e.auth)

	if err != nil {
		return meshtask.Result{
			TaskID: task.ID,
			NodeID: e.nodeID,
			Fence:  fence,
			Status: meshtask.ResultStatusError,
			Error:  err.Error(),
			At:     time.Now(),
		}
	}
	if taskResult == nil {
		return meshtask.Result{
			TaskID: task.ID,
			NodeID: e.nodeID,
			Fence:  fence,
			Status: meshtask.ResultStatusError,
			Error:  "task handler returned nil result",
			At:     time.Now(),
		}
	}

	// Convert task result to mesh result
	var status meshtask.ResultStatus
	switch taskResult.Status {
	case handlers.TaskStatusCompleted:
		status = meshtask.ResultStatusOK
	case handlers.TaskStatusFailed:
		status = meshtask.ResultStatusError
	case handlers.TaskStatusTimeout:
		status = meshtask.ResultStatusError
	default:
		status = meshtask.ResultStatusError
	}

	return meshtask.Result{
		TaskID: task.ID,
		NodeID: e.nodeID,
		Fence:  fence,
		Status: status,
		Output: taskResult.Payload,
		Error:  taskResult.Error,
		At:     time.Now(),
	}
}

func (e *Executor) rejectedTaskResult(taskID, fence, reason string) meshtask.Result {
	return meshtask.Result{
		TaskID: taskID,
		NodeID: e.nodeID,
		Fence:  fence,
		Status: meshtask.ResultStatusRejected,
		Error:  reason,
		At:     time.Now(),
	}
}

// Stop gracefully stops the executor
func (e *Executor) Stop() {
	log.Printf("[TaskExecutor] Stopping executor: %s", e.nodeID)

	// Cancel all active executions
	e.mu.Lock()
	for _, state := range e.active {
		state.cancel()
	}
	e.mu.Unlock()
}

// ActiveTasks returns the number of currently executing tasks
func (e *Executor) ActiveTasks() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return len(e.active)
}
