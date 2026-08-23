/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package gateway

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	task "github.com/bbmumford/loom/core/task"
	"github.com/bbmumford/loom/ports"
)

type gatewayPrincipalKey struct{}

type gatewayPrincipal struct {
	owner ports.ExecutionOwnerKey
}

func (p gatewayPrincipal) OwnerKey() ports.ExecutionOwnerKey { return p.owner }

func (p gatewayPrincipal) AuthorizeExecution(
	ctx context.Context,
) (context.Context, func(), error) {
	if !p.owner.Valid() {
		return ctx, nil, fmt.Errorf("principal owner is invalid")
	}
	return context.WithValue(
		ctx,
		gatewayPrincipalKey{},
		ports.ExecutionPrincipal(p),
	), func() {}, nil
}

type gatewayPrincipalReader struct{}

func (gatewayPrincipalReader) ExecutionPrincipal(ctx context.Context) (ports.ExecutionPrincipal, bool) {
	principal, ok := ctx.Value(gatewayPrincipalKey{}).(ports.ExecutionPrincipal)
	return principal, ok && principal != nil && principal.OwnerKey().Valid()
}

func gatewayPrincipalFor(t *testing.T, canonical string) gatewayPrincipal {
	t.Helper()
	owner, err := ports.NewExecutionOwnerKey(canonical)
	if err != nil {
		t.Fatalf("NewExecutionOwnerKey: %v", err)
	}
	return gatewayPrincipal{owner: owner}
}

func gatewayPrincipalContext(principal gatewayPrincipal) context.Context {
	return context.WithValue(
		context.Background(),
		gatewayPrincipalKey{},
		ports.ExecutionPrincipal(principal),
	)
}

type blockingGatewayPrincipal struct {
	owner   ports.ExecutionOwnerKey
	entered chan struct{}
	release <-chan struct{}
}

func (p *blockingGatewayPrincipal) OwnerKey() ports.ExecutionOwnerKey { return p.owner }

func (p *blockingGatewayPrincipal) AuthorizeExecution(
	ctx context.Context,
) (context.Context, func(), error) {
	close(p.entered)
	select {
	case <-p.release:
	case <-ctx.Done():
		return ctx, nil, ctx.Err()
	}
	return context.WithValue(
		ctx,
		gatewayPrincipalKey{},
		ports.ExecutionPrincipal(p),
	), func() {}, nil
}

func TestEnqueueAuthorizedBindsOnlyTheExactTaskPointer(t *testing.T) {
	g, err := NewGateway(
		GatewayConfig{ID: "authority-test"},
		WithExecutionPrincipalReader(gatewayPrincipalReader{}),
	)
	if err != nil {
		t.Fatalf("NewGateway: %v", err)
	}
	defer g.Stop()

	principal := gatewayPrincipalFor(t, "realm=test|tenant=app|org=owner|kind=user|id=7|space=default")
	authorized := &task.Task{ID: "same-id", TenantID: "mutable-body-value"}
	if err := g.EnqueueAuthorized(gatewayPrincipalContext(principal), authorized); err != nil {
		t.Fatalf("EnqueueAuthorized: %v", err)
	}
	if owner, ok := g.EstablishedExecutionOwner(authorized); !ok || owner != principal.owner {
		t.Fatalf("authorized binding = (%s, %v), want (%s, true)",
			owner.Fingerprint(), ok, principal.owner.Fingerprint())
	}

	collision := *authorized
	if owner, ok := g.EstablishedExecutionOwner(&collision); ok || owner.Valid() {
		t.Fatalf("same-ID copy binding = (%s, %v), want empty/false", owner.Fingerprint(), ok)
	}

	bodyOnly := &task.Task{ID: "body-only", TenantID: "owner"}
	if err := g.Enqueue(bodyOnly); err != nil {
		t.Fatalf("legacy Enqueue: %v", err)
	}
	if owner, ok := g.EstablishedExecutionOwner(bodyOnly); ok || owner.Valid() {
		t.Fatalf("body-only binding = (%s, %v), want empty/false", owner.Fingerprint(), ok)
	}

	// Mutable body tenant data neither selects nor vetoes the opaque owner.
	// Product authorization—not this generic gateway—owns that policy.
	foreign := &task.Task{ID: "foreign", TenantID: "other"}
	err = g.EnqueueAuthorized(gatewayPrincipalContext(principal), foreign)
	if err != nil {
		t.Fatalf("body claim influenced owner admission: %v", err)
	}
	if owner, ok := g.EstablishedExecutionOwner(foreign); !ok || owner != principal.owner {
		t.Fatal("authorized task did not retain the established opaque owner")
	}

	missing := &task.Task{ID: "missing", TenantID: "owner"}
	err = g.EnqueueAuthorized(context.Background(), missing)
	if err == nil || !strings.Contains(err.Error(), "established execution principal") {
		t.Fatalf("missing principal error = %v, want fail-closed error", err)
	}
}

func TestAuthorizedBindingMovesToRetryCloneAndReleasesAtTerminal(t *testing.T) {
	g, err := NewGateway(
		GatewayConfig{
			ID:           "retry-authority-test",
			MaxAttempts:  1,
			RequeueDelay: time.Millisecond,
		},
		WithExecutionPrincipalReader(gatewayPrincipalReader{}),
	)
	if err != nil {
		t.Fatalf("NewGateway: %v", err)
	}
	defer g.Stop()

	principal := gatewayPrincipalFor(t, "realm=test|tenant=app|org=owner|kind=service|id=retry|space=default")
	original := &task.Task{ID: "retry-task", TenantID: "body"}
	if err := g.EnqueueAuthorized(gatewayPrincipalContext(principal), original); err != nil {
		t.Fatalf("EnqueueAuthorized: %v", err)
	}
	// Remove the initial queue entry and model a matching in-flight assignment.
	owned := g.queue.Pop()
	if owned == nil || owned == original {
		t.Fatalf("queued pointer = %p, want a gateway-owned snapshot distinct from %p", owned, original)
	}
	g.inFlight[owned.ID] = &assignmentState{
		task: owned,
		assignment: task.Assignment{
			Task:   owned,
			TaskID: owned.ID,
			Fence:  "fence-1",
		},
	}
	g.handleResult(task.Result{
		TaskID: owned.ID,
		Fence:  "fence-1",
		Status: task.ResultStatusError,
	})

	if _, ok := g.EstablishedExecutionOwner(owned); ok {
		t.Fatal("retry left authority attached to the superseded gateway snapshot")
	}

	var retry *task.Task
	deadline := time.Now().Add(time.Second)
	for retry == nil && time.Now().Before(deadline) {
		retry = g.queue.Pop()
		if retry == nil {
			time.Sleep(time.Millisecond)
		}
	}
	if retry == nil {
		t.Fatal("retry clone was not enqueued")
	}
	if retry == original || retry == owned || retry.Attempt != 1 {
		t.Fatalf("retry = (%p, attempt %d), want distinct pointer attempt 1", retry, retry.Attempt)
	}
	if owner, ok := g.EstablishedExecutionOwner(retry); !ok || owner != principal.owner {
		t.Fatalf("retry binding = (%s, %v), want (%s, true)",
			owner.Fingerprint(), ok, principal.owner.Fingerprint())
	}

	// A deterministic authorization rejection is terminal and must release
	// rather than retrying or retaining the private principal.
	g.inFlight[retry.ID] = &assignmentState{
		task: retry,
		assignment: task.Assignment{
			Task:   retry,
			TaskID: retry.ID,
			Fence:  "fence-2",
		},
	}
	g.handleResult(task.Result{
		TaskID: retry.ID,
		Fence:  "fence-2",
		Status: task.ResultStatusRejected,
	})
	if _, ok := g.EstablishedExecutionOwner(retry); ok {
		t.Fatal("terminal rejection retained a private principal")
	}
	if got := g.queue.Pop(); got != nil {
		t.Fatalf("terminal rejection scheduled unexpected retry: %+v", got)
	}
}

func TestStopReleasesAuthorizedBindings(t *testing.T) {
	g, err := NewGateway(
		GatewayConfig{ID: "stop-authority-test"},
		WithExecutionPrincipalReader(gatewayPrincipalReader{}),
	)
	if err != nil {
		t.Fatalf("NewGateway: %v", err)
	}

	principal := gatewayPrincipalFor(t, "realm=test|tenant=app|org=owner|kind=service|id=stop|space=default")
	admitted := &task.Task{ID: "stop-task", TenantID: "body"}
	if err := g.EnqueueAuthorized(gatewayPrincipalContext(principal), admitted); err != nil {
		t.Fatalf("EnqueueAuthorized: %v", err)
	}
	g.Stop()
	if _, ok := g.EstablishedExecutionOwner(admitted); ok {
		t.Fatal("Stop retained an authorized task binding")
	}
	if err := g.EnqueueAuthorized(gatewayPrincipalContext(principal), &task.Task{
		ID:       "after-stop",
		TenantID: "owner",
	}); err == nil || !strings.Contains(err.Error(), "gateway stopped") {
		t.Fatalf("enqueue after Stop = %v, want stopped error", err)
	}
}

func TestAuthorizedAdmissionIsBoundedIndependentlyPerOpaqueOwner(t *testing.T) {
	g, err := NewGateway(
		GatewayConfig{
			ID:                     "owner-limit-test",
			MaxOutstandingPerOwner: 1,
		},
		WithExecutionPrincipalReader(gatewayPrincipalReader{}),
	)
	if err != nil {
		t.Fatalf("NewGateway: %v", err)
	}
	defer g.Stop()

	firstOwner := gatewayPrincipalFor(t, "realm=test|tenant=app|org=one|kind=service|id=w|space=ops")
	secondOwner := gatewayPrincipalFor(t, "realm=test|tenant=app|org=two|kind=service|id=w|space=ops")
	first := &task.Task{ID: "first"}
	if err := g.EnqueueAuthorized(gatewayPrincipalContext(firstOwner), first); err != nil {
		t.Fatalf("first owner admission: %v", err)
	}
	if err := g.EnqueueAuthorized(gatewayPrincipalContext(firstOwner), &task.Task{ID: "over-limit"}); err == nil ||
		!strings.Contains(err.Error(), "outstanding limit") {
		t.Fatalf("same-owner second admission = %v, want outstanding limit", err)
	}
	if err := g.EnqueueAuthorized(gatewayPrincipalContext(secondOwner), &task.Task{ID: "other-owner"}); err != nil {
		t.Fatalf("independent owner was coupled to first owner's limit: %v", err)
	}
	if err := g.EnqueueAuthorized(gatewayPrincipalContext(firstOwner), first); err == nil ||
		!strings.Contains(err.Error(), "already admitted") {
		t.Fatalf("duplicate exact pointer admission = %v, want rejection", err)
	}

	g.mu.Lock()
	g.releaseAdmissionLocked(g.admissionsBySubmitted[first].current)
	g.mu.Unlock()
	if err := g.EnqueueAuthorized(gatewayPrincipalContext(firstOwner), &task.Task{ID: "after-release"}); err != nil {
		t.Fatalf("owner slot did not settle after terminal release: %v", err)
	}
}

func TestAdmissionOwnsSnapshotAndRejectsAllExternalPointerReuse(t *testing.T) {
	g, err := NewGateway(
		GatewayConfig{ID: "snapshot-authority-test"},
		WithExecutionPrincipalReader(gatewayPrincipalReader{}),
	)
	if err != nil {
		t.Fatalf("NewGateway: %v", err)
	}
	defer g.Stop()

	principal := gatewayPrincipalFor(t, "realm=test|tenant=app|org=one|kind=user|id=7|space=ops")
	submitted := &task.Task{
		ID:       "immutable-id",
		TenantID: "body-tenant",
		Payload:  []byte(`{"safe":true}`),
	}
	if err := g.EnqueueAuthorized(gatewayPrincipalContext(principal), submitted); err != nil {
		t.Fatalf("EnqueueAuthorized: %v", err)
	}
	if err := g.Enqueue(submitted); err == nil || !strings.Contains(err.Error(), "already admitted") {
		t.Fatalf("compatibility re-enqueue = %v, want already admitted", err)
	}
	if err := g.EnqueueAuthorized(gatewayPrincipalContext(principal), submitted); err == nil ||
		!strings.Contains(err.Error(), "already admitted") {
		t.Fatalf("authorized re-enqueue = %v, want already admitted", err)
	}

	submitted.ID = "caller-mutated-id"
	submitted.TenantID = "caller-mutated-tenant"
	submitted.Payload[0] = '['
	owned := g.queue.Pop()
	if owned == nil {
		t.Fatal("gateway-owned snapshot missing")
	}
	if owned == submitted {
		t.Fatal("queue retained caller-owned pointer")
	}
	if owned.ID != "immutable-id" || owned.TenantID != "body-tenant" ||
		string(owned.Payload) != `{"safe":true}` {
		t.Fatalf("owned snapshot changed with caller: %+v payload=%s", owned, owned.Payload)
	}

	if _, accepted := g.queue.Enqueue(owned); !accepted {
		t.Fatal("could not restore owned snapshot for offer test")
	}
	g.produceOffers()
	offer := <-g.offers
	if offer.Task == owned {
		t.Fatal("offer exposed gateway-owned pointer")
	}
	offer.Task.ID = "offer-mutated-id"
	offer.Task.TenantID = "offer-mutated-tenant"
	state := g.open["immutable-id"]
	if state == nil || state.task.ID != "immutable-id" || state.task.TenantID != "body-tenant" {
		t.Fatalf("offer mutation reached gateway state: %+v", state)
	}
}

func TestAssignmentIdentityMutationRejectsBeforePrincipalAuthorization(t *testing.T) {
	g, err := NewGateway(
		GatewayConfig{ID: "assignment-mutation-test"},
		WithExecutionPrincipalReader(gatewayPrincipalReader{}),
	)
	if err != nil {
		t.Fatalf("NewGateway: %v", err)
	}
	defer g.Stop()

	principal := gatewayPrincipalFor(t, "realm=test|tenant=app|org=one|kind=service|id=w|space=ops")
	submitted := &task.Task{ID: "pinned-id", TenantID: "pinned-tenant"}
	if err := g.EnqueueAuthorized(gatewayPrincipalContext(principal), submitted); err != nil {
		t.Fatalf("EnqueueAuthorized: %v", err)
	}
	owned := g.queue.Pop()
	owned.ID = "mutated-id"
	if _, _, _, err := g.AuthorizeAssignment(context.Background(), owned); err == nil ||
		!strings.Contains(err.Error(), "identity changed") {
		t.Fatalf("mutated assignment authorization = %v, want identity rejection", err)
	}
}

func TestQueueRejectionRollsBackOwnerAndPointerAdmission(t *testing.T) {
	g, err := NewGateway(
		GatewayConfig{
			ID:                     "queue-bound-test",
			MaxQueueDepth:          1,
			MaxOutstandingPerOwner: 2,
		},
		WithExecutionPrincipalReader(gatewayPrincipalReader{}),
	)
	if err != nil {
		t.Fatalf("NewGateway: %v", err)
	}
	defer g.Stop()

	principal := gatewayPrincipalFor(t, "realm=test|tenant=app|org=one|kind=service|id=q|space=ops")
	first := &task.Task{ID: "queue-first"}
	second := &task.Task{ID: "queue-second"}
	if err := g.EnqueueAuthorized(gatewayPrincipalContext(principal), first); err != nil {
		t.Fatalf("first admission: %v", err)
	}
	if err := g.EnqueueAuthorized(gatewayPrincipalContext(principal), second); err == nil ||
		!strings.Contains(err.Error(), "queue is full") {
		t.Fatalf("second admission = %v, want queue-full rejection", err)
	}
	if _, ok := g.EstablishedExecutionOwner(second); ok {
		t.Fatal("queue rejection stranded a private owner binding")
	}
	if got := g.ownerOutstanding[principal.owner]; got != 1 {
		t.Fatalf("owner outstanding = %d, want 1 after rollback", got)
	}

	firstOwned := g.queue.Pop()
	g.mu.Lock()
	firstOwned.Status = task.TaskStatusCompleted
	g.releaseAdmissionLocked(firstOwned)
	g.mu.Unlock()
	if err := g.EnqueueAuthorized(gatewayPrincipalContext(principal), second); err != nil {
		t.Fatalf("rolled-back pointer could not be admitted after capacity cleared: %v", err)
	}
}

func TestStartContextCancellationConvergesWithStop(t *testing.T) {
	g, err := NewGateway(
		GatewayConfig{ID: "context-stop-test"},
		WithExecutionPrincipalReader(gatewayPrincipalReader{}),
	)
	if err != nil {
		t.Fatalf("NewGateway: %v", err)
	}
	principal := gatewayPrincipalFor(t, "realm=test|tenant=app|org=one|kind=service|id=stop|space=ops")
	admitted := &task.Task{ID: "cancelled-gateway-task"}
	if err := g.EnqueueAuthorized(gatewayPrincipalContext(principal), admitted); err != nil {
		t.Fatalf("EnqueueAuthorized: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	g.Start(ctx)
	cancel()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		g.mu.Lock()
		stopped := g.stopped
		g.mu.Unlock()
		if stopped {
			break
		}
		time.Sleep(time.Millisecond)
	}
	g.mu.Lock()
	stopped := g.stopped
	g.mu.Unlock()
	if !stopped {
		t.Fatal("context cancellation did not stop the gateway")
	}
	if _, ok := g.EstablishedExecutionOwner(admitted); ok {
		t.Fatal("context cancellation retained an authorized admission")
	}
	if err := g.EnqueueAuthorized(gatewayPrincipalContext(principal), &task.Task{ID: "after-cancel"}); err == nil ||
		!strings.Contains(err.Error(), "gateway stopped") {
		t.Fatalf("enqueue after context cancellation = %v, want stopped", err)
	}
}

func TestStopDuringBlockingAuthorizationInvalidatesTheResult(t *testing.T) {
	g, err := NewGateway(
		GatewayConfig{ID: "authorize-stop-race-test"},
		WithExecutionPrincipalReader(gatewayPrincipalReader{}),
	)
	if err != nil {
		t.Fatalf("NewGateway: %v", err)
	}
	owner, err := ports.NewExecutionOwnerKey(
		"realm=test|tenant=app|org=one|kind=service|id=blocked|space=ops",
	)
	if err != nil {
		t.Fatalf("NewExecutionOwnerKey: %v", err)
	}
	release := make(chan struct{})
	principal := &blockingGatewayPrincipal{
		owner:   owner,
		entered: make(chan struct{}),
		release: release,
	}
	admissionCtx := context.WithValue(
		context.Background(),
		gatewayPrincipalKey{},
		ports.ExecutionPrincipal(principal),
	)
	submitted := &task.Task{ID: "blocked-authorization", TenantID: "body"}
	if err := g.EnqueueAuthorized(admissionCtx, submitted); err != nil {
		t.Fatalf("EnqueueAuthorized: %v", err)
	}
	owned := g.queue.Pop()
	if owned == nil {
		t.Fatal("gateway-owned task snapshot missing")
	}

	authorized := make(chan error, 1)
	go func() {
		_, _, releaseAuthorization, err := g.AuthorizeAssignment(
			context.Background(),
			owned,
		)
		if releaseAuthorization != nil {
			releaseAuthorization()
		}
		authorized <- err
	}()
	<-principal.entered
	g.Stop()
	close(release)
	err = <-authorized
	if err == nil || !strings.Contains(err.Error(), "gateway stopped") {
		t.Fatalf("authorization after concurrent Stop = %v, want stopped rejection", err)
	}
}
