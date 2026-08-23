/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package execution

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	meshtask "github.com/bbmumford/loom/core/task"
	"github.com/bbmumford/loom/core/task/gateway"
	"github.com/bbmumford/loom/node/handlers"
	"github.com/bbmumford/loom/pkg/rpc/scope"
	"github.com/bbmumford/loom/ports"
)

type executorTenantKey struct{}
type executorPrincipalKey struct{}

type executorPrincipal struct {
	owner      ports.ExecutionOwnerKey
	tenant     string
	authorized int
}

func (p *executorPrincipal) OwnerKey() ports.ExecutionOwnerKey { return p.owner }

func (p *executorPrincipal) AuthorizeExecution(
	ctx context.Context,
) (context.Context, func(), error) {
	p.authorized++
	ctx = context.WithValue(ctx, executorPrincipalKey{}, ports.ExecutionPrincipal(p))
	if p.tenant != "" {
		ctx = context.WithValue(ctx, executorTenantKey{}, p.tenant)
		ctx = scope.WithAuthenticatedIdentity(
			ctx,
			scope.AuthenticatedIdentity{PlatformTenantID: p.tenant},
		)
	}
	return ctx, func() {}, nil
}

type leasedExecutorPrincipal struct {
	owner       ports.ExecutionOwnerKey
	tenant      string
	calls       atomic.Int32
	released    chan struct{}
	releaseOnce sync.Once
}

func (p *leasedExecutorPrincipal) OwnerKey() ports.ExecutionOwnerKey {
	return p.owner
}

func (p *leasedExecutorPrincipal) AuthorizeExecution(
	ctx context.Context,
) (context.Context, func(), error) {
	call := p.calls.Add(1)
	ctx = context.WithValue(
		ctx,
		executorPrincipalKey{},
		ports.ExecutionPrincipal(p),
	)
	ctx = context.WithValue(ctx, executorTenantKey{}, p.tenant)
	ctx = scope.WithAuthenticatedIdentity(
		ctx,
		scope.AuthenticatedIdentity{PlatformTenantID: p.tenant},
	)
	return ctx, func() {
		if call == 2 {
			p.releaseOnce.Do(func() { close(p.released) })
		}
	}, nil
}

func newExecutorPrincipal(t *testing.T, canonical, tenant string) *executorPrincipal {
	t.Helper()
	owner, err := ports.NewExecutionOwnerKey(canonical)
	if err != nil {
		t.Fatalf("NewExecutionOwnerKey: %v", err)
	}
	return &executorPrincipal{owner: owner, tenant: tenant}
}

type executorAuthProbe struct {
	wantTenant string
	validated  int
	stamped    int
}

func (a *executorAuthProbe) ValidateExecutionAuth(ctx context.Context, _ ports.SecureHandler) error {
	a.validated++
	if got, _ := ctx.Value(executorTenantKey{}).(string); got != a.wantTenant {
		return fmt.Errorf("private tenant = %q, want %q", got, a.wantTenant)
	}
	return nil
}

func (a *executorAuthProbe) WithTenantID(ctx context.Context, tenantID string) context.Context {
	a.stamped++
	return context.WithValue(ctx, executorTenantKey{}, tenantID)
}

func (a *executorAuthProbe) ExecutionTenantID(ctx context.Context) (string, bool) {
	tenantID, ok := ctx.Value(executorTenantKey{}).(string)
	return tenantID, ok && tenantID != ""
}

func (a *executorAuthProbe) ExecutionPrincipal(ctx context.Context) (ports.ExecutionPrincipal, bool) {
	principal, ok := ctx.Value(executorPrincipalKey{}).(ports.ExecutionPrincipal)
	return principal, ok && principal != nil && principal.OwnerKey().Valid()
}

type executorTaskProbe struct {
	events *[]string
}

func (h *executorTaskProbe) Name() string               { return "executor.secure-task" }
func (h *executorTaskProbe) Role() string               { return "executor-test" }
func (h *executorTaskProbe) RequiresAuth() bool         { return true }
func (h *executorTaskProbe) AllowedAuthTypes() []string { return nil }
func (h *executorTaskProbe) Scopes() []string           { return nil }
func (h *executorTaskProbe) TenantScope() handlers.TenantScope {
	return handlers.TenantScopeTenant
}

type blockingExecutorTaskProbe struct {
	entered chan struct{}
	release <-chan struct{}
}

func (*blockingExecutorTaskProbe) Name() string {
	return "executor.blocking-secure-task"
}
func (*blockingExecutorTaskProbe) Role() string               { return "executor-test" }
func (*blockingExecutorTaskProbe) RequiresAuth() bool         { return true }
func (*blockingExecutorTaskProbe) AllowedAuthTypes() []string { return nil }
func (*blockingExecutorTaskProbe) Scopes() []string           { return nil }
func (*blockingExecutorTaskProbe) TenantScope() handlers.TenantScope {
	return handlers.TenantScopeTenant
}
func (*blockingExecutorTaskProbe) AllowedTenants() []string { return nil }
func (h *blockingExecutorTaskProbe) ExecuteTask(
	_ context.Context,
	task *handlers.Task,
) (*handlers.TaskResult, error) {
	close(h.entered)
	<-h.release
	return &handlers.TaskResult{
		TaskID: task.ID,
		Status: handlers.TaskStatusCompleted,
	}, nil
}
func (h *executorTaskProbe) AllowedTenants() []string { return nil }
func (h *executorTaskProbe) ExecuteTask(
	ctx context.Context,
	task *handlers.Task,
) (*handlers.TaskResult, error) {
	*h.events = append(*h.events, "handler")
	if got, _ := ctx.Value(executorTenantKey{}).(string); got == "" {
		return nil, fmt.Errorf("private tenant missing in handler")
	}
	return &handlers.TaskResult{
		TaskID: task.ID,
		Status: handlers.TaskStatusCompleted,
	}, nil
}

type executorMiddlewareProbe struct {
	events *[]string
}

func (m *executorMiddlewareProbe) Name() string { return "executor-middleware" }
func (m *executorMiddlewareProbe) Before(
	ctx context.Context,
	_ string,
	_ *handlers.RPCRequest,
) (context.Context, error) {
	*m.events = append(*m.events, "before")
	return ctx, nil
}
func (m *executorMiddlewareProbe) After(
	_ context.Context,
	_ string,
	resp *handlers.RPCResponse,
	err error,
) (*handlers.RPCResponse, error) {
	*m.events = append(*m.events, "after")
	return resp, err
}

func TestExecutorUsesCanonicalTaskDispatchPipeline(t *testing.T) {
	var events []string
	reg := handlers.NewHandlerRegistry()
	reg.Use(&executorMiddlewareProbe{events: &events})
	h := &executorTaskProbe{events: &events}
	if err := reg.RegisterTask(h); err != nil {
		t.Fatalf("RegisterTask: %v", err)
	}
	auth := &executorAuthProbe{wantTenant: "orbtr"}
	executor := &Executor{
		nodeID:   "node-test",
		registry: reg,
		auth:     auth,
	}

	principal := newExecutorPrincipal(
		t,
		"realm=test|tenant=orbtr|org=acme|kind=user|id=7|space=default",
		"orbtr",
	)
	ctx, releaseAuthorization, err := principal.AuthorizeExecution(
		context.Background(),
	)
	if err != nil {
		t.Fatalf("AuthorizeExecution: %v", err)
	}
	defer releaseAuthorization()
	result := executor.executeTask(ctx, &meshtask.Task{
		ID:        "task-1",
		TenantID:  "orbtr",
		Tags:      []string{h.Name()},
		SLO:       meshtask.TaskSLO{Latency: time.Second},
		CreatedAt: time.Now(),
	}, "fence-1")

	if result.Status != meshtask.ResultStatusOK {
		t.Fatalf("result = %+v, want OK", result)
	}
	if auth.validated != 1 {
		t.Fatalf("auth validations = %d, want 1", auth.validated)
	}
	if auth.stamped != 0 {
		t.Fatalf("WithTenantID calls = %d, want 0: task body must not mint authority", auth.stamped)
	}
	if want := []string{"before", "handler", "after"}; !reflect.DeepEqual(events, want) {
		t.Fatalf("events = %v, want %v", events, want)
	}
}

func TestExecutorDoesNotTreatMutableTaskTenantAsOwnerIdentity(t *testing.T) {
	var events []string
	reg := handlers.NewHandlerRegistry()
	reg.Use(&executorMiddlewareProbe{events: &events})
	h := &executorTaskProbe{events: &events}
	if err := reg.RegisterTask(h); err != nil {
		t.Fatalf("RegisterTask: %v", err)
	}
	auth := &executorAuthProbe{wantTenant: "platform-app"}
	executor := &Executor{nodeID: "node-test", registry: reg, auth: auth}
	principal := newExecutorPrincipal(
		t,
		"realm=test|tenant=platform-app|org=org-owner|kind=user|id=7|space=default",
		"platform-app",
	)
	ctx, releaseAuthorization, err := principal.AuthorizeExecution(
		context.Background(),
	)
	if err != nil {
		t.Fatalf("AuthorizeExecution: %v", err)
	}
	defer releaseAuthorization()

	result := executor.executeTask(ctx, &meshtask.Task{
		ID:        "task-body-axis",
		TenantID:  "mutable-and-unrelated-body-claim",
		Tags:      []string{h.Name()},
		SLO:       meshtask.TaskSLO{Latency: time.Second},
		CreatedAt: time.Now(),
	}, "fence-body-axis")

	if result.Status != meshtask.ResultStatusOK {
		t.Fatalf("result = %+v, want OK from product-established owner", result)
	}
	if auth.validated != 1 {
		t.Fatalf("auth validations = %d, want 1", auth.validated)
	}
}

func TestExecutorRejectsMissingExecutionPrincipalBeforePipeline(t *testing.T) {
	var events []string
	reg := handlers.NewHandlerRegistry()
	reg.Use(&executorMiddlewareProbe{events: &events})
	h := &executorTaskProbe{events: &events}
	if err := reg.RegisterTask(h); err != nil {
		t.Fatalf("RegisterTask: %v", err)
	}
	auth := &executorAuthProbe{wantTenant: "platform-app"}
	executor := &Executor{nodeID: "node-test", registry: reg, auth: auth}

	result := executor.executeTask(context.Background(), &meshtask.Task{
		ID:        "task-no-owner",
		TenantID:  "body-cannot-mint-owner",
		Tags:      []string{h.Name()},
		SLO:       meshtask.TaskSLO{Latency: time.Second},
		CreatedAt: time.Now(),
	}, "fence-no-owner")

	if result.Status != meshtask.ResultStatusRejected {
		t.Fatalf("result = %+v, want REJECTED", result)
	}
	if !strings.Contains(result.Error, "established execution principal is missing") {
		t.Fatalf("error = %q, want missing execution principal", result.Error)
	}
	if auth.stamped != 0 || auth.validated != 0 {
		t.Fatalf("authority pipeline ran: stamps=%d validations=%d", auth.stamped, auth.validated)
	}
	if len(events) != 0 {
		t.Fatalf("pipeline events = %v, want none", events)
	}
}

type executorAuthWithoutTenantReader struct {
	validated int
	stamped   int
}

func (a *executorAuthWithoutTenantReader) ValidateExecutionAuth(
	context.Context,
	ports.SecureHandler,
) error {
	a.validated++
	return nil
}

func (a *executorAuthWithoutTenantReader) WithTenantID(
	ctx context.Context,
	tenantID string,
) context.Context {
	a.stamped++
	return context.WithValue(ctx, executorTenantKey{}, tenantID)
}

func TestExecutorRejectsValidatorWithoutExecutionPrincipalReader(t *testing.T) {
	var events []string
	reg := handlers.NewHandlerRegistry()
	reg.Use(&executorMiddlewareProbe{events: &events})
	h := &executorTaskProbe{events: &events}
	if err := reg.RegisterTask(h); err != nil {
		t.Fatalf("RegisterTask: %v", err)
	}
	auth := &executorAuthWithoutTenantReader{}
	executor := &Executor{
		nodeID:   "node-test",
		registry: reg,
		auth:     auth,
	}

	result := executor.executeTask(
		context.WithValue(context.Background(), executorTenantKey{}, "orbtr"),
		&meshtask.Task{
			ID:        "task-no-reader",
			TenantID:  "orbtr",
			Tags:      []string{h.Name()},
			SLO:       meshtask.TaskSLO{Latency: time.Second},
			CreatedAt: time.Now(),
		},
		"fence-no-reader",
	)

	if result.Status != meshtask.ResultStatusRejected {
		t.Fatalf("result = %+v, want REJECTED", result)
	}
	if auth.stamped != 0 || auth.validated != 0 {
		t.Fatalf(
			"validator calls = (WithTenantID=%d, ValidateExecutionAuth=%d), want (0, 0)",
			auth.stamped,
			auth.validated,
		)
	}
	if len(events) != 0 {
		t.Fatalf("pipeline events = %v, want none", events)
	}
}

func TestExecutorCarriesExactGatewayPrincipalFromBackgroundContext(t *testing.T) {
	var events []string
	reg := handlers.NewHandlerRegistry()
	reg.Use(&executorMiddlewareProbe{events: &events})
	h := &executorTaskProbe{events: &events}
	if err := reg.RegisterTask(h); err != nil {
		t.Fatalf("RegisterTask: %v", err)
	}

	auth := &executorAuthProbe{wantTenant: "hstles"}
	gw, err := gateway.NewGateway(
		gateway.GatewayConfig{ID: "executor-authority-test"},
		gateway.WithExecutionPrincipalReader(auth),
	)
	if err != nil {
		t.Fatalf("NewGateway: %v", err)
	}
	defer gw.Stop()

	executor, err := NewExecutor(gw, reg, ExecutorConfig{
		NodeID:        "node-test",
		Roles:         []string{"executor-test"},
		AuthValidator: auth,
	})
	if err != nil {
		t.Fatalf("NewExecutor: %v", err)
	}

	admitted := &meshtask.Task{
		ID:        "authorized-task",
		TenantID:  "hstles",
		Role:      "executor-test",
		Tags:      []string{h.Name()},
		SLO:       meshtask.TaskSLO{Latency: time.Second},
		CreatedAt: time.Now(),
	}
	principal := newExecutorPrincipal(
		t,
		"realm=hstles|tenant=hstles|org=-|kind=service|id=node-maintenance|space=platform",
		"hstles",
	)
	admissionCtx, releaseAuthorization, err := principal.AuthorizeExecution(
		context.Background(),
	)
	if err != nil {
		t.Fatalf("AuthorizeExecution: %v", err)
	}
	defer releaseAuthorization()
	if err := gw.EnqueueAuthorized(admissionCtx, admitted); err != nil {
		t.Fatalf("EnqueueAuthorized: %v", err)
	}
	gatewayCtx, cancelGateway := context.WithCancel(context.Background())
	defer cancelGateway()
	gw.Start(gatewayCtx)
	offer := <-gw.Offers()
	if offer.Task == admitted {
		t.Fatal("offer exposed the caller-owned task pointer")
	}
	gw.BidSink() <- meshtask.Bid{
		TaskID:        offer.Task.ID,
		NodeID:        "node-test",
		CapacityScore: 1,
		ReceivedAt:    time.Now(),
	}
	assignment := <-gw.Assignments()
	executor.executeAssignment(context.Background(), assignment)
	if principal.authorized != 2 {
		t.Fatalf("principal AuthorizeExecution calls = %d, want admission setup + assignment recheck", principal.authorized)
	}
	if auth.stamped != 0 {
		t.Fatalf("WithTenantID calls = %d, want 0: gateway principal owns context binding", auth.stamped)
	}
	if auth.validated != 1 {
		t.Fatalf("auth validations = %d, want 1", auth.validated)
	}
	if want := []string{"before", "handler", "after"}; !reflect.DeepEqual(events, want) {
		t.Fatalf("authorized events = %v, want %v", events, want)
	}

	// A same-ID copy does not possess the pointer-bound admission authority.
	collision := meshtask.Task{
		ID:        "authorized-task",
		TenantID:  "hstles",
		Role:      "executor-test",
		Tags:      []string{h.Name()},
		SLO:       meshtask.TaskSLO{Latency: time.Second},
		CreatedAt: time.Now(),
	}
	events = nil
	executor.executeAssignment(context.Background(), meshtask.Assignment{
		Task:          &collision,
		TaskID:        collision.ID,
		Primary:       "node-test",
		Fence:         "fence-collision",
		LeaseDuration: time.Second,
	})
	if principal.authorized != 2 {
		t.Fatalf("same-ID copy reauthorized principal; total = %d, want 2", principal.authorized)
	}
	if auth.validated != 1 {
		t.Fatalf("same-ID copy reached auth; validations = %d, want 1", auth.validated)
	}
	if len(events) != 0 {
		t.Fatalf("same-ID copy reached pipeline: %v", events)
	}

	// The compatibility enqueue is intentionally body-only and also cannot
	// establish execution authority from Task.TenantID.
	bodyOnly := &meshtask.Task{
		ID:        "body-only-task",
		TenantID:  "hstles",
		Role:      "executor-test",
		Tags:      []string{h.Name()},
		SLO:       meshtask.TaskSLO{Latency: time.Second},
		CreatedAt: time.Now(),
	}
	if err := gw.Enqueue(bodyOnly); err != nil {
		t.Fatalf("legacy Enqueue: %v", err)
	}
	executor.executeAssignment(context.Background(), meshtask.Assignment{
		Task:          bodyOnly,
		TaskID:        bodyOnly.ID,
		Primary:       "node-test",
		Fence:         "fence-body-only",
		LeaseDuration: time.Second,
	})
	if principal.authorized != 2 || auth.stamped != 0 || auth.validated != 1 {
		t.Fatalf(
			"body-only task reached authority pipeline: authorizations=%d stamps=%d validations=%d",
			principal.authorized,
			auth.stamped,
			auth.validated,
		)
	}
	if len(events) != 0 {
		t.Fatalf("body-only task reached pipeline: %v", events)
	}
}

func TestExecutorRetainsAuthorizationLeaseThroughHandlerDispatch(t *testing.T) {
	reg := handlers.NewHandlerRegistry()
	handlerRelease := make(chan struct{})
	handler := &blockingExecutorTaskProbe{
		entered: make(chan struct{}),
		release: handlerRelease,
	}
	if err := reg.RegisterTask(handler); err != nil {
		t.Fatalf("RegisterTask: %v", err)
	}
	auth := &executorAuthProbe{wantTenant: "hstles"}
	gw, err := gateway.NewGateway(
		gateway.GatewayConfig{ID: "executor-lease-test"},
		gateway.WithExecutionPrincipalReader(auth),
	)
	if err != nil {
		t.Fatalf("NewGateway: %v", err)
	}
	defer gw.Stop()
	executor, err := NewExecutor(gw, reg, ExecutorConfig{
		NodeID:        "node-test",
		Roles:         []string{"executor-test"},
		AuthValidator: auth,
	})
	if err != nil {
		t.Fatalf("NewExecutor: %v", err)
	}
	owner, err := ports.NewExecutionOwnerKey(
		"realm=hstles|tenant=hstles|org=-|kind=service|id=lease|space=platform",
	)
	if err != nil {
		t.Fatal(err)
	}
	principal := &leasedExecutorPrincipal{
		owner:    owner,
		tenant:   "hstles",
		released: make(chan struct{}),
	}
	task := &meshtask.Task{
		ID:        "lease-through-dispatch",
		TenantID:  "hstles",
		Role:      "executor-test",
		Tags:      []string{handler.Name()},
		SLO:       meshtask.TaskSLO{Latency: time.Second},
		CreatedAt: time.Now(),
	}
	admissionCtx, releaseAdmission, err := principal.AuthorizeExecution(
		context.Background(),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := gw.EnqueueAuthorized(admissionCtx, task); err != nil {
		t.Fatal(err)
	}
	releaseAdmission()

	gatewayCtx, cancelGateway := context.WithCancel(context.Background())
	defer cancelGateway()
	gw.Start(gatewayCtx)
	offer := <-gw.Offers()
	gw.BidSink() <- meshtask.Bid{
		TaskID:        offer.Task.ID,
		NodeID:        "node-test",
		CapacityScore: 1,
		ReceivedAt:    time.Now(),
	}
	assignment := <-gw.Assignments()
	done := make(chan struct{})
	go func() {
		defer close(done)
		executor.executeAssignment(context.Background(), assignment)
	}()
	select {
	case <-handler.entered:
	case <-time.After(time.Second):
		t.Fatal("handler did not enter")
	}
	select {
	case <-principal.released:
		t.Fatal("execution authority lease released before handler returned")
	case <-time.After(25 * time.Millisecond):
	}
	close(handlerRelease)
	select {
	case <-principal.released:
	case <-time.After(time.Second):
		t.Fatal("execution authority lease was not released after handler returned")
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("assignment did not complete")
	}
	if got := principal.calls.Load(); got != 2 {
		t.Fatalf("authorization calls = %d, want admission + assignment", got)
	}
}
