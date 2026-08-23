/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bbmumford/loom/node/handlers"
	"github.com/bbmumford/loom/ports"
	"github.com/bbmumford/loom/secrets"
)

type lifetimeTaskHandler struct {
	name string
	role string

	entered  chan struct{}
	proceed  <-chan struct{}
	terminal atomic.Bool
	runs     atomic.Int32
}

func (h *lifetimeTaskHandler) Name() string                      { return h.name }
func (h *lifetimeTaskHandler) Role() string                      { return h.role }
func (h *lifetimeTaskHandler) RequiresAuth() bool                { return false }
func (h *lifetimeTaskHandler) AllowedAuthTypes() []string        { return nil }
func (h *lifetimeTaskHandler) Scopes() []string                  { return nil }
func (h *lifetimeTaskHandler) TenantScope() handlers.TenantScope { return handlers.TenantScopeNone }
func (h *lifetimeTaskHandler) AllowedTenants() []string          { return nil }

func (h *lifetimeTaskHandler) ExecuteTask(
	ctx context.Context,
	task *handlers.Task,
) (*handlers.TaskResult, error) {
	h.runs.Add(1)
	if h.entered != nil {
		select {
		case h.entered <- struct{}{}:
		default:
		}
	}
	if h.proceed != nil {
		select {
		case <-h.proceed:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	h.terminal.Store(true)
	return &handlers.TaskResult{
		TaskID: task.ID,
		Status: handlers.TaskStatusCompleted,
	}, nil
}

type lifetimeRoleActivator struct {
	rt   *Runtime
	role string
	name string

	mu       sync.Mutex
	handlers []*lifetimeTaskHandler
	scope    ports.RegistrationScope
	build    func(generation int) *lifetimeTaskHandler

	deactivateStarted        chan struct{}
	deactivateErr            error
	teardownBeforeTerminal   atomic.Bool
	deactivateStartedClosing sync.Once
}

func (a *lifetimeRoleActivator) Role() string { return a.role }

func (a *lifetimeRoleActivator) Activate(_ context.Context, env ports.ActivationEnv) error {
	a.mu.Lock()
	generation := len(a.handlers) + 1
	h := a.build(generation)
	a.handlers = append(a.handlers, h)
	a.scope = env.RegistrationScope()
	a.mu.Unlock()
	_, err := a.rt.Registry().RegisterTaskScoped(env.RegistrationScope(), h)
	return err
}

func (a *lifetimeRoleActivator) Deactivate(context.Context) error {
	a.mu.Lock()
	var current *lifetimeTaskHandler
	if len(a.handlers) != 0 {
		current = a.handlers[len(a.handlers)-1]
	}
	a.mu.Unlock()
	if current != nil && !current.terminal.Load() && current.runs.Load() != 0 {
		a.teardownBeforeTerminal.Store(true)
	}
	if a.deactivateStarted != nil {
		a.deactivateStartedClosing.Do(func() { close(a.deactivateStarted) })
	}
	return a.deactivateErr
}

func (a *lifetimeRoleActivator) handler(generation int) *lifetimeTaskHandler {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.handlers[generation-1]
}

func (a *lifetimeRoleActivator) registrationScope() ports.RegistrationScope {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.scope
}

var _ ports.RoleActivator = (*lifetimeRoleActivator)(nil)
var _ handlers.TaskHandler = (*lifetimeTaskHandler)(nil)

func activateLifetimeRole(t *testing.T, rt *Runtime, a *lifetimeRoleActivator) {
	t.Helper()
	if err := rt.RegisterRoleActivator(a); err != nil {
		t.Fatalf("RegisterRoleActivator: %v", err)
	}
	if err := rt.ActivateRole(context.Background(), a.role, secrets.ConfigBundle{}); err != nil {
		t.Fatalf("ActivateRole: %v", err)
	}
}

func waitForRoleAdmissionClosed(t *testing.T, rt *Runtime, role string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		rt.roleActivation.mu.Lock()
		generation := rt.roleActivation.generations[role]
		closed := generation != nil && !generation.accepting
		rt.roleActivation.mu.Unlock()
		if closed {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("role %q admission did not close", role)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestRoleDeactivationClosesAdmissionAndDrainsBeforeResourceTeardown(t *testing.T) {
	rt := activationFixture("system")
	proceed := make(chan struct{})
	entered := make(chan struct{}, 1)
	const handlerName = "orbtr.io.auth.GenerationTask"
	a := &lifetimeRoleActivator{
		rt:                rt,
		role:              "auth",
		name:              handlerName,
		deactivateStarted: make(chan struct{}),
		build: func(int) *lifetimeTaskHandler {
			return &lifetimeTaskHandler{
				name:    handlerName,
				role:    "auth",
				entered: entered,
				proceed: proceed,
			}
		},
	}
	activateLifetimeRole(t, rt, a)

	// Capture a stable registration before deactivation. It must remain bound
	// to this generation and become non-admissible when teardown starts.
	stale, ok := rt.Registry().Resolve(handlerName)
	if !ok {
		t.Fatal("Resolve(active generation) = false")
	}

	firstDone := make(chan *handlers.TaskResult, 1)
	firstErr := make(chan error, 1)
	go func() {
		result, err := rt.Registry().DispatchTask(
			context.Background(),
			&handlers.Task{ID: "admitted", Handler: handlerName},
		)
		firstDone <- result
		firstErr <- err
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("first task did not enter the handler")
	}

	deactivateDone := make(chan error, 1)
	go func() {
		deactivateDone <- rt.DeactivateRole(context.Background(), "auth")
	}()
	waitForRoleAdmissionClosed(t, rt, "auth")

	select {
	case <-a.deactivateStarted:
		t.Fatal("role resources began teardown before the admitted task drained")
	default:
	}

	rejected, err := stale.DispatchTaskWithAuth(
		context.Background(),
		&handlers.Task{ID: "stale-after-close", Handler: handlerName},
		nil,
	)
	if err != nil {
		t.Fatalf("stale DispatchTaskWithAuth: %v", err)
	}
	if rejected == nil || rejected.Status != handlers.TaskStatusFailed {
		t.Fatalf("stale result = %+v, want failed admission", rejected)
	}
	if got := a.handler(1).runs.Load(); got != 1 {
		t.Fatalf("handler ran %d times; closed generation admitted new work", got)
	}

	close(proceed)
	if err := <-firstErr; err != nil {
		t.Fatalf("admitted task: %v", err)
	}
	if result := <-firstDone; result == nil || result.Status != handlers.TaskStatusCompleted {
		t.Fatalf("admitted result = %+v, want completed", result)
	}
	if err := <-deactivateDone; err != nil {
		t.Fatalf("DeactivateRole: %v", err)
	}
	if a.teardownBeforeTerminal.Load() {
		t.Fatal("activator teardown observed a non-terminal admitted task")
	}
}

func TestResolvedTaskFromOldRoleGenerationCannotEnterReplacement(t *testing.T) {
	rt := activationFixture("system")
	const handlerName = "orbtr.io.auth.ReactivatedTask"
	a := &lifetimeRoleActivator{
		rt:   rt,
		role: "auth",
		name: handlerName,
		build: func(int) *lifetimeTaskHandler {
			return &lifetimeTaskHandler{name: handlerName, role: "auth"}
		},
	}
	activateLifetimeRole(t, rt, a)
	old, ok := rt.Registry().Resolve(handlerName)
	if !ok {
		t.Fatal("Resolve(first generation) = false")
	}
	if err := rt.DeactivateRole(context.Background(), "auth"); err != nil {
		t.Fatalf("DeactivateRole(first): %v", err)
	}
	if err := rt.ActivateRole(context.Background(), "auth", secrets.ConfigBundle{}); err != nil {
		t.Fatalf("ActivateRole(second): %v", err)
	}

	result, err := old.DispatchTaskWithAuth(
		context.Background(),
		&handlers.Task{ID: "old-generation", Handler: handlerName},
		nil,
	)
	if err != nil {
		t.Fatalf("old generation dispatch: %v", err)
	}
	if result == nil || result.Status != handlers.TaskStatusFailed {
		t.Fatalf("old generation result = %+v, want failed admission", result)
	}
	if got := a.handler(1).runs.Load(); got != 0 {
		t.Fatalf("old generation handler ran %d times after replacement", got)
	}

	current, err := rt.Registry().DispatchTask(
		context.Background(),
		&handlers.Task{ID: "current-generation", Handler: handlerName},
	)
	if err != nil {
		t.Fatalf("current generation dispatch: %v", err)
	}
	if current == nil || current.Status != handlers.TaskStatusCompleted {
		t.Fatalf("current generation result = %+v, want completed", current)
	}
	if got := a.handler(2).runs.Load(); got != 1 {
		t.Fatalf("replacement generation ran %d times, want 1", got)
	}
}

func TestFailedRoleTeardownReopensTaskAdmission(t *testing.T) {
	rt := activationFixture("system")
	boom := errors.New("resource teardown failed")
	const (
		handlerName = "orbtr.io.auth.RetryableTask"
		lateName    = "orbtr.io.auth.RetryableLateTask"
	)
	a := &lifetimeRoleActivator{
		rt:            rt,
		role:          "auth",
		name:          handlerName,
		deactivateErr: ports.RoleStillActiveAfterDeactivationError(boom),
		build: func(int) *lifetimeTaskHandler {
			return &lifetimeTaskHandler{name: handlerName, role: "auth"}
		},
	}
	activateLifetimeRole(t, rt, a)

	if err := rt.DeactivateRole(context.Background(), "auth"); !errors.Is(err, boom) {
		t.Fatalf("DeactivateRole error = %v, want %v", err, boom)
	}
	result, err := rt.Registry().DispatchTask(
		context.Background(),
		&handlers.Task{ID: "after-failed-teardown", Handler: handlerName},
	)
	if err != nil {
		t.Fatalf("dispatch after failed teardown: %v", err)
	}
	if result == nil || result.Status != handlers.TaskStatusCompleted {
		t.Fatalf("result after failed teardown = %+v, want completed", result)
	}
	late := &lifetimeTaskHandler{name: lateName, role: "auth"}
	if _, err := rt.Registry().RegisterTaskScoped(a.registrationScope(), late); err != nil {
		t.Fatalf("late scoped registration after StillActive rollback: %v", err)
	}
	result, err = rt.Registry().DispatchTask(
		context.Background(),
		&handlers.Task{ID: "late-after-failed-teardown", Handler: lateName},
	)
	if err != nil || result == nil || result.Status != handlers.TaskStatusCompleted {
		t.Fatalf("late dispatch after failed teardown = (%+v, %v), want completed", result, err)
	}

	a.deactivateErr = nil
	if err := rt.DeactivateRole(context.Background(), "auth"); err != nil {
		t.Fatalf("final DeactivateRole: %v", err)
	}
	if _, ok := rt.Registry().GetMeta(lateName); ok {
		t.Fatal("late registration survived final exact teardown")
	}
}

func TestUnknownPartialRoleTeardownFailureKeepsTaskAdmissionClosed(t *testing.T) {
	rt := activationFixture("system")
	boom := errors.New("resources may be partially released")
	const handlerName = "orbtr.io.auth.DegradedTask"
	a := &lifetimeRoleActivator{
		rt:            rt,
		role:          "auth",
		name:          handlerName,
		deactivateErr: boom,
		build: func(int) *lifetimeTaskHandler {
			return &lifetimeTaskHandler{name: handlerName, role: "auth"}
		},
	}
	activateLifetimeRole(t, rt, a)

	if err := rt.DeactivateRole(context.Background(), "auth"); !errors.Is(err, boom) {
		t.Fatalf("DeactivateRole error = %v, want %v", err, boom)
	}
	result, err := rt.Registry().DispatchTask(
		context.Background(),
		&handlers.Task{ID: "after-partial-teardown", Handler: handlerName},
	)
	if err != nil {
		t.Fatalf("dispatch after partial teardown: %v", err)
	}
	if result == nil || result.Status != handlers.TaskStatusFailed {
		t.Fatalf("result after partial teardown = %+v, want failed admission", result)
	}
	if got := a.handler(1).runs.Load(); got != 0 {
		t.Fatalf("partially torn-down handler ran %d times", got)
	}
}

func TestCanceledRoleDrainReopensAdmissionWhileExistingTaskFinishes(t *testing.T) {
	rt := activationFixture("system")
	proceed := make(chan struct{})
	entered := make(chan struct{}, 2)
	const (
		handlerName = "orbtr.io.auth.CanceledDrainTask"
		lateName    = "orbtr.io.auth.CanceledDrainLateTask"
	)
	a := &lifetimeRoleActivator{
		rt:   rt,
		role: "auth",
		name: handlerName,
		build: func(int) *lifetimeTaskHandler {
			return &lifetimeTaskHandler{
				name:    handlerName,
				role:    "auth",
				entered: entered,
				proceed: proceed,
			}
		},
	}
	activateLifetimeRole(t, rt, a)

	dispatchDone := make(chan error, 2)
	dispatch := func(id string) {
		_, err := rt.Registry().DispatchTask(
			context.Background(),
			&handlers.Task{ID: id, Handler: handlerName},
		)
		dispatchDone <- err
	}
	go dispatch("already-running")
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("first task did not enter the handler")
	}

	drainCtx, cancelDrain := context.WithCancel(context.Background())
	deactivateDone := make(chan error, 1)
	go func() { deactivateDone <- rt.DeactivateRole(drainCtx, "auth") }()
	waitForRoleAdmissionClosed(t, rt, "auth")
	cancelDrain()
	if err := <-deactivateDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled DeactivateRole error = %v, want context.Canceled", err)
	}

	// Cancellation abandons this teardown attempt; the still-active
	// generation must accept work again even while its original task remains
	// in flight.
	late := &lifetimeTaskHandler{name: lateName, role: "auth"}
	if _, err := rt.Registry().RegisterTaskScoped(a.registrationScope(), late); err != nil {
		t.Fatalf("late scoped registration after canceled drain: %v", err)
	}
	lateResult, err := rt.Registry().DispatchTask(
		context.Background(),
		&handlers.Task{ID: "late-after-cancel", Handler: lateName},
	)
	if err != nil || lateResult == nil || lateResult.Status != handlers.TaskStatusCompleted {
		t.Fatalf("late dispatch after canceled drain = (%+v, %v), want completed", lateResult, err)
	}
	go dispatch("admitted-after-cancel")
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("admission did not reopen after the canceled drain")
	}

	close(proceed)
	for i := 0; i < 2; i++ {
		if err := <-dispatchDone; err != nil {
			t.Fatalf("task #%d: %v", i+1, err)
		}
	}
	if err := rt.DeactivateRole(context.Background(), "auth"); err != nil {
		t.Fatalf("final DeactivateRole: %v", err)
	}
	if _, ok := rt.Registry().GetMeta(lateName); ok {
		t.Fatal("late registration survived final exact teardown")
	}
}
