/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bbmumford/loom/compose"
	"github.com/bbmumford/loom/node/handlers"
	"github.com/bbmumford/loom/ports"
)

type composeTaskProbe struct {
	name         string
	scope        handlers.TenantScope
	requiresAuth bool
	ran          chan context.Context
	run          func(context.Context, *handlers.Task) (*handlers.TaskResult, error)
}

func (h *composeTaskProbe) Name() string                      { return h.name }
func (h *composeTaskProbe) Role() string                      { return "compose-test" }
func (h *composeTaskProbe) RequiresAuth() bool                { return h.requiresAuth }
func (h *composeTaskProbe) AllowedAuthTypes() []string        { return nil }
func (h *composeTaskProbe) Scopes() []string                  { return nil }
func (h *composeTaskProbe) TenantScope() handlers.TenantScope { return h.scope }
func (h *composeTaskProbe) AllowedTenants() []string          { return nil }

func (h *composeTaskProbe) ExecuteTask(ctx context.Context, task *handlers.Task) (*handlers.TaskResult, error) {
	h.ran <- ctx
	if h.run != nil {
		return h.run(ctx, task)
	}
	return &handlers.TaskResult{
		TaskID: task.ID,
		Status: handlers.TaskStatusCompleted,
	}, nil
}

var _ handlers.TaskHandler = (*composeTaskProbe)(nil)

type composeRPCProbe struct {
	name             string
	requiresAuth     bool
	allowedAuthTypes []string
	scopes           []string
	ran              atomic.Int32
}

func (h *composeRPCProbe) Name() string               { return h.name }
func (h *composeRPCProbe) Role() string               { return "compose-rpc-test" }
func (h *composeRPCProbe) RequiresAuth() bool         { return h.requiresAuth }
func (h *composeRPCProbe) AllowedAuthTypes() []string { return h.allowedAuthTypes }
func (h *composeRPCProbe) Scopes() []string           { return h.scopes }
func (h *composeRPCProbe) TenantScope() handlers.TenantScope {
	return handlers.TenantScopeNone
}
func (h *composeRPCProbe) AllowedTenants() []string { return nil }
func (h *composeRPCProbe) ExecuteRPC(
	context.Context,
	*handlers.RPCRequest,
) (*handlers.RPCResponse, error) {
	h.ran.Add(1)
	return &handlers.RPCResponse{
		Success: true,
		Payload: []byte("rpc-authorized"),
	}, nil
}

var _ handlers.RPCHandler = (*composeRPCProbe)(nil)

type composeCallerContextKey struct{}
type composeMiddlewareContextKey struct{}
type composeAuthContextKey struct{}
type composePrincipalContextKey struct{}

type composeTestPrincipal struct {
	owner      ports.ExecutionOwnerKey
	revoked    *atomic.Bool
	authorizes atomic.Int32
}

func (p *composeTestPrincipal) OwnerKey() ports.ExecutionOwnerKey {
	return p.owner
}

func (p *composeTestPrincipal) AuthorizeExecution(
	ctx context.Context,
) (context.Context, func(), error) {
	p.authorizes.Add(1)
	if p.revoked != nil && p.revoked.Load() {
		return ctx, nil, errors.New("compose principal revoked")
	}
	return context.WithValue(
		ctx,
		composePrincipalContextKey{},
		p,
	), func() {}, nil
}

func newComposeTestPrincipal(identity string) *composeTestPrincipal {
	owner, err := ports.NewExecutionOwnerKey(identity)
	if err != nil {
		panic(err)
	}
	return &composeTestPrincipal{owner: owner}
}

var defaultComposeTestPrincipal = newComposeTestPrincipal("compose-test-owner")

type composeMiddlewareProbe struct {
	before atomic.Int32
}

func (m *composeMiddlewareProbe) Name() string { return "compose-test-middleware" }

func (m *composeMiddlewareProbe) Before(ctx context.Context, _ string, _ *handlers.RPCRequest) (context.Context, error) {
	m.before.Add(1)
	return context.WithValue(ctx, composeMiddlewareContextKey{}, "middleware"), nil
}

func (m *composeMiddlewareProbe) After(
	_ context.Context,
	_ string,
	resp *handlers.RPCResponse,
	err error,
) (*handlers.RPCResponse, error) {
	return resp, err
}

var _ handlers.Middleware = (*composeMiddlewareProbe)(nil)

type composeAuthValidator struct {
	calls     atomic.Int32
	principal ports.ExecutionPrincipal
}

func (v *composeAuthValidator) ValidateExecutionAuth(ctx context.Context, h ports.SecureHandler) error {
	v.calls.Add(1)
	if h.RequiresAuth() {
		if authenticated, _ := ctx.Value(composeAuthContextKey{}).(bool); !authenticated {
			return errors.New("compose principal missing")
		}
	}
	return nil
}

func (v *composeAuthValidator) WithTenantID(ctx context.Context, _ string) context.Context {
	return ctx
}

func (v *composeAuthValidator) ExecutionPrincipal(
	ctx context.Context,
) (ports.ExecutionPrincipal, bool) {
	if principal, ok := ctx.Value(composePrincipalContextKey{}).(ports.ExecutionPrincipal); ok {
		return principal, principal != nil && principal.OwnerKey().Valid()
	}
	principal := v.principal
	if principal == nil {
		principal = defaultComposeTestPrincipal
	}
	return principal, principal.OwnerKey().Valid()
}

var _ ports.AuthValidator = (*composeAuthValidator)(nil)
var _ ports.ExecutionPrincipalReader = (*composeAuthValidator)(nil)

type composeAuthWithoutPrincipalReader struct{}

func (*composeAuthWithoutPrincipalReader) ValidateExecutionAuth(
	context.Context,
	ports.SecureHandler,
) error {
	return nil
}

func (*composeAuthWithoutPrincipalReader) WithTenantID(
	ctx context.Context,
	_ string,
) context.Context {
	return ctx
}

var _ ports.AuthValidator = (*composeAuthWithoutPrincipalReader)(nil)

type composeRPCAuthProbe struct {
	calls atomic.Int32
	err   error
}

func (v *composeRPCAuthProbe) ValidateExecutionAuth(
	_ context.Context,
	h ports.SecureHandler,
) error {
	v.calls.Add(1)
	if len(h.AllowedAuthTypes()) == 0 || len(h.Scopes()) == 0 {
		return errors.New("test handler omitted authentication policy")
	}
	return v.err
}

func (v *composeRPCAuthProbe) WithTenantID(
	ctx context.Context,
	_ string,
) context.Context {
	return ctx
}

var _ ports.AuthValidator = (*composeRPCAuthProbe)(nil)

type composeTriggerContextProvider struct {
	ctx   context.Context
	err   error
	calls atomic.Int32
}

func (p *composeTriggerContextProvider) AuthorizeTriggerFire(
	_ context.Context,
	_ compose.TriggerInvocation,
) (context.Context, func(), error) {
	p.calls.Add(1)
	if p.err != nil {
		return p.ctx, nil, p.err
	}
	return p.ctx, func() {}, nil
}

var _ ComposeTriggerPrincipalProvider = (*composeTriggerContextProvider)(nil)

func composePrincipalContext(principal ports.ExecutionPrincipal) context.Context {
	return context.WithValue(
		context.Background(),
		composePrincipalContextKey{},
		principal,
	)
}

func composeRuntimeFixture(t *testing.T, h handlers.TaskHandler) *Runtime {
	t.Helper()
	registry := handlers.NewHandlerRegistry()
	if err := registry.RegisterTask(h); err != nil {
		t.Fatalf("RegisterTask: %v", err)
	}
	runtimeCtx, cancel := context.WithCancel(
		context.WithValue(context.Background(), composeCallerContextKey{}, "runtime"),
	)
	t.Cleanup(cancel)
	rt := &Runtime{
		ctx:       runtimeCtx,
		cancel:    cancel,
		rpcServer: NewRPCServer(registry),
	}
	rt.rpcServer.SetAuthValidator(&composeAuthValidator{})
	return rt
}

func requireCancelledComposeCompletion(
	t *testing.T,
	rt *Runtime,
	handle string,
) ComposeTaskCompletion {
	t.Helper()
	completion, ok := rt.LookupComposeTaskCompletion(context.Background(), handle)
	if !ok {
		t.Fatalf("cancelled Deferred handle %q has no terminal completion", handle)
	}
	if completion.Status != handlers.TaskStatusFailed ||
		completion.Result.Kind != compose.ResultFailure ||
		!strings.Contains(completion.Result.Err, context.Canceled.Error()) {
		t.Fatalf("cancelled completion = %+v, want failed context cancellation", completion)
	}
	return completion
}

func requireSingleObservedComposeCompletion(
	t *testing.T,
	observed <-chan ComposeTaskCompletion,
	handle string,
) {
	t.Helper()
	select {
	case completion := <-observed:
		if completion.Handle != handle {
			t.Fatalf("observed completion handle = %q, want %q", completion.Handle, handle)
		}
	default:
		t.Fatalf("Deferred handle %q produced no observed terminal completion", handle)
	}
	select {
	case duplicate := <-observed:
		t.Fatalf("Deferred handle %q produced duplicate completion: %+v", handle, duplicate)
	default:
	}
}

func TestComposeInvokeTaskUsesInjectedAuthValidator(t *testing.T) {
	h := &composeTaskProbe{
		name:         "compose.secure-task",
		scope:        handlers.TenantScopeNone,
		requiresAuth: true,
		ran:          make(chan context.Context, 1),
	}
	rt := composeRuntimeFixture(t, h)
	auth := &composeAuthValidator{}
	rt.rpcServer.SetAuthValidator(auth)

	got := rt.ComposeInvoke(context.Background(), compose.FunctionID(h.name), []byte("event"))
	if got.Kind != compose.ResultDeferred {
		t.Fatalf("ComposeInvoke result = %q, want Deferred", got.Kind)
	}
	rt.wg.Wait()

	select {
	case <-h.ran:
		t.Fatal("unauthenticated compose task executed")
	default:
	}
	if got := auth.calls.Load(); got != 1 {
		t.Fatalf("injected auth validator calls = %d, want 1", got)
	}
	denied, ok := rt.LookupComposeTaskCompletion(
		context.Background(),
		string(got.Payload),
	)
	if !ok {
		t.Fatal("authorization-denied Deferred handle has no terminal completion")
	}
	if denied.Status != handlers.TaskStatusFailed ||
		denied.Result.Kind != compose.ResultFailure ||
		!strings.Contains(denied.Result.Err, "authorization failed") {
		t.Fatalf("authorization-denied completion = %+v, want failed authorization", denied)
	}

	authenticatedCtx := context.WithValue(context.Background(), composeAuthContextKey{}, true)
	got = rt.ComposeInvoke(authenticatedCtx, compose.FunctionID(h.name), []byte("event"))
	if got.Kind != compose.ResultDeferred {
		t.Fatalf("authenticated ComposeInvoke result = %q, want Deferred", got.Kind)
	}
	rt.wg.Wait()

	select {
	case <-h.ran:
	default:
		t.Fatal("authenticated compose task did not execute")
	}
	if got := auth.calls.Load(); got != 2 {
		t.Fatalf("injected auth validator calls = %d, want 2", got)
	}
}

func TestComposeTriggerRPCUsesInjectedAuthBeforeMiddlewareAndHandler(t *testing.T) {
	registry := handlers.NewHandlerRegistry()
	handler := &composeRPCProbe{
		name:             "compose.secure-rpc",
		requiresAuth:     true,
		allowedAuthTypes: []string{"service"},
		scopes:           []string{"compose.trigger.fire"},
	}
	if err := registry.RegisterRPC(handler); err != nil {
		t.Fatalf("RegisterRPC: %v", err)
	}
	middleware := &composeMiddlewareProbe{}
	registry.Use(middleware)
	runtimeCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rt := &Runtime{
		ctx:       runtimeCtx,
		cancel:    cancel,
		rpcServer: NewRPCServer(registry),
	}
	auth := &composeRPCAuthProbe{
		err: errors.New("missing authentication type or required scope"),
	}
	rt.rpcServer.SetAuthValidator(auth)
	provider := &composeTriggerContextProvider{
		ctx: context.Background(),
	}
	rt.cfg.ComposeTriggerPrincipalProvider = provider
	registration := compose.TriggerInvocation{
		ID:                     "secure-rpc-trigger",
		Kind:                   compose.TriggerState,
		Function:               compose.FunctionID(handler.Name()),
		RegistrationRevision:   "rev-auth",
		RegistrationGeneration: 7,
		SpecDigest:             sha256.Sum256([]byte("secure-rpc-spec")),
	}

	denied := rt.invokeComposeTrigger(
		context.Background(),
		registration,
		[]byte("event"),
	)
	if denied.Kind != compose.ResultFailure ||
		!strings.Contains(denied.Err, "authorization failed") ||
		!strings.Contains(denied.Err, "missing authentication type or required scope") {
		t.Fatalf("denied trigger RPC = %+v", denied)
	}
	if got := auth.calls.Load(); got != 1 {
		t.Fatalf("auth calls after denial = %d, want 1", got)
	}
	if got := middleware.before.Load(); got != 0 {
		t.Fatalf("middleware ran before authorization: calls=%d", got)
	}
	if got := handler.ran.Load(); got != 0 {
		t.Fatalf("handler ran before authorization: calls=%d", got)
	}

	auth.err = nil
	accepted := rt.invokeComposeTrigger(
		context.Background(),
		registration,
		[]byte("event"),
	)
	if accepted.Kind != compose.ResultSuccess ||
		string(accepted.Payload) != "rpc-authorized" {
		t.Fatalf("authorized trigger RPC = %+v", accepted)
	}
	if got := provider.calls.Load(); got != 2 {
		t.Fatalf("product provider calls = %d, want 2", got)
	}
	if got := auth.calls.Load(); got != 2 {
		t.Fatalf("auth calls = %d, want 2", got)
	}
	if got := middleware.before.Load(); got != 1 {
		t.Fatalf("middleware calls = %d, want 1", got)
	}
	if got := handler.ran.Load(); got != 1 {
		t.Fatalf("handler calls = %d, want 1", got)
	}
}

func TestComposeInvokeSnapshotsAuthAndResolvedHandlerAtAdmission(t *testing.T) {
	const handlerName = "compose.stable-task"
	first := &composeTaskProbe{
		name:  handlerName,
		scope: handlers.TenantScopeNone,
		ran:   make(chan context.Context, 1),
	}
	replacement := &composeTaskProbe{
		name:  handlerName,
		scope: handlers.TenantScopeNone,
		ran:   make(chan context.Context, 1),
	}
	rt := composeRuntimeFixture(t, first)
	firstAuth := &composeAuthValidator{}
	replacementAuth := &composeAuthValidator{}
	rt.rpcServer.SetAuthValidator(firstAuth)

	// Hold admission after classification. composeTaskSeq increments after
	// the handler and auth snapshots are captured but before tryGo takes this
	// lock, giving the test a deterministic replacement window.
	rt.lifecycleMu.Lock()
	locked := true
	defer func() {
		if locked {
			rt.lifecycleMu.Unlock()
		}
	}()
	invoked := make(chan compose.FunctionResult, 1)
	go func() {
		invoked <- rt.ComposeInvoke(
			context.Background(),
			compose.FunctionID(handlerName),
			[]byte("event"),
		)
	}()
	deadline := time.After(2 * time.Second)
	for rt.composeTaskSeq.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("ComposeInvoke did not reach the admission boundary")
		default:
			time.Sleep(time.Millisecond)
		}
	}

	if !rt.Registry().Unregister(handlerName) {
		t.Fatal("Unregister(first) = false")
	}
	if err := rt.Registry().RegisterTask(replacement); err != nil {
		t.Fatalf("RegisterTask(replacement): %v", err)
	}
	rt.rpcServer.SetAuthValidator(replacementAuth)
	rt.lifecycleMu.Unlock()
	locked = false

	got := <-invoked
	if got.Kind != compose.ResultDeferred {
		t.Fatalf("ComposeInvoke result = %q, want Deferred", got.Kind)
	}
	rt.wg.Wait()
	select {
	case <-first.ran:
	default:
		t.Fatal("captured handler did not execute")
	}
	select {
	case <-replacement.ran:
		t.Fatal("replacement handler stole an already-admitted task")
	default:
	}
	if got := firstAuth.calls.Load(); got != 1 {
		t.Fatalf("captured auth validator calls = %d, want 1", got)
	}
	if got := replacementAuth.calls.Load(); got != 0 {
		t.Fatalf("replacement auth validator calls = %d, want 0", got)
	}
}

func TestComposeInvokePublishesAndStoresFailedTaskCompletion(t *testing.T) {
	h := &composeTaskProbe{
		name:  "compose.failed-task",
		scope: handlers.TenantScopeNone,
		ran:   make(chan context.Context, 1),
		run: func(_ context.Context, _ *handlers.Task) (*handlers.TaskResult, error) {
			return &handlers.TaskResult{
				// A handler result cannot replace the Deferred correlation
				// handle the caller already received.
				TaskID: "handler-selected-foreign-id",
				Status: handlers.TaskStatusFailed,
				Error:  "task failed explicitly",
			}, nil
		},
	}
	rt := composeRuntimeFixture(t, h)
	observed := make(chan ComposeTaskCompletion, 1)
	rt.cfg.ComposeTaskCompletionObserver = func(
		_ context.Context,
		completion ComposeTaskCompletion,
	) {
		observed <- completion
	}

	got := rt.ComposeInvoke(context.Background(), compose.FunctionID(h.name), []byte("event"))
	if got.Kind != compose.ResultDeferred {
		t.Fatalf("ComposeInvoke result = %q, want Deferred", got.Kind)
	}
	rt.wg.Wait()
	handle := string(got.Payload)

	stored, ok := rt.LookupComposeTaskCompletion(context.Background(), handle)
	if !ok {
		t.Fatalf("LookupComposeTaskCompletion(%q) = false", handle)
	}
	if stored.Handle != handle ||
		stored.Function != compose.FunctionID(h.name) ||
		stored.Status != handlers.TaskStatusFailed ||
		stored.Result.Kind != compose.ResultFailure ||
		stored.Result.Err != "task failed explicitly" {
		t.Fatalf("stored completion = %+v", stored)
	}
	select {
	case callback := <-observed:
		if callback.Handle != handle || callback.Result.Err != "task failed explicitly" {
			t.Fatalf("observer completion = %+v", callback)
		}
	default:
		t.Fatal("completion observer was not called")
	}
	select {
	case duplicate := <-observed:
		t.Fatalf("completion observer called more than once: %+v", duplicate)
	default:
	}
}

func TestComposeInvokeSnapshotsCompletionObserverAtAdmission(t *testing.T) {
	h := &composeBlockingTaskProbe{
		started: make(chan struct{}),
		stopped: make(chan error, 1),
	}
	rt := composeRuntimeFixture(t, h)
	first := make(chan ComposeTaskCompletion, 1)
	replacement := make(chan ComposeTaskCompletion, 1)
	rt.cfg.ComposeTaskCompletionObserver = func(_ context.Context, completion ComposeTaskCompletion) {
		first <- completion
	}
	callerCtx, cancelCaller := context.WithCancel(context.Background())

	got := rt.ComposeInvoke(callerCtx, compose.FunctionID(h.Name()), nil)
	if got.Kind != compose.ResultDeferred {
		t.Fatalf("ComposeInvoke result = %q, want Deferred", got.Kind)
	}
	select {
	case <-h.started:
	case <-time.After(2 * time.Second):
		t.Fatal("compose task did not start")
	}
	rt.cfg.ComposeTaskCompletionObserver = func(_ context.Context, completion ComposeTaskCompletion) {
		replacement <- completion
	}
	cancelCaller()
	rt.wg.Wait()

	select {
	case completion := <-first:
		if completion.Handle != string(got.Payload) {
			t.Fatalf("captured observer handle = %q, want %q", completion.Handle, got.Payload)
		}
	default:
		t.Fatal("observer captured at admission was not called")
	}
	select {
	case completion := <-replacement:
		t.Fatalf("replacement observer stole completion: %+v", completion)
	default:
	}
}

func TestComposeInvokeRecordsHandlerPanicBeforeObserverFailure(t *testing.T) {
	h := &composeTaskProbe{
		name:  "compose.panicking-task",
		scope: handlers.TenantScopeNone,
		ran:   make(chan context.Context, 1),
		run: func(context.Context, *handlers.Task) (*handlers.TaskResult, error) {
			panic("handler exploded")
		},
	}
	rt := composeRuntimeFixture(t, h)
	rt.cfg.ComposeTaskCompletionObserver = func(context.Context, ComposeTaskCompletion) {
		panic("observer exploded")
	}

	got := rt.ComposeInvoke(context.Background(), compose.FunctionID(h.name), nil)
	if got.Kind != compose.ResultDeferred {
		t.Fatalf("ComposeInvoke result = %q, want Deferred", got.Kind)
	}
	rt.wg.Wait()

	completion, ok := rt.LookupComposeTaskCompletion(
		context.Background(),
		string(got.Payload),
	)
	if !ok {
		t.Fatal("panicking handler Deferred handle has no terminal completion")
	}
	if completion.Status != handlers.TaskStatusFailed ||
		completion.Result.Kind != compose.ResultFailure ||
		!strings.Contains(completion.Result.Err, "handler exploded") {
		t.Fatalf("panic completion = %+v, want failed panic", completion)
	}
}

func TestComposeTaskCompletionHistoryIsBoundedAndDefensive(t *testing.T) {
	rt := &Runtime{rpcServer: NewRPCServer(handlers.NewHandlerRegistry())}
	rt.rpcServer.SetAuthValidator(&composeAuthValidator{})
	rt.cfg.ComposeTaskCompletionRetentionPerOwner = composeTaskCompletionRetention + 1
	owner := defaultComposeTestPrincipal.OwnerKey()
	var observed atomic.Int32
	rt.cfg.ComposeTaskCompletionObserver = func(
		_ context.Context,
		completion ComposeTaskCompletion,
	) {
		observed.Add(1)
		completion.Result.Payload[0] ^= 0xff
	}
	for i := 0; i <= composeTaskCompletionRetention; i++ {
		handle := fmt.Sprintf("completion-%d", i)
		rt.recordComposeTaskCompletion(
			context.Background(),
			handle,
			compose.FunctionID("compose.history"),
			owner,
			sha256.Sum256([]byte(handle)),
			nil,
			&handlers.TaskResult{
				TaskID:  handle,
				Status:  handlers.TaskStatusCompleted,
				Payload: []byte{byte(i)},
			},
			nil,
			rt.cfg.ComposeTaskCompletionObserver,
		)
	}
	if got := observed.Load(); got != composeTaskCompletionRetention+1 {
		t.Fatalf("completion observer calls = %d, want %d", got, composeTaskCompletionRetention+1)
	}

	if _, ok := rt.LookupComposeTaskCompletion(
		context.Background(),
		"completion-0",
	); ok {
		t.Fatal("oldest completion was not evicted")
	}
	latestHandle := fmt.Sprintf("completion-%d", composeTaskCompletionRetention)
	latest, ok := rt.LookupComposeTaskCompletion(
		context.Background(),
		latestHandle,
	)
	if !ok {
		t.Fatalf("latest completion %q was evicted", latestHandle)
	}
	latest.Result.Payload[0] ^= 0xff
	again, ok := rt.LookupComposeTaskCompletion(
		context.Background(),
		latestHandle,
	)
	if !ok {
		t.Fatalf("latest completion %q disappeared", latestHandle)
	}
	if again.Result.Payload[0] != byte(composeTaskCompletionRetention%256) {
		t.Fatal("caller mutation changed stored completion payload")
	}
}

func TestComposeTaskCompletionPublishesOnlyFirstTerminalRecord(t *testing.T) {
	rt := &Runtime{}
	owner := defaultComposeTestPrincipal.OwnerKey()
	requestDigest := sha256.Sum256([]byte("completion-once"))
	observed := make(chan ComposeTaskCompletion, 2)
	observer := func(_ context.Context, completion ComposeTaskCompletion) {
		observed <- completion
	}
	rt.recordComposeTaskCompletion(
		context.Background(),
		"completion-once",
		compose.FunctionID("compose.once"),
		owner,
		requestDigest,
		nil,
		&handlers.TaskResult{
			TaskID: "completion-once",
			Status: handlers.TaskStatusCompleted,
		},
		nil,
		observer,
	)
	rt.recordComposeTaskCompletion(
		context.Background(),
		"completion-once",
		compose.FunctionID("compose.once"),
		owner,
		requestDigest,
		nil,
		&handlers.TaskResult{
			TaskID: "completion-once",
			Status: handlers.TaskStatusFailed,
			Error:  "late replacement",
		},
		errors.New("late replacement"),
		observer,
	)

	storedRecord, ok := rt.lookupComposeTaskCompletion("completion-once")
	if !ok {
		t.Fatal("first terminal completion was not stored")
	}
	stored := storedRecord.completion
	if stored.Status != handlers.TaskStatusCompleted ||
		stored.Result.Kind != compose.ResultSuccess {
		t.Fatalf("stored completion = %+v, want first successful terminal record", stored)
	}
	select {
	case first := <-observed:
		if first.Status != handlers.TaskStatusCompleted {
			t.Fatalf("observed completion = %+v, want first terminal record", first)
		}
	default:
		t.Fatal("first terminal record was not published")
	}
	select {
	case duplicate := <-observed:
		t.Fatalf("duplicate terminal record was published: %+v", duplicate)
	default:
	}
}

func TestComposeInvokeSnapshotsPayloadBeforeDeferredReturn(t *testing.T) {
	release := make(chan struct{})
	received := make(chan []byte, 1)
	h := &composeTaskProbe{
		name:  "compose.stable-payload",
		scope: handlers.TenantScopeNone,
		ran:   make(chan context.Context, 1),
		run: func(_ context.Context, task *handlers.Task) (*handlers.TaskResult, error) {
			<-release
			received <- append([]byte(nil), task.Payload...)
			return &handlers.TaskResult{
				TaskID: task.ID,
				Status: handlers.TaskStatusCompleted,
			}, nil
		},
	}
	rt := composeRuntimeFixture(t, h)
	event := []byte("original")

	got := rt.ComposeInvoke(context.Background(), compose.FunctionID(h.name), event)
	if got.Kind != compose.ResultDeferred {
		t.Fatalf("ComposeInvoke result = %q, want Deferred", got.Kind)
	}
	select {
	case <-h.ran:
	case <-time.After(2 * time.Second):
		t.Fatal("compose task did not reach the payload barrier")
	}
	copy(event, []byte("mutated!"))
	close(release)
	rt.wg.Wait()

	select {
	case payload := <-received:
		if string(payload) != "original" {
			t.Fatalf("admitted task payload = %q, want admission snapshot", payload)
		}
	default:
		t.Fatal("compose task did not report its admitted payload")
	}
}

func TestComposeInvokeBoundsInFlightAdmissionAndExportsPressure(t *testing.T) {
	h := &composeBlockingTaskProbe{
		started: make(chan struct{}),
		stopped: make(chan error, 1),
	}
	rt := composeRuntimeFixture(t, h)
	rt.cfg.ComposeTaskMaxInFlight = 1
	callerCtx, cancelCaller := context.WithCancel(context.Background())

	first := rt.ComposeInvoke(callerCtx, compose.FunctionID(h.Name()), nil)
	if first.Kind != compose.ResultDeferred {
		t.Fatalf("first ComposeInvoke result = %q, want Deferred", first.Kind)
	}
	select {
	case <-h.started:
	case <-time.After(2 * time.Second):
		t.Fatal("first compose task did not start")
	}
	second := rt.ComposeInvoke(context.Background(), compose.FunctionID(h.Name()), nil)
	if second.Kind != compose.ResultFailure || second.Err != composeTaskAdmissionFullError {
		t.Fatalf("saturated ComposeInvoke result = %+v, want deterministic admission failure", second)
	}
	metrics := rt.MeshMetrics()
	if got := metrics["compose_task_in_flight"]; got != int64(1) {
		t.Fatalf("compose_task_in_flight = %#v, want 1", got)
	}
	if got := metrics["compose_task_in_flight_limit"]; got != int64(1) {
		t.Fatalf("compose_task_in_flight_limit = %#v, want 1", got)
	}
	if got := metrics["compose_task_admission_rejected"]; got != uint64(1) {
		t.Fatalf("compose_task_admission_rejected = %#v, want 1", got)
	}

	cancelCaller()
	rt.wg.Wait()
	requireCancelledComposeCompletion(t, rt, string(first.Payload))
	if got := rt.MeshMetrics()["compose_task_in_flight"]; got != int64(0) {
		t.Fatalf("compose_task_in_flight after completion = %#v, want 0", got)
	}
}

func TestComposeInvokeTaskUsesHandlerResolvedBeforeAdmission(t *testing.T) {
	const handlerName = "compose.stable-task"
	first := &composeTaskProbe{
		name:  handlerName,
		scope: handlers.TenantScopeNone,
		ran:   make(chan context.Context, 1),
	}
	replacement := &composeTaskProbe{
		name:  handlerName,
		scope: handlers.TenantScopeNone,
		ran:   make(chan context.Context, 1),
	}
	rt := composeRuntimeFixture(t, first)

	// Hold admission after ComposeInvoke resolves the registration. The task
	// sequence increments after resolution and before tryGo takes this lock,
	// giving the test a deterministic replacement window.
	rt.lifecycleMu.Lock()
	locked := true
	defer func() {
		if locked {
			rt.lifecycleMu.Unlock()
		}
	}()
	resultCh := make(chan compose.FunctionResult, 1)
	go func() {
		resultCh <- rt.ComposeInvoke(
			context.Background(),
			compose.FunctionID(handlerName),
			[]byte("event"),
		)
	}()

	deadline := time.Now().Add(2 * time.Second)
	for rt.composeTaskSeq.Load() != 1 {
		if time.Now().After(deadline) {
			t.Fatal("ComposeInvoke did not resolve task before admission")
		}
		time.Sleep(time.Millisecond)
	}
	if !rt.Registry().Unregister(handlerName) {
		t.Fatal("Unregister(first) = false")
	}
	if err := rt.Registry().RegisterTask(replacement); err != nil {
		t.Fatalf("RegisterTask(replacement): %v", err)
	}
	rt.lifecycleMu.Unlock()
	locked = false

	var got compose.FunctionResult
	select {
	case got = <-resultCh:
	case <-time.After(2 * time.Second):
		t.Fatal("ComposeInvoke remained blocked after admission released")
	}
	if got.Kind != compose.ResultDeferred {
		t.Fatalf("ComposeInvoke result = %q, want Deferred", got.Kind)
	}
	rt.wg.Wait()

	select {
	case <-first.ran:
	default:
		t.Fatal("resolved task did not execute the first registration")
	}
	select {
	case <-replacement.ran:
		t.Fatal("resolved task executed the replacement registration")
	default:
	}
}

func TestComposeInvokeTaskUsesRegistryTenantScopeGate(t *testing.T) {
	h := &composeTaskProbe{
		name:  "compose.tenant-task",
		scope: handlers.TenantScopeTenant,
		ran:   make(chan context.Context, 1),
	}
	rt := composeRuntimeFixture(t, h)

	got := rt.ComposeInvoke(context.Background(), compose.FunctionID(h.name), []byte("event"))
	if got.Kind != compose.ResultDeferred {
		t.Fatalf("ComposeInvoke result = %q, want Deferred", got.Kind)
	}
	rt.wg.Wait()

	select {
	case <-h.ran:
		t.Fatal("tenant-scoped compose task executed without a tenant context")
	default:
	}
}

func TestComposeInvokeTaskUsesIncomingContextAndRegistryMiddleware(t *testing.T) {
	h := &composeTaskProbe{
		name:  "compose.public-task",
		scope: handlers.TenantScopeNone,
		ran:   make(chan context.Context, 1),
	}
	rt := composeRuntimeFixture(t, h)
	mw := &composeMiddlewareProbe{}
	rt.Registry().Use(mw)

	callerCtx, cancelCaller := context.WithTimeout(
		context.WithValue(context.Background(), composeCallerContextKey{}, "caller"),
		time.Hour,
	)
	defer cancelCaller()
	callerDeadline, _ := callerCtx.Deadline()
	got := rt.ComposeInvoke(callerCtx, compose.FunctionID(h.name), []byte("event"))
	if got.Kind != compose.ResultDeferred {
		t.Fatalf("ComposeInvoke result = %q, want Deferred", got.Kind)
	}
	rt.wg.Wait()

	var handlerCtx context.Context
	select {
	case handlerCtx = <-h.ran:
	default:
		t.Fatal("public compose task did not execute")
	}
	if got := handlerCtx.Value(composeCallerContextKey{}); got != "caller" {
		t.Fatalf("handler caller context marker = %v, want caller", got)
	}
	if got := handlerCtx.Value(composeMiddlewareContextKey{}); got != "middleware" {
		t.Fatalf("handler middleware context marker = %v, want middleware", got)
	}
	if got, ok := handlerCtx.Deadline(); !ok || !got.Equal(callerDeadline) {
		t.Fatalf("handler deadline = (%v, %v), want %v", got, ok, callerDeadline)
	}
	if got := mw.before.Load(); got != 1 {
		t.Fatalf("middleware Before calls = %d, want 1", got)
	}
}

type composeBlockingTaskProbe struct {
	started chan struct{}
	stopped chan error
}

func (*composeBlockingTaskProbe) Name() string               { return "compose.blocking-task" }
func (*composeBlockingTaskProbe) Role() string               { return "compose-test" }
func (*composeBlockingTaskProbe) RequiresAuth() bool         { return false }
func (*composeBlockingTaskProbe) AllowedAuthTypes() []string { return nil }
func (*composeBlockingTaskProbe) Scopes() []string           { return nil }
func (*composeBlockingTaskProbe) TenantScope() handlers.TenantScope {
	return handlers.TenantScopeNone
}
func (*composeBlockingTaskProbe) AllowedTenants() []string { return nil }
func (h *composeBlockingTaskProbe) ExecuteTask(
	ctx context.Context,
	task *handlers.Task,
) (*handlers.TaskResult, error) {
	close(h.started)
	<-ctx.Done()
	h.stopped <- ctx.Err()
	return &handlers.TaskResult{
		TaskID: task.ID,
		Status: handlers.TaskStatusFailed,
		Error:  ctx.Err().Error(),
	}, ctx.Err()
}

func TestComposeInvokeBackgroundTaskIsCancelledByRuntimeShutdown(t *testing.T) {
	h := &composeBlockingTaskProbe{
		started: make(chan struct{}),
		stopped: make(chan error, 1),
	}
	rt := composeRuntimeFixture(t, h)
	observed := make(chan ComposeTaskCompletion, 2)
	rt.cfg.ComposeTaskCompletionObserver = func(
		_ context.Context,
		completion ComposeTaskCompletion,
	) {
		observed <- completion
	}

	got := rt.ComposeInvoke(context.Background(), compose.FunctionID(h.Name()), []byte("event"))
	if got.Kind != compose.ResultDeferred {
		t.Fatalf("ComposeInvoke result = %q, want Deferred", got.Kind)
	}
	select {
	case <-h.started:
	case <-time.After(2 * time.Second):
		t.Fatal("compose task did not start")
	}

	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- rt.Shutdown() }()

	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Shutdown hung on a compose task launched with context.Background()")
	}
	select {
	case err := <-h.stopped:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("task context error = %v, want context.Canceled", err)
		}
	default:
		t.Fatal("compose task did not observe mandatory runtime cancellation")
	}
	handle := string(got.Payload)
	requireCancelledComposeCompletion(t, rt, handle)
	requireSingleObservedComposeCompletion(t, observed, handle)
}

func TestComposeInvokeTaskPreservesCallerCancellation(t *testing.T) {
	h := &composeBlockingTaskProbe{
		started: make(chan struct{}),
		stopped: make(chan error, 1),
	}
	rt := composeRuntimeFixture(t, h)
	observed := make(chan ComposeTaskCompletion, 2)
	rt.cfg.ComposeTaskCompletionObserver = func(
		_ context.Context,
		completion ComposeTaskCompletion,
	) {
		observed <- completion
	}
	callerCtx, cancelCaller := context.WithCancel(context.Background())

	got := rt.ComposeInvoke(callerCtx, compose.FunctionID(h.Name()), []byte("event"))
	if got.Kind != compose.ResultDeferred {
		t.Fatalf("ComposeInvoke result = %q, want Deferred", got.Kind)
	}
	select {
	case <-h.started:
	case <-time.After(2 * time.Second):
		t.Fatal("compose task did not start")
	}
	cancelCaller()

	select {
	case err := <-h.stopped:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("task context error = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("compose task ignored caller cancellation")
	}
	rt.wg.Wait()
	handle := string(got.Payload)
	requireCancelledComposeCompletion(t, rt, handle)
	requireSingleObservedComposeCompletion(t, observed, handle)
}

func TestComposeInvokeTaskRejectsAfterRuntimeShutdown(t *testing.T) {
	h := &composeTaskProbe{
		name:  "compose.after-shutdown",
		scope: handlers.TenantScopeNone,
		ran:   make(chan context.Context, 1),
	}
	rt := composeRuntimeFixture(t, h)
	if err := rt.Shutdown(); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	got := rt.ComposeInvoke(context.Background(), compose.FunctionID(h.name), []byte("event"))
	if got.Kind != compose.ResultFailure {
		t.Fatalf("ComposeInvoke result = %q, want Failure", got.Kind)
	}
	if got := rt.MeshMetrics()["compose_task_in_flight"]; got != int64(0) {
		t.Fatalf("compose_task_in_flight after shutdown rejection = %#v, want 0", got)
	}
	select {
	case <-h.ran:
		t.Fatal("compose task executed after runtime shutdown")
	default:
	}
}

type composeMultiBlockingProbe struct {
	started chan string
}

func (*composeMultiBlockingProbe) Name() string { return "compose.multi-blocking-task" }
func (*composeMultiBlockingProbe) Role() string { return "compose-test" }
func (*composeMultiBlockingProbe) RequiresAuth() bool {
	return false
}
func (*composeMultiBlockingProbe) AllowedAuthTypes() []string { return nil }
func (*composeMultiBlockingProbe) Scopes() []string           { return nil }
func (*composeMultiBlockingProbe) TenantScope() handlers.TenantScope {
	return handlers.TenantScopeNone
}
func (*composeMultiBlockingProbe) AllowedTenants() []string { return nil }
func (h *composeMultiBlockingProbe) ExecuteTask(
	ctx context.Context,
	task *handlers.Task,
) (*handlers.TaskResult, error) {
	principal, _ := ctx.Value(composePrincipalContextKey{}).(ports.ExecutionPrincipal)
	owner := ""
	if principal != nil {
		owner = principal.OwnerKey().Fingerprint()
	}
	h.started <- owner
	<-ctx.Done()
	return &handlers.TaskResult{
		TaskID: task.ID,
		Status: handlers.TaskStatusFailed,
		Error:  ctx.Err().Error(),
	}, ctx.Err()
}

func TestComposeInvokeRequiresOpaqueExecutionPrincipal(t *testing.T) {
	h := &composeTaskProbe{
		name:  "compose.owner-required",
		scope: handlers.TenantScopeNone,
		ran:   make(chan context.Context, 1),
	}
	rt := composeRuntimeFixture(t, h)
	rt.rpcServer.SetAuthValidator(&composeAuthWithoutPrincipalReader{})

	got := rt.ComposeInvoke(context.Background(), compose.FunctionID(h.Name()), nil)
	if got.Kind != compose.ResultFailure || got.Err != composeTaskPrincipalMissingError {
		t.Fatalf("ComposeInvoke result = %+v, want missing-principal failure", got)
	}
	if rt.composeTaskInFlight.Load() != 0 {
		t.Fatalf("missing principal reserved capacity: %d", rt.composeTaskInFlight.Load())
	}
	select {
	case <-h.ran:
		t.Fatal("task without an opaque execution principal ran")
	default:
	}
}

func TestComposeInvokeReservesGlobalAndPerOwnerCapacityAtomically(t *testing.T) {
	h := &composeMultiBlockingProbe{started: make(chan string, 2)}
	rt := composeRuntimeFixture(t, h)
	rt.cfg.ComposeTaskMaxInFlight = 2
	rt.cfg.ComposeTaskMaxInFlightPerOwner = 1
	p1 := newComposeTestPrincipal("owner-one")
	p2 := newComposeTestPrincipal("owner-two")
	p3 := newComposeTestPrincipal("owner-three")
	ctx1, cancel1 := context.WithCancel(composePrincipalContext(p1))
	ctx2, cancel2 := context.WithCancel(composePrincipalContext(p2))

	first := rt.ComposeInvoke(ctx1, compose.FunctionID(h.Name()), nil)
	if first.Kind != compose.ResultDeferred {
		t.Fatalf("first owner admission = %+v, want Deferred", first)
	}
	if got := <-h.started; got != p1.OwnerKey().Fingerprint() {
		t.Fatalf("first running owner = %q, want %q", got, p1.OwnerKey().Fingerprint())
	}
	sameOwner := rt.ComposeInvoke(ctx1, compose.FunctionID(h.Name()), nil)
	if sameOwner.Kind != compose.ResultFailure ||
		sameOwner.Err != composeTaskOwnerAdmissionFullError {
		t.Fatalf("same-owner admission = %+v, want owner ceiling", sameOwner)
	}

	second := rt.ComposeInvoke(ctx2, compose.FunctionID(h.Name()), nil)
	if second.Kind != compose.ResultDeferred {
		t.Fatalf("second owner admission = %+v, want Deferred", second)
	}
	if got := <-h.started; got != p2.OwnerKey().Fingerprint() {
		t.Fatalf("second running owner = %q, want %q", got, p2.OwnerKey().Fingerprint())
	}
	global := rt.ComposeInvoke(
		composePrincipalContext(p3),
		compose.FunctionID(h.Name()),
		nil,
	)
	if global.Kind != compose.ResultFailure || global.Err != composeTaskAdmissionFullError {
		t.Fatalf("global admission = %+v, want global ceiling", global)
	}
	if got := rt.composeTaskInFlight.Load(); got != 2 {
		t.Fatalf("global in-flight = %d, want 2", got)
	}
	if got := rt.composeTaskRejected.Load(); got != 2 {
		t.Fatalf("admission rejections = %d, want 2", got)
	}

	cancel1()
	cancel2()
	rt.wg.Wait()
	if got := rt.composeTaskInFlight.Load(); got != 0 {
		t.Fatalf("global in-flight after settle = %d, want 0", got)
	}
	if len(rt.composeTaskByOwner) != 0 {
		t.Fatalf("owner reservations after settle = %v, want empty", rt.composeTaskByOwner)
	}
}

func TestComposeCompletionLookupReauthorizesExactOwnerAndHidesExistence(t *testing.T) {
	h := &composeTaskProbe{
		name:  "compose.owner-completion",
		scope: handlers.TenantScopeNone,
		ran:   make(chan context.Context, 2),
	}
	rt := composeRuntimeFixture(t, h)
	var revoked atomic.Bool
	p1 := newComposeTestPrincipal("completion-owner")
	p1.revoked = &revoked
	p2 := newComposeTestPrincipal("foreign-owner")
	event := []byte("owner-bound-request")

	first := rt.ComposeInvoke(
		composePrincipalContext(p1),
		compose.FunctionID(h.Name()),
		event,
	)
	second := rt.ComposeInvoke(
		composePrincipalContext(p1),
		compose.FunctionID(h.Name()),
		event,
	)
	if first.Kind != compose.ResultDeferred || second.Kind != compose.ResultDeferred {
		t.Fatalf("admissions = (%+v, %+v), want Deferred/Deferred", first, second)
	}
	rt.wg.Wait()
	firstHandle := string(first.Payload)
	secondHandle := string(second.Payload)
	if firstHandle == secondHandle ||
		strings.HasPrefix(firstHandle, "compose-") ||
		strings.HasPrefix(secondHandle, "compose-") {
		t.Fatalf("handles are predictable/colliding: %q %q", firstHandle, secondHandle)
	}
	for _, handle := range []string{firstHandle, secondHandle} {
		decoded, err := base64.RawURLEncoding.DecodeString(handle)
		if err != nil || len(decoded) != composeTaskHandleEntropyBytes {
			t.Fatalf("handle %q entropy = (%d, %v), want %d bytes", handle, len(decoded), err, composeTaskHandleEntropyBytes)
		}
	}

	if _, ok := rt.LookupComposeTaskCompletion(
		composePrincipalContext(p2),
		firstHandle,
	); ok {
		t.Fatal("foreign owner learned a completion exists")
	}
	if _, ok := rt.LookupComposeTaskCompletion(
		composePrincipalContext(p1),
		"absent-handle",
	); ok {
		t.Fatal("absent handle unexpectedly resolved")
	}
	completion, ok := rt.LookupComposeTaskCompletion(
		composePrincipalContext(p1),
		firstHandle,
	)
	if !ok || completion.Result.Kind != compose.ResultSuccess {
		t.Fatalf("owner completion = (%+v, %v), want success/true", completion, ok)
	}
	record, ok := rt.lookupComposeTaskCompletion(firstHandle)
	if !ok ||
		record.owner != p1.OwnerKey() ||
		record.requestDigest != sha256.Sum256(event) {
		t.Fatalf("private completion binding = %+v, want exact owner and request digest", record)
	}

	revoked.Store(true)
	if _, ok := rt.LookupComposeTaskCompletion(
		composePrincipalContext(p1),
		firstHandle,
	); ok {
		t.Fatal("revoked owner retained completion visibility")
	}
}

func TestComposeCompletionRetentionIsBoundedPerOwner(t *testing.T) {
	rt := &Runtime{}
	rt.cfg.ComposeTaskCompletionRetentionPerOwner = 1
	ownerA := newComposeTestPrincipal("retention-owner-a").OwnerKey()
	ownerB := newComposeTestPrincipal("retention-owner-b").OwnerKey()
	store := func(handle string, owner ports.ExecutionOwnerKey) {
		rt.recordComposeTaskCompletion(
			context.Background(),
			handle,
			compose.FunctionID("compose.retention"),
			owner,
			sha256.Sum256([]byte(handle)),
			nil,
			&handlers.TaskResult{
				TaskID: handle,
				Status: handlers.TaskStatusCompleted,
			},
			nil,
			nil,
		)
	}
	store("a-1", ownerA)
	store("b-1", ownerB)
	store("a-2", ownerA)

	if _, ok := rt.lookupComposeTaskCompletion("a-1"); ok {
		t.Fatal("oldest owner-A completion was not evicted")
	}
	if _, ok := rt.lookupComposeTaskCompletion("a-2"); !ok {
		t.Fatal("latest owner-A completion was evicted")
	}
	if _, ok := rt.lookupComposeTaskCompletion("b-1"); !ok {
		t.Fatal("owner-B completion was evicted by owner-A pressure")
	}
	if len(rt.composeTaskCompletionOrder) != 2 {
		t.Fatalf("global order = %v, want two live handles", rt.composeTaskCompletionOrder)
	}
}

type composeTriggerPrincipalProbe struct {
	principal      ports.ExecutionPrincipal
	err            error
	registration   chan compose.TriggerInvocation
	afterAuthorize func()
	releases       atomic.Int32
}

func (p *composeTriggerPrincipalProbe) AuthorizeTriggerFire(
	ctx context.Context,
	registration compose.TriggerInvocation,
) (context.Context, func(), error) {
	p.registration <- registration
	if p.err != nil {
		return nil, nil, p.err
	}
	authorized, release, err := p.principal.AuthorizeExecution(ctx)
	if err != nil {
		return nil, nil, err
	}
	if p.afterAuthorize != nil {
		p.afterAuthorize()
	}
	return authorized, func() {
		release()
		p.releases.Add(1)
	}, nil
}

// composePolicyLeasePrincipal is the service-policy concurrency shape used by
// the product provider: an authorization retains a read lease through the
// complete deferred dispatch, while revoke and replacement take the write
// lock. Waiting until TryRLock fails proves the writer is actually queued
// before the provider returns the authorized context to the runtime.
type composePolicyLeasePrincipal struct {
	mu         sync.RWMutex
	owner      ports.ExecutionOwnerKey
	revoked    atomic.Bool
	authorizes atomic.Int32
	releases   atomic.Int32
}

func newComposePolicyLeasePrincipal(identity string) *composePolicyLeasePrincipal {
	owner, err := ports.NewExecutionOwnerKey(identity)
	if err != nil {
		panic(err)
	}
	return &composePolicyLeasePrincipal{owner: owner}
}

func (p *composePolicyLeasePrincipal) OwnerKey() ports.ExecutionOwnerKey {
	return p.owner
}

func (p *composePolicyLeasePrincipal) AuthorizeExecution(
	ctx context.Context,
) (context.Context, func(), error) {
	p.authorizes.Add(1)
	p.mu.RLock()
	if p.revoked.Load() {
		p.mu.RUnlock()
		return ctx, nil, errors.New("compose service policy revoked")
	}
	var once sync.Once
	return context.WithValue(
			ctx,
			composePrincipalContextKey{},
			ports.ExecutionPrincipal(p),
		), func() {
			once.Do(func() {
				p.releases.Add(1)
				p.mu.RUnlock()
			})
		}, nil
}

func (p *composePolicyLeasePrincipal) invalidate() {
	p.mu.Lock()
	p.revoked.Store(true)
	p.mu.Unlock()
}

func TestComposeTriggerFireRequiresProductResolutionAndCurrentPrincipal(t *testing.T) {
	handlerRelease := make(chan struct{})
	h := &composeTaskProbe{
		name:  "compose.trigger-owned",
		scope: handlers.TenantScopeNone,
		ran:   make(chan context.Context, 1),
		run: func(_ context.Context, task *handlers.Task) (*handlers.TaskResult, error) {
			<-handlerRelease
			return &handlers.TaskResult{
				TaskID: task.ID,
				Status: handlers.TaskStatusCompleted,
			}, nil
		},
	}
	rt := composeRuntimeFixture(t, h)
	registration := compose.TriggerInvocation{
		ID:                     "trigger-1",
		Kind:                   compose.TriggerState,
		Function:               compose.FunctionID(h.Name()),
		RegistrationRevision:   "rev-9",
		RegistrationGeneration: 4,
		SpecDigest:             sha256.Sum256([]byte("immutable-spec")),
	}

	missing := rt.invokeComposeTrigger(context.Background(), registration, nil)
	if missing.Kind != compose.ResultFailure ||
		missing.Err != "compose trigger principal provider missing" {
		t.Fatalf("missing provider result = %+v", missing)
	}
	select {
	case <-h.ran:
		t.Fatal("trigger without a product principal executed")
	default:
	}

	rejectedProvider := &composeTriggerPrincipalProbe{
		err:          errors.New("activation revoked"),
		registration: make(chan compose.TriggerInvocation, 1),
	}
	rt.cfg.ComposeTriggerPrincipalProvider = rejectedProvider
	rejected := rt.invokeComposeTrigger(context.Background(), registration, nil)
	if rejected.Kind != compose.ResultFailure ||
		rejected.Err != "compose trigger principal rejected" {
		t.Fatalf("rejected provider result = %+v", rejected)
	}
	if got := <-rejectedProvider.registration; got != registration {
		t.Fatalf("rejected provider evidence = %+v, want %+v", got, registration)
	}

	provider := &composeTriggerPrincipalProbe{
		principal:    newComposeTestPrincipal("trigger-machine-owner"),
		registration: make(chan compose.TriggerInvocation, 1),
	}
	rt.cfg.ComposeTriggerPrincipalProvider = provider
	accepted := rt.invokeComposeTrigger(
		context.Background(),
		registration,
		[]byte("trigger-event"),
	)
	if accepted.Kind != compose.ResultDeferred {
		t.Fatalf("accepted provider result = %+v, want Deferred", accepted)
	}
	if got := <-provider.registration; got != registration {
		t.Fatalf("accepted provider evidence = %+v, want %+v", got, registration)
	}
	select {
	case <-h.ran:
	case <-time.After(time.Second):
		t.Fatal("product-authorized trigger did not reach its handler")
	}
	principal := provider.principal.(*composeTestPrincipal)
	if got := principal.authorizes.Load(); got != 1 {
		t.Fatalf("trigger principal authorizations = %d, want provider-only 1", got)
	}
	if got := provider.releases.Load(); got != 0 {
		t.Fatalf("trigger authority released before handler completion: %d", got)
	}
	close(handlerRelease)
	rt.wg.Wait()
	if got := provider.releases.Load(); got != 1 {
		t.Fatalf("trigger authority releases = %d, want 1", got)
	}
	record, ok := rt.lookupComposeTaskCompletion(string(accepted.Payload))
	if !ok {
		t.Fatal("trigger completion was not recorded")
	}
	if !record.hasTrigger || record.trigger != registration {
		t.Fatalf(
			"trigger completion evidence = (%+v, %v), want (%+v, true)",
			record.trigger,
			record.hasTrigger,
			registration,
		)
	}
}

func TestComposeTriggerFireQueuesServicePolicyWriterUntilDeferredCompletion(
	t *testing.T,
) {
	for _, writerKind := range []string{"revoke", "replace"} {
		t.Run(writerKind, func(t *testing.T) {
			handlerRelease := make(chan struct{})
			h := &composeTaskProbe{
				name:  "compose.trigger-policy-writer." + writerKind,
				scope: handlers.TenantScopeNone,
				ran:   make(chan context.Context, 1),
				run: func(
					_ context.Context,
					task *handlers.Task,
				) (*handlers.TaskResult, error) {
					<-handlerRelease
					return &handlers.TaskResult{
						TaskID: task.ID,
						Status: handlers.TaskStatusCompleted,
					}, nil
				},
			}
			rt := composeRuntimeFixture(t, h)
			registration := compose.TriggerInvocation{
				ID:                     "trigger-policy-writer-" + writerKind,
				Kind:                   compose.TriggerState,
				Function:               compose.FunctionID(h.Name()),
				RegistrationRevision:   "rev-policy-writer",
				RegistrationGeneration: 1,
				SpecDigest: sha256.Sum256(
					[]byte("service-policy-" + writerKind),
				),
			}

			principal := newComposePolicyLeasePrincipal(
				"trigger-policy-writer-owner-" + writerKind,
			)
			writerAttempted := make(chan struct{})
			writerDone := make(chan struct{})
			provider := &composeTriggerPrincipalProbe{
				principal:    principal,
				registration: make(chan compose.TriggerInvocation, 1),
				afterAuthorize: func() {
					go func() {
						close(writerAttempted)
						// Revoke and replacement share the exact write-lock
						// exclusion contract; both invalidate the old issuance.
						principal.invalidate()
						close(writerDone)
					}()
					<-writerAttempted
					for principal.mu.TryRLock() {
						principal.mu.RUnlock()
						runtime.Gosched()
					}
				},
			}
			rt.cfg.ComposeTriggerPrincipalProvider = provider

			invoked := make(chan compose.FunctionResult, 1)
			go func() {
				invoked <- rt.invokeComposeTrigger(
					context.Background(),
					registration,
					[]byte("trigger-event"),
				)
			}()
			var accepted compose.FunctionResult
			select {
			case accepted = <-invoked:
			case <-time.After(time.Second):
				t.Fatal(
					"trigger dispatch blocked behind its own queued " +
						writerKind + " writer",
				)
			}
			if accepted.Kind != compose.ResultDeferred {
				t.Fatalf(
					"accepted provider result = %+v, want Deferred",
					accepted,
				)
			}
			if got := <-provider.registration; got != registration {
				t.Fatalf(
					"provider evidence = %+v, want %+v",
					got,
					registration,
				)
			}
			select {
			case <-h.ran:
			case <-time.After(time.Second):
				t.Fatal("product-authorized trigger did not reach its handler")
			}
			if got := principal.authorizes.Load(); got != 1 {
				t.Fatalf(
					"trigger principal authorizations = %d, want provider-only 1",
					got,
				)
			}
			select {
			case <-writerDone:
				t.Fatal(
					"service-policy " + writerKind +
						" crossed the deferred handler lease",
				)
			default:
			}
			if got := provider.releases.Load(); got != 0 {
				t.Fatalf(
					"trigger authority released before handler completion: %d",
					got,
				)
			}

			close(handlerRelease)
			drained := make(chan struct{})
			go func() {
				rt.wg.Wait()
				close(drained)
			}()
			select {
			case <-drained:
			case <-time.After(time.Second):
				t.Fatal("deferred handler did not drain")
			}
			select {
			case <-writerDone:
			case <-time.After(time.Second):
				t.Fatal(
					"service-policy " + writerKind +
						" did not complete after trigger lease release",
				)
			}
			if got := provider.releases.Load(); got != 1 {
				t.Fatalf("trigger authority releases = %d, want 1", got)
			}
			if got := principal.releases.Load(); got != 1 {
				t.Fatalf("service-policy lease releases = %d, want 1", got)
			}
		})
	}
}
