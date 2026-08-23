/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"runtime/debug"
	"time"

	swarm "github.com/bbmumford/swarm"

	"github.com/bbmumford/loom/compose"
	"github.com/bbmumford/loom/node/handlers"
	"github.com/bbmumford/loom/ports"
)

const (
	composeTaskCompletionRetention         = 1024
	defaultComposeTaskOwnerRetention       = 64
	defaultComposeTaskMaxInFlight          = 256
	defaultComposeTaskOwnerMaxInFlight     = 32
	composeTaskHandleEntropyBytes          = 32
	composeTaskAdmissionFullError          = "compose task admission limit reached"
	composeTaskOwnerAdmissionFullError     = "compose task owner admission limit reached"
	composeTaskPrincipalMissingError       = "compose task execution principal missing"
	composeTaskPrincipalAuthorizationError = "compose task execution principal rejected"
)

// ComposeTaskCompletion is the terminal outcome correlated with a Deferred
// ComposeInvoke handle. Result uses the same four-way compose semantics as the
// initial invocation; Status preserves the task-specific terminal status.
type ComposeTaskCompletion struct {
	Handle      string
	Function    compose.FunctionID
	Status      handlers.TaskStatus
	Result      compose.FunctionResult
	CompletedAt time.Time
}

// composeTaskCompletionRecord is intentionally private. A Deferred handle is
// correlation, never bearer authority: every stored terminal record retains
// the immutable opaque owner captured at admission plus the exact request
// digest that produced it.
type composeTaskCompletionRecord struct {
	completion    ComposeTaskCompletion
	owner         ports.ExecutionOwnerKey
	requestDigest [sha256.Size]byte
	trigger       compose.TriggerInvocation
	hasTrigger    bool
}

// ComposeTaskCompletionObserver receives a completion after it has first been
// stored in the runtime's bounded history.
type ComposeTaskCompletionObserver func(context.Context, ComposeTaskCompletion)

// ComposeTriggerPrincipalProvider is the product-owned trigger authority
// boundary. It must re-read the authoritative registration, exact-match the
// immutable evidence captured by compose.Registry, verify current activation
// and revocation, and return a context containing a typed ExecutionPrincipal.
// Trigger ID, Spec, callback context and a prior human approver mint nothing.
type ComposeTriggerPrincipalProvider interface {
	AuthorizeTriggerFire(
		ctx context.Context,
		registration compose.TriggerInvocation,
	) (authorized context.Context, release func(), err error)
}

// This file is the Phase-2 composition bridge: it maps the iii-derived
// compose surfaces onto what the mesh already has.
//
//   REGISTERED — Registry().List()/AllHandlers() enumerate every function.
//   CALLABLE   — ComposeInvoke dispatches a FunctionID locally with the
//                4-way FunctionResult semantics; cross-mesh calls keep
//                using rpc.Call (forwarding fragilities untouched).
//   TRIGGERABLE— TriggerRegistry() arms compose.Trigger instances;
//                "subscribe" (swarm topic) and "cron" (interval) kinds are
//                built in; consumers late-bind "http"/"state" kinds (chi
//                attach, RoleMissing) via RegisterKind.
//   OBSERVABLE — the existing Prometheus export + /mesh/status wire
//                contracts are untouched; the scope tracker inside role
//                activation gives per-role function ownership.

// ComposeInvoke dispatches one FunctionID with an opaque event payload and
// maps the outcome onto compose.FunctionResult:
//
//	RPC handler   → Success/Failure (payload/error)
//	Task handler  → Deferred (accepted for async execution)
//	no handler    → Failure (ErrHandlerNotFound text)
func (rt *Runtime) ComposeInvoke(ctx context.Context, fn compose.FunctionID, event []byte) compose.FunctionResult {
	return rt.composeInvoke(ctx, fn, event, nil, nil)
}

// composeInvoke is the single task-admission path. trigger is immutable
// registry evidence, never authority; product authorization has already
// exact-matched it before this method is called, and the terminal record keeps
// the same value so completion can be attributed to the admitted revision.
func (rt *Runtime) composeInvoke(
	ctx context.Context,
	fn compose.FunctionID,
	event []byte,
	trigger *compose.TriggerInvocation,
	releaseTrigger func(),
) compose.FunctionResult {
	reg := rt.Registry()
	resolved, ok := reg.Resolve(string(fn))
	if !ok {
		if releaseTrigger != nil {
			releaseTrigger()
		}
		return compose.FunctionResult{Kind: compose.ResultFailure, Err: handlers.ErrHandlerNotFound.Error()}
	}
	meta := resolved.Meta()

	if _, isRPC := meta.(handlers.RPCHandler); isRPC {
		if releaseTrigger != nil {
			defer releaseTrigger()
		}
		// Snapshot both the exact registration and the runtime-injected
		// product validator before dispatch. Trigger callbacks are an
		// authority-bearing entry point: they may not use the legacy
		// unauthenticated registry path or re-resolve a same-name replacement.
		auth := rt.rpcServer.authValidator
		resp, err := resolved.DispatchRPCWithAuth(ctx, &handlers.RPCRequest{
			ID:      fmt.Sprintf("compose-%d", time.Now().UnixNano()),
			Handler: string(fn),
			Payload: event,
			Context: map[string]interface{}{},
		}, auth)
		switch {
		case err != nil:
			return compose.FunctionResult{Kind: compose.ResultFailure, Err: err.Error()}
		case resp == nil:
			return compose.FunctionResult{Kind: compose.ResultNoResult}
		case !resp.Success:
			return compose.FunctionResult{Kind: compose.ResultFailure, Err: resp.Error}
		default:
			return compose.FunctionResult{Kind: compose.ResultSuccess, Payload: resp.Payload}
		}
	}

	if _, isTask := meta.(handlers.TaskHandler); isTask {
		auth := rt.rpcServer.authValidator
		observer := rt.cfg.ComposeTaskCompletionObserver
		var (
			executionCtx     context.Context
			owner            ports.ExecutionOwnerKey
			releasePrincipal func()
			err              error
		)
		if releaseTrigger != nil {
			// AuthorizeTriggerFire already resolved the current product
			// principal and owns its service-policy lease. Re-authorizing the
			// same principal here is not only redundant: a writer queued
			// between two RWMutex reads can block the second read behind
			// itself while the first lease remains held. Reconfirm the opaque
			// owner from the authorized context without reacquiring policy.
			executionCtx = ctx
			_, owner, err = establishedComposePrincipal(ctx, auth)
			releasePrincipal = func() {}
		} else {
			executionCtx, owner, releasePrincipal, err =
				authorizeComposePrincipal(ctx, auth)
		}
		if err != nil {
			if releaseTrigger != nil {
				releaseTrigger()
			}
			return compose.FunctionResult{
				Kind: compose.ResultFailure,
				Err:  err.Error(),
			}
		}
		if admissionErr := rt.tryAcquireComposeTaskSlot(owner); admissionErr != "" {
			releasePrincipal()
			if releaseTrigger != nil {
				releaseTrigger()
			}
			return compose.FunctionResult{
				Kind: compose.ResultFailure,
				Err:  admissionErr,
			}
		}
		handle, err := newComposeTaskHandle()
		if err != nil {
			releasePrincipal()
			if releaseTrigger != nil {
				releaseTrigger()
			}
			rt.releaseComposeTaskSlot(owner)
			return compose.FunctionResult{
				Kind: compose.ResultFailure,
				Err:  "compose task handle generation failed",
			}
		}
		requestDigest := sha256.Sum256(event)
		task := &handlers.Task{
			ID:        handle,
			Handler:   string(fn),
			Payload:   append([]byte(nil), event...),
			CreatedAt: time.Now(),
			Metadata:  map[string]interface{}{},
		}
		rt.composeTaskSeq.Add(1)
		// Fire-and-track: the task runs on a runtime goroutine; the
		// trigger receives Deferred immediately (iii semantics).
		taskCtx, cancelTask := context.WithCancel(executionCtx)
		stopRuntimeCancel := context.AfterFunc(rt.ctx, cancelTask)
		if !rt.tryGo("compose.task."+string(fn), func() {
			if releaseTrigger != nil {
				defer releaseTrigger()
			}
			defer releasePrincipal()
			defer rt.releaseComposeTaskSlot(owner)
			defer cancelTask()
			defer stopRuntimeCancel()
			result, dispatchErr := dispatchResolvedComposeTask(
				taskCtx,
				fn,
				resolved,
				task,
				auth,
			)
			rt.recordComposeTaskCompletion(
				taskCtx,
				task.ID,
				fn,
				owner,
				requestDigest,
				trigger,
				result,
				dispatchErr,
				observer,
			)
			if dispatchErr != nil {
				dbgNode.Printf("compose deferred task %s failed: %v", fn, dispatchErr)
			} else if result != nil && result.Status != handlers.TaskStatusCompleted {
				dbgNode.Printf("compose deferred task %s completed with %s: %s", fn, result.Status, result.Error)
			}
		}) {
			releasePrincipal()
			if releaseTrigger != nil {
				releaseTrigger()
			}
			rt.releaseComposeTaskSlot(owner)
			stopRuntimeCancel()
			cancelTask()
			return compose.FunctionResult{Kind: compose.ResultFailure, Err: "node runtime is shutting down"}
		}
		return compose.FunctionResult{Kind: compose.ResultDeferred, Payload: []byte(handle)}
	}

	if releaseTrigger != nil {
		releaseTrigger()
	}
	return compose.FunctionResult{Kind: compose.ResultFailure, Err: handlers.ErrUnsupportedMode.Error()}
}

func (rt *Runtime) composeTaskInFlightLimit() int64 {
	if rt != nil && rt.cfg.ComposeTaskMaxInFlight > 0 {
		return int64(rt.cfg.ComposeTaskMaxInFlight)
	}
	return defaultComposeTaskMaxInFlight
}

func (rt *Runtime) composeTaskOwnerInFlightLimit() int64 {
	if rt != nil && rt.cfg.ComposeTaskMaxInFlightPerOwner > 0 {
		return int64(rt.cfg.ComposeTaskMaxInFlightPerOwner)
	}
	return defaultComposeTaskOwnerMaxInFlight
}

func (rt *Runtime) composeTaskOwnerRetention() int {
	if rt != nil && rt.cfg.ComposeTaskCompletionRetentionPerOwner > 0 {
		return rt.cfg.ComposeTaskCompletionRetentionPerOwner
	}
	return defaultComposeTaskOwnerRetention
}

func (rt *Runtime) tryAcquireComposeTaskSlot(owner ports.ExecutionOwnerKey) string {
	if rt == nil || !owner.Valid() {
		return composeTaskPrincipalMissingError
	}
	rt.composeTaskAdmissionMu.Lock()
	defer rt.composeTaskAdmissionMu.Unlock()
	current := rt.composeTaskInFlight.Load()
	if current >= rt.composeTaskInFlightLimit() {
		rt.composeTaskRejected.Add(1)
		return composeTaskAdmissionFullError
	}
	if rt.composeTaskByOwner == nil {
		rt.composeTaskByOwner = make(map[ports.ExecutionOwnerKey]int64)
	}
	if rt.composeTaskByOwner[owner] >= rt.composeTaskOwnerInFlightLimit() {
		rt.composeTaskRejected.Add(1)
		return composeTaskOwnerAdmissionFullError
	}
	rt.composeTaskByOwner[owner]++
	rt.composeTaskInFlight.Add(1)
	return ""
}

func (rt *Runtime) releaseComposeTaskSlot(owner ports.ExecutionOwnerKey) {
	rt.composeTaskAdmissionMu.Lock()
	defer rt.composeTaskAdmissionMu.Unlock()
	if !owner.Valid() || rt.composeTaskByOwner[owner] <= 0 {
		panic("compose task owner in-flight counter underflow")
	}
	if rt.composeTaskByOwner[owner] == 1 {
		delete(rt.composeTaskByOwner, owner)
	} else {
		rt.composeTaskByOwner[owner]--
	}
	if remaining := rt.composeTaskInFlight.Add(-1); remaining < 0 {
		panic("compose task in-flight counter underflow")
	}
}

func newComposeTaskHandle() (string, error) {
	var entropy [composeTaskHandleEntropyBytes]byte
	if _, err := rand.Read(entropy[:]); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(entropy[:]), nil
}

func dispatchResolvedComposeTask(
	ctx context.Context,
	fn compose.FunctionID,
	resolved handlers.ResolvedHandler,
	task *handlers.Task,
	auth ports.AuthValidator,
) (result *handlers.TaskResult, dispatchErr error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			dispatchErr = fmt.Errorf("task handler panic: %v", recovered)
			result = &handlers.TaskResult{
				TaskID: task.ID,
				Status: handlers.TaskStatusFailed,
				Error:  dispatchErr.Error(),
			}
			dbgNode.Printf(
				"compose deferred task %s panicked: %v\n%s",
				fn,
				recovered,
				debug.Stack(),
			)
		}
	}()
	return resolved.DispatchTaskWithAuth(ctx, task, auth)
}

// LookupComposeTaskCompletion returns a defensive copy only when the current
// product-private execution principal has the same opaque owner captured at
// admission. A false result deliberately conflates running, absent, evicted,
// and unauthorized handles. Handles are correlation, never bearer authority.
func (rt *Runtime) LookupComposeTaskCompletion(
	ctx context.Context,
	handle string,
) (ComposeTaskCompletion, bool) {
	if rt == nil || handle == "" || rt.rpcServer == nil {
		return ComposeTaskCompletion{}, false
	}
	_, owner, release, err := authorizeComposePrincipal(ctx, rt.rpcServer.authValidator)
	if err != nil {
		return ComposeTaskCompletion{}, false
	}
	defer release()
	record, ok := rt.lookupComposeTaskCompletion(handle)
	if !ok || record.owner != owner {
		return ComposeTaskCompletion{}, false
	}
	completion := record.completion
	completion.Result.Payload = append([]byte(nil), completion.Result.Payload...)
	return completion, true
}

func (rt *Runtime) lookupComposeTaskCompletion(
	handle string,
) (composeTaskCompletionRecord, bool) {
	if rt == nil || handle == "" {
		return composeTaskCompletionRecord{}, false
	}
	rt.composeTaskCompletionMu.RLock()
	record, ok := rt.composeTaskCompletions[handle]
	rt.composeTaskCompletionMu.RUnlock()
	return record, ok
}

func (rt *Runtime) recordComposeTaskCompletion(
	ctx context.Context,
	handle string,
	fn compose.FunctionID,
	owner ports.ExecutionOwnerKey,
	requestDigest [sha256.Size]byte,
	trigger *compose.TriggerInvocation,
	taskResult *handlers.TaskResult,
	dispatchErr error,
	observer ComposeTaskCompletionObserver,
) {
	completion := composeTaskCompletion(handle, fn, taskResult, dispatchErr)
	record := composeTaskCompletionRecord{
		completion:    completion,
		owner:         owner,
		requestDigest: requestDigest,
	}
	if trigger != nil {
		record.trigger = *trigger
		record.hasTrigger = true
	}

	rt.composeTaskCompletionMu.Lock()
	if rt.composeTaskCompletions == nil {
		rt.composeTaskCompletions = make(map[string]composeTaskCompletionRecord)
	}
	if rt.composeTaskCompletionOwner == nil {
		rt.composeTaskCompletionOwner = make(map[ports.ExecutionOwnerKey][]string)
	}
	if _, exists := rt.composeTaskCompletions[completion.Handle]; exists {
		rt.composeTaskCompletionMu.Unlock()
		return
	}
	rt.composeTaskCompletionOrder = append(rt.composeTaskCompletionOrder, completion.Handle)
	rt.composeTaskCompletionOwner[owner] = append(
		rt.composeTaskCompletionOwner[owner],
		completion.Handle,
	)
	rt.composeTaskCompletions[completion.Handle] = record
	for len(rt.composeTaskCompletionOwner[owner]) > rt.composeTaskOwnerRetention() {
		oldest := rt.composeTaskCompletionOwner[owner][0]
		rt.removeComposeTaskCompletionLocked(oldest)
	}
	for len(rt.composeTaskCompletionOrder) > composeTaskCompletionRetention {
		oldest := rt.composeTaskCompletionOrder[0]
		rt.removeComposeTaskCompletionLocked(oldest)
	}
	rt.composeTaskCompletionMu.Unlock()

	if observer != nil {
		observed := completion
		observed.Result.Payload = append([]byte(nil), completion.Result.Payload...)
		observer(ctx, observed)
	}
}

func (rt *Runtime) removeComposeTaskCompletionLocked(handle string) {
	record, ok := rt.composeTaskCompletions[handle]
	if !ok {
		return
	}
	delete(rt.composeTaskCompletions, handle)
	rt.composeTaskCompletionOrder = removeComposeTaskHandle(
		rt.composeTaskCompletionOrder,
		handle,
	)
	ownerHandles := removeComposeTaskHandle(
		rt.composeTaskCompletionOwner[record.owner],
		handle,
	)
	if len(ownerHandles) == 0 {
		delete(rt.composeTaskCompletionOwner, record.owner)
	} else {
		rt.composeTaskCompletionOwner[record.owner] = ownerHandles
	}
}

func removeComposeTaskHandle(handles []string, target string) []string {
	for i, handle := range handles {
		if handle == target {
			copy(handles[i:], handles[i+1:])
			return handles[:len(handles)-1]
		}
	}
	return handles
}

func authorizeComposePrincipal(
	ctx context.Context,
	auth ports.AuthValidator,
) (context.Context, ports.ExecutionOwnerKey, func(), error) {
	principal, owner, err := establishedComposePrincipal(ctx, auth)
	if err != nil {
		return ctx, ports.ExecutionOwnerKey{}, nil, err
	}
	authorized, release, err := principal.AuthorizeExecution(ctx)
	if err != nil || authorized == nil || release == nil {
		if release != nil {
			release()
		}
		return ctx, ports.ExecutionOwnerKey{}, nil, errors.New(composeTaskPrincipalAuthorizationError)
	}
	rechecked, _, recheckErr := establishedComposePrincipal(authorized, auth)
	if recheckErr != nil || rechecked.OwnerKey() != owner {
		release()
		return ctx, ports.ExecutionOwnerKey{}, nil, errors.New(composeTaskPrincipalAuthorizationError)
	}
	return authorized, owner, release, nil
}

func establishedComposePrincipal(
	ctx context.Context,
	auth ports.AuthValidator,
) (ports.ExecutionPrincipal, ports.ExecutionOwnerKey, error) {
	if auth == nil {
		return nil, ports.ExecutionOwnerKey{}, errors.New(composeTaskPrincipalMissingError)
	}
	reader, ok := auth.(ports.ExecutionPrincipalReader)
	if !ok {
		return nil, ports.ExecutionOwnerKey{}, errors.New(composeTaskPrincipalMissingError)
	}
	principal, ok := reader.ExecutionPrincipal(ctx)
	if !ok || principal == nil || !principal.OwnerKey().Valid() {
		return nil, ports.ExecutionOwnerKey{}, errors.New(composeTaskPrincipalMissingError)
	}
	return principal, principal.OwnerKey(), nil
}

func composeTaskCompletion(
	handle string,
	fn compose.FunctionID,
	taskResult *handlers.TaskResult,
	dispatchErr error,
) ComposeTaskCompletion {
	completedAt := time.Now()
	status := handlers.TaskStatusFailed
	if taskResult != nil {
		status = taskResult.Status
		if !taskResult.CompletedAt.IsZero() {
			completedAt = taskResult.CompletedAt
		}
	}

	result := compose.FunctionResult{Kind: compose.ResultFailure}
	switch {
	case dispatchErr != nil:
		result.Err = dispatchErr.Error()
		if taskResult != nil && taskResult.Error != "" {
			result.Err = taskResult.Error
		}
	case taskResult == nil:
		result.Err = "task returned no result"
	case taskResult.Status == handlers.TaskStatusCompleted:
		result.Kind = compose.ResultSuccess
		result.Payload = append([]byte(nil), taskResult.Payload...)
	default:
		result.Err = taskResult.Error
		if result.Err == "" {
			result.Err = fmt.Sprintf("task completed with status %s", taskResult.Status)
		}
	}

	return ComposeTaskCompletion{
		Handle:      handle,
		Function:    fn,
		Status:      status,
		Result:      result,
		CompletedAt: completedAt,
	}
}

// TriggerRegistry returns the runtime's compose trigger registry, creating
// it on first use with the built-in kinds armed. All trigger watch
// goroutines bind rt.Context() through rt.Go (H7 rule).
func (rt *Runtime) TriggerRegistry() *compose.Registry {
	rt.lifecycleMu.Lock()
	defer rt.lifecycleMu.Unlock()
	if rt.composeReg != nil {
		return rt.composeReg
	}
	spawn := func(name string, fn func(ctx context.Context)) {
		rt.Go(name, func() { fn(rt.ctx) })
	}
	reg := compose.NewRegistry(rt.ctx, spawn, rt.invokeComposeTrigger)
	if rt.goClosed {
		reg.Close()
		rt.composeReg = reg
		return reg
	}
	_ = reg.RegisterKind(compose.TriggerSubscribe, &swarmSubscribeKind{rt: rt})
	_ = reg.RegisterKind(compose.TriggerCron, &intervalCronKind{})
	rt.composeReg = reg
	return reg
}

func (rt *Runtime) invokeComposeTrigger(
	ctx context.Context,
	registration compose.TriggerInvocation,
	event []byte,
) compose.FunctionResult {
	provider := rt.cfg.ComposeTriggerPrincipalProvider
	if provider == nil {
		return compose.FunctionResult{
			Kind: compose.ResultFailure,
			Err:  "compose trigger principal provider missing",
		}
	}
	authorized, release, err := provider.AuthorizeTriggerFire(ctx, registration)
	if err != nil || authorized == nil || release == nil {
		if release != nil {
			release()
		}
		return compose.FunctionResult{
			Kind: compose.ResultFailure,
			Err:  "compose trigger principal rejected",
		}
	}
	return rt.composeInvoke(
		authorized,
		registration.Function,
		event,
		&registration,
		release,
	)
}

// swarmSubscribeKind arms "subscribe" triggers: every record accepted on
// the spec'd swarm topic invokes the function with the record body.
// Spec JSON: {"topic": "<swarm topic>"}.
type swarmSubscribeKind struct{ rt *Runtime }

func (k *swarmSubscribeKind) Arm(ctx context.Context, t compose.Trigger, invoke func(ctx context.Context, fn compose.FunctionID, event []byte) compose.FunctionResult) error {
	var spec struct {
		Topic string `json:"topic"`
	}
	if err := json.Unmarshal(t.Spec, &spec); err != nil || spec.Topic == "" {
		return fmt.Errorf("subscribe trigger %s: invalid spec (want {\"topic\":…}): %v", t.ID, err)
	}
	if k.rt.swarm == nil || k.rt.swarm.Node == nil {
		return fmt.Errorf("subscribe trigger %s: swarm not initialized", t.ID)
	}
	unsub, err := k.rt.swarm.Node.Subscribe(swarm.Topic(spec.Topic), func(r swarm.Record) error {
		if ctx.Err() != nil {
			return nil
		}
		invoke(ctx, t.Function, r.Body)
		return nil
	})
	if err != nil {
		return err
	}
	<-ctx.Done()
	unsub()
	return nil
}

// intervalCronKind arms "cron" triggers on a fixed interval.
// Spec JSON: {"everyMs": N}. Fan-in coordination across nodes belongs to
// the claim-set (plan §2.5 REJECTS a central cron lock) — a cron trigger
// fires on every node that arms it; the invoked function decides whether
// this node acts (e.g. by claim ranking).
type intervalCronKind struct{}

func (k *intervalCronKind) Arm(ctx context.Context, t compose.Trigger, invoke func(ctx context.Context, fn compose.FunctionID, event []byte) compose.FunctionResult) error {
	var spec struct {
		EveryMs int64 `json:"everyMs"`
	}
	if err := json.Unmarshal(t.Spec, &spec); err != nil || spec.EveryMs <= 0 {
		return fmt.Errorf("cron trigger %s: invalid spec (want {\"everyMs\":N>0}): %v", t.ID, err)
	}
	ticker := time.NewTicker(time.Duration(spec.EveryMs) * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case now := <-ticker.C:
			event, _ := json.Marshal(map[string]int64{"firedAtUnixMs": now.UnixMilli()})
			invoke(ctx, t.Function, event)
		}
	}
}
