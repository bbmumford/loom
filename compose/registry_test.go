/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package compose

import (
	"context"
	"crypto/sha256"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// blockingKind arms instances that fire once then block until torn down.
type blockingKind struct {
	armed atomic.Int32
}

func (k *blockingKind) Arm(ctx context.Context, t Trigger, invoke func(ctx context.Context, fn FunctionID, event []byte) FunctionResult) error {
	k.armed.Add(1)
	invoke(ctx, t.Function, []byte("fired:"+t.ID))
	<-ctx.Done()
	k.armed.Add(-1)
	return nil
}

type retainedCallback struct {
	fn func(context.Context, FunctionID, []byte) FunctionResult
	t  Trigger
}

type retainingKind struct {
	callbacks chan retainedCallback
}

func (k *retainingKind) Arm(
	ctx context.Context,
	t Trigger,
	invoke func(context.Context, FunctionID, []byte) FunctionResult,
) error {
	k.callbacks <- retainedCallback{fn: invoke, t: t}
	<-ctx.Done()
	return nil
}

type returningKind struct {
	callback chan retainedCallback
}

func (k *returningKind) Arm(
	_ context.Context,
	t Trigger,
	invoke func(context.Context, FunctionID, []byte) FunctionResult,
) error {
	k.callback <- retainedCallback{fn: invoke, t: t}
	return nil
}

type asyncReturningKind struct {
	started <-chan struct{}
	result  chan FunctionResult
}

func (k *asyncReturningKind) Arm(
	ctx context.Context,
	t Trigger,
	invoke func(context.Context, FunctionID, []byte) FunctionResult,
) error {
	go func() {
		k.result <- invoke(ctx, t.Function, nil)
	}()
	// Ensure the callback has crossed the exact currentness/lease boundary
	// before Arm returns. The registry must then invalidate the entry and
	// wait for that already-admitted product invocation.
	<-k.started
	return nil
}

func newTestRegistry(t *testing.T) (*Registry, *sync.WaitGroup, *sync.Map) {
	t.Helper()
	var wg sync.WaitGroup
	var calls sync.Map // FunctionID → event payload
	spawn := func(name string, fn func(ctx context.Context)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fn(context.Background())
		}()
	}
	invoke := func(
		_ context.Context,
		registration TriggerInvocation,
		event []byte,
	) FunctionResult {
		calls.Store(registration.Function, string(event))
		return FunctionResult{Kind: ResultSuccess}
	}
	return NewRegistry(context.Background(), spawn, invoke), &wg, &calls
}

func TestTriggerRegistrationReceiptAndExactRemovalRejectReplacement(t *testing.T) {
	reg, _, _ := newTestRegistry(t)
	spec := []byte(`{"selector":"one"}`)
	trigger := Trigger{
		ID:                   "exact-receipt",
		Kind:                 TriggerState,
		Function:             "svc.first",
		RegistrationRevision: "revision-one",
		Spec:                 spec,
	}
	first, err := reg.RegisterTriggerWithInvocation(trigger)
	if err != nil {
		t.Fatal(err)
	}
	spec[0] = '!'
	if first.SpecDigest != sha256.Sum256([]byte(`{"selector":"one"}`)) {
		t.Fatal("registration receipt retained caller-owned spec bytes")
	}
	if current, ok := reg.CurrentTriggerInvocation(trigger.ID); !ok ||
		current != first {
		t.Fatalf("current receipt = (%+v, %v), want (%+v, true)", current, ok, first)
	}

	trigger.Function = "svc.replacement"
	trigger.RegistrationRevision = "revision-two"
	trigger.Spec = []byte(`{"selector":"two"}`)
	second, err := reg.RegisterTriggerWithInvocation(trigger)
	if err != nil {
		t.Fatal(err)
	}
	if second.RegistrationGeneration == first.RegistrationGeneration {
		t.Fatal("same-ID replacement reused a registration generation")
	}
	if err := reg.RemoveTriggerExact(first); err == nil {
		t.Fatal("stale receipt removed a same-ID replacement")
	}
	if current, ok := reg.CurrentTriggerInvocation(trigger.ID); !ok ||
		current != second {
		t.Fatalf("replacement after stale remove = (%+v, %v)", current, ok)
	}
	if err := reg.RemoveTriggerExact(second); err != nil {
		t.Fatalf("RemoveTriggerExact(current): %v", err)
	}
	if _, ok := reg.CurrentTriggerInvocation(trigger.ID); ok {
		t.Fatal("exactly removed registration remains current")
	}
}

func TestTriggerRegistrationGenerationExhaustionIsPermanent(t *testing.T) {
	reg, _, _ := newTestRegistry(t)
	reg.nextGen = ^uint64(0)
	trigger := Trigger{
		ID:       "exhausted",
		Kind:     TriggerState,
		Function: "svc.exhausted",
	}
	for attempt := 0; attempt < 3; attempt++ {
		if _, err := reg.RegisterTriggerWithInvocation(trigger); err == nil {
			t.Fatalf("post-exhaustion attempt %d succeeded", attempt+1)
		}
	}
	if _, ok := reg.CurrentTriggerInvocation(trigger.ID); ok {
		t.Fatal("exhausted registration published an entry")
	}
}

func TestRetainedCallbackRequiresExactLiveRegistration(t *testing.T) {
	var wg sync.WaitGroup
	var calls atomic.Int32
	spawn := func(_ string, fn func(context.Context)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fn(context.Background())
		}()
	}
	reg := NewRegistry(
		context.Background(),
		spawn,
		func(context.Context, TriggerInvocation, []byte) FunctionResult {
			calls.Add(1)
			return FunctionResult{Kind: ResultSuccess}
		},
	)
	kind := &retainingKind{callbacks: make(chan retainedCallback, 4)}
	if err := reg.RegisterKind(TriggerState, kind); err != nil {
		t.Fatal(err)
	}
	trigger := Trigger{
		ID:                   "same",
		Kind:                 TriggerState,
		Function:             "svc.same",
		RegistrationRevision: "rev-same",
		Spec:                 []byte(`{"same":true}`),
	}
	if err := reg.RegisterTrigger(trigger); err != nil {
		t.Fatal(err)
	}
	first := <-kind.callbacks

	// Even byte-identical replacement is a distinct registration generation.
	if err := reg.RegisterTrigger(trigger); err != nil {
		t.Fatal(err)
	}
	second := <-kind.callbacks
	if got := first.fn(context.Background(), first.t.Function, nil); got.Kind != ResultFailure {
		t.Fatalf("retained callback after replacement = %+v, want failure", got)
	}
	if got := second.fn(context.Background(), second.t.Function, nil); got.Kind != ResultSuccess {
		t.Fatalf("current replacement callback = %+v, want success", got)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("product invocations after replacement = %d, want 1", got)
	}

	if err := reg.RemoveTrigger(trigger.ID); err != nil {
		t.Fatal(err)
	}
	if got := second.fn(context.Background(), second.t.Function, nil); got.Kind != ResultFailure {
		t.Fatalf("retained callback after removal = %+v, want failure", got)
	}

	if err := reg.RegisterTrigger(trigger); err != nil {
		t.Fatal(err)
	}
	third := <-kind.callbacks
	reg.Close()
	if got := third.fn(context.Background(), third.t.Function, nil); got.Kind != ResultFailure {
		t.Fatalf("retained callback after close = %+v, want failure", got)
	}
	wg.Wait()
	if got := calls.Load(); got != 1 {
		t.Fatalf("stale callbacks reached product invoke: calls=%d, want 1", got)
	}
}

func TestCallbackIsNotLiveAfterArmReturns(t *testing.T) {
	var wg sync.WaitGroup
	var calls atomic.Int32
	reg := NewRegistry(
		context.Background(),
		func(_ string, fn func(context.Context)) {
			wg.Add(1)
			go func() {
				defer wg.Done()
				fn(context.Background())
			}()
		},
		func(context.Context, TriggerInvocation, []byte) FunctionResult {
			calls.Add(1)
			return FunctionResult{Kind: ResultSuccess}
		},
	)
	kind := &returningKind{callback: make(chan retainedCallback, 1)}
	if err := reg.RegisterKind(TriggerState, kind); err != nil {
		t.Fatal(err)
	}
	if err := reg.RegisterTrigger(Trigger{
		ID:       "returned",
		Kind:     TriggerState,
		Function: "svc.returned",
	}); err != nil {
		t.Fatal(err)
	}
	retained := <-kind.callback
	wg.Wait()
	if reg.Armed("returned") {
		t.Fatal("entry remained armed after KindHandler.Arm returned")
	}
	if got := retained.fn(context.Background(), retained.t.Function, nil); got.Kind != ResultFailure {
		t.Fatalf("retained callback after Arm return = %+v, want failure", got)
	}
	if got := calls.Load(); got != 0 {
		t.Fatalf("returned instance reached product invoke: calls=%d", got)
	}
	reg.Close()
}

func TestTriggerLifecycleDrainsInvocationThatWonCurrentnessLease(t *testing.T) {
	for _, tc := range []struct {
		name      string
		operation func(*Registry, Trigger) error
		invalid   func(*Registry, Trigger) bool
	}{
		{
			name: "remove",
			operation: func(reg *Registry, trigger Trigger) error {
				return reg.RemoveTrigger(trigger.ID)
			},
			invalid: func(reg *Registry, trigger Trigger) bool {
				reg.mu.Lock()
				defer reg.mu.Unlock()
				_, present := reg.entries[trigger.ID]
				return !present
			},
		},
		{
			name: "identical replacement",
			operation: func(reg *Registry, trigger Trigger) error {
				return reg.RegisterTrigger(trigger)
			},
			invalid: func(reg *Registry, trigger Trigger) bool {
				reg.mu.Lock()
				defer reg.mu.Unlock()
				_, present := reg.entries[trigger.ID]
				return !present
			},
		},
		{
			name: "close",
			operation: func(reg *Registry, _ Trigger) error {
				reg.Close()
				return nil
			},
			invalid: func(reg *Registry, _ Trigger) bool {
				reg.mu.Lock()
				defer reg.mu.Unlock()
				return reg.closed
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var wg sync.WaitGroup
			started := make(chan struct{})
			release := make(chan struct{})
			reg := NewRegistry(
				context.Background(),
				func(_ string, fn func(context.Context)) {
					wg.Add(1)
					go func() {
						defer wg.Done()
						fn(context.Background())
					}()
				},
				func(context.Context, TriggerInvocation, []byte) FunctionResult {
					close(started)
					<-release
					return FunctionResult{Kind: ResultSuccess}
				},
			)
			kind := &retainingKind{
				callbacks: make(chan retainedCallback, 2),
			}
			if err := reg.RegisterKind(TriggerState, kind); err != nil {
				t.Fatal(err)
			}
			trigger := Trigger{
				ID:                   "drained",
				Kind:                 TriggerState,
				Function:             "svc.drained",
				RegistrationRevision: "rev-drained",
				Spec:                 []byte(`{"drained":true}`),
			}
			if err := reg.RegisterTrigger(trigger); err != nil {
				t.Fatal(err)
			}
			callback := <-kind.callbacks
			result := make(chan FunctionResult, 1)
			go func() {
				result <- callback.fn(
					context.Background(),
					callback.t.Function,
					nil,
				)
			}()
			<-started

			lifecycleDone := make(chan error, 1)
			go func() {
				lifecycleDone <- tc.operation(reg, trigger)
			}()
			waitFor(t, func() bool {
				return tc.invalid(reg, trigger)
			})
			select {
			case err := <-lifecycleDone:
				t.Fatalf("lifecycle returned before invocation drain: %v", err)
			default:
			}

			close(release)
			if got := <-result; got.Kind != ResultSuccess {
				t.Fatalf("admitted invocation = %+v, want success", got)
			}
			if err := <-lifecycleDone; err != nil {
				t.Fatalf("lifecycle: %v", err)
			}
			if tc.name == "identical replacement" {
				// The replacement may arm only after the old invocation has
				// drained and RegisterTrigger returns.
				<-kind.callbacks
			}
			reg.Close()
			wg.Wait()
		})
	}
}

func TestArmReturnDrainsInvocationItStarted(t *testing.T) {
	var wg sync.WaitGroup
	armDone := make(chan struct{})
	started := make(chan struct{})
	release := make(chan struct{})
	reg := NewRegistry(
		context.Background(),
		func(_ string, fn func(context.Context)) {
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer close(armDone)
				fn(context.Background())
			}()
		},
		func(context.Context, TriggerInvocation, []byte) FunctionResult {
			close(started)
			<-release
			return FunctionResult{Kind: ResultSuccess}
		},
	)
	kind := &asyncReturningKind{
		started: started,
		result:  make(chan FunctionResult, 1),
	}
	if err := reg.RegisterKind(TriggerState, kind); err != nil {
		t.Fatal(err)
	}
	if err := reg.RegisterTrigger(Trigger{
		ID:       "async-return",
		Kind:     TriggerState,
		Function: "svc.async-return",
	}); err != nil {
		t.Fatal(err)
	}
	<-started
	waitFor(t, func() bool {
		return !reg.Armed("async-return")
	})
	select {
	case <-armDone:
		t.Fatal("Arm goroutine returned before its invocation drained")
	default:
	}

	close(release)
	if got := <-kind.result; got.Kind != ResultSuccess {
		t.Fatalf("admitted invocation = %+v, want success", got)
	}
	<-armDone
	wg.Wait()
	reg.Close()
}

func TestTriggerInvocationCapturesImmutableRegistrationEvidence(t *testing.T) {
	var wg sync.WaitGroup
	invoked := make(chan TriggerInvocation, 2)
	spawn := func(_ string, fn func(ctx context.Context)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fn(context.Background())
		}()
	}
	reg := NewRegistry(
		context.Background(),
		spawn,
		func(
			_ context.Context,
			registration TriggerInvocation,
			_ []byte,
		) FunctionResult {
			invoked <- registration
			return FunctionResult{Kind: ResultSuccess}
		},
	)
	defer func() {
		reg.Close()
		wg.Wait()
	}()
	kind := &blockingKind{}
	if err := reg.RegisterKind(TriggerState, kind); err != nil {
		t.Fatal(err)
	}
	spec := []byte(`{"selector":"first"}`)
	if err := reg.RegisterTrigger(Trigger{
		ID:                   "stable",
		Kind:                 TriggerState,
		Function:             "svc.first",
		RegistrationRevision: "rev-1",
		Spec:                 spec,
	}); err != nil {
		t.Fatal(err)
	}
	copy(spec, []byte(`{"selector":"evil!"}`))

	first := <-invoked
	if first.ID != "stable" ||
		first.Function != "svc.first" ||
		first.RegistrationRevision != "rev-1" ||
		first.RegistrationGeneration == 0 ||
		first.SpecDigest != sha256.Sum256([]byte(`{"selector":"first"}`)) {
		t.Fatalf("first invocation evidence = %+v", first)
	}
	firstGeneration := first.RegistrationGeneration

	if err := reg.RegisterTrigger(Trigger{
		ID:                   "stable",
		Kind:                 TriggerState,
		Function:             "svc.second",
		RegistrationRevision: "rev-2",
		Spec:                 []byte(`{"selector":"second"}`),
	}); err != nil {
		t.Fatal(err)
	}
	second := <-invoked
	if second.Function != "svc.second" ||
		second.RegistrationRevision != "rev-2" ||
		second.RegistrationGeneration <= firstGeneration ||
		second.SpecDigest != sha256.Sum256([]byte(`{"selector":"second"}`)) {
		t.Fatalf("replacement invocation evidence = %+v", second)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("condition not reached in 2s")
}

func TestLateBindingInstanceBeforeKind(t *testing.T) {
	reg, wg, calls := newTestRegistry(t)
	defer func() { reg.Close(); wg.Wait() }()

	// Instance first — parks pending.
	if err := reg.RegisterTrigger(Trigger{ID: "t1", Kind: TriggerState, Function: "svc.fn"}); err != nil {
		t.Fatal(err)
	}
	if reg.Armed("t1") {
		t.Fatal("t1 must be pending before its kind registers")
	}

	// Kind arrives — pending instance replays.
	k := &blockingKind{}
	if err := reg.RegisterKind(TriggerState, k); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return reg.Armed("t1") && k.armed.Load() == 1 })
	waitFor(t, func() bool {
		v, ok := calls.Load(FunctionID("svc.fn"))
		return ok && v == "fired:t1"
	})
}

func TestKindBeforeInstanceArmsImmediately(t *testing.T) {
	reg, wg, _ := newTestRegistry(t)
	defer func() { reg.Close(); wg.Wait() }()

	k := &blockingKind{}
	if err := reg.RegisterKind(TriggerSubscribe, k); err != nil {
		t.Fatal(err)
	}
	if err := reg.RegisterTrigger(Trigger{ID: "t2", Kind: TriggerSubscribe, Function: "svc.sub"}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return reg.Armed("t2") })
}

func TestRemoveTriggerTearsDown(t *testing.T) {
	reg, wg, _ := newTestRegistry(t)
	defer func() { reg.Close(); wg.Wait() }()

	k := &blockingKind{}
	_ = reg.RegisterKind(TriggerState, k)
	_ = reg.RegisterTrigger(Trigger{ID: "t3", Kind: TriggerState, Function: "svc.fn"})
	waitFor(t, func() bool { return k.armed.Load() == 1 })

	if err := reg.RemoveTrigger("t3"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return k.armed.Load() == 0 })
	if len(reg.Triggers()) != 0 {
		t.Fatal("removed trigger still enumerated")
	}
}

func TestCloseTearsDownAllAndRejectsNew(t *testing.T) {
	reg, wg, _ := newTestRegistry(t)
	k := &blockingKind{}
	_ = reg.RegisterKind(TriggerState, k)
	_ = reg.RegisterTrigger(Trigger{ID: "a", Kind: TriggerState, Function: "f"})
	_ = reg.RegisterTrigger(Trigger{ID: "b", Kind: TriggerState, Function: "g"})
	waitFor(t, func() bool { return k.armed.Load() == 2 })

	reg.Close()
	wg.Wait()
	if k.armed.Load() != 0 {
		t.Fatal("Close must tear down all armed instances")
	}
	if err := reg.RegisterTrigger(Trigger{ID: "c", Kind: TriggerState, Function: "h"}); err == nil {
		t.Fatal("closed registry must reject registrations")
	}
}

func TestScopeTracker(t *testing.T) {
	tr := NewTracker()

	// Two concurrent owners must not bleed.
	tr.Begin("role:auth")
	tr.Begin("role:billing")
	tr.Note("role:auth", "hstles.auth.validate")
	tr.Note("role:billing", "hstles.billing.charge")
	tr.Note("role:auth", "hstles.auth.mint")

	got := tr.End("role:auth")
	if len(got) != 2 || got[0] != "hstles.auth.validate" || got[1] != "hstles.auth.mint" {
		t.Fatalf("auth scope = %v", got)
	}
	if owned := tr.Owned("role:billing"); len(owned) != 0 {
		t.Fatalf("billing owned before End = %v", owned)
	}
	if got := tr.End("role:billing"); len(got) != 1 || got[0] != "hstles.billing.charge" {
		t.Fatalf("billing scope = %v", got)
	}

	// Owned persists after End; Release forgets.
	if owned := tr.Owned("role:auth"); len(owned) != 2 {
		t.Fatalf("auth owned = %v", owned)
	}
	released := tr.Release("role:auth")
	if len(released) != 2 || len(tr.Owned("role:auth")) != 0 {
		t.Fatalf("release = %v, remaining = %v", released, tr.Owned("role:auth"))
	}

	// Note outside a bracket still attributes (never lost).
	tr.Note("role:auth", "hstles.auth.stray")
	if owned := tr.Owned("role:auth"); len(owned) != 1 || owned[0] != "hstles.auth.stray" {
		t.Fatalf("stray note = %v", owned)
	}
}
