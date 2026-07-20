/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package compose

import (
	"context"
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
	invoke := func(_ context.Context, fn FunctionID, event []byte) FunctionResult {
		calls.Store(fn, string(event))
		return FunctionResult{Kind: ResultSuccess}
	}
	return NewRegistry(context.Background(), spawn, invoke), &wg, &calls
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
	if v, ok := calls.Load(FunctionID("svc.fn")); !ok || v != "fired:t1" {
		t.Fatalf("invoke not observed: %v", v)
	}
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
