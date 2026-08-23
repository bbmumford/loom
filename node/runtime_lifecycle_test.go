/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bbmumford/loom/compose"
)

func bareRuntimeForLifecycleTest(t *testing.T) *Runtime {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	return &Runtime{ctx: ctx, cancel: cancel}
}

func TestRuntimeGoRejectsAfterShutdown(t *testing.T) {
	rt := bareRuntimeForLifecycleTest(t)
	started := make(chan struct{})
	if !rt.tryGoCtx("lifecycle.blocked", func(ctx context.Context) {
		close(started)
		<-ctx.Done()
	}) {
		t.Fatal("runtime rejected work before shutdown")
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("tracked goroutine did not start")
	}

	if err := rt.Shutdown(); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	var ran atomic.Bool
	if rt.tryGo("lifecycle.after-shutdown", func() { ran.Store(true) }) {
		t.Fatal("runtime admission accepted work after shutdown")
	}
	if rt.tryGoCtx("lifecycle.ctx-after-shutdown", func(context.Context) { ran.Store(true) }) {
		t.Fatal("runtime context admission accepted work after shutdown")
	}
	rt.Go("lifecycle.public-after-shutdown", func() { ran.Store(true) })
	rt.GoCtx("lifecycle.public-ctx-after-shutdown", func(context.Context) { ran.Store(true) })
	rt.wg.Wait()
	if ran.Load() {
		t.Fatal("public Runtime.Go/GoCtx executed post-shutdown work")
	}
}

func TestRuntimeGoAdmissionIsSerializedWithShutdown(t *testing.T) {
	rt := bareRuntimeForLifecycleTest(t)

	const callers = 24
	const attempts = 40
	start := make(chan struct{})
	var callWG sync.WaitGroup
	var accepted atomic.Int64
	var completed atomic.Int64
	callWG.Add(callers)
	for i := 0; i < callers; i++ {
		go func(worker int) {
			defer callWG.Done()
			<-start
			for attempt := 0; attempt < attempts; attempt++ {
				if rt.tryGo(fmt.Sprintf("lifecycle.race.%d.%d", worker, attempt), func() {
					<-rt.ctx.Done()
					completed.Add(1)
				}) {
					accepted.Add(1)
				}
			}
		}(i)
	}

	close(start)
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- rt.Shutdown() }()
	callWG.Wait()
	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Shutdown did not drain goroutines admitted before closure")
	}
	if got, want := completed.Load(), accepted.Load(); got != want {
		t.Fatalf("completed goroutines = %d, accepted = %d", got, want)
	}
	if rt.tryGo("lifecycle.final-probe", func() {}) {
		t.Fatal("Runtime.Go reopened after concurrent shutdown")
	}
}

func TestRuntimeShutdownClosesComposeRegistry(t *testing.T) {
	rt := bareRuntimeForLifecycleTest(t)
	reg := rt.TriggerRegistry()

	const writers = 16
	start := make(chan struct{})
	var writerWG sync.WaitGroup
	writerWG.Add(writers)
	for i := 0; i < writers; i++ {
		go func(worker int) {
			defer writerWG.Done()
			<-start
			for attempt := 0; attempt < 40; attempt++ {
				_ = reg.RegisterTrigger(compose.Trigger{
					ID:       fmt.Sprintf("pending-%d-%d", worker, attempt),
					Kind:     compose.TriggerState,
					Function: compose.FunctionID("test.pending"),
				})
			}
		}(i)
	}

	close(start)
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- rt.Shutdown() }()
	writerWG.Wait()
	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatalf("Shutdown: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Shutdown deadlocked with concurrent trigger registration")
	}

	if got := rt.TriggerRegistry(); got != reg {
		t.Fatal("post-shutdown TriggerRegistry created a second registry")
	}
	if err := reg.RegisterTrigger(compose.Trigger{
		ID:       "after-shutdown",
		Kind:     compose.TriggerState,
		Function: compose.FunctionID("test.after"),
	}); err == nil {
		t.Fatal("compose registry accepted a trigger after runtime shutdown")
	}
}

func TestTriggerRegistryCreatedAfterShutdownIsClosed(t *testing.T) {
	rt := bareRuntimeForLifecycleTest(t)
	if err := rt.Shutdown(); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}

	reg := rt.TriggerRegistry()
	if err := reg.RegisterTrigger(compose.Trigger{
		ID:       "late",
		Kind:     compose.TriggerState,
		Function: compose.FunctionID("test.late"),
	}); err == nil {
		t.Fatal("post-shutdown TriggerRegistry returned a live registry")
	}
}
