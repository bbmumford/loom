/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
 */

package handlers

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/bbmumford/loom/pkg/rpc/scope"
)

// countingHandler is an RPC, task and stream handler that counts every run, so a
// test can tell a refusal that ran nothing from one the handler answered.
type countingHandler struct {
	name string
	role string
	runs atomic.Int32
}

func (h *countingHandler) Name() string                { return h.name }
func (h *countingHandler) Role() string                { return h.role }
func (h *countingHandler) RequiresAuth() bool          { return false }
func (h *countingHandler) AllowedAuthTypes() []string  { return nil }
func (h *countingHandler) Scopes() []string            { return nil }
func (h *countingHandler) Scope() Scope                { return ScopeNone }
func (h *countingHandler) AllowedPartitions() []string { return nil }

func (h *countingHandler) ExecuteRPC(context.Context, *RPCRequest) (*RPCResponse, error) {
	h.runs.Add(1)
	return &RPCResponse{Success: true}, nil
}

func (h *countingHandler) ExecuteTask(_ context.Context, task *Task) (*TaskResult, error) {
	h.runs.Add(1)
	return &TaskResult{TaskID: task.ID, Status: TaskStatusCompleted}, nil
}

func (h *countingHandler) HandleStream(context.Context, MessageStream) error {
	h.runs.Add(1)
	return nil
}

// A held registry refuses every dispatch, of a name it holds and of one it does
// not, through every entry point and through a resolution taken while it was
// held, with an error wrapping scope.ErrStarting, and runs nothing; once marked
// ready the same handler runs.
func TestAHeldRegistryRunsNothingUntilMarkedReady(t *testing.T) {
	reg := NewHandlerRegistry()
	h := &countingHandler{name: "test.hold.Run", role: "test.hold"}
	if err := reg.RegisterRPC(h); err != nil {
		t.Fatalf("RegisterRPC: %v", err)
	}
	reg.HoldUntilReady()
	if reg.Ready() {
		t.Fatal("a held registry reports itself ready")
	}
	ctx := context.Background()
	starting := func(what string, err error) {
		t.Helper()
		if !errors.Is(err, scope.ErrStarting) {
			t.Errorf("%s while held: %v, want an error wrapping scope.ErrStarting", what, err)
		}
	}
	for _, name := range []string{h.name, "test.hold.Unregistered"} {
		_, err := reg.Dispatch(ctx, &RPCRequest{Handler: name})
		starting("Dispatch "+name, err)
		_, err = reg.DispatchRPCWithAuth(ctx, &RPCRequest{Handler: name}, nil)
		starting("DispatchRPCWithAuth "+name, err)
		_, err = reg.DispatchTask(ctx, &Task{Handler: name})
		starting("DispatchTask "+name, err)
		starting("DispatchStream "+name, reg.DispatchStream(ctx, name, nil))
	}
	resolved, ok := reg.Resolve(h.name)
	if !ok {
		t.Fatal("Resolve finds no handler the held registry holds")
	}
	_, err := resolved.DispatchRPCWithAuth(ctx, &RPCRequest{Handler: h.name}, nil)
	starting("a resolution's DispatchRPCWithAuth", err)
	_, err = resolved.DispatchTaskWithAuth(ctx, &Task{Handler: h.name}, nil)
	starting("a resolution's DispatchTaskWithAuth", err)
	if n := h.runs.Load(); n != 0 {
		t.Fatalf("the handler ran %d time(s) while the registry was held, want none", n)
	}

	reg.MarkReady()
	if !reg.Ready() {
		t.Fatal("a registry marked ready reports itself held")
	}
	resp, err := reg.Dispatch(ctx, &RPCRequest{Handler: h.name})
	if _, failure := Outcome(resp, err); failure != nil {
		t.Fatalf("Dispatch once ready: %v", failure)
	}
	if n := h.runs.Load(); n != 1 {
		t.Fatalf("the handler ran %d time(s) once ready, want 1", n)
	}
}

// Catalogue lists what the registry dispatches for its endpoint, sorted by name:
// a handler registered directly and one registered for loom's own mesh layer are
// both dispatched, and only the first is catalogued.
func TestTheCatalogueLeavesOutLoomsOwnMeshHandlers(t *testing.T) {
	reg := NewHandlerRegistry()
	served := []*countingHandler{
		{name: "test.catalogue.B", role: "test.catalogue"},
		{name: "test.catalogue.A", role: "test.catalogue"},
	}
	for _, h := range served {
		if err := reg.RegisterRPC(h); err != nil {
			t.Fatalf("RegisterRPC(%s): %v", h.name, err)
		}
	}
	builtin := &countingHandler{name: "ping", role: "system"}
	if err := reg.RegisterInfrastructure(builtin); err != nil {
		t.Fatalf("RegisterInfrastructure: %v", err)
	}
	var names []string
	for _, meta := range reg.Catalogue() {
		names = append(names, meta.Name())
	}
	if len(names) != 2 || names[0] != "test.catalogue.A" || names[1] != "test.catalogue.B" {
		t.Fatalf("Catalogue() = %v, want [test.catalogue.A test.catalogue.B]", names)
	}
	resp, err := reg.Dispatch(context.Background(), &RPCRequest{Handler: "ping"})
	if _, failure := Outcome(resp, err); failure != nil || builtin.runs.Load() != 1 {
		t.Fatalf("the infrastructure handler is not dispatched: %v (runs %d)", failure, builtin.runs.Load())
	}
}
