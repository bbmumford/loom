/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package handlers

import (
	"context"
	"sync/atomic"
	"testing"
)

type staticTaskAdmission struct{ accepting bool }

func (a staticTaskAdmission) Acquire() (func(), bool) {
	if !a.accepting {
		return nil, false
	}
	return func() {}, true
}

func TestResolveAtomicallyCapturesAdmissionBeforeSameNameReplacement(t *testing.T) {
	const name = "generation.atomic"
	var firstRan atomic.Bool
	var replacementRan atomic.Bool
	first := &taskDispatchProbe{
		name: name,
		run: func(context.Context, *Task) (*TaskResult, error) {
			firstRan.Store(true)
			return &TaskResult{Status: TaskStatusCompleted}, nil
		},
	}
	replacement := &taskDispatchProbe{
		name: name,
		run: func(context.Context, *Task) (*TaskResult, error) {
			replacementRan.Store(true)
			return &TaskResult{Status: TaskStatusCompleted}, nil
		},
	}

	reg := NewHandlerRegistry()
	firstScope, err := reg.OpenRegistrationScope(
		first.Role(),
		staticTaskAdmission{accepting: false},
	)
	if err != nil {
		t.Fatalf("OpenRegistrationScope(first): %v", err)
	}
	firstHandle, err := reg.RegisterTaskScoped(firstScope, first)
	if err != nil {
		t.Fatalf("RegisterTaskScoped(first): %v", err)
	}
	if err := reg.PublishRegistrationScope(firstScope); err != nil {
		t.Fatalf("PublishRegistrationScope(first): %v", err)
	}
	resolved, ok := reg.Resolve(name)
	if !ok {
		t.Fatal("Resolve(first) failed")
	}
	if !reg.UnregisterExact(firstHandle) {
		t.Fatal("UnregisterExact(first) failed")
	}
	replacementScope, err := reg.OpenRegistrationScope(
		replacement.Role(),
		staticTaskAdmission{accepting: true},
	)
	if err != nil {
		t.Fatalf("OpenRegistrationScope(replacement): %v", err)
	}
	if _, err := reg.RegisterTaskScoped(replacementScope, replacement); err != nil {
		t.Fatalf("RegisterTaskScoped(replacement): %v", err)
	}
	if err := reg.PublishRegistrationScope(replacementScope); err != nil {
		t.Fatalf("PublishRegistrationScope(replacement): %v", err)
	}

	oldResult, err := resolved.DispatchTaskWithAuth(
		context.Background(),
		&Task{ID: "old", Handler: name},
		nil,
	)
	if err != nil {
		t.Fatalf("old resolved dispatch: %v", err)
	}
	if oldResult == nil || oldResult.Status != TaskStatusFailed {
		t.Fatalf("old result = %+v, want failed admission", oldResult)
	}
	if firstRan.Load() {
		t.Fatal("old handler ran against the replacement generation")
	}

	currentResult, err := reg.DispatchTask(
		context.Background(),
		&Task{ID: "replacement", Handler: name},
	)
	if err != nil {
		t.Fatalf("replacement dispatch: %v", err)
	}
	if currentResult == nil || currentResult.Status != TaskStatusCompleted {
		t.Fatalf("replacement result = %+v, want completed", currentResult)
	}
	if !replacementRan.Load() {
		t.Fatal("replacement handler did not run")
	}
}
