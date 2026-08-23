/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package handlers

import (
	"context"
	"errors"
	"reflect"
	"testing"

	tenantScope "github.com/bbmumford/loom/pkg/rpc/scope"
)

type taskDispatchProbe struct {
	name         string
	requiresAuth bool
	scope        TenantScope
	run          func(context.Context, *Task) (*TaskResult, error)
}

func (h *taskDispatchProbe) Name() string               { return h.name }
func (h *taskDispatchProbe) Role() string               { return "task-dispatch-test" }
func (h *taskDispatchProbe) RequiresAuth() bool         { return h.requiresAuth }
func (h *taskDispatchProbe) AllowedAuthTypes() []string { return nil }
func (h *taskDispatchProbe) Scopes() []string           { return nil }
func (h *taskDispatchProbe) TenantScope() TenantScope {
	if h.scope == "" {
		return TenantScopeNone
	}
	return h.scope
}
func (h *taskDispatchProbe) AllowedTenants() []string { return nil }
func (h *taskDispatchProbe) ExecuteTask(ctx context.Context, task *Task) (*TaskResult, error) {
	return h.run(ctx, task)
}

type taskDispatchMiddleware struct {
	name   string
	events *[]string
	reject bool
}

func (m *taskDispatchMiddleware) Name() string { return m.name }

func (m *taskDispatchMiddleware) Before(
	ctx context.Context,
	_ string,
	_ *RPCRequest,
) (context.Context, error) {
	*m.events = append(*m.events, m.name+".before")
	if m.reject {
		return ctx, errors.New(m.name + " rejected")
	}
	return ctx, nil
}

func (m *taskDispatchMiddleware) After(
	_ context.Context,
	_ string,
	resp *RPCResponse,
	err error,
) (*RPCResponse, error) {
	*m.events = append(*m.events, m.name+".after")
	return resp, err
}

func TestDispatchTaskRequiresExecutionAuth(t *testing.T) {
	ran := false
	h := &taskDispatchProbe{
		name:         "secure.task",
		requiresAuth: true,
		run: func(context.Context, *Task) (*TaskResult, error) {
			ran = true
			return &TaskResult{Status: TaskStatusCompleted}, nil
		},
	}
	reg := NewHandlerRegistry()
	if err := reg.RegisterTask(h); err != nil {
		t.Fatalf("RegisterTask: %v", err)
	}

	result, err := reg.DispatchTask(context.Background(), &Task{ID: "task-auth", Handler: h.name})
	if err != nil {
		t.Fatalf("DispatchTask: %v", err)
	}
	if result == nil || result.Status != TaskStatusFailed {
		t.Fatalf("result = %+v, want failed authorization", result)
	}
	if ran {
		t.Fatal("RequiresAuth task executed without an authenticated principal")
	}
}

func TestTaskScopeIgnoresMetadataIdentityHints(t *testing.T) {
	var calls int
	var events []string
	handler := &taskDispatchProbe{
		name:  "org.task",
		scope: TenantScopeOrg,
		run: func(_ context.Context, task *Task) (*TaskResult, error) {
			calls++
			return &TaskResult{TaskID: task.ID, Status: TaskStatusCompleted}, nil
		},
	}
	registry := NewHandlerRegistry()
	registry.Use(&taskDispatchMiddleware{name: "scope-mw", events: &events})
	if err := registry.RegisterTask(handler); err != nil {
		t.Fatalf("RegisterTask: %v", err)
	}
	task := &Task{
		ID:      "org-task",
		Handler: handler.name,
		Metadata: map[string]interface{}{
			"tenantId":  "evil-camel",
			"tenant_id": "evil-snake",
			"orgId":     "evil-camel",
			"org_id":    "evil-snake",
		},
	}

	platformOnly := WithTransportTenant(context.Background(), "orbtr")
	result, err := registry.DispatchTask(platformOnly, task)
	if err != nil {
		t.Fatalf("map-only DispatchTask error: %v", err)
	}
	if result == nil || result.Status != TaskStatusFailed {
		t.Fatalf("map-only result=%+v, want failed", result)
	}
	if calls != 0 || len(events) != 0 {
		t.Fatalf("denied task reached handler/middleware: calls=%d events=%v", calls, events)
	}

	exact := tenantScope.WithAuthenticatedIdentity(platformOnly, tenantScope.AuthenticatedIdentity{
		PlatformTenantID: "orbtr",
		OrganizationID:   "org-1",
		UserID:           "user-1",
	})
	result, err = registry.DispatchTask(exact, task)
	if err != nil || result == nil || result.Status != TaskStatusCompleted {
		t.Fatalf("exact authenticated DispatchTask=(%+v, %v)", result, err)
	}
	if calls != 1 {
		t.Fatalf("handler calls=%d, want 1", calls)
	}
	if len(events) != 2 || events[0] != "scope-mw.before" || events[1] != "scope-mw.after" {
		t.Fatalf("middleware events=%v, want before/after", events)
	}
}

func TestDispatchTaskMiddlewareLifecycle(t *testing.T) {
	for _, tc := range []struct {
		name       string
		rejectB    bool
		handlerErr error
		want       []string
	}{
		{
			name: "success",
			want: []string{"a.before", "b.before", "handler", "b.after", "a.after"},
		},
		{
			name:       "handler error",
			handlerErr: errors.New("handler failed"),
			want:       []string{"a.before", "b.before", "handler", "b.after", "a.after"},
		},
		{
			name:    "later before rejection",
			rejectB: true,
			want:    []string{"a.before", "b.before", "a.after"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var events []string
			h := &taskDispatchProbe{
				name: "lifecycle." + tc.name,
				run: func(context.Context, *Task) (*TaskResult, error) {
					events = append(events, "handler")
					if tc.handlerErr != nil {
						return nil, tc.handlerErr
					}
					return &TaskResult{Status: TaskStatusCompleted}, nil
				},
			}
			reg := NewHandlerRegistry()
			reg.Use(&taskDispatchMiddleware{name: "a", events: &events})
			reg.Use(&taskDispatchMiddleware{name: "b", events: &events, reject: tc.rejectB})
			if err := reg.RegisterTask(h); err != nil {
				t.Fatalf("RegisterTask: %v", err)
			}

			result, err := reg.DispatchTask(
				context.Background(),
				&Task{ID: "task-lifecycle", Handler: h.name},
			)
			if tc.handlerErr != nil && !errors.Is(err, tc.handlerErr) {
				t.Fatalf("DispatchTask error = %v, want %v", err, tc.handlerErr)
			}
			if tc.handlerErr == nil && err != nil {
				t.Fatalf("DispatchTask: %v", err)
			}
			if result == nil {
				t.Fatal("DispatchTask returned nil result")
			}
			if result.TaskID != "task-lifecycle" {
				t.Fatalf("result TaskID = %q, want task-lifecycle", result.TaskID)
			}
			if !reflect.DeepEqual(events, tc.want) {
				t.Fatalf("events = %v, want %v", events, tc.want)
			}
		})
	}
}

func TestResolvedTaskDispatchUsesCapturedRegistration(t *testing.T) {
	const handlerName = "stable.task"
	var ran string
	first := &taskDispatchProbe{
		name: handlerName,
		run: func(context.Context, *Task) (*TaskResult, error) {
			ran = "first"
			return &TaskResult{Status: TaskStatusCompleted}, nil
		},
	}
	replacement := &taskDispatchProbe{
		name: handlerName,
		run: func(context.Context, *Task) (*TaskResult, error) {
			ran = "replacement"
			return &TaskResult{Status: TaskStatusCompleted}, nil
		},
	}

	reg := NewHandlerRegistry()
	if err := reg.RegisterTask(first); err != nil {
		t.Fatalf("RegisterTask(first): %v", err)
	}
	resolved, ok := reg.Resolve(handlerName)
	if !ok {
		t.Fatal("Resolve(first) = false")
	}
	if !reg.Unregister(handlerName) {
		t.Fatal("Unregister(first) = false")
	}
	if err := reg.RegisterTask(replacement); err != nil {
		t.Fatalf("RegisterTask(replacement): %v", err)
	}

	result, err := resolved.DispatchTaskWithAuth(
		context.Background(),
		&Task{ID: "stable-1", Handler: handlerName},
		nil,
	)
	if err != nil {
		t.Fatalf("resolved DispatchTaskWithAuth: %v", err)
	}
	if result == nil || result.Status != TaskStatusCompleted {
		t.Fatalf("resolved result = %+v, want completed", result)
	}
	if ran != "first" {
		t.Fatalf("resolved dispatch ran %q, want first", ran)
	}

	ran = ""
	if _, err := reg.DispatchTask(
		context.Background(),
		&Task{ID: "stable-2", Handler: handlerName},
	); err != nil {
		t.Fatalf("current DispatchTask: %v", err)
	}
	if ran != "replacement" {
		t.Fatalf("current registry dispatch ran %q, want replacement", ran)
	}
}

func TestResolvedTaskDispatchRejectsRelabelledTask(t *testing.T) {
	h := &taskDispatchProbe{
		name: "stable.original",
		run: func(context.Context, *Task) (*TaskResult, error) {
			t.Fatal("relabelled task reached handler")
			return nil, nil
		},
	}
	reg := NewHandlerRegistry()
	if err := reg.RegisterTask(h); err != nil {
		t.Fatalf("RegisterTask: %v", err)
	}
	resolved, ok := reg.Resolve(h.name)
	if !ok {
		t.Fatal("Resolve = false")
	}

	_, err := resolved.DispatchTaskWithAuth(
		context.Background(),
		&Task{ID: "stable-mismatch", Handler: "stable.foreign"},
		nil,
	)
	if err == nil {
		t.Fatal("relabelled resolved task unexpectedly succeeded")
	}
}
