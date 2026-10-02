/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package handlers

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	rpcscope "github.com/bbmumford/loom/pkg/rpc/scope"
	"github.com/bbmumford/loom/ports"
)

type resolvedRPCProbe struct {
	name  string
	scope Scope
	ran   atomic.Int32
}

func (h *resolvedRPCProbe) Name() string             { return h.name }
func (*resolvedRPCProbe) Role() string               { return "resolved-rpc-test" }
func (*resolvedRPCProbe) RequiresAuth() bool         { return true }
func (*resolvedRPCProbe) AllowedAuthTypes() []string { return []string{"service"} }
func (*resolvedRPCProbe) Scopes() []string           { return []string{"rpc.execute"} }
func (h *resolvedRPCProbe) Scope() Scope {
	if h.scope == "" {
		return ScopeNone
	}
	return h.scope
}
func (*resolvedRPCProbe) AllowedPartitions() []string { return nil }
func (h *resolvedRPCProbe) ExecuteRPC(context.Context, *RPCRequest) (*RPCResponse, error) {
	h.ran.Add(1)
	return &RPCResponse{Success: true}, nil
}

type allowingRPCAuth struct {
	calls atomic.Int32
}

func (a *allowingRPCAuth) ValidateExecutionAuth(context.Context, ports.SecureHandler) error {
	a.calls.Add(1)
	return nil
}

func (*allowingRPCAuth) WithPartitionID(ctx context.Context, _ string) context.Context {
	return ctx
}

func TestResolvedRPCWithAuthPinsExactRegistrationAcrossReplacement(t *testing.T) {
	registry := NewHandlerRegistry()
	first := &resolvedRPCProbe{name: "rpc.exact"}
	replacement := &resolvedRPCProbe{name: first.name}
	if err := registry.RegisterRPC(first); err != nil {
		t.Fatalf("RegisterRPC(first): %v", err)
	}
	resolved, ok := registry.Resolve(first.name)
	if !ok {
		t.Fatal("Resolve(first) = false")
	}
	if !registry.Unregister(first.name) {
		t.Fatal("Unregister(first) = false")
	}
	if err := registry.RegisterRPC(replacement); err != nil {
		t.Fatalf("RegisterRPC(replacement): %v", err)
	}
	auth := &allowingRPCAuth{}

	resp, err := resolved.DispatchRPCWithAuth(
		context.Background(),
		&RPCRequest{Handler: first.name},
		auth,
	)
	if err != nil || resp == nil || !resp.Success {
		t.Fatalf("captured dispatch = (%+v, %v)", resp, err)
	}
	if got := first.ran.Load(); got != 1 {
		t.Fatalf("captured handler calls = %d, want 1", got)
	}
	if got := replacement.ran.Load(); got != 0 {
		t.Fatalf("replacement stole captured dispatch: calls=%d", got)
	}

	resp, err = registry.DispatchRPCWithAuth(
		context.Background(),
		&RPCRequest{Handler: replacement.name},
		auth,
	)
	if err != nil || resp == nil || !resp.Success {
		t.Fatalf("current dispatch = (%+v, %v)", resp, err)
	}
	if got := replacement.ran.Load(); got != 1 {
		t.Fatalf("replacement handler calls = %d, want 1", got)
	}
	if got := auth.calls.Load(); got != 2 {
		t.Fatalf("auth calls = %d, want 2", got)
	}
}

func TestAuthenticatedRPCScopeIgnoresRequestIdentityHints(t *testing.T) {
	registry := newScopedRegistry()
	handler := &resolvedRPCProbe{name: "rpc.org", scope: ScopeWithinPartition}
	var events []string
	registry.Use(&taskDispatchMiddleware{name: "scope-mw", events: &events})
	if err := registry.RegisterRPC(handler); err != nil {
		t.Fatalf("RegisterRPC: %v", err)
	}
	auth := &allowingRPCAuth{}
	req := &RPCRequest{
		Handler: handler.name,
		Context: map[string]interface{}{
			"tenantId":  "evil-camel",
			"tenant_id": "evil-snake",
			"orgId":     "evil-camel",
			"org_id":    "evil-snake",
		},
	}

	platformOnly := WithTransportPartition(context.Background(), "orbtr")
	resp, err := registry.DispatchRPCWithAuth(platformOnly, req, auth)
	if err != nil {
		t.Fatalf("map-only DispatchRPCWithAuth error: %v", err)
	}
	if resp == nil || resp.Success || resp.Error == "" {
		t.Fatalf("map-only response=%+v, want scope denied", resp)
	}
	if got := handler.ran.Load(); got != 0 {
		t.Fatalf("handler ran on map-only organisation: %d", got)
	}
	if len(events) != 0 {
		t.Fatalf("middleware ran before scope denial: %v", events)
	}

	exact := rpcscope.WithAuthenticatedIdentity(platformOnly, scopeIdentity("orbtr", "org-1", "user-1"))
	resp, err = registry.DispatchRPCWithAuth(exact, req, auth)
	if err != nil || resp == nil || !resp.Success {
		t.Fatalf("exact authenticated DispatchRPCWithAuth=(%+v, %v)", resp, err)
	}
	if got := handler.ran.Load(); got != 1 {
		t.Fatalf("handler calls=%d, want 1", got)
	}
	if len(events) != 2 || events[0] != "scope-mw.before" || events[1] != "scope-mw.after" {
		t.Fatalf("middleware events=%v, want before/after", events)
	}
}

// scopeRefusalProbe serves the RPC, task and stream modes and declares
// ScopeWithinPartition. On a registry with no presence policy validateScope
// refuses that tier on every path, so each dispatch below reaches the scope
// refusal and nothing past it.
type scopeRefusalProbe struct {
	name string
	ran  atomic.Int32
}

func (h *scopeRefusalProbe) Name() string              { return h.name }
func (*scopeRefusalProbe) Role() string                { return "scope-refusal-test" }
func (*scopeRefusalProbe) RequiresAuth() bool          { return true }
func (*scopeRefusalProbe) AllowedAuthTypes() []string  { return []string{"service"} }
func (*scopeRefusalProbe) Scopes() []string            { return nil }
func (*scopeRefusalProbe) Scope() Scope                { return ScopeWithinPartition }
func (*scopeRefusalProbe) AllowedPartitions() []string { return nil }
func (h *scopeRefusalProbe) HandleStream(context.Context, MessageStream) error {
	h.ran.Add(1)
	return nil
}
func (h *scopeRefusalProbe) ExecuteRPC(context.Context, *RPCRequest) (*RPCResponse, error) {
	h.ran.Add(1)
	return &RPCResponse{Success: true}, nil
}
func (h *scopeRefusalProbe) ExecuteTask(_ context.Context, task *Task) (*TaskResult, error) {
	h.ran.Add(1)
	return &TaskResult{TaskID: task.ID, Status: TaskStatusCompleted}, nil
}

// scopeDeniedPrefix is the text every validateScope refusal starts with on the
// RPC response, the task result and the stream error. A caller holding only
// the text tells the registry's scope refusal from a handler's own error by it.
const scopeDeniedPrefix = "scope denied: "

// TestScopeRefusalIsReportedAsScopeDeniedOnEveryMode dispatches one refused
// handler through the RPC, task and stream pipelines and requires each to
// report the refusal under scopeDeniedPrefix, still wrapping ErrDenied where an
// error is returned, with the handler never run.
func TestScopeRefusalIsReportedAsScopeDeniedOnEveryMode(t *testing.T) {
	registry := NewHandlerRegistry()
	rpcProbe := &scopeRefusalProbe{name: "scope.refused.rpc"}
	taskProbe := &scopeRefusalProbe{name: "scope.refused.task"}
	streamProbe := &scopeRefusalProbe{name: "scope.refused.stream"}
	if err := registry.RegisterRPC(rpcProbe); err != nil {
		t.Fatalf("RegisterRPC: %v", err)
	}
	if err := registry.RegisterTask(taskProbe); err != nil {
		t.Fatalf("RegisterTask: %v", err)
	}
	if err := registry.RegisterStream(streamProbe); err != nil {
		t.Fatalf("RegisterStream: %v", err)
	}
	ctx := context.Background()

	resp, err := registry.DispatchRPCWithAuth(ctx, &RPCRequest{Handler: rpcProbe.name}, &allowingRPCAuth{})
	if err != nil || resp == nil || resp.Success || !strings.HasPrefix(resp.Error, scopeDeniedPrefix) {
		t.Errorf("RPC refusal = (%+v, %v), want an unsuccessful response starting %q", resp, err, scopeDeniedPrefix)
	}

	result, err := registry.DispatchTaskWithAuth(ctx, &Task{ID: "t-1", Handler: taskProbe.name}, &allowingRPCAuth{})
	if err != nil || result == nil || result.Status != TaskStatusFailed || !strings.HasPrefix(result.Error, scopeDeniedPrefix) {
		t.Errorf("task refusal = (%+v, %v), want a failed result starting %q", result, err, scopeDeniedPrefix)
	}

	err = registry.DispatchStream(ctx, streamProbe.name, nil)
	if err == nil || !strings.HasPrefix(err.Error(), scopeDeniedPrefix) || !errors.Is(err, rpcscope.ErrDenied) {
		t.Errorf("stream refusal = %v, want an error starting %q that wraps ErrDenied", err, scopeDeniedPrefix)
	}

	for _, p := range []*scopeRefusalProbe{rpcProbe, taskProbe, streamProbe} {
		if got := p.ran.Load(); got != 0 {
			t.Errorf("%s ran %d times past its scope refusal", p.name, got)
		}
	}
}
