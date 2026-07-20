/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package rpchttp

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/bbmumford/loom/pkg/rpc"
)

// Finding rev-081 tests — the HTTP bridge's executeLocal and executeProxy
// MUST mirror Registry.Dispatch's pre-handler gates:
//   - rpc.EnforceScope on every handler regardless of mount style
//   - rpc.VerifyWebhookSignature on local execution paths
//
// These tests pin the fail-closed behaviour so future bridge edits cannot
// silently reopen the gap.

// stubHandler builds a registry handler with the given scope. Func echoes
// the request back as the response.
func stubHandler(scope rpc.TenantScope) rpc.Handler {
	return rpc.Handler{
		Namespace: "test",
		Domain:    "rev081",
		Operation: "Op",
		Func: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			return req, nil
		},
		Request:  (*structpb.Struct)(nil),
		Response: (*structpb.Struct)(nil),
		Scope:    scope,
	}
}

// newBridgeWithHandler registers h in a fresh registry and returns the
// bridge + handler pointer.
func newBridgeWithHandler(t *testing.T, h rpc.Handler) (*Bridge, *rpc.Handler) {
	t.Helper()
	reg := rpc.NewRegistry()
	if err := reg.Register(h); err != nil {
		t.Fatalf("register: %v", err)
	}
	stored, ok := reg.Get(h.FQN())
	if !ok {
		t.Fatalf("registry lost handler %s", h.FQN())
	}
	return NewBridge(reg), stored
}

// newPostRequest builds a /rpc POST with empty JSON body and the given ctx.
func newPostRequest(ctx context.Context) *http.Request {
	r := httptest.NewRequest(http.MethodPost, "/rpc/test.rev081.Op", bytes.NewReader([]byte("{}")))
	return r.WithContext(ctx)
}

// TestExecuteLocal_ScopeUser_Denied — missing user_id on a ScopeUser
// handler must fail closed with ErrScopeDenied before the handler runs.
func TestExecuteLocal_ScopeUser_Denied(t *testing.T) {
	bridge, h := newBridgeWithHandler(t, stubHandler(rpc.ScopeUser))
	req := &structpb.Struct{}
	_, err := bridge.executeLocal(newPostRequest(context.Background()), h, req)
	if err == nil {
		t.Fatal("expected denial, got nil")
	}
	if !errors.Is(err, rpc.ErrScopeDenied) {
		t.Fatalf("expected ErrScopeDenied, got %v", err)
	}
	var be *bridgeError
	if !errors.As(err, &be) || be.status != http.StatusForbidden {
		t.Fatalf("expected 403 bridgeError, got %v", err)
	}
}

// TestExecuteLocal_ScopeTenant_Denied — missing tenantId on ScopeTenant
// fails closed.
func TestExecuteLocal_ScopeTenant_Denied(t *testing.T) {
	bridge, h := newBridgeWithHandler(t, stubHandler(rpc.ScopeTenant))
	_, err := bridge.executeLocal(newPostRequest(context.Background()), h, &structpb.Struct{})
	if !errors.Is(err, rpc.ErrScopeDenied) {
		t.Fatalf("expected ErrScopeDenied, got %v", err)
	}
}

// TestExecuteLocal_ScopeNone_Passes — ScopeNone has no precondition; the
// handler must run and return its response.
func TestExecuteLocal_ScopeNone_Passes(t *testing.T) {
	bridge, h := newBridgeWithHandler(t, stubHandler(rpc.ScopeNone))
	out, err := bridge.executeLocal(newPostRequest(context.Background()), h, &structpb.Struct{})
	if err != nil {
		t.Fatalf("ScopeNone must pass, got %v", err)
	}
	if len(out) == 0 {
		t.Fatal("expected non-empty response body")
	}
}

// TestExecuteLocal_ScopeUser_StampedCtx_Passes — positive path: a stamped
// ctx (tenantId+userId) satisfies ScopeUser and the handler runs.
func TestExecuteLocal_ScopeUser_StampedCtx_Passes(t *testing.T) {
	bridge, h := newBridgeWithHandler(t, stubHandler(rpc.ScopeUser))
	ctx := rpc.WithRPCContext(context.Background(), map[string]string{
		"tenantId": "orbtr",
		"userId":   "u-1",
	})
	out, err := bridge.executeLocal(newPostRequest(ctx), h, &structpb.Struct{})
	if err != nil {
		t.Fatalf("stamped ctx must satisfy ScopeUser, got %v", err)
	}
	if len(out) == 0 {
		t.Fatal("expected response body")
	}
}

// TestExecuteProxy_ScopePlatform_NilPlatforms_Allowed — a thin gateway
// (no SetPlatformTenants) MUST NOT 403 a ScopePlatform proxy handler; the
// destination registry runs the real enforcement. Reviewer concern C3.
func TestExecuteProxy_ScopePlatform_NilPlatforms_Allowed(t *testing.T) {
	reg := rpc.NewRegistry()
	// Proxy handler: Func nil, ScopePlatform.
	h := rpc.Handler{
		Namespace: "test",
		Domain:    "rev081",
		Operation: "Op",
		Request:   (*structpb.Struct)(nil),
		Response:  (*structpb.Struct)(nil),
		Scope:     rpc.ScopePlatform,
	}
	if err := reg.Register(h); err != nil {
		t.Fatalf("register: %v", err)
	}
	bridge := NewBridge(reg)
	stored, _ := reg.Get(h.FQN())

	// PlatformTenants is nil — IsPlatform always false. Without the C3
	// guard this would 403 before mesh dispatch. With the guard, scope
	// is skipped and we proceed to mesh — proto.Marshal succeeds, then
	// rpc.CallRaw is invoked. We expect a mesh error (no caller wired),
	// NOT a scope error.
	_, err := bridge.executeProxy(newPostRequest(context.Background()), stored, &structpb.Struct{})
	if errors.Is(err, rpc.ErrScopeDenied) {
		t.Fatalf("thin gateway must not 403 ScopePlatform proxy, got ErrScopeDenied")
	}
}

// TestExecuteProxy_ScopeTenant_Denied — non-platform scopes ARE enforced
// on the proxy path so malformed callers fail before wasting mesh dispatch.
func TestExecuteProxy_ScopeTenant_Denied(t *testing.T) {
	reg := rpc.NewRegistry()
	h := rpc.Handler{
		Namespace: "test",
		Domain:    "rev081",
		Operation: "Op",
		Request:   (*structpb.Struct)(nil),
		Response:  (*structpb.Struct)(nil),
		Scope:     rpc.ScopeTenant,
	}
	if err := reg.Register(h); err != nil {
		t.Fatalf("register: %v", err)
	}
	bridge := NewBridge(reg)
	stored, _ := reg.Get(h.FQN())

	_, err := bridge.executeProxy(newPostRequest(context.Background()), stored, &structpb.Struct{})
	if !errors.Is(err, rpc.ErrScopeDenied) {
		t.Fatalf("expected ErrScopeDenied for missing tenant on proxy path, got %v", err)
	}
}

// TestExecuteLocal_WebhookSignature_MissingFields_4xx — handler with
// WebhookSignature whose request proto lacks payload/signature fields
// (or whose signature is missing) must fail closed with a 4xx wrapping
// ErrWebhookSignatureInvalid BEFORE the handler runs.
func TestExecuteLocal_WebhookSignature_MissingFields_4xx(t *testing.T) {
	// Algorithm registry: ensure our stub algo exists.
	rpc.RegisterWebhookVerifier("rev081-stub-noop", func(payload, sig, secret string, skew time.Duration) error {
		return nil
	})

	handlerCalled := false
	h := rpc.Handler{
		Namespace: "test",
		Domain:    "rev081wh",
		Operation: "Recv",
		Func: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			handlerCalled = true
			return req, nil
		},
		Request:  (*structpb.Struct)(nil),
		Response: (*structpb.Struct)(nil),
		Scope:    rpc.ScopeNone,
		WebhookSignature: &rpc.WebhookSignatureConfig{
			Algorithm:      "rev081-stub-noop",
			SecretProvider: func() string { return "stub-secret" },
		},
	}
	reg := rpc.NewRegistry()
	if err := reg.Register(h); err != nil {
		t.Fatalf("register: %v", err)
	}
	bridge := NewBridge(reg)
	stored, _ := reg.Get(h.FQN())

	// structpb.Struct lacks `payload`+`signature` string fields, so the
	// pre-dispatch hook fails at field extraction with
	// ErrWebhookSignatureInvalid.
	_, err := bridge.executeLocal(newPostRequest(context.Background()), stored, &structpb.Struct{})
	if !errors.Is(err, rpc.ErrWebhookSignatureInvalid) {
		t.Fatalf("expected ErrWebhookSignatureInvalid, got %v", err)
	}
	if handlerCalled {
		t.Fatal("handler must NOT run when signature check fails")
	}
	var be *bridgeError
	if !errors.As(err, &be) || be.status != http.StatusUnauthorized {
		t.Fatalf("expected 401 bridgeError for signature failure, got %v", err)
	}
}

// TestClassifyHandlerStatus_WebhookErrors — symmetry check: webhook
// signature failures map to 401, not 500.
func TestClassifyHandlerStatus_WebhookErrors(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want int
	}{
		{"signature invalid", rpc.ErrWebhookSignatureInvalid, http.StatusUnauthorized},
		{"signature wrapped", &wrappedErr{rpc.ErrWebhookSignatureInvalid}, http.StatusUnauthorized},
		{"secret unset", rpc.ErrWebhookSecretUnset, http.StatusUnauthorized},
		{"scope denied", rpc.ErrScopeDenied, http.StatusForbidden},
	}
	for _, tc := range cases {
		if got := classifyHandlerStatus(tc.err); got != tc.want {
			t.Errorf("%s: got %d, want %d", tc.name, got, tc.want)
		}
	}
}

// wrappedErr satisfies errors.Is via Unwrap to mirror how registry
// wrapping produces fmt.Errorf("...: %w", sentinel) chains.
type wrappedErr struct{ inner error }

func (w *wrappedErr) Error() string { return "wrapped: " + w.inner.Error() }
func (w *wrappedErr) Unwrap() error { return w.inner }

// TestExecuteLocal_ScopeUser_HandlerNeverRunsOnDenial — ensures the
// fail-closed gate actually prevents handler execution (no side effects
// on a denied request). Distinct from the status-code test.
func TestExecuteLocal_ScopeUser_HandlerNeverRunsOnDenial(t *testing.T) {
	called := false
	h := rpc.Handler{
		Namespace: "test",
		Domain:    "rev081",
		Operation: "NoSide",
		Func: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			called = true
			return req, nil
		},
		Request:  (*structpb.Struct)(nil),
		Response: (*structpb.Struct)(nil),
		Scope:    rpc.ScopeUser,
	}
	reg := rpc.NewRegistry()
	if err := reg.Register(h); err != nil {
		t.Fatalf("register: %v", err)
	}
	stored, _ := reg.Get(h.FQN())
	bridge := NewBridge(reg)

	_, err := bridge.executeLocal(newPostRequest(context.Background()), stored, &structpb.Struct{})
	if err == nil {
		t.Fatal("expected denial")
	}
	if called {
		t.Fatal("handler must not run when scope is denied — side effects leaked")
	}
	// Sanity: the error text references the FQN so log correlation works.
	if !strings.Contains(err.Error(), "test.rev081.NoSide") {
		t.Errorf("expected FQN in error, got %q", err.Error())
	}
}
