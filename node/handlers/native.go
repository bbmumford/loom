/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// ---------------------------------------------------------------------------
// NativeRPCWrapper — implements RPCHandler with JSON marshal/unmarshal
// Used by ORBTR domain handlers that accept native Go types (not proto).
// Transitional: will be replaced by GenericRPCWrapper once handlers migrate to proto.
// ---------------------------------------------------------------------------

// NativeRPCWrapper implements RPCHandler using JSON serialization and Go generics.
// The type parameters ensure compile-time type safety between the handler method
// and the native Go types. The sender (mesh dispatch) must also use JSON.
type NativeRPCWrapper[Req any, Resp any] struct {
	name           string
	role           string
	auth           bool
	authTypes      []string
	scopes         []string
	tenantScope    TenantScope
	allowedTenants []string
	handler        func(ctx context.Context, req Req) (Resp, error)
	newRequest     func() Req
}

// NewNativeRPCWrapper creates a NativeRPCWrapper with JSON serialization.
//
// Parameters:
//   - name: handler name (e.g., "policy.get_bundle")
//   - role: domain role (e.g., "policy")
//   - handler: typed handler function (e.g., h.GetBundle)
//   - newReq: factory that returns an empty request pointer
func NewNativeRPCWrapper[Req any, Resp any](
	name, role string,
	handler func(ctx context.Context, req Req) (Resp, error),
	newReq func() Req,
) *NativeRPCWrapper[Req, Resp] {
	return &NativeRPCWrapper[Req, Resp]{
		name:       name,
		role:       role,
		auth:       true,
		handler:    handler,
		newRequest: newReq,
	}
}

// WithAuth configures authentication requirements. Returns self for chaining.
func (w *NativeRPCWrapper[Req, Resp]) WithAuth(required bool, authTypes []string, scopes []string) *NativeRPCWrapper[Req, Resp] {
	w.auth = required
	w.authTypes = authTypes
	w.scopes = scopes
	return w
}

// WithTenantScope configures tenant isolation. Returns self for chaining.
func (w *NativeRPCWrapper[Req, Resp]) WithTenantScope(scope TenantScope, allowed []string) *NativeRPCWrapper[Req, Resp] {
	w.tenantScope = scope
	w.allowedTenants = allowed
	return w
}

func (w *NativeRPCWrapper[Req, Resp]) Name() string               { return w.name }
func (w *NativeRPCWrapper[Req, Resp]) Role() string               { return w.role }
func (w *NativeRPCWrapper[Req, Resp]) RequiresAuth() bool         { return w.auth }
func (w *NativeRPCWrapper[Req, Resp]) AllowedAuthTypes() []string { return w.authTypes }
func (w *NativeRPCWrapper[Req, Resp]) Scopes() []string           { return w.scopes }
func (w *NativeRPCWrapper[Req, Resp]) TenantScope() TenantScope   { return w.tenantScope }
func (w *NativeRPCWrapper[Req, Resp]) AllowedTenants() []string   { return w.allowedTenants }

func (w *NativeRPCWrapper[Req, Resp]) ExecuteRPC(ctx context.Context, req *RPCRequest) (*RPCResponse, error) {
	start := time.Now()

	// JSON unmarshal payload into typed request
	handlerReq := w.newRequest()
	if len(req.Payload) > 0 {
		if err := json.Unmarshal(req.Payload, handlerReq); err != nil {
			return &RPCResponse{
				Success: false,
				Error:   fmt.Sprintf("unmarshal request for %s: %v", w.name, err),
				Latency: time.Since(start),
			}, nil
		}
	}

	// Execute handler
	handlerResp, err := w.handler(ctx, handlerReq)
	if err != nil {
		return &RPCResponse{
			Success: false,
			Error:   err.Error(),
			Latency: time.Since(start),
		}, nil
	}

	// JSON marshal response
	payload, err := json.Marshal(handlerResp)
	if err != nil {
		return &RPCResponse{
			Success: false,
			Error:   fmt.Sprintf("marshal response for %s: %v", w.name, err),
			Latency: time.Since(start),
		}, nil
	}

	return &RPCResponse{
		Success: true,
		Payload: payload,
		Latency: time.Since(start),
	}, nil
}

// ---------------------------------------------------------------------------
// NativeTaskWrapper — implements TaskHandler with JSON marshal/unmarshal
// ---------------------------------------------------------------------------

// NativeTaskWrapper implements TaskHandler using JSON serialization and Go generics.
type NativeTaskWrapper[Req any, Resp any] struct {
	name           string
	role           string
	auth           bool
	authTypes      []string
	scopes         []string
	tenantScope    TenantScope
	allowedTenants []string
	handler        func(ctx context.Context, req Req) (Resp, error)
	newRequest     func() Req
}

// NewNativeTaskWrapper creates a NativeTaskWrapper with JSON serialization.
func NewNativeTaskWrapper[Req, Resp any](
	name, role string,
	handler func(ctx context.Context, req Req) (Resp, error),
	newReq func() Req,
) *NativeTaskWrapper[Req, Resp] {
	return &NativeTaskWrapper[Req, Resp]{
		name:       name,
		role:       role,
		auth:       true,
		handler:    handler,
		newRequest: newReq,
	}
}

// WithAuth configures authentication requirements. Returns self for chaining.
func (w *NativeTaskWrapper[Req, Resp]) WithAuth(required bool, authTypes []string, scopes []string) *NativeTaskWrapper[Req, Resp] {
	w.auth = required
	w.authTypes = authTypes
	w.scopes = scopes
	return w
}

// WithTenantScope configures tenant isolation. Returns self for chaining.
func (w *NativeTaskWrapper[Req, Resp]) WithTenantScope(scope TenantScope, allowed []string) *NativeTaskWrapper[Req, Resp] {
	w.tenantScope = scope
	w.allowedTenants = allowed
	return w
}

func (w *NativeTaskWrapper[Req, Resp]) Name() string               { return w.name }
func (w *NativeTaskWrapper[Req, Resp]) Role() string               { return w.role }
func (w *NativeTaskWrapper[Req, Resp]) RequiresAuth() bool         { return w.auth }
func (w *NativeTaskWrapper[Req, Resp]) AllowedAuthTypes() []string { return w.authTypes }
func (w *NativeTaskWrapper[Req, Resp]) Scopes() []string           { return w.scopes }
func (w *NativeTaskWrapper[Req, Resp]) TenantScope() TenantScope   { return w.tenantScope }
func (w *NativeTaskWrapper[Req, Resp]) AllowedTenants() []string   { return w.allowedTenants }

func (w *NativeTaskWrapper[Req, Resp]) ExecuteTask(ctx context.Context, task *Task) (*TaskResult, error) {
	start := time.Now()

	// JSON unmarshal payload into typed request
	handlerReq := w.newRequest()
	if len(task.Payload) > 0 {
		if err := json.Unmarshal(task.Payload, handlerReq); err != nil {
			return &TaskResult{
				TaskID:      task.ID,
				Status:      TaskStatusFailed,
				Error:       fmt.Sprintf("unmarshal task for %s: %v", w.name, err),
				Duration:    time.Since(start),
				CompletedAt: time.Now(),
			}, nil
		}
	}

	// Execute handler
	handlerResp, err := w.handler(ctx, handlerReq)
	if err != nil {
		return &TaskResult{
			TaskID:      task.ID,
			Status:      TaskStatusFailed,
			Error:       err.Error(),
			Duration:    time.Since(start),
			CompletedAt: time.Now(),
		}, nil
	}

	// JSON marshal response into result payload
	payload, err := json.Marshal(handlerResp)
	if err != nil {
		return &TaskResult{
			TaskID:      task.ID,
			Status:      TaskStatusFailed,
			Error:       fmt.Sprintf("marshal task result for %s: %v", w.name, err),
			Duration:    time.Since(start),
			CompletedAt: time.Now(),
		}, nil
	}

	return &TaskResult{
		TaskID:      task.ID,
		Status:      TaskStatusCompleted,
		Payload:     payload,
		Duration:    time.Since(start),
		CompletedAt: time.Now(),
	}, nil
}
