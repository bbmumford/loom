/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package handlers

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"
)

// ---------------------------------------------------------------------------
// GenericRPCWrapper — implements RPCHandler with proto marshal/unmarshal
// ---------------------------------------------------------------------------

// GenericRPCWrapper implements RPCHandler using proto serialization and Go generics.
// Generated code creates instances via NewRPCWrapper. The type parameters ensure
// compile-time type safety between the handler method and the proto types.
type GenericRPCWrapper[Req, Resp proto.Message] struct {
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

// NewRPCWrapper creates a GenericRPCWrapper with proto serialization.
//
// Parameters:
//   - name: handler name (e.g., "policy.get_bundle")
//   - role: domain role (e.g., "policy")
//   - handler: typed handler function (e.g., h.GetBundle)
//   - newReq: factory that returns an empty proto request pointer
func NewRPCWrapper[Req, Resp proto.Message](
	name, role string,
	handler func(ctx context.Context, req Req) (Resp, error),
	newReq func() Req,
) *GenericRPCWrapper[Req, Resp] {
	return &GenericRPCWrapper[Req, Resp]{
		name:       name,
		role:       role,
		auth:       true,
		handler:    handler,
		newRequest: newReq,
	}
}

// WithAuth configures authentication requirements. Returns self for chaining.
func (w *GenericRPCWrapper[Req, Resp]) WithAuth(required bool, authTypes []string, scopes []string) *GenericRPCWrapper[Req, Resp] {
	w.auth = required
	w.authTypes = authTypes
	w.scopes = scopes
	return w
}

// WithTenantScope configures tenant isolation. Returns self for chaining.
func (w *GenericRPCWrapper[Req, Resp]) WithTenantScope(scope TenantScope, allowed []string) *GenericRPCWrapper[Req, Resp] {
	w.tenantScope = scope
	w.allowedTenants = allowed
	return w
}

func (w *GenericRPCWrapper[Req, Resp]) Name() string               { return w.name }
func (w *GenericRPCWrapper[Req, Resp]) Role() string               { return w.role }
func (w *GenericRPCWrapper[Req, Resp]) RequiresAuth() bool         { return w.auth }
func (w *GenericRPCWrapper[Req, Resp]) AllowedAuthTypes() []string { return w.authTypes }
func (w *GenericRPCWrapper[Req, Resp]) Scopes() []string           { return w.scopes }
func (w *GenericRPCWrapper[Req, Resp]) TenantScope() TenantScope   { return w.tenantScope }
func (w *GenericRPCWrapper[Req, Resp]) AllowedTenants() []string   { return w.allowedTenants }

func (w *GenericRPCWrapper[Req, Resp]) ExecuteRPC(ctx context.Context, req *RPCRequest) (*RPCResponse, error) {
	start := time.Now()

	// Proto unmarshal payload into typed request
	handlerReq := w.newRequest()
	if len(req.Payload) > 0 {
		if err := proto.Unmarshal(req.Payload, handlerReq); err != nil {
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

	// Proto marshal response
	payload, err := proto.Marshal(handlerResp)
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
// GenericTaskWrapper — implements TaskHandler with proto marshal/unmarshal
// ---------------------------------------------------------------------------

// GenericTaskWrapper implements TaskHandler using proto serialization and Go generics.
type GenericTaskWrapper[Req proto.Message, Resp proto.Message] struct {
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

// NewTaskWrapper creates a GenericTaskWrapper with proto serialization.
func NewTaskWrapper[Req, Resp proto.Message](
	name, role string,
	handler func(ctx context.Context, req Req) (Resp, error),
	newReq func() Req,
) *GenericTaskWrapper[Req, Resp] {
	return &GenericTaskWrapper[Req, Resp]{
		name:       name,
		role:       role,
		auth:       true,
		handler:    handler,
		newRequest: newReq,
	}
}

// WithAuth configures authentication requirements. Returns self for chaining.
func (w *GenericTaskWrapper[Req, Resp]) WithAuth(required bool, authTypes []string, scopes []string) *GenericTaskWrapper[Req, Resp] {
	w.auth = required
	w.authTypes = authTypes
	w.scopes = scopes
	return w
}

// WithTenantScope configures tenant isolation. Returns self for chaining.
func (w *GenericTaskWrapper[Req, Resp]) WithTenantScope(scope TenantScope, allowed []string) *GenericTaskWrapper[Req, Resp] {
	w.tenantScope = scope
	w.allowedTenants = allowed
	return w
}

func (w *GenericTaskWrapper[Req, Resp]) Name() string               { return w.name }
func (w *GenericTaskWrapper[Req, Resp]) Role() string               { return w.role }
func (w *GenericTaskWrapper[Req, Resp]) RequiresAuth() bool         { return w.auth }
func (w *GenericTaskWrapper[Req, Resp]) AllowedAuthTypes() []string { return w.authTypes }
func (w *GenericTaskWrapper[Req, Resp]) Scopes() []string           { return w.scopes }
func (w *GenericTaskWrapper[Req, Resp]) TenantScope() TenantScope   { return w.tenantScope }
func (w *GenericTaskWrapper[Req, Resp]) AllowedTenants() []string   { return w.allowedTenants }

func (w *GenericTaskWrapper[Req, Resp]) ExecuteTask(ctx context.Context, task *Task) (*TaskResult, error) {
	start := time.Now()

	// Proto unmarshal payload into typed request
	handlerReq := w.newRequest()
	if len(task.Payload) > 0 {
		if err := proto.Unmarshal(task.Payload, handlerReq); err != nil {
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

	// Proto marshal response into result payload
	payload, err := proto.Marshal(handlerResp)
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
