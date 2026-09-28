/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package handlers

import (
	"context"
	"fmt"
)

// BaseHandler implements HandlerMeta, RPCHandler and TaskHandler from a
// BaseHandlerConfig. Embed it in a concrete handler to reduce boilerplate.
type BaseHandler struct {
	name            string
	role            string
	requiresAuth    bool
	requiredScopes  []string
	scope           Scope
	executeRPCFunc  func(ctx context.Context, req *RPCRequest) (*RPCResponse, error)
	executeTaskFunc func(ctx context.Context, task *Task) (*TaskResult, error)
	validateFunc    func(req interface{}) error
}

// BaseHandlerConfig configures a base handler
type BaseHandlerConfig struct {
	Name           string
	Role           string
	RequiresAuth   bool
	RequiredScopes []string
	Scope          Scope
	ExecuteRPC     func(ctx context.Context, req *RPCRequest) (*RPCResponse, error)
	ExecuteTask    func(ctx context.Context, task *Task) (*TaskResult, error)
	Validate       func(req interface{}) error
}

// NewBaseHandler creates a new base handler with the given configuration
func NewBaseHandler(cfg BaseHandlerConfig) *BaseHandler {
	return &BaseHandler{
		name:            cfg.Name,
		role:            cfg.Role,
		requiresAuth:    cfg.RequiresAuth,
		requiredScopes:  cfg.RequiredScopes,
		scope:           cfg.Scope,
		executeRPCFunc:  cfg.ExecuteRPC,
		executeTaskFunc: cfg.ExecuteTask,
		validateFunc:    cfg.Validate,
	}
}

// Name returns the handler name
func (h *BaseHandler) Name() string {
	return h.name
}

// Role returns the handler role
func (h *BaseHandler) Role() string {
	return h.role
}

// RequiresAuth returns whether authentication is required
func (h *BaseHandler) RequiresAuth() bool {
	return h.requiresAuth
}

// Scopes returns the scope strings a caller must hold, never nil so a caller
// can range over the result without a length check.
func (h *BaseHandler) Scopes() []string {
	if h.requiredScopes == nil {
		return []string{}
	}
	return h.requiredScopes
}

// AllowedAuthTypes returns acceptable authentication methods
func (h *BaseHandler) AllowedAuthTypes() []string {
	return []string{} // Default: any auth type if RequiresAuth is true
}

// Scope returns the opaque scope label this handler declares. The zero value is
// intentionally invalid so that omitting Scope in BaseHandlerConfig is rejected
// at registration rather than read as "no scope required"; a deliberately public
// handler declares ScopeNone.
//
// handlers.Scope and rpc.ScopeTag are the same type identity — both alias the
// single declaration in pkg/rpc/scope/scopetag.go, so the handlers.Scope* and
// rpc.Scope* constants are interchangeable values and cannot drift.
func (h *BaseHandler) Scope() Scope {
	return h.scope
}

// AllowedPartitions returns nil: BaseHandlerConfig carries no partition
// values. validateScope and the loom-local AuthValidator do not read the list
// (see HandlerMeta.AllowedPartitions).
func (h *BaseHandler) AllowedPartitions() []string {
	return nil
}

// ExecuteRPC executes the RPC handler function
func (h *BaseHandler) ExecuteRPC(ctx context.Context, req *RPCRequest) (*RPCResponse, error) {
	if h.executeRPCFunc == nil {
		return &RPCResponse{
			Success: false,
			Error:   fmt.Sprintf("RPC execution not implemented for handler %s", h.name),
		}, nil
	}
	return h.executeRPCFunc(ctx, req)
}

// ExecuteTask executes the task handler function
func (h *BaseHandler) ExecuteTask(ctx context.Context, task *Task) (*TaskResult, error) {
	if h.executeTaskFunc == nil {
		// Default: delegate to RPC
		resp, err := h.ExecuteRPC(ctx, &RPCRequest{
			ID:      task.ID,
			Handler: task.Handler,
			Payload: task.Payload,
			Context: task.Metadata,
			Timeout: 0, // No timeout for tasks
		})
		if err != nil {
			return &TaskResult{
				TaskID: task.ID,
				Status: TaskStatusFailed,
				Error:  err.Error(),
			}, nil
		}
		if !resp.Success {
			return &TaskResult{
				TaskID: task.ID,
				Status: TaskStatusFailed,
				Error:  resp.Error,
			}, nil
		}
		return &TaskResult{
			TaskID:  task.ID,
			Status:  TaskStatusCompleted,
			Payload: resp.Payload,
		}, nil
	}
	return h.executeTaskFunc(ctx, task)
}

// ValidateRequest validates the request payload
func (h *BaseHandler) ValidateRequest(req interface{}) error {
	if h.validateFunc == nil {
		return nil // No validation by default
	}
	return h.validateFunc(req)
}
