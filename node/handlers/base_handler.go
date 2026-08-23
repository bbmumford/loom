/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package handlers

import (
	"context"
	"fmt"
)

// BaseHandler provides a base implementation for the Handler interface
// Embed this in concrete handlers to reduce boilerplate
type BaseHandler struct {
	name            string
	role            string
	requiresAuth    bool
	requiredScopes  []string
	tenantScope     TenantScope
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
	TenantScope    TenantScope
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
		tenantScope:     cfg.TenantScope,
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

// Scopes returns required scopes (was previously named RequiredScopes in interface)
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

// TenantScope returns the required tenant isolation level. The zero value is
// intentionally invalid: BaseHandler users must declare a tier in
// BaseHandlerConfig, including TenantScopeNone for a deliberate public opt-out.
//
// handlers.TenantScope and rpc.TenantScope are now the SAME type identity —
// both alias the canonical declaration in rpc/scope. A handler may override
// with either the handlers.TenantScope* (legacy) or rpc.Scope* (canonical,
// includes Profile) constant set; they are literally the same values with
// two names during the migration. See rpc/scope/tenantscope.go for the
// canonical enum and finding rev-069 for the rationale.
func (h *BaseHandler) TenantScope() TenantScope {
	return h.tenantScope
}

// AllowedTenants returns an optional tenant whitelist.
// Default: empty (all tenants allowed).
func (h *BaseHandler) AllowedTenants() []string {
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
