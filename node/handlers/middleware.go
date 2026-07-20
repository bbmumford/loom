/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package handlers

import "context"

// Middleware enriches context or modifies responses around handler execution.
// Implementations are stateful — they hold typed references to domain services
// (repositories, bot pools, etc.) and inject them into context before handlers run.
//
// Register middleware via HandlerRegistry.UseForRole() or HandlerRegistry.Use().
// Role-scoped middleware runs only for handlers matching that role.
// Global middleware runs for all handlers.
//
// Execution order: global middleware (in registration order) → role middleware (in registration order).
// After hooks run in reverse order.
type Middleware interface {
	// Name returns a descriptive name for logging (e.g., "storage-context").
	Name() string

	// Before is called before handler execution. Returns an enriched context.
	// If err is non-nil, handler execution is skipped and the error is returned.
	Before(ctx context.Context, handlerName string, req *RPCRequest) (context.Context, error)

	// After is called after handler execution. Can inspect or modify the response.
	After(ctx context.Context, handlerName string, resp *RPCResponse, err error) (*RPCResponse, error)
}
