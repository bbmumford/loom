/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

// Package securityctx is loom's fail-closed default implementation of
// ports.AuthValidator, plus the loom-local security context keys it reads.
//
// IMPORTANT SCOPE LIMIT: these context keys are loom-PRIVATE. The
// authentication keys ValidateExecutionAuth reads are written only by
// WithAuth, which nothing outside loom's tests calls, and the package is
// internal, so no code outside loom can call it. The default validator
// therefore denies every RequiresAuth handler — the safe behaviour when no
// validator was injected. A composition root that authenticates callers MUST
// inject its own AuthValidator (Config.AuthValidator /
// ExecutorConfig.AuthValidator) that reads the context keys its
// authentication writes.
package securityctx

import (
	"context"
	"fmt"

	"github.com/bbmumford/loom/ports"
)

type ctxKey int

const (
	partitionIDKey ctxKey = iota
	userIDKey
	scopesKey
	authTypeKey
	authenticatedKey
	executionPrincipalKey
)

// WithPartitionID stamps the partition onto ctx under the loom-local key.
func WithPartitionID(ctx context.Context, partitionID string) context.Context {
	return context.WithValue(ctx, partitionIDKey, partitionID)
}

// ExtractPartitionID reads the loom-local partition key ("" when absent).
func ExtractPartitionID(ctx context.Context) string {
	if v, ok := ctx.Value(partitionIDKey).(string); ok {
		return v
	}
	return ""
}

// WithAuth stamps a full authenticated principal onto ctx (used by
// pure-loom deployments that run their own authentication front-end).
func WithAuth(ctx context.Context, userID, authType string, scopes []string) context.Context {
	ctx = context.WithValue(ctx, userIDKey, userID)
	ctx = context.WithValue(ctx, authTypeKey, authType)
	ctx = context.WithValue(ctx, scopesKey, append([]string(nil), scopes...))
	return context.WithValue(ctx, authenticatedKey, true)
}

// ExtractUserID reads the loom-local user key ("" when absent).
func ExtractUserID(ctx context.Context) string {
	if v, ok := ctx.Value(userIDKey).(string); ok {
		return v
	}
	return ""
}

// ExtractScopes reads the loom-local scopes key (nil when absent).
func ExtractScopes(ctx context.Context) []string {
	if v, ok := ctx.Value(scopesKey).([]string); ok {
		return v
	}
	return nil
}

// ExtractAuthType reads the loom-local auth-type key ("" when absent).
func ExtractAuthType(ctx context.Context) string {
	if v, ok := ctx.Value(authTypeKey).(string); ok {
		return v
	}
	return ""
}

// IsAuthenticated reports whether WithAuth ran on this ctx.
func IsAuthenticated(ctx context.Context) bool {
	v, ok := ctx.Value(authenticatedKey).(bool)
	return ok && v
}

// WithExecutionPrincipal stamps an immutable principal a composition root
// created onto the Pure-Loom private context. The principal—not a task
// body—must have established its execution partition key before calling
// this helper.
func WithExecutionPrincipal(
	ctx context.Context,
	principal ports.ExecutionPrincipal,
) context.Context {
	return context.WithValue(ctx, executionPrincipalKey, principal)
}

// Default returns the fail-closed loom-local validator.
func Default() ports.AuthValidator { return defaultValidator{} }

type defaultValidator struct{}

// ValidateExecutionAuth applies the handler's auth rules against the
// loom-local keys: no-auth handlers pass; RequiresAuth handlers
// need an authenticated principal with a permitted auth type and every
// required scope. With nothing writing these keys the result is a uniform
// deny for RequiresAuth handlers — fail closed, never fail open.
func (defaultValidator) ValidateExecutionAuth(ctx context.Context, h ports.SecureHandler) error {
	if !h.RequiresAuth() {
		return nil
	}
	if !IsAuthenticated(ctx) {
		return fmt.Errorf("handler %s requires authentication", h.Name())
	}
	authType := ExtractAuthType(ctx)
	if allowed := h.AllowedAuthTypes(); len(allowed) > 0 {
		ok := false
		for _, a := range allowed {
			if authType == a {
				ok = true
				break
			}
		}
		if !ok {
			return fmt.Errorf("handler %s requires auth type in %v, got: %s", h.Name(), allowed, authType)
		}
	}
	scopes := ExtractScopes(ctx)
	for _, required := range h.Scopes() {
		ok := false
		for _, s := range scopes {
			if s == required {
				ok = true
				break
			}
		}
		if !ok {
			return fmt.Errorf("handler %s requires scope %q, user %s missing it", h.Name(), required, ExtractUserID(ctx))
		}
	}
	return nil
}

func (defaultValidator) WithPartitionID(ctx context.Context, partitionID string) context.Context {
	return WithPartitionID(ctx, partitionID)
}

// ExecutionPartitionID reads the private Pure-Loom partition principal without
// creating one. This is the read-only executor side of WithPartitionID: callers
// must establish the principal before task dispatch, and the mutable task body
// is only compared against it.
func (defaultValidator) ExecutionPartitionID(ctx context.Context) (string, bool) {
	partitionID := ExtractPartitionID(ctx)
	return partitionID, partitionID != ""
}

// ExecutionPrincipal reads the private Pure-Loom execution principal without
// constructing one from request or task data.
func (defaultValidator) ExecutionPrincipal(ctx context.Context) (ports.ExecutionPrincipal, bool) {
	principal, ok := ctx.Value(executionPrincipalKey).(ports.ExecutionPrincipal)
	return principal, ok && principal != nil && principal.PartitionKey().Valid()
}
