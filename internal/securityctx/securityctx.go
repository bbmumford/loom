/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

// Package securityctx is loom's fail-closed default implementation of
// ports.AuthValidator, plus the loom-local security context keys it reads.
//
// IMPORTANT SCOPE LIMIT: these context keys are loom-PRIVATE. In an HSTLES
// build nothing writes them (SecurityCore writes platform/security/helpers'
// unexported keys), so this validator denies every RequiresAuth handler —
// which is exactly the safe behaviour when the real validator was not
// injected. HSTLES builds MUST inject an AuthValidator that delegates to
// the real helpers package (Config.AuthValidator / ExecutorConfig.
// AuthValidator); this package is the default only so pure-loom
// deployments get coherent, fail-closed semantics without HSTLES.
package securityctx

import (
	"context"
	"fmt"

	"github.com/bbmumford/loom/ports"
)

type ctxKey int

const (
	tenantIDKey ctxKey = iota
	userIDKey
	scopesKey
	authTypeKey
	authenticatedKey
	executionPrincipalKey
)

// WithTenantID stamps the tenant onto ctx under the loom-local key.
func WithTenantID(ctx context.Context, tenantID string) context.Context {
	return context.WithValue(ctx, tenantIDKey, tenantID)
}

// ExtractTenantID reads the loom-local tenant key ("" when absent).
func ExtractTenantID(ctx context.Context) string {
	if v, ok := ctx.Value(tenantIDKey).(string); ok {
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

// WithExecutionPrincipal stamps a product-created immutable principal onto
// the Pure-Loom private context. The principal—not a task body—must have
// established its owner key before calling this helper.
func WithExecutionPrincipal(
	ctx context.Context,
	principal ports.ExecutionPrincipal,
) context.Context {
	return context.WithValue(ctx, executionPrincipalKey, principal)
}

// Default returns the fail-closed loom-local validator.
func Default() ports.AuthValidator { return defaultValidator{} }

type defaultValidator struct{}

// ValidateExecutionAuth replicates the platform/security/helpers logic
// against the loom-local keys: no-auth handlers pass; RequiresAuth handlers
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

func (defaultValidator) WithTenantID(ctx context.Context, tenantID string) context.Context {
	return WithTenantID(ctx, tenantID)
}

// ExecutionTenantID reads the private Pure-Loom tenant principal without
// creating one. This is the read-only executor side of WithTenantID: callers
// must establish the principal before task dispatch, and the mutable task body
// is only compared against it.
func (defaultValidator) ExecutionTenantID(ctx context.Context) (string, bool) {
	tenantID := ExtractTenantID(ctx)
	return tenantID, tenantID != ""
}

// ExecutionPrincipal reads the private Pure-Loom owner principal without
// constructing one from request or task data.
func (defaultValidator) ExecutionPrincipal(ctx context.Context) (ports.ExecutionPrincipal, bool) {
	principal, ok := ctx.Value(executionPrincipalKey).(ports.ExecutionPrincipal)
	return principal, ok && principal != nil && principal.OwnerKey().Valid()
}

// WithWireIdentity implements ports.ScopeStamper for the loom-local default
// validator (#K-32): it records a mesh-propagated principal (userId + scope
// list) as authenticated so ValidateExecutionAuth's user + scope checks pass
// for pure-loom deployments. authType is left empty — a mesh peer that
// already authenticated the caller upstream carries no local auth-type, and
// handlers that pin AllowedAuthTypes gate that separately. An HSTLES build
// injects its own validator that stamps the real security-helpers keys
// instead; this loom-local implementation is the fail-closed default only.
func (defaultValidator) WithWireIdentity(ctx context.Context, userID string, scopes []string) context.Context {
	return WithAuth(ctx, userID, "", scopes)
}
