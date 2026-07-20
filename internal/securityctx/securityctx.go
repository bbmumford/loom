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
