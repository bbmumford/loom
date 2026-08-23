/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package scope

import "context"

type authenticatedIdentityContextKey struct{}

// WithAuthenticatedIdentity stamps one complete, server-established identity
// snapshot onto ctx. It replaces an earlier snapshot rather than merging
// caller-controlled fragments across layers.
func WithAuthenticatedIdentity(
	ctx context.Context,
	identity AuthenticatedIdentity,
) context.Context {
	return context.WithValue(ctx, authenticatedIdentityContextKey{}, identity)
}

// AuthenticatedIdentityFromContext returns the server-established scope
// identity, or the zero value when none was established.
func AuthenticatedIdentityFromContext(ctx context.Context) AuthenticatedIdentity {
	if ctx == nil {
		return AuthenticatedIdentity{}
	}
	identity, _ := ctx.Value(authenticatedIdentityContextKey{}).(AuthenticatedIdentity)
	return identity
}

// WithAuthenticatedPlatformTenant installs a new platform-only identity
// snapshot. The mesh transport uses this when it binds a session to its
// server-resolved ScopeID. It must not retain organisation or user components
// from an earlier platform snapshot: those components are valid only when a
// trusted adapter establishes the complete identity together.
func WithAuthenticatedPlatformTenant(ctx context.Context, tenantID string) context.Context {
	return WithAuthenticatedIdentity(ctx, AuthenticatedIdentity{
		PlatformTenantID: tenantID,
	})
}
