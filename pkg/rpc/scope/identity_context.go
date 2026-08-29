/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package scope

import "context"

type authenticatedIdentityContextKey struct{}

// WithAuthenticatedIdentity stamps one complete, server-established identity
// snapshot, validated and defensively cloned. It REPLACES any prior identity AND
// CLEARS any prior authenticated principal: a raw identity carries no grants, so
// an earlier principal's grants must never remain beside a different identity
// (two contradictory snapshots that Authorize could read against each other).
// An identity that fails the canonical axis invariant fails CLOSED — both keys
// are cleared and the context carries no authority, never the prior one.
func WithAuthenticatedIdentity(ctx context.Context, identity AuthenticatedIdentity) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	if identity.validate() != nil {
		return failClosed(ctx)
	}
	ctx = context.WithValue(ctx, authenticatedIdentityContextKey{}, identity.clone())
	// A raw identity replacement is not coherent with a prior principal's grants;
	// clear the principal so only WithAuthenticatedPrincipal installs the two
	// together.
	return context.WithValue(ctx, authenticatedPrincipalContextKey{}, AuthenticatedPrincipal{})
}

// stampIdentityOnly installs a validated identity WITHOUT touching the principal
// key. Package-internal: only WithAuthenticatedPrincipal, which installs a
// coherent identity+grants pair in the same operation, may leave the principal
// intact. The identity is cloned so no caller slice aliases stamped authority.
func stampIdentityOnly(ctx context.Context, identity AuthenticatedIdentity) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, authenticatedIdentityContextKey{}, identity.clone())
}

// failClosed clears both authority keys so no prior identity or principal
// survives an invalid stamp.
func failClosed(ctx context.Context) context.Context {
	ctx = context.WithValue(ctx, authenticatedIdentityContextKey{}, AuthenticatedIdentity{})
	return context.WithValue(ctx, authenticatedPrincipalContextKey{}, AuthenticatedPrincipal{})
}

// AuthenticatedIdentityFromContext returns a defensive COPY of the
// server-established scope identity, or the zero value when none was
// established. Cloning on read stops a caller from mutating the stored axis
// slice after enforcement has read it.
func AuthenticatedIdentityFromContext(ctx context.Context) AuthenticatedIdentity {
	if ctx == nil {
		return AuthenticatedIdentity{}
	}
	identity, _ := ctx.Value(authenticatedIdentityContextKey{}).(AuthenticatedIdentity)
	return identity.clone()
}

// WithAuthenticatedScope installs a single-axis identity snapshot through the
// validating stamper. An empty/non-canonical name or empty value fails closed.
// It REPLACES any earlier snapshot and clears any prior principal: additional
// axes are valid only when a trusted adapter establishes the complete identity
// together via NewCompleteIdentity, so a single-axis bind must not retain other
// components from an earlier snapshot.
func WithAuthenticatedScope(ctx context.Context, name, value string) context.Context {
	return WithAuthenticatedIdentity(ctx, AuthenticatedIdentity{
		Scopes: []ScopeValue{{Name: name, Value: value}},
	})
}
