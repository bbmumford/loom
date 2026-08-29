/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package scope

import (
	"context"
	"fmt"
	"strings"
)

// AuthenticatedPrincipal is one immutable, server-established caller snapshot.
// It couples the opaque presence identity with the exact grants (scope
// strings) granted to that identity. The fields are deliberately private:
// callers construct and read snapshots through copying accessors, so a slice
// retained by middleware cannot mutate authority after the principal has
// entered a dispatch context.
type AuthenticatedPrincipal struct {
	identity AuthenticatedIdentity
	grants   []string
}

type authenticatedPrincipalContextKey struct{}

// NewAuthenticatedPrincipal builds a complete authority snapshot from an opaque
// identity and its granted scope strings. loom copies and validates the grants
// (each trimmed non-empty) but interprets neither the identity axes nor the
// grants: their meaning is the consumer's PresencePolicy's decision. The
// identity is taken as established — which axes it must carry for a given
// handler is the policy's call, not loom's.
func NewAuthenticatedPrincipal(
	identity AuthenticatedIdentity,
	grants []string,
) (AuthenticatedPrincipal, error) {
	if err := identity.validate(); err != nil {
		return AuthenticatedPrincipal{}, err
	}
	grantCopy := make([]string, len(grants))
	for i, granted := range grants {
		if granted == "" || strings.TrimSpace(granted) != granted {
			return AuthenticatedPrincipal{}, fmt.Errorf(
				"authenticated principal scope %d is empty or not canonical",
				i,
			)
		}
		grantCopy[i] = granted
	}

	return AuthenticatedPrincipal{
		identity: identity.clone(),
		grants:   grantCopy,
	}, nil
}

// Identity returns a defensive copy of the principal's opaque presence identity.
func (p AuthenticatedPrincipal) Identity() AuthenticatedIdentity {
	return p.identity.clone()
}

// Scopes returns a defensive copy of the principal's granted scopes.
func (p AuthenticatedPrincipal) Scopes() []string {
	return append([]string(nil), p.grants...)
}

// Valid reports whether p carries any established presence: at least one
// identity axis or a non-empty subject. An empty snapshot confers no authority
// and is not stamped onto a context.
func (p AuthenticatedPrincipal) Valid() bool {
	return len(p.identity.Scopes) > 0 || p.identity.Subject != ""
}

// WithAuthenticatedPrincipal replaces the complete authority snapshot on ctx.
// It never merges fragments from an earlier principal. The canonical identity
// consumed by handler scope checks is stamped from this same snapshot.
func WithAuthenticatedPrincipal(
	ctx context.Context,
	principal AuthenticatedPrincipal,
) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	// Fail closed: an empty or invalid principal confers no authority and must not
	// leave a poisoned prior identity/principal live.
	if !principal.Valid() || principal.identity.validate() != nil {
		return failClosed(ctx)
	}
	sealed := AuthenticatedPrincipal{
		identity: principal.identity.clone(),
		grants:   principal.Scopes(),
	}
	ctx = context.WithValue(ctx, authenticatedPrincipalContextKey{}, sealed)
	// stampIdentityOnly (not WithAuthenticatedIdentity) so this atomic install does
	// NOT clear the principal it just stamped — the two projections stay coherent.
	return stampIdentityOnly(ctx, sealed.identity)
}

// AuthenticatedPrincipalFromContext returns a defensive copy of the complete
// principal, or false when trusted middleware established none.
func AuthenticatedPrincipalFromContext(
	ctx context.Context,
) (AuthenticatedPrincipal, bool) {
	if ctx == nil {
		return AuthenticatedPrincipal{}, false
	}
	principal, ok := ctx.Value(authenticatedPrincipalContextKey{}).(AuthenticatedPrincipal)
	if !ok || !principal.Valid() {
		return AuthenticatedPrincipal{}, false
	}
	principal.identity = principal.identity.clone()
	principal.grants = principal.Scopes()
	return principal, true
}
