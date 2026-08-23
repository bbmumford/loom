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
// It couples presence identity with the exact scopes granted to that identity.
// The fields are deliberately private: callers construct and read snapshots
// through copying accessors, so a slice retained by middleware cannot mutate
// authority after the principal has entered a dispatch context.
type AuthenticatedPrincipal struct {
	identity AuthenticatedIdentity
	scopes   []string
}

type authenticatedPrincipalContextKey struct{}

// NewAuthenticatedPrincipal builds a complete authority snapshot. Platform
// tenant is mandatory because it is reconciled with the authenticated mesh
// session on receipt. Customer organisation and user are optional axes: their
// absence is meaningful and causes org/user-scoped handlers to fail closed.
func NewAuthenticatedPrincipal(
	platformTenantID string,
	customerOrgID string,
	userID string,
	scopes []string,
) (AuthenticatedPrincipal, error) {
	if err := validatePrincipalID("platform tenant", platformTenantID, true); err != nil {
		return AuthenticatedPrincipal{}, err
	}
	if err := validatePrincipalID("customer organisation", customerOrgID, false); err != nil {
		return AuthenticatedPrincipal{}, err
	}
	if err := validatePrincipalID("user", userID, false); err != nil {
		return AuthenticatedPrincipal{}, err
	}

	scopeCopy := make([]string, len(scopes))
	for i, granted := range scopes {
		if granted == "" || strings.TrimSpace(granted) != granted {
			return AuthenticatedPrincipal{}, fmt.Errorf(
				"authenticated principal scope %d is empty or not canonical",
				i,
			)
		}
		scopeCopy[i] = granted
	}

	return AuthenticatedPrincipal{
		identity: AuthenticatedIdentity{
			PlatformTenantID: platformTenantID,
			OrganizationID:   customerOrgID,
			UserID:           userID,
		},
		scopes: scopeCopy,
	}, nil
}

func validatePrincipalID(name string, value string, required bool) error {
	if value == "" {
		if required {
			return fmt.Errorf("authenticated principal %s is required", name)
		}
		return nil
	}
	if strings.TrimSpace(value) != value {
		return fmt.Errorf("authenticated principal %s is not canonical", name)
	}
	return nil
}

// Identity returns the principal's value-only presence identity.
func (p AuthenticatedPrincipal) Identity() AuthenticatedIdentity {
	return p.identity
}

// Scopes returns a defensive copy of the principal's granted scopes.
func (p AuthenticatedPrincipal) Scopes() []string {
	return append([]string(nil), p.scopes...)
}

// Valid reports whether p was created from a nonempty platform identity.
func (p AuthenticatedPrincipal) Valid() bool {
	return p.identity.PlatformTenantID != ""
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
	if !principal.Valid() {
		return ctx
	}
	sealed := AuthenticatedPrincipal{
		identity: principal.identity,
		scopes:   principal.Scopes(),
	}
	ctx = context.WithValue(ctx, authenticatedPrincipalContextKey{}, sealed)
	return WithAuthenticatedIdentity(ctx, sealed.identity)
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
	principal.scopes = principal.Scopes()
	return principal, true
}
