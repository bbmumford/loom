/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package rpc

import (
	"context"
	"errors"
	"fmt"
)

// rpcContextKey is the typed key used to stamp the RPCRequest.Context map
// onto a Go context.Context. Receivers (BidiRPC server, local dispatch,
// HTTP bridge) call WithRPCContext to surface the wire-side context map
// to in-process handlers + scope enforcement.
type rpcContextKey struct{}

// WithRPCContext stamps the RPCRequest.Context map onto ctx so receivers
// can read tenantId/userId/orgId/opClass via the *FromContext helpers
// in this package — and so EnforceScope sees the wire-supplied identity.
//
// Empty / nil maps return ctx unchanged. Re-calling MERGES with the prior
// value (later keys win) so middleware can layer additional fields without
// dropping what the transport injected.
func WithRPCContext(ctx context.Context, m map[string]string) context.Context {
	if len(m) == 0 {
		return ctx
	}
	if prev := rpcContextMap(ctx); prev != nil {
		merged := make(map[string]string, len(prev)+len(m))
		for k, v := range prev {
			merged[k] = v
		}
		for k, v := range m {
			merged[k] = v
		}
		return context.WithValue(ctx, rpcContextKey{}, merged)
	}
	return context.WithValue(ctx, rpcContextKey{}, m)
}

// rpcContextMap returns the previously-stamped map (or nil).
func rpcContextMap(ctx context.Context) map[string]string {
	if m, ok := ctx.Value(rpcContextKey{}).(map[string]string); ok {
		return m
	}
	return nil
}

// TenantFromContext extracts tenantId from the rpc context.
// Returns "" when no tenant has been stamped.
func TenantFromContext(ctx context.Context) string {
	return rpcContextMap(ctx)["tenantId"]
}

// UserFromContext extracts userId (if set by upstream auth middleware).
func UserFromContext(ctx context.Context) string {
	return rpcContextMap(ctx)["userId"]
}

// OrgFromContext extracts orgId (if set by upstream session middleware).
func OrgFromContext(ctx context.Context) string {
	return rpcContextMap(ctx)["orgId"]
}

// OpClassFromContext extracts opClass (set by the caller via
// dispatch.WithOpClass). Receivers use this for stream-priority routing.
func OpClassFromContext(ctx context.Context) string {
	return rpcContextMap(ctx)["opClass"]
}

// ErrScopeDenied is the sentinel returned by EnforceScope when a handler's
// declared TenantScope cannot be satisfied by the caller's context. Callers
// should use errors.Is at boundaries (e.g. HTTP bridge → 403, server-side
// dispatch → 403 wire error). The wrapping error includes the FQN +
// offending fields for diagnostic logging.
var ErrScopeDenied = errors.New("rpc: scope denied")

// PlatformTenants is the set of tenantIds that satisfy ScopePlatform.
// Registries inject this at construction time (typically on the node
// endpoints that host platform-tier handlers). An empty set keeps
// platform checks failing closed — safer than an "anyone goes" default.
type PlatformTenants map[string]struct{}

// NewPlatformTenants builds a PlatformTenants set from a list of tenant ids.
// Convenience constructor; duplicates collapse, empty strings are dropped.
func NewPlatformTenants(ids ...string) PlatformTenants {
	out := make(PlatformTenants, len(ids))
	for _, id := range ids {
		if id == "" {
			continue
		}
		out[id] = struct{}{}
	}
	return out
}

// IsPlatform returns true when tenantID is in the platform set.
// A nil receiver answers false — the safe default for an unconfigured
// registry.
func (p PlatformTenants) IsPlatform(tenantID string) bool {
	if p == nil {
		return false
	}
	_, ok := p[tenantID]
	return ok
}

// EnforceScope rejects calls whose ctx fails the handler's declared
// TenantScope. Returns nil for ScopeNone (the only "anyone goes" tier).
// For every other scope a missing or wrong tier-field surfaces as
// ErrScopeDenied wrapped with the handler FQN and the offending field
// name — callers should use errors.Is(err, ErrScopeDenied) at boundaries.
//
// Scope semantics (see rpc/handler.go for the canonical declarations):
//   - ScopeNone     — no check (used for liveness, /heartbeat, OAuth init)
//   - ScopePlatform — caller is the HSTLES platform itself (tenant ∈ platforms)
//   - ScopeTenant   — caller has stamped some tenantId (any valid app)
//   - ScopeOrg      — caller has tenantId + orgId (tenant + organisation)
//   - ScopeUser     — caller has tenantId + userId (per-user actions)
//   - ScopeProfile  — caller has tenantId; profile membership is a
//                     scope-string check at the middleware tier
//                     (RequireScopesWithParam — Quorum ADR-0012)
//
// Identity-establishing handlers (ValidateSession, OAuth callbacks,
// MintSession) MUST use ScopeTenant — using ScopeUser deadlocks them
// against the chicken-and-egg requirement of carrying their own user_id.
func EnforceScope(ctx context.Context, h *Handler, platforms PlatformTenants) error {
	if h == nil {
		return fmt.Errorf("%w: nil handler", ErrScopeDenied)
	}
	switch h.Scope {
	case ScopeNone:
		return nil
	case ScopePlatform:
		t := TenantFromContext(ctx)
		if !platforms.IsPlatform(t) {
			return fmt.Errorf("%w: handler %s requires platform tier (tenant=%q)", ErrScopeDenied, h.FQN(), t)
		}
	case ScopeTenant:
		if TenantFromContext(ctx) == "" {
			return fmt.Errorf("%w: handler %s requires tenantId", ErrScopeDenied, h.FQN())
		}
	case ScopeOrg:
		if TenantFromContext(ctx) == "" {
			return fmt.Errorf("%w: handler %s requires tenantId", ErrScopeDenied, h.FQN())
		}
		if OrgFromContext(ctx) == "" {
			return fmt.Errorf("%w: handler %s requires orgId", ErrScopeDenied, h.FQN())
		}
	case ScopeUser:
		if TenantFromContext(ctx) == "" {
			return fmt.Errorf("%w: handler %s requires tenantId", ErrScopeDenied, h.FQN())
		}
		if UserFromContext(ctx) == "" {
			return fmt.Errorf("%w: handler %s requires userId", ErrScopeDenied, h.FQN())
		}
	case ScopeProfile:
		// Profile membership is enforced at the middleware tier
		// (RequireScopesWithParam). At the dispatch tier we ensure the
		// tenant prerequisite is present; the middleware decides whether
		// the user belongs to the named billing profile.
		if TenantFromContext(ctx) == "" {
			return fmt.Errorf("%w: handler %s requires tenantId", ErrScopeDenied, h.FQN())
		}
	case ScopeUnknown:
		// Fail-closed sentinel installed by mapProtoScope when a proto enum
		// is not recognised. A handler bound to ScopeUnknown is structurally
		// invalid — refuse every dispatch attempt with a clear diagnostic so
		// the misconfiguration shows up in logs at the first call site.
		return fmt.Errorf("%w: handler %s has fail-closed ScopeUnknown — add an explicit case in rpc/protoscan.go:mapProtoScope", ErrScopeDenied, h.FQN())
	default:
		return fmt.Errorf("%w: unknown scope %q on handler %s", ErrScopeDenied, h.Scope, h.FQN())
	}
	return nil
}
