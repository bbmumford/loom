/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package rpc

import (
	"context"
	"fmt"
	"strings"

	tenantScope "github.com/bbmumford/loom/pkg/rpc/scope"
)

// rpcContextKey is the typed key used to stamp the RPCRequest.Context map
// onto a Go context.Context. Receivers (BidiRPC server, local dispatch,
// HTTP bridge) call WithRPCContext to surface the wire-side context map
// to in-process handlers + scope enforcement.
type rpcContextKey struct{}

// WithRPCContext stamps the RPCRequest.Context map onto ctx so receivers
// can read tenantId/userId/orgId/opClass via the *FromContext helpers
// in this package.
//
// This map is caller/wire data, not scope authority. EnforceScope deliberately
// ignores it and reads only the typed identity installed through
// WithAuthenticatedScopeIdentity. Keeping the two context surfaces separate
// prevents a request body or forwarded map from manufacturing tenant,
// organisation, or user presence.
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

// RPCContextValue reads one value stamped into the rpc context by WithRPCContext. It is the exported
// read counterpart to WithRPCContext for a caller that threads a custom key across the mesh dispatch
// (e.g. a surface session ticket the target's EnforceScope re-verifies); ok is false when the key is
// absent. Unlike the tenant/user/org accessors this is a plain passthrough of author-set metadata and
// confers no authority on its own — the reader must still validate whatever it finds.
func RPCContextValue(ctx context.Context, key string) (string, bool) {
	m := rpcContextMap(ctx)
	if m == nil {
		return "", false
	}
	v, ok := m[key]
	return v, ok
}

// TenantFromContext extracts tenantId from the rpc context.
// Returns "" when no tenant has been stamped.
//
// 🛑 THERE IS DELIBERATELY NO WRITE-SIDE COUNTERPART TO THIS, AND THAT
// ABSENCE IS DOCUMENTED HERE BECAUSE THIS IS WHERE PEOPLE LOOK FOR IT
// (#R-1598 ③).
//
// On the SEND side, dispatch stamps tenantId from the process-wide platform
// tenant (HWPCaller.SetPlatformTenant) and copies ONLY scopes and userId out
// of the caller's context — so a tenant placed in a ctx before an outgoing
// call is SILENTLY IGNORED and the RPC goes out carrying the platform
// identity. That is #R-782/#K-32's anti-spoof property, not an oversight: a
// caller-supplied context must not be able to choose the authoritative
// tenant.
//
// So the value this returns is the PLATFORM tenant, and it authenticates the
// calling node — it is not the end user's org and must not be used as an
// authorisation subject. The authenticated principal travels as userId +
// scopes; per platform §4.2 (ruled #R-1598 ①) a handler that needs the
// end-user tenant RESOLVES IT SERVER-SIDE from that principal, the same way
// §4.6 requires ingress routing be a server-side lookup rather than a token
// decode.
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

// AuthenticatedScopeIdentity is the typed, server-established identity used by
// EnforceScope. It aliases the leaf scope package so the legacy handlers
// registry and this package consume the same value and evaluator without an
// import cycle.
type AuthenticatedScopeIdentity = tenantScope.AuthenticatedIdentity

// WithAuthenticatedScopeIdentity stamps one complete authenticated identity
// snapshot. Only trusted transport/session/product adapters may call this:
// mutable RPC maps, request bodies and task metadata are not authority.
func WithAuthenticatedScopeIdentity(
	ctx context.Context,
	identity AuthenticatedScopeIdentity,
) context.Context {
	return tenantScope.WithAuthenticatedIdentity(ctx, identity)
}

// AuthenticatedScopeIdentityFromContext reads the typed scope identity.
func AuthenticatedScopeIdentityFromContext(ctx context.Context) AuthenticatedScopeIdentity {
	return tenantScope.AuthenticatedIdentityFromContext(ctx)
}

// OpClassFromContext extracts opClass (set by the caller via
// dispatch.WithOpClass). Receivers use this for stream-priority routing.
func OpClassFromContext(ctx context.Context) string {
	return rpcContextMap(ctx)["opClass"]
}

// ScopesFromContext extracts the caller's authenticated scope list from the
// rpc context map. The sender stamps it via dispatch.WithScopes, which
// buildRPCRequestCtx serializes into req.Context["scopes"] as a space-joined
// string (RFC-6749 §3.3 convention, #K-32); the receiver surfaces that map
// onto ctx via WithRPCContext, and this splits it back to a slice. Returns
// nil when no scopes were stamped. strings.Fields drops empty tokens, so a
// stray double-space in the wire form yields no phantom scope.
func ScopesFromContext(ctx context.Context) []string {
	return ParseScopes(rpcContextMap(ctx)["scopes"])
}

// ParseScopes decodes the space-joined wire form of the scope list back to
// a slice (nil for empty). Exported so the RPC server's receive path can
// decode req.Context["scopes"] directly without first re-stamping the map
// onto a ctx. strings.Fields drops empty tokens (robust to stray spaces).
func ParseScopes(raw string) []string {
	if raw == "" {
		return nil
	}
	return strings.Fields(raw)
}

// ErrScopeDenied is the sentinel returned by EnforceScope when a handler's
// declared TenantScope cannot be satisfied by the caller's context. Callers
// should use errors.Is at boundaries (e.g. HTTP bridge → 403, server-side
// dispatch → 403 wire error). The wrapping error includes the FQN +
// offending fields for diagnostic logging.
var ErrScopeDenied = tenantScope.ErrDenied

// PlatformTenants is the canonical set of tenantIds that satisfy
// ScopePlatform. It aliases the dependency-free scope implementation so the
// rpc and legacy handlers registries cannot drift. An empty set fails closed.
type PlatformTenants = tenantScope.PlatformTenants

// NewPlatformTenants builds a PlatformTenants set from a list of tenant ids.
// Convenience constructor; duplicates collapse, empty strings are dropped.
func NewPlatformTenants(ids ...string) PlatformTenants {
	return tenantScope.NewPlatformTenants(ids...)
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
//     scope-string check at the middleware tier
//     (RequireScopesWithParam — Quorum ADR-0012)
//
// Identity-establishing handlers (ValidateSession, OAuth callbacks,
// MintSession) MUST use ScopeTenant — using ScopeUser deadlocks them
// against the chicken-and-egg requirement of carrying their own user_id.
func EnforceScope(ctx context.Context, h *Handler, platforms PlatformTenants) error {
	if h == nil {
		return fmt.Errorf("%w: nil handler", ErrScopeDenied)
	}
	identity := tenantScope.AuthenticatedIdentityFromContext(ctx)
	switch tenantScope.CheckPresence(
		h.Scope,
		identity,
		platforms.IsPlatform(identity.PlatformTenantID),
	) {
	case tenantScope.PresenceSatisfied:
		return nil
	case tenantScope.PresencePlatformRequired:
		return fmt.Errorf(
			"%w: handler %s requires platform tier (tenant=%q)",
			ErrScopeDenied,
			h.FQN(),
			identity.PlatformTenantID,
		)
	case tenantScope.PresenceTenantRequired:
		return fmt.Errorf("%w: handler %s requires authenticated platform tenant", ErrScopeDenied, h.FQN())
	case tenantScope.PresenceOrganizationRequired:
		return fmt.Errorf("%w: handler %s requires authenticated organisation", ErrScopeDenied, h.FQN())
	case tenantScope.PresenceUserRequired:
		return fmt.Errorf("%w: handler %s requires authenticated user", ErrScopeDenied, h.FQN())
	case tenantScope.PresenceUnknownScope:
		if h.Scope == ScopeUnknown {
			// Fail-closed sentinel installed by mapProtoScope when a proto enum
			// is not recognised.
			return fmt.Errorf("%w: handler %s has fail-closed ScopeUnknown — add an explicit case in rpc/protoscan.go:mapProtoScope", ErrScopeDenied, h.FQN())
		}
		return fmt.Errorf("%w: unknown scope %q on handler %s", ErrScopeDenied, h.Scope, h.FQN())
	default:
		return fmt.Errorf("%w: handler %s has fail-closed ScopeUnknown — add an explicit case in rpc/protoscan.go:mapProtoScope", ErrScopeDenied, h.FQN())
	}
}
