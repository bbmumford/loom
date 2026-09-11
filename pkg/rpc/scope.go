/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package rpc

import (
	"context"
	"fmt"

	rpcscope "github.com/bbmumford/loom/pkg/rpc/scope"
)

// rpcContextKey is the typed key for the in-process string map WithRPCContext
// stamps onto a Go context.Context. Only server code in the calling process
// writes it; loom's receive path does not copy the wire request map into it.
type rpcContextKey struct{}

// WithRPCContext stamps an in-process string map onto ctx. The readers in this
// package (RPCContextValue, OpClassFromContext and the identity accessors
// below) read it. Nothing copies it onto an outgoing mesh request: identity
// crosses the hop only as the sealed principal.
//
// This map is caller/wire data, not scope authority. EnforceScope deliberately
// ignores it and reads only the typed identity installed through
// WithAuthenticatedScopeIdentity. Keeping the two context surfaces separate
// prevents a request body or forwarded map from manufacturing partition,
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
// absent. It is a plain passthrough of author-set metadata and confers no authority on its own — the
// reader must still validate whatever it finds.
func RPCContextValue(ctx context.Context, key string) (string, bool) {
	m := rpcContextMap(ctx)
	if m == nil {
		return "", false
	}
	v, ok := m[key]
	return v, ok
}

// TenantFromContext returns the "tenantId" an in-process writer stamped via
// WithRPCContext, or "". It is never taken from the wire, and it names the
// node's transport-pinned value, not a data partition.
//
// Deprecated: select data on AuthenticatedScopeIdentityFromContext(ctx).Partition(),
// and gate a platform-plane operation on an explicit capability the caller
// holds, not on this value.
func TenantFromContext(ctx context.Context) string {
	return rpcContextMap(ctx)["tenantId"]
}

// UserFromContext returns the "userId" an in-process writer stamped via
// WithRPCContext, or "".
//
// Deprecated: read AuthenticatedScopeIdentityFromContext(ctx).Subject.
func UserFromContext(ctx context.Context) string {
	return rpcContextMap(ctx)["userId"]
}

// AuthenticatedScopeIdentity is the typed, server-established identity used by
// EnforceScope. It aliases the leaf scope package so the legacy handlers
// registry and this package consume the same value and evaluator without an
// import cycle.
type AuthenticatedScopeIdentity = rpcscope.AuthenticatedIdentity

// WithAuthenticatedScopeIdentity stamps one complete authenticated identity
// snapshot. Only trusted transport/session/product adapters may call this:
// mutable RPC maps, request bodies and task metadata are not authority.
func WithAuthenticatedScopeIdentity(
	ctx context.Context,
	identity AuthenticatedScopeIdentity,
) context.Context {
	return rpcscope.WithAuthenticatedIdentity(ctx, identity)
}

// AuthenticatedScopeIdentityFromContext reads the typed scope identity.
func AuthenticatedScopeIdentityFromContext(ctx context.Context) AuthenticatedScopeIdentity {
	return rpcscope.AuthenticatedIdentityFromContext(ctx)
}

// OpClassFromContext extracts opClass (set by the caller via
// dispatch.WithOpClass). Receivers use this for stream-priority routing.
func OpClassFromContext(ctx context.Context) string {
	return rpcContextMap(ctx)["opClass"]
}

// ErrScopeDenied is the sentinel returned by EnforceScope when a handler's
// declared Scope cannot be satisfied by the caller's context. Callers
// should use errors.Is at boundaries (e.g. HTTP bridge → 403, server-side
// dispatch → 403 wire error). The wrapping error includes the FQN +
// offending fields for diagnostic logging.
var ErrScopeDenied = rpcscope.ErrDenied

// EnforceScope rejects calls whose ctx fails the handler's declared
// Scope. loom itself decides only three things and delegates the rest:
//
//   - ScopeNone admits unconditionally — it is the handler's explicit opt-out
//     of scope enforcement (liveness, /heartbeat, OAuth init).
//   - ScopeUnset (the undeclared zero value) fails CLOSED: a handler must
//     choose a tag.
//   - a nil policy fails CLOSED for every other tag: without a consumer
//     evaluator loom cannot judge presence, and the failure mode of this
//     core is a silent fail-OPEN, so absence denies.
//
// For every other tag loom hands the opaque declared tag and the opaque
// server-established identity to the consumer-supplied PresencePolicy and
// denies when it reports unsatisfied. The wrapped error carries the handler
// FQN and the policy's reason; callers should use errors.Is(err,
// ErrScopeDenied) at boundaries.
// authz is an optional two-arm op authorizer (variadic so pre-two-arm callers
// keep the 3-arg form). When a handler declares a Requirement it is judged by
// that authorizer, not its Scope tag; a caller that supplies none, or a nil one,
// fails a Requirement-bearing handler CLOSED (symmetric to a nil PresencePolicy).
func EnforceScope(ctx context.Context, h *Handler, policy rpcscope.PresencePolicy, authz ...*rpcscope.Policy) error {
	if h == nil {
		return fmt.Errorf("%w: nil handler", ErrScopeDenied)
	}
	// Two-arm authorization supersedes the presence tier when the handler declares
	// a Requirement. A nil Requirement (the zero value) falls through to the Scope
	// path below, so this is behavior-preserving until handlers populate it.
	if h.Requirement != nil {
		var az *rpcscope.Policy
		if len(authz) > 0 {
			az = authz[0]
		}
		if az == nil {
			return fmt.Errorf("%w: handler %s declares a Requirement but no authorization policy is configured", ErrScopeDenied, h.FQN())
		}
		if ok, reason := az.Authorize(*h.Requirement, rpcscope.PrincipalForEnforcement(ctx)); !ok {
			return fmt.Errorf("%w: handler %s denied by authorization policy: %s", ErrScopeDenied, h.FQN(), reason)
		}
		return nil
	}
	if h.Scope == ScopeNone {
		return nil
	}
	if h.Scope == rpcscope.ScopeUnset {
		return fmt.Errorf("%w: handler %s has an undeclared scope (ScopeUnset) — declare a scope tier explicitly", ErrScopeDenied, h.FQN())
	}
	if policy == nil {
		// Fail CLOSED: no evaluator means loom cannot judge presence for a
		// handler that opted into enforcement.
		return fmt.Errorf("%w: handler %s requires a presence policy but none is configured", ErrScopeDenied, h.FQN())
	}
	ok, reason := policy.CheckPresence(h.Scope, rpcscope.AuthenticatedIdentityFromContext(ctx))
	if !ok {
		return fmt.Errorf("%w: handler %s denied by presence policy: %s", ErrScopeDenied, h.FQN(), reason)
	}
	return nil
}
