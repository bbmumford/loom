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

// WithRPCContext stamps an in-process string map onto ctx. RPCContextValue and
// OpClassFromContext in this package read it. Nothing copies it onto an
// outgoing mesh request: identity crosses the hop only as the sealed principal,
// read through AuthenticatedScopeIdentityFromContext.
//
// This map is caller/wire data, not scope authority. EnforceScope deliberately
// ignores it and reads only the typed identity installed through
// WithAuthenticatedScopeIdentity. Keeping the two context surfaces separate
// prevents a request body or forwarded map from manufacturing presence on any
// identity axis.
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

// rpcContextMap returns the map WithRPCContext stamped on ctx, or nil.
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

// AuthenticatedScopeIdentity is the typed, server-established identity used by
// EnforceScope. It aliases the leaf scope package so the node/handlers
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

// OpClassFromContext extracts the opClass entry of the request's context map:
// the class the calling process named with dispatch.WithOpClass, which
// dispatch carries in the request it sends. Receivers use it for
// stream-priority routing. It reads nothing dispatch.WithOpClass wrote into a
// context of this process.
func OpClassFromContext(ctx context.Context) string {
	return rpcContextMap(ctx)["opClass"]
}

// ErrScopeDenied is the sentinel returned by EnforceScope when a handler's
// declared Scope cannot be satisfied by the caller's context. Callers
// should use errors.Is at boundaries (e.g. HTTP bridge → 403, server-side
// dispatch → 403 wire error). The wrapping error includes the FQN +
// offending fields for diagnostic logging.
var ErrScopeDenied = rpcscope.ErrDenied

// ErrRefusalUnrecorded is joined to a refusal Enforce returns when the
// registry's refusal recorder could not record it; the joined error still
// unwraps to ErrScopeDenied. It is rpcscope's sentinel under this package's
// name.
var ErrRefusalUnrecorded = rpcscope.ErrRefusalUnrecorded

// ErrStarting is what a call fails with when the node that would serve it is
// still starting (its handler registry is held until ready) and so ran
// nothing; rpcscope's sentinel under this package's name. A caller may retry,
// and the HTTP bridge answers it 503 with Retry-After.
var ErrStarting = rpcscope.ErrStarting

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
// authz is an optional two-arm op authorizer (variadic so a caller that judges
// only presence tiers keeps the 3-arg form). When a handler declares a
// Requirement it is judged by that authorizer, not its Scope tag; a caller that
// supplies none, or a nil one, fails a Requirement-bearing handler CLOSED
// (symmetric to a nil PresencePolicy).
//
// EnforceScope is the judge alone and records nothing, so it is the probe a test
// judges a handler's declaration with. A server judges a call through
// Registry.Enforce or EnforceAt, which record its refusal; library hostcontract
// refuses a non-test caller of EnforceScope.
func EnforceScope(ctx context.Context, h *Handler, policy rpcscope.PresencePolicy, authz ...*rpcscope.Policy) error {
	return enforceScope(ctx, h, policy, authz...)
}

// CheckPoint is what a call is judged under: the presence and authorization
// policies EnforceScope applies and the recorder each refusal is recorded
// through before it returns. *Registry is one. A server that dispatches a call
// outside a Registry's Dispatch and HTTP bridge (node.orbtr.io's per-partition
// mesh dispatch, whose resolver holds the three) is another, and judges it with
// EnforceAt.
type CheckPoint interface {
	PresencePolicy() rpcscope.PresencePolicy
	AuthzPolicy() *rpcscope.Policy
	RefusalRecorder() rpcscope.RefusalRecorder
}

// Enforce is this registry's check point: EnforceAt over the registry's own
// policies and recorder. Dispatch and the HTTP bridge's local, proxied and
// streamed calls judge through it.
func (r *Registry) Enforce(ctx context.Context, h *Handler) error {
	return EnforceAt(ctx, r, h)
}

// EnforceAt judges h under cp's presence and authorization policies
// (EnforceScope), and for a refusal of h calls cp's refusal recorder with ctx,
// the caller and the permission h asks before the refusal returns
// (rpcscope.RecordRefusal). A refusal the recorder could not record returns as
// the refusal, joined with rpcscope.ErrRefusalUnrecorded. A nil cp refuses every
// call, as a nil policy does.
func EnforceAt(ctx context.Context, cp CheckPoint, h *Handler) error {
	if cp == nil {
		return fmt.Errorf("%w: no check point to judge the call under", ErrScopeDenied)
	}
	err := enforceScope(ctx, h, cp.PresencePolicy(), cp.AuthzPolicy())
	if err == nil || h == nil {
		return err
	}
	return rpcscope.RecordRefusal(ctx, cp.RefusalRecorder(), rpcscope.Refusal{
		Operation:   h.FQN(),
		Caller:      rpcscope.AuthenticatedIdentityFromContext(ctx),
		Requirement: h.Requirement,
		Scope:       h.Scope,
	}, err)
}

func enforceScope(ctx context.Context, h *Handler, policy rpcscope.PresencePolicy, authz ...*rpcscope.Policy) error {
	if h == nil {
		return fmt.Errorf("%w: nil handler", ErrScopeDenied)
	}
	// Two-arm authorization supersedes the presence tier when the handler declares
	// a Requirement. A nil Requirement pointer (the Handler field's zero value)
	// falls through to the Scope path below.
	if h.Requirement != nil {
		var az *rpcscope.Policy
		if len(authz) > 0 {
			az = authz[0]
		}
		if ok, reason := rpcscope.AuthorizeRequirement(ctx, *h.Requirement, az); !ok {
			return fmt.Errorf("%w: handler %s %s", ErrScopeDenied, h.FQN(), reason)
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
