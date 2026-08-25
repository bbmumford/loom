/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package scope

// PresencePolicy is the consumer-supplied evaluator loom delegates ALL scope
// semantics to. loom carries a handler's declared ScopeTag and the opaque
// AuthenticatedIdentity established by a trusted transport/session/product
// adapter, but interprets neither: what a given ScopeTag requires of an
// identity is the consuming project's decision. The dispatch surfaces call
// CheckPresence for every declared scope other than the opt-out tier and deny
// the call when it reports unsatisfied.
//
// A nil PresencePolicy is not "allow": every loom enforcement site treats an
// absent policy as fail-CLOSED for any handler that declares a scope, because
// the failure mode of this core is a silent fail-OPEN. Supplying a policy is
// how a consumer opts a deployment into evaluated scope enforcement.
type PresencePolicy interface {
	// CheckPresence reports whether identity satisfies declared. reason is a
	// short human-readable diagnostic surfaced (wrapped in ErrDenied) when
	// satisfied is false; it carries no authority and callers must not parse
	// it. The evaluator must be pure: it consumes only its two arguments and
	// never a request body, context map or other mutable caller field.
	CheckPresence(declared ScopeTag, identity AuthenticatedIdentity) (satisfied bool, reason string)
}

// PresencePolicyFunc adapts a plain function to the PresencePolicy interface so
// a consumer can supply an evaluator without declaring a named type.
type PresencePolicyFunc func(declared ScopeTag, identity AuthenticatedIdentity) (satisfied bool, reason string)

// CheckPresence calls f.
func (f PresencePolicyFunc) CheckPresence(declared ScopeTag, identity AuthenticatedIdentity) (bool, string) {
	return f(declared, identity)
}
