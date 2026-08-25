/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package scope

// ScopeValue is one opaque, named identity axis a trusted server-side
// transport, session middleware, or product adapter established. loom carries
// Name and Value without interpreting either; the consuming project assigns
// meaning to Name. loom never derives one axis from another.
type ScopeValue struct {
	Name  string
	Value string
}

// AuthenticatedIdentity is the opaque identity presence a trusted server-side
// transport, session middleware, or product adapter established before scope
// enforcement. Scopes is an ordered set of named axes; Subject is the
// authenticated principal subject. loom interprets neither a Name nor the
// Subject: a consumer's PresencePolicy decides what a handler's declared
// ScopeTag requires of them.
//
// Request bodies, RPCRequest.Context maps, task Metadata and other mutable
// caller fields MUST NOT be copied into this value. A consuming project that
// needs an axis (e.g. a customer organisation) resolves it from its own store
// first, then supplies the resulting presence to the dispatch adapter.
type AuthenticatedIdentity struct {
	Scopes  []ScopeValue
	Subject string
}

// Scope returns the value of the named axis, or "" when the axis is absent.
func (a AuthenticatedIdentity) Scope(name string) string {
	for i := range a.Scopes {
		if a.Scopes[i].Name == name {
			return a.Scopes[i].Value
		}
	}
	return ""
}

// Has reports whether the named axis is present with a non-empty value.
func (a AuthenticatedIdentity) Has(name string) bool {
	return a.Scope(name) != ""
}

// clone returns a defensive copy of the identity's axis slice.
func (a AuthenticatedIdentity) clone() AuthenticatedIdentity {
	if len(a.Scopes) == 0 {
		return AuthenticatedIdentity{Subject: a.Subject}
	}
	out := AuthenticatedIdentity{
		Scopes:  make([]ScopeValue, len(a.Scopes)),
		Subject: a.Subject,
	}
	copy(out.Scopes, a.Scopes)
	return out
}
