/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package scope

// AuthenticatedIdentity is the identity presence that a trusted server-side
// transport, session middleware, or product adapter established before scope
// enforcement. PlatformTenantID identifies the calling application/platform
// tenant. OrganizationID is a separate customer-organisation presence proof;
// Loom never derives it from PlatformTenantID. UserID is the authenticated
// principal subject.
//
// Request bodies, RPCRequest.Context maps, task Metadata and other mutable
// caller fields MUST NOT be copied into this value. Product code that needs to
// establish customer organisation authority resolves it from its private
// tenancy store first, then supplies the resulting presence to the dispatch
// adapter.
type AuthenticatedIdentity struct {
	PlatformTenantID string
	OrganizationID   string
	UserID           string
}

// PlatformTenants is the canonical allowlist used to decide whether an
// authenticated platform-tenant identity may satisfy ScopePlatform. Keeping
// this evaluator in the dependency-free scope package lets the rpc and legacy
// handlers registries consume exactly the same fail-closed rule.
//
// The zero value is deliberately safe: nil and empty sets authorize nobody.
type PlatformTenants map[string]struct{}

// NewPlatformTenants builds a platform-tenant allowlist. Empty identities are
// not authority and are discarded; duplicates collapse naturally.
func NewPlatformTenants(ids ...string) PlatformTenants {
	out := make(PlatformTenants, len(ids))
	for _, id := range ids {
		if id != "" {
			out[id] = struct{}{}
		}
	}
	return out
}

// IsPlatform reports whether tenantID is an exact member of the allowlist.
// Nil, empty, empty-identity and foreign-identity checks all fail closed.
func (p PlatformTenants) IsPlatform(tenantID string) bool {
	if tenantID == "" {
		return false
	}
	_, ok := p[tenantID]
	return ok
}

// PresenceVerdict is the single canonical result vocabulary shared by the rpc
// and legacy handlers dispatch surfaces. Callers add handler-specific
// diagnostics and public sentinel errors at their own boundary.
type PresenceVerdict uint8

const (
	PresenceSatisfied PresenceVerdict = iota
	PresencePlatformRequired
	PresenceTenantRequired
	PresenceOrganizationRequired
	PresenceUserRequired
	PresenceUnknownScope
)

// CheckPresence is a dependency-free pure evaluator. It accepts no context,
// mutable map, handler or product resolver. Product authorisation remains
// outside Loom: OrganizationID being present is not proof that a resource
// belongs to it. isPlatform must be computed from the registry's configured
// allowlist against identity.PlatformTenantID.
func CheckPresence(
	declared TenantScope,
	identity AuthenticatedIdentity,
	isPlatform bool,
) PresenceVerdict {
	switch declared {
	case None:
		return PresenceSatisfied
	case Platform:
		if !isPlatform {
			return PresencePlatformRequired
		}
	case Tenant:
		if identity.PlatformTenantID == "" {
			return PresenceTenantRequired
		}
	case Org:
		if identity.PlatformTenantID == "" {
			return PresenceTenantRequired
		}
		if identity.OrganizationID == "" {
			return PresenceOrganizationRequired
		}
	case User:
		if identity.PlatformTenantID == "" {
			return PresenceTenantRequired
		}
		if identity.UserID == "" {
			return PresenceUserRequired
		}
	case Profile:
		if identity.PlatformTenantID == "" {
			return PresenceTenantRequired
		}
	default:
		return PresenceUnknownScope
	}
	return PresenceSatisfied
}
