/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package ports

import (
	"context"
	"net"
)

// This file holds the Config injection seams that replace the HSTLES
// library couplings (plan §0.3). Zero-value = current behaviour for all of
// them EXCEPT Platform: loom cannot reproduce host.Detect() auto-detection,
// so nil Platform falls back to DevPlatform() and cloud deployments MUST
// inject their platform implementation or they silently lose
// fly-global-services UDP bind, 6PN origin detection, and region codes.

// MeshFallbackStatsFunc supplies session-validate fallback-cache counters
// (§0.3 #1, replaces platform/session.MeshFallbackStats). The return type is
// uint64 — NOT int — because the values are assigned directly into the
// metrics map. When nil, the runtime still emits
// session_validate_cache_hits=0 / session_validate_cache_misses=0 so the
// metrics schema is unchanged.
type MeshFallbackStatsFunc func() (hits, misses uint64)

// TenantScope is the tenant-isolation level of a handler. It is an ALIAS of
// string — the same underlying declaration as pkg/rpc/scope.TenantScope and
// handlers.TenantScope — so SecureHandler is structurally satisfied by the
// registry's handler metadata without any adaptation. Do not change this to
// a defined type: that silently breaks the implicit interface conversion at
// every ValidateExecutionAuth call site.
type TenantScope = string

// SecureHandler is the handler metadata surface AuthValidator judges.
// It mirrors platform/security/helpers.SecureHandler method-for-method
// (all parameter/return types are stdlib or aliases), so the registry's
// handler entries satisfy both interfaces and the HSTLES-side validator
// can delegate to the real helpers package with a plain conversion.
type SecureHandler interface {
	Name() string
	RequiresAuth() bool
	AllowedAuthTypes() []string
	Scopes() []string
	TenantScope() TenantScope
	AllowedTenants() []string
}

// AuthValidator replaces the platform/security/helpers hard import
// (§0.3 #5 — the HIGHEST-DANGER cut, shared unexported context keys).
//
// The HSTLES-build implementation MUST delegate to the real helpers package:
// WithTenantID must write the SAME unexported context key that HSTLES domain
// handlers read via helpers.ExtractTenantID, or nested rpc.Calls silently
// lose the tenant and multi-tenant authz/data-scoping breaks with no
// compile-time signal. The loom-local default (internal/securityctx) is
// fail-closed and safe ONLY in builds with no HSTLES domain handlers.
type AuthValidator interface {
	// ValidateExecutionAuth gates handler execution on the task-executor
	// path (fail closed: unknown auth state denies RequiresAuth handlers).
	ValidateExecutionAuth(ctx context.Context, h SecureHandler) error

	// WithTenantID stamps the tenant onto ctx for nested calls.
	WithTenantID(ctx context.Context, tenantID string) context.Context
}

// ScopeStamper is an OPTIONAL interface an injected AuthValidator MAY also
// implement to receive the caller's wire-propagated identity (#K-32). When
// the RPC server finds its AuthValidator also satisfies ScopeStamper it
// lifts the userId + scope-list decoded from the request envelope onto ctx,
// so scope enforcement (a handler's RequiredScopes) and userId-scoped
// handlers see the authenticated caller that crossed the mesh hop.
//
// It is deliberately a SEPARATE, optional interface rather than two more
// methods on AuthValidator: extending AuthValidator would break every
// existing injected validator at compile time. A validator that does not
// implement ScopeStamper simply does not get mesh-propagated scopes — the
// safe, non-breaking default (enforcement stays closed until the endpoint
// adopts it). The HSTLES/ORBTR validator implements it by delegating to the
// real security-helpers keys (symmetric to WithTenantID); loom's default
// securityctx validator implements it against its loom-local keys.
type ScopeStamper interface {
	// WithWireIdentity stamps the mesh-propagated userId + scope-list onto
	// ctx as an authenticated principal. Called once per inbound RPC after
	// the tenant lift, before dispatch. An empty userId + nil scopes is a
	// no-op the implementation may skip.
	WithWireIdentity(ctx context.Context, userID string, scopes []string) context.Context
}

// BootstrapInfo is what the VL1 bootstrap handshake knows about a joining
// node — lifted verbatim from the X-VL1-* header contract (Node-ID,
// Service-Name, Region, Roles, Private-IP, Public-IP; the header set is on
// the wire no-drift list).
type BootstrapInfo struct {
	NodeID      NodeID
	ServiceName string
	Region      string
	Roles       []string
	PrivateIP   string
	PublicIP    string
}

// VerifyBootstrapFunc abstracts the hstles.anchor.VerifyBootstrap client
// call previously hard-coded in the mesh runtime (§0.3 #7 — the coupling
// the original cut-list missed). Non-anchor nodes invoke it during VL1
// bootstrap. Semantics the implementation and caller preserve exactly:
//   - err != nil (verifier unreachable) → graceful ALLOW — the Noise
//     handshake is the real security boundary, availability wins;
//   - allowed=false → reject the join with 403 and reason;
//   - anchor-capable nodes skip the call entirely.
//
// nil = always allow (standalone/dev meshes). The HSTLES implementation
// performs the rpc.Call against its anchor domain proto.
type VerifyBootstrapFunc func(ctx context.Context, info BootstrapInfo) (allowed bool, reason string, err error)

// AddressSource identifies a reach address discovery source, priority-ordered
// per platform by PlatformInfo.PreferredSources.
type AddressSource string

const (
	SourcePlatformEnv AddressSource = "platform-env" // FLY_PUBLIC_IP etc.
	SourceIMDS        AddressSource = "imds"
	SourceDNS         AddressSource = "dns"
	SourceInterface   AddressSource = "interface"
	SourceK8sDownward AddressSource = "k8s-downward"
	SourceSTUN        AddressSource = "stun"
	SourceUPnP        AddressSource = "upnp"
)

// PlatformInfo is loom's platform seam (§0.3 #4, replaces platform/host —
// NOT a leaf: 913 LOC of Fly/AWS/GCP/k8s detectors stay in HSTLES and are
// injected). The method set mirrors host.PlatformInfo; because
// PreferredSources returns a loom-local AddressSource slice, HSTLES wraps
// its host.PlatformInfo in a one-line adapter at injection time. The
// interface remains structurally assignable where aether expects a
// bind-address resolver (ResolveUDPBind/OpenUDPListener are stdlib-typed).
type PlatformInfo interface {
	// Region returns the normalized 3-letter region code ("iad", "syd");
	// "unknown" when undeterminable.
	Region() string

	// MachineID returns the platform machine/instance identifier, "" if none.
	MachineID() string

	// AppName returns the app/service name as known to the platform, "" if none.
	AppName() string

	// PublicIP returns the platform-known public IP, "" → caller falls back
	// to STUN.
	PublicIP() string

	// PrivateIP returns the private mesh IP (Fly: FLY_PRIVATE_IP 6PN fdaa:;
	// AWS/GCP: VPC IP; k8s: POD_IP), "" → interface enumeration fallback.
	// Feeds ourOrigin detection for the cross-org forwarder and the reach
	// publisher's interface filter.
	PrivateIP() string

	// ResolveUDPBind returns the UDP bind address for a port
	// (Fly: "fly-global-services:<port>"; others: ":<port>").
	ResolveUDPBind(port int) string

	// ResolveUDPBindDualStack returns separate IPv4/IPv6 binds, ("","") when
	// one socket handles both.
	ResolveUDPBindDualStack(port int) (ipv4Addr, ipv6Addr string)

	// OpenUDPListener opens a UDP socket with platform bind semantics.
	OpenUDPListener(network, address string) (*net.UDPConn, error)

	// IsCloud reports whether this is a cloud platform (not local dev).
	IsCloud() bool

	// Provider returns "fly", "aws", "gcp", "k8s", or "dev".
	Provider() string

	// EnvTags returns provenance tags the reach publisher attaches to
	// emitted addresses ("fly:app=X"); empty map acceptable.
	EnvTags() map[string]string

	// PreferredSources returns the platform's ordered reach-source
	// preference; empty = publisher defaults.
	PreferredSources() []AddressSource
}
