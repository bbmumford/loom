/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package ports

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"strings"

	"github.com/bbmumford/loom/pkg/rpc/scope"
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

// Scope is the opaque scope label of a handler. It keeps the historical
// name but aliases the one canonical defined type (scope.ScopeTag) in
// pkg/rpc/scope, so SecureHandler remains structurally satisfied by registry
// metadata without permitting arbitrary strings or creating a second scope
// identity.
type Scope = scope.ScopeTag

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
	Scope() Scope
	AllowedPartitions() []string
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

// PartitionPrincipalReader is an OPTIONAL, read-only extension implemented by an
// AuthValidator that can read the private partition principal already
// established by its authenticated transport, session, or authoritative store.
//
// It deliberately cannot stamp a partition. The asynchronous task body carries
// a caller-supplied partition claim, so the executor may project only this
// private principal into handler metadata and must never turn the body field
// into authority by calling AuthValidator.WithPartitionID.
type PartitionPrincipalReader interface {
	ExecutionPartitionID(ctx context.Context) (partitionID string, ok bool)
}

// ExecutionPartitionKey is an opaque, immutable and comparable identity used
// for async admission, quota and completion accounting. A product adapter
// canonicalizes whatever identity fields its own authority model defines into
// one string; loom retains only its SHA-256 digest, so loom cannot read or
// reinterpret any field the adapter encoded.
type ExecutionPartitionKey struct {
	digest [sha256.Size]byte
}

// NewExecutionPartitionKey seals a product-canonical partition identity. The
// input is authority material: callers must build it from server-established
// identity, never a task body, trigger spec, correlation handle or other
// caller-supplied claim.
func NewExecutionPartitionKey(canonical string) (ExecutionPartitionKey, error) {
	if strings.TrimSpace(canonical) == "" {
		return ExecutionPartitionKey{}, fmt.Errorf("execution partition canonical identity is empty")
	}
	return ExecutionPartitionKey{digest: sha256.Sum256([]byte(canonical))}, nil
}

// Valid reports whether the key was constructed from a nonempty canonical
// identity.
func (k ExecutionPartitionKey) Valid() bool {
	return k != ExecutionPartitionKey{}
}

// Fingerprint returns the non-reversible digest used for diagnostics and
// durable map keys. It does not reveal the product's canonical identity.
func (k ExecutionPartitionKey) Fingerprint() string {
	if !k.Valid() {
		return ""
	}
	return hex.EncodeToString(k.digest[:])
}

// ExecutionPrincipal is the product-owned authority retained by an
// asynchronous admission. PartitionKey is immutable identity; AuthorizeExecution
// must re-establish product-private context and re-check any current,
// revocable policy before execution. The returned release function holds that
// exact authorization generation through the caller's complete dispatch
// boundary. Callers MUST invoke it exactly once on every successful
// authorization. Loom never derives identity or authority from task payload
// fields.
type ExecutionPrincipal interface {
	PartitionKey() ExecutionPartitionKey
	AuthorizeExecution(
		ctx context.Context,
	) (authorized context.Context, release func(), err error)
}

// ExecutionPrincipalReader is the read-only product seam used at admission
// and after AuthorizeExecution. Implementations read a private context key
// populated by authenticated transport/session or a product-owned service
// root; they cannot derive a principal from mutable task data.
type ExecutionPrincipalReader interface {
	ExecutionPrincipal(ctx context.Context) (ExecutionPrincipal, bool)
}

// AuthenticatedPrincipalStamper is the preferred receive-side identity seam.
// The RPC server calls it only after the typed wire principal's platform
// partition has matched the server-resolved transport ScopeID. Implementations
// project that one immutable snapshot onto their private product context keys;
// they must not consult RPCRequest.Context or merge caller fragments.
//
// It remains optional so products that have not adopted customer-organisation
// authority fail closed at org/user scope rather than gaining authority from
// the legacy mutable context map.
type AuthenticatedPrincipalStamper interface {
	WithAuthenticatedPrincipal(
		ctx context.Context,
		principal scope.AuthenticatedPrincipal,
	) context.Context
}

// TransportAxisPin is the OPTIONAL consumer seam that names which received
// identity axis a node reconciles against its transport-established partition.
// The RPC server gates that axis's wire value for equality with the transport
// tenant before the reconstructed identity enters dispatch — the cross-tenant
// forge gate: a mesh peer bound to transport tenant T cannot present a
// principal whose pinned axis is any tenant but T. loom never names the axis;
// the consuming project returns the canonical name it stamps that axis under
// (e.g. its platform-tenant axis). A validator that omits this seam cannot
// present customer authority over the mesh — the RPC server fails closed rather
// than accept an un-pinned principal, so a missing seam never becomes a forge
// hole. The owner partition is NOT pinned here: it is derived receive-side from
// a within-tenant axis after this gate, so within-tenant multi-owner traffic
// still round-trips (over-pinning it would deny that legitimately).
type TransportAxisPin interface {
	TransportPinnedAxis() string
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
	// the partition lift, before dispatch. An empty userId + nil scopes is a
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
