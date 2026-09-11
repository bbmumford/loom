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
// WithPartitionID must write the SAME unexported context key that HSTLES domain
// handlers read via helpers.ExtractTenantID, or nested rpc.Calls silently
// lose the tenant and multi-tenant authz/data-scoping breaks with no
// compile-time signal. The loom-local default (internal/securityctx) is
// fail-closed and safe ONLY in builds with no HSTLES domain handlers.
// EXTENSIONS, AND WHAT WRAPPING ONE COSTS. Four optional interfaces in this
// package are type-asserted on the value supplied here, and a failed assertion
// changes behaviour rather than erroring: AuthenticatedPrincipalStamper (the
// RPC receive path, stamping the admitted principal onto ctx; skipped when
// absent), PrincipalAdmitter (the RPC receive path, judging a mesh principal
// against its transport; every mesh principal is refused when absent), and
// ExecutionPrincipalReader with PartitionPrincipalReader (the task executor;
// skipped when absent).
//
// A DECORATOR MUST THEREFORE RE-EXPOSE EVERY EXTENSION ITS INNER VALIDATOR
// IMPLEMENTS. Wrapping a validator to add one behaviour drops the others
// silently — the assertion fails, the feature stops, and there is no error and
// no compile-time signal. Declare `var _ <Interface> = <decorator>{}` for each
// forwarded extension so a dropped method fails the build instead.
//
// PrincipalAdmitter is the one a wrapper must not answer on its own behalf:
// admitting where the inner validator implements no admitter converts a receive
// path that refused every mesh principal into one that accepts whatever a peer
// asserts. Delegate that answer to the inner validator, or refuse — both
// preserve what the inner validator alone would have admitted.
type AuthValidator interface {
	// ValidateExecutionAuth gates handler execution on the task-executor
	// path (fail closed: unknown auth state denies RequiresAuth handlers).
	ValidateExecutionAuth(ctx context.Context, h SecureHandler) error

	// WithPartitionID stamps the partition onto ctx for nested calls.
	WithPartitionID(ctx context.Context, partitionID string) context.Context
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
// The RPC server calls it only after the PrincipalAdmitter has admitted the
// typed wire principal. Implementations project that one immutable snapshot
// onto their private product context keys; they must not consult
// RPCRequest.Context or merge caller fragments.
//
// It is optional: without it the product's private identity keys stay empty,
// so a handler that reads them denies rather than taking authority from the
// mutable context map.
type AuthenticatedPrincipalStamper interface {
	WithAuthenticatedPrincipal(
		ctx context.Context,
		principal scope.AuthenticatedPrincipal,
	) context.Context
}

// PrincipalAdmitter is the consumer seam that judges a principal a mesh peer
// asserted before the RPC server installs it. Every field of the wire principal
// (subject, axes, grants, partition) is the sending node's assertion; the RPC
// server has already required that it arrived over a partition-bound transport,
// and passes that server-established transport partition here with the
// reconstructed principal. A nil error admits the principal exactly as
// asserted; any error refuses the request.
//
// loom names no axis and applies no rule of its own: which assertions a
// transport may carry is the consuming project's policy. It is required for
// mesh authority: a validator that does not implement it has every mesh
// principal refused, so an absent seam never becomes a forge hole. An admitter
// must not require the principal's partition to equal the transport partition:
// one transport carries principals of many partitions.
type PrincipalAdmitter interface {
	AdmitPrincipal(
		ctx context.Context,
		transportPartition string,
		principal scope.AuthenticatedPrincipal,
	) error
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

// VerifyBootstrapFunc is the bootstrap admission check non-anchor nodes
// invoke during VL1 bootstrap; anchor.Client is the implementation that asks
// an anchor-capable node. Semantics the implementation and caller preserve
// exactly:
//   - err != nil (verifier unreachable) → graceful ALLOW — the Noise
//     handshake is the real security boundary, availability wins;
//   - allowed=false → reject the join with 403 and reason;
//   - anchor-capable nodes skip the call entirely.
//
// nil = always allow (standalone/dev meshes).
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
