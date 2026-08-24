/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"fmt"
	"time"

	"github.com/bbmumford/loom/pkg/rpc"
	"github.com/bbmumford/loom/ports"
	meshdbcfg "github.com/bbmumford/tursoraft/manager"
)

// AnchorConfig holds configuration for secure bootstrapping via signed snapshots
type AnchorConfig struct {
	PublicKey  string
	PrivateKey string
}

// TenantTransportConfig configures a single platform tenant's aether.
// Each tenant gets its own PSK and either a dedicated UDP port or a slot
// on the shared transport (preamble-based routing).
type TenantTransportConfig struct {
	TenantID    string   // Platform tenant identifier (required)
	NetworkKeys []string // PSKs — first is active, rest accepted for receiving (required)
	Dedicated   bool     // If true, gets its own UDP port; if false, uses shared transport
	UDPPort     int      // Only used when Dedicated=true
}

// Config holds all configuration for a mesh node runtime.
// All values must be provided by the caller (typically from main.go after loading
// platform configs). The node does NOT load configs itself - it's a pure runtime component.
//
// Configuration is logically separated into:
//   - Networking: VL1 transport, LAD peer discovery, relay
//   - Databases: Tursoraft replication (configured separately via DatabasesConfig in main.go)
type Config struct {
	// Platform provides runtime environment metadata (region, machine ID,
	// UDP bind semantics, private/public IPs). If nil, falls back to
	// ports.DevPlatform() — a generic dev implementation. ⚠ Unlike every
	// other injected seam, nil is NOT "current behaviour": cloud
	// deployments (Fly especially) must inject their platform impl or they
	// silently lose fly-global-services UDP bind, 6PN origin detection,
	// and region codes.
	Platform ports.PlatformInfo

	// MeshFallbackStats supplies session-validate fallback-cache counters
	// for the metrics map (replaces the platform/session import). nil →
	// both counters emitted as 0 (schema unchanged).
	MeshFallbackStats ports.MeshFallbackStatsFunc

	// AuthValidator gates task-executor handler execution and stamps
	// tenant IDs onto nested-call contexts (replaces the
	// platform/security/helpers import). nil → loom-local fail-closed
	// validator. HSTLES builds MUST inject a validator that delegates to
	// the real helpers package so its unexported context keys — the ones
	// domain handlers read — are the ones written.
	AuthValidator ports.AuthValidator

	// SwarmTrustMode selects the inbound trust gate on the swarm
	// live-directory convergence path. "" / "off" = no gate (current
	// behaviour). "observe" runs the FULL pre-store authorization —
	// TrustPolicy.AuthorizePublish: NodeID↔key bind (aether scheme), tenant
	// scope, and the topic publish rule, seeded from the self-owned baseline —
	// and COUNTS records that WOULD be rejected without rejecting any; that is
	// the shadow-parity step, and it must show zero would-rejects for legit
	// traffic fleet-wide (read TrustGateStats + the trust-shadow ShadowStats /
	// ShadowParity) before cutover. "enforce" rejects a record that fails the
	// same authorization. Default off.
	//
	// ⚠ Do NOT reach for the swarm RequireNodeKeyBinding bool instead: it
	// derives NodeID as hex(pubkey), incompatible with aether's vl1_
	// fingerprint, so on this fleet it would reject EVERY record. TrustCheck
	// with the aether scheme (wired from this field) is the correct gate.
	SwarmTrustMode string

	// ShadowParityInterval, when > 0, paces a periodic comparison of the live
	// LADDirectory (rt.liveDir) against the Swarm-backed trust-shadow
	// projection and records any divergence into the shadow_parity_* mesh
	// metrics plus a structured log. It is inert unless a trust shadow is also
	// running (SwarmTrustMode="observe" builds it); with no shadow the pass
	// finds nothing to compare and records nothing. 0 (the default) starts no
	// comparison goroutine at all, so a default node adds zero readers of the
	// live directory. Size it in the tens of seconds — each pass walks members,
	// per-member reach and handler adverts on both sides, so a short interval
	// multiplies those reads.
	ShadowParityInterval time.Duration

	// AdaptiveGossipCadence, when true, paces the gossip loop's adaptive
	// interval from a live network profile (link type + measured peer RTT via
	// GetPeerLatencies) instead of the built-in (GossipInterval, 2s, 60s)
	// envelope: startGossipCadence installs a gossip.SetGossipCadence source
	// backed by netpolicy.GossipBounds and refreshes it on the telemetry
	// cadence. Default false = the gossip loop keeps its fixed envelope, so
	// enabling this is an explicit opt-in to network-adaptive gossip timing
	// (a fleet-wide change to how often nodes exchange).
	AdaptiveGossipCadence bool

	// RoleTakeover, when non-nil, arms the leaderless role-takeover engine after InitSwarm:
	// this node guards the listed roles and covers a shortfall once a role stays under-covered
	// past the corroboration window. Gated per role by the config's Policy
	// (AuthorizeSecretRecipient) — an unseeded policy entitles nothing, so it fails closed. nil
	// = off (current behaviour). No role is exclusive: the engine ranks claims but never fences
	// a winner, so several nodes may hold a role. See RoleTakeoverConfig.
	RoleTakeover *RoleTakeoverConfig

	// ComposeTaskCompletionObserver receives terminal outcomes for task
	// invocations that ComposeInvoke accepted as Deferred. The runtime stores
	// each outcome in its bounded completion history before invoking this
	// callback, so an observer panic cannot make the Deferred handle
	// unqueryable. The callback runs on the tracked task goroutine and should
	// respect its context.
	ComposeTaskCompletionObserver ComposeTaskCompletionObserver

	// ComposeTriggerPrincipalProvider re-resolves every trigger fire against
	// the product registration/activation authority and establishes the
	// current machine/service principal. nil is fail-closed: triggers may arm
	// but cannot invoke a function.
	ComposeTriggerPrincipalProvider ComposeTriggerPrincipalProvider

	// ComposeTaskMaxInFlight bounds task invocations accepted by
	// ComposeInvoke but not yet terminal. Values <= 0 use the safe runtime
	// default; there is no configuration that restores unbounded admission.
	// Admission also requires a nonempty opaque owner from the injected
	// AuthValidator's ports.ExecutionPrincipalReader extension.
	ComposeTaskMaxInFlight int

	// ComposeTaskMaxInFlightPerOwner bounds live task invocations for one
	// opaque execution owner. Values <= 0 use the safe default. The global and
	// owner reservations are acquired atomically, so neither ceiling can be
	// oversubscribed by concurrent admissions.
	ComposeTaskMaxInFlightPerOwner int

	// ComposeTaskCompletionRetentionPerOwner bounds terminal records retained
	// for one opaque owner inside the global 1024-record history. Values <= 0
	// use the safe default.
	ComposeTaskCompletionRetentionPerOwner int

	// RegisterDomains registers external RPC domains (e.g. the HSTLES
	// anchor domain) on anchor-capable nodes. Each entry runs against a
	// fresh rpc.Registry which is then exported to the handler registry
	// (replaces the domain/anchor import).
	RegisterDomains []func(*rpc.Registry) error

	// VerifyBootstrap authorizes a joining node during the VL1 bootstrap
	// handshake on non-anchor-capable nodes (replaces the hard-coded
	// hstles.anchor.VerifyBootstrap rpc.Call). nil → allow all. Error →
	// graceful allow (the Noise handshake is the real boundary);
	// allowed=false → 403 with reason.
	VerifyBootstrap ports.VerifyBootstrapFunc

	// Service identity (required)
	ServiceName string

	// Public domain for this endpoint (e.g., "devices.orbtr.io").
	// Used for DNS self-resolution to discover inbound public IPs.
	// Falls back to STUN if empty or DNS lookup fails.
	PublicDomain string

	// Private IP for internal mesh networking (e.g., "fdaa:4d:ce3c:a7b:...").
	// Advertised to peers via VL1 headers so they can connect over private networks.
	// If empty, private reach records are only created by the bootstrap server.
	PrivateIP string

	// Data directory for node state (required)
	DataDir string

	// ═══════════════════════════════════════════════════════════════════════════
	// NETWORKING: VL1 Transport + LAD Peer Discovery
	// ═══════════════════════════════════════════════════════════════════════════

	// Network Keys (Shared PSKs for mesh authorization)
	// First key is used for sending, all are accepted for receiving.
	// Used as a single-tenant fallback when Tenants is empty.
	NetworkKeys []string

	// Tenants configures per-tenant transports for multi-tenant mesh segmentation.
	// Each tenant entry creates either a dedicated NoiseTransport (own UDP port)
	// or registers with the shared transport (preamble protocol on VL1.UDPPort).
	// If empty, NetworkKeys is used as a single default transport (backward compat).
	Tenants []TenantTransportConfig

	// Anchor bootstrapping (Secure Snapshot)
	Anchor AnchorConfig

	// Roles lists the roles this node has been assigned (e.g. from ROLES env var).
	// If "anchor" is in this list, the node gets priority in anchor leader election.
	Roles []string

	// GossipInterval is the interval between gossip exchanges on all transports.
	// Protocol-agnostic — the same interval applies regardless of transport type.
	// Default: 10s. The gossip keepalive ping fires at the same interval between
	// exchanges, so the maximum idle time is approximately GossipInterval.
	GossipInterval time.Duration `json:"gossipInterval,omitempty"`

	// VL1 transport configuration (UDP overlay with hole punching)
	VL1 VL1TransportConfig

	// LAD directory configuration (peer discovery via gossip)
	LAD LADDirectoryConfig

	// ═══════════════════════════════════════════════════════════════════════════
	// DATABASES: Tursoraft Replication (Optional - can be initialized separately)
	// ═══════════════════════════════════════════════════════════════════════════

	// Databases enables Tursoraft database replication.
	// NOTE: This is optional. Applications can initialize Tursoraft directly
	// in main.go for more control over role-specific database groups.
	// If nil/empty, no databases will be initialized by the node runtime.
	Databases *DatabasesConfig

	// ═══════════════════════════════════════════════════════════════════════════
	// OBSERVABILITY: Health, Circuit Breaker, Audit
	// ═══════════════════════════════════════════════════════════════════════════

	// Health monitoring
	Health HealthConfig

	// Connection budget overrides (optional — defaults used if nil)
	ConnectionBudget *ConnectionBudgetConfig

	// Circuit breaker
	CircuitBreaker CircuitBreakerConfig

	// Audit logging
	Audit AuditConfig

	// Custom RPC handlers (use handlers.RPCHandler with RegisterRPC)
}

// HasRole returns true if the given role is in the Roles list.
func (c Config) HasRole(role string) bool {
	for _, r := range c.Roles {
		if r == role {
			return true
		}
	}
	return false
}

// VL1TransportConfig configures the VL1 UDP overlay aether.
type VL1TransportConfig struct {
	Enabled           bool
	UDPPort           int           // Primary UDP port for mesh traffic (default: 41641)
	FailoverPort      int           // Failover UDP port (default: 9993, ZeroTier-compatible)
	MaxPeers          int           // Maximum concurrent peer connections
	DialTimeout       time.Duration // Timeout for establishing sessions
	KeepAliveInterval time.Duration // Keep-alive ping interval

	// HolePunchDisabled turns off coordinated NAT hole-punching. When a
	// direct noise-UDP dial to a peer fails, the dialer normally signals
	// the peer over its existing mesh session and attempts a synchronized
	// simultaneous-open punch before keeping the WebSocket fallback. This
	// is the kill switch — zero value (false) keeps hole-punching ON.
	HolePunchDisabled bool

	// Relay configuration
	Relay RelayConfig
}

// RelayConfig configures relay functionality for VL1.
type RelayConfig struct {
	Enabled   bool
	MaxRate   int           // Max bytes/sec per peer
	TicketTTL time.Duration // Relay session TTL
}

// LADDirectoryConfig configures LAD (Ledger-as-Directory) peer discovery.
type LADDirectoryConfig struct {
	// Storage determines where peer discovery records are stored:
	//   - "local": Persistent embedded libSQL database (survives restarts)
	//   - "memory": In-memory only (lost on restart, for testing/ephemeral)
	//   - "mesh": Replicated via Tursoraft (requires RaftBindAddr)
	//   - "": LAD disabled
	Storage        string
	SyncInterval   time.Duration // How often to sync LAD state
	LocalDir       string        // Directory for local LAD storage (required for "local" or "mesh" storage)
	BootstrapHosts string        // Comma-separated bootstrap hosts for initial discovery and mesh peer list

	// RetentionDays is how many days to keep LAD records (default: 30, optional)
	RetentionDays int

	// --- Mesh storage options (only used when Storage = "mesh") ---

	// RaftBindAddr for LAD mesh replication. Required when Storage = "mesh".
	// Should be different from app database Raft port if both are used.
	RaftBindAddr string

	// CompactionInterval defines how often to run compaction (default: 24h, optional)
	CompactionInterval time.Duration

	// EnableBloomFilters enables Bloom filters for query optimization (default: true, optional)
	EnableBloomFilters *bool
}

// DatabasesConfig configures Tursoraft database replication.
// This is for APPLICATION DATABASES replicated via Raft consensus.
type DatabasesConfig struct {
	Enabled bool

	// Tursoraft consensus settings
	RaftBindAddr string // Address for Raft consensus communication
	LocalDir     string // Directory for Raft state/WAL
	SnapshotDir  string // Directory for Raft snapshots
	SyncInterval time.Duration

	// Turso cloud settings
	TursoAPIToken string
	TursoOrgName  string

	// Database groups to replicate
	Groups []meshdbcfg.GroupConfig
}

// HealthConfig configures health monitoring.
type HealthConfig struct {
	Enabled       bool
	CheckInterval time.Duration
}

// CircuitBreakerConfig configures the circuit breaker.
type CircuitBreakerConfig struct {
	Enabled   bool
	Threshold int
	ResetTime time.Duration
}

// AuditConfig configures audit logging.
type AuditConfig struct {
	Enabled bool
	LogPath string
}

// ConnectionBudgetConfig allows overriding default connection budget values.
type ConnectionBudgetConfig struct {
	MaxPerPeer       int `json:"maxPerPeer"`       // default: 3
	MaxTotal         int `json:"maxTotal"`         // default: 50
	MinPerPeer       int `json:"minPerPeer"`       // default: 1
	PreferredPerPeer int `json:"preferredPerPeer"` // default: 1
	CrossRegionBonus int `json:"crossRegionBonus"` // default: 1
}

// AuthKeys returns all network keys available for node-level auth (snapshot HMAC, etc.).
// Returns global NetworkKeys first, then unique keys from tenant configs.
func (c *Config) AuthKeys() []string {
	seen := make(map[string]bool, len(c.NetworkKeys))
	keys := make([]string, 0, len(c.NetworkKeys))
	for _, k := range c.NetworkKeys {
		if !seen[k] {
			keys = append(keys, k)
			seen[k] = true
		}
	}
	for _, tc := range c.Tenants {
		for _, k := range tc.NetworkKeys {
			if !seen[k] {
				keys = append(keys, k)
				seen[k] = true
			}
		}
	}
	return keys
}

// Validate ensures the configuration is valid
func (c *Config) Validate() error {
	if c.ServiceName == "" {
		return fmt.Errorf("service name is required")
	}
	if c.DataDir == "" {
		return fmt.Errorf("data directory is required")
	}
	// Network key validation: either NetworkKeys or Tenants must be provided.
	// When Tenants is non-empty, each tenant carries its own keys.
	// When Tenants is empty, NetworkKeys is used as single-tenant fallback.
	if len(c.NetworkKeys) == 0 && len(c.Tenants) == 0 {
		return fmt.Errorf("at least one network key or tenant config is required")
	}

	// Validate tenant configs
	seenTenants := make(map[string]bool, len(c.Tenants))
	seenPorts := make(map[int]string) // port → tenantID (for collision detection)
	for i, tc := range c.Tenants {
		if tc.TenantID == "" {
			return fmt.Errorf("tenant[%d]: tenant ID is required", i)
		}
		if seenTenants[tc.TenantID] {
			return fmt.Errorf("tenant[%d]: duplicate tenant ID %q", i, tc.TenantID)
		}
		seenTenants[tc.TenantID] = true

		if len(tc.NetworkKeys) == 0 {
			return fmt.Errorf("tenant[%d] (%s): at least one network key is required", i, tc.TenantID)
		}
		if tc.Dedicated {
			if tc.UDPPort <= 0 {
				return fmt.Errorf("tenant[%d] (%s): UDP port is required for dedicated transport", i, tc.TenantID)
			}
			if other, exists := seenPorts[tc.UDPPort]; exists {
				return fmt.Errorf("tenant[%d] (%s): UDP port %d conflicts with tenant %s", i, tc.TenantID, tc.UDPPort, other)
			}
			seenPorts[tc.UDPPort] = tc.TenantID
		}
	}

	if c.Audit.Enabled && c.Audit.LogPath == "" {
		return fmt.Errorf("audit log path is required when audit is enabled")
	}

	// LAD validation
	if c.LAD.Storage != "" {
		validStorage := c.LAD.Storage == "local" || c.LAD.Storage == "memory" || c.LAD.Storage == "mesh"
		if !validStorage {
			return fmt.Errorf("LAD storage must be 'local', 'memory', 'mesh', or empty (disabled)")
		}
		if (c.LAD.Storage == "local" || c.LAD.Storage == "mesh") && c.LAD.LocalDir == "" {
			return fmt.Errorf("LAD local directory is required when LAD storage is 'local' or 'mesh'")
		}
		if c.LAD.Storage == "mesh" && c.LAD.RaftBindAddr == "" {
			return fmt.Errorf("LAD raft bind address is required when LAD storage is 'mesh'")
		}
		if c.LAD.BootstrapHosts == "" {
			return fmt.Errorf("LAD bootstrap hosts required when LAD is enabled")
		}
	}

	// Databases validation (optional - applications can initialize Tursoraft directly)
	if c.Databases != nil && c.Databases.Enabled {
		if c.Databases.RaftBindAddr == "" {
			return fmt.Errorf("databases raft bind address is required when databases are enabled")
		}
		if c.Databases.LocalDir == "" {
			return fmt.Errorf("databases local directory is required when databases are enabled")
		}
	}

	return nil
}
