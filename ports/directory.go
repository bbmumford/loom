/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package ports

import "context"

// Member is the typed membership projection of a fleet.peer record.
type Member struct {
	NodeID         NodeID
	ServiceName    string
	Roles          []string // service roles ("auth", "identity", …) — NOT swarm.Role
	Tenant         string
	Region         string
	LastSeenUnixMs int64
	Tombstoned     bool
}

// ReachAddress is a typed dial candidate for a node.
type ReachAddress struct {
	Protocol string // "noise-udp" > "ws" > "grpc" > "http" priority order; the
	// "noise-udp"↔reach-"udp" mapping must stay consistent (B.5 address table)
	Address  string
	Scope    string // "public" / "private" / … (pex_address scope inference)
	Priority int
	NATClass string // RFC 5780 class; absent/malformed → Unknown (forward-compat)
}

// LatencySample is one predicted/observed RTT observation between two nodes.
// Multiple observations per node require composite record keys (Phase-0.5
// blocker 7) — the schema reason latency cuts over last.
type LatencySample struct {
	From, To         NodeID
	RTTMs            float64
	ObservedAtUnixMs int64
}

// HandlerAdvert is a typed RPC-handler advertisement (who serves a name).
type HandlerAdvert struct {
	Name   string // FQN, e.g. "hstles.auth.<op>"
	NodeID NodeID
	Roles  []string
}

// DirectorySnapshot is a deterministic point-in-time view: two nodes with the
// same accepted-record set produce the same Fingerprint. Used for shadow
// parity comparison (Phase-0.5 stage 3) and anchor snapshot input.
type DirectorySnapshot struct {
	Watermark   Watermark
	Fingerprint [32]byte
	Records     []Record
}

// LiveDirectory is the single typed read surface for live replicated state
// (Phase 0.5.1). Every Mesh consumer reads through it — the cutover
// inventory must eliminate direct DirectoryCache/Ledger reads outside
// adapters before shadow mode starts.
//
// Implementations: (1) transitional adapter over the LAD DirectoryCache;
// (2) Swarm-backed immutable projections fed by one accepted-record stream.
// Records surface here only AFTER TrustPolicy authorization (pre-store for
// Swarm; pre-projection during shadow migration).
//
// Capability-parity requirement (plan B.4): every LAD typed query/index,
// liveness override, ACL, changed-since/fingerprint/bucket view, and
// anchor-snapshot input must map explicitly to a method here or to
// DurableJournal before Ledger/Whisper removal. "Swarm has a record map"
// is not parity — RecordsByTopic is an escape hatch for KeyOps/Quorum
// consumers, not a substitute for the typed projections.
type LiveDirectory interface {
	// Members returns the live membership projection (tombstoned/TTL-expired
	// entries excluded unless still within retention for observers).
	Members(ctx context.Context) ([]Member, error)

	// Member returns one node's membership entry; ok=false when absent.
	Member(ctx context.Context, id NodeID) (m Member, ok bool, err error)

	// NodesByRole returns nodes currently advertising a service role.
	// Advertisement is not authorization — recipients of anything sensitive
	// must additionally pass TrustPolicy.
	NodesByRole(ctx context.Context, role string) ([]NodeID, error)

	// Reach returns priority-ordered dial candidates for a node.
	Reach(ctx context.Context, id NodeID) ([]ReachAddress, error)

	// Latency returns the freshest RTT observation between two nodes;
	// ok=false when no observation exists. Dropping this feed silently
	// reverts route/dispatch ordering to grade-only (plan B.5 Vivaldi note).
	Latency(ctx context.Context, from, to NodeID) (s LatencySample, ok bool, err error)

	// HandlersByName returns the nodes advertising an RPC handler FQN.
	// rpc_forward target discovery reads this (via RoleTable today) — the
	// cutover must not re-point it at retired LAD Roles records.
	HandlersByName(ctx context.Context, name string) ([]HandlerAdvert, error)

	// RecordsByTopic returns the accepted records of one topic (KeyOps,
	// Quorum, and other consumers without a typed projection yet).
	RecordsByTopic(ctx context.Context, topic Topic) ([]Record, error)

	// OverrideLiveness applies the live-session liveness override (the
	// connection reporter marks a directly-connected peer alive regardless
	// of gossip staleness). ttlMs bounds the override; 0 clears it.
	OverrideLiveness(id NodeID, alive bool, ttlMs int64)

	// Snapshot returns a deterministic snapshot for parity comparison and
	// signed-anchor input generation.
	Snapshot(ctx context.Context) (DirectorySnapshot, error)

	// Fingerprint returns the deterministic digest of the current accepted
	// state without materializing records.
	Fingerprint(ctx context.Context) ([32]byte, error)

	// Subscribe streams accepted records for the given topics from a
	// watermark. History→live handoff is lossless: subscribe-before-history
	// with dedup (never query-then-subscribe — the gap loses records).
	// Slow consumers must be resumable from their last watermark, never
	// silently dropped. Cancel ctx to unsubscribe.
	Subscribe(ctx context.Context, topics []Topic, from Watermark) (<-chan Record, error)
}
