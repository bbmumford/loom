/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

// Package ports defines the explicit seams between loom's node runtime and
// the directory, journal, trust and host implementations it is composed with.
// Nothing in this package has behaviour — these are contracts:
//
//   - LiveDirectory  — the single typed live replicated-state read surface,
//     implemented over the LAD DirectoryCache and over Swarm-backed
//     immutable projections.
//   - DurableJournal — durable history and recovery behind a narrow append
//     port. It is never a second live merge authority.
//   - TrustPolicy    — fail-closed NodeID↔key / partition / topic / role /
//     observer / secret-recipient authorization. Mandatory pre-store gate;
//     self-advertised RoleTable state is NOT a trust root.
//   - RoleActivator  — the runtime role-init path that brings a role's
//     services up after boot.
//   - The Config injection seams — MeshFallbackStatsFunc, AuthValidator,
//     BootstrapInfo/VerifyBootstrapFunc, PlatformInfo — through which a
//     composition root supplies host- and deployment-specific behaviour.
//     (The domain-registration seam is node.Config.RegisterDomains, typed
//     directly with the in-module *rpc.Registry.)
//
// Type provenance: NodeID, Topic, Watermark and Record are loom-local. Where
// the public surface needs type identity with an upstream module
// (aether.NodeID, grade.Grade, lad types), loom aliases or depends on the
// identical upstream version; adapters map these port types at the boundary.
//
// Invariants every implementation keeps:
//
//   - Exactly one live merge/convergence authority at a time; no permanent
//     Swarm↔LAD feedback loop.
//   - Authorization happens before a record can affect roles, reachability,
//     routing, handlers, anchor snapshots, or secret recipients.
//   - Owner-signed provenance survives gossip, persistence, snapshot,
//     recovery, and projection verbatim; observer facts remain explicitly
//     third-party attestations.
//   - Tombstones are retained until causal stability / acknowledged journal
//     checkpoint, not deleted by wall-clock compaction.
//   - Slow consumers resume from a watermark; no silent channel drops and
//     no history→live subscription gap.
//   - A restart from journal+checkpoint reproduces the same live projection
//     and Merkle root as an uninterrupted node.
package ports
