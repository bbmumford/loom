/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

// Package ports defines the explicit seams the loom extraction and the
// Phase-0.5 directory cutover are built on. Nothing in this package has
// behaviour — these are contracts:
//
//   - LiveDirectory  — the single typed live replicated-state read surface
//     (Phase 0.5.1). Initially adapted over the LAD DirectoryCache; cutover
//     target is Swarm-backed immutable projections.
//   - DurableJournal — durable history and recovery behind a narrow append
//     port (Phase 0.5.1). Initially a hardened Ledger/MeshLedger adapter.
//     It is never a second live merge authority.
//   - TrustPolicy    — fail-closed NodeID↔key / tenant / topic / role /
//     observer / secret-recipient authorization (Phase 0.5.1). Mandatory
//     pre-store gate; self-advertised RoleTable state is NOT a trust root.
//   - RoleActivator  — the Phase-1 runtime role-init path (the single
//     largest net-new build; roles are fixed at boot today).
//   - The §0.3 Config injection seams — MeshFallbackStatsFunc,
//     AuthValidator, BootstrapInfo/VerifyBootstrapFunc, PlatformInfo —
//     that replace the HSTLES library couplings. (The domain-registration
//     seam is node.Config.RegisterDomains, typed directly with the
//     in-module *rpc.Registry.)
//
// Type provenance: types here are loom-local and provisional where the plan
// allows it (NodeID, Topic, Watermark, Record). Where the plan REQUIRES
// type identity with an upstream module for the preserved public surface
// (aether.NodeID, grade.Grade, lad types — Appendix A hazard), the Phase-0
// extraction must alias or depend on the identical upstream version;
// adapters map these port types at the boundary.
//
// Non-negotiable migration invariants (plan §0.5.4):
//
//   - Exactly one live merge/convergence authority at each cutover stage;
//     no permanent Swarm↔LAD feedback loop.
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
