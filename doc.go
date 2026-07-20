/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

// Package loom is a standalone Go mesh runtime with CRDT-encrypted secret
// gossip and a universal composition surface: a self-forming service
// overlay that handles peer discovery, NAT traversal, multipath transport
// selection, RPC routing, and a convergent live directory, plus encrypted
// role hand-off and a register/call/trigger/observe composition layer.
//
// node.Initialize(cfg) (*Runtime, error) is the sole entry point; see the
// README for a quick start.
//
// Module layout:
//
//   - node/           — the runtime: Runtime, Config, RPC server, sessions,
//     directory, health, role activation, and the composition bridge.
//   - core/           — directory/gossip codecs, well-known streams, the
//     distributed task framework, and the per-tenant transport manager.
//   - grade/          — connection quality grades and operation classes.
//   - contracts/pb/   — mesh transport protobufs.
//   - pkg/rpc         — the reflection RPC registry (Call, Registry, HTTP
//     bridge, tenant scopes).
//   - pkg/dispatch    — the local-dispatch short-circuit and mesh-caller
//     wiring.
//   - pkg/trace       — e2e trace-ID context propagation.
//   - pkg/obshealth   — the subsystem-health registry.
//   - ports/          — the interface seams: LiveDirectory, DurableJournal,
//     TrustPolicy, RoleActivator, PlatformInfo, AuthValidator.
//   - directory/      — a fail-closed TrustPolicy, a Swarm-backed
//     LiveDirectory projection, and a shadow-parity comparator.
//   - journal/        — a crash-consistent file-backed DurableJournal.
//   - secrets/        — the role.secrets.<role> encrypted-gossip envelope,
//     sealer/opener, and coverage evaluator.
//   - compose/        — function, trigger, and scope-tracker surfaces.
//   - internal/       — a DEBUG=<ns> logger and a fail-closed AuthValidator
//     default (module-internal).
package loom
