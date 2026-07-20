/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

// Package compose defines the Phase-2 iii-derived universal composition
// surfaces: every mesh capability REGISTERED, CALLABLE, TRIGGERABLE, and
// OBSERVABLE — three primitives (Worker · Function · Trigger) mapped onto
// what the mesh already has.
//
//   - REGISTERED — a mesh RPC handler IS a Function (stable FQN id like
//     "hstles.auth.<op>"). The existing reflection rpc.Registry is the
//     FunctionsRegistry; the two-registry bridge (rpc.Registry ↔
//     handlers.HandlerRegistry via the adapter registryShim) must be
//     respected, not collapsed.
//   - CALLABLE — cross-mesh call is already built (rpc.Call/forwarding/
//     bidi). Phase 2 unifies the id space and adds the 4-way
//     FunctionResult semantics. Preserve the forwarding fragilities:
//     RoleTable.Lookup discovery (never re-point at retired LAD Roles) and
//     merged-ctx probe cancellation.
//   - TRIGGERABLE — TriggerKind {http, cron, state, subscribe, stream}
//     with late binding: instances registered before their kind are stored
//     pending and replayed when the kind registers. A swarm topic
//     subscription is a subscribe trigger; RoleMissing is a state trigger
//     firing role activation; the chi routers attaching the Runtime's HTTP
//     handlers are http triggers.
//   - OBSERVABLE — wraps the existing 4-layer HealthEvaluator, Prometheus
//     /metrics export, /mesh/status DTO, connection-event history, and
//     latency histograms as an observability worker. The metric names/label
//     sets and the /mesh/status JSON shape + CORS allow-list are an
//     EXTERNAL WIRE CONTRACT (help.orbtr.io dashboards) — never rename.
//
// The scope tracker (scope.go) is the piece Phase 1 depends on: it captures
// exactly the function IDs a role registers during activation so takeover
// teardown removes precisely that role's handlers — the mesh has no per-role
// unregister today.
//
// Machinery patterns (plan §2.5): adopt the shape — pluggable ports,
// name→func registry, Signature payload, chains/groups/chords — atop
// mesh-native transport (core/task); reject the central broker, central
// result backend, and central distributed lock.
package compose
