/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

// Package compose defines the universal composition surfaces: every mesh
// capability REGISTERED, CALLABLE, TRIGGERABLE, and OBSERVABLE — three
// primitives (Worker · Function · Trigger) mapped onto what the mesh already
// has.
//
//   - REGISTERED — a mesh RPC handler IS a Function (stable FQN id like
//     "orbtr.<domain>.<Op>"). The reflection rpc.Registry is the
//     FunctionsRegistry; the two-registry bridge (rpc.Registry ↔
//     handlers.HandlerRegistry via the adapter registryShim) must be
//     respected, not collapsed.
//   - CALLABLE — cross-mesh call runs through rpc.Call/forwarding/bidi;
//     compose gives it one id space (FunctionID) and the 4-way
//     FunctionResult semantics. Preserve the forwarding fragilities:
//     RoleTable.Lookup(role, handler) discovery and merged-ctx probe
//     cancellation. RoleTable is built from every peer's own fleet.peer
//     PeerRecord, which carries that peer's handler FQNs. LAD Roles records
//     are not a substitute: the ones the fleet.peer bridge writes carry role
//     lists and no handlers, and Runtime.PublishHandlersToLAD adds handler
//     metadata only for a composition root that calls it.
//   - TRIGGERABLE — TriggerKind {http, cron, state, subscribe, stream}
//     with late binding: instances registered before their kind are stored
//     pending and replayed when the kind registers. A swarm topic
//     subscription is a subscribe trigger; RoleMissing is a state trigger
//     firing role activation; the chi routers attaching the Runtime's HTTP
//     handlers are http triggers.
//   - OBSERVABLE — wraps the 4-layer HealthEvaluator, Prometheus
//     /metrics export, /mesh/status DTO, connection-event history, and
//     latency histograms as an observability worker. The metric names/label
//     sets and the /mesh/status JSON shape + CORS allow-list are an
//     EXTERNAL WIRE CONTRACT (monitoring clients' dashboards read them): a
//     rename lands together with every reader of the old name, and no
//     second name is emitted beside the new one.
//
// The scope tracker (scope.go) captures exactly the function IDs a role
// registers during activation, so takeover teardown can remove precisely
// that role's handlers.
//
// Machinery patterns: adopt the shape — pluggable ports, name→func registry,
// Signature payload, chains/groups/chords — atop mesh-native transport
// (core/task); no central broker, central result backend, or central
// distributed lock.
package compose
