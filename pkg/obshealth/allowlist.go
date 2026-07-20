/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

// Package health hosts the platform-wide subsystem health Registry.
//
// The Registry tracks which named *subsystems* (components) are currently
// degraded. Per-event errors and per-request failures DO NOT belong here —
// they belong in slog / metrics. Subsystem health is the durable signal
// that downstream consumers (meshmon, status page, notifier, SLA
// reconciliation) read to decide whether the platform itself is healthy.
//
// Identifier namespace is finite and explicit. New identifiers are added
// here, in source, reviewed for blast-radius. Mark / Clear refuses any
// identifier not in this allow-list (fail-closed) so callers cannot
// invent unbounded compound keys like "webhook.stripe.evt_xxx" at
// runtime and grow the Registry map.
package health

// Allowed subsystem identifiers. Add new entries here only after
// reviewing what the subsystem actually means and how recovery is
// detected — the Registry models healthy/degraded transitions, not
// transient operational noise.
const (
	// Mesh / node.Runtime composite subsystems.

	// SubsystemMeshRaft — the Raft consensus layer inside the local
	// node. Degraded means we observed no leader / lost the leader /
	// failed to refresh peer set within the health-check interval.
	SubsystemMeshRaft SubsystemID = "mesh.raft"

	// SubsystemMeshLedger — the LAD ledger backing the directory.
	// Degraded means Head() / Reach() RPCs are failing.
	SubsystemMeshLedger SubsystemID = "mesh.ledger"

	// SubsystemMeshGossip — the gossip / swarm overlay. Degraded when
	// peer count drops below threshold or convergence stalls.
	SubsystemMeshGossip SubsystemID = "mesh.gossip"

	// SubsystemMeshTransport — the aether transport layer. Degraded
	// when the connection scaler or upgrade walker reports persistent
	// failure to maintain target K.
	SubsystemMeshTransport SubsystemID = "mesh.transport"

	// SubsystemHealthEvaluator — the periodic health evaluator
	// itself. Degraded when its evaluation lag exceeds the lagging
	// threshold (mirrors SelfHealthMonitor's "lagging"/"stalled").
	SubsystemHealthEvaluator SubsystemID = "observability.healthEvaluator"

	// SubsystemConnectionReporter — the connection reporter feeding
	// Layer-4 transport grade into the evaluator. Degraded when the
	// scan goroutine has not advanced.
	SubsystemConnectionReporter SubsystemID = "observability.connectionReporter"

	// Monitoring domain subsystems.

	// SubsystemMonitoringChecker — the uptime checker that probes
	// configured endpoints.
	SubsystemMonitoringChecker SubsystemID = "monitoring.checker"

	// SubsystemMonitoringAggregator — the rollup goroutine that
	// computes service-level status from per-endpoint check results.
	SubsystemMonitoringAggregator SubsystemID = "monitoring.aggregator"

	// SubsystemMonitoringNotifier — the StatusNotifier outbound mail
	// pipeline. Degraded when the notify RPC is failing or the
	// event channel is saturated.
	SubsystemMonitoringNotifier SubsystemID = "monitoring.notifier"

	// SubsystemMonitoringSLA — the SLA reconciliation worker.
	SubsystemMonitoringSLA SubsystemID = "monitoring.sla"

	// Billing subsystems.

	// SubsystemBillingWebhooks — inbound billing webhooks (Stripe,
	// etc.) — degraded when signature verification or dispatch is
	// rejecting the live stream.
	SubsystemBillingWebhooks SubsystemID = "billing.webhooks"

	// SubsystemBillingReconciler — periodic billing reconciliation.
	SubsystemBillingReconciler SubsystemID = "billing.reconciler"

	// Command / WS subsystems.

	// SubsystemCommandWS — the command WebSocket fanout used by the
	// admin / agent control plane. Degraded means the listener is
	// down or rejecting upgrades.
	SubsystemCommandWS SubsystemID = "command.ws"

	// SubsystemUptimeRunner — the long-running uptime runner that
	// performs scheduled remote probes.
	SubsystemUptimeRunner SubsystemID = "uptime.runner"

	// ORBTR agent PolicyEngine post-commit appliers (rev-054). Each
	// identifier corresponds to a single applyOrLog site inside
	// PolicyEngine.PullAndApply: a bundle has already been committed by
	// the time these run, so failure is a soft-degradation reflected here
	// (fail-observably) rather than a rollback. The agent maintains its
	// own DegradedSubsystems() map for local heartbeat surfacing; when
	// wired via PolicyEngine.SetHealthRegistry, every Mark / Clear is
	// mirrored to this Registry so the platform /api/monitoring/subsystems
	// surface sees the same events. Identifier stems match the internal
	// applyOrLog name so mesh-side and agent-side surfacing stay 1:1.

	// SubsystemPolicyLocalStore — SavePolicyBundle to agent_local.db.
	SubsystemPolicyLocalStore SubsystemID = "policy.localStore"

	// SubsystemPolicyOSFirewall — OS firewall Apply / Clear (opt-in host
	// hardening; PacketFilter remains the primary enforcement mechanism).
	SubsystemPolicyOSFirewall SubsystemID = "policy.osFirewall"

	// SubsystemPolicyPacketFilter — in-process packet filter ruleset load.
	SubsystemPolicyPacketFilter SubsystemID = "policy.packetFilter"

	// SubsystemPolicyAddressGroups — address-group resolver load.
	SubsystemPolicyAddressGroups SubsystemID = "policy.addressGroups"

	// SubsystemPolicyQuarantine — quarantine manager activation.
	SubsystemPolicyQuarantine SubsystemID = "policy.quarantine"

	// SubsystemPolicyEdgeSegment — edge segment configuration apply /
	// remove (VRF / VLAN).
	SubsystemPolicyEdgeSegment SubsystemID = "policy.edgeSegment"

	// SubsystemPolicyEdgeRole — edge role (print, DHCP) apply.
	SubsystemPolicyEdgeRole SubsystemID = "policy.edgeRole"

	// SubsystemPolicyAck — policy ack transport to the control plane.
	SubsystemPolicyAck SubsystemID = "policy.ack"
)

// AllowedSubsystems returns a snapshot of the allow-listed subsystem
// identifiers. Returned slice is safe to mutate by the caller; the
// internal allow-list is rebuilt on every call.
func AllowedSubsystems() []SubsystemID {
	return []SubsystemID{
		SubsystemMeshRaft,
		SubsystemMeshLedger,
		SubsystemMeshGossip,
		SubsystemMeshTransport,
		SubsystemHealthEvaluator,
		SubsystemConnectionReporter,
		SubsystemMonitoringChecker,
		SubsystemMonitoringAggregator,
		SubsystemMonitoringNotifier,
		SubsystemMonitoringSLA,
		SubsystemBillingWebhooks,
		SubsystemBillingReconciler,
		SubsystemCommandWS,
		SubsystemUptimeRunner,
		SubsystemPolicyLocalStore,
		SubsystemPolicyOSFirewall,
		SubsystemPolicyPacketFilter,
		SubsystemPolicyAddressGroups,
		SubsystemPolicyQuarantine,
		SubsystemPolicyEdgeSegment,
		SubsystemPolicyEdgeRole,
		SubsystemPolicyAck,
	}
}
