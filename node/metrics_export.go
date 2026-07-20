/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/bbmumford/loom/pkg/obshealth"
)

// ---------------------------------------------------------------------------
// MetricsExporter — Prometheus-format /metrics endpoint.
// ---------------------------------------------------------------------------

// MetricsExporter collects mesh observability data from ConnectionReporter,
// HealthEvaluator, SelfHealthMonitor, and ConnectionScaler, then formats it
// as Prometheus text exposition format for scraping.
type MetricsExporter struct {
	mu             sync.RWMutex
	reporter       ConnectionReporter
	evaluator      HealthEvaluator
	selfHealth     *SelfHealthMonitor
	scaler         *ConnectionScaler
	budget         *ConnectionBudget
	rpcServer      *RPCServer
	healthRegistry *health.Registry // platform-wide degradedSubsystem sink (optional)
	lastScrape     time.Time
}

// MetricsExporterConfig holds the dependencies for the metrics exporter.
type MetricsExporterConfig struct {
	Reporter       ConnectionReporter
	Evaluator      HealthEvaluator
	SelfHealth     *SelfHealthMonitor
	Scaler         *ConnectionScaler
	Budget         *ConnectionBudget
	RPCServer      *RPCServer // optional — exposes per-handler RPC metrics
	HealthRegistry *health.Registry
}

// NewMetricsExporter creates a metrics exporter. All dependencies are optional;
// nil components produce no metrics for their section.
func NewMetricsExporter(cfg MetricsExporterConfig) *MetricsExporter {
	reporter := cfg.Reporter
	if reporter == nil {
		reporter = NilConnectionReporter{}
	}
	return &MetricsExporter{
		reporter:       reporter,
		evaluator:      cfg.Evaluator,
		selfHealth:     cfg.SelfHealth,
		scaler:         cfg.Scaler,
		budget:         cfg.Budget,
		rpcServer:      cfg.RPCServer,
		healthRegistry: cfg.HealthRegistry,
	}
}

// Handler returns an http.HandlerFunc suitable for mounting on a mux.
func (e *MetricsExporter) Handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		e.writeMetrics(w)
		e.mu.Lock()
		e.lastScrape = time.Now()
		e.mu.Unlock()
	}
}

// LastScrape returns the time of the most recent Prometheus scrape.
func (e *MetricsExporter) LastScrape() time.Time {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.lastScrape
}

// writeMetrics writes all mesh metrics in Prometheus exposition format.
func (e *MetricsExporter) writeMetrics(w io.Writer) {
	e.writeConnectionMetrics(w)
	e.writeRTTMetrics(w)
	e.writeServiceHealthMetrics(w)
	e.writeBudgetUtilization(w)
	e.writeGossipMetrics(w)
	e.writeSelfHealthMetrics(w)
	e.writeConnectionEventMetrics(w)
	e.writeRPCMetrics(w)
	e.writeDegradedSubsystemMetrics(w)
}

// writeDegradedSubsystemMetrics emits mesh_subsystem_degraded per
// currently-degraded subsystem in the platform-wide health Registry.
// Absence of a label means the subsystem is healthy — Prometheus users
// alert on `mesh_subsystem_degraded{subsystem=...} > 0`. The
// mesh_subsystem_degraded_total gauge exposes the aggregate count so
// meshmon and /heartbeat consumers can trigger on a single integer.
func (e *MetricsExporter) writeDegradedSubsystemMetrics(w io.Writer) {
	fmt.Fprintf(w, "# HELP mesh_subsystem_degraded_total Count of degraded subsystems in the platform-wide health registry.\n")
	fmt.Fprintf(w, "# TYPE mesh_subsystem_degraded_total gauge\n")
	if e.healthRegistry == nil {
		fmt.Fprintf(w, "mesh_subsystem_degraded_total 0\n")
		return
	}
	snap := e.healthRegistry.Snapshot()
	fmt.Fprintf(w, "mesh_subsystem_degraded_total %d\n", len(snap.Entries))
	if len(snap.Entries) == 0 {
		return
	}
	fmt.Fprintf(w, "# HELP mesh_subsystem_degraded Which subsystems are currently degraded (1=degraded).\n")
	fmt.Fprintf(w, "# TYPE mesh_subsystem_degraded gauge\n")
	for _, ent := range snap.Entries {
		fmt.Fprintf(w, "mesh_subsystem_degraded{subsystem=%q} 1\n", string(ent.Name))
	}
}

// writeConnectionMetrics emits mesh_connections_total grouped by transport and grade.
func (e *MetricsExporter) writeConnectionMetrics(w io.Writer) {
	conns := e.reporter.ActiveConnections()
	if len(conns) == 0 {
		// Emit zero-value so Prometheus knows the metric exists.
		fmt.Fprintf(w, "# HELP mesh_connections_total Number of active mesh connections by transport and grade.\n")
		fmt.Fprintf(w, "# TYPE mesh_connections_total gauge\n")
		fmt.Fprintf(w, "mesh_connections_total{transport=\"none\",grade=\"F\"} 0\n")
		return
	}

	type connKey struct{ transport, grade string }
	counts := make(map[connKey]int)
	for _, ci := range conns {
		key := connKey{
			transport: ci.Transport,
			grade:     ci.Grade.String(),
		}
		counts[key] += ci.ConnCount
		// If ConnCount is zero (single-connection peer), count the peer itself.
		if ci.ConnCount == 0 {
			counts[key]++
		}
	}

	fmt.Fprintf(w, "# HELP mesh_connections_total Number of active mesh connections by transport and grade.\n")
	fmt.Fprintf(w, "# TYPE mesh_connections_total gauge\n")
	for key, count := range counts {
		fmt.Fprintf(w, "mesh_connections_total{transport=%q,grade=%q} %d\n",
			key.transport, key.grade, count)
	}
}

// writeRTTMetrics emits mesh_connection_rtt_milliseconds per peer.
func (e *MetricsExporter) writeRTTMetrics(w io.Writer) {
	conns := e.reporter.ActiveConnections()
	if len(conns) == 0 {
		return
	}

	fmt.Fprintf(w, "# HELP mesh_connection_rtt_milliseconds RTT to peer in milliseconds.\n")
	fmt.Fprintf(w, "# TYPE mesh_connection_rtt_milliseconds gauge\n")
	for _, ci := range conns {
		rttMs := ci.RTT.Milliseconds()
		fmt.Fprintf(w, "mesh_connection_rtt_milliseconds{peer=%q,transport=%q} %d\n",
			truncID(ci.PeerNodeID), ci.Transport, rttMs)
	}
}

// writeServiceHealthMetrics emits mesh_service_health from HealthEvaluator.
// Value encoding: 1 = healthy, 0.5 = degraded, 0.25 = connecting, 0 = unreachable.
func (e *MetricsExporter) writeServiceHealthMetrics(w io.Writer) {
	if e.evaluator == nil {
		return
	}

	reports := e.evaluator.AllServiceHealth()
	if len(reports) == 0 {
		return
	}

	fmt.Fprintf(w, "# HELP mesh_service_health Health status of monitored services (1=healthy, 0.5=degraded, 0=unreachable).\n")
	fmt.Fprintf(w, "# TYPE mesh_service_health gauge\n")
	for _, r := range reports {
		val := healthStatusToFloat(r.CombinedStatus)
		fmt.Fprintf(w, "mesh_service_health{service=%q,mesh_status=%q,http_status=%q,best_transport=%q} %.2f\n",
			r.ServiceName, r.MeshStatus, r.HTTPStatus, r.BestTransport, val)
	}
}

// writeBudgetUtilization emits mesh_connection_budget_utilization from ConnectionBudget.
func (e *MetricsExporter) writeBudgetUtilization(w io.Writer) {
	fmt.Fprintf(w, "# HELP mesh_connection_budget_utilization Fraction of connection budget used (0.0 to 1.0).\n")
	fmt.Fprintf(w, "# TYPE mesh_connection_budget_utilization gauge\n")

	if e.budget != nil {
		fmt.Fprintf(w, "mesh_connection_budget_utilization %.4f\n", e.budget.Utilization())
	} else {
		// Fall back to counting active connections vs a default budget.
		total := e.reporter.ConnectedPeerCount()
		fmt.Fprintf(w, "mesh_connection_budget_utilization{peers=%d} %.4f\n",
			total, 0.0)
	}
}

// writeGossipMetrics emits connection event and gossip activity counters.
func (e *MetricsExporter) writeGossipMetrics(w io.Writer) {
	fmt.Fprintf(w, "# HELP mesh_connection_events_total Total connection lifecycle events.\n")
	fmt.Fprintf(w, "# TYPE mesh_connection_events_total gauge\n")

	if e.scaler != nil && e.scaler.EventLog() != nil {
		fmt.Fprintf(w, "mesh_connection_events_total %d\n", e.scaler.EventLog().Count())
	} else {
		fmt.Fprintf(w, "mesh_connection_events_total 0\n")
	}
}

// writeSelfHealthMetrics emits mesh_self_health_status from SelfHealthMonitor.
// Value encoding: 1 = healthy, 0.5 = lagging, 0 = stalled.
func (e *MetricsExporter) writeSelfHealthMetrics(w io.Writer) {
	fmt.Fprintf(w, "# HELP mesh_self_health_status Health of the observability system itself (1=healthy, 0.5=lagging, 0=stalled).\n")
	fmt.Fprintf(w, "# TYPE mesh_self_health_status gauge\n")

	if e.selfHealth == nil {
		fmt.Fprintf(w, "mesh_self_health_status 0\n")
		return
	}

	health := e.selfHealth.Check()
	val := selfHealthStatusToFloat(health.Status)
	fmt.Fprintf(w, "mesh_self_health_status{status=%q,eval_lag_ms=%q,scan_lag_ms=%q} %.2f\n",
		health.Status,
		fmt.Sprintf("%d", health.EvaluationLag),
		fmt.Sprintf("%d", health.ConnectionScanLag),
		val)
}

// writeConnectionEventMetrics emits mesh_connection_events_total from the event log.
func (e *MetricsExporter) writeConnectionEventMetrics(w io.Writer) {
	if e.scaler == nil || e.scaler.EventLog() == nil {
		return
	}

	summary := e.scaler.EventLog().Summary(time.Now().Add(-5 * time.Minute))
	if len(summary) == 0 {
		return
	}

	fmt.Fprintf(w, "# HELP mesh_connection_events_total Connection events in last 5 minutes.\n")
	fmt.Fprintf(w, "# TYPE mesh_connection_events_total gauge\n")
	for eventType, count := range summary {
		fmt.Fprintf(w, "mesh_connection_events_total{type=%q} %d\n", eventType, count)
	}
}

// healthStatusToFloat converts a combined health status string to a numeric value
// for Prometheus (higher = better).
func healthStatusToFloat(status string) float64 {
	switch status {
	case "healthy":
		return 1.0
	case "degraded":
		return 0.5
	case "connecting":
		return 0.25
	case "unreachable":
		return 0.0
	default:
		return 0.0
	}
}

// writeRPCMetrics emits per-handler RPC call/error/forward counters.
func (e *MetricsExporter) writeRPCMetrics(w io.Writer) {
	if e.rpcServer == nil {
		return
	}
	m := e.rpcServer.Metrics()
	if m == nil {
		return
	}
	stats := m.Stats()
	if len(stats) == 0 {
		return
	}

	fmt.Fprintf(w, "# HELP mesh_rpc_handler_calls_total Total RPC calls per handler.\n")
	fmt.Fprintf(w, "# TYPE mesh_rpc_handler_calls_total counter\n")
	fmt.Fprintf(w, "# HELP mesh_rpc_handler_errors_total Total RPC errors per handler.\n")
	fmt.Fprintf(w, "# TYPE mesh_rpc_handler_errors_total counter\n")
	fmt.Fprintf(w, "# HELP mesh_rpc_handler_forwards_total Total RPC forwards per handler.\n")
	fmt.Fprintf(w, "# TYPE mesh_rpc_handler_forwards_total counter\n")

	for handler, s := range stats {
		fmt.Fprintf(w, "mesh_rpc_handler_calls_total{handler=%q} %d\n", handler, s["calls"])
		fmt.Fprintf(w, "mesh_rpc_handler_errors_total{handler=%q} %d\n", handler, s["errors"])
		fmt.Fprintf(w, "mesh_rpc_handler_forwards_total{handler=%q} %d\n", handler, s["forwards"])
	}

	// Dedup cache stats
	hits, misses := e.rpcServer.DedupStats()
	fmt.Fprintf(w, "# HELP mesh_rpc_dedup_hits_total Response cache hits (parallel probe dedup).\n")
	fmt.Fprintf(w, "# TYPE mesh_rpc_dedup_hits_total counter\n")
	fmt.Fprintf(w, "mesh_rpc_dedup_hits_total %d\n", hits)
	fmt.Fprintf(w, "# HELP mesh_rpc_dedup_misses_total Response cache misses.\n")
	fmt.Fprintf(w, "# TYPE mesh_rpc_dedup_misses_total counter\n")
	fmt.Fprintf(w, "mesh_rpc_dedup_misses_total %d\n", misses)

	// Active handlers
	fmt.Fprintf(w, "# HELP mesh_rpc_active_handlers Current concurrent RPC handler goroutines.\n")
	fmt.Fprintf(w, "# TYPE mesh_rpc_active_handlers gauge\n")
	fmt.Fprintf(w, "mesh_rpc_active_handlers %d\n", e.rpcServer.ActiveHandlers())
}

// selfHealthStatusToFloat converts a self-health status string to a numeric value.
func selfHealthStatusToFloat(status string) float64 {
	switch status {
	case "healthy":
		return 1.0
	case "lagging":
		return 0.5
	case "stalled":
		return 0.0
	default:
		return 0.0
	}
}
