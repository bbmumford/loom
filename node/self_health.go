/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/bbmumford/loom/pkg/obshealth"
)

// ---------------------------------------------------------------------------
// ObservabilityHealth — health status of the observability system itself.
// ---------------------------------------------------------------------------

// ObservabilityHealth tracks the health of the observability system itself.
// Returned by SelfHealthMonitor.Check() and included in topology API responses.
type ObservabilityHealth struct {
	LastEvaluation    time.Time     `json:"lastEvaluation"`
	LastConnectionScan time.Time    `json:"lastConnectionScan"`
	EvaluationLag     int64 `json:"evaluationLagMs"`
	ConnectionScanLag int64 `json:"connectionScanLagMs"`
	Status            string        `json:"status"` // "healthy", "lagging", "stalled"
}

// ---------------------------------------------------------------------------
// SelfHealthMonitorConfig
// ---------------------------------------------------------------------------

// SelfHealthMonitorConfig holds configuration for the self-health monitor.
type SelfHealthMonitorConfig struct {
	// CheckInterval is how often the monitor checks evaluator and reporter health.
	// Default: 30s.
	CheckInterval time.Duration

	// LaggingMultiplier is the multiplier of the evaluation interval before
	// status becomes "lagging". Default: 2.
	LaggingMultiplier int

	// StalledMultiplier is the multiplier of the evaluation interval before
	// status becomes "stalled". Default: 5.
	StalledMultiplier int
}

// DefaultSelfHealthMonitorConfig returns sensible defaults.
func DefaultSelfHealthMonitorConfig() SelfHealthMonitorConfig {
	return SelfHealthMonitorConfig{
		CheckInterval:     30 * time.Second,
		LaggingMultiplier: 2,
		StalledMultiplier: 5,
	}
}

// ---------------------------------------------------------------------------
// SelfHealthMonitor — watches the HealthEvaluator and ConnectionReporter.
// ---------------------------------------------------------------------------

// SelfHealthMonitor periodically checks that the HealthEvaluator and
// ConnectionReporter are running and not stalled. If either component falls
// behind its expected interval, the monitor reports a degraded status.
type SelfHealthMonitor struct {
	mu               sync.RWMutex
	evaluator        HealthEvaluator
	reporter         ConnectionReporter
	evalInterval     time.Duration // the evaluator's configured eval interval
	cfg              SelfHealthMonitorConfig
	lastConnScan     time.Time // when we last successfully read from reporter
	stopCh           chan struct{}
	wg               sync.WaitGroup
	// registry is the optional platform-wide subsystem health sink. When
	// non-nil, the monitor reflects "lagging"/"stalled" states into
	// SubsystemHealthEvaluator and SubsystemConnectionReporter — so
	// meshmon and the /api/monitoring/subsystems surface see the
	// specific stalled component instead of just a single connection bit.
	// Nil is legal and disables the reflection entirely.
	registry *health.Registry
}

// NewSelfHealthMonitor creates a monitor that watches the health evaluator
// and connection reporter for staleness.
//
// Parameters:
//   - evaluator: the HealthEvaluator to monitor (must not be nil)
//   - reporter: the ConnectionReporter to monitor (must not be nil)
//   - evalInterval: the evaluator's configured evaluation interval (used for threshold calculation)
//   - cfg: monitor configuration
func NewSelfHealthMonitor(
	evaluator HealthEvaluator,
	reporter ConnectionReporter,
	evalInterval time.Duration,
	cfg SelfHealthMonitorConfig,
) *SelfHealthMonitor {
	if cfg.CheckInterval == 0 {
		cfg.CheckInterval = 30 * time.Second
	}
	if cfg.LaggingMultiplier <= 0 {
		cfg.LaggingMultiplier = 2
	}
	if cfg.StalledMultiplier <= 0 {
		cfg.StalledMultiplier = 5
	}
	if evalInterval == 0 {
		evalInterval = 30 * time.Second
	}

	return &SelfHealthMonitor{
		evaluator:    evaluator,
		reporter:     reporter,
		evalInterval: evalInterval,
		cfg:          cfg,
		stopCh:       make(chan struct{}),
	}
}

// SetRegistry attaches the platform-wide subsystem health registry.
// May be called at most once, before Start. Passing a nil Registry is
// a no-op — subsystem reflection stays disabled.
func (m *SelfHealthMonitor) SetRegistry(r *health.Registry) {
	if r == nil {
		return
	}
	m.mu.Lock()
	m.registry = r
	m.mu.Unlock()
}

// Start begins the periodic self-health monitoring loop.
func (m *SelfHealthMonitor) Start() {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()

		// Initial scan to seed lastConnScan.
		m.scanReporter()

		ticker := time.NewTicker(m.cfg.CheckInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				m.scanReporter()
				h := m.Check()
				m.reflectToRegistry(h)
				if h.Status == "stalled" {
					log.Printf("[WATCHDOG] Observability stalled: eval_lag=%v scan_lag=%v",
						h.EvaluationLag, h.ConnectionScanLag)
				}
			case <-m.stopCh:
				return
			}
		}
	}()
}

// reflectToRegistry mirrors the composite observability status into the
// platform-wide Registry so downstream consumers (meshmon, notifier,
// /api/monitoring/subsystems) see per-subsystem degradation instead of a
// single connected/disconnected bit.
//
// Encoding:
//
//   - status="healthy"  → Clear evaluator + reporter subsystems
//   - status="lagging"  → Mark evaluator + reporter (recoverable)
//   - status="stalled"  → Mark evaluator + reporter (unrecoverable
//     without intervention)
//
// Both subsystems are marked together because the composite status is
// dominated by the WORSE of the two lags; per-lane splitting would
// require the caller to reason about two independent thresholds and
// isn't worth the complexity for the current call sites.
func (m *SelfHealthMonitor) reflectToRegistry(h ObservabilityHealth) {
	m.mu.RLock()
	reg := m.registry
	m.mu.RUnlock()
	if reg == nil {
		return
	}
	at := time.Now()
	switch h.Status {
	case "lagging":
		err := fmt.Errorf("observability lagging: eval_lag_ms=%d scan_lag_ms=%d",
			h.EvaluationLag, h.ConnectionScanLag)
		_ = reg.Mark(health.SubsystemHealthEvaluator, at, err)
		_ = reg.Mark(health.SubsystemConnectionReporter, at, err)
	case "stalled":
		err := fmt.Errorf("observability stalled: eval_lag_ms=%d scan_lag_ms=%d",
			h.EvaluationLag, h.ConnectionScanLag)
		_ = reg.Mark(health.SubsystemHealthEvaluator, at, err)
		_ = reg.Mark(health.SubsystemConnectionReporter, at, err)
	default:
		_ = reg.Clear(health.SubsystemHealthEvaluator, at)
		_ = reg.Clear(health.SubsystemConnectionReporter, at)
	}
}

// Stop halts the self-health monitoring loop.
func (m *SelfHealthMonitor) Stop() {
	close(m.stopCh)
	m.wg.Wait()
}

// Check returns the current health of the observability system.
//
// If the evaluator has never run (LastEvaluation is zero), the evaluator-lag
// is reported as 0 and treated as healthy — we can't know the component is
// stalled until it's had at least one chance to run. Same for the reporter
// scan. Without this guard, time.Since(zeroTime) returns ~292 years and
// overflows to a junk lag value (seen in logs as eval_lag=9223372036854).
func (m *SelfHealthMonitor) Check() ObservabilityHealth {
	now := time.Now()

	lastEval := m.evaluator.LastEvaluation()
	var evalLag time.Duration
	if !lastEval.IsZero() {
		evalLag = now.Sub(lastEval)
	}

	m.mu.RLock()
	lastScan := m.lastConnScan
	m.mu.RUnlock()

	var scanLag time.Duration
	if !lastScan.IsZero() {
		scanLag = now.Sub(lastScan)
	}

	laggingThreshold := time.Duration(m.cfg.LaggingMultiplier) * m.evalInterval
	stalledThreshold := time.Duration(m.cfg.StalledMultiplier) * m.evalInterval

	status := "healthy"
	if evalLag > laggingThreshold || scanLag > laggingThreshold {
		status = "lagging"
	}
	if evalLag > stalledThreshold || scanLag > stalledThreshold {
		status = "stalled"
	}

	return ObservabilityHealth{
		LastEvaluation:     lastEval,
		LastConnectionScan: lastScan,
		EvaluationLag:      evalLag.Milliseconds(),
		ConnectionScanLag:  scanLag.Milliseconds(),
		Status:             status,
	}
}

// scanReporter reads from the ConnectionReporter to verify it is responsive.
// On success, updates lastConnScan to now.
func (m *SelfHealthMonitor) scanReporter() {
	// Call ActiveConnections to verify the reporter is responsive.
	// We don't need the result — just confirming it doesn't hang.
	_ = m.reporter.ActiveConnections()

	m.mu.Lock()
	m.lastConnScan = time.Now()
	m.mu.Unlock()
}
