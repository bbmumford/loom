/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package dispatch

import (
	"sync"
	"time"
)

// RoleStats holds per-role RPC statistics.
type RoleStats struct {
	Calls        int64         `json:"calls"`
	Successes    int64         `json:"successes"`
	Failures     int64         `json:"failures"`
	Timeouts     int64         `json:"timeouts"`
	CircuitTrips int64         `json:"circuitTrips"`
	LatencySum   time.Duration `json:"-"`
	LatencyMin   time.Duration `json:"latencyMin"`
	LatencyMax   time.Duration `json:"latencyMax"`
}

// AvgLatency returns the average latency across successful calls.
func (rs *RoleStats) AvgLatency() time.Duration {
	if rs.Successes == 0 {
		return 0
	}
	return rs.LatencySum / time.Duration(rs.Successes)
}

// RPCMetrics tracks per-role RPC call statistics.
type RPCMetrics struct {
	mu    sync.Mutex
	roles map[string]*RoleStats
}

// NewRPCMetrics creates a new RPC metrics tracker.
func NewRPCMetrics() *RPCMetrics {
	return &RPCMetrics{roles: make(map[string]*RoleStats)}
}

func (m *RPCMetrics) get(role string) *RoleStats {
	rs, ok := m.roles[role]
	if !ok {
		rs = &RoleStats{}
		m.roles[role] = rs
	}
	return rs
}

// RecordCall records an RPC call result with latency.
func (m *RPCMetrics) RecordCall(role string, latency time.Duration, success bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rs := m.get(role)
	rs.Calls++
	if success {
		rs.Successes++
		rs.LatencySum += latency
		if rs.LatencyMin == 0 || latency < rs.LatencyMin {
			rs.LatencyMin = latency
		}
		if latency > rs.LatencyMax {
			rs.LatencyMax = latency
		}
	} else {
		rs.Failures++
	}
}

// RecordTimeout records a timeout for a role.
func (m *RPCMetrics) RecordTimeout(role string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rs := m.get(role)
	rs.Timeouts++
}

// RecordCircuitTrip records a circuit breaker trip for a role.
func (m *RPCMetrics) RecordCircuitTrip(role string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rs := m.get(role)
	rs.CircuitTrips++
}

// Snapshot returns a copy of all role stats.
func (m *RPCMetrics) Snapshot() map[string]RoleStats {
	m.mu.Lock()
	defer m.mu.Unlock()
	snap := make(map[string]RoleStats, len(m.roles))
	for role, rs := range m.roles {
		snap[role] = *rs
	}
	return snap
}
