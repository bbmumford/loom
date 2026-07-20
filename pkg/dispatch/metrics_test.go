/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package dispatch

import (
	"testing"
	"time"
)

func TestRPCMetrics_RecordAndSnapshot(t *testing.T) {
	m := NewRPCMetrics()
	m.RecordCall("platform", 5*time.Millisecond, true)
	m.RecordCall("platform", 10*time.Millisecond, true)
	m.RecordCall("platform", 0, false)

	snap := m.Snapshot()
	rs, ok := snap["platform"]
	if !ok {
		t.Fatal("expected platform in snapshot")
	}
	if rs.Calls != 3 {
		t.Fatalf("calls: got %d, want 3", rs.Calls)
	}
	if rs.Successes != 2 {
		t.Fatalf("successes: got %d, want 2", rs.Successes)
	}
	if rs.Failures != 1 {
		t.Fatalf("failures: got %d, want 1", rs.Failures)
	}
	if rs.AvgLatency() != 7500*time.Microsecond {
		t.Fatalf("avg latency: got %v, want 7.5ms", rs.AvgLatency())
	}
}

func TestRPCMetrics_RecordTimeout(t *testing.T) {
	m := NewRPCMetrics()
	m.RecordTimeout("identity")
	m.RecordTimeout("identity")
	snap := m.Snapshot()
	if snap["identity"].Timeouts != 2 {
		t.Fatalf("timeouts: got %d, want 2", snap["identity"].Timeouts)
	}
}

func TestRPCMetrics_RecordCircuitTrip(t *testing.T) {
	m := NewRPCMetrics()
	m.RecordCircuitTrip("billing")
	snap := m.Snapshot()
	if snap["billing"].CircuitTrips != 1 {
		t.Fatalf("trips: got %d, want 1", snap["billing"].CircuitTrips)
	}
}
