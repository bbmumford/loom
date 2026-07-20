/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/bbmumford/loom/pkg/obshealth"
)

// TestSubsystemsHandler_EmptyRegistryReturnsHealthy verifies that a
// Runtime with a fresh registry reports zero degraded subsystems as a
// 2xx with count=0 (not a 204), matching the documented contract that
// clients treat every 2xx as "reachable and healthy".
func TestSubsystemsHandler_EmptyRegistryReturnsHealthy(t *testing.T) {
	rt := &Runtime{healthRegistry: health.New(health.AllowedSubsystems())}
	h := rt.SubsystemsHandler()
	if h == nil {
		t.Fatal("SubsystemsHandler returned nil for a Runtime with a registry")
	}

	req := httptest.NewRequest(http.MethodGet, "/api/monitoring/subsystems", nil)
	rr := httptest.NewRecorder()
	h(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}

	var resp SubsystemsResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Count != 0 {
		t.Fatalf("count = %d, want 0", resp.Count)
	}
	if len(resp.Entries) != 0 {
		t.Fatalf("entries = %d, want 0", len(resp.Entries))
	}
	if resp.GeneratedAt.IsZero() {
		t.Fatal("GeneratedAt should be non-zero")
	}
}

// TestSubsystemsHandler_MarkedSubsystemAppearsInSnapshot exercises the
// end-to-end wire: Mark → HTTP response → JSON decode → verify entry
// shape. Confirms the registry snapshot is what the /api/monitoring
// surface actually emits.
func TestSubsystemsHandler_MarkedSubsystemAppearsInSnapshot(t *testing.T) {
	reg := health.New(health.AllowedSubsystems())
	rt := &Runtime{healthRegistry: reg}

	at := time.Now()
	if err := reg.Mark(health.SubsystemBillingWebhooks, at, errors.New("stripe 5xx")); err != nil {
		t.Fatalf("Mark: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/monitoring/subsystems", nil)
	rr := httptest.NewRecorder()
	rt.SubsystemsHandler()(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rr.Code)
	}
	var resp SubsystemsResponse
	if err := json.NewDecoder(rr.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Count != 1 || len(resp.Entries) != 1 {
		t.Fatalf("count/entries = %d/%d, want 1/1", resp.Count, len(resp.Entries))
	}
	got := resp.Entries[0]
	if got.Name != health.SubsystemBillingWebhooks {
		t.Fatalf("entry name = %s, want %s", got.Name, health.SubsystemBillingWebhooks)
	}
	if got.Error != "stripe 5xx" {
		t.Fatalf("entry error = %q, want stripe 5xx", got.Error)
	}
}

// TestSubsystemsHandler_NilRegistryReturnsNilHandler verifies the
// fail-closed guard: a Runtime built without Initialize (test path)
// returns nil so chi treats it as "no handler mounted" instead of
// panicking on a nil-registry Snapshot call.
func TestSubsystemsHandler_NilRegistryReturnsNilHandler(t *testing.T) {
	rt := &Runtime{}
	if h := rt.SubsystemsHandler(); h != nil {
		t.Fatalf("SubsystemsHandler on zero-Runtime should be nil; got %T", h)
	}
}

// TestHealthCheck_MarksAndClearsSubsystems drives the HealthCheck loop
// with a raft leader lookup that fails then recovers, and asserts the
// SubsystemMeshRaft entry appears in the registry Snapshot during
// failure and drops out on recovery. This is the finding's core
// contract: a subsystem failure surfaces as a queryable degraded entry,
// not just a log line.
func TestHealthCheck_MarksAndClearsSubsystems(t *testing.T) {
	reg := health.New(health.AllowedSubsystems())

	var (
		failMode  = true
		callCount int
	)
	deps := HealthCheckDeps{
		GetLeader: func() (string, error) {
			callCount++
			if failMode {
				return "", errors.New("no leader")
			}
			return "node-1", nil
		},
		IsLeader: func() bool { return false },
		Registry: reg,
	}
	hc := NewHealthCheck(deps, time.Hour) // interval only used by Start; we drive manually
	hc.performHealthCheck()

	snap := reg.Snapshot()
	if len(snap.Entries) != 1 || snap.Entries[0].Name != health.SubsystemMeshRaft {
		t.Fatalf("expected 1 mesh.raft entry after failure; got %+v", snap.Entries)
	}

	failMode = false
	hc.performHealthCheck()

	snap = reg.Snapshot()
	if len(snap.Entries) != 0 {
		t.Fatalf("expected registry cleared after recovery; got %+v", snap.Entries)
	}
	if callCount < 2 {
		t.Fatalf("expected at least 2 GetLeader calls; got %d", callCount)
	}
}

// TestSelfHealthMonitor_MarksOnLaggingAndClearsOnHealthy exercises the
// SelfHealthMonitor → Registry reflection path. Uses a stub evaluator
// with a fixed LastEvaluation to drive the observability status
// deterministically.
func TestSelfHealthMonitor_MarksOnLaggingAndClearsOnHealthy(t *testing.T) {
	reg := health.New(health.AllowedSubsystems())

	stub := &stubEvaluator{lastEval: time.Now().Add(-2 * time.Minute)}
	reporter := NilConnectionReporter{}
	cfg := DefaultSelfHealthMonitorConfig()
	m := NewSelfHealthMonitor(stub, reporter, 30*time.Second, cfg)
	m.SetRegistry(reg)

	// Seed the connection scan so scanLag is non-zero-based but healthy.
	m.scanReporter()

	// First: evaluator is stale → status should be lagging or stalled.
	h := m.Check()
	if h.Status == "healthy" {
		t.Fatalf("expected lagging/stalled; got %s (evalLag=%d)", h.Status, h.EvaluationLag)
	}
	m.reflectToRegistry(h)

	snap := reg.Snapshot()
	if snap.Entries == nil || len(snap.Entries) == 0 {
		t.Fatalf("expected marked subsystems after lagging tick; got empty")
	}
	seen := map[health.SubsystemID]bool{}
	for _, e := range snap.Entries {
		seen[e.Name] = true
	}
	if !seen[health.SubsystemHealthEvaluator] {
		t.Fatalf("SubsystemHealthEvaluator not marked; got %+v", snap.Entries)
	}

	// Recover the evaluator → status returns healthy → registry
	// should Clear both subsystems.
	stub.lastEval = time.Now()
	m.scanReporter()
	h = m.Check()
	if h.Status != "healthy" {
		t.Fatalf("expected healthy after recovery; got %s", h.Status)
	}
	m.reflectToRegistry(h)
	snap = reg.Snapshot()
	if len(snap.Entries) != 0 {
		t.Fatalf("expected registry cleared after recovery; got %+v", snap.Entries)
	}
}

// stubEvaluator is a minimal HealthEvaluator stub for driving
// SelfHealthMonitor tests without a full node.Runtime.
type stubEvaluator struct {
	lastEval time.Time
}

func (s *stubEvaluator) ServiceHealth(string) *ServiceHealthReport      { return nil }
func (s *stubEvaluator) AllServiceHealth() []*ServiceHealthReport       { return nil }
func (s *stubEvaluator) MeshStatus(string) string                       { return "unreachable" }
func (s *stubEvaluator) LastEvaluation() time.Time                      { return s.lastEval }
func (s *stubEvaluator) Start()                                         {}
func (s *stubEvaluator) Stop()                                          {}
