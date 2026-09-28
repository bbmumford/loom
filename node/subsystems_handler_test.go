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

func (s *stubEvaluator) ServiceHealth(string) *ServiceHealthReport { return nil }
func (s *stubEvaluator) AllServiceHealth() []*ServiceHealthReport  { return nil }
func (s *stubEvaluator) MeshStatus(string) string                  { return "unreachable" }
func (s *stubEvaluator) LastEvaluation() time.Time                 { return s.lastEval }
func (s *stubEvaluator) Start()                                    {}
func (s *stubEvaluator) Stop()                                     {}

// subsystemsCORSHeader runs one request with the given Origin header (none
// when origin is "") through h and returns the Access-Control-Allow-Origin
// value and the status code.
func subsystemsCORSHeader(t *testing.T, h http.HandlerFunc, method, origin string) (string, int) {
	t.Helper()
	req := httptest.NewRequest(method, "/api/monitoring/subsystems", nil)
	if origin != "" {
		req.Header.Set("Origin", origin)
	}
	rr := httptest.NewRecorder()
	h(rr, req)
	return rr.Header().Get("Access-Control-Allow-Origin"), rr.Code
}

// TestSubsystemsHandler_DefaultOriginsAdmitExactlyBuiltInPair pins the
// behaviour of a Config that leaves SubsystemsAllowedOrigins nil: the two
// built-in origins are echoed, on GET and on the OPTIONS preflight, and no
// other origin — including near-misses on scheme, port and suffix — is.
func TestSubsystemsHandler_DefaultOriginsAdmitExactlyBuiltInPair(t *testing.T) {
	rt := &Runtime{healthRegistry: health.New(health.AllowedSubsystems())}
	h := rt.SubsystemsHandler()

	for _, origin := range []string{"https://help.orbtr.io", "https://localhost:8080"} {
		for _, method := range []string{http.MethodGet, http.MethodOptions} {
			got, _ := subsystemsCORSHeader(t, h, method, origin)
			if got != origin {
				t.Fatalf("%s Origin=%q: Access-Control-Allow-Origin = %q, want %q", method, origin, got, origin)
			}
		}
	}
	for _, origin := range []string{
		"",
		"https://node.example.com",
		"http://help.orbtr.io",
		"https://help.orbtr.io.example.com",
		"http://localhost:8080",
		"https://localhost:8081",
		"null",
	} {
		if got, _ := subsystemsCORSHeader(t, h, http.MethodGet, origin); got != "" {
			t.Fatalf("Origin=%q admitted by the default list: Access-Control-Allow-Origin = %q", origin, got)
		}
	}
}

// TestSubsystemsHandler_ConfiguredOriginsReplaceDefault shows a non-nil
// SubsystemsAllowedOrigins replaces the built-in pair rather than adding to
// it, that an empty non-nil list admits nothing, and that the handler keeps
// the list it was built with when the caller's slice changes afterwards.
func TestSubsystemsHandler_ConfiguredOriginsReplaceDefault(t *testing.T) {
	configured := []string{"https://node.example.com", ""}
	rt := &Runtime{
		healthRegistry: health.New(health.AllowedSubsystems()),
		cfg:            Config{SubsystemsAllowedOrigins: configured},
	}
	h := rt.SubsystemsHandler()

	if got, code := subsystemsCORSHeader(t, h, http.MethodOptions, "https://node.example.com"); got != "https://node.example.com" || code != http.StatusNoContent {
		t.Fatalf("configured origin preflight: Access-Control-Allow-Origin = %q, status %d; want echo and 204", got, code)
	}
	for _, origin := range []string{"https://help.orbtr.io", "https://localhost:8080", ""} {
		if got, _ := subsystemsCORSHeader(t, h, http.MethodGet, origin); got != "" {
			t.Fatalf("Origin=%q admitted although the configured list replaces the default: %q", origin, got)
		}
	}

	configured[0] = "https://other.example.com"
	if got, _ := subsystemsCORSHeader(t, h, http.MethodGet, "https://node.example.com"); got != "https://node.example.com" {
		t.Fatalf("handler followed a mutation of the caller's slice: Access-Control-Allow-Origin = %q", got)
	}
	if got, _ := subsystemsCORSHeader(t, h, http.MethodGet, "https://other.example.com"); got != "" {
		t.Fatalf("handler admitted an origin written into the caller's slice after build: %q", got)
	}

	none := &Runtime{
		healthRegistry: health.New(health.AllowedSubsystems()),
		cfg:            Config{SubsystemsAllowedOrigins: []string{}},
	}
	hn := none.SubsystemsHandler()
	for _, origin := range []string{"https://help.orbtr.io", "https://localhost:8080", "https://node.example.com"} {
		if got, _ := subsystemsCORSHeader(t, hn, http.MethodGet, origin); got != "" {
			t.Fatalf("empty non-nil list admitted Origin=%q: %q", origin, got)
		}
	}
}
