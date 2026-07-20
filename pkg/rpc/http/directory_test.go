/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package rpchttp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/go-chi/chi/v5"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/bbmumford/loom/pkg/rpc"
)

// Finding rev-080 tests — directoryAuth must be per-Bridge (per-Registry),
// not a package-global. These tests pin:
//
//  1. HandleDirectory(reg, nil) returns SUMMARY (fail-closed default).
//  2. HandleDirectory(reg, allowFn) returns VERBOSE only when allowFn is true.
//  3. Two Bridges with distinct auth predicates DO NOT share state — one
//     bridge's SetDirectoryAuth cannot open the other bridge's directory.
//  4. Bridge.SetDirectoryAuth is safe under concurrent request traffic
//     (atomic.Pointer swap, no torn reads on the hot path).
//  5. HandleRPC's _directory special case routes through the bridge's own
//     auth predicate — a wildcard mount cannot bypass per-registry gating.

func dirTestHandler() rpc.Handler {
	return rpc.Handler{
		Namespace: "test",
		Domain:    "rev080",
		Operation: "Ping",
		Func: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			return req, nil
		},
		Request:  (*structpb.Struct)(nil),
		Response: (*structpb.Struct)(nil),
		Scope:    rpc.ScopeNone,
	}
}

func mustRegisterDirTestHandler(t *testing.T) *rpc.Registry {
	t.Helper()
	reg := rpc.NewRegistry()
	if err := reg.Register(dirTestHandler()); err != nil {
		t.Fatalf("register: %v", err)
	}
	return reg
}

func decodeSummary(t *testing.T, body string) DirectorySummary {
	t.Helper()
	var s DirectorySummary
	if err := json.Unmarshal([]byte(body), &s); err != nil {
		t.Fatalf("decode summary: %v (body=%q)", err, body)
	}
	return s
}

func decodeVerbose(t *testing.T, body string) DirectoryResponse {
	t.Helper()
	var v DirectoryResponse
	if err := json.Unmarshal([]byte(body), &v); err != nil {
		t.Fatalf("decode verbose: %v (body=%q)", err, body)
	}
	return v
}

// isSummary distinguishes the two wire shapes: DirectorySummary has NO
// "entries" key; DirectoryResponse always emits "entries" (possibly []).
func isSummary(body string) bool {
	return !strings.Contains(body, `"entries"`)
}

// TestHandleDirectory_NilAuth_ReturnsSummary — nil predicate MUST fail-
// closed to summary. This is the safe default for a fresh mount that
// hasn't wired an auth predicate yet.
func TestHandleDirectory_NilAuth_ReturnsSummary(t *testing.T) {
	reg := mustRegisterDirTestHandler(t)
	rec := httptest.NewRecorder()
	HandleDirectory(reg, nil).ServeHTTP(rec, httptest.NewRequest("GET", "/rpc/_directory", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rec.Code)
	}
	body := rec.Body.String()
	if !isSummary(body) {
		t.Fatalf("nil auth must fail-closed to summary, got verbose body: %s", body)
	}
	s := decodeSummary(t, body)
	if s.Count != 1 {
		t.Fatalf("want count=1, got %d", s.Count)
	}
}

// TestHandleDirectory_AuthDenies_ReturnsSummary — predicate returns false,
// caller sees summary only.
func TestHandleDirectory_AuthDenies_ReturnsSummary(t *testing.T) {
	reg := mustRegisterDirTestHandler(t)
	rec := httptest.NewRecorder()
	HandleDirectory(reg, func(*http.Request) bool { return false }).
		ServeHTTP(rec, httptest.NewRequest("GET", "/rpc/_directory", nil))

	if !isSummary(rec.Body.String()) {
		t.Fatalf("auth=false must return summary, got: %s", rec.Body.String())
	}
}

// TestHandleDirectory_AuthAllows_ReturnsVerbose — predicate returns true,
// caller sees full entries with FQN/Scope/Tags.
func TestHandleDirectory_AuthAllows_ReturnsVerbose(t *testing.T) {
	reg := mustRegisterDirTestHandler(t)
	rec := httptest.NewRecorder()
	HandleDirectory(reg, func(*http.Request) bool { return true }).
		ServeHTTP(rec, httptest.NewRequest("GET", "/rpc/_directory", nil))

	body := rec.Body.String()
	if isSummary(body) {
		t.Fatalf("auth=true must return verbose, got summary: %s", body)
	}
	v := decodeVerbose(t, body)
	if len(v.Entries) != 1 {
		t.Fatalf("want 1 entry, got %d", len(v.Entries))
	}
	if v.Entries[0].FQN != "test.rev080.Ping" {
		t.Fatalf("want FQN=test.rev080.Ping, got %q", v.Entries[0].FQN)
	}
}

// TestBridge_HandleDirectory_NilAuth_FailsClosed — a fresh Bridge with
// no SetDirectoryAuth call MUST default to summary. This is the exact
// fail-closed regression rev-080 protects against — the previous
// package-global default was "nil → summary" too, but a sibling
// registry's SetDirectoryAuthCheck(func { return true }) would flip
// EVERY bridge in the process. The per-Bridge state prevents that.
func TestBridge_HandleDirectory_NilAuth_FailsClosed(t *testing.T) {
	reg := mustRegisterDirTestHandler(t)
	bridge := NewBridge(reg)
	rec := httptest.NewRecorder()
	bridge.HandleDirectory().ServeHTTP(rec, httptest.NewRequest("GET", "/rpc/_directory", nil))
	if !isSummary(rec.Body.String()) {
		t.Fatalf("fresh Bridge must default to summary, got: %s", rec.Body.String())
	}
}

// TestBridge_SetDirectoryAuth_AllowsVerbose — after SetDirectoryAuth with
// a permissive predicate, the same bridge exposes verbose entries.
func TestBridge_SetDirectoryAuth_AllowsVerbose(t *testing.T) {
	reg := mustRegisterDirTestHandler(t)
	bridge := NewBridge(reg)
	bridge.SetDirectoryAuth(func(*http.Request) bool { return true })

	rec := httptest.NewRecorder()
	bridge.HandleDirectory().ServeHTTP(rec, httptest.NewRequest("GET", "/rpc/_directory", nil))
	if isSummary(rec.Body.String()) {
		t.Fatalf("bridge with allow-all auth must return verbose, got: %s", rec.Body.String())
	}
}

// TestBridge_SetDirectoryAuth_Nil_RevertsToSummary — clearing the
// predicate with nil MUST revert to summary-only (fail-closed).
func TestBridge_SetDirectoryAuth_Nil_RevertsToSummary(t *testing.T) {
	reg := mustRegisterDirTestHandler(t)
	bridge := NewBridge(reg)
	bridge.SetDirectoryAuth(func(*http.Request) bool { return true })
	// Sanity: verbose now.
	rec := httptest.NewRecorder()
	bridge.HandleDirectory().ServeHTTP(rec, httptest.NewRequest("GET", "/rpc/_directory", nil))
	if isSummary(rec.Body.String()) {
		t.Fatalf("precondition failed: expected verbose before nil clear")
	}
	// Clear.
	bridge.SetDirectoryAuth(nil)
	rec = httptest.NewRecorder()
	bridge.HandleDirectory().ServeHTTP(rec, httptest.NewRequest("GET", "/rpc/_directory", nil))
	if !isSummary(rec.Body.String()) {
		t.Fatalf("SetDirectoryAuth(nil) must fail-closed to summary, got: %s", rec.Body.String())
	}
}

// TestBridge_DirectoryAuth_PerRegistryIsolation — THE core rev-080
// regression test. Two Bridges around two Registries: setting an
// allow-all predicate on Bridge A MUST NOT open Bridge B's directory.
// The old package-global directoryAuth caused exactly that cross-
// registry leak.
func TestBridge_DirectoryAuth_PerRegistryIsolation(t *testing.T) {
	regA := mustRegisterDirTestHandler(t)
	regB := mustRegisterDirTestHandler(t)
	bridgeA := NewBridge(regA)
	bridgeB := NewBridge(regB)

	// Open A only.
	bridgeA.SetDirectoryAuth(func(*http.Request) bool { return true })

	// A → verbose.
	recA := httptest.NewRecorder()
	bridgeA.HandleDirectory().ServeHTTP(recA, httptest.NewRequest("GET", "/rpc/_directory", nil))
	if isSummary(recA.Body.String()) {
		t.Fatalf("bridge A with allow-all must return verbose, got: %s", recA.Body.String())
	}

	// B → summary (isolation).
	recB := httptest.NewRecorder()
	bridgeB.HandleDirectory().ServeHTTP(recB, httptest.NewRequest("GET", "/rpc/_directory", nil))
	if !isSummary(recB.Body.String()) {
		t.Fatalf("bridge B MUST remain summary-only — cross-registry leak (rev-080), got: %s", recB.Body.String())
	}
}

// TestBridge_HandleRPC_DirectoryWildcard_UsesBridgeAuth — the wildcard
// route (POST /rpc/{handler}) special-cases "_directory" and MUST route
// through the bridge's own predicate. Prior to rev-080 it routed
// through HandleDirectory(reg), which read the package-global. Now it
// consults b.HandleDirectory() → b.loadDirectoryAuth().
func TestBridge_HandleRPC_DirectoryWildcard_UsesBridgeAuth(t *testing.T) {
	reg := mustRegisterDirTestHandler(t)
	bridge := NewBridge(reg)

	// Route via chi to populate the {handler} URL param exactly as prod does.
	r := chiRouterForBridge(bridge)

	// Default (no auth wired) — summary.
	rec := httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest("POST", "/rpc/_directory", strings.NewReader("{}")))
	if !isSummary(rec.Body.String()) {
		t.Fatalf("wildcard _directory default must fail-closed to summary, got: %s", rec.Body.String())
	}

	// Wire allow-all on this bridge → verbose.
	bridge.SetDirectoryAuth(func(*http.Request) bool { return true })
	rec = httptest.NewRecorder()
	r.ServeHTTP(rec, httptest.NewRequest("POST", "/rpc/_directory", strings.NewReader("{}")))
	if isSummary(rec.Body.String()) {
		t.Fatalf("wildcard _directory must respect bridge's SetDirectoryAuth, got summary: %s", rec.Body.String())
	}
}

// chiRouterForBridge builds a minimal chi router mounting the wildcard
// bridge dispatch route. Used by wildcard-path directory tests so
// chi.URLParam("handler") is populated exactly as prod does.
func chiRouterForBridge(b *Bridge) http.Handler {
	r := chi.NewRouter()
	r.Post("/rpc/{handler}", b.HandleRPC)
	return r
}

// TestBridge_SetDirectoryAuth_ConcurrentReadDuringSwap — the atomic.Pointer
// swap MUST be race-free with concurrent HandleDirectory reads. Under
// -race this test catches torn pointer reads; without -race it still
// exercises the atomic contract shape.
func TestBridge_SetDirectoryAuth_ConcurrentReadDuringSwap(t *testing.T) {
	reg := mustRegisterDirTestHandler(t)
	bridge := NewBridge(reg)

	var stop atomic.Bool
	var readersWG sync.WaitGroup
	var writerWG sync.WaitGroup

	// Writer: alternate nil ↔ allow-all until readers finish and set stop.
	writerWG.Add(1)
	go func() {
		defer writerWG.Done()
		toggle := false
		for !stop.Load() {
			if toggle {
				bridge.SetDirectoryAuth(func(*http.Request) bool { return true })
			} else {
				bridge.SetDirectoryAuth(nil)
			}
			toggle = !toggle
		}
	}()

	// Readers: bounded iterations. Any non-200 or panic indicates a race
	// or a torn read. Both shapes (summary/verbose) are acceptable — the
	// point is fault-free servicing during a swap.
	for i := 0; i < 4; i++ {
		readersWG.Add(1)
		go func() {
			defer readersWG.Done()
			h := bridge.HandleDirectory()
			for j := 0; j < 200; j++ {
				rec := httptest.NewRecorder()
				h.ServeHTTP(rec, httptest.NewRequest("GET", "/rpc/_directory", nil))
				if rec.Code != http.StatusOK {
					t.Errorf("want 200 during concurrent swap, got %d", rec.Code)
					return
				}
			}
		}()
	}

	// Wait readers → signal writer → wait writer. This avoids the
	// deadlock shape where main waits on a group the writer is in and
	// the writer waits on a stop signal main never sends.
	readersWG.Wait()
	stop.Store(true)
	writerWG.Wait()
}
