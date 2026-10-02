/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package meshstatus

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ORBTR/aether"
	"github.com/bbmumford/loom/ports"
)

// stubProvider is a RuntimeProvider with no directory, enough for the
// handler to render a response without a live runtime.
type stubProvider struct{}

func (stubProvider) NodeID() aether.NodeID               { return aether.NodeID("vl1_test") }
func (stubProvider) MeshMetrics() map[string]interface{} { return map[string]interface{}{} }
func (stubProvider) Directory() DirectoryProvider        { return nil }
func (stubProvider) ServiceName() string                 { return "node.example.com" }

// corsHeader runs one request with the given Origin header (none when origin
// is "") through h and returns the Access-Control-Allow-Origin value and the
// status code.
func corsHeader(t *testing.T, h http.HandlerFunc, method, origin string) (string, int) {
	t.Helper()
	req := httptest.NewRequest(method, "/mesh/status", nil)
	if origin != "" {
		req.Header.Set("Origin", origin)
	}
	rr := httptest.NewRecorder()
	h(rr, req)
	return rr.Header().Get("Access-Control-Allow-Origin"), rr.Code
}

// TestNewHandler_NoOptionAdmitsOnlyTheBuiltInOrigin pins the behaviour of a
// handler built with no Option: the built-in origin https://localhost:8080 is
// echoed on GET and on the OPTIONS preflight, and no other origin is, including
// a monitoring page's origin and near-misses of the built-in one on scheme and
// port. The preflight still answers 204 and GET still serves the status.
func TestNewHandler_NoOptionAdmitsOnlyTheBuiltInOrigin(t *testing.T) {
	h := NewHandler(stubProvider{}, ports.DevPlatform(), time.Now())

	const builtIn = "https://localhost:8080"
	if got, code := corsHeader(t, h, http.MethodGet, builtIn); got != builtIn || code != http.StatusOK {
		t.Fatalf("GET Origin=%q: Access-Control-Allow-Origin = %q, status %d; want echo and 200", builtIn, got, code)
	}
	if got, code := corsHeader(t, h, http.MethodOptions, builtIn); got != builtIn || code != http.StatusNoContent {
		t.Fatalf("OPTIONS Origin=%q: Access-Control-Allow-Origin = %q, status %d; want echo and 204", builtIn, got, code)
	}
	for _, origin := range []string{
		"https://help.orbtr.io",
		"https://node.example.com",
		"http://localhost:8080",
		"https://localhost:8081",
		"null",
		"",
	} {
		if got, code := corsHeader(t, h, http.MethodGet, origin); got != "" || code != http.StatusOK {
			t.Fatalf("GET Origin=%q: Access-Control-Allow-Origin = %q, status %d; want none and 200", origin, got, code)
		}
		if got, code := corsHeader(t, h, http.MethodOptions, origin); got != "" || code != http.StatusNoContent {
			t.Fatalf("OPTIONS Origin=%q: Access-Control-Allow-Origin = %q, status %d; want none and 204", origin, got, code)
		}
	}
}

// TestNewHandler_WithAllowedOriginsAdmitsExactlyTheList shows
// WithAllowedOrigins admits exactly the listed origins, byte-for-byte, in place
// of the built-in one, that calling it with no origins admits nothing, that the
// last option wins, and that the handler keeps the list it was built with when
// the caller's slice changes.
func TestNewHandler_WithAllowedOriginsAdmitsExactlyTheList(t *testing.T) {
	configured := []string{"https://node.example.com", ""}
	h := NewHandler(stubProvider{}, ports.DevPlatform(), time.Now(), WithAllowedOrigins(configured...))

	if got, _ := corsHeader(t, h, http.MethodGet, "https://node.example.com"); got != "https://node.example.com" {
		t.Fatalf("configured origin: Access-Control-Allow-Origin = %q, want echo", got)
	}
	for _, origin := range []string{
		"",
		"http://node.example.com",
		"https://node.example.com.example.org",
		"https://node.example.com:8443",
		"https://localhost:8080",
	} {
		if got, _ := corsHeader(t, h, http.MethodGet, origin); got != "" {
			t.Fatalf("Origin=%q admitted although it is not in the list: %q", origin, got)
		}
	}

	configured[0] = "https://other.example.com"
	if got, _ := corsHeader(t, h, http.MethodGet, "https://node.example.com"); got != "https://node.example.com" {
		t.Fatalf("handler followed a mutation of the caller's slice: %q", got)
	}

	none := NewHandler(stubProvider{}, ports.DevPlatform(), time.Now(), WithAllowedOrigins())
	for _, origin := range []string{"https://localhost:8080", "https://node.example.com"} {
		if got, _ := corsHeader(t, none, http.MethodGet, origin); got != "" {
			t.Fatalf("WithAllowedOrigins() admitted Origin=%q: %q", origin, got)
		}
	}

	last := NewHandler(stubProvider{}, ports.DevPlatform(), time.Now(),
		WithAllowedOrigins("https://first.example.com"),
		WithAllowedOrigins("https://second.example.com"),
	)
	if got, _ := corsHeader(t, last, http.MethodGet, "https://first.example.com"); got != "" {
		t.Fatalf("an earlier WithAllowedOrigins survived a later one: %q", got)
	}
	if got, _ := corsHeader(t, last, http.MethodGet, "https://second.example.com"); got != "https://second.example.com" {
		t.Fatalf("the last WithAllowedOrigins was not applied: %q", got)
	}
}

// TestNewHandlerFromRuntime_PassesOptions shows the convenience constructor
// admits only the built-in origin with no Option and forwards
// WithAllowedOrigins to NewHandler.
func TestNewHandlerFromRuntime_PassesOptions(t *testing.T) {
	metrics := func() map[string]interface{} { return map[string]interface{}{} }

	def := NewHandlerFromRuntime(aether.NodeID("vl1_test"), metrics, nil, "node.example.com", nil, time.Now())
	if got, _ := corsHeader(t, def, http.MethodGet, "https://localhost:8080"); got != "https://localhost:8080" {
		t.Fatalf("no Option: Access-Control-Allow-Origin = %q, want the built-in origin echoed", got)
	}
	for _, origin := range []string{"https://help.orbtr.io", "https://node.example.com"} {
		if got, _ := corsHeader(t, def, http.MethodGet, origin); got != "" {
			t.Fatalf("no Option: Origin=%q echoed as %q, want none", origin, got)
		}
	}

	configured := NewHandlerFromRuntime(aether.NodeID("vl1_test"), metrics, nil, "node.example.com", nil, time.Now(),
		WithAllowedOrigins("https://node.example.com"))
	if got, _ := corsHeader(t, configured, http.MethodGet, "https://node.example.com"); got != "https://node.example.com" {
		t.Fatalf("configured: Access-Control-Allow-Origin = %q, want echo", got)
	}
	for _, origin := range []string{"https://other.example.com", "https://localhost:8080"} {
		if got, _ := corsHeader(t, configured, http.MethodGet, origin); got != "" {
			t.Fatalf("configured: unlisted Origin=%q admitted: %q", origin, got)
		}
	}
}
