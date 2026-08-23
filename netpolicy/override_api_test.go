/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package netpolicy

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestOverrideAPI_SetGetClear(t *testing.T) {
	p := NewPolicy(LinkEthernet, SynthConfig{})
	api := &OverrideAPI{Policy: p, Prefix: "/mesh/netpolicy/override/"}

	// PUT an override for a flaky peer: cellular profile but with fanout pinned to 1.
	custom := ProfileFor(LinkCellular)
	custom.RumorFanout = 1
	body, _ := json.Marshal(custom)
	rr := httptest.NewRecorder()
	api.ServeHTTP(rr, httptest.NewRequest("PUT", "/mesh/netpolicy/override/peer-flaky", strings.NewReader(string(body))))
	if rr.Code != http.StatusNoContent {
		t.Fatalf("PUT override: %d", rr.Code)
	}

	// GET reflects the override.
	rr = httptest.NewRecorder()
	api.ServeHTTP(rr, httptest.NewRequest("GET", "/mesh/netpolicy/override/peer-flaky", nil))
	var got NetworkProfile
	json.Unmarshal(rr.Body.Bytes(), &got)
	if got.RumorFanout != 1 {
		t.Fatalf("GET must return the override (fanout 1), got %d", got.RumorFanout)
	}

	// A peer with no override gets the global profile.
	rr = httptest.NewRecorder()
	api.ServeHTTP(rr, httptest.NewRequest("GET", "/mesh/netpolicy/override/peer-other", nil))
	json.Unmarshal(rr.Body.Bytes(), &got)
	if got.RumorFanout != p.Profile().RumorFanout {
		t.Fatalf("un-overridden peer must get the global profile, got fanout %d", got.RumorFanout)
	}

	// DELETE clears it → back to the global profile.
	rr = httptest.NewRecorder()
	api.ServeHTTP(rr, httptest.NewRequest("DELETE", "/mesh/netpolicy/override/peer-flaky", nil))
	if rr.Code != http.StatusNoContent {
		t.Fatalf("DELETE override: %d", rr.Code)
	}
	rr = httptest.NewRecorder()
	api.ServeHTTP(rr, httptest.NewRequest("GET", "/mesh/netpolicy/override/peer-flaky", nil))
	json.Unmarshal(rr.Body.Bytes(), &got)
	if got.RumorFanout != p.Profile().RumorFanout {
		t.Fatalf("cleared peer must revert to the global profile, got fanout %d", got.RumorFanout)
	}
}

func TestOverrideAPI_Rejections(t *testing.T) {
	api := &OverrideAPI{Policy: NewPolicy(LinkWiFi, SynthConfig{}), Prefix: "/p/"}
	// Missing peer id.
	rr := httptest.NewRecorder()
	api.ServeHTTP(rr, httptest.NewRequest("GET", "/p/", nil))
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("missing peer must 400, got %d", rr.Code)
	}
	// Malformed body on PUT.
	rr = httptest.NewRecorder()
	api.ServeHTTP(rr, httptest.NewRequest("PUT", "/p/peer1", strings.NewReader("{not json")))
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("malformed profile must 400, got %d", rr.Code)
	}
	// Unsupported method.
	rr = httptest.NewRecorder()
	api.ServeHTTP(rr, httptest.NewRequest("POST", "/p/peer1", nil))
	if rr.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST must 405, got %d", rr.Code)
	}
	// No policy → 503.
	rr = httptest.NewRecorder()
	(&OverrideAPI{Prefix: "/p/"}).ServeHTTP(rr, httptest.NewRequest("GET", "/p/peer1", nil))
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("nil policy must 503, got %d", rr.Code)
	}
}
