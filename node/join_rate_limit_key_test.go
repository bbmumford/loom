/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
)

// joinRequest is a VL1 bootstrap request as the HTTP server hands it to
// handleVL1Upgrade: remoteAddr is the connection's peer, headers are the
// caller's.
func joinRequest(remoteAddr string, headers map[string]string) *http.Request {
	req := httptest.NewRequest(http.MethodGet, "/mesh/vl1", nil)
	req.RemoteAddr = remoteAddr
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	return req
}

// joinStatus runs one bootstrap request against rt and returns its status.
// A request the rate limit admits stops at the missing Upgrade header (400);
// one it refuses answers 429.
func joinStatus(rt *Runtime, req *http.Request) int {
	rec := httptest.NewRecorder()
	rt.handleVL1Upgrade(rec, req)
	return rec.Code
}

// A caller connecting directly writes its own X-Forwarded-For. Rotating that
// header must not mint a fresh rate-limit key per request: the requests are
// charged to the connection's peer address, so they cannot fill the limiter,
// and a new joiner from another address is still admitted.
func TestJoinRateLimitKeysADirectCallerOnItsPeerAddressNotItsForwardedHeader(t *testing.T) {
	rt := &Runtime{}
	for i := 0; i < snapshotRateLimitMaxIPs+10; i++ {
		joinStatus(rt, joinRequest("203.0.113.9:40000", map[string]string{
			"X-Forwarded-For": fmt.Sprintf("10.%d.%d.%d", i>>16&0xff, i>>8&0xff, i&0xff),
		}))
	}
	if got := joinStatus(rt, joinRequest("203.0.113.9:40001", map[string]string{"X-Forwarded-For": "10.250.0.1"})); got != http.StatusTooManyRequests {
		t.Fatalf("the rotating caller's own budget: status %d, want 429", got)
	}
	if got := joinStatus(rt, joinRequest("198.51.100.7:5000", nil)); got == http.StatusTooManyRequests {
		t.Fatalf("a new joiner from another address was refused 429 after another caller rotated X-Forwarded-For")
	}
}

// Behind the trusted edge every request arrives from the proxy's address, and
// the edge names the client in Fly-Client-IP: two clients behind the proxy
// have budgets of their own.
func TestJoinRateLimitKeysAProxiedCallerOnTheClientTheTrustedEdgeNames(t *testing.T) {
	rt := &Runtime{}
	for i := 0; i < 6; i++ {
		joinStatus(rt, joinRequest("[fdaa::3]:443", map[string]string{"Fly-Client-IP": "192.0.2.10"}))
	}
	if got := joinStatus(rt, joinRequest("[fdaa::3]:443", map[string]string{"Fly-Client-IP": "192.0.2.10"})); got != http.StatusTooManyRequests {
		t.Fatalf("the first client's own budget: status %d, want 429", got)
	}
	if got := joinStatus(rt, joinRequest("[fdaa::3]:443", map[string]string{"Fly-Client-IP": "192.0.2.11"})); got == http.StatusTooManyRequests {
		t.Fatalf("a second client behind the edge was refused 429 on the first client's budget")
	}
}

// The snapshot handler keys its rate limit the same way: a rotating
// X-Forwarded-For from one direct caller does not lock out another caller.
// An admitted request stops at the missing X-Mesh-Auth (401).
func TestSnapshotRateLimitKeysADirectCallerOnItsPeerAddressNotItsForwardedHeader(t *testing.T) {
	rt := &Runtime{}
	h := rt.SnapshotHandler()
	status := func(req *http.Request) int {
		rec := httptest.NewRecorder()
		h(rec, req)
		return rec.Code
	}
	for i := 0; i < snapshotRateLimitMaxIPs+10; i++ {
		status(joinRequest("203.0.113.9:40000", map[string]string{
			"X-Forwarded-For": fmt.Sprintf("10.%d.%d.%d", i>>16&0xff, i>>8&0xff, i&0xff),
		}))
	}
	if got := status(joinRequest("198.51.100.7:5000", nil)); got == http.StatusTooManyRequests {
		t.Fatalf("a new caller from another address was refused 429 after another caller rotated X-Forwarded-For")
	}
}

// When the limiter holds as many keys as it can, a new key's request is
// refused and every key it already holds keeps its own budget: abuse limits
// fail closed for the newcomer and evict no one.
func TestJoinRateLimitWhenFullRefusesTheNewKeyAndEvictsNoOne(t *testing.T) {
	rt := &Runtime{}
	addr := func(i int) string { return fmt.Sprintf("198.18.%d.%d:7000", i>>8&0xff, i&0xff) }
	for i := 0; i < snapshotRateLimitMaxIPs; i++ {
		if got := joinStatus(rt, joinRequest(addr(i), nil)); got == http.StatusTooManyRequests {
			t.Fatalf("key %d refused before the limiter was full", i)
		}
	}
	if got := joinStatus(rt, joinRequest("192.0.2.200:7000", nil)); got != http.StatusTooManyRequests {
		t.Fatalf("a new key on a full limiter: status %d, want 429", got)
	}
	for _, i := range []int{0, snapshotRateLimitMaxIPs / 2, snapshotRateLimitMaxIPs - 1} {
		if got := joinStatus(rt, joinRequest(addr(i), nil)); got == http.StatusTooManyRequests {
			t.Fatalf("key %d already held was refused after a new key arrived on a full limiter", i)
		}
	}
}
