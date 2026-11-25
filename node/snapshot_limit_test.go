/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
 */
package node

import (
	"fmt"
	"testing"
)

// TestSnapshotLimitRefusesACallerPastItsBurst pins the snapshot limit the runtime declares: five requests from one
// client IP, then a refusal, while another IP keeps its own five. A limiter whose limit was never declared would
// admit every request.
func TestSnapshotLimitRefusesACallerPastItsBurst(t *testing.T) {
	rt := &Runtime{}
	for i := 0; i < 5; i++ {
		if !rt.checkSnapshotRateLimit("198.51.100.7") {
			t.Fatalf("request %d of the burst of five was refused", i+1)
		}
	}
	if rt.checkSnapshotRateLimit("198.51.100.7") {
		t.Fatal("a sixth request inside the same second was admitted: the burst of five did not bound it")
	}
	if !rt.checkSnapshotRateLimit("198.51.100.8") {
		t.Fatal("another IP was refused: one caller's burst spent another's")
	}
}

// TestSnapshotLimitWhenFullRefusesTheNewIPAndKeepsEveryHeldBudget pins the bound: with snapshotRateLimitMaxIPs IPs
// holding buckets, a new IP is refused, and an IP already holding one is still served from its own budget.
func TestSnapshotLimitWhenFullRefusesTheNewIPAndKeepsEveryHeldBudget(t *testing.T) {
	rt := &Runtime{}
	for i := 0; i < snapshotRateLimitMaxIPs; i++ {
		if !rt.checkSnapshotRateLimit(fmt.Sprintf("10.%d.%d.%d", i>>16&0xff, i>>8&0xff, i&0xff)) {
			t.Fatalf("IP %d of %d was refused before the limiter was full", i+1, snapshotRateLimitMaxIPs)
		}
	}
	if rt.checkSnapshotRateLimit("198.51.100.9") {
		t.Fatal("a new IP was admitted with the limiter full: it grows without bound")
	}
	if !rt.checkSnapshotRateLimit("10.0.0.0") {
		t.Fatal("an IP holding a bucket was refused once the limiter filled: its budget was evicted")
	}
}
