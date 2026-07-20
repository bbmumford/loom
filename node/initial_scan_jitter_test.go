/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"fmt"
	"testing"
	"time"
)

// TestInitialScanJitter_BoundedDeterministicSpread guards the fleet-reform
// desync. The first connect scan is delayed by a per-node deterministic jitter
// so a simultaneous fleet restart does not produce all-pairs dial races (which
// collapse gossip setup: measured 0 exchanges, members stuck at 2).
func TestInitialScanJitter_BoundedDeterministicSpread(t *testing.T) {
	// Deterministic: same ID → same jitter.
	id := "vl1_anchor_example_node"
	if initialScanJitter(id) != initialScanJitter(id) {
		t.Fatal("jitter is not deterministic for a fixed node ID")
	}

	// Bounded: always within [0, maxInitialScanJitter).
	for i := 0; i < 200; i++ {
		id := fmt.Sprintf("vl1_node_%d", i)
		j := initialScanJitter(id)
		if j < 0 || j >= maxInitialScanJitter {
			t.Fatalf("jitter %v for %s out of [0,%v)", j, id, maxInitialScanJitter)
		}
	}

	// Empty ID → 0 (feature no-op, never negative).
	if initialScanJitter("") != 0 {
		t.Fatal("empty ID must yield zero jitter")
	}

	// Spread: a realistic 11-node fleet must NOT collapse onto one instant.
	// Require at least a few distinct buckets so the reform genuinely staggers.
	fleet := []string{
		"vl1_bootstrap", "vl1_nodehstles", "vl1_nodeorbtr", "vl1_app_a", "vl1_app_b",
		"vl1_relay_a", "vl1_relay_b", "vl1_get", "vl1_help", "vl1_devices_a", "vl1_devices_b",
	}
	buckets := map[time.Duration]bool{}
	for _, id := range fleet {
		buckets[initialScanJitter(id)/time.Second] = true
	}
	if len(buckets) < 5 {
		t.Fatalf("11-node fleet spread into only %d second-buckets — too clustered to "+
			"desync the reform", len(buckets))
	}
}
