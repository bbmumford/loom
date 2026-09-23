/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package streams

import (
	"testing"

	"github.com/ORBTR/aether"
)

// The IDs are wire values both ends of a session must agree on, so each is
// pinned to its number rather than only to being distinct.
func TestStreamIDsAreFixedWireValues(t *testing.T) {
	for _, tc := range []struct {
		name string
		got  uint64
		want uint64
	}{
		{"StreamGossip", StreamGossip, 0},
		{"StreamRPC", StreamRPC, 1},
		{"StreamKeepalive", StreamKeepalive, 2},
		{"StreamControl", StreamControl, 3},
		{"StreamReconcile", StreamReconcile, 4},
		{"StreamSwarm", StreamSwarm, 100},
		{"StreamMeshDB", StreamMeshDB, 101},
	} {
		if tc.got != tc.want {
			t.Errorf("%s = %d, want %d", tc.name, tc.got, tc.want)
		}
	}
}

// Per-peer application streams sit in aether's application band (>= 100) and
// never share an ID with each other.
func TestApplicationStreamsAreDistinctAndInTheApplicationBand(t *testing.T) {
	const applicationBand = 100
	seen := map[uint64]string{}
	for name, id := range map[string]uint64{
		"StreamSwarm":  StreamSwarm,
		"StreamMeshDB": StreamMeshDB,
	} {
		if id < applicationBand {
			t.Errorf("%s = %d, below the application band %d", name, id, applicationBand)
		}
		if other, dup := seen[id]; dup {
			t.Errorf("%s and %s share stream ID %d", name, other, id)
		}
		seen[id] = name
	}
}

func TestDefaultLatencyClassKeepsSwarmInteractive(t *testing.T) {
	if got := DefaultLatencyClass(StreamSwarm); got != aether.ClassINTERACTIVE {
		t.Fatalf("DefaultLatencyClass(StreamSwarm) = %v, want ClassINTERACTIVE", got)
	}
}
