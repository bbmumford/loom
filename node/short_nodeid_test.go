/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"strings"
	"testing"
)

// Node IDs reach this package from the wire: ingestPeerReachRecord takes one
// from the X-VL1-Node-ID response header, and aether.NormalizeNodeID only
// lowercases and trims it — there is no length or prefix check. Every log line
// that formatted such an id with a fixed-width slice panicked on anything
// shorter than the slice width, and a panic inside the bootstrap path takes the
// node down during Initialize rather than degrading.
//
// truncID is the length-safe helper the package already used in most places;
// these tests pin its boundary behaviour, since it is now what stands between a
// hostile header and a slice panic at two dozen call sites.

func TestTruncIDIsSafeAtAndBelowItsBoundary(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   string
	}{
		{"empty", ""},
		{"one char", "a"},
		{"eleven", strings.Repeat("a", 11)},
		{"exactly twelve", strings.Repeat("a", 12)},
		{"thirteen", strings.Repeat("a", 13)},
		{"multibyte shorter than the width", "日本語"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("truncID(%q) panicked: %v", tc.in, r)
				}
			}()
			got := truncID(tc.in)
			if len(tc.in) <= 12 && got != tc.in {
				t.Errorf("truncID(%q) = %q, want the input unchanged at or below the "+
					"boundary", tc.in, got)
			}
			if len(tc.in) > 12 && !strings.HasSuffix(got, "...") {
				t.Errorf("truncID(%q) = %q, want a truncation marker", tc.in, got)
			}
		})
	}
}

// 🔴 THE REACHABLE PATH. ingestPeerReachRecord logs the node ID on its
// fetch-failure branch, which any unreachable host reaches — so a peer that
// answers the VL1 upgrade with a short X-VL1-Node-ID header and then refuses
// the reach fetch was enough to panic out of bootstrap.
//
// 🔬 The fixture uses a deliberately short id AND a host that cannot be dialled:
// both are needed. A long id never reaches the slice bug, and a reachable host
// would take a different branch.
func TestAShortWireNodeIDDoesNotPanicTheBootstrapReachIngest(t *testing.T) {
	rt := &Runtime{}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("ingestPeerReachRecord panicked on a short wire-supplied node ID: %v "+
				"— this runs during Initialize, so a peer returning a short "+
				"X-VL1-Node-ID header takes the node down at boot", r)
		}
	}()

	// 127.0.0.1:1 is reserved and refuses immediately, so the fetch fails fast
	// and the failure log — the site that formatted the id — runs.
	rt.ingestPeerReachRecord(context.Background(), "127.0.0.1:1", "abc", nil)
}

// The empty-id guard must still short-circuit before any formatting.
//
// ⚠ WHAT THIS CANNOT ASSERT. Now that the formatting is length-safe, removing
// the guard entirely no longer panics — so this test cannot distinguish a
// short-circuit from a full pass through the function. What the guard still
// buys is not fetching https://host/mesh/reach/ with an empty path segment,
// and the HTTP client is built inside the function with no seam to observe the
// request through. The guard is kept for that reason, not for panic-safety.
func TestAnEmptyWireNodeIDIsRejectedBeforeAnyFormatting(t *testing.T) {
	rt := &Runtime{}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("an empty node ID panicked: %v", r)
		}
	}()

	rt.ingestPeerReachRecord(context.Background(), "127.0.0.1:1", "", nil)
}
