/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package directory

import (
	"testing"

	"github.com/bbmumford/loom/ports"
)

// 🛑 THIS TEST EXISTS BECAUSE THE OBVIOUS WAY TO VERIFY THE COMPARATOR
// PROVED NOTHING, AND THAT IS WORTH RECORDING AT THE SITE.
//
// I strengthened reachSetEqual's key to include Priority (#M-547), then
// mutated LADDirectory's reachPriority and expected TestShadowParity to
// go red. It stayed GREEN — because TestShadowParity builds BOTH sides
// with newTestDirectory, so it compares a SwarmDirectory against another
// SwarmDirectory and the LAD side is not in the comparison at all.
//
// That is the same single-implementation-oracle problem the second
// implementation was built to remove, still present in that test. So the
// comparator's own discrimination is asserted HERE, directly, where no
// fixture-construction choice can quietly remove the axis.

func reachAddr(proto, addr, scope string, prio int) ports.ReachAddress {
	return ports.ReachAddress{
		Protocol: proto, Address: addr, Scope: scope, Priority: prio,
	}
}

func TestReachSetEqualDiscriminatesOnPriority(t *testing.T) {
	base := []ports.ReachAddress{
		reachAddr("noise-udp", "[fdaa:0:1234:a7b::2]:41641", "private", 0),
		reachAddr("ws", "203.0.113.7:443", "public", 1),
	}
	// Identical in every field the key USED to cover; only the rank of the
	// ws entry differs. Before #M-547 this compared EQUAL, so two
	// directories that would dial the peer over different transports
	// reported full parity.
	ranked := []ports.ReachAddress{
		reachAddr("noise-udp", "[fdaa:0:1234:a7b::2]:41641", "private", 0),
		reachAddr("ws", "203.0.113.7:443", "public", 7),
	}

	if reachSetEqual(base, ranked) {
		t.Fatal("reachSetEqual reports parity for two sets that rank the same " +
			"address differently — both implementations sort ascending on " +
			"Priority, so this is a reordered dial-candidate list being " +
			"certified as identical")
	}

	// Control: the comparator must still report parity for genuinely equal
	// sets, or the assertion above is satisfied by a comparator that simply
	// always disagrees.
	same := []ports.ReachAddress{
		reachAddr("ws", "203.0.113.7:443", "public", 1),
		reachAddr("noise-udp", "[fdaa:0:1234:a7b::2]:41641", "private", 0),
	}
	if !reachSetEqual(base, same) {
		t.Fatal("control failed: reachSetEqual denies parity for equal sets " +
			"differing only in order, so the Priority check above proves nothing")
	}
}

// The fields the key covered before must keep discriminating — adding
// Priority must not have displaced them.
func TestReachSetEqualStillDiscriminatesOnTheOriginalFields(t *testing.T) {
	base := []ports.ReachAddress{reachAddr("ws", "203.0.113.7:443", "public", 1)}

	cases := map[string][]ports.ReachAddress{
		"protocol": {reachAddr("grpc", "203.0.113.7:443", "public", 1)},
		"address":  {reachAddr("ws", "203.0.113.8:443", "public", 1)},
		"scope":    {reachAddr("ws", "203.0.113.7:443", "private", 1)},
		"length":   {},
	}
	for field, other := range cases {
		if reachSetEqual(base, other) {
			t.Errorf("reachSetEqual ignores a difference in %s", field)
		}
	}
}
