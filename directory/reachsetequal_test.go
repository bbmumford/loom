/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package directory

import (
	"testing"

	"github.com/bbmumford/loom/ports"
)

// 🛑 THE COMPARATOR'S DISCRIMINATION IS ASSERTED HERE, DIRECTLY.
//
// reachSetEqual's key includes Priority. TestShadowParity cannot show that:
// it builds BOTH sides with newTestDirectory, so it compares a
// SwarmDirectory against another SwarmDirectory, the LAD side is not in the
// comparison, and a mutant of LADDirectory's reachPriority leaves it green.
// Asserting the comparator on hand-built address sets means no
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
	// Identical in protocol, address and scope; only the rank of the ws
	// entry differs. A key without Priority compares these EQUAL, so two
	// directories that would dial the peer over different transports
	// would report full parity.
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
