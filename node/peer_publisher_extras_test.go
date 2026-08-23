package node

import (
	"strings"
	"testing"
)

// The capability bag rides the signed PeerRecord. These pin the two properties
// that make it safe to publish: a caller cannot mutate what a publish is
// marshalling, and an oversized bag cannot take the record down with it.

func TestCapabilityExtrasAreCopiedFromTheCaller(t *testing.T) {
	// The caller's map must not remain aliased: publishOnce marshals outside
	// the lock, so a later mutation by the caller would be a data race against
	// proto.Marshal, not merely a stale read.
	p := &PeerPublisher{}
	supplied := map[string]string{"chip_models": "gen1_t100"}
	p.SetCapabilityExtras(supplied)

	supplied["chip_models"] = "mutated-after-the-call"

	if got := p.extras["chip_models"]; got != "gen1_t100" {
		t.Fatalf("publisher aliased the caller's map: %q", got)
	}
}

func TestAnOversizedBagIsDroppedRatherThanPublished(t *testing.T) {
	// 🔑 Records are capped. An oversized bag makes the WHOLE record
	// unpublishable, taking roles and addresses with it — the node would
	// vanish from the mesh over a telemetry field. Silence about capabilities
	// is recoverable; disappearing is not.
	p := &PeerPublisher{}
	p.SetCapabilityExtras(map[string]string{
		"huge": strings.Repeat("x", maxCapabilityExtrasBytes+1),
	})
	if p.extras != nil {
		t.Fatalf("an oversized bag must be dropped, kept %d entries", len(p.extras))
	}
}

func TestABagInsideTheBudgetIsKept(t *testing.T) {
	// Positive control. A limiter that dropped everything would satisfy the
	// test above while making the channel useless.
	p := &PeerPublisher{}
	p.SetCapabilityExtras(map[string]string{
		"chip_models":         "gen1_t100",
		"total_ternary_lanes": "16777216",
		"lane_capacity_fp64":  "524288",
	})
	if len(p.extras) != 3 {
		t.Fatalf("a small bag must survive; kept %v", p.extras)
	}
	if p.extras["lane_capacity_fp64"] != "524288" {
		t.Fatalf("values must round-trip: %v", p.extras)
	}
}

func TestClearingTheBagIsPossible(t *testing.T) {
	// A node whose capabilities become undiscovered must be able to withdraw
	// what it advertised before, or a stale claim outlives its evidence.
	p := &PeerPublisher{}
	p.SetCapabilityExtras(map[string]string{"chip_models": "gen1_t100"})
	p.SetCapabilityExtras(nil)
	if len(p.extras) != 0 {
		t.Fatalf("the bag must be clearable, kept %v", p.extras)
	}
}
