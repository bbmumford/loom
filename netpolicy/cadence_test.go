/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package netpolicy

import "testing"

func TestGossipCadence_TracksLiveProfile(t *testing.T) {
	p := NewPolicy(LinkEthernet, SynthConfig{})
	cad := GossipCadence(p)
	fresh := FreshnessCadence(p)

	if cad() != ProfileFor(LinkEthernet).GossipBase {
		t.Fatalf("cadence should start at the ethernet base, got %v", cad())
	}
	if fresh() != ProfileFor(LinkEthernet).FreshnessProbeMax {
		t.Fatalf("freshness should start at the ethernet max, got %v", fresh())
	}

	// A profile change is reflected on the NEXT read of the same seam (no loop restart).
	p.Update(NetworkSignals{LinkType: LinkCellular, LinkStableSince: 1 << 40})
	if cad() != ProfileFor(LinkCellular).GossipBase {
		t.Fatalf("cadence must follow the live profile to cellular, got %v", cad())
	}
	if cad() <= ProfileFor(LinkEthernet).GossipBase {
		t.Fatal("cellular cadence must be wider than ethernet")
	}
}

func TestGossipCadence_NilPolicyYieldsZero(t *testing.T) {
	if GossipCadence(nil)() != 0 || FreshnessCadence(nil)() != 0 {
		t.Fatal("a nil policy must yield 0 (loop uses its own default)")
	}
}

func TestGossipBounds_MatchesProfileTriple(t *testing.T) {
	p := NewPolicy(LinkEthernet, SynthConfig{})
	base, min, max := GossipBounds(p)
	eth := ProfileFor(LinkEthernet)
	if base != eth.GossipBase || min != eth.GossipMin || max != eth.GossipMax {
		t.Fatalf("bounds must equal the profile triple: got %v/%v/%v", base, min, max)
	}
	// Ordering invariant the adaptive interval relies on: min <= base <= max.
	if !(min <= base && base <= max) {
		t.Fatalf("bounds must be ordered min<=base<=max: %v/%v/%v", min, base, max)
	}

	// Follows the live profile to cellular (wider envelope).
	p.Update(NetworkSignals{LinkType: LinkCellular, LinkStableSince: 1 << 40})
	cBase, cMin, cMax := GossipBounds(p)
	cell := ProfileFor(LinkCellular)
	if cBase != cell.GossipBase || cMin != cell.GossipMin || cMax != cell.GossipMax {
		t.Fatalf("bounds must follow the profile to cellular: %v/%v/%v", cBase, cMin, cMax)
	}
	if !(cBase > base && cMax >= max) {
		t.Fatal("cellular envelope must be wider than ethernet")
	}

	// nil → zero triple.
	if b, mn, mx := GossipBounds(nil); b != 0 || mn != 0 || mx != 0 {
		t.Fatal("nil policy must yield the zero triple")
	}
}
