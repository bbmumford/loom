/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 */

package node

import (
	"testing"
	"time"

	"github.com/bbmumford/loom/netpolicy"
)

func TestGossipSignalsAggregation(t *testing.T) {
	// Empty map → a zero-RTT reading the synthesizer treats as "no measurement",
	// zero peers, and on-power battery (never restrict on battery for a server).
	empty := gossipSignals(netpolicy.LinkEthernet, nil)
	if empty.AvgRTT != 0 || empty.PeerCount != 0 || empty.BatteryLevel != -1 || empty.LinkType != netpolicy.LinkEthernet {
		t.Fatalf("empty signals wrong: %+v", empty)
	}

	// Mean of {10ms, 30ms} = 20ms over 2 peers.
	lat := map[string]time.Duration{"a": 10 * time.Millisecond, "b": 30 * time.Millisecond}
	s := gossipSignals(netpolicy.LinkEthernet, lat)
	if s.AvgRTT != 20*time.Millisecond || s.PeerCount != 2 {
		t.Fatalf("mean signals wrong: %+v", s)
	}
	if s.BatteryLevel != -1 {
		t.Fatal("battery must be on-power (-1) so the profile never restricts a server on battery")
	}
}

// TestGossipCadencePolicyBounds pins the envelope the gossip loop adopts when the
// adaptive cadence is on for a wired node: the Ethernet profile, which is what
// gossip.SetGossipCadence hands NewAdaptiveInterval. It differs from the fixed
// (GossipInterval, 2s, 60s) default — exactly why enabling it is an opt-in.
func TestGossipCadencePolicyBounds(t *testing.T) {
	p := netpolicy.NewPolicy(gossipCadenceLink, gossipCadenceSynth)
	base, min, max := netpolicy.GossipBounds(p)
	if base != 15*time.Second || min != 5*time.Second || max != 60*time.Second {
		t.Fatalf("ethernet gossip bounds = %v/%v/%v, want 15s/5s/60s", base, min, max)
	}
}
