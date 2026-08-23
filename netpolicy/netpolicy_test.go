/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package netpolicy

import (
	"testing"
	"time"
)

func TestProfileFor_CellularIsPacedAndConservative(t *testing.T) {
	eth := ProfileFor(LinkEthernet)
	cell := ProfileFor(LinkCellular)
	if cell.GossipBase <= eth.GossipBase {
		t.Fatal("cellular must gossip less often than ethernet")
	}
	if cell.RumorFanout >= eth.RumorFanout {
		t.Fatal("cellular must fan out to fewer peers")
	}
	if cell.ReconcilePacing != 8*1024 {
		t.Fatalf("cellular reconcile pacing should default to 8 KB/s, got %d", cell.ReconcilePacing)
	}
	if eth.ReconcilePacing != 0 {
		t.Fatalf("ethernet must be unpaced, got %d", eth.ReconcilePacing)
	}
}

func TestSynth_LooseningIsImmediate(t *testing.T) {
	s := NewSynthesizer(LinkCellular, SynthConfig{})
	// Ethernet appears; even with LinkStableSince = 0, loosening takes effect at once.
	prof, changed := s.Next(NetworkSignals{LinkType: LinkEthernet, LinkStableSince: 0})
	if !changed || s.Current() != LinkEthernet {
		t.Fatalf("loosening to ethernet must be immediate, current=%v changed=%v", s.Current(), changed)
	}
	if prof.RumorFanout != ProfileFor(LinkEthernet).RumorFanout {
		t.Fatal("profile must switch to ethernet")
	}
}

func TestSynth_TighteningRequiresConfirmation(t *testing.T) {
	s := NewSynthesizer(LinkEthernet, SynthConfig{ConfirmWindow: 60 * time.Second})

	// Cellular appears but has only held 10s — hysteresis keeps ethernet.
	if _, changed := s.Next(NetworkSignals{LinkType: LinkCellular, LinkStableSince: 10 * time.Second}); changed {
		t.Fatal("a briefly-held tighter link must not flip the profile")
	}
	if s.Current() != LinkEthernet {
		t.Fatalf("still ethernet expected, got %v", s.Current())
	}

	// Once it has held past the window, the tightening takes effect.
	if _, changed := s.Next(NetworkSignals{LinkType: LinkCellular, LinkStableSince: 90 * time.Second}); !changed {
		t.Fatal("a sustained tighter link must switch")
	}
	if s.Current() != LinkCellular {
		t.Fatalf("cellular expected after confirmation, got %v", s.Current())
	}
}

func TestSynth_PredictiveRestrictBeforeLinkChange(t *testing.T) {
	s := NewSynthesizer(LinkWiFi, SynthConfig{RTTRiseFactor: 1.2, LossThreshold: 0.1, ConfirmWindow: 60 * time.Second})

	// Establish a low RTT baseline over a couple of clean readings on stable wifi.
	for i := 0; i < 4; i++ {
		s.Next(NetworkSignals{LinkType: LinkWiFi, AvgRTT: 20 * time.Millisecond, LinkStableSince: 5 * time.Minute})
	}
	if s.Current() != LinkWiFi {
		t.Fatalf("should still be on wifi, got %v", s.Current())
	}

	// Now RTT spikes, loss climbs, battery draining — the predictive rule pulls one step tighter
	// (wifi→vpn) even though the link still reports wifi and vpn "held" long enough to confirm.
	_, changed := s.Next(NetworkSignals{
		LinkType: LinkWiFi, AvgRTT: 200 * time.Millisecond, LossRate: 0.2,
		BatteryDraining: true, LinkStableSince: 5 * time.Minute,
	})
	if !changed || s.Current() != LinkVPN {
		t.Fatalf("predictive rule should restrict wifi→vpn early, got current=%v changed=%v", s.Current(), changed)
	}
}

func TestPolicy_PeerOverrideAndSubscribe(t *testing.T) {
	p := NewPolicy(LinkEthernet, SynthConfig{})

	var fired []NetworkProfile
	p.Subscribe(func(np NetworkProfile) { fired = append(fired, np) })

	// A no-op update (still ethernet, immediately) does not change the profile → no notification.
	p.Update(NetworkSignals{LinkType: LinkEthernet})
	if len(fired) != 0 {
		t.Fatalf("unchanged profile must not notify, got %d", len(fired))
	}

	// Loosen is a no-op here; force a change by loosening from a tighter start instead.
	p2 := NewPolicy(LinkCellular, SynthConfig{})
	var n int
	p2.Subscribe(func(NetworkProfile) { n++ })
	p2.Update(NetworkSignals{LinkType: LinkEthernet})
	if n != 1 {
		t.Fatalf("a profile change must notify exactly once, got %d", n)
	}

	// Per-peer override wins over the global profile; clearing restores it.
	custom := ProfileFor(LinkCellular)
	custom.RumorFanout = 1
	p.SetPeerOverride("flaky-peer", custom)
	if p.PeerOverride("flaky-peer").RumorFanout != 1 {
		t.Fatal("peer override must apply")
	}
	if p.PeerOverride("other-peer").RumorFanout != p.Profile().RumorFanout {
		t.Fatal("a peer with no override must get the global profile")
	}
	p.SetPeerOverride("flaky-peer", NetworkProfile{})
	if p.PeerOverride("flaky-peer").RumorFanout != p.Profile().RumorFanout {
		t.Fatal("clearing an override must restore the global profile")
	}
}
