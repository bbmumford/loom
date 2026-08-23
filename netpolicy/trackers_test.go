/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package netpolicy

import (
	"testing"
	"time"
)

func TestRTTTracker_AvgAndRisingTrend(t *testing.T) {
	tr := NewRTTTracker(0.3, 0.05)
	// Settle on a low RTT.
	for i := 0; i < 20; i++ {
		tr.Observe(20 * time.Millisecond)
	}
	if tr.Avg() < 15*time.Millisecond || tr.Avg() > 25*time.Millisecond {
		t.Fatalf("avg should sit near 20ms, got %s", tr.Avg())
	}
	if tr.RisingBy(1.2) {
		t.Fatal("a stable RTT must not read as rising")
	}

	// Now a sustained spike: the fast EMA climbs above the slow baseline.
	for i := 0; i < 10; i++ {
		tr.Observe(200 * time.Millisecond)
	}
	if !tr.RisingBy(1.2) {
		t.Fatalf("a sustained spike must trip RisingBy: avg=%s baseline=%s", tr.Avg(), tr.Baseline())
	}
}

func TestRTTTracker_NoBaselineNoTrend(t *testing.T) {
	tr := NewRTTTracker(0, 0) // defaults
	if tr.RisingBy(1.2) {
		t.Fatal("with no samples there is no trend")
	}
	tr.Observe(0) // ignored
	if tr.Avg() != 0 {
		t.Fatal("a non-positive sample must be ignored")
	}
}

func TestLossTracker_SlidingRate(t *testing.T) {
	l := NewLossTracker(4)
	if l.Rate() != 0 {
		t.Fatal("no observations → 0 loss")
	}
	// 2 missed of 4 → 0.5.
	l.Observe(true)
	l.Observe(false)
	l.Observe(true)
	l.Observe(false)
	if l.Rate() != 0.5 {
		t.Fatalf("expected 0.5 loss, got %v", l.Rate())
	}
	// The window slides: four fresh acks push the misses out.
	l.Observe(true)
	l.Observe(true)
	l.Observe(true)
	l.Observe(true)
	if l.Rate() != 0 {
		t.Fatalf("window should have slid to all-acked, got %v", l.Rate())
	}
}

// The trackers feed a synthesizer end-to-end: a clean link stays put; a degrading one (RTT rising +
// loss climbing + battery draining) trips the predictive restriction.
func TestTrackers_DriveSynthesizerPredictive(t *testing.T) {
	rtt := NewRTTTracker(0.3, 0.05)
	loss := NewLossTracker(10)
	s := NewSynthesizer(LinkWiFi, SynthConfig{RTTRiseFactor: 1.2, LossThreshold: 0.1})

	// Healthy wifi for a while.
	for i := 0; i < 12; i++ {
		rtt.Observe(20 * time.Millisecond)
		loss.Observe(true)
		s.Next(NetworkSignals{LinkType: LinkWiFi, AvgRTT: rtt.Avg(), LossRate: loss.Rate(), LinkStableSince: time.Hour})
	}
	if s.Current() != LinkWiFi {
		t.Fatalf("healthy wifi should stay wifi, got %v", s.Current())
	}

	// Degrade: RTT spikes and ACKs start missing while the battery drains.
	for i := 0; i < 8; i++ {
		rtt.Observe(300 * time.Millisecond)
		loss.Observe(false)
	}
	_, changed := s.Next(NetworkSignals{
		LinkType: LinkWiFi, AvgRTT: rtt.Avg(), LossRate: loss.Rate(),
		BatteryDraining: true, LinkStableSince: time.Hour,
	})
	if !changed || s.Current() == LinkWiFi {
		t.Fatalf("degrading signals should predictively restrict off wifi: current=%v changed=%v rtt=%s loss=%.2f",
			s.Current(), changed, rtt.Avg(), loss.Rate())
	}
}
