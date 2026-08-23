/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package netpolicy

import (
	"os"
	"path/filepath"
	"testing"
)

func writeBattery(t *testing.T, root, name, capacity, status string) {
	t.Helper()
	dir := filepath.Join(root, name)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	os.WriteFile(filepath.Join(dir, "capacity"), []byte(capacity), 0o600)
	os.WriteFile(filepath.Join(dir, "status"), []byte(status), 0o600)
}

func TestSysfsBattery_ReadsCapacityAndStatus(t *testing.T) {
	root := t.TempDir()
	// A non-battery supply (AC adapter) must be skipped; the battery is read.
	writeBattery(t, root, "AC", "", "")
	writeBattery(t, root, "BAT0", "42\n", "Discharging\n")

	b := &SysfsBattery{Root: root}
	sig, ok := b.Read()
	if !ok {
		t.Fatal("a present battery must read ok")
	}
	if sig.Level < 0.41 || sig.Level > 0.43 {
		t.Fatalf("level ~0.42 expected, got %v", sig.Level)
	}
	if !sig.Draining {
		t.Fatal("Discharging status must set Draining")
	}
}

func TestSysfsBattery_NoBattery(t *testing.T) {
	// Empty tree → no battery.
	if _, ok := (&SysfsBattery{Root: t.TempDir()}).Read(); ok {
		t.Fatal("an empty power-supply tree must report no battery")
	}
	// Absent tree (non-Linux) → no battery, no panic.
	if _, ok := (&SysfsBattery{Root: filepath.Join(t.TempDir(), "absent")}).Read(); ok {
		t.Fatal("an absent tree must report no battery")
	}
}

func TestApplyBattery_SetsSignalsOrACSentinel(t *testing.T) {
	root := t.TempDir()
	writeBattery(t, root, "BAT0", "80", "Discharging")

	sig := ApplyBattery(NetworkSignals{}, &SysfsBattery{Root: root})
	if sig.BatteryLevel < 0.79 || sig.BatteryLevel > 0.81 || !sig.BatteryDraining {
		t.Fatalf("battery signals not applied: %+v", sig)
	}

	// No source / no battery → AC sentinel (negative level), not draining.
	acSig := ApplyBattery(NetworkSignals{}, nil)
	if acSig.BatteryLevel >= 0 || acSig.BatteryDraining {
		t.Fatalf("no-battery must set the AC sentinel: %+v", acSig)
	}
	noBat := ApplyBattery(NetworkSignals{}, &SysfsBattery{Root: t.TempDir()})
	if noBat.BatteryLevel >= 0 {
		t.Fatalf("absent battery must set the AC sentinel, got %v", noBat.BatteryLevel)
	}
}

// The battery reading drives the synthesizer's predictive rule end to end.
func TestBattery_FeedsPredictiveRestriction(t *testing.T) {
	root := t.TempDir()
	writeBattery(t, root, "BAT0", "15", "Discharging")
	s := NewSynthesizer(LinkWiFi, SynthConfig{RTTRiseFactor: 1.2, LossThreshold: 0.1})

	// Establish a low RTT baseline.
	for i := 0; i < 4; i++ {
		s.Next(NetworkSignals{LinkType: LinkWiFi, AvgRTT: 20_000_000, LinkStableSince: 1 << 40})
	}
	// Degrading RTT + loss + a draining battery (read from sysfs) → predictive restrict off wifi.
	sig := ApplyBattery(NetworkSignals{LinkType: LinkWiFi, AvgRTT: 300_000_000, LossRate: 0.2, LinkStableSince: 1 << 40}, &SysfsBattery{Root: root})
	if _, changed := s.Next(sig); !changed || s.Current() == LinkWiFi {
		t.Fatalf("draining battery + degrading link must predictively restrict: current=%v changed=%v", s.Current(), changed)
	}
}
