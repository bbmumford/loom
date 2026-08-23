/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package netpolicy

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// BatterySignals is a host battery reading: Level is 0..1, Draining is true when the host is running
// on battery and discharging. This is the I/O adapter side of the policy — the Synthesizer itself
// stays pure and consumes these values through NetworkSignals.BatteryLevel / BatteryDraining.
type BatterySignals struct {
	Level    float64
	Draining bool
}

// BatterySource reads the host battery. A host with no battery (AC-only server, unsupported platform)
// returns ok=false, and the caller should leave the signal fields at their "on AC" defaults so the
// predictive rule does not back off for a machine that never drains.
type BatterySource interface {
	Read() (BatterySignals, bool)
}

// SysfsBattery reads the Linux power-supply sysfs (default /sys/class/power_supply). It is safe to
// construct on any platform: where the tree is absent (macOS, Windows, a server) ReadDir fails and
// Read returns ok=false. Root is injectable so the reader is testable against a fixture tree.
type SysfsBattery struct {
	Root string
}

// NewSysfsBattery returns a reader over the standard Linux power-supply path.
func NewSysfsBattery() *SysfsBattery { return &SysfsBattery{Root: "/sys/class/power_supply"} }

// Read returns the first battery's level + draining state, or ok=false when no battery is present.
func (b *SysfsBattery) Read() (BatterySignals, bool) {
	entries, err := os.ReadDir(b.Root)
	if err != nil {
		return BatterySignals{}, false
	}
	for _, e := range entries {
		if !strings.HasPrefix(strings.ToUpper(e.Name()), "BAT") {
			continue
		}
		dir := filepath.Join(b.Root, e.Name())
		capRaw, cerr := os.ReadFile(filepath.Join(dir, "capacity"))
		if cerr != nil {
			continue
		}
		pct, perr := strconv.Atoi(strings.TrimSpace(string(capRaw)))
		if perr != nil {
			continue
		}
		if pct < 0 {
			pct = 0
		}
		if pct > 100 {
			pct = 100
		}
		statusRaw, _ := os.ReadFile(filepath.Join(dir, "status"))
		return BatterySignals{
			Level:    float64(pct) / 100,
			Draining: strings.EqualFold(strings.TrimSpace(string(statusRaw)), "Discharging"),
		}, true
	}
	return BatterySignals{}, false
}

// ApplyBattery folds a battery reading into a signal vector: on a real battery it sets Level +
// Draining; with no battery it marks BatteryLevel negative (the "on AC power" sentinel the signals
// document), leaving the predictive rule unconstrained by power. Returns the updated signals.
func ApplyBattery(sig NetworkSignals, src BatterySource) NetworkSignals {
	if src == nil {
		sig.BatteryLevel = -1
		return sig
	}
	if b, ok := src.Read(); ok {
		sig.BatteryLevel = b.Level
		sig.BatteryDraining = b.Draining
	} else {
		sig.BatteryLevel = -1
		sig.BatteryDraining = false
	}
	return sig
}
