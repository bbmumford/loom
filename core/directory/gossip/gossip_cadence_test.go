/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package gossip

import (
	"testing"
	"time"
)

func TestGossipCadenceBounds_DefaultsWhenUnwired(t *testing.T) {
	SetGossipCadence(nil)
	base, min, max := gossipCadenceBounds(30*time.Second, 2*time.Second, 60*time.Second)
	if base != 30*time.Second || min != 2*time.Second || max != 60*time.Second {
		t.Fatalf("unwired cadence must pass defaults through, got base=%v min=%v max=%v", base, min, max)
	}
}

func TestGossipCadenceBounds_ProviderOverrides(t *testing.T) {
	SetGossipCadence(func() (time.Duration, time.Duration, time.Duration, bool) {
		return 5 * time.Second, 1 * time.Second, 20 * time.Second, true
	})
	defer SetGossipCadence(nil)

	base, min, max := gossipCadenceBounds(30*time.Second, 2*time.Second, 60*time.Second)
	if base != 5*time.Second || min != 1*time.Second || max != 20*time.Second {
		t.Fatalf("wired cadence must override defaults, got base=%v min=%v max=%v", base, min, max)
	}
}

func TestGossipCadenceBounds_RejectsInvalidRanges(t *testing.T) {
	def := func() (time.Duration, time.Duration, time.Duration) {
		return gossipCadenceBounds(30*time.Second, 2*time.Second, 60*time.Second)
	}
	cases := map[string]GossipCadenceFunc{
		"declined":      func() (time.Duration, time.Duration, time.Duration, bool) { return 5, 1, 20, false },
		"zero base":     func() (time.Duration, time.Duration, time.Duration, bool) { return 0, 1, 20, true },
		"negative min":  func() (time.Duration, time.Duration, time.Duration, bool) { return 5, -1, 20, true },
		"min above max": func() (time.Duration, time.Duration, time.Duration, bool) { return 5, 30, 20, true },
		"base below min": func() (time.Duration, time.Duration, time.Duration, bool) {
			return 1 * time.Second, 2 * time.Second, 20 * time.Second, true
		},
		"base above max": func() (time.Duration, time.Duration, time.Duration, bool) {
			return 25 * time.Second, 2 * time.Second, 20 * time.Second, true
		},
	}
	for name, fn := range cases {
		SetGossipCadence(fn)
		base, min, max := def()
		if base != 30*time.Second || min != 2*time.Second || max != 60*time.Second {
			t.Fatalf("%s: invalid range must fall back to defaults, got base=%v min=%v max=%v", name, base, min, max)
		}
	}
	SetGossipCadence(nil)
}
