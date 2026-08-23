/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package netpolicy

import "time"

// GossipCadence returns a function that yields the CURRENT gossip interval from the policy's live
// profile. It is the seam a publisher / gossip loop calls once per iteration to pace itself by the
// active network profile instead of a fixed interval captured at loop start — so a wifi→cellular
// transition widens the cadence on the next tick without restarting the loop. It reads GossipBase
// (the nominal cadence); a caller wanting the min/max envelope reads those profile fields directly.
// A nil policy yields 0, which callers treat as "use the loop's own default".
func GossipCadence(p *Policy) func() time.Duration {
	return func() time.Duration {
		if p == nil {
			return 0
		}
		return p.Profile().GossipBase
	}
}

// FreshnessCadence is the analogous seam for the freshness-probe safety net: it yields the profile's
// current FreshnessProbeMax so the probe relaxes on a metered link and tightens on ethernet.
func FreshnessCadence(p *Policy) func() time.Duration {
	return func() time.Duration {
		if p == nil {
			return 0
		}
		return p.Profile().FreshnessProbeMax
	}
}

// GossipBounds returns the profile's (base, min, max) gossip cadence in the exact argument order the
// adaptive-interval constructor takes, so a gossip loop can pace its churn-adaptive interval from the
// active network profile in a single call — NewAdaptiveInterval(GossipBounds(policy)) — instead of
// the fixed base + hardcoded 2s/60s envelope it uses today. This is the precise seam the profile
// (which already parameterizes min/base/max per link) plugs into. A nil policy yields the zero triple,
// which the loop treats as "use its own defaults".
func GossipBounds(p *Policy) (base, min, max time.Duration) {
	if p == nil {
		return 0, 0, 0
	}
	prof := p.Profile()
	return prof.GossipBase, prof.GossipMin, prof.GossipMax
}
