/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package netpolicy

import "time"

// unlimitedRampSentinel stands in for an unpaced (ReconcilePacing/SnapshotChunkMax == 0) value while
// interpolating, so a ramp between "unlimited" and a finite cap has two real endpoints to move
// between. A ramped value that lands at or above it maps back to 0 (= unlimited).
const unlimitedRampSentinel = 1 << 30 // 1 GiB/s

// InterpolateProfile returns the effective profile part-way through a transition from `from` to `to`.
// It implements the synthesizer's smooth-gradient rule: the bandwidth/pacing values (ReconcilePacing,
// SnapshotChunkMax) ramp LINEARLY over the ramp window — so a wifi→cellular move eases a transfer
// from line-rate down to 8 KB/s rather than cutting it dead — while every cadence interval and
// structural count flips to the target immediately (there is no meaningful gradient between "next
// gossip in 10 min" and "in 30 min"). elapsed ≤ 0 yields `from`; elapsed ≥ ramp (or ramp ≤ 0) yields
// `to` exactly.
func InterpolateProfile(from, to NetworkProfile, elapsed, ramp time.Duration) NetworkProfile {
	if ramp <= 0 || elapsed >= ramp {
		return to
	}
	if elapsed <= 0 {
		return from
	}
	frac := float64(elapsed) / float64(ramp)

	// Everything but the two bandwidth caps flips to the target immediately.
	out := to
	out.ReconcilePacing = rampBytes(from.ReconcilePacing, to.ReconcilePacing, frac)
	out.SnapshotChunkMax = rampBytes(from.SnapshotChunkMax, to.SnapshotChunkMax, frac)
	return out
}

// rampBytes linearly interpolates a byte-rate/size cap where 0 means "unlimited". It swaps 0 for a
// large sentinel at both endpoints so the ramp has real numbers to move between, then maps a result
// at/above the sentinel back to 0. At frac 0 it returns from, at frac 1 it returns to (unlimited
// preserved at either end).
func rampBytes(from, to int, frac float64) int {
	f, t := from, to
	if f == 0 {
		f = unlimitedRampSentinel
	}
	if t == 0 {
		t = unlimitedRampSentinel
	}
	v := float64(f) + frac*(float64(t)-float64(f))
	res := int(v)
	if res >= unlimitedRampSentinel {
		return 0 // at or above the sentinel is indistinguishable from unlimited
	}
	if res < 0 {
		return 0
	}
	return res
}
