/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package gossip

import (
	"sync/atomic"
	"time"
)

// GossipCadenceFunc supplies the adaptive gossip interval's bounds — base,
// minimum, and maximum — that runGossipLoopInternal hands to NewAdaptiveInterval
// when a loop starts. A host installs one via SetGossipCadence to drive cadence
// from a live network profile (link type, loss, battery); returning ok=false
// leaves the loop on its built-in bounds. It is a plain func, not a policy-type
// reference, so this package carries no dependency on the package that computes
// the bounds — the host composes the two, the same way it wires ReconcileDriver.
type GossipCadenceFunc func() (base, min, max time.Duration, ok bool)

// cadenceHolder boxes the func so atomic.Value always stores one concrete type
// (a *cadenceHolder); a bare func value stored directly cannot guarantee that
// across a nil clear and a later non-nil set.
type cadenceHolder struct{ fn GossipCadenceFunc }

// sharedGossipCadence is read once per loop start by gossipCadenceBounds and
// written by SetGossipCadence. Write-rarely / read-per-loop-spin-up, so it uses
// the same atomic.Value shape as the driver registry rather than a mutex.
var sharedGossipCadence atomic.Value // *cadenceHolder

// SetGossipCadence registers the process-wide gossip cadence source. Pass nil to
// clear (tests, or a host tearing down its network-profile synthesizer). Later
// calls override earlier ones.
func SetGossipCadence(fn GossipCadenceFunc) {
	sharedGossipCadence.Store(&cadenceHolder{fn: fn})
}

// gossipCadenceBounds returns the registered cadence bounds, or the supplied
// defaults when no source is wired or it declines. A source that returns a
// non-positive or inverted range (min>max, or base outside [min,max]) is ignored
// in favour of the defaults, so a misconfigured profile can never wedge the loop
// at a zero or negative interval — the timer this feeds would busy-spin.
func gossipCadenceBounds(defBase, defMin, defMax time.Duration) (base, min, max time.Duration) {
	base, min, max = defBase, defMin, defMax
	v := sharedGossipCadence.Load()
	if v == nil {
		return
	}
	h, _ := v.(*cadenceHolder)
	if h == nil || h.fn == nil {
		return
	}
	b, lo, hi, ok := h.fn()
	if !ok || b <= 0 || lo <= 0 || hi <= 0 || lo > hi || b < lo || b > hi {
		return
	}
	return b, lo, hi
}
