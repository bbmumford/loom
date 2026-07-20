/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

// Package ewma provides a lock-free exponentially-weighted moving average.
//
// It is a drop-in replacement for the common "mutex around an
// α·sample + (1-α)·old scalar" smoothing pattern, removing a mutex
// acquire from the per-sample hot path. The value is stored as the IEEE-754
// bits of a float64 in an atomic.Uint64 and updated with a compare-and-swap
// loop, so concurrent Update calls never panic and never corrupt the value.
//
// Semantics under contention are last-writer-wins-with-retry: if two
// goroutines Update concurrently, the CAS loser recomputes against the
// winner's freshly-stored value before retrying, so no sample's
// contribution is silently dropped — but the exact interleaving order is
// not guaranteed. Use this only for genuine single-scalar smoothing where
// approximate, order-tolerant accumulation is acceptable (RTT, reliability,
// throughput estimates). Do NOT use it where exact, ordered accumulation is
// required.
//
// Ported from pollen pkg/observability/metrics/ewma.go (Apache-2.0).
package ewma

import (
	"math"
	"sync/atomic"
)

// EWMA is a lock-free exponentially-weighted moving average. The zero value
// is not usable; construct with New.
type EWMA struct {
	bits  atomic.Uint64
	alpha float64
}

// New returns an EWMA with the given smoothing factor alpha (0 < alpha <= 1;
// larger alpha weights recent samples more heavily) seeded to initial.
func New(alpha, initial float64) *EWMA {
	e := &EWMA{alpha: alpha}
	e.Reset(initial)
	return e
}

// Update folds sample into the average: new = alpha·sample + (1-alpha)·old.
// Safe for concurrent use.
func (e *EWMA) Update(sample float64) {
	for {
		oldBits := e.bits.Load()
		oldVal := math.Float64frombits(oldBits)
		newVal := e.alpha*sample + (1-e.alpha)*oldVal
		if e.bits.CompareAndSwap(oldBits, math.Float64bits(newVal)) {
			return
		}
	}
}

// Reset stores value directly, discarding history. Safe for concurrent use.
func (e *EWMA) Reset(value float64) {
	e.bits.Store(math.Float64bits(value))
}

// Value returns the current smoothed value. Safe for concurrent use.
func (e *EWMA) Value() float64 {
	return math.Float64frombits(e.bits.Load())
}
