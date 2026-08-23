/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package netpolicy

import "time"

// RTTTracker maintains a fast exponential moving average of round-trip samples plus a slower
// baseline, so a caller can feed NetworkSignals.AvgRTT from Avg() and drive the synthesizer's
// predictive rule from RisingBy(). It reads no clock — a sample is a measured duration — so it is
// deterministic and unit-testable. Use one per peer for per-peer overrides and one global instance
// for the aggregate signal.
type RTTTracker struct {
	alpha    float64 // fast EMA weight for a new sample (0..1)
	beta     float64 // slower baseline weight
	ema      time.Duration
	baseline time.Duration
	seen     bool
}

// NewRTTTracker returns a tracker. alpha/beta ≤ 0 fall back to 0.3 (fast) and 0.05 (baseline); a
// smaller beta makes the baseline lag further behind, so a genuine sustained rise is what trips
// RisingBy rather than a single spike.
func NewRTTTracker(alpha, beta float64) *RTTTracker {
	if alpha <= 0 {
		alpha = 0.3
	}
	if beta <= 0 {
		beta = 0.05
	}
	return &RTTTracker{alpha: alpha, beta: beta}
}

// Observe folds one RTT sample into the fast average and the slower baseline.
func (t *RTTTracker) Observe(sample time.Duration) {
	if sample <= 0 {
		return
	}
	if !t.seen {
		t.ema, t.baseline, t.seen = sample, sample, true
		return
	}
	t.ema += time.Duration(t.alpha * float64(sample-t.ema))
	t.baseline += time.Duration(t.beta * float64(sample-t.baseline))
}

// Avg returns the fast EMA — the NetworkSignals.AvgRTT input.
func (t *RTTTracker) Avg() time.Duration { return t.ema }

// Baseline returns the slow reference the fast average is compared against.
func (t *RTTTracker) Baseline() time.Duration { return t.baseline }

// RisingBy reports whether the fast average has climbed above the baseline by at least factor (e.g.
// 1.2 for "up 20%") — the sustained-rise trend the predictive rule consumes. False until a baseline
// exists.
func (t *RTTTracker) RisingBy(factor float64) bool {
	if !t.seen || t.baseline <= 0 {
		return false
	}
	return float64(t.ema) > float64(t.baseline)*factor
}

// LossTracker is a sliding-window miss-rate over the most recent rumor-ACK outcomes, feeding
// NetworkSignals.LossRate. It keeps a fixed ring of the last N outcomes so the rate reflects recent
// conditions rather than lifetime totals.
type LossTracker struct {
	window []bool // true = acked, false = missed
	next   int
	filled int
}

// NewLossTracker returns a tracker over the last window outcomes (window ≤ 0 falls back to 32).
func NewLossTracker(window int) *LossTracker {
	if window <= 0 {
		window = 32
	}
	return &LossTracker{window: make([]bool, window)}
}

// Observe records one rumor outcome: acked true = delivered, false = the ACK was missed.
func (l *LossTracker) Observe(acked bool) {
	l.window[l.next] = acked
	l.next = (l.next + 1) % len(l.window)
	if l.filled < len(l.window) {
		l.filled++
	}
}

// Rate returns the miss fraction over the observed window (0 when nothing has been observed yet).
func (l *LossTracker) Rate() float64 {
	if l.filled == 0 {
		return 0
	}
	misses := 0
	for i := 0; i < l.filled; i++ {
		if !l.window[i] {
			misses++
		}
	}
	return float64(misses) / float64(l.filled)
}
