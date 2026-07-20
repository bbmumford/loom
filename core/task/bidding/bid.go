/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package bidding

import (
	"time"

	task "github.com/bbmumford/loom/core/task"
)

// BidWeights controls the relative weighting of score computation.
type BidWeights struct {
	Capacity  float64
	ETA       float64
	Freshness float64
	Diversity float64
}

// BidPolicy configures bid evaluation at the gateway.
type BidPolicy struct {
	Weights         BidWeights
	MaxRecentWindow time.Duration
	MinScore        float64
}

// DefaultBidPolicy aligned with design heuristics.
func DefaultBidPolicy() BidPolicy {
	return BidPolicy{
		Weights: BidWeights{
			Capacity:  2.0,
			ETA:       -1.2,
			Freshness: -0.5,
			Diversity: 0.3,
		},
		MaxRecentWindow: 30 * time.Second,
		MinScore:        0.0,
	}
}

// Evaluate computes a score given bid inputs plus historical modifiers.
func (p BidPolicy) Evaluate(bid task.Bid, now time.Time, recentWinner bool) float64 {
	capacity := clamp(bid.CapacityScore, 0, 10)
	score := p.Weights.Capacity*capacity + p.Weights.ETA*bid.ETA.Seconds()
	if recentWinner {
		score += p.Weights.Diversity * -1.0
	}
	if !bid.ReceivedAt.IsZero() {
		t := now.Sub(bid.ReceivedAt)
		if t < 0 {
			t = 0
		}
		score += p.Weights.Freshness * t.Seconds()
	}
	return score
}

// clamp bounds value between min and max.
func clamp(value, min, max float64) float64 {
	if value < min {
		return min
	}
	if value > max {
		return max
	}
	return value
}
