/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"sort"
	"sync"
	"time"
)

// reputationMaxPeers caps the reputation tracker to prevent unbounded growth
// if the event log contains data for many historical peers.
const reputationMaxPeers = 500

// PeerReputation holds the computed reputation metrics for a single peer.
type PeerReputation struct {
	NodeID             string        // full node ID
	UptimePercent      float64       // fraction of observed window spent connected (0-1)
	AvgGrade           float64       // average connection grade normalized to 0-1 (A=1.0 … F=0.0)
	DropFrequency      float64       // drops per hour (lower is better)
	AvgRTT             time.Duration // average round-trip time across connected events
	Score              float64       // composite reputation score (0-1, higher is better)
	GradeStableSince   time.Time     // when the current grade was established (feeds stability factor)
	EffectiveGrade     float64       // may differ from transport grade under sustained RTT bloat
}

// ComputeScore calculates the composite score from the individual metrics.
//
//	Score = 0.30*uptime + 0.30*gradeNorm + 0.25*stability + 0.15*latencyNorm
//
// Each component is normalized to 0-1 where 1 is best.
func (r *PeerReputation) ComputeScore() {
	// Uptime component: already 0-1
	uptime := clampFloat(r.UptimePercent, 0, 1)

	// Grade component: already 0-1. MESH-G04: prefer EffectiveGrade when set — it
	// captures the sustained-RTT-bloat demotion injected via InjectGradeInfo,
	// which AvgGrade alone ignored entirely (so the documented demotion had no
	// effect on the score). Fall back to AvgGrade when no effective grade exists.
	gradeVal := r.AvgGrade
	if r.EffectiveGrade > 0 {
		gradeVal = r.EffectiveGrade
	}
	grade := clampFloat(gradeVal, 0, 1)

	// Stability component: inverse of drop frequency, factored by grade stability.
	// 0 drops/hr → 1.0, 1 drop/hr → 0.5, 5+ drops/hr → ~0.0
	dropStability := 1.0 / (1.0 + r.DropFrequency)

	// Grade stability factor — longer continuous time at the current
	// grade yields a higher score so flapping peers rank below steady ones.
	// Stable >10min → 1.0, 1-10min → 0.5, <1min → 0.2
	gradeStabilityFactor := 0.2
	if !r.GradeStableSince.IsZero() {
		stableDur := time.Since(r.GradeStableSince)
		switch {
		case stableDur > 10*time.Minute:
			gradeStabilityFactor = 1.0
		case stableDur > 1*time.Minute:
			gradeStabilityFactor = 0.5
		}
	}
	stability := dropStability * gradeStabilityFactor

	// Latency component: inverse log scale.
	// 0ms → 1.0, 50ms → ~0.59, 200ms → ~0.40, 1000ms → ~0.26
	rttMs := float64(r.AvgRTT.Milliseconds())
	if rttMs < 0 {
		rttMs = 0
	}
	latency := 1.0 / (1.0 + rttMs/50.0)

	r.Score = 0.30*uptime + 0.30*grade + 0.25*stability + 0.15*latency
}

// ReputationTracker computes peer reputation scores from a ConnectionEventLog.
type ReputationTracker struct {
	mu       sync.RWMutex
	eventLog *ConnectionEventLog
	window   time.Duration // how far back to look (default: 1 hour)
	scores   map[string]*PeerReputation
}

// NewReputationTracker creates a tracker that reads from the given event log.
// The scoring window defaults to 1 hour.
func NewReputationTracker(eventLog *ConnectionEventLog) *ReputationTracker {
	return &ReputationTracker{
		eventLog: eventLog,
		window:   1 * time.Hour,
		scores:   make(map[string]*PeerReputation),
	}
}

// gradeToNormalized converts a Grade enum value to a 0-1 float.
func gradeToNormalized(g Grade) float64 {
	switch g {
	case GradeA:
		return 1.0
	case GradeB:
		return 0.75
	case GradeC:
		return 0.5
	default:
		return 0.0
	}
}

// ComputeAll scans the event log and recomputes reputation scores for all peers.
func (rt *ReputationTracker) ComputeAll() {
	if rt.eventLog == nil {
		return
	}

	now := time.Now()
	windowStart := now.Add(-rt.window)
	events := rt.eventLog.EventsSince(windowStart)
	if len(events) == 0 {
		// MESH-G04: no events in the window means every previously-scored peer is
		// now stale. Clear the scores rather than leaving them frozen at their
		// last value forever (the old early-return left rt.scores populated).
		rt.mu.Lock()
		rt.scores = make(map[string]*PeerReputation)
		rt.mu.Unlock()
		return
	}

	// Per-peer accumulators
	type peerAccum struct {
		connectedAt   time.Time     // most recent connect timestamp (within window)
		totalUptime   time.Duration // total connected duration
		gradeSum      float64       // sum of normalized grades (weighted by duration or per-event)
		gradeCount    int           // number of grade observations
		drops         int           // number of disconnect events
		rttSum        time.Duration // placeholder — we use RTT from peerConn, not events
		rttCount      int
		lastConnected bool // was the peer connected at the end of the window?
	}

	accum := make(map[string]*peerAccum)

	getAccum := func(nodeID string) *peerAccum {
		a, ok := accum[nodeID]
		if !ok {
			a = &peerAccum{}
			accum[nodeID] = a
		}
		return a
	}

	for _, e := range events {
		a := getAccum(e.PeerNodeID)

		switch e.Reason {
		case "connected":
			a.connectedAt = e.Timestamp
			a.lastConnected = true
			a.gradeSum += gradeToNormalized(e.NewGrade)
			a.gradeCount++

		case "upgraded":
			// Close out previous connected span
			if !a.connectedAt.IsZero() {
				a.totalUptime += e.Timestamp.Sub(a.connectedAt)
			}
			a.connectedAt = e.Timestamp
			a.gradeSum += gradeToNormalized(e.NewGrade)
			a.gradeCount++

		case "downgraded":
			if !a.connectedAt.IsZero() {
				a.totalUptime += e.Timestamp.Sub(a.connectedAt)
			}
			a.connectedAt = e.Timestamp
			a.gradeSum += gradeToNormalized(e.NewGrade)
			a.gradeCount++

		case "disconnected", "drained":
			if !a.connectedAt.IsZero() {
				a.totalUptime += e.Timestamp.Sub(a.connectedAt)
				a.connectedAt = time.Time{} // reset
			}
			a.lastConnected = false
			a.drops++
		}
	}

	// Finalize: if a peer is still connected at the end of the window,
	// count uptime from last connectedAt to now.
	for _, a := range accum {
		if a.lastConnected && !a.connectedAt.IsZero() {
			a.totalUptime += now.Sub(a.connectedAt)
		}
	}

	// Build reputation map
	windowSecs := rt.window.Seconds()
	windowHours := rt.window.Hours()

	rt.mu.Lock()
	defer rt.mu.Unlock()

	// Cap the number of peers we track to prevent unbounded memory growth.
	// If more peers than the cap exist, we only keep the ones with the most
	// uptime (most relevant to current mesh health).
	if len(accum) > reputationMaxPeers {
		dbgPeers.Printf("Reputation capping scored peers from %d to %d", len(accum), reputationMaxPeers)
		type kv struct {
			id     string
			uptime time.Duration
		}
		sorted := make([]kv, 0, len(accum))
		for id, a := range accum {
			sorted = append(sorted, kv{id, a.totalUptime})
		}
		sort.Slice(sorted, func(i, j int) bool {
			return sorted[i].uptime > sorted[j].uptime
		})
		keep := make(map[string]bool, reputationMaxPeers)
		for i := 0; i < reputationMaxPeers && i < len(sorted); i++ {
			keep[sorted[i].id] = true
		}
		for id := range accum {
			if !keep[id] {
				delete(accum, id)
			}
		}
	}

	rt.scores = make(map[string]*PeerReputation, len(accum))
	for nodeID, a := range accum {
		rep := &PeerReputation{
			NodeID: nodeID,
		}

		// Uptime as fraction of window
		if windowSecs > 0 {
			rep.UptimePercent = clampFloat(a.totalUptime.Seconds()/windowSecs, 0, 1)
		}

		// Average grade
		if a.gradeCount > 0 {
			rep.AvgGrade = a.gradeSum / float64(a.gradeCount)
		}

		// Drop frequency (drops per hour)
		if windowHours > 0 {
			rep.DropFrequency = float64(a.drops) / windowHours
		}

		// AvgRTT: will be populated from live peer data if available
		// (event log doesn't carry RTT; we patch this in ScoreFor)

		rep.ComputeScore()
		rt.scores[nodeID] = rep
	}
}

// ScoreFor returns the reputation score for a single peer.
// Returns the cached score from the most recent ComputeAll() call.
// If the peer has no history, returns a zero-value PeerReputation with Score 0.
func (rt *ReputationTracker) ScoreFor(nodeID string) PeerReputation {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	if rep, ok := rt.scores[nodeID]; ok {
		return *rep
	}
	return PeerReputation{NodeID: nodeID}
}

// RankedPeers returns all tracked peers sorted by score descending (best first).
func (rt *ReputationTracker) RankedPeers() []PeerReputation {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	ranked := make([]PeerReputation, 0, len(rt.scores))
	for _, rep := range rt.scores {
		ranked = append(ranked, *rep)
	}
	sort.Slice(ranked, func(i, j int) bool {
		return ranked[i].Score > ranked[j].Score
	})
	return ranked
}

// InjectRTT updates the AvgRTT for a peer and recomputes its score.
// This is called with live RTT data from peerConn since the event log
// does not carry RTT measurements.
func (rt *ReputationTracker) InjectRTT(nodeID string, rtt time.Duration) {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	rep, ok := rt.scores[nodeID]
	if !ok {
		return
	}
	rep.AvgRTT = rtt
	rep.ComputeScore()
}

// InjectGradeInfo updates the grade stability and effective grade for a peer.
// Called with live data from peerConn since the event log doesn't carry these.
func (rt *ReputationTracker) InjectGradeInfo(nodeID string, gradeStableSince time.Time, effectiveGrade float64) {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	rep, ok := rt.scores[nodeID]
	if !ok {
		return
	}
	rep.GradeStableSince = gradeStableSince
	rep.EffectiveGrade = effectiveGrade
	rep.ComputeScore()
}

// clampFloat clamps v to [lo, hi].
func clampFloat(v, lo, hi float64) float64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}
