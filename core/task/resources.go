/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package task

import (
	"math"
	"time"
)

// ResourceWeights controls how executor resource metrics influence capacity scoring.
type ResourceWeights struct {
	CPU     float64
	RAM     float64
	Disk    float64
	Net     float64
	Queue   float64
	Latency float64
}

// DefaultResourceWeights emphasise CPU and queue depth for low-latency workloads.
func DefaultResourceWeights() ResourceWeights {
	return ResourceWeights{
		CPU:     25,
		RAM:     15,
		Disk:    5,
		Net:     10,
		Queue:   25,
		Latency: 20,
	}
}

// ResourceSnapshot captures measurements used to compute a capacity score.
type ResourceSnapshot struct {
	CPUFree       float64       `json:"cpuFree"`
	RAMPressure   float64       `json:"ramPressure"`
	DiskFree      float64       `json:"diskFree"`
	NetCongestion float64       `json:"netCongestion"`
	QueueDepth    float64       `json:"queueDepth"`
	QueueCapacity float64       `json:"queueCapacity"`
	AvgLatency    time.Duration `json:"avgLatency"`
	LatencySLO    time.Duration `json:"latencySlo"`
}

const (
	maxRawCapacityScore  = 100.0
	defaultLatencyFloor  = 5 * time.Millisecond
	defaultQueueCapacity = 1.0
)

// ComputeCapacityScore returns a raw score (0..100) using the weighted resource formula.
// Higher scores indicate greater capacity headroom.
func ComputeCapacityScore(snapshot ResourceSnapshot, weights ResourceWeights) float64 {
	cpuFree := clamp(snapshot.CPUFree, 0, 1)
	ramPressure := clamp(snapshot.RAMPressure, 0, 1)
	diskFree := clamp(snapshot.DiskFree, 0, 1)
	netCongestion := clamp(snapshot.NetCongestion, 0, 1)
	queueCap := snapshot.QueueCapacity
	if queueCap <= 0 {
		queueCap = defaultQueueCapacity
	}
	queueNorm := snapshot.QueueDepth / queueCap
	queueNorm = clamp(queueNorm, 0, 1)
	latencySLO := snapshot.LatencySLO
	if latencySLO <= 0 {
		latencySLO = defaultLatencyFloor
	}
	latencyNorm := float64(snapshot.AvgLatency) / float64(latencySLO)
	latencyNorm = clamp(latencyNorm, 0, 1)

	score := maxRawCapacityScore
	score -= weights.CPU * (1 - cpuFree)
	score -= weights.RAM * ramPressure
	score -= weights.Disk * (1 - diskFree)
	score -= weights.Net * netCongestion
	score -= weights.Queue * queueNorm
	score -= weights.Latency * latencyNorm

	return clamp(score, 0, maxRawCapacityScore)
}

// NormalizeCapacityScore scales the raw capacity score into a bounded range (e.g. 0..10).
func NormalizeCapacityScore(raw float64, upper float64) float64 {
	if upper <= 0 {
		upper = 1
	}
	normalized := (clamp(raw, 0, maxRawCapacityScore) / maxRawCapacityScore) * upper
	return clamp(normalized, 0, upper)
}

func clamp(value, min, max float64) float64 {
	return math.Max(min, math.Min(max, value))
}
