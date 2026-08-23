/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

// Package netpolicy is the mesh's network-awareness engine: it turns a vector of live link signals
// (link type, RTT, loss, battery, churn) into a NetworkProfile that every cadence and pacing knob
// reads from, so the node gossips aggressively on ethernet and conservatively on a draining cellular
// link. This file is the deterministic core — the profile table + the signal→profile synthesizer.
// Wiring the emitted profile into the live publisher cadence, rumor retry, and reconciliation pacing
// is a separate integration step; the synthesizer here reads no clock and holds no sockets, so its
// rules are unit-testable in isolation.
package netpolicy

import (
	"sync"
	"time"
)

// LinkType classifies the active network path. Ordering matters only through restrictiveness (below),
// not the constant values.
type LinkType int

const (
	LinkUnknown LinkType = iota
	LinkEthernet
	LinkWiFi
	LinkVPN
	LinkCellular
)

// restrictiveness ranks a link from least to most bandwidth/energy constrained. A higher rank is a
// more conservative profile; hysteresis only guards transitions that INCREASE the rank.
func (l LinkType) restrictiveness() int {
	switch l {
	case LinkEthernet:
		return 0
	case LinkWiFi:
		return 1
	case LinkVPN:
		return 2
	case LinkCellular:
		return 3
	default:
		return 1 // unknown is treated like wifi — neither trusted-fast nor assumed-metered
	}
}

// NetworkSignals is the input vector the synthesizer reads. It carries no clock: LinkStableSince and
// the trend inputs are measured by the caller and passed in, so the synthesizer stays pure.
type NetworkSignals struct {
	LinkType        LinkType
	AvgRTT          time.Duration // exponential moving average
	LossRate        float64       // recent rumor-ACK miss rate, 0..1
	EventBurstRate  float64       // address-change events/min
	BatteryLevel    float64       // 0..1; negative = on AC power
	BatteryDraining bool
	TimeOfDay       int           // 0-23 local hour
	LinkStableSince time.Duration // how long the current LinkType has held
	PeerCount       int
}

// NetworkProfile is the synthesized tuning every pacing knob reads. Cadence values are intervals;
// ReconcilePacing/SnapshotChunkMax bound bandwidth.
type NetworkProfile struct {
	GossipMin          time.Duration
	GossipBase         time.Duration
	GossipMax          time.Duration
	FreshnessProbeMin  time.Duration
	FreshnessProbeMax  time.Duration
	RumorRetryInitial  time.Duration
	RumorRetryMax      time.Duration
	RumorFanout        int
	ReconcilePacing    int // bytes/sec; 0 = unpaced (line-rate)
	SnapshotChunkMax   int
	SnapshotShardCount int
	HypercubeMaxDims   int
}

// defaultProfiles is the per-link baseline tuning. Ethernet gossips aggressively and unpaced;
// cellular widens every interval, cuts fanout, and paces reconciliation to 8 KB/s so a metered link
// is not saturated.
var defaultProfiles = map[LinkType]NetworkProfile{
	LinkEthernet: {
		GossipMin: 5 * time.Second, GossipBase: 15 * time.Second, GossipMax: 60 * time.Second,
		FreshnessProbeMin: 30 * time.Second, FreshnessProbeMax: 2 * time.Minute,
		RumorRetryInitial: 200 * time.Millisecond, RumorRetryMax: 5 * time.Second,
		RumorFanout: 6, ReconcilePacing: 0, SnapshotChunkMax: 256 * 1024,
		SnapshotShardCount: 4, HypercubeMaxDims: 8,
	},
	LinkWiFi: {
		GossipMin: 10 * time.Second, GossipBase: 30 * time.Second, GossipMax: 120 * time.Second,
		FreshnessProbeMin: 45 * time.Second, FreshnessProbeMax: 3 * time.Minute,
		RumorRetryInitial: 400 * time.Millisecond, RumorRetryMax: 8 * time.Second,
		RumorFanout: 4, ReconcilePacing: 0, SnapshotChunkMax: 128 * 1024,
		SnapshotShardCount: 4, HypercubeMaxDims: 6,
	},
	LinkVPN: {
		GossipMin: 15 * time.Second, GossipBase: 45 * time.Second, GossipMax: 180 * time.Second,
		FreshnessProbeMin: 60 * time.Second, FreshnessProbeMax: 4 * time.Minute,
		RumorRetryInitial: 600 * time.Millisecond, RumorRetryMax: 12 * time.Second,
		RumorFanout: 3, ReconcilePacing: 64 * 1024, SnapshotChunkMax: 64 * 1024,
		SnapshotShardCount: 5, HypercubeMaxDims: 4,
	},
	LinkCellular: {
		GossipMin: 30 * time.Second, GossipBase: 60 * time.Second, GossipMax: 5 * time.Minute,
		FreshnessProbeMin: 2 * time.Minute, FreshnessProbeMax: 10 * time.Minute,
		RumorRetryInitial: time.Second, RumorRetryMax: 20 * time.Second,
		RumorFanout: 2, ReconcilePacing: 8 * 1024, SnapshotChunkMax: 32 * 1024,
		SnapshotShardCount: 6, HypercubeMaxDims: 3,
	},
}

// ProfileFor returns the baseline profile for a link type (a copy — callers may tune it freely).
func ProfileFor(l LinkType) NetworkProfile {
	if p, ok := defaultProfiles[l]; ok {
		return p
	}
	return defaultProfiles[LinkWiFi]
}

// SynthConfig tunes the synthesizer's rule thresholds. Zero values fall back to the documented
// defaults, so a caller can construct a Synthesizer with an empty config.
type SynthConfig struct {
	ConfirmWindow time.Duration // hysteresis: how long a more-restricted link must hold before switching
	RTTRiseFactor float64       // predictive: AvgRTT above baseline×this counts as "trending up"
	LossThreshold float64       // predictive: LossRate at/above this counts as elevated
	BaselineDecay float64       // EMA weight for the new sample when updating the RTT baseline (0..1)
}

func (c SynthConfig) withDefaults() SynthConfig {
	if c.ConfirmWindow == 0 {
		c.ConfirmWindow = 60 * time.Second
	}
	if c.RTTRiseFactor == 0 {
		c.RTTRiseFactor = 1.2
	}
	if c.LossThreshold == 0 {
		c.LossThreshold = 0.1
	}
	if c.BaselineDecay == 0 {
		c.BaselineDecay = 0.25
	}
	return c
}

// Synthesizer holds the small amount of state the profile rules need: the currently selected link
// and a slow RTT baseline for trend detection. It is not safe for concurrent Next calls (a single
// signal loop drives it); Policy wraps it with a lock.
type Synthesizer struct {
	cfg      SynthConfig
	current  LinkType
	baseline time.Duration // slow EMA of AvgRTT, for the predictive rule
}

// NewSynthesizer starts on link and its baseline profile.
func NewSynthesizer(link LinkType, cfg SynthConfig) *Synthesizer {
	return &Synthesizer{cfg: cfg.withDefaults(), current: link}
}

// Next folds one signal reading into the selected link and returns the resulting profile plus
// whether the selection changed. The rules, in order:
//
//   - Predictive (rule 2): a sustained RTT rise (above baseline×RTTRiseFactor) with elevated loss
//     while the battery drains pulls the target one step MORE restricted than the raw link, so the
//     node backs off before the link actually flips.
//   - Hysteresis (rule 1): a move to a MORE restricted target takes effect only once the link has
//     held that long (LinkStableSince ≥ ConfirmWindow); a move to a LESS restricted target takes
//     effect on the first signal. This stops a flapping link from thrashing profiles.
func (s *Synthesizer) Next(sig NetworkSignals) (NetworkProfile, bool) {
	target := sig.LinkType

	// Predictive escalation before the link type itself changes.
	if s.predictiveRestrict(sig) {
		target = moreRestricted(target)
	}

	next := s.current
	switch {
	case target.restrictiveness() > s.current.restrictiveness():
		// Tightening — require the confirmation window.
		if sig.LinkStableSince >= s.cfg.ConfirmWindow {
			next = target
		}
	case target.restrictiveness() < s.current.restrictiveness():
		// Loosening — take it immediately.
		next = target
	}

	s.updateBaseline(sig.AvgRTT)
	changed := next != s.current
	s.current = next
	return ProfileFor(next), changed
}

// Current reports the selected link type.
func (s *Synthesizer) Current() LinkType { return s.current }

func (s *Synthesizer) predictiveRestrict(sig NetworkSignals) bool {
	if s.baseline <= 0 {
		return false // no baseline yet — nothing to trend against
	}
	rttRising := float64(sig.AvgRTT) > float64(s.baseline)*s.cfg.RTTRiseFactor
	return rttRising && sig.LossRate >= s.cfg.LossThreshold && sig.BatteryDraining
}

func (s *Synthesizer) updateBaseline(sample time.Duration) {
	if sample <= 0 {
		return
	}
	if s.baseline <= 0 {
		s.baseline = sample
		return
	}
	// baseline += decay*(sample-baseline)
	s.baseline += time.Duration(s.cfg.BaselineDecay * float64(sample-s.baseline))
}

// moreRestricted returns the next tighter link one step up the restrictiveness ladder, saturating at
// cellular.
func moreRestricted(l LinkType) LinkType {
	switch l {
	case LinkEthernet:
		return LinkWiFi
	case LinkWiFi:
		return LinkVPN
	case LinkVPN, LinkCellular:
		return LinkCellular
	default:
		return LinkCellular
	}
}

// Policy is the concrete NetworkPolicy: it drives the synthesizer from Update calls, notifies
// subscribers on a profile change, and holds per-peer overrides. It is safe for concurrent use.
type Policy struct {
	mu        sync.RWMutex
	synth     *Synthesizer
	signals   NetworkSignals
	profile   NetworkProfile
	overrides map[string]NetworkProfile
	subs      []func(NetworkProfile)
}

// NewPolicy starts a policy on link with its baseline profile.
func NewPolicy(link LinkType, cfg SynthConfig) *Policy {
	return &Policy{
		synth:     NewSynthesizer(link, cfg),
		profile:   ProfileFor(link),
		overrides: map[string]NetworkProfile{},
	}
}

// Update feeds one signal reading; if the synthesized profile changed, subscribers are notified with
// the new profile (outside the lock, so a subscriber may read the policy without deadlocking).
func (p *Policy) Update(sig NetworkSignals) NetworkProfile {
	p.mu.Lock()
	prof, changed := p.synth.Next(sig)
	p.signals = sig
	p.profile = prof
	var subs []func(NetworkProfile)
	if changed {
		subs = append(subs, p.subs...)
	}
	p.mu.Unlock()
	for _, fn := range subs {
		fn(prof)
	}
	return prof
}

// Profile returns the current global profile.
func (p *Policy) Profile() NetworkProfile {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.profile
}

// Signals returns the last observed signals.
func (p *Policy) Signals() NetworkSignals {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return p.signals
}

// SetPeerOverride assigns a fixed profile for interactions with one peer (e.g. tighter retries for a
// known-flaky peer). Pass the zero profile to clear it.
func (p *Policy) SetPeerOverride(peer string, prof NetworkProfile) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if prof == (NetworkProfile{}) {
		delete(p.overrides, peer)
		return
	}
	p.overrides[peer] = prof
}

// PeerOverride returns the effective profile for a peer: its override if one is set, else the global
// profile.
func (p *Policy) PeerOverride(peer string) NetworkProfile {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if prof, ok := p.overrides[peer]; ok {
		return prof
	}
	return p.profile
}

// Subscribe registers fn to fire on every profile change.
func (p *Policy) Subscribe(fn func(NetworkProfile)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.subs = append(p.subs, fn)
}
