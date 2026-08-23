/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"time"

	"github.com/bbmumford/loom/core/directory/gossip"
	"github.com/bbmumford/loom/netpolicy"
)

// gossipCadenceLink is the link profile a mesh node paces its gossip cadence
// for. Server/relay nodes run on wired datacenter links; the synthesizer still
// tightens the cadence predictively when measured RTT rises over baseline, so
// this is the starting profile, not a fixed one.
const gossipCadenceLink = netpolicy.LinkEthernet

// gossipCadenceRefresh is how often the cadence policy is refreshed from
// measured peer RTT. It is finer than the runtime's re-bootstrap tick so the
// profile tracks a degrading link without waiting minutes, but coarse enough
// that the aggregation cost is negligible.
const gossipCadenceRefresh = 30 * time.Second

// gossipCadenceSynth is a conservative synthesizer config: a more-restricted
// link must hold for the confirm window before it sticks (hysteresis against
// flapping), RTT at 1.5× baseline counts as trending up, 5% loss is elevated,
// and the RTT baseline tracks each new sample at 0.2.
var gossipCadenceSynth = netpolicy.SynthConfig{
	ConfirmWindow: 30 * time.Second,
	RTTRiseFactor: 1.5,
	LossThreshold: 0.05,
	BaselineDecay: 0.2,
}

// startGossipCadence paces the gossip loop's adaptive-interval bounds from a
// live network profile when Config.AdaptiveGossipCadence is set. It installs the
// process-wide cadence source once — gossip.SetGossipCadence reads the policy's
// GossipBounds, which runGossipLoopInternal hands to NewAdaptiveInterval on every
// per-peer loop start — then refreshes the policy from measured peer RTT
// (GetPeerLatencies) on gossipCadenceRefresh. A no-op when the flag is off, in
// which case the gossip loop keeps its built-in (GossipInterval, 2s, 60s)
// envelope. The source is cleared when ctx ends so a torn-down runtime never
// leaves a stale cadence behind for a later runtime in the same process (tests).
func (rt *Runtime) startGossipCadence(ctx context.Context) {
	if !rt.cfg.AdaptiveGossipCadence {
		return
	}
	policy := netpolicy.NewPolicy(gossipCadenceLink, gossipCadenceSynth)
	gossip.SetGossipCadence(func() (base, min, max time.Duration, ok bool) {
		b, lo, hi := netpolicy.GossipBounds(policy)
		return b, lo, hi, true
	})
	rt.Go("gossip.cadence_feed", func() {
		ticker := time.NewTicker(gossipCadenceRefresh)
		defer ticker.Stop()
		defer gossip.SetGossipCadence(nil)
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				policy.Update(gossipSignals(gossipCadenceLink, rt.GetPeerLatencies()))
			}
		}
	})
}

// gossipSignals folds the per-peer RTT map into one NetworkSignals reading: the
// mean RTT over active peers and the peer count. Battery is reported as on-power
// (negative) — a mesh node is not a battery device, and 0 would read as a dead
// battery and wrongly restrict the cadence. An empty map yields a zero-RTT
// signal, which the synthesizer treats as "no measurement" and leaves the
// baseline untouched.
func gossipSignals(link netpolicy.LinkType, lat map[string]time.Duration) netpolicy.NetworkSignals {
	var sum time.Duration
	for _, d := range lat {
		sum += d
	}
	var avg time.Duration
	if len(lat) > 0 {
		avg = sum / time.Duration(len(lat))
	}
	return netpolicy.NetworkSignals{
		LinkType:     link,
		AvgRTT:       avg,
		PeerCount:    len(lat),
		BatteryLevel: -1,
	}
}
