/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"sync"
	"sync/atomic"
)

// Role affinity is the proactive counterpart to TrafficWeight's reactive
// per-peer scaling. TrafficWeight only bumps the target connection count
// AFTER RPCs have flowed; that leaves a freshly-deployed peer with a
// critical role (e.g. orbtr.signal on the only same-region devices node)
// at the K=2 floor until traffic first arrives. The first dispatch then
// pays the cold-start cost: the registry maps the FQN to that peer, the
// connection manager sees no session, the dial loop spins, and the
// caller waits.
//
// roleAffinityBonus adds an upfront K multiplier for any peer whose
// PeerRecord published roles intersect the local node's interest set.
// "Interest" is decomposed into three independent signals:
//
//	  +1 carryAny     peer publishes ≥1 role → warm at least one session
//	  +1 sameRegion   peer publishes ≥1 role AND is co-regional → intra-
//	                  region dispatch path stays inside the region
//	  +1 dispatchTgt  peer publishes ≥1 role we DON'T serve locally →
//	                  unique remote capability, prioritise over redundancy
//
// Each tier fires independently; bonuses stack so a same-region peer
// holding a role nobody else co-regional has gets +3. The TargetConnections
// final clamp still bounds the result by EffectiveMaxPerPeer, so a per-
// peer cap can't be exceeded; the scaler's adjustForGlobalBalance hotspot
// reduction still applies. The bonus is purely additive on top of the
// LatencyFactor × GradePenalty × FailureFactor × TrafficWeight base, so
// no existing behaviour regresses for peers without roles.
//
// Cache: roleAffinityBonus computes the bonus per peer on every Rebalance
// tick (10s default). The local-role lookup is O(roles_we_publish), the
// peer-role intersect is O(peer_roles × roles_we_publish); both are tiny
// (typically <20 elements). The single global affinityCache lives on
// ConnectionManager.affinityCache and is invalidated whenever
// PeerPublisher.SetRoles fires — see InvalidateLocalRoleCache.
type roleAffinity struct {
	carryAny       bool
	sameRegion     bool
	dispatchTarget bool
}

// total returns the sum of the per-tier bonuses (0-3). Bool→int via the
// idiomatic boolToInt helper keeps the hot path branch-free.
func (a roleAffinity) total() int {
	n := 0
	if a.carryAny {
		n++
	}
	if a.sameRegion {
		n++
	}
	if a.dispatchTarget {
		n++
	}
	return n
}

// peerRoleAffinity computes the affinity tiers for one peer. Returns
// zero-valued roleAffinity (total=0) when the swarm RoleTable hasn't
// observed the peer yet or the peer has no published roles — those
// cases collapse to today's behaviour (TargetConnections falls through
// to LatencyFactor × ... × K-floor).
//
// Lock discipline: peerRoleAffinity must NOT be called with m.mu held.
// RoleTable.PeerInfo and RoleTable.RolesOf acquire RoleTable.mu, which
// any swarm record applier might be holding while waiting on a callback
// chain that synchronises with m.mu downstream. Safe to call from the
// scaler's Rebalance loop which holds neither.
func (m *ConnectionManager) peerRoleAffinity(peerNodeID, peerRegion string) roleAffinity {
	if m == nil || m.rt == nil || m.rt.swarm == nil || m.rt.swarm.RoleTable == nil {
		return roleAffinity{}
	}
	peerRoles := m.rt.swarm.RoleTable.RolesOf(peerNodeID)
	if len(peerRoles) == 0 {
		return roleAffinity{}
	}
	local := m.localRoleSet()
	out := roleAffinity{
		carryAny:   true,
		sameRegion: peerRegion != "" && peerRegion == m.selfRegion,
	}
	for _, r := range peerRoles {
		if _, served := local[r]; !served {
			out.dispatchTarget = true
			break
		}
	}
	return out
}

// localRoleSet returns the set of roles this node publishes via its
// PeerPublisher. Sourced from rt.cfg.Roles plus any swarm-published
// roles added at runtime (e.g. "anchor" added when the anchor service
// starts after boot). Cached behind affinityCacheMu and invalidated by
// InvalidateLocalRoleCache; the cache survives a full Rebalance tick so
// every per-peer affinity computation reuses the same snapshot.
func (m *ConnectionManager) localRoleSet() map[string]struct{} {
	if m == nil || m.rt == nil {
		return map[string]struct{}{}
	}
	m.affinityCacheMu.RLock()
	cached := m.affinityCache
	m.affinityCacheMu.RUnlock()
	if cached != nil {
		return cached
	}
	// Rebuild. Pull canonical role list from PeerPublisher snapshot
	// (which already merges cfg.Roles + runtime additions like anchor).
	roles := m.rt.publishedRolesSnapshot()
	set := make(map[string]struct{}, len(roles))
	for _, r := range roles {
		if r != "" {
			set[r] = struct{}{}
		}
	}
	m.affinityCacheMu.Lock()
	// Re-check under write lock — another goroutine may have populated
	// the cache while we were rebuilding. Either snapshot is correct;
	// using the existing one avoids a needless rewrite of the pointer
	// (no atomic-store cache-line bounce in the common case).
	if m.affinityCache == nil {
		m.affinityCache = set
	} else {
		set = m.affinityCache
	}
	m.affinityCacheMu.Unlock()
	return set
}

// InvalidateLocalRoleCache clears the cached local-role set so the next
// peerRoleAffinity call rebuilds it. Called by Runtime.SetRoles /
// PeerPublisher.SetRoles whenever the local node's published role set
// changes (anchor role being added at boot, role drops during deployment,
// etc.). Cheap: a single nil-store under a write lock.
func (m *ConnectionManager) InvalidateLocalRoleCache() {
	if m == nil {
		return
	}
	m.affinityCacheMu.Lock()
	m.affinityCache = nil
	m.affinityCacheMu.Unlock()
}

// publishedRolesSnapshot returns the local node's published role set
// without exposing the PeerPublisher internals. Returns an empty slice
// when no publisher is configured (dev/test mode without swarm).
func (rt *Runtime) publishedRolesSnapshot() []string {
	if rt == nil {
		return nil
	}
	// PeerPublisher is the source of truth for the actively-advertised
	// role set; fall back to cfg.Roles when the publisher hasn't been
	// wired (e.g. pre-PublishRPCHandlersToLAD boot phase).
	if rt.swarm != nil && rt.swarm.Publisher != nil {
		if pub := rt.swarm.Publisher; pub != nil {
			if r := pub.Roles(); len(r) > 0 {
				return r
			}
		}
	}
	out := make([]string, 0, len(rt.cfg.Roles))
	for _, r := range rt.cfg.Roles {
		if r != "" {
			out = append(out, r)
		}
	}
	return out
}

// roleAffinityTelemetry tracks how often each affinity tier fires so the
// operator can quantify the bonus's effect via mesh_metrics. Surfaces
// under upgrade_walker_role_affinity_* keys in MeshMetrics.
type roleAffinityTelemetry struct {
	rebalanceTicks atomic.Uint64
	carryAnyHits   atomic.Uint64
	sameRegionHits atomic.Uint64
	dispatchHits   atomic.Uint64
	totalBonus     atomic.Uint64
}

// recordRebalanceObservation increments the relevant counters for one
// per-peer affinity computation during Rebalance.
func (t *roleAffinityTelemetry) recordRebalanceObservation(a roleAffinity) {
	if a.carryAny {
		t.carryAnyHits.Add(1)
	}
	if a.sameRegion {
		t.sameRegionHits.Add(1)
	}
	if a.dispatchTarget {
		t.dispatchHits.Add(1)
	}
	t.totalBonus.Add(uint64(a.total()))
}

// snapshot returns the telemetry counters as a tuple for MeshMetrics
// surfacing. Mirrors the WalkerStats accessor style.
func (t *roleAffinityTelemetry) snapshot() (ticks, carryAny, sameRegion, dispatch, totalBonus uint64) {
	return t.rebalanceTicks.Load(),
		t.carryAnyHits.Load(),
		t.sameRegionHits.Load(),
		t.dispatchHits.Load(),
		t.totalBonus.Load()
}

// roleAffinityFieldsForCM bundles the cache + telemetry into a single
// struct so peer_connections.go's ConnectionManager declaration only
// gains one field rather than five. Kept here so the affinity feature
// is self-contained.
type roleAffinityFields struct {
	cacheMu   sync.RWMutex
	cache     map[string]struct{}
	telemetry roleAffinityTelemetry
}
