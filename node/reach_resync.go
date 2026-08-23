/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"log"
	"time"

	lad "github.com/bbmumford/ledger"
)

// reachResyncInterval is how often the active reach-resync backstop
// sweeps for connected peers whose peer.addresses is empty or stale.
// Passive convergence via swarm gossip is the primary refresh path; this
// backstop catches the case where a peer record never arrived (lost
// announce, late join, gossip-storm rejected) and a peer sits at
// "(unresolved)" or with empty dial candidates indefinitely. 2 minutes
// matches the period the deleted reach-refresh ticker used.
const reachResyncInterval = 2 * time.Minute

// runReachResyncWalker drives the periodic reach-resync sweep. Started
// from ConnectionManager.Start as a sibling of the upgrade walker and
// scanAndConnect ticker. Stops when ctx is cancelled (typically on
// Stop).
//
// The period is a PARAMETER rather than a direct read of
// reachResyncInterval for one reason: this is a BACKSTOP, so if the loop
// ever stopped sweeping the symptom would not be an error — it would be
// the intermittent return of the original "(unresolved)" bug, months
// later, with nothing pointing here. A test cannot observe a second
// sweep on a 2-minute period, so without this seam the "keeps sweeping"
// property is permanently unverifiable. Production still passes
// reachResyncInterval from the single call site in Start, so the live
// period is unchanged and still declared in exactly one place.
//
// every <= 0 falls back to the constant instead of reaching
// time.NewTicker, which PANICS on a non-positive duration — and this
// runs in its own goroutine, where that panic takes the whole node down
// rather than failing locally.
func (m *ConnectionManager) runReachResyncWalker(ctx context.Context, every time.Duration) {
	if every <= 0 {
		every = reachResyncInterval
	}
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m.resyncStalePeerAddresses(ctx)
		}
	}
}

// resyncStalePeerAddresses scans every PeerConnected peer and, for any
// whose peer.addresses is empty or carries no UDP entry, re-pulls the
// swarm AddressTable's view and merges it back into peer.addresses.
// The case targeted is: peer connected (so we know about them), swarm
// AddressTable has data for them (so the publisher's record arrived
// somewhere), but the original scanAndConnect merge ran before the
// AddressTable had the record and never re-ran for this nodeID.
//
// Locks: takes m.mu briefly to enumerate then mutate peer.addresses.
// Doesn't take any swarm locks beyond what AddressTable.All() acquires
// internally.
func (m *ConnectionManager) resyncStalePeerAddresses(ctx context.Context) {
	if ctx.Err() != nil {
		return
	}
	if m.rt == nil || m.rt.swarm == nil || m.rt.swarm.AddressTable == nil {
		return
	}

	// Snapshot stale peers under m.mu so the rest of the work runs
	// outside the lock. A peer qualifies as stale when (a) it's
	// connected, (b) peer.addresses has no entry whose Proto names a
	// UDP-capable transport. Other transport entries staying around
	// is fine — those keep WS fallback working — but missing UDP
	// candidates blocks the noise-UDP upgrade path entirely.
	m.mu.Lock()
	staleNodeIDs := make([]string, 0)
	for nodeID, p := range m.peers {
		if p == nil || p.state != PeerConnected {
			continue
		}
		hasUDP := false
		for _, a := range p.addresses {
			if a.Proto == "udp" {
				hasUDP = true
				break
			}
		}
		if !hasUDP {
			staleNodeIDs = append(staleNodeIDs, nodeID)
		}
	}
	m.mu.Unlock()

	if len(staleNodeIDs) == 0 {
		return
	}

	resynced := 0
	for _, nodeID := range staleNodeIDs {
		cands := m.rt.swarm.AddressTable.Get(nodeID)
		if len(cands) == 0 {
			continue
		}
		additions := make([]lad.ReachAddress, 0, len(cands))
		for _, c := range cands {
			proto := swarmTransportToReachProto(c.Transport)
			if proto == "" || c.Host == "" || c.Port == 0 {
				continue
			}
			scope := c.Scope
			if scope == "" {
				scope = scopeForSwarmHost(c.Host)
			}
			additions = append(additions, lad.ReachAddress{
				Host:  c.Host,
				Port:  int(c.Port),
				Proto: proto,
				Scope: scope,
			})
		}
		if len(additions) == 0 {
			continue
		}

		m.mu.Lock()
		peer, ok := m.peers[nodeID]
		if !ok || peer == nil {
			m.mu.Unlock()
			continue
		}
		seen := make(map[lad.ReachAddress]struct{}, len(peer.addresses))
		for _, a := range peer.addresses {
			seen[a] = struct{}{}
		}
		oldProtos := make(map[string]bool, len(peer.addresses))
		for _, a := range peer.addresses {
			oldProtos[a.Proto] = true
		}
		mergedAny := false
		for _, a := range additions {
			if _, dup := seen[a]; dup {
				continue
			}
			peer.addresses = append(peer.addresses, a)
			seen[a] = struct{}{}
			mergedAny = true
			if !oldProtos[a.Proto] {
				switch a.Proto {
				case "udp":
					m.recordDialSuccess(peer.nodeID, ProtoNoiseUDP)
					m.recordDialSuccess(peer.nodeID, ProtoQUIC)
				case "wss":
					m.recordDialSuccess(peer.nodeID, ProtoWebSocket)
				case "tls":
					m.recordDialSuccess(peer.nodeID, ProtoTLS)
				}
			}
		}
		if mergedAny {
			peer.crossOrigin = m.isCrossOrigin(peer.addresses)
			resynced++
		}
		m.mu.Unlock()
	}

	if resynced > 0 {
		log.Printf("[REACH-RESYNC] merged AddressTable entries into %d previously-empty peer(s)", resynced)
	}
}
