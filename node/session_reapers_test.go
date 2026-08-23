/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"testing"
	"time"

	"github.com/ORBTR/aether"
	"github.com/bbmumford/swarm"
)

func TestReapMultipathClosedRemovesNilAndClosedPaths(t *testing.T) {
	m := registerTestManager()
	live := noiseSession()
	closed := wsSession()
	closed.closed = true

	mgr := withMultipath(t, m, testNodeIDA, nil, closed, live)
	m.reapMultipathClosed(testNodeIDA)

	got, ok := m.GetMultipathManager(testNodeIDA)
	if !ok || got != mgr {
		t.Fatal("a manager with a live path was dropped while reaping dead siblings")
	}
	if got.PathCount() != 1 {
		t.Fatalf("PathCount = %d, want 1: nil/closed paths still pin the manager", got.PathCount())
	}
	sessions := got.AllSessions()
	if len(sessions) != 1 || sessions[0] != aether.Session(live) {
		t.Fatalf("remaining sessions = %v, want only the live path", sessions)
	}

	allDead := wsSession()
	allDead.closed = true
	withMultipath(t, m, testNodeIDB, allDead)
	m.reapMultipathClosed(testNodeIDB)
	if _, ok := m.GetMultipathManager(testNodeIDB); ok {
		t.Fatal("an all-closed multipath manager survived the reap")
	}

	// An absent manager is an ordinary race outcome, not an error.
	m.reapMultipathClosed("absent")
}

func TestSweepZombieSessionsReapsOrphansAndPreservesLiveState(t *testing.T) {
	m := registerTestManager()
	deadID, liveID, orphanID := "dead-peer", "live-peer", "orphan-peer"
	dead := wsSession()
	dead.closed = true
	live := noiseSession()
	orphan := wsSession()
	orphan.closed = true

	m.meshSessions = map[string]aether.Session{
		deadID: dead,
		liveID: live,
	}
	m.peers = map[string]*peerConn{
		deadID:   {nodeID: deadID, state: PeerConnected, connCount: 2},
		liveID:   {nodeID: liveID, state: PeerConnected, connCount: 1},
		orphanID: {nodeID: orphanID, state: PeerConnected, connCount: 1},
	}
	withMultipath(t, m, deadID, dead)
	liveMgr := withMultipath(t, m, liveID, live)
	withMultipath(t, m, orphanID, orphan)

	freshTimer := time.NewTimer(time.Hour)
	t.Cleanup(func() { freshTimer.Stop() })
	m.proving = map[string]*provingSession{
		"nil-proving": nil,
		"old-proving": {startedAt: time.Now().Add(-2 * time.Minute)},
		"fresh":       {startedAt: time.Now(), timer: freshTimer},
	}
	m.gossipActive = map[string]*gossipOwner{
		deadID:   nil,
		liveID:   nil,
		orphanID: nil,
	}

	m.sweepZombieSessions()

	if _, ok := m.meshSessions[deadID]; ok {
		t.Fatal("closed session survived the zombie sweep")
	}
	if got := m.meshSessions[liveID]; got != aether.Session(live) {
		t.Fatal("live session was replaced or removed by the zombie sweep")
	}
	for _, id := range []string{deadID, orphanID} {
		p := m.peers[id]
		if p.state != PeerDisconnected || p.connCount != 0 {
			t.Fatalf("%s state/count = %s/%d, want disconnected/0", id, p.state, p.connCount)
		}
		if _, ok := m.GetMultipathManager(id); ok {
			t.Fatalf("%s retained an empty multipath manager", id)
		}
		if _, ok := m.gossipActive[id]; ok {
			t.Fatalf("%s retained an orphan gossip owner", id)
		}
	}
	if p := m.peers[liveID]; p.state != PeerConnected || p.connCount != 1 {
		t.Fatalf("live peer state/count = %s/%d, want connected/1", p.state, p.connCount)
	}
	if got, ok := m.GetMultipathManager(liveID); !ok || got != liveMgr || got.PathCount() != 1 {
		t.Fatal("live peer's multipath manager did not survive intact")
	}
	if _, ok := m.gossipActive[liveID]; !ok {
		t.Fatal("live peer's gossip owner was removed")
	}
	if _, ok := m.proving["nil-proving"]; ok {
		t.Fatal("nil proving entry survived")
	}
	if _, ok := m.proving["old-proving"]; ok {
		t.Fatal("expired proving entry survived")
	}
	if _, ok := m.proving["fresh"]; !ok {
		t.Fatal("fresh proving entry was reaped")
	}
}

func TestNilZombieSnapshotIsStillIdentityGuarded(t *testing.T) {
	t.Run("unchanged nil slot is removed", func(t *testing.T) {
		m := registerTestManager()
		m.meshSessions = map[string]aether.Session{"peer": nil}

		m.unregisterMeshSession("peer", nil)

		if _, ok := m.meshSessions["peer"]; ok {
			t.Fatal("the captured nil zombie slot survived")
		}
	})

	t.Run("live replacement survives stale nil snapshot", func(t *testing.T) {
		m := registerTestManager()
		m.meshSessions = map[string]aether.Session{"peer": nil}
		captured := m.meshSessions["peer"]
		live := noiseSession()

		// This is the exact sweep race: after Phase 1 releases dispatchMu,
		// registration replaces the captured nil slot before unregister runs.
		m.meshSessions["peer"] = live
		beforeSkipped := m.unregisterSkippedNotOwner.Load()
		beforeDeleted := m.unregisterDeleted.Load()

		m.unregisterMeshSession("peer", captured)

		if got := m.meshSessions["peer"]; got != aether.Session(live) {
			t.Fatal("the stale nil snapshot deleted the live replacement")
		}
		if got := m.unregisterSkippedNotOwner.Load(); got != beforeSkipped+1 {
			t.Fatalf("skipped-not-owner = %d, want %d", got, beforeSkipped+1)
		}
		if got := m.unregisterDeleted.Load(); got != beforeDeleted {
			t.Fatalf("unregisterDeleted = %d, want unchanged %d", got, beforeDeleted)
		}
	})
}

func TestZombieSweepDoesNotDowngradeLiveReplacement(t *testing.T) {
	for _, tc := range []struct {
		name     string
		captured func() aether.Session
	}{
		{name: "nil snapshot", captured: func() aether.Session { return nil }},
		{name: "closed snapshot", captured: func() aether.Session {
			closed := wsSession()
			closed.closed = true
			return closed
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const nodeID = "replacement-peer"
			captured := tc.captured()
			live := noiseSession()
			m, tombstones, cache := tombstoneFixture(swarm.RoleAnchor)
			m.meshSessions = map[string]aether.Session{nodeID: live}
			m.meshSessionInitiators = map[string]bool{nodeID: true}
			m.sessionInitiators = map[aether.Session]bool{live: true}
			m.sessionRegisteredAt = map[string]time.Time{nodeID: time.Now()}
			m.peers[nodeID] = &peerConn{
				nodeID:    nodeID,
				state:     PeerConnected,
				connCount: 2,
			}
			liveMgr := withMultipath(t, m, nodeID, live)
			m.gossipActive = map[string]*gossipOwner{nodeID: nil}
			seenAgo(cache, nodeID, 5*time.Minute)
			cache.SetGossipLivenessOverride(nodeID, true)

			m.reapZombieSessionSnapshots([]zombieSessionSnapshot{{
				nodeID: nodeID,
				sess:   captured,
			}})

			if got := m.meshSessions[nodeID]; got != aether.Session(live) {
				t.Fatal("stale zombie snapshot removed the live replacement")
			}
			if p := m.peers[nodeID]; p.state != PeerConnected || p.connCount != 2 {
				t.Fatalf("replacement peer state/count = %s/%d, want connected/2",
					p.state, p.connCount)
			}
			if got, ok := m.GetMultipathManager(nodeID); !ok || got != liveMgr || got.PathCount() != 1 {
				t.Fatal("replacement peer lost its live multipath path")
			}
			if _, ok := m.gossipActive[nodeID]; !ok {
				t.Fatal("replacement peer lost its live gossip ownership")
			}
			if cache.LastGossipSeen(nodeID).IsZero() {
				t.Fatal("replacement peer lost its LAD gossip liveness")
			}
			if len(tombstones.published) != 0 {
				t.Fatalf("replacement peer emitted observer tombstones: %v", tombstones.published)
			}
		})
	}
}

func TestZombieSweepCompletesCleanupBeforeFreshAdmission(t *testing.T) {
	const nodeID = "unregister-then-admit-peer"
	dead := wsSession()
	dead.closed = true
	live := noiseSession()
	m, tombstones, cache := tombstoneFixture(swarm.RoleAnchor)
	m.meshSessions = map[string]aether.Session{nodeID: dead}
	m.meshSessionInitiators = map[string]bool{nodeID: false}
	m.sessionInitiators = map[aether.Session]bool{dead: false}
	m.sessionRegisteredAt = map[string]time.Time{nodeID: time.Now().Add(-time.Minute)}
	m.peers[nodeID] = &peerConn{
		nodeID:    nodeID,
		state:     PeerConnected,
		connCount: 1,
	}
	withMultipath(t, m, nodeID, dead)
	m.gossipActive = map[string]*gossipOwner{nodeID: nil}
	seenAgo(cache, nodeID, 5*time.Second)
	cache.SetGossipLivenessOverride(nodeID, true)

	// The removal hook is the deterministic edge immediately after a
	// successful unregister. Before the lifecycle transaction it ran before
	// peer-wide cleanup, so this re-entrant fresh admission was subsequently
	// downgraded to disconnected/count=0. The repaired sweep defers the hook
	// until cleanup and the lifecycle lock are both complete.
	admitted := make(chan bool, 1)
	m.SetSessionHook(func(gotNodeID string, _ aether.Session, joined bool) {
		if gotNodeID != nodeID || joined {
			return
		}
		lifecycleMu := m.sessionLifecycleLock(nodeID)
		lifecycleMu.Lock()
		m.mu.Lock()
		p := m.peers[nodeID]
		p.state = PeerConnected
		p.connCount = 1
		m.mu.Unlock()
		if !m.registerMeshSession(context.Background(), nodeID, live, true) {
			lifecycleMu.Unlock()
			admitted <- false
			return
		}
		m.addMultipathSession(nodeID, live, ProtoNoiseUDP)
		m.gossipMu.Lock()
		m.gossipActive[nodeID] = nil
		m.gossipMu.Unlock()
		seenAgo(cache, nodeID, 5*time.Second)
		cache.SetGossipLivenessOverride(nodeID, true)
		lifecycleMu.Unlock()
		admitted <- true
	})

	sweepDone := make(chan struct{})
	go func() {
		m.sweepZombieSessions()
		close(sweepDone)
	}()

	select {
	case <-sweepDone:
	case <-time.After(2 * time.Second):
		t.Fatal("zombie sweep deadlocked while the removal hook re-entered admission")
	}
	select {
	case ok := <-admitted:
		if !ok {
			t.Fatal("fresh admission was rejected after the zombie cleanup transaction")
		}
	default:
		t.Fatal("successful unregister did not invoke the post-cleanup admission hook")
	}

	if got := m.meshSessions[nodeID]; got != aether.Session(live) {
		t.Fatal("fresh admission lost dispatch ownership after zombie cleanup")
	}
	if p := m.peers[nodeID]; p.state != PeerConnected || p.connCount != 1 {
		t.Fatalf("fresh peer state/count = %s/%d, want connected/1", p.state, p.connCount)
	}
	if mgr, ok := m.GetMultipathManager(nodeID); !ok || mgr.PathCount() != 1 ||
		len(mgr.AllSessions()) != 1 || mgr.AllSessions()[0] != aether.Session(live) {
		t.Fatal("fresh admission did not retain its sole live multipath path")
	}
	m.gossipMu.Lock()
	_, gossipLive := m.gossipActive[nodeID]
	m.gossipMu.Unlock()
	if !gossipLive {
		t.Fatal("fresh admission lost its gossip ownership")
	}
	if lastSeen := cache.LastGossipSeen(nodeID); lastSeen.IsZero() ||
		time.Since(lastSeen) >= observerSilenceThreshold {
		t.Fatalf("fresh admission lost LAD/gossip liveness: lastSeen=%v", lastSeen)
	}
	if len(tombstones.published) != 0 {
		t.Fatalf("fresh admission emitted stale observer tombstones: %v", tombstones.published)
	}
}

func TestPruneStalePeersHonoursStateAgeAndActivity(t *testing.T) {
	now := time.Now()
	staleConnected := now.Add(-stalePeerTTL - time.Minute)
	freshConnected := now.Add(-stalePeerTTL + time.Minute)
	staleDiscovered := now.Add(-staleDiscoveryTTL - time.Minute)
	freshDiscovered := now.Add(-staleDiscoveryTTL + time.Minute)

	m := registerTestManager()
	m.peers = map[string]*peerConn{
		"stale-disconnected": {
			nodeID: "stale-disconnected", state: PeerDisconnected, lastConnected: staleConnected,
		},
		"fresh-disconnected": {
			nodeID: "fresh-disconnected", state: PeerDisconnected, lastConnected: freshConnected,
		},
		"stale-never-connected": {
			nodeID: "stale-never-connected", state: PeerDisconnected, discoveredAt: staleDiscovered,
		},
		"stale-discovered": {
			nodeID: "stale-discovered", state: PeerDiscovered, discoveredAt: staleDiscovered,
		},
		"fresh-discovered": {
			nodeID: "fresh-discovered", state: PeerDiscovered, discoveredAt: freshDiscovered,
		},
		"active-transport": {
			nodeID: "active-transport", state: PeerDisconnected, lastConnected: staleConnected, connCount: 1,
		},
		"draining": {
			nodeID: "draining", state: PeerDisconnected, lastConnected: staleConnected, drainState: DrainStarted,
		},
		"connected-state": {
			nodeID: "connected-state", state: PeerConnected, lastConnected: staleConnected,
		},
		"reconnecting-state": {
			nodeID: "reconnecting-state", state: PeerReconnecting, lastConnected: staleConnected,
		},
		"no-age-evidence": {
			nodeID: "no-age-evidence", state: PeerDisconnected,
		},
	}
	staleMgr := withMultipath(t, m, "stale-disconnected")
	_ = staleMgr
	freshPath := noiseSession()
	freshMgr := withMultipath(t, m, "fresh-disconnected", freshPath)

	m.pruneStalePeers()

	for _, id := range []string{"stale-disconnected", "stale-never-connected", "stale-discovered"} {
		if _, ok := m.peers[id]; ok {
			t.Errorf("%s survived past its applicable TTL", id)
		}
		if _, ok := m.GetMultipathManager(id); ok {
			t.Errorf("%s retained satellite multipath state after eviction", id)
		}
	}
	for _, id := range []string{
		"fresh-disconnected", "fresh-discovered", "active-transport", "draining",
		"connected-state", "reconnecting-state", "no-age-evidence",
	} {
		if _, ok := m.peers[id]; !ok {
			t.Errorf("%s was pruned without the complete state/age/activity predicate", id)
		}
	}
	if got, ok := m.GetMultipathManager("fresh-disconnected"); !ok || got != freshMgr || got.PathCount() != 1 {
		t.Fatal("a retained peer lost its live multipath satellite state")
	}
}
