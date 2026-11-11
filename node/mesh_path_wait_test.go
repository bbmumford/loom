/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"testing"
	"time"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/health"
)

// healthAtRTT is a monitor whose estimator has measured eight round trips of rtt, so a
// session's keepalive plan is past its warmup and its Interval follows rtt.
func healthAtRTT(rtt time.Duration) *health.Monitor {
	h := health.NewMonitor(0)
	for seq := uint32(1); seq <= 8; seq++ {
		h.RecordPingSent(seq)
		h.RecordPongRecv(seq, time.Now().Add(-rtt))
	}
	return h
}

// A dead path on a peer's multipath manager waits its own keepalive plan's Interval
// before the manager reconsiders it: the path whose round trips call for a 5 s cycle
// is back in standby after 5 s, and the path whose round trips call for a longer cycle
// is still dead then. The wait is the session's plan, as the keepalive probing that
// path reads it, so no fixed cycle holds a slow path to a fast path's wait.
func TestAPeersDeadPathWaitsItsOwnPlansInterval(t *testing.T) {
	t.Parallel()
	const peer = "vl1_path_wait"
	cm := &ConnectionManager{}
	primary := &keepaliveSession{health: seededHealth()}
	fast := &keepaliveSession{health: seededHealth()}
	slow := &keepaliveSession{health: healthAtRTT(10 * time.Second)}
	for _, s := range []*keepaliveSession{primary, fast, slow} {
		cm.addMultipathSession(peer, s, ProtoNoiseUDP)
	}
	t.Cleanup(func() {
		for _, s := range []*keepaliveSession{primary, fast, slow} {
			cm.removeMultipathSession(peer, s)
		}
	})
	mgr, ok := cm.GetMultipathManager(peer)
	if !ok {
		t.Fatal("no multipath manager for the peer")
	}
	if p, _ := mgr.PrimarySession(); p != primary {
		t.Fatal("premise wrong: the first path is not the primary")
	}
	fastWait, slowWait := aether.KeepalivePlanFor(fast).Interval, aether.KeepalivePlanFor(slow).Interval
	if slowWait < fastWait+5*time.Second {
		t.Fatalf("premise wrong: the slow path's plan Interval %v is not well past the fast path's %v", slowWait, fastWait)
	}

	if mgr.OnPathFailure(fast) || mgr.OnPathFailure(slow) {
		t.Fatal("premise wrong: a standby's failure reported a failover")
	}
	time.Sleep(fastWait + 500*time.Millisecond)
	mgr.PickPath(1) // reconsiders every dead path whose wait has passed

	live := map[aether.Session]bool{}
	for _, s := range mgr.AllSessions() {
		live[s] = true
	}
	if !live[fast] {
		t.Errorf("the fast path is still dead %v after its failure, past its plan's Interval %v", fastWait+500*time.Millisecond, fastWait)
	}
	if live[slow] {
		t.Errorf("the slow path was reconsidered %v after its failure, before its plan's Interval %v", fastWait+500*time.Millisecond, slowWait)
	}
}
