/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"testing"
)

// AcceptMeshConnection's cleanup for a standby path that dies while the peer's
// primary is live. The primary is a Grade A session that owns dispatch; the standby
// is a Grade C session registerMeshSession accepts as a dormant fallback, so the
// peer's multipath manager holds both. The standby's OpenStream fails, so
// AcceptMeshConnection goes straight to its cleanup while the standby is not yet
// closed: the cleanup's OnPathFailure must mark only the standby dead and report no
// failover, so the primary keeps the manager's primary slot and dispatch, the
// failover counter does not move, and the cleanup closes the standby. A cleanup that
// fails the primary instead (OnPrimaryFailure) promotes the dying standby, points
// dispatch at it and returns before closing it.
func TestADyingStandbyLeavesThePrimaryAndDispatchAsTheyAre(t *testing.T) {
	ctx := context.Background()
	m := acceptTestManager()
	defer stopProving(t, m)

	primary := noiseSession()
	if !m.registerMeshSession(ctx, testNodeIDB, primary, true) {
		t.Fatal("premise wrong: the primary was refused")
	}
	m.addMultipathSession(testNodeIDB, primary, ProtoNoiseUDP)
	t.Cleanup(func() { m.removeMultipathSession(testNodeIDB, primary) })

	standby := wsSession()
	if err := m.AcceptMeshConnection(ctx, AcceptMeshConnectionOpts{
		NodeID:      testNodeIDB,
		Proto:       ProtoWebSocket,
		Session:     standby,
		IsInitiator: true,
	}); err != nil {
		t.Fatalf("the dying standby's AcceptMeshConnection returned %v", err)
	}
	if standby.opens.Load() == 0 {
		t.Fatal("premise wrong: the standby was refused before it reached the multipath manager and the cleanup")
	}

	if got := m.multipathFailoverTotal.Load(); got != 0 {
		t.Errorf("the dying standby's cleanup counted %d failovers, want 0", got)
	}
	m.dispatchMu.RLock()
	current := m.meshSessions[testNodeIDB]
	m.dispatchMu.RUnlock()
	if current != primary {
		t.Errorf("dispatch points at %v after the standby died, want the live primary", current)
	}
	mgr, ok := m.GetMultipathManager(testNodeIDB)
	if !ok {
		t.Fatal("the standby's cleanup dropped the peer's multipath manager while the primary holds a path")
	}
	if p, ok := mgr.PrimarySession(); !ok || p != primary {
		t.Errorf("the manager's primary is %v after the standby died, want the live primary", p)
	}
	if got := mgr.PathCount(); got != 1 {
		t.Errorf("the manager holds %d paths after the standby's cleanup, want the primary's 1", got)
	}
	if got := standby.closeCalls.Load(); got == 0 {
		t.Error("the cleanup left the dying standby open")
	}
	if got := primary.closeCalls.Load(); got != 0 {
		t.Errorf("the standby's cleanup closed the live primary %d times", got)
	}
}
