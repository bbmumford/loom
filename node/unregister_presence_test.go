/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"testing"

	"github.com/ORBTR/aether"
)

// unregisterMeshSessionWithoutHook has two "am I the owner" guards. The
// nil-expected guard binds `present` and refuses an absent entry; the
// non-nil-expected guard did not, so an absent entry produced current == nil,
// skipped the ownership comparison, and fell through to the teardown.
//
// Two concurrent teardowns for one peer reach that state: the second finds the
// entry already removed by the first.

func unregFixture(peer string, sess aether.Session) *ConnectionManager {
	m := registerTestManager()
	m.meshSessions = map[string]aether.Session{}
	if sess != nil {
		m.meshSessions[peer] = sess
	}
	return m
}

// 🔴 AN ABSENT ENTRY IS NOT SOMETHING THIS CALLER OWNS. Reporting a removal it
// did not perform makes unregisterMeshSession fire a disconnect hook for a
// session that was not there, counts a deletion in unregisterDeleted, and drops
// the peer's dial back-off via clearDedupRejectAllForPeer.
func TestUnregisteringAnAbsentSessionIsRefused(t *testing.T) {
	m := unregFixture("peer-a", nil) // no entry at all
	owned := &probeSession{local: "self", remote: "peer-a"}

	beforeSkipped := m.unregisterSkippedNotOwner.Load()
	beforeDeleted := m.unregisterDeleted.Load()

	if removed := m.unregisterMeshSessionWithoutHook("peer-a", owned); removed {
		t.Error("unregister reported a removal for a peer with no dispatch entry — the " +
			"caller then fires a disconnect hook for a session that was never registered")
	}
	if got := m.unregisterDeleted.Load(); got != beforeDeleted {
		t.Errorf("unregisterDeleted advanced from %d to %d without deleting anything",
			beforeDeleted, got)
	}
	if got := m.unregisterSkippedNotOwner.Load(); got != beforeSkipped+1 {
		t.Errorf("unregisterSkippedNotOwner = %d, want %d — the refusal was not counted "+
			"as a not-owner skip", got, beforeSkipped+1)
	}
}

// 🔬 THE CONTROL. A guard that refused everything would satisfy the test above
// while disabling session teardown entirely, so the owning caller must still
// remove its own session.
func TestUnregisteringAnOwnedSessionStillRemovesIt(t *testing.T) {
	owned := &probeSession{local: "self", remote: "peer-a"}
	m := unregFixture("peer-a", owned)

	if removed := m.unregisterMeshSessionWithoutHook("peer-a", owned); !removed {
		t.Fatal("the owning caller could not remove its own session — dispatch entries " +
			"are never cleaned up, so a dead session keeps receiving traffic")
	}
	m.dispatchMu.Lock()
	_, still := m.meshSessions["peer-a"]
	m.dispatchMu.Unlock()
	if still {
		t.Error("the dispatch entry survived its owner's unregister")
	}
}

// A different live session under the same peer must still be refused — the
// pre-existing half of the guard must survive the presence addition.
func TestUnregisteringSomeoneElsesSessionIsStillRefused(t *testing.T) {
	live := &probeSession{local: "self", remote: "peer-a"}
	stale := &probeSession{local: "self", remote: "peer-a"}
	m := unregFixture("peer-a", live)

	if removed := m.unregisterMeshSessionWithoutHook("peer-a", stale); removed {
		t.Error("an obsolete caller removed a newer session's dispatch entry")
	}
	m.dispatchMu.Lock()
	got := m.meshSessions["peer-a"]
	m.dispatchMu.Unlock()
	if got != live {
		t.Error("the live session was evicted by an obsolete caller")
	}
}
