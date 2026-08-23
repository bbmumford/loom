/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"testing"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/rpc/pb"
)

// Covers CallViaBidi's "no usable bidi" contract and evictDeadBidiForSession.
//
// From CallViaBidi's own comment: when a session closes, BidiRPC.readLoop exits and
// closes `done` BEFORE unregisterMeshSession evicts the bidi entry. A call
// inside that window gets a still-registered-but-dead bidi. Without the
// IsAlive check the dispatch layer surfaced "bidi stream closed" as the probe
// error and SKIPPED its dynamic-stream fallback — which produced fleet-wide
// "all 3 probes failed: bidi stream closed" auth-callback failures
// (CompleteOAuth) when WS sessions to cross-org anchors churned.
//
// The contract that fixes it: a dead bidi must be reported as NO BIDI —
// (nil, false, nil) — so the caller falls back, and the stale entry must be
// evicted so the next call goes straight to the dynamic-stream path.
//
// The fixture is small because IsAlive() only reads a channel, so a dead bidi
// is a struct literal holding a closed one.

// deadBidi() builds a BidiRPC whose readLoop has already exited.
func deadBidi() *BidiRPC {
	done := make(chan struct{})
	close(done)
	return &BidiRPC{done: done}
}

// liveBidi() builds one whose readLoop is still running.
func liveBidi() *BidiRPC {
	return &BidiRPC{done: make(chan struct{})}
}

func bidiManager(t *testing.T) (*ConnectionManager, aether.Session) {
	t.Helper()
	m := registerTestManager()
	m.selfID = testNodeIDA
	m.rt = &Runtime{}
	s := wsSession()
	s.remote = aether.NodeID(testNodeIDB)
	m.dispatchMu.Lock()
	m.meshSessions = map[string]aether.Session{testNodeIDB: s}
	m.bidisBySession = map[aether.Session]*BidiRPC{}
	m.dispatchMu.Unlock()
	return m, s
}

// 🔴 THE INCIDENT: a registered-but-dead bidi must read as "no bidi".
func TestCallViaBidiReportsADeadBidiAsNoBidiAndEvictsIt(t *testing.T) {
	m, sess := bidiManager(t)
	dead := deadBidi()
	m.dispatchMu.Lock()
	m.bidisBySession[sess] = dead
	m.dispatchMu.Unlock()

	if got, ok := m.GetBidiRPC(testNodeIDB); !ok || got != dead {
		t.Fatal("premise wrong: the dead bidi is not registered, so this test " +
			"would pass for a manager that simply has no bidi at all")
	}
	if dead.IsAlive() {
		t.Fatal("premise wrong: the fixture bidi is alive")
	}

	resp, ok, err := m.CallViaBidi(context.Background(), testNodeIDB, &pb.RPCRequest{})

	if ok {
		t.Fatal("CallViaBidi reported SUCCESS on a dead bidi — the dispatch " +
			"layer treats that as a routing result and skips its dynamic-stream " +
			"fallback, which is the 'all 3 probes failed: bidi stream closed' " +
			"incident")
	}
	if resp != nil || err != nil {
		t.Fatalf("want (nil,false,nil) so the caller falls back cleanly; got "+
			"resp=%v err=%v — a non-nil error here is surfaced as a routing "+
			"FAILURE rather than as 'no bidi available'", resp, err)
	}

	// And the stale entry must be gone, so the next call does not repeat the
	// probe against the same corpse.
	m.dispatchMu.RLock()
	_, still := m.bidisBySession[sess]
	m.dispatchMu.RUnlock()
	if still {
		t.Fatal("the dead bidi was left registered — every subsequent call " +
			"re-probes it and falls back again, permanently, until the session " +
			"is torn down")
	}
}

// No session at all: there is nothing to be dead, and the answer is still
// "no bidi" rather than an error.
func TestCallViaBidiWithNoSessionIsNotAnError(t *testing.T) {
	m, _ := bidiManager(t)
	const unknown = "cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33"

	resp, ok, err := m.CallViaBidi(context.Background(), unknown, &pb.RPCRequest{})
	if ok || resp != nil || err != nil {
		t.Fatalf("want (nil,false,nil) for a peer with no session; got ok=%v "+
			"resp=%v err=%v", ok, resp, err)
	}
}

// A session with no bidi registered is the ordinary pre-bidi state, not a
// fault — the caller uses a dynamic stream.
func TestCallViaBidiWithNoBidiRegisteredIsNotAnError(t *testing.T) {
	m, _ := bidiManager(t) // session bound, bidisBySession left empty

	resp, ok, err := m.CallViaBidi(context.Background(), testNodeIDB, &pb.RPCRequest{})
	if ok || resp != nil || err != nil {
		t.Fatalf("want (nil,false,nil) with no bidi registered; got ok=%v "+
			"resp=%v err=%v", ok, resp, err)
	}
}

// evictDeadBidiForSession is session-AWARE: it must not evict an entry that a
// newer bidi already owns — the same stale-cleanup-races-fresher-registration
// shape removeTransport guards against.
func TestEvictDeadBidiLeavesAFresherEntryAlone(t *testing.T) {
	m, sess := bidiManager(t)
	stale, fresh := deadBidi(), liveBidi()
	m.dispatchMu.Lock()
	m.bidisBySession[sess] = fresh // the fresher bidi owns the slot
	m.dispatchMu.Unlock()

	m.evictDeadBidiForSession(sess, stale)

	m.dispatchMu.RLock()
	got, ok := m.bidisBySession[sess]
	m.dispatchMu.RUnlock()
	if !ok || got != fresh {
		t.Fatal("a STALE bidi's eviction removed the FRESH one — the session " +
			"loses its live bidi and every call falls back to a dynamic stream " +
			"even though a working bidi had just been registered")
	}
}
