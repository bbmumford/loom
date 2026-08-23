/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"testing"
	"time"
)

// COVERAGE of the scaler's traffic counters and hotspot adjustment,
// all 0.0%: rpcCounter.record, rpcCounter.rpcsLastMinute, RecordRPC,
// pruneCountersFor, adjustForGlobalBalance.
//
// These are the INPUTS the scaler uses to decide drains — the loop the
// v0.0.228-era WS flap fed. Every failure here is silent: the
// scaler simply decides wrongly, and a wrong drain looks like normal churn.

// 🔴 THE LOAD-BEARING PROPERTY, AND IT IS EASY TO GET WRONG BY SIMPLIFYING.
//
// rpcsLastMinute computes its OWN cutoff rather than trusting that pruning
// has happened. If it were shortened to `return len(r.calls)` every test
// below that only records fresh calls would still pass — but a peer that went
// quiet would report its last busy minute FOREVER, and the scaler would never
// scale it down.
func TestRpcsLastMinuteExcludesStaleCallsWithoutRelyingOnPruning(t *testing.T) {
	rc := &rpcCounter{}

	// Seed directly: calls older than the window, never pruned because
	// record() (the only pruner) has not run.
	rc.calls = []time.Time{
		time.Now().Add(-10 * time.Minute),
		time.Now().Add(-5 * time.Minute),
		time.Now().Add(-90 * time.Second),
	}
	if len(rc.calls) == 0 {
		t.Fatal("premise wrong: the fixture is empty, so a zero result below " +
			"would be self-fulfilling")
	}

	if got := rc.rpcsLastMinute(); got != 0 {
		t.Fatalf("rpcsLastMinute = %d with only stale entries, want 0 — a peer "+
			"that has gone quiet keeps reporting its last busy minute and the "+
			"scaler will never scale it down", got)
	}

	// Positive control on the SAME counter: the zero above must be the cutoff
	// working, not the counter being inert.
	rc.calls = append(rc.calls, time.Now())
	if got := rc.rpcsLastMinute(); got != 1 {
		t.Fatalf("rpcsLastMinute = %d after a fresh call, want 1 — the counter "+
			"does not observe traffic at all, which would make the assertion "+
			"above meaningless", got)
	}
}

// record() is the only pruner. Without it the slice grows for the lifetime of
// the process on any peer that keeps sending.
func TestRecordPrunesEntriesThatHaveLeftTheWindow(t *testing.T) {
	rc := &rpcCounter{calls: []time.Time{
		time.Now().Add(-10 * time.Minute),
		time.Now().Add(-5 * time.Minute),
		time.Now().Add(-2 * time.Minute),
	}}
	before := len(rc.calls)

	rc.record()

	if len(rc.calls) != 1 {
		t.Fatalf("calls = %d entries after recording one fresh call over %d "+
			"stale ones, want 1 — stale timestamps are retained forever and the "+
			"per-peer counter grows without bound", len(rc.calls), before)
	}
	if got := rc.rpcsLastMinute(); got != 1 {
		t.Fatalf("rpcsLastMinute = %d, want 1 — pruning dropped the fresh call "+
			"along with the stale ones", got)
	}
}

func scalerFixture() *ConnectionScaler {
	return NewConnectionScaler(&ConnectionManager{budget: DefaultConnectionBudget()}, nil)
}

// RecordRPC lazily creates the per-peer counter. The second call must REUSE
// it — replacing it would reset the peer's traffic history on every RPC, so
// TrafficWeight would read 1 forever regardless of real load.
func TestRecordRPCCreatesOnePerPeerCounterAndKeepsIt(t *testing.T) {
	s := scalerFixture()

	s.RecordRPC(testNodeIDA)
	s.mu.Lock()
	first, ok := s.rpcCounters[testNodeIDA]
	s.mu.Unlock()
	if !ok {
		t.Fatal("no counter was created for a peer that made an RPC")
	}

	s.RecordRPC(testNodeIDA)
	s.mu.Lock()
	second := s.rpcCounters[testNodeIDA]
	s.mu.Unlock()

	if second != first {
		t.Fatal("the second RPC REPLACED the peer's counter — its traffic " +
			"history resets on every call, so TrafficWeight reads 1 no matter " +
			"how busy the peer is")
	}
	if got := first.rpcsLastMinute(); got != 2 {
		t.Fatalf("rpcsLastMinute = %d after two RPCs, want 2", got)
	}

	// A different peer must not share the counter.
	s.RecordRPC(testNodeIDB)
	if got := first.rpcsLastMinute(); got != 2 {
		t.Fatalf("another peer's RPC was billed to this one (%d, want 2)", got)
	}
}

// pruneCountersFor is the leak guard for the scaler's satellite map: without
// it, rpcCounters keeps an entry for every peer ever seen while the main
// peers map shrinks.
func TestPruneCountersForRemovesOnlyWhatThePredicateSelects(t *testing.T) {
	s := scalerFixture()
	s.RecordRPC(testNodeIDA)
	s.RecordRPC(testNodeIDB)
	if len(s.rpcCounters) != 2 {
		t.Fatalf("premise wrong: %d counters, want 2", len(s.rpcCounters))
	}

	s.pruneCountersFor(func(id string) bool { return id == testNodeIDA })

	if _, still := s.rpcCounters[testNodeIDA]; still {
		t.Fatal("the selected peer's counter survived the prune — the scaler's " +
			"satellite map grows for the life of the process")
	}
	if _, kept := s.rpcCounters[testNodeIDB]; !kept {
		t.Fatal("an UNSELECTED peer's counter was deleted — the prune ignores " +
			"its predicate and discards live peers' traffic history")
	}
}

// adjustForGlobalBalance steers new connections away from hotspots, but must
// never push a peer below MinPerPeer — that floor is what keeps a busy node
// reachable at all.
func TestAdjustForGlobalBalanceSteersOffHotspotsButHonoursTheFloor(t *testing.T) {
	const rawTarget = 3

	t.Run("no connection map is a passthrough", func(t *testing.T) {
		s := scalerFixture() // connectionMap nil
		if got := s.adjustForGlobalBalance(testNodeIDA, rawTarget); got != rawTarget {
			t.Fatalf("target = %d without a gossip map, want %d unchanged — the "+
				"scaler is acting on hotspot data it does not have", got, rawTarget)
		}
	})

	t.Run("a hotspot is reduced by one", func(t *testing.T) {
		s := scalerFixture()
		cm := NewConnectionMap()
		cm.Update(testNodeIDA, 10, 50) // far above the mesh mean
		cm.Update(testNodeIDB, 1, 50)
		cm.Update("cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33", 1, 50)
		s.connectionMap = cm

		if !cm.IsHotspot(testNodeIDA) {
			t.Fatal("premise wrong: the fixture peer is not a hotspot, so the " +
				"assertion below would pass for the wrong reason")
		}
		if got, want := s.adjustForGlobalBalance(testNodeIDA, rawTarget), rawTarget-1; got != want {
			t.Fatalf("hotspot target = %d, want %d — new connections keep being "+
				"steered toward the most loaded node in the mesh", got, want)
		}
		// A non-hotspot peer in the same map must be untouched.
		if got := s.adjustForGlobalBalance(testNodeIDB, rawTarget); got != rawTarget {
			t.Fatalf("non-hotspot target = %d, want %d — every peer is being "+
				"treated as a hotspot", got, rawTarget)
		}
	})

	// 🛑 THE THREE-PEER FIXTURE IS LOAD-BEARING AND TWO PEERS MADE THIS TEST
	// VACUOUS — caught only by mutation.
	//
	// IsHotspot requires conns > 2 × mesh mean. With just {10, 1} the mean is
	// 5.5, the bar is 11, and the 10-connection peer IS NOT A HOTSPOT — so the
	// branch under test never executed and "returns the floor unchanged" was
	// satisfied by the non-hotspot passthrough. Adding a third peer drops the
	// mean to 4 and clears the bar at 8. The premise assertion below is what
	// the first version of this subtest was missing.
	t.Run("the MinPerPeer floor is never breached", func(t *testing.T) {
		s := scalerFixture()
		cm := NewConnectionMap()
		cm.Update(testNodeIDA, 10, 50)
		cm.Update(testNodeIDB, 1, 50)
		cm.Update("cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33", 1, 50)
		s.connectionMap = cm
		floor := s.connMgr.budget.MinPerPeer
		if floor < 1 {
			t.Fatalf("premise wrong: MinPerPeer = %d, so there is no floor to "+
				"breach and this test proves nothing", floor)
		}
		if !cm.IsHotspot(testNodeIDA) {
			t.Fatal("premise wrong: the peer is not a hotspot, so the reduction " +
				"branch never runs and the floor is never actually tested")
		}

		if got := s.adjustForGlobalBalance(testNodeIDA, floor); got != floor {
			t.Fatalf("a hotspot already AT MinPerPeer was reduced to %d, want %d "+
				"— the busiest node in the mesh gets driven toward zero "+
				"connections and becomes unreachable", got, floor)
		}
	})
}
