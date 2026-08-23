/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"testing"
	"time"
)

// COVERAGE of the upgrade-walker consumer chain — the second half of the
// entry condition, across all six functions:
// markWalkerPendingSession, consumeWalkerPendingSession,
// drainStaleWalkerPendingSessions, SignalWalkerWake, WalkerStats,
// WalkerWakeStats.
//
// 🛑 THIS CHAIN IS ATTRIBUTION AND BACKPRESSURE, SO EVERY FAILURE MODE IS
// SILENT. Nothing here returns an error to anyone. A broken tag/consume
// pair does not fail a dial — it mis-bills a probe, and the walker's own
// telemetry (the thing an operator would consult) is what goes wrong.
// A blocking wake does not error either; it stalls the gossip record
// path that calls it.

func pendingTestManager() *ConnectionManager {
	return &ConnectionManager{walkerWakeCh: make(chan struct{}, 1)}
}

// 🔑 CONSUME IS ONE-SHOT, AND THAT IS THE POINT OF IT.
// registerMeshSession calls consume on the upgrade-promote branch to
// decide whether to bill a proving session to the walker. If a tag
// survived consumption, a SECOND unrelated session for the same peer
// would also be attributed to the walker, inflating provingSucceeded —
// the counter WalkerStats documents as "the honest upgrade-actually-
// stuck count", which is exactly the number nobody could sanity-check.
func TestWalkerPendingTagIsConsumedExactlyOnce(t *testing.T) {
	m := pendingTestManager()
	m.markWalkerPendingSession(testNodeIDA)

	if !m.consumeWalkerPendingSession(testNodeIDA) {
		t.Fatal("a tagged peer did not consume — walker probes are never " +
			"attributed and provingSucceeded stays at zero forever")
	}
	if m.consumeWalkerPendingSession(testNodeIDA) {
		t.Fatal("the tag survived consumption — a second session for the same " +
			"peer is billed to the walker too, inflating the proving counters")
	}
}

func TestWalkerPendingConsumeIsFalseWithoutATag(t *testing.T) {
	m := pendingTestManager()

	// Nil map (never marked) must not panic and must not claim a tag.
	if m.walkerPendingSessions != nil {
		t.Fatal("premise wrong: this case needs the lazily-initialised map to be nil")
	}
	if m.consumeWalkerPendingSession(testNodeIDA) {
		t.Fatal("consume claimed a tag from a nil map")
	}

	// Populated map, different peer — attribution must not leak across peers.
	m.markWalkerPendingSession(testNodeIDA)
	if m.consumeWalkerPendingSession(testNodeIDB) {
		t.Fatal("peer B consumed peer A's tag — walker attribution is not " +
			"keyed by peer, so any probe would be billed to any session")
	}
	if !m.consumeWalkerPendingSession(testNodeIDA) {
		t.Fatal("peer A's tag was destroyed by peer B's lookup")
	}
}

// An empty peerID must be rejected at BOTH ends. The key is a peer
// nodeID that arrives from a dial path; a "" key would collide with
// every other unidentified probe and let one consume another's tag.
func TestWalkerPendingRejectsEmptyPeerID(t *testing.T) {
	m := pendingTestManager()

	m.markWalkerPendingSession("")
	if len(m.walkerPendingSessions) != 0 {
		t.Fatalf("an empty peerID was stored (%d entries) — every unidentified "+
			"probe shares one key and consumes the others' tags",
			len(m.walkerPendingSessions))
	}
	if m.consumeWalkerPendingSession("") {
		t.Fatal("an empty peerID consumed a tag")
	}
}

// 🔑 THE DRAIN MUST BE SELECTIVE, and the surviving half is the half
// worth asserting.
//
// A reaper that removes everything looks identical in production to one
// that works: the map stays bounded either way. What breaks is silent —
// every tag is gone before registerMeshSession can consume it, so no
// probe is ever attributed. The TTL is deliberately 2 walker intervals
// so a tag that misses ONE register call still survives the next tick.
func TestDrainReapsOnlyExpiredWalkerTags(t *testing.T) {
	m := pendingTestManager()

	m.markWalkerPendingSession(testNodeIDA) // fresh — must survive
	m.markWalkerPendingSession(testNodeIDB)

	// Backdate B past the TTL. Marking with a real timestamp and waiting
	// would take 2*upgradeWalkInterval, so the clock is set directly.
	m.walkerPendingMu.Lock()
	m.walkerPendingSessions[testNodeIDB] = time.Now().Add(-walkerPendingTTL - time.Second)
	m.walkerPendingMu.Unlock()

	m.drainStaleWalkerPendingSessions()

	if !m.consumeWalkerPendingSession(testNodeIDA) {
		t.Fatal("the drain reaped a FRESH tag — every walker probe loses its " +
			"attribution before registerMeshSession can consume it, and the " +
			"proving counters silently stay at zero")
	}
	// Re-mark A, because the assertion above consumed it, then confirm B
	// is genuinely gone rather than merely unconsumed.
	if m.consumeWalkerPendingSession(testNodeIDB) {
		t.Fatal("an expired tag survived the drain — walkerPendingSessions " +
			"grows without bound for every probe whose register never lands")
	}
}

// A tag exactly at the boundary is not yet expired: the reaper uses a
// strict Before(cutoff), so "aged exactly TTL" survives. Pinned because
// a flip to <= would start reaping tags one tick early, which presents
// as intermittent lost attribution rather than as a bug.
func TestDrainKeepsATagThatHasNotYetReachedTheTTL(t *testing.T) {
	m := pendingTestManager()
	m.markWalkerPendingSession(testNodeIDA)

	m.walkerPendingMu.Lock()
	// Just INSIDE the TTL.
	m.walkerPendingSessions[testNodeIDA] = time.Now().Add(-walkerPendingTTL + time.Second)
	m.walkerPendingMu.Unlock()

	m.drainStaleWalkerPendingSessions()

	if !m.consumeWalkerPendingSession(testNodeIDA) {
		t.Fatal("a tag still inside the TTL was reaped — attribution is lost " +
			"a tick early for any probe with a slow SetupMeshSession")
	}
}

func TestDrainOnANilMapIsANoOp(t *testing.T) {
	m := pendingTestManager()
	if m.walkerPendingSessions != nil {
		t.Fatal("premise wrong: this case needs the nil map")
	}
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("the drain panicked on a nil map: %v — it runs once per "+
				"walker tick, so this crashes the node on the first tick before "+
				"any probe has ever been marked", r)
		}
	}()
	m.drainStaleWalkerPendingSessions()
}

// 🔴 SignalWalkerWake MUST NEVER BLOCK, and that is a backpressure
// property, not a performance one.
//
// Its expected caller is AddressTable.onRecord — the gossip record
// path. address_table.go's own comment says a blocking callback there
// would "stall all subsequent fleet.peer records". So if the `default`
// arm were ever lost, a full wake channel would wedge gossip intake for
// the whole node, and the symptom would be peers going stale — the very
// condition the walker exists to fix.
//
// The call runs in a goroutine with a deadline so a blocking mutant
// FAILS rather than hanging the suite.
func TestSignalWalkerWakeNeverBlocksWhenAWakeIsAlreadyQueued(t *testing.T) {
	m := pendingTestManager()

	m.SignalWalkerWake() // fills the cap-1 channel
	if got, _ := m.WalkerWakeStats(); got != 1 {
		t.Fatalf("delivered = %d after the first signal, want 1", got)
	}
	if len(m.walkerWakeCh) != 1 {
		t.Fatal("premise wrong: the channel is not full, so the second signal " +
			"would take the delivered path and the coalesce arm is untested")
	}

	done := make(chan struct{})
	go func() { m.SignalWalkerWake(); close(done) }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("SignalWalkerWake BLOCKED on a full wake channel — its caller " +
			"is AddressTable.onRecord, so this wedges the gossip record path " +
			"and every subsequent fleet.peer record stalls behind it")
	}

	delivered, coalesced := m.WalkerWakeStats()
	if delivered != 1 || coalesced != 1 {
		t.Fatalf("wake stats = (delivered %d, coalesced %d), want (1, 1) — the "+
			"dropped signal is not observable, so a wedged walker looks idle",
			delivered, coalesced)
	}

	// The queued wake is real: the walker's select can actually receive it.
	select {
	case <-m.walkerWakeCh:
	default:
		t.Fatal("the delivered wake is not readable from the channel — the " +
			"walker would never run its early pass")
	}
}

// Safe before the walker exists and on a nil manager: the wake hook is
// wired from AddressTable at InitSwarm, which can run before or without
// a ConnectionManager Start loop.
func TestSignalWalkerWakeIsSafeWithoutAChannel(t *testing.T) {
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("SignalWalkerWake panicked with no channel: %v — this is "+
				"called from the gossip path, so it takes the node down", r)
		}
	}()
	(&ConnectionManager{}).SignalWalkerWake() // nil channel
	var nilMgr *ConnectionManager
	nilMgr.SignalWalkerWake() // nil receiver
}

// WalkerStats is a pure snapshot accessor, but it returns ELEVEN
// same-typed values in a documented order, and runtime.MeshMetrics maps
// them positionally onto snake_case keys. A transposition there is
// invisible to the compiler and mislabels operator telemetry silently —
// so each counter is set to a distinct value and read back by position.
func TestWalkerStatsReturnsCountersInTheDocumentedOrder(t *testing.T) {
	m := &ConnectionManager{}
	m.walkerTicks.Store(1)
	m.walkerCandidates.Store(2)
	m.walkerProbesStarted.Store(3)
	m.walkerProbesSucceeded.Store(4)
	m.walkerProbesProving.Store(5)
	m.walkerProbesProvingSucceeded.Store(6)
	m.walkerProbesProvingFailed.Store(7)
	m.walkerProbesStallCooled.Store(8)
	m.walkerProbesEscalated.Store(9)
	m.walkerProbesSkippedRace.Store(10)
	m.walkerProbesSkippedSlot.Store(11)

	ticks, cands, started, ok, proving, provingOK, provingFail,
		stalled, escalated, race, slot := m.WalkerStats()

	got := []uint64{ticks, cands, started, ok, proving, provingOK,
		provingFail, stalled, escalated, race, slot}
	for i, v := range got {
		if v != uint64(i+1) {
			t.Fatalf("WalkerStats position %d = %d, want %d — the return tuple "+
				"no longer matches its documented order, and runtime.MeshMetrics "+
				"maps it positionally, so operator telemetry is mislabelled: %v",
				i, v, i+1, got)
		}
	}
}
