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
)

// Covers registerMeshSession's upgrade-promote branch, the consuming half of
// the walker-attribution handshake.
//
// The branch consumes the tag set by markWalkerPendingSession and read by
// consumeWalkerPendingSession, and it is that tag's only production consumer:
// exercising the mark/consume pair alone leaves this branch unrun. What the
// branch produces is the proving-outcome billing — walkerProbesProvingSucceeded
// against walkerProbesProvingFailed — so those counters move only from here.
//
// The promote branch installs a 60-second time.AfterFunc proving timer, and
// every test here stops it explicitly: a timer still armed when the fixture is
// torn down fires against freed state and reports its failure inside whichever
// unrelated test happens to be running at the time.

// registerTestManager() builds the minimum ConnectionManager state
// registerMeshSession touches. Maps are left nil deliberately: the function
// lazily creates each one, and that lazy-init() is part of what is under test.
func registerTestManager() *ConnectionManager {
	return &ConnectionManager{
		walkerWakeCh: make(chan struct{}, 1),
	}
}

// stopProving cancels any proving timer the register path installed, so a
// 60s AfterFunc cannot fire after the test completes.
func stopProving(t *testing.T, m *ConnectionManager) {
	t.Helper()
	m.dispatchMu.Lock()
	defer m.dispatchMu.Unlock()
	for id, ps := range m.proving {
		if ps.timer != nil {
			ps.timer.Stop()
		}
		delete(m.proving, id)
	}
}

// wsSession() is Grade C (WebSocket: reliable, no native mux, no native
// encryption); noiseSession() is Grade A (Noise-UDP: encrypted, unreliable).
// A > C, so registering noise over ws takes the upgrade-promote path.
func wsSession() *probeSession {
	return &probeSession{proto: aether.ProtoWebSocket}
}
func noiseSession() *probeSession {
	return &probeSession{proto: aether.ProtoNoise}
}

func TestUpgradePromoteConsumesTheWalkerTagExactlyOnce(t *testing.T) {
	ctx := context.Background()
	m := registerTestManager()
	defer stopProving(t, m)

	// PREMISE: the two sessions really do differ in grade in the upgrade
	// direction, or the promote branch is never entered and everything below
	// is vacuous.
	oldS, newS := wsSession(), noiseSession()
	if got := SessionGrade(oldS); got != GradeC {
		t.Fatalf("premise wrong: old session grade = %v, want GradeC", got)
	}
	if got := SessionGrade(newS); got != GradeA {
		t.Fatalf("premise wrong: new session grade = %v, want GradeA", got)
	}

	// Install the low-grade session first.
	if !m.registerMeshSession(ctx, testNodeIDA, oldS, true) {
		t.Fatal("the initial low-grade registration was refused")
	}

	// The walker tags the peer, exactly as probeUpgrade's handoff does.
	m.markWalkerPendingSession(testNodeIDA)

	beforeProving := m.walkerProbesProving.Load()
	if !m.registerMeshSession(ctx, testNodeIDA, newS, true) {
		t.Fatal("the grade upgrade was refused — the promote branch did not run")
	}

	// ① The tag was consumed: a second consume must find nothing.
	if m.consumeWalkerPendingSession(testNodeIDA) {
		t.Fatal("the walker tag SURVIVED the upgrade-promote branch — the next " +
			"unrelated session for this peer would inherit walker attribution " +
			"and inflate the proving counters")
	}

	// ② The proving session was billed to the walker.
	if got := m.walkerProbesProving.Load(); got != beforeProving+1 {
		t.Fatalf("walkerProbesProving = %d, want %d — the upgrade was not billed "+
			"to the walker, so a probe that succeeded reports as if it never ran",
			got, beforeProving+1)
	}

	// ③ The proving session records the attribution for the timer/revert paths.
	m.dispatchMu.Lock()
	ps, ok := m.proving[testNodeIDA]
	m.dispatchMu.Unlock()
	if !ok {
		t.Fatal("no proving session was installed by the upgrade")
	}
	if !ps.fromWalker {
		t.Fatal("the proving session is not marked fromWalker — the 60s timer " +
			"and the revert path will bill this outcome to neither " +
			"walkerProbesProvingSucceeded nor ...Failed")
	}
}

// An upgrade that the walker did NOT initiate must not be billed to it.
// Without this, every counter assertion above is satisfiable by code that
// bills unconditionally.
func TestUpgradePromoteWithoutAWalkerTagIsNotBilled(t *testing.T) {
	ctx := context.Background()
	m := registerTestManager()
	defer stopProving(t, m)

	if !m.registerMeshSession(ctx, testNodeIDA, wsSession(), true) {
		t.Fatal("the initial registration was refused")
	}
	// NO markWalkerPendingSession here — this upgrade came from some other
	// path (inbound dial, scanAndConnect).
	before := m.walkerProbesProving.Load()

	if !m.registerMeshSession(ctx, testNodeIDA, noiseSession(), true) {
		t.Fatal("the grade upgrade was refused")
	}

	if got := m.walkerProbesProving.Load(); got != before {
		t.Fatalf("walkerProbesProving moved %d -> %d for an upgrade the walker "+
			"never initiated — the counter measures upgrades, not walker probes, "+
			"and every conclusion drawn from it is wrong", before, got)
	}
	m.dispatchMu.Lock()
	ps, ok := m.proving[testNodeIDA]
	m.dispatchMu.Unlock()
	if !ok {
		t.Fatal("no proving session installed")
	}
	if ps.fromWalker {
		t.Fatal("an unattributed upgrade is marked fromWalker")
	}
}

// The tag is keyed by peer nodeID, so a tag for one peer must not be consumed
// by another peer's upgrade.
func TestWalkerTagIsNotConsumedByADifferentPeersUpgrade(t *testing.T) {
	ctx := context.Background()
	m := registerTestManager()
	defer stopProving(t, m)

	m.markWalkerPendingSession(testNodeIDA)

	if !m.registerMeshSession(ctx, testNodeIDB, wsSession(), true) {
		t.Fatal("peer B initial registration refused")
	}
	before := m.walkerProbesProving.Load()
	if !m.registerMeshSession(ctx, testNodeIDB, noiseSession(), true) {
		t.Fatal("peer B upgrade refused")
	}

	if got := m.walkerProbesProving.Load(); got != before {
		t.Fatalf("peer B's upgrade consumed peer A's walker tag (proving %d -> %d)",
			before, got)
	}
	if !m.consumeWalkerPendingSession(testNodeIDA) {
		t.Fatal("peer A's tag was destroyed by peer B's upgrade — attribution " +
			"is not keyed by peer")
	}
}

// Covers the proving-REVERT branch in unregisterMeshSession, which bills
// walkerProbesProvingFailed and reinstalls the old session.
//
// Together with the promote branch above, these two answer the question
// walkerProbesSucceeded structurally cannot: "did walker probes produce
// DURABLE upgrades?" — succeeded fires on handshake completion, so a probe
// that dies inside the 60s proving window still counts there.
//
// 🛑 A FAILURE HERE IS NOT AN ERROR, IT IS A DISCONNECTED PEER. If the revert
// does not fire, the dying upgraded session takes the peer's dispatch entry
// with it and the healthy old session is discarded — the node loses a peer it
// still had a working transport to.

// provingFixture installs a low-grade session, tags it as walker-initiated,
// then upgrades it — leaving a live proving window. Returns old and new.
func provingFixture(t *testing.T, m *ConnectionManager, nodeID string) (*probeSession, *probeSession) {
	t.Helper()
	ctx := context.Background()
	oldS, newS := wsSession(), noiseSession()
	if !m.registerMeshSession(ctx, nodeID, oldS, true) {
		t.Fatal("fixture: initial registration refused")
	}
	m.markWalkerPendingSession(nodeID)
	if !m.registerMeshSession(ctx, nodeID, newS, true) {
		t.Fatal("fixture: upgrade refused")
	}
	m.dispatchMu.Lock()
	ps, ok := m.proving[nodeID]
	m.dispatchMu.Unlock()
	if !ok || !ps.fromWalker {
		t.Fatal("fixture premise wrong: no walker-attributed proving session")
	}
	return oldS, newS
}

func TestProvingRevertReinstatesOldSessionAndBillsTheWalker(t *testing.T) {
	m := registerTestManager()
	defer stopProving(t, m)
	oldS, newS := provingFixture(t, m, testNodeIDA)

	before := m.walkerProbesProvingFailed.Load()

	// The upgraded session dies inside the proving window.
	m.unregisterMeshSession(testNodeIDA, newS)

	m.dispatchMu.Lock()
	current := m.meshSessions[testNodeIDA]
	_, stillProving := m.proving[testNodeIDA]
	m.dispatchMu.Unlock()

	if current != aether.Session(oldS) {
		t.Fatalf("dispatch session after revert = %v, want the OLD session — the "+
			"peer lost a working transport because the failed upgrade took the "+
			"dispatch entry with it", current)
	}
	if stillProving {
		t.Fatal("the proving entry survived the revert — the 60s timer will fire " +
			"later and act on a window that already resolved")
	}
	if got := m.walkerProbesProvingFailed.Load(); got != before+1 {
		t.Fatalf("walkerProbesProvingFailed = %d, want %d — a walker probe that "+
			"failed its proving window is not counted, so the walker's honest "+
			"durable-upgrade rate reads better than it is", got, before+1)
	}
}

// The revert must NOT fire when a third session has taken over: `expected`
// belongs to neither party of the proving window, so this cleanup is obsolete
// and reverting would clobber a session this caller does not own.
func TestProvingRevertIsSkippedWhenAThirdSessionOwnsDispatch(t *testing.T) {
	m := registerTestManager()
	defer stopProving(t, m)
	_, newS := provingFixture(t, m, testNodeIDA)

	third := &probeSession{proto: aether.ProtoQUIC}
	beforeSkip := m.unregisterSkippedNotOwner.Load()
	beforeFailed := m.walkerProbesProvingFailed.Load()

	// A stale cleanup arrives owning a session that is neither old nor new.
	m.unregisterMeshSession(testNodeIDA, third)

	if got := m.unregisterSkippedNotOwner.Load(); got != beforeSkip+1 {
		t.Fatalf("unregisterSkippedNotOwner = %d, want %d — the ownership guard "+
			"did not fire", got, beforeSkip+1)
	}
	m.dispatchMu.Lock()
	current := m.meshSessions[testNodeIDA]
	_, stillProving := m.proving[testNodeIDA]
	m.dispatchMu.Unlock()

	if current != aether.Session(newS) {
		t.Fatalf("a stale cleanup mutated dispatch (now %v) — it owns neither "+
			"party of the proving window and must not touch it", current)
	}
	if !stillProving {
		t.Fatal("a stale cleanup tore down a proving window it does not own")
	}
	if got := m.walkerProbesProvingFailed.Load(); got != beforeFailed {
		t.Fatalf("a skipped cleanup billed a proving failure (%d -> %d)",
			beforeFailed, got)
	}
}

// If the OLD session is already closed there is nothing to revert to; the
// window is cleaned up without reinstating a dead transport.
func TestProvingRevertDoesNotReinstateAClosedOldSession(t *testing.T) {
	m := registerTestManager()
	defer stopProving(t, m)
	oldS, newS := provingFixture(t, m, testNodeIDA)

	oldS.closed = true // the fallback died too
	m.unregisterMeshSession(testNodeIDA, newS)

	m.dispatchMu.Lock()
	current, present := m.meshSessions[testNodeIDA]
	_, stillProving := m.proving[testNodeIDA]
	m.dispatchMu.Unlock()

	if stillProving {
		t.Fatal("the proving window survived even though it was resolved")
	}
	if present && current == aether.Session(oldS) {
		t.Fatal("a CLOSED old session was reinstated as the dispatch entry — the " +
			"peer now dispatches through a dead transport, which is worse than " +
			"having no entry at all")
	}
}

// 🔑 THE TIE-BREAK, WHICH IS THE ONLY THING STANDING BETWEEN THE FLEET AND A
// DOCUMENTED FLAPPING INCIDENT.
//
// registerMeshSession's own comment describes it: A and B dial each other at
// nearly the same instant, each registers its own outbound as primary, then
// each receives the peer's outbound. "Without a deterministic rule each side
// independently rejected the inbound and kept its own outbound — but the peer
// was doing the same with the OTHER session, so each side ended up with a
// session the peer had discarded." Operator-visible as peers flapping every
// ~5s with longest_session_seconds pinned to the keepalive interval.
//
// The rule is "the session whose initiator has the LOWER nodeID wins", and its
// correctness is SYMMETRY: both peers must independently converge on the same
// winner. A test on one side alone cannot see that — so this drives BOTH.
func TestSimultaneousDialTieBreakConvergesOnBothPeers(t *testing.T) {
	ctx := context.Background()
	// testNodeIDA ("aa11…") < testNodeIDB ("bb22…"), so A is the lower nodeID.
	if !(testNodeIDA < testNodeIDB) {
		t.Fatal("premise wrong: this test needs testNodeIDA < testNodeIDB")
	}

	// --- Peer A's view: it dialled B, then receives B's outbound. ---
	a := registerTestManager()
	a.selfID = testNodeIDA
	defer stopProving(t, a)
	aOutbound := &probeSession{proto: aether.ProtoWebSocket} // A initiated
	bInbound := &probeSession{proto: aether.ProtoWebSocket}  // B's outbound, inbound to A
	if !a.registerMeshSession(ctx, testNodeIDB, aOutbound, true) {
		t.Fatal("A: own outbound refused")
	}
	aKeptInbound := a.registerMeshSession(ctx, testNodeIDB, bInbound, false)

	// --- Peer B's view: it dialled A, then receives A's outbound. ---
	b := registerTestManager()
	b.selfID = testNodeIDB
	defer stopProving(t, b)
	bOutbound := &probeSession{proto: aether.ProtoWebSocket} // B initiated
	aInbound := &probeSession{proto: aether.ProtoWebSocket}  // A's outbound, inbound to B
	if !b.registerMeshSession(ctx, testNodeIDA, bOutbound, true) {
		t.Fatal("B: own outbound refused")
	}
	bKeptInbound := b.registerMeshSession(ctx, testNodeIDA, aInbound, false)

	// Both sides must end up dispatching through the SAME logical session —
	// the one A initiated, because A holds the lower nodeID.
	a.dispatchMu.Lock()
	aCurrent := a.meshSessions[testNodeIDB]
	a.dispatchMu.Unlock()
	b.dispatchMu.Lock()
	bCurrent := b.meshSessions[testNodeIDA]
	b.dispatchMu.Unlock()

	if aCurrent != aether.Session(aOutbound) {
		t.Errorf("A kept the wrong session: it must keep its OWN outbound, "+
			"because A's nodeID is lower (keptInbound=%v)", aKeptInbound)
	}
	if bCurrent != aether.Session(aInbound) {
		t.Errorf("B kept the wrong session: it must ADOPT A's outbound and drop "+
			"its own, because A's nodeID is lower (keptInbound=%v)", bKeptInbound)
	}

	// 🛑 THE CONVERGENCE ASSERTION. Both peers chose the session A initiated.
	// If this fails, each side holds a session the other discarded — which is
	// exactly the flap the tie-break exists to prevent, and it does NOT show
	// up as an error anywhere.
	if !(aCurrent == aether.Session(aOutbound) && bCurrent == aether.Session(aInbound)) {
		t.Fatal("the two peers did NOT converge on the same logical session — " +
			"each holds one the other has discarded, keepalive kills the " +
			"abandoned half in 5-9s, both redial, and the fleet flaps")
	}
}

// Pins a dead branch as dead.
//
// registerMeshSession's lower-grade arm reads:
//
//	if newGrade < oldGrade {
//	    if newGrade.CanCoexistWith(oldGrade) { …dormant fallback; return true }
//	    // "Same grade types can't coexist — reject"
//	    …recordDedupRejectLocked; session.Close(); return false
//	}
//
// CanCoexistWith is `g != other` (grade/grade.go:97) and it has EXACTLY ONE
// non-test caller — this one. Inside `newGrade < oldGrade` the grades differ
// by definition, so the predicate is a TAUTOLOGY and the reject arm is
// UNREACHABLE.
//
// ⇒ ***A lower-grade session is ALWAYS accepted as a dormant fallback; it is
// never rejected here, and the comment describes a policy that does not
// exist.*** This test states the behaviour that actually ships, so a future
// change to either the predicate or the guard fails loudly instead of quietly
// switching a never-taken branch back on.
func TestLowerGradeSessionIsAlwaysAcceptedAsDormantFallback(t *testing.T) {
	ctx := context.Background()
	m := registerTestManager()
	defer stopProving(t, m)

	primary := noiseSession() // Grade A
	if !m.registerMeshSession(ctx, testNodeIDA, primary, true) {
		t.Fatal("primary registration refused")
	}
	beforeRejects := m.dedupRejectCount()

	// Every grade strictly below A must be accepted, none rejected.
	for _, lower := range []*probeSession{
		{proto: aether.ProtoQUIC},      // Grade B
		{proto: aether.ProtoWebSocket}, // Grade C
	} {
		if got := SessionGrade(lower); got >= SessionGrade(primary) {
			t.Fatalf("premise wrong: %v is not below the primary's grade", got)
		}
		if !m.registerMeshSession(ctx, testNodeIDA, lower, false) {
			t.Fatalf("a lower-grade session (%v) was REJECTED — the "+
				"reject-cant-coexist arm has become reachable, which means "+
				"either CanCoexistWith or the enclosing `newGrade < oldGrade` "+
				"guard changed; confirm that is intended", SessionGrade(lower))
		}
		m.dispatchMu.Lock()
		current := m.meshSessions[testNodeIDA]
		m.dispatchMu.Unlock()
		if current != aether.Session(primary) {
			t.Fatal("a dormant fallback replaced the higher-grade primary")
		}
	}

	if got := m.dedupRejectCount(); got != beforeRejects {
		t.Fatalf("dedup rejects moved %d -> %d on the lower-grade path, which "+
			"is unreachable", beforeRejects, got)
	}
}

// dedupRejectCount reads the reject map size under the manager's own lock.
func (m *ConnectionManager) dedupRejectCount() int {
	m.dispatchMu.Lock()
	defer m.dispatchMu.Unlock()
	return len(m.dedupRejectAt)
}

// Covers the proving-SUCCESS path, which a test reaches only by driving the
// 60-second time.AfterFunc directly.
//
// Together with the revert branch, this completes the only honest answer to
// "did walker probes produce DURABLE upgrades?": walkerProbesSucceeded fires
// at handshake completion and cannot distinguish an upgrade that stuck from
// one that died 3 seconds later.
func TestProvingSuccessClosesTheOldSessionAndBillsTheWalker(t *testing.T) {
	m := registerTestManager()
	defer stopProving(t, m)
	oldS, newS := provingFixture(t, m, testNodeIDA)

	m.dispatchMu.Lock()
	ps := m.proving[testNodeIDA]
	m.dispatchMu.Unlock()
	before := m.walkerProbesProvingSucceeded.Load()

	// Fire what the 60s timer would have fired.
	m.completeProving(testNodeIDA, ps, oldS, newS)

	if got := m.walkerProbesProvingSucceeded.Load(); got != before+1 {
		t.Fatalf("walkerProbesProvingSucceeded = %d, want %d — a walker probe "+
			"whose upgrade SURVIVED its proving window is not counted, so the "+
			"walker looks less effective than it is and the honest "+
			"durable-upgrade rate is unmeasurable in BOTH directions",
			got, before+1)
	}
	m.dispatchMu.Lock()
	_, stillProving := m.proving[testNodeIDA]
	current := m.meshSessions[testNodeIDA]
	m.dispatchMu.Unlock()
	if stillProving {
		t.Fatal("the proving entry survived completion — a later revert could " +
			"still act on a window that already resolved")
	}
	if current != aether.Session(newS) {
		t.Fatal("the upgraded session is no longer the dispatch entry after a " +
			"SUCCESSFUL proving window")
	}
}

// 🛑 THE IDENTITY GUARD: if a THIRD upgrade replaced this one, the outcome
// belongs to that later proving cycle — not this timer.
//
// Without the `newStillCurrent` check the stale timer would bill a success it
// did not earn AND close a session that is now someone else's live fallback.
func TestProvingSuccessDoesNotBillWhenALaterUpgradeTookOver(t *testing.T) {
	m := registerTestManager()
	defer stopProving(t, m)
	oldS, newS := provingFixture(t, m, testNodeIDA)

	m.dispatchMu.Lock()
	ps := m.proving[testNodeIDA]
	m.dispatchMu.Unlock()

	// A third, higher-grade session takes over before the timer fires.
	// (noise is already Grade A, so simulate the takeover directly — the
	// dispatch entry is no longer newS.)
	third := &probeSession{proto: aether.ProtoNoise}
	m.dispatchMu.Lock()
	m.meshSessions[testNodeIDA] = third
	m.dispatchMu.Unlock()

	before := m.walkerProbesProvingSucceeded.Load()
	m.completeProving(testNodeIDA, ps, oldS, newS)

	if got := m.walkerProbesProvingSucceeded.Load(); got != before {
		t.Fatalf("a stale proving timer billed a SUCCESS (%d -> %d) after a "+
			"later upgrade had already replaced its session — the outcome "+
			"belongs to the later proving cycle", before, got)
	}
	m.dispatchMu.Lock()
	current := m.meshSessions[testNodeIDA]
	m.dispatchMu.Unlock()
	if current != aether.Session(third) {
		t.Fatal("a stale proving timer disturbed the dispatch entry of a session " +
			"it does not own")
	}
}

// A timer whose proving entry has already been replaced must do nothing at
// all — the `p == ps` identity check, not merely the presence check.
func TestProvingSuccessIsANoOpForASupersededProvingEntry(t *testing.T) {
	m := registerTestManager()
	defer stopProving(t, m)
	oldS, newS := provingFixture(t, m, testNodeIDA)

	m.dispatchMu.Lock()
	stale := m.proving[testNodeIDA]
	// A different proving cycle now owns the slot.
	m.proving[testNodeIDA] = &provingSession{oldSession: oldS, newSession: newS}
	m.dispatchMu.Unlock()

	before := m.walkerProbesProvingSucceeded.Load()
	m.completeProving(testNodeIDA, stale, oldS, newS)

	m.dispatchMu.Lock()
	_, stillThere := m.proving[testNodeIDA]
	m.dispatchMu.Unlock()
	if !stillThere {
		t.Fatal("a superseded timer deleted the CURRENT proving entry — the live " +
			"cycle loses its window and its outcome is never billed")
	}
	if got := m.walkerProbesProvingSucceeded.Load(); got != before {
		t.Fatalf("a superseded timer billed a success (%d -> %d)", before, got)
	}
}

// Pins registerMeshSession's dedup rule: a duplicate dial is rejected without
// the live session being touched.
//
// The rule exists because closing the existing session on a failed liveness
// probe cascades to the peer — its readLoop hits EOF and both sides redial —
// and on a young, in-use session that probe false-negatives often.
//
// The assertion that carries the weight is the NEGATIVE one: the existing
// session must not be closed. Asserting only "the duplicate was rejected"
// passes either way, because closing the wrong session also ends with exactly
// one session installed.
func TestDuplicateDialIsRejectedWithoutTouchingTheLiveSession(t *testing.T) {
	ctx := context.Background()
	m := registerTestManager()
	m.selfID = testNodeIDA
	defer stopProving(t, m)

	// Both sessions initiated by US, same grade — a duplicate dial from
	// EnsureK or scanAndConnect reaching a peer we already have.
	live := wsSession()
	dup := wsSession()
	if !m.registerMeshSession(ctx, testNodeIDB, live, true) {
		t.Fatal("the first session was refused")
	}
	if live.closeCalls.Load() != 0 {
		t.Fatal("premise wrong: the live session was closed during its own registration")
	}

	accepted := m.registerMeshSession(ctx, testNodeIDB, dup, true)

	if accepted {
		t.Fatal("a duplicate same-initiator dial was ACCEPTED — dedup's only job " +
			"is not to add a duplicate")
	}
	m.dispatchMu.Lock()
	current := m.meshSessions[testNodeIDB]
	m.dispatchMu.Unlock()
	if current != aether.Session(live) {
		t.Fatal("the duplicate replaced the live session as dispatch primary")
	}

	// The unused duplicate SHOULD be closed — it never carried traffic, so its
	// teardown cannot disturb the peer. This ALSO serves as the
	// synchronisation point for the negative assertion below.
	if !waitFor(func() bool { return dup.closeCalls.Load() > 0 }, 2*time.Second) {
		t.Fatal("the rejected duplicate was never closed — it leaks a session " +
			"the peer believes is live")
	}

	// The ordering of this assertion is load-bearing. Both teardowns run as
	// `go X.Close()`, so checking it before the wait above races the scheduler
	// and passes whether or not the live session was closed. Asserting after
	// the duplicate's close has landed means a `go old.Close()` had the same
	// opportunity to run.
	if n := live.closeCalls.Load(); n != 0 {
		t.Fatalf("the EXISTING, still-open session was closed %d time(s) — the "+
			"peer's readLoop then hits EOF and both sides redial, which is the "+
			"session-replacement churn this dedup rule exists to stop", n)
	}
}

// Covers registerMeshSession's fall-through install, NOT its
// `same-initiator-old-closed` arm. The outer dedup guard is:
//
//	if old, ok := m.meshSessions[nodeID]; ok && old != session && !old.IsClosed()
//
// so a CLOSED old session skips the entire dedup block and reaches the plain
// install at the bottom. That fall-through is the path under test: without it
// a peer keeps a dead dispatch entry and every RPC fails until some other path
// notices.
//
// The name is precise for that reason. Because the outer guard already
// required `!old.IsClosed()`, the inner `if !old.IsClosed()` is almost always
// true and its `same-initiator-old-closed` else-branch is reachable only if
// the session closes BETWEEN the two checks — race-reachable rather than dead,
// so no test here can claim to cover it.
func TestClosedOldSessionSkipsDedupAndInstallsTheNewOne(t *testing.T) {
	ctx := context.Background()
	m := registerTestManager()
	m.selfID = testNodeIDA
	defer stopProving(t, m)

	dead := wsSession()
	fresh := wsSession()
	if !m.registerMeshSession(ctx, testNodeIDB, dead, true) {
		t.Fatal("the first session was refused")
	}
	dead.closed = true // readLoop exited / keepalive killed it

	if !m.registerMeshSession(ctx, testNodeIDB, fresh, true) {
		t.Fatal("a replacement for a CLOSED session was refused — the peer is " +
			"left dispatching through a dead transport and every RPC fails " +
			"until another path happens to notice")
	}
	m.dispatchMu.Lock()
	current := m.meshSessions[testNodeIDB]
	initiator := m.meshSessionInitiators[testNodeIDB]
	m.dispatchMu.Unlock()

	if current != aether.Session(fresh) {
		t.Fatal("the fresh session did not become the dispatch primary")
	}
	if !initiator {
		t.Fatal("the initiator hint was not refreshed for the installed session")
	}
	// The dead session must not be re-closed; it is already gone.
	if n := dead.closeCalls.Load(); n != 0 {
		t.Fatalf("the already-closed session was closed again (%d) — harmless "+
			"here but it means the path is not distinguishing dead from live", n)
	}
}

// A second upgrade must cancel the first proving cycle.
//
// Two upgrades in quick succession (C → B → A) leave two proving windows if
// the first is not cancelled. The consequences are cumulative and silent:
//   - the first window's 60s timer still fires, and the identity checks inside
//     its callback are then the only thing stopping it acting on a session it
//     no longer owns; and
//   - the first cycle's old session is never closed, so a superseded
//     transport lingers for the process lifetime.
func TestSecondUpgradeCancelsThePreviousProvingCycle(t *testing.T) {
	ctx := context.Background()
	m := registerTestManager()
	m.selfID = testNodeIDA
	defer stopProving(t, m)

	gradeC := wsSession()                             // Grade C
	gradeB := &probeSession{proto: aether.ProtoQUIC}  // Grade B
	gradeA := &probeSession{proto: aether.ProtoNoise} // Grade A

	// PREMISE: three strictly increasing grades, or the second registration
	// is not an upgrade and no second proving cycle is created.
	if !(SessionGrade(gradeC) < SessionGrade(gradeB) && SessionGrade(gradeB) < SessionGrade(gradeA)) {
		t.Fatalf("premise wrong: grades are %v/%v/%v, need strictly increasing",
			SessionGrade(gradeC), SessionGrade(gradeB), SessionGrade(gradeA))
	}

	if !m.registerMeshSession(ctx, testNodeIDB, gradeC, true) {
		t.Fatal("initial registration refused")
	}
	if !m.registerMeshSession(ctx, testNodeIDB, gradeB, true) {
		t.Fatal("first upgrade refused")
	}
	m.dispatchMu.Lock()
	first := m.proving[testNodeIDB]
	m.dispatchMu.Unlock()
	if first == nil {
		t.Fatal("premise wrong: the first upgrade installed no proving window")
	}

	// Second upgrade — must cancel the first cycle.
	if !m.registerMeshSession(ctx, testNodeIDB, gradeA, true) {
		t.Fatal("second upgrade refused")
	}

	m.dispatchMu.Lock()
	second := m.proving[testNodeIDB]
	m.dispatchMu.Unlock()
	if second == nil {
		t.Fatal("the second upgrade installed no proving window")
	}
	if second == first {
		t.Fatal("the proving entry was not replaced — the second upgrade is " +
			"sharing the first cycle's window and its outcome will be billed " +
			"to the wrong upgrade")
	}
	if second.oldSession != aether.Session(gradeB) || second.newSession != aether.Session(gradeA) {
		t.Fatalf("the new proving window has the wrong parties: old=%v new=%v",
			second.oldSession.Protocol(), second.newSession.Protocol())
	}

	// The superseded cycle's OLD session must be closed, or a replaced
	// transport lingers for the process lifetime.
	if !waitFor(func() bool { return gradeC.closeCalls.Load() > 0 }, 2*time.Second) {
		t.Fatal("the cancelled proving cycle's old session was never closed — a " +
			"superseded transport is retained until the process exits")
	}
	// And the session that is now the proving FALLBACK must NOT be closed.
	if n := gradeB.closeCalls.Load(); n != 0 {
		t.Fatalf("the new proving window's fallback was closed %d time(s) — if "+
			"the Grade-A upgrade then fails its window, the revert has nothing "+
			"live to fall back to and the peer is disconnected", n)
	}
}

// Pins the bidi registry's session-pointer key.
//
// registerBidiRPC keys on the session pointer so that multipath siblings —
// multiple sessions to the same peer — each own their own bidi instead of the
// latest registration clobbering the previous one.
//
// A per-peer key instead makes one sibling's teardown destroy another's
// channel: the deferred cleanup on an OLD session deletes the entry a NEWER
// session installed, which surfaces as walker probes counting successes while
// every peer's bestEverGrade stays where it started.
func TestBidiRegistryKeepsMultipathSiblingsSeparate(t *testing.T) {
	m := registerTestManager()

	// Two DISTINCT sessions to the SAME peer — the multipath shape.
	sibA, sibB := wsSession(), noiseSession()
	bidiA, bidiB := &BidiRPC{}, &BidiRPC{}

	m.registerBidiRPC(sibA, bidiA) // also exercises the lazy map init()
	m.registerBidiRPC(sibB, bidiB)

	m.dispatchMu.Lock()
	gotA, okA := m.bidisBySession[sibA]
	gotB, okB := m.bidisBySession[sibB]
	n := len(m.bidisBySession)
	m.dispatchMu.Unlock()

	if !okA || !okB || n != 2 {
		t.Fatalf("premise wrong: registry holds %d entries (A=%v B=%v), want 2 — "+
			"the two siblings are not separately registered, so nothing below "+
			"is measured", n, okA, okB)
	}
	if gotA != bidiA || gotB != bidiB {
		t.Fatal("a sibling's registration overwrote the other's channel — the " +
			"registry is behaving as if keyed by peer, not by session")
	}

	// 🛑 THE PROPERTY: tearing down ONE sibling must not touch the other.
	m.unregisterBidiForSession(sibA)

	m.dispatchMu.Lock()
	_, stillA := m.bidisBySession[sibA]
	survivor, stillB := m.bidisBySession[sibB]
	m.dispatchMu.Unlock()

	if stillA {
		t.Fatal("the unregistered sibling's entry survived")
	}
	if !stillB || survivor != bidiB {
		t.Fatal("tearing down ONE multipath sibling destroyed the OTHER's bidi — " +
			"the surviving session's RPC channel is gone while its session is " +
			"still live, and nothing errors: calls simply stop being answered")
	}
}

// Unregistering a session that was never registered must be a no-op. The
// cleanup path calls this unconditionally, including for sessions that never
// reached bidi setup.
func TestUnregisterBidiForUnknownSessionIsSafe(t *testing.T) {
	m := registerTestManager()

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("unregistering an unknown session panicked: %v — the "+
				"cleanup path calls this for every dying session, including "+
				"ones that never completed bidi setup", r)
		}
	}()

	// Against a nil map (never registered anything).
	m.unregisterBidiForSession(wsSession())

	// And against a populated map, for a session that is not in it.
	known := wsSession()
	m.registerBidiRPC(known, &BidiRPC{})
	m.unregisterBidiForSession(noiseSession())

	m.dispatchMu.Lock()
	_, survived := m.bidisBySession[known]
	m.dispatchMu.Unlock()
	if !survived {
		t.Fatal("unregistering an UNKNOWN session removed a DIFFERENT session's " +
			"entry")
	}
}

// Covers the read side of the bidi registry, and the claim GetBidiRPC's doc
// makes: routing through the active session pointer means a failover that
// promotes a standby to primary immediately exposes that standby's bidi, with
// no eviction or re-registration step, because every session always owned its
// own bidi.
//
// That is testable exactly as stated: swap the dispatch entry, then look up
// the bidi WITHOUT touching the registry, and the standby's own channel must
// come back.
func TestGetBidiRPCFollowsTheActiveSessionWithoutReRegistration(t *testing.T) {
	ctx := context.Background()
	m := registerTestManager()
	m.selfID = testNodeIDA

	primary, standby := wsSession(), noiseSession()
	primaryBidi, standbyBidi := &BidiRPC{}, &BidiRPC{}
	m.registerBidiRPC(primary, primaryBidi)
	m.registerBidiRPC(standby, standbyBidi)

	if !m.registerMeshSession(ctx, testNodeIDB, primary, true) {
		t.Fatal("primary registration refused")
	}
	// PREMISE: the lookup resolves to the primary's OWN bidi first.
	got, ok := m.GetBidiRPC(testNodeIDB)
	if !ok || got != primaryBidi {
		t.Fatalf("premise wrong: lookup returned (%v, %v), want the primary's "+
			"bidi — the failover assertion below would prove nothing", got, ok)
	}

	// Failover: the standby becomes the dispatch entry. NOTHING is
	// re-registered in the bidi map — that is the claim under test.
	m.dispatchMu.Lock()
	m.meshSessions[testNodeIDB] = standby
	m.dispatchMu.Unlock()

	got2, ok2 := m.GetBidiRPC(testNodeIDB)
	if !ok2 {
		t.Fatal("after failover the lookup found no bidi — the promoted standby " +
			"has no RPC channel, so dispatch to this peer fails while a healthy " +
			"session is installed")
	}
	if got2 != standbyBidi {
		t.Fatal("after failover the lookup still returns the OLD session's bidi — " +
			"RPCs are written to a channel whose session is no longer the " +
			"dispatch entry, and nothing errors")
	}
}

// The two documented (nil, false) cases, asserted as fail-closed rather than
// as a panic or a stale hit.
func TestGetBidiRPCFailsClosedOnItsTwoDocumentedCases(t *testing.T) {
	ctx := context.Background()

	t.Run("no active session for the peer", func(t *testing.T) {
		m := registerTestManager()
		if bidi, ok := m.GetBidiRPC(testNodeIDB); ok || bidi != nil {
			t.Fatalf("got (%v, %v) for an unknown peer, want (nil, false)", bidi, ok)
		}
	})

	t.Run("active session but no bidi registered yet", func(t *testing.T) {
		// The race the doc names: session installed, AcceptMeshConnection has
		// not yet registered the bidi.
		m := registerTestManager()
		m.selfID = testNodeIDA
		sess := wsSession()
		if !m.registerMeshSession(ctx, testNodeIDB, sess, true) {
			t.Fatal("registration refused")
		}
		if bidi, ok := m.GetBidiRPC(testNodeIDB); ok || bidi != nil {
			t.Fatalf("got (%v, %v) with no bidi registered, want (nil, false) — "+
				"a caller that trusts ok=true would write to a nil channel", bidi, ok)
		}
	})
}

// Covers GetMeshSession's three fail-closed returns.
//
// This is the dispatcher's single question — "which session do I write to?" —
// and every one of its negative answers must be (nil, false). The dangerous
// one is the third: a CLOSED session still sitting in the dispatch map. If it
// were returned, every RPC would be written to a dead transport and nothing
// would error at the call site.
func TestGetMeshSessionFailsClosedOnAllThreeNegativeCases(t *testing.T) {
	ctx := context.Background()

	t.Run("nil dispatch map", func(t *testing.T) {
		m := registerTestManager()
		if m.meshSessions != nil {
			t.Fatal("premise wrong: this case needs the lazily-created map to be nil")
		}
		if sess, ok := m.GetMeshSession(testNodeIDB); ok || sess != nil {
			t.Fatalf("got (%v, %v), want (nil, false)", sess, ok)
		}
	})

	t.Run("peer absent from a populated map", func(t *testing.T) {
		m := registerTestManager()
		m.selfID = testNodeIDA
		if !m.registerMeshSession(ctx, testNodeIDB, wsSession(), true) {
			t.Fatal("premise registration refused")
		}
		if sess, ok := m.GetMeshSession(testNodeIDA); ok || sess != nil {
			t.Fatalf("got (%v, %v) for a peer with no session, want (nil, false)",
				sess, ok)
		}
	})

	t.Run("session present but CLOSED", func(t *testing.T) {
		m := registerTestManager()
		m.selfID = testNodeIDA
		sess := wsSession()
		if !m.registerMeshSession(ctx, testNodeIDB, sess, true) {
			t.Fatal("premise registration refused")
		}
		// PREMISE: it resolves while open, or the closure below proves nothing.
		if got, ok := m.GetMeshSession(testNodeIDB); !ok || got != aether.Session(sess) {
			t.Fatalf("premise wrong: the OPEN session did not resolve (%v, %v)", got, ok)
		}

		sess.closed = true // readLoop exit / keepalive kill

		if got, ok := m.GetMeshSession(testNodeIDB); ok || got != nil {
			t.Fatalf("got (%v, %v) for a CLOSED session, want (nil, false) — the "+
				"dispatcher would write every RPC to a dead transport and "+
				"nothing errors at the call site", got, ok)
		}
	})
}

// GetBidiRPC delegates to GetMeshSession, so a closed session must fail closed
// at BOTH levels. Covering the callee alone leaves the caller's inherited
// behaviour unasserted.
func TestGetBidiRPCInheritsTheClosedSessionGuard(t *testing.T) {
	ctx := context.Background()
	m := registerTestManager()
	m.selfID = testNodeIDA

	sess := wsSession()
	bidi := &BidiRPC{}
	m.registerBidiRPC(sess, bidi)
	if !m.registerMeshSession(ctx, testNodeIDB, sess, true) {
		t.Fatal("premise registration refused")
	}
	if got, ok := m.GetBidiRPC(testNodeIDB); !ok || got != bidi {
		t.Fatalf("premise wrong: the bidi did not resolve while open (%v, %v)", got, ok)
	}

	sess.closed = true

	// The bidi is STILL in the registry — nothing unregistered it. The only
	// thing standing between the dispatcher and a dead channel is the
	// closed-session guard one level down.
	m.dispatchMu.Lock()
	_, stillRegistered := m.bidisBySession[sess]
	m.dispatchMu.Unlock()
	if !stillRegistered {
		t.Fatal("premise wrong: the bidi was unregistered, so this test is not " +
			"measuring the inherited guard")
	}

	if got, ok := m.GetBidiRPC(testNodeIDB); ok || got != nil {
		t.Fatalf("GetBidiRPC returned (%v, %v) for a CLOSED session — it hands "+
			"out a live-looking channel whose session is dead, because the "+
			"registry entry outlives the session", got, ok)
	}
}
