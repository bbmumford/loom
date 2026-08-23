/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/quality"
)

// Covers the DIAL BACK-OFF LADDER: dialKeyFor, dialIsSuppressed, tryDial,
// recordDialFailure, recordStallCooldown and defaultQualityWeights.
//
// Censused per symbol, one level out, and checked for interface satisfaction.
// Call syntax counts, NOT doc-comment mentions — the two are easy to conflate
// here because this cluster is unusually heavily commented:
//
//	dialKeyFor             5 call sites, all in this file (:416 :430 :440 :474 :483)
//	tryDial                3 <- upgrade_walker.go:393
//	recordDialFailure      6 <- upgrade_walker.go:406, mesh_connection.go
//	recordStallCooldown    3 <- upgrade_walker.go:412, mesh_connection.go:1054,
//	                            peer_connections.go:2891
//	defaultQualityWeights  2 <- :59, :167
//	dialIsSuppressed       0 <- see TestDialIsSuppressedHasNoCallersAtHead
//
// 🔑 WHY THIS CLUSTER MATTERS: it decides whether the mesh retries a peer or
// gives up on it. Fail one way and a permanently-broken path burns a handshake
// every 60 s forever; fail the other and a recovered peer is never re-dialed.
//
// 🛑 NOT DUPLICATED HERE: the tracker's own cooldown semantics are already
// tested in aether (quality/score_test.go — RecordCooldown_PreservesLongerExpiry,
// _DoesNotBumpCounter, _ZeroIsNoOp). These tests cover the LOOM-SIDE WRAPPERS:
// the key they derive, the constant they pass, and their nil-tracker behaviour.

func trackedManager() *ConnectionManager {
	m := registerTestManager()
	m.qualityTracker = quality.NewTracker()
	return m
}

// ── The key space ───────────────────────────────────────────────────────────

// dialKeyFor exists so "every caller uses the exact same string format". If it
// were not deterministic, a failure recorded under one key would never suppress
// the dial that reads the other, and the whole ladder would be inert.
func TestDialKeyIsDeterministicAndPerPeer(t *testing.T) {
	if a, b := dialKeyFor(testNodeIDA, ProtoQUIC), dialKeyFor(testNodeIDA, ProtoQUIC); a != b {
		t.Fatalf("same (peer, proto) produced two keys: %q vs %q — a failure "+
			"recorded under one would never suppress a dial reading the other", a, b)
	}
	if a, b := dialKeyFor(testNodeIDA, ProtoQUIC), dialKeyFor(testNodeIDB, ProtoQUIC); a == b {
		t.Fatalf("two different peers share key %q — one peer's dial failures "+
			"would suppress dials to every other peer", a)
	}
	if a, b := dialKeyFor(testNodeIDA, ProtoQUIC), dialKeyFor(testNodeIDA, ProtoNoiseUDP); a == b {
		t.Fatalf("QUIC and noise-udp share key %q — per-transport back-off "+
			"collapses into per-peer back-off, so one bad transport takes the "+
			"peer's other transports down with it", a)
	}
}

// 🔴 TLS AND WEBSOCKET NOW HAVE SEPARATE COOLDOWN LADDERS.
//
// dialKeyFor used to derive its key through mapProtocol, which selects a
// transport ADAPTER and folds ProtoTLS onto aether.ProtoWebSocket because both
// ride the same reliable-stream adapter. Used as an identity that fold merged
// two dial paths: a TLS bootstrap failure put WebSocket into cooldown for the
// same peer, and vice versa, so one broken transport suppressed dials on a
// transport that had not failed.
//
// The key is the node Protocol now, which is injective. This test asserts the
// separation in both directions — the keys differ, AND a failure recorded on
// one does not suppress the other. The second half is the load-bearing one: the
// keys could differ while some shared upstream still transferred suppression.
func TestTLSAndWebSocketHaveIndependentCooldownLadders(t *testing.T) {
	tlsKey := dialKeyFor(testNodeIDA, ProtoTLS)
	wsKey := dialKeyFor(testNodeIDA, ProtoWebSocket)

	if tlsKey == wsKey {
		t.Fatalf("ProtoTLS and ProtoWebSocket still share the dial key %q — a failure on "+
			"either suppresses both", tlsKey)
	}

	m := trackedManager()
	m.recordDialFailure(testNodeIDA, ProtoTLS)

	if !m.dialIsSuppressed(testNodeIDA, ProtoTLS) {
		t.Fatal("fixture wrong: the recorded TLS failure did not suppress TLS, so this " +
			"test cannot tell an isolated ladder from an inert one")
	}
	if m.dialIsSuppressed(testNodeIDA, ProtoWebSocket) {
		t.Error("a TLS dial failure suppressed WebSocket dials to the same peer — the " +
			"ladders are still shared despite distinct keys")
	}
}

// ── Fail-open on a tracker-less node ────────────────────────────────────────

// 🔴 THE MOST IMPORTANT PROPERTY IN THE FILE. Every method here nil-guards
// m.qualityTracker, and tryDial's guard returns TRUE. If it returned false, a
// node whose tracker was never constructed could not dial ANY peer on ANY
// transport — total mesh isolation, from a nil field rather than a network
// fault. The guards must fail OPEN.
func TestATrackerLessManagerNeverBlocksDialing(t *testing.T) {
	m := registerTestManager() // qualityTracker deliberately nil
	if m.qualityTracker != nil {
		t.Fatal("fixture wrong: this test is about the nil-tracker path")
	}

	if !m.tryDial(testNodeIDA, ProtoNoiseUDP) {
		t.Fatal("tryDial refused a dial on a manager with no quality tracker — a " +
			"node with an unconstructed tracker cannot dial any peer on any " +
			"transport, which is total mesh isolation caused by a nil field")
	}
	if m.dialIsSuppressed(testNodeIDA, ProtoNoiseUDP) {
		t.Fatal("dialIsSuppressed reported suppression with no tracker to " +
			"suppress anything — observability would show every peer cooled down")
	}

	// And the recorders must be no-ops rather than panics: they are called from
	// the walker and the stall detector, neither of which checks the tracker.
	m.recordDialFailure(testNodeIDA, ProtoNoiseUDP)
	m.recordStallCooldown(testNodeIDA, ProtoNoiseUDP)
	m.recordDialSuccess(testNodeIDA, ProtoNoiseUDP)
	if !m.tryDial(testNodeIDA, ProtoNoiseUDP) {
		t.Fatal("a recorder call on a tracker-less manager changed tryDial's " +
			"answer — the nil guards are not no-ops")
	}
}

// ── The ladder itself ───────────────────────────────────────────────────────

// A recorded dial failure must suppress, and a success must clear it — the two
// halves of "a recovered path comes back into rotation".
func TestADialFailureSuppressesAndASuccessClearsIt(t *testing.T) {
	m := trackedManager()

	if m.dialIsSuppressed(testNodeIDA, ProtoQUIC) {
		t.Fatal("a fresh tracker reports a peer already suppressed")
	}

	m.recordDialFailure(testNodeIDA, ProtoQUIC)
	if !m.dialIsSuppressed(testNodeIDA, ProtoQUIC) {
		t.Fatal("a dial failure did not suppress the pair — the back-off ladder " +
			"never engages and a dead path is re-dialed at the full scan rate")
	}

	m.recordDialSuccess(testNodeIDA, ProtoQUIC)
	if m.dialIsSuppressed(testNodeIDA, ProtoQUIC) {
		t.Fatal("a successful handshake did not clear the cooldown — a peer that " +
			"has demonstrably recovered stays suppressed for the rest of the ladder")
	}
}

// A failure on one transport must not suppress a DIFFERENT transport to the
// same peer (TLS/WS excepted — see the characterisation test above). Otherwise
// one broken path takes the peer's healthy paths down with it.
func TestSuppressionIsScopedToOneTransport(t *testing.T) {
	m := trackedManager()
	m.recordDialFailure(testNodeIDA, ProtoQUIC)

	if m.dialIsSuppressed(testNodeIDA, ProtoNoiseUDP) {
		t.Fatal("a QUIC dial failure suppressed noise-udp to the same peer — the " +
			"peer's best transport is taken out by an unrelated path's failure")
	}
	if m.dialIsSuppressed(testNodeIDB, ProtoQUIC) {
		t.Fatal("a dial failure against one peer suppressed the same transport " +
			"to a DIFFERENT peer — the key is not peer-scoped")
	}
}

// 🔑 recordStallCooldown MUST NOT SHORTEN A LONGER ACTIVE COOLDOWN. The tracker
// guarantees this (aether quality/score_test.go); what is tested here is that
// the loom wrapper actually routes through that guarantee instead of stamping
// its own expiry. If it shortened, a stall arriving during a 10-minute failure
// cooldown would drop a permanently-broken path back to 60 s retries.
// ⚠ MEASURED, and it is why this test escalates first: the FIRST failure
// cooldown is 30 s, which is SHORTER than stallCooldown (60 s). A test that
// records one failure cannot distinguish "preserved the longer" from "stamped
// its own 60 s" — both leave 60 s. My first draft skipped for exactly that
// reason, and a skipped test is not evidence. So climb the ladder past 60 s
// first, then the two outcomes differ.
func TestAStallCooldownDoesNotShortenAFailureCooldown(t *testing.T) {
	m := trackedManager()
	key := dialKeyFor(testNodeIDA, ProtoNoiseUDP)

	var afterFailure time.Duration
	for i := 0; i < 8 && afterFailure <= stallCooldown; i++ {
		m.recordDialFailure(testNodeIDA, ProtoNoiseUDP)
		_, afterFailure = m.qualityTracker.IsDialSuppressed(key)
	}
	if afterFailure <= stallCooldown {
		t.Fatalf("premise unreachable: 8 dial failures produced only a %v "+
			"cooldown, never exceeding stallCooldown (%v) — the documented "+
			"30s→1m→2m→4m→…→10m escalation ladder is not escalating",
			afterFailure, stallCooldown)
	}

	m.recordStallCooldown(testNodeIDA, ProtoNoiseUDP)
	_, afterStall := m.qualityTracker.IsDialSuppressed(key)

	// IsDialSuppressed returns the REMAINING cooldown, not the configured one,
	// so `afterFailure` and `afterStall` are two samples of a DECREASING
	// quantity taken microseconds apart. Asserting afterStall >= afterFailure
	// compares a moving value against itself and its truth depends on when it
	// ran: 2m0s vs 1m59.999999s fails under -count=40 while a single run
	// passes.
	//
	// The real property does not depend on the sampling instant: an overwrite
	// would leave EXACTLY stallCooldown remaining, so what must hold is that the
	// remaining cooldown is still far above stallCooldown, and that it has not
	// dropped by more than clock advance can explain.
	const clockSlop = time.Second

	if afterStall <= stallCooldown+clockSlop {
		t.Fatalf("a stall cooldown REPLACED an active failure cooldown: %v "+
			"remaining, which is stallCooldown (%v) rather than the %v the "+
			"failure ladder had set — a path already judged permanently broken "+
			"returns to %v retries, burning a handshake every minute forever",
			afterStall, stallCooldown, afterFailure, stallCooldown)
	}
	if afterFailure-afterStall > clockSlop {
		t.Fatalf("the remaining cooldown fell from %v to %v — a drop of %v, more "+
			"than elapsed time can explain, so the stall path is rewriting the "+
			"failure expiry rather than preserving the longer of the two",
			afterFailure, afterStall, afterFailure-afterStall)
	}
}

// recordStallCooldown must not bump the dial-failure counter: the whole point
// of the stall path is that it backs off WITHOUT escalating the ladder, so a
// path that recovers re-probes quickly.
func TestAStallCooldownDoesNotEscalateTheFailureLadder(t *testing.T) {
	m := trackedManager()
	key := dialKeyFor(testNodeIDA, ProtoNoiseUDP)

	for i := 0; i < 3; i++ {
		m.recordStallCooldown(testNodeIDA, ProtoNoiseUDP)
	}

	if n := m.qualityTracker.DialFailureCount(key); n != 0 {
		t.Fatalf("three stall cooldowns advanced the failure counter to %d — "+
			"transient stalls escalate onto the exponential dial-failure ladder, "+
			"which is exactly what stallTransientThreshold (%d) exists to gate",
			n, stallTransientThreshold)
	}
	if !m.dialIsSuppressed(testNodeIDA, ProtoNoiseUDP) {
		t.Fatal("a stall cooldown did not suppress at all — the stall detector " +
			"trips and nothing backs off")
	}
}

// ── Classification before recording ─────────────────────────────────────────

// A local precondition must not touch the peer's ladder. "QUIC transport not
// initialized", "no suitable address", "no TLS host" and "unknown protocol"
// are facts about THIS node. Recording them suppresses a reachable peer for up
// to 10 minutes over our own state, and because ProtoTLS and ProtoWebSocket
// share one tracker key it takes the other transport down with it.
func TestALocalDialPreconditionNeverSuppressesThePeer(t *testing.T) {
	m := trackedManager()

	m.recordDialOutcome(testNodeIDA, ProtoQUIC,
		fmt.Errorf("QUIC transport not initialized: %w", errLocalDialState))

	if m.dialIsSuppressed(testNodeIDA, ProtoQUIC) {
		t.Fatal("a local precondition failure suppressed the peer — this node's " +
			"own missing transport is being recorded as evidence that the PEER " +
			"is unreachable")
	}
	if n := m.qualityTracker.DialFailureCount(dialKeyFor(testNodeIDA, ProtoQUIC)); n != 0 {
		t.Fatalf("failure counter advanced to %d on a local precondition", n)
	}
}

// dialWithProtocol must actually WRAP the sentinel, or the classifier above is
// dead code. This exercises the real function rather than a hand-built error:
// a peer with no addresses takes the "no suitable address" return.
func TestDialWithProtocolWrapsItsLocalPreconditionErrors(t *testing.T) {
	m := trackedManager()
	peer := &peerConn{nodeID: testNodeIDA}

	_, err := m.dialWithProtocol(context.Background(), peer, ProtoQUIC)
	if err == nil {
		t.Fatal("premise wrong: a peer with no addresses dialed successfully")
	}
	if !errors.Is(err, errLocalDialState) {
		t.Fatalf("dialWithProtocol returned %v, which does not wrap "+
			"errLocalDialState — recordDialOutcome cannot tell this node's own "+
			"state from a real reachability failure, so it falls back to blaming "+
			"the peer", err)
	}
}

// 🔑 ONE ERROR IS NOT EVIDENCE THAT A PATH IS DEAD. A single dial error cannot
// distinguish transient loss, a cert rotation, or one bad resolver answer from
// a genuinely broken path — so the first failures take the FLAT stall cooldown
// and must NOT advance the exponential ladder.
func TestASingleReachabilityErrorTakesTheFlatCooldownNotTheLadder(t *testing.T) {
	m := trackedManager()
	key := dialKeyFor(testNodeIDA, ProtoQUIC)

	m.recordDialOutcome(testNodeIDA, ProtoQUIC, errors.New("connection refused"))

	if !m.dialIsSuppressed(testNodeIDA, ProtoQUIC) {
		t.Fatal("a reachability error produced no back-off at all — a broken " +
			"path is re-dialed at the full scan rate")
	}
	if n := m.qualityTracker.DialFailureCount(key); n != 0 {
		t.Fatalf("a SINGLE dial error advanced the exponential ladder to %d — "+
			"one undifferentiated error is being treated as proof the path is "+
			"dead, before classifyDialError has ruled out a local cause", n)
	}
}

// …but a REPEATED failure is evidence, and must escalate. Without this the
// ladder never engages and a permanently broken path retries every 60s forever.
func TestRepeatedReachabilityErrorsEscalateToTheLadder(t *testing.T) {
	m := trackedManager()
	key := dialKeyFor(testNodeIDA, ProtoQUIC)

	for i := 0; i < stallTransientThreshold; i++ {
		m.recordDialOutcome(testNodeIDA, ProtoQUIC, errors.New("connection refused"))
	}

	if n := m.qualityTracker.DialFailureCount(key); n == 0 {
		t.Fatalf("%d consecutive dial errors never escalated onto the "+
			"exponential ladder — a permanently broken path keeps retrying at the "+
			"flat stall cooldown forever", stallTransientThreshold)
	}
}

// 🔴 THE HALF-JOIN THAT WOULD HAVE LATCHED THE GATE. recordDialOutcome counts
// CONSECUTIVE failures, so a success must clear the counter. Without the clear
// in recordDialSuccess, a peer that accumulated enough failures over its
// lifetime would send every later single error straight to the ladder.
func TestASuccessfulDialResetsTheEscalationGate(t *testing.T) {
	m := trackedManager()
	key := dialKeyFor(testNodeIDA, ProtoQUIC)

	for i := 0; i < stallTransientThreshold; i++ {
		m.recordDialOutcome(testNodeIDA, ProtoQUIC, errors.New("connection refused"))
	}
	m.recordDialSuccess(testNodeIDA, ProtoQUIC)

	// A single error after the success must be back to the flat cooldown.
	m.recordDialOutcome(testNodeIDA, ProtoQUIC, errors.New("connection refused"))
	if n := m.qualityTracker.DialFailureCount(key); n != 0 {
		t.Fatalf("after a successful dial, one further error escalated straight "+
			"to the ladder (counter=%d) — the consecutive-failure gate was not "+
			"reset, so it latches for the life of the peer", n)
	}
}

// ── tryDial vs dialIsSuppressed ─────────────────────────────────────────────

// The doc contract: dialIsSuppressed is a PURE PREDICATE and tryDial consumes
// an opportunistic-probe slot. So repeated observation must not change what the
// dialer is allowed to do — otherwise a dashboard scraping suppression state
// would spend the probe slots that real dials need.
func TestObservingSuppressionDoesNotConsumeAProbeSlot(t *testing.T) {
	m := trackedManager()
	m.recordDialFailure(testNodeIDA, ProtoQUIC)

	before := m.tryDial(testNodeIDA, ProtoQUIC)
	for i := 0; i < 50; i++ {
		m.dialIsSuppressed(testNodeIDA, ProtoQUIC)
	}
	after := m.tryDial(testNodeIDA, ProtoQUIC)

	if before != after {
		t.Fatalf("50 pure-predicate reads changed tryDial from %v to %v — "+
			"dialIsSuppressed is consuming probe slots, so any dashboard polling "+
			"it starves the real dialer", before, after)
	}
}

// ── The unwired observability hook ──────────────────────────────────────────

// dialIsSuppressed has no call sites, though its doc says it exists "for
// observability, dashboards, and dual-checks" and peer_connections.go instructs
// readers to query through it — so those comments describe a path no code
// takes.
//
// This test deliberately asserts no call count, since a count in a comment goes
// stale silently. It pins the reachability premise instead: the function is
// callable and returns a real answer, so if a caller is ever wired the
// behaviour is already exercised.
func TestDialIsSuppressedIsCallableAndAnswersCorrectly(t *testing.T) {
	m := trackedManager()

	if m.dialIsSuppressed(testNodeIDA, ProtoQUIC) {
		t.Fatal("reported suppressed before anything was recorded")
	}
	m.qualityTracker.RecordCooldown(dialKeyFor(testNodeIDA, ProtoQUIC), time.Hour)
	if !m.dialIsSuppressed(testNodeIDA, ProtoQUIC) {
		t.Fatal("a one-hour cooldown was recorded and the pure predicate still " +
			"reports the pair dialable — the observability answer is wrong, which " +
			"is worse than having no observability at all")
	}
}

// defaultQualityWeights is documented as "the central knob" so a config-driven
// override can later be added in one place. Pin that it is that single source:
// it must return the same weights every call, and they must be non-zero, or
// score-component weighting silently collapses to "everything counts equally".
func TestDefaultQualityWeightsIsAStableNonZeroKnob(t *testing.T) {
	a, b := defaultQualityWeights(), defaultQualityWeights()
	if a != b {
		t.Fatalf("two calls returned different weights (%+v vs %+v) — the "+
			"'central knob' is not a single source of truth", a, b)
	}
	if a == (quality.Weights{}) {
		t.Fatal("the default weights are the zero value — every score component " +
			"is weighted zero, so path scoring cannot discriminate at all")
	}
}

// ── The installable-session check ───────────────────────────────────────────
//
// This pins the logic, NOT the call site. `connectPeer` cannot be reached by a
// unit test, because its three transports are concrete pointer types with no
// seam to substitute a fake. That gap is stated here rather than left for a
// green suite to imply it is closed.

// A nil session is not installable — and must not panic. dialWithProtocol can
// return (nil, nil) through a transport that reports success without a session.
func TestANilSessionIsNotInstallable(t *testing.T) {
	if bs, ok := installableMeshConn(nil); ok || bs != nil {
		t.Fatalf("installableMeshConn(nil) = (%v, %v), want (nil, false) — a nil "+
			"session reported as installable is dereferenced immediately after", bs, ok)
	}
}

// 🔴 THE MESH-B01/E04 CASE: a dial that SUCCEEDS but hands back a session type
// the mesh cannot install. Before the guard this panicked; the defer-recover
// then marked the peer Disconnected without Closing, leaking the conn and its
// goroutines after dial-success had already been recorded.
func TestASessionOfTheWrongConcreteTypeIsNotInstallable(t *testing.T) {
	var session aether.Connection = &plainSession{nodeID: testNodeIDA}

	bs, ok := installableMeshConn(session)
	if ok {
		t.Fatalf("a %T was reported installable — the caller then reads .Conn off "+
			"a type that does not have it, which is the MESH-B01/E04 panic", session)
	}
	if bs != nil {
		t.Fatal("a rejected session still returned a non-nil connection")
	}
}

// And the admission twin: the guard must OPEN for the type it exists to accept,
// or "it refused" is indistinguishable from "it is broken shut".
func TestABaseConnectionIsInstallable(t *testing.T) {
	base := &aether.BaseConnection{}

	bs, ok := installableMeshConn(base)
	if !ok {
		t.Fatal("a *aether.BaseConnection — the exact type the mesh installs — was " +
			"REFUSED. Every refusal test above would still pass against a guard " +
			"that is broken shut, which is why this twin exists")
	}
	if bs != base {
		t.Fatalf("returned %p, want the session that was passed in (%p)", bs, base)
	}
}
