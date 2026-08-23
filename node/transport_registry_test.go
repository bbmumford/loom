/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"testing"
	"time"
)

// Covers the per-peer TRANSPORT REGISTRY — the layer beneath sessions, where
// the dormant fallback that registerMeshSession installs actually lands. Six
// functions: addTransport, removeTransport, hasActiveTransport,
// getDormantTransport, promoteGrade, and the ParseProtocol/String pair.
//
// The registry decides `allDead`, and `allDead` drives teardown and re-dial.
// A wrong answer here does not error — the peer is either torn down while a
// live path remains, or kept while none does. That is the silent class this
// pass keeps finding.

// tregNodeID is wire-shaped: production truncates NodeIDs for logging
// (truncID slices [:12]) and real IDs are 64-char hex(pubkey). A short
// readable label panics the code under test, which would be a fact about
// the fixture and not about the registry.
const tregNodeID = "3f2a9c1e4b8d7605f1a2b3c4d5e6f708192a3b4c5d6e7f8091a2b3c4d5e6f708"

func tregTransport(proto Protocol, g Grade, dormant bool) *transportConn {
	return &transportConn{
		protocol:    proto,
		grade:       g,
		isDormant:   dormant,
		connectedAt: time.Now(),
	}
}

func tregPeer() *peerConn {
	return &peerConn{nodeID: tregNodeID, reconnectDelay: baseCooldown}
}

// THE PROPERTY THE DELETED DOC LINE DENIED.
//
// addTransport's body is an unconditional map assignment with no grade
// comparison and no early return, so the freshest transport always wins its
// slot. This test pins that behaviour, so reintroducing a grade guard — or a
// doc line claiming a higher-grade transport is not replaced — fails loudly
// here.
func TestAddTransportInstallsTheFreshestTransportRegardlessOfGrade(t *testing.T) {
	p := tregPeer()

	best := tregTransport(ProtoWebSocket, GradeA, false)
	p.addTransport(best)
	if p.transports[ProtoWebSocket] != best {
		t.Fatal("premise wrong: the first transport was not installed at all")
	}

	// A STRICTLY LOWER grade, same protocol key. Under the deleted doc's
	// claim this must be refused; under v0.0.218 it must win.
	worse := tregTransport(ProtoWebSocket, GradeC, false)
	if !best.grade.BetterThan(worse.grade) {
		t.Fatal("premise wrong: the two fixtures do not straddle a grade boundary")
	}
	p.addTransport(worse)

	if got := p.transports[ProtoWebSocket]; got != worse {
		t.Fatalf("a lower-grade transport did NOT replace the higher-grade one "+
			"(slot holds %p, want the fresher %p) — a grade guard has been "+
			"reintroduced into addTransport's unconditional assignment", got, worse)
	}
	if len(p.transports) != 1 {
		t.Fatalf("replacement grew the map to %d entries — the protocol key is "+
			"no longer single-slot, which is the drift v0.0.218 set out to fix",
			len(p.transports))
	}
}

// The observed-good signal: an accepted transport proves the peer is
// reachable right now, so an escalated backoff must collapse. Without this,
// a peer that flapped up to maxCooldown keeps that backoff after a fresh
// transport lands and waits minutes before re-dialling the next drop.
func TestAddTransportCollapsesAnEscalatedReconnectBackoff(t *testing.T) {
	if maxCooldown == baseCooldown {
		t.Fatal("premise wrong: the two cooldowns are equal, so this test " +
			"cannot distinguish a reset from a no-op")
	}
	p := tregPeer()
	p.reconnectDelay = maxCooldown

	p.addTransport(tregTransport(ProtoNoiseUDP, GradeA, false))

	if p.reconnectDelay != baseCooldown {
		t.Fatalf("reconnectDelay = %v after a transport landed, want the boot "+
			"default %v — a peer that flapped earlier keeps its long backoff and "+
			"the next transient drop waits minutes before re-dial",
			p.reconnectDelay, baseCooldown)
	}
}

// THE ORPHAN-STATE DRIFT THE SESSION-AWARE SIGNATURE EXISTS TO PREVENT.
//
// Session A registers; the upgrade flow produces session B on the same
// protocol; A subsequently closes and its cleanup goroutine calls
// removeTransport. If that removal were keyed on protocol alone it would
// wipe B's slot — peer.transports would show nothing for the protocol while
// the multipath registry still held B, so scan-loop gates reading
// peer.transports would stop seeing a live path.
func TestRemoveTransportWillNotEvictAFresherSessionsSlot(t *testing.T) {
	p := tregPeer()
	stale := tregTransport(ProtoNoiseUDP, GradeA, false)
	fresh := tregTransport(ProtoNoiseUDP, GradeA, false)

	p.addTransport(stale)
	p.addTransport(fresh) // same protocol key: fresh now owns the slot
	if p.transports[ProtoNoiseUDP] != fresh {
		t.Fatal("premise wrong: the fresher session never took the slot")
	}

	allDead := p.removeTransport(ProtoNoiseUDP, stale)

	if p.transports[ProtoNoiseUDP] != fresh {
		t.Fatal("the STALE session's cleanup evicted the FRESH session's slot — " +
			"peer.transports now shows no path for a protocol that has a live one, " +
			"and every scan-loop gate reading it goes blind")
	}
	if allDead {
		t.Fatal("removeTransport reported allDead while a live transport remains — " +
			"the caller will tear down a connected peer")
	}
}

// The legacy force-delete. Documented as "pass nil for tc to force-delete by
// protocol"; pinned so the nil branch cannot quietly become a no-op, which
// would leak the slot and keep a dead peer looking connected.
func TestRemoveTransportWithNilForceDeletesBySlot(t *testing.T) {
	p := tregPeer()
	p.addTransport(tregTransport(ProtoGRPC, GradeB, false))

	allDead := p.removeTransport(ProtoGRPC, nil)

	if _, still := p.transports[ProtoGRPC]; still {
		t.Fatal("the nil force-delete left the slot populated")
	}
	if !allDead {
		t.Fatal("allDead was false after the peer's only transport was removed")
	}
}

// DORMANT IS NOT ACTIVE, and allDead is computed from ACTIVE transports only.
//
// registerMeshSession always keeps a lower-grade session as a dormant
// fallback, so a peer can hold a dormant transport and still be correctly
// reported all-dead when its active one goes.
func TestAllDeadIgnoresASurvivingDormantTransport(t *testing.T) {
	p := tregPeer()
	active := tregTransport(ProtoNoiseUDP, GradeA, false)
	dormant := tregTransport(ProtoWebSocket, GradeC, true)
	p.addTransport(active)
	p.addTransport(dormant)

	if p.removeTransport(ProtoNoiseUDP, active) != true {
		t.Fatal("a peer whose only remaining transport is DORMANT was reported " +
			"still-alive — dormant is a routing flag, not a usable path, so the " +
			"peer will never be re-dialled and the fallback is never reactivated")
	}
	// The dormant entry must survive the removal: it is the fallback.
	if p.transports[ProtoWebSocket] != dormant {
		t.Fatal("removing the active transport also dropped the dormant fallback")
	}
}

// THE NIL-MAP / EMPTY-MAP ASYMMETRY — the trap in this file.
//
// hasActiveTransport consults p.state when transports is nil ("nil until
// first multi-transport connection" — the legacy single-transport peer), and
// ignores p.state entirely once the map exists. So the SAME peer state gives
// OPPOSITE answers either side of one add/remove cycle.
//
// Pinned rather than changed: the nil branch is a deliberate legacy fallback,
// and the direction that matters (an emptied map reports all-dead even while
// state still says connected) is the safe one.
//
// The other direction — a nil map plus PeerConnected reporting a live path
// with zero transports recorded — is unreachable:
//
//	addTransport has exactly ONE non-test call site, AcceptMeshConnection:113,
//	and it is unconditional on every path that goes on to register a session
//	(registerMeshSession's sole caller is :181, downstream of it). All three
//	writers of state = PeerConnected therefore imply a non-nil map:
//	mesh_connection.go:114 runs one line after the addTransport;
//	peer_connections.go:2840 is guarded by a live meshSessions entry; and
//	:4520 is guarded by getDormantTransport() != nil.
//
// So this half of the test constructs a state production does not currently
// build. It is kept deliberately: it DEFINES the answer for the day someone
// adds a fourth PeerConnected writer that does not go through :113 — which
// is the cheap mistake this shape invites — rather than leaving it to be
// discovered as a peer that is never torn down.
func TestHasActiveTransportAnswersFromStateOnlyWhileTheMapIsNil(t *testing.T) {
	nilMap := tregPeer()
	nilMap.state = PeerConnected
	if nilMap.transports != nil {
		t.Fatal("premise wrong: the fixture's map is not nil")
	}
	if !nilMap.hasActiveTransport() {
		t.Fatal("a nil-map peer in PeerConnected reported no active transport — " +
			"the legacy single-transport fallback is gone and such peers will be " +
			"torn down as dead")
	}

	emptied := tregPeer()
	emptied.state = PeerConnected // identical state to the case above
	tc := tregTransport(ProtoQUIC, GradeB, false)
	emptied.addTransport(tc)
	emptied.removeTransport(ProtoQUIC, tc)
	if emptied.transports == nil {
		t.Fatal("premise wrong: the map went back to nil, so both halves of this " +
			"test are the same case and the asymmetry is untested")
	}
	if emptied.hasActiveTransport() {
		t.Fatal("a peer whose transports were all removed still reported an active " +
			"transport because p.state says PeerConnected — a dead peer would be " +
			"kept and never re-dialled")
	}
}

func TestGetDormantTransportSelectsOnlyDormantEntries(t *testing.T) {
	p := tregPeer()
	if p.getDormantTransport() != nil {
		t.Fatal("a peer with a nil transport map offered a dormant transport")
	}

	active := tregTransport(ProtoNoiseUDP, GradeA, false)
	p.addTransport(active)
	if got := p.getDormantTransport(); got != nil {
		t.Fatalf("an ACTIVE transport was offered for reactivation (%v) — "+
			"reactivating a path that is already carrying traffic", got.protocol)
	}

	dormant := tregTransport(ProtoWebSocket, GradeC, true)
	p.addTransport(dormant)
	if got := p.getDormantTransport(); got != dormant {
		t.Fatalf("getDormantTransport returned %v, want the sole dormant entry", got)
	}
}

// promoteGrade is a monotonic high-water mark: it raises bestEverGrade and
// never lowers it. Anything reading bestEverGrade as "the grade now" is
// therefore wrong by construction — bestActiveGrade is that reading.
func TestPromoteGradeIsAMonotonicHighWaterMark(t *testing.T) {
	p := tregPeer()
	now := time.Now()

	p.promoteGrade(GradeC, now)
	if p.bestEverGrade != GradeC {
		t.Fatalf("bestEverGrade = %v after the first promotion, want %v",
			p.bestEverGrade, GradeC)
	}

	p.promoteGrade(GradeA, now)
	if p.bestEverGrade != GradeA {
		t.Fatalf("a BETTER grade did not raise the high-water mark (%v, want %v) — "+
			"an upgraded peer is never recognised as upgraded", p.bestEverGrade, GradeA)
	}

	p.promoteGrade(GradeF, now)
	if p.bestEverGrade != GradeA {
		t.Fatalf("a WORSE grade lowered the high-water mark to %v — bestEverGrade "+
			"is no longer 'best ever' and a peer that degrades once loses the record "+
			"of what it was capable of", p.bestEverGrade)
	}
}

// The wire vocabulary. A protocol name that does not survive String→Parse is
// a silent downgrade, not an error: ParseProtocol's documented default is
// ProtoNoiseUDP, so any unrecognised name becomes a UDP dial attempt.
func TestProtocolVocabularyRoundTripsAndDefaultsToUDP(t *testing.T) {
	for _, proto := range protocolOrder {
		if got := ParseProtocol(proto.String()); got != proto {
			t.Errorf("ParseProtocol(%q) = %v, want %v — this protocol's own "+
				"String() output is not accepted by its own parser, so every "+
				"round-trip through a string field silently retargets the dial",
				proto.String(), got, proto)
		}
	}

	// Accepted aliases. These are the names other producers actually write, the
	// same set directory.normaliseReachProto accepts — a case arm matching a
	// string no producer emits recognises nothing.
	for name, want := range map[string]Protocol{
		"vl1": ProtoNoiseUDP, // VL1 listeners
		"ws":  ProtoWebSocket,
		"wss": ProtoWebSocket,
	} {
		if got := ParseProtocol(name); got != want {
			t.Errorf("ParseProtocol(%q) = %v, want %v — a live alias is "+
				"unrecognised and falls through to the UDP default", name, got, want)
		}
	}

	// The documented fail-toward-UDP default, including the empty string: an
	// unmapped zero value here yields an address with no protocol, which no
	// dialer can act on.
	for _, unknown := range []string{"", "unknown", "sctp", "WSS", "noise_udp"} {
		if got := ParseProtocol(unknown); got != ProtoNoiseUDP {
			t.Errorf("ParseProtocol(%q) = %v, want the documented ProtoNoiseUDP "+
				"default", unknown, got)
		}
	}

	if Protocol(99).String() != "unknown" {
		t.Errorf("an out-of-range Protocol stringified as %q, want \"unknown\"",
			Protocol(99).String())
	}
}
