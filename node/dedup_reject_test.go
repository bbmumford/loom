/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"testing"
	"time"
)

// COVERAGE of dedupRejectAtFor (dedup_reject.go:53), which was 0.0%, plus the
// eviction and clear paths around it.
//
// 🔴 WHAT THIS TABLE DECIDES. A non-zero entry here SUPPRESSES a dial
// (mesh_connection.go:1239, multipath_dial.go:325). Too broad a key and a peer
// becomes undialable over a protocol that was never rejected; too narrow, or a
// read/write key mismatch, and the dedup never fires at all. Both failures are
// silent — one shows up as an unreachable peer, the other as a dial loop.
//
// 🔑 READ AND WRITE MUST AGREE ON THE KEY, AND THEY DO — MEASURED. Writers use
// session.Protocol.String (mesh_connection.go:1464, :1510); readers use
// mapProtocol(nodeProto).String (:1239, multipath_dial.go:325). Both land on
// the AETHER-layer string, so the table is self-consistent. Worth stating
// explicitly because it is the OPPOSITE choice from RecordPathSuccess, which
// deliberately keys at the NODE layer (see path_score_keying_test.go) — two
// neighbouring tables, two different protocol namespaces, and nothing in
// either file names the other.

func dedupFixture() *ConnectionManager {
	return &ConnectionManager{dedupRejectAt: map[string]time.Time{}}
}

// The composite key is what keeps the protocols independent. Keyed by nodeID
// alone, a WS dedup-reject blanket-blocks noise-UDP upgrade dials for the full
// 3-minute cooldown. The protocols must not see each other's rejects.
func TestARejectOnOneProtocolDoesNotSuppressAnother(t *testing.T) {
	m := dedupFixture()
	const peer = "peer-a"

	m.recordDedupRejectLocked(peer, mapProtocol(ProtoWebSocket).String())

	if m.dedupRejectAtFor(peer, mapProtocol(ProtoWebSocket).String()).IsZero() {
		t.Fatal("fixture wrong: the websocket reject was not recorded, so this test " +
			"cannot distinguish per-protocol keying from no keying at all")
	}
	if got := m.dedupRejectAtFor(peer, mapProtocol(ProtoNoiseUDP).String()); !got.IsZero() {
		t.Errorf("a websocket reject also suppressed noise-udp (stamped %v) — this is the "+
			"pre-L3-#11 behaviour, where one protocol's reject blanket-blocks upgrade "+
			"dials on every other for the full cooldown", got)
	}
}

// 🔴 THE SAME ROOT CAUSE, IN A SECOND TABLE — CHARACTERISATION, NOT APPROVAL.
//
// already pinned this collapse for the DIAL-COOLDOWN ladder
// (TestTLSAndWebSocketCurrentlyShareOneCooldownLadder in
// dial_suppression_test.go): mapProtocol is an ADAPTER-selection mapping that
// folds ProtoTLS onto aether.ProtoWebSocket because "TLS uses same adapter as
// WS", and using it as a KEY-space mapping merges two distinct protocols.
//
// 🔑 THIS FILE ESTABLISHES IT IS NOT ONE TABLE'S QUIRK. The dedup-reject table
// is a SEPARATE map with separate call sites, and it inherits the identical
// collapse from the identical cause. Two independent suppression mechanisms —
// dial cooldown and dedup reject — both fold TLS bootstrap into WebSocket, so
// a TLS failure suppresses WebSocket twice over, by two paths that do not know
// about each other. A characterisation of one table reads as a quirk; two
// instances of one cause is a pattern, and it is the pattern that justifies
// fixing mapProtocol's use as a key rather than patching either table.
//
// Routed as a question, not a change: the fix is a key-space decision spanning
// both tables, which is @R/DESIGN's call, not this lane's.
func TestTheDedupTableInheritsTheSameTLSWebSocketMergeAsTheCooldownLadder(t *testing.T) {
	tlsTag := mapProtocol(ProtoTLS).String()
	wsTag := mapProtocol(ProtoWebSocket).String()

	if tlsTag != wsTag {
		t.Fatalf("ProtoTLS (%q) and ProtoWebSocket (%q) no longer share a dedup tag — "+
			"that is very likely the FIX; update this test and its twin in "+
			"dial_suppression_test.go deliberately, and see ", tlsTag, wsTag)
	}

	// The consequence, exercised rather than inferred from the tags alone.
	m := dedupFixture()
	m.recordDedupRejectLocked("peer-a", tlsTag)
	if m.dedupRejectAtFor("peer-a", wsTag).IsZero() {
		t.Fatal("premise wrong: the shared tag did not actually transfer suppression " +
			"from TLS to WebSocket in the dedup table")
	}
}

// clearDedupRejectAllForPeer runs when a session registers successfully: every
// stale reject for that peer must go, on every protocol. Leaving one behind
// keeps suppressing dials on a peer that is demonstrably reachable.
func TestASuccessfulRegistrationClearsEveryProtocolForThatPeer(t *testing.T) {
	m := dedupFixture()
	const peer = "peer-a"
	for _, p := range []Protocol{ProtoNoiseUDP, ProtoQUIC, ProtoWebSocket, ProtoGRPC} {
		m.recordDedupRejectLocked(peer, mapProtocol(p).String())
	}
	if len(m.dedupRejectAt) != 4 {
		t.Fatalf("fixture wrong: %d entries, want 4 distinct protocol buckets", len(m.dedupRejectAt))
	}

	m.clearDedupRejectAllForPeer(peer)

	if n := len(m.dedupRejectAt); n != 0 {
		t.Errorf("%d reject entries survived a successful registration — dials stay "+
			"suppressed against a peer that has just proved it is reachable", n)
	}
}

// 🔴 THE SEPARATOR EARNS ITS KEEP. The clear is a PREFIX match on nodeID+"|".
// Without the "|" in the prefix, clearing "peer-a" would also clear "peer-ab",
// silently discarding a different peer's cooldown and re-opening the dial loop
// the reject exists to stop.
//
// 🔬 The fixture is anti-correlated on purpose: one node ID is a strict prefix
// of the other. With two unrelated IDs this test passes whether or not the
// separator is there, and proves nothing.
func TestClearingOnePeerDoesNotClearAPeerWhoseIDExtendsIt(t *testing.T) {
	m := dedupFixture()
	m.recordDedupRejectLocked("peer-a", "ws")
	m.recordDedupRejectLocked("peer-ab", "ws") // strict extension of the first

	m.clearDedupRejectAllForPeer("peer-a")

	if !m.dedupRejectAtFor("peer-a", "ws").IsZero() {
		t.Error("the targeted peer's entry survived its own clear")
	}
	if m.dedupRejectAtFor("peer-ab", "ws").IsZero() {
		t.Error("clearing \"peer-a\" also cleared \"peer-ab\" — the prefix match is not " +
			"anchored on the separator, so one peer registering discards the cooldown of " +
			"every peer whose ID extends its own")
	}
}

// eviction: a stale entry must be reclaimed, or the map grows without
// bound across a churning fleet.
func TestRecordingARejectEvictsEntriesPastTheTTL(t *testing.T) {
	m := dedupFixture()
	m.dedupRejectAt["ancient|ws"] = time.Now().Add(-dedupRejectEvictTTL - time.Minute)

	m.recordDedupRejectLocked("peer-a", "ws")

	if _, still := m.dedupRejectAt["ancient|ws"]; still {
		t.Error("an entry older than the eviction TTL survived — the table grows without " +
			"bound across a fleet whose node IDs churn")
	}
	if m.dedupRejectAtFor("peer-a", "ws").IsZero() {
		t.Error("the new reject was lost during eviction")
	}
}

// 🔴 THE HALF OF EVICTION THAT COULD DO HARM, and the reason the previous test
// is not enough on its own. dedup_reject.go:11 claims the TTL is "far past any
// cooldown window, so eviction never drops a live-cooldown entry". Dropping one
// would silently re-open the dial loop the reject exists to suppress — the
// eviction would look like it was working while quietly defeating the feature.
//
// 🔬 Anti-correlated with the test above: that one asserts an entry DOES go,
// this one that a different entry does NOT. An eviction rule that fired on
// everything would pass the first test alone.
func TestEvictionDoesNotDropAnEntryStillInsideItsCooldown(t *testing.T) {
	const observedCooldown = 3 * time.Minute // the window named in dedup_reject.go:21
	if dedupRejectEvictTTL <= observedCooldown {
		t.Fatalf("premise broken: eviction TTL %v is not past the %v cooldown, so eviction "+
			"can drop entries that are still suppressing dials", dedupRejectEvictTTL, observedCooldown)
	}
	m := dedupFixture()
	m.dedupRejectAt["peer-b|ws"] = time.Now().Add(-observedCooldown + time.Second)

	m.recordDedupRejectLocked("peer-a", "ws")

	if m.dedupRejectAtFor("peer-b", "ws").IsZero() {
		t.Errorf("an entry only %v old was evicted despite a %v TTL — a peer still inside "+
			"its cooldown had the suppression lifted, re-opening the dial loop",
			observedCooldown, dedupRejectEvictTTL)
	}
}

// Both readers must tolerate the map never having been created. recordDedup
// lazily allocates it, so every read before the first write sees nil.
//
// 🔬 A SURVIVING MUTANT THAT IS NOT A TEST GAP. Deleting the
// `if m.dedupRejectAt == nil` guard in dedupRejectAtFor leaves every test here
// green — correctly. Reading a nil map in Go yields the zero value and ranging
// one is a no-op, so both guards are REDUNDANT rather than load-bearing, and
// no test can kill their removal because removing them changes no behaviour.
//
// Recorded because the alternative readings are both wrong and both tempting:
// treating it as a hole invites a test that cannot exist, and leaving it
// unremarked lets a later reader assume the guards are what make this path
// safe. They are not — Go's map semantics are. The guards stay: they cost
// nothing and they state the intent.
func TestDedupRejectReadersTolerateAnUnallocatedTable(t *testing.T) {
	m := &ConnectionManager{} // dedupRejectAt is nil

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("a dedup-reject read panicked on an unallocated table: %v — every "+
				"read before the first reject takes this path", r)
		}
	}()

	if got := m.dedupRejectAtFor("peer-a", "ws"); !got.IsZero() {
		t.Errorf("dedupRejectAtFor on a nil table = %v, want the zero time (no reject "+
			"recorded); a non-zero value here suppresses a dial that was never rejected", got)
	}
	m.clearDedupRejectAllForPeer("peer-a")
}
