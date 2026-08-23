/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	lad "github.com/bbmumford/ledger"
	ladcache "github.com/bbmumford/ledger/cache"
)

// Covers next-hop selection for forwarded RPCs: `findNextHops` and
// `ForwardingStats`. Censused per symbol, one level out, and checked for
// interface satisfaction:
//
//	findNextHops     <- rpc_forward.go:166, inside Forward
//	ForwardingStats  <- runtime.go:1871, the forwarding MeshMetrics keys
//	Forward          <- satisfies RPCForwarder (rpc.go:30) and is dispatched
//	                    through s.forwarder, so mode 3 applies to it
//
// 🔴 THIS IS WHERE A FORWARDED RPC CHOOSES ITS RELAY. Every failure is a
// routing failure that still returns something: a hop we cannot reach, the
// target itself, or ourselves. The dedup and the self-exclusion are the two
// guards that make the returned list dialable at all.

func forwarderFixture(t *testing.T) *runtimeForwarder {
	t.Helper()
	m := registerTestManager()
	m.selfID = testNodeIDA
	m.peers = map[string]*peerConn{}
	rt := &Runtime{cache: ladcache.NewDirectoryCache()}
	rt.connMgr = m
	m.rt = rt
	return newRuntimeForwarder(rt)
}

// 🔴 NEVER RETURN OURSELVES AS A RELAY. A self-hop is an immediate forwarding
// loop, and the hop counter is the only thing that would eventually stop it.
func TestSelfIsNeverOfferedAsANextHop(t *testing.T) {
	f := forwarderFixture(t)
	// A latency edge between us and the target: the only candidate the LAD
	// scan could derive from it is ourselves.
	applyLatency(t, f.rt.connMgr, testNodeIDA, testNodeIDB, 10)

	for _, c := range f.findNextHops(testNodeIDB, testNodeIDA) {
		if c.nodeID == testNodeIDA {
			t.Fatalf("self was offered as a next hop toward %s — the RPC "+
				"forwards to this node, which forwards again, until MaxRPCHops "+
				"rejects it", testNodeIDB)
		}
	}
}

// 🔑 LATENCY EDGES ARE BIDIRECTIONAL, so a relay must be found from either
// direction of the record. Checking only one direction halves the usable relay
// set for no reason — gossip connections measure both ways.
func TestARelayIsFoundFromEitherDirectionOfALatencyEdge(t *testing.T) {
	const relay = "cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33"

	t.Run("relay -> target", func(t *testing.T) {
		f := forwarderFixture(t)
		applyLatency(t, f.rt.connMgr, relay, testNodeIDB, 12)
		if !hasHop(f.findNextHops(testNodeIDB, testNodeIDA), relay) {
			t.Fatal("a relay measured TOWARD the target was not offered — half " +
				"the usable relay set is invisible")
		}
	})

	t.Run("target -> relay", func(t *testing.T) {
		f := forwarderFixture(t)
		applyLatency(t, f.rt.connMgr, testNodeIDB, relay, 12)
		if !hasHop(f.findNextHops(testNodeIDB, testNodeIDA), relay) {
			t.Fatal("a relay measured FROM the target was not offered — the " +
				"reverse direction of a bidirectional edge is being ignored")
		}
	})
}

// One peer must appear at most once however many latency records mention it.
// A duplicated hop makes the probe layer fan out twice down the same path.
func TestEachPeerAppearsAtMostOnce(t *testing.T) {
	const relay = "cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33"
	f := forwarderFixture(t)
	// Two records naming the same relay: one in each direction.
	applyLatency(t, f.rt.connMgr, relay, testNodeIDB, 12)
	applyLatency(t, f.rt.connMgr, testNodeIDB, relay, 40)

	hops := f.findNextHops(testNodeIDB, testNodeIDA)
	seen := 0
	for _, c := range hops {
		if c.nodeID == relay {
			seen++
		}
	}
	if seen == 0 {
		t.Fatalf("premise wrong: the relay was not offered at all (%d hops)", len(hops))
	}
	if seen > 1 {
		t.Fatalf("the same relay appears %d times — the probe layer fans out "+
			"twice down one path and burns two of its three route slots on the "+
			"same peer", seen)
	}
}

// A cache-less forwarder must return the route-engine candidates (none here)
// rather than panicking: `rt.cache` is nil on a minimal build, and
// findNextHops runs on the forwarding path of every node.
func TestACacheLessForwarderReturnsNothingRatherThanPanicking(t *testing.T) {
	m := registerTestManager()
	m.selfID = testNodeIDA
	rt := &Runtime{} // no cache, no route engine
	rt.connMgr = m
	m.rt = rt

	f := newRuntimeForwarder(rt)
	if got := f.findNextHops(testNodeIDB, testNodeIDA); len(got) != 0 {
		t.Fatalf("a forwarder with no cache offered %d hops — they were derived "+
			"from nothing", len(got))
	}
}

// A latency record that mentions neither the target nor a distinct third party
// contributes no hop. Without this the list fills with peers that have no
// measured path to the target at all.
func TestUnrelatedLatencyRecordsContributeNoHops(t *testing.T) {
	const other = "dd44dd44dd44dd44dd44dd44dd44dd44dd44dd44dd44dd44dd44dd44dd44dd44"
	const alsoOther = "ee55ee55ee55ee55ee55ee55ee55ee55ee55ee55ee55ee55ee55ee55ee55ee55"
	f := forwarderFixture(t)
	applyLatency(t, f.rt.connMgr, other, alsoOther, 5) // nothing to do with the target

	if got := f.findNextHops(testNodeIDB, testNodeIDA); len(got) != 0 {
		t.Fatalf("an unrelated latency edge produced %d hops (%+v) — the "+
			"forwarder would relay through a peer with no measured path to the "+
			"target", len(got), got)
	}
}

// ── Forwarding counters ─────────────────────────────────────────────────────

// The three counters feed distinct MeshMetrics keys. They must be independent,
// or an operator cannot tell a direct forward from a LAD-routed one — which is
// the difference between a healthy mesh and one relying on relays.
func TestForwardingStatsExposesThreeIndependentCounters(t *testing.T) {
	f := forwarderFixture(t)

	if got := f.ForwardingStats(); len(got) != 3 {
		t.Fatalf("ForwardingStats has %d keys, want 3 (%v)", len(got), got)
	}
	for _, k := range []string{"direct", "role", "lad_routed"} {
		if v, ok := f.ForwardingStats()[k]; !ok {
			t.Fatalf("key %q missing — its MeshMetrics series disappears", k)
		} else if v != 0 {
			t.Fatalf("key %q starts at %d, want 0", k, v)
		}
	}

	atomic.AddInt64(&f.forwardDirect, 3)
	atomic.AddInt64(&f.forwardRole, 2)
	atomic.AddInt64(&f.forwardLADRouted, 1)

	got := f.ForwardingStats()
	if got["direct"] != 3 || got["role"] != 2 || got["lad_routed"] != 1 {
		t.Fatalf("stats = %v, want direct=3 role=2 lad_routed=1 — the counters "+
			"are crossed or share storage, so an operator cannot tell a direct "+
			"forward from a relayed one", got)
	}
}

// hasHop reports whether the candidate list contains the given node.
func hasHop(hops []nextHopCandidate, nodeID string) bool {
	for _, c := range hops {
		if c.nodeID == nodeID {
			return true
		}
	}
	return false
}

// Freshness is a tiebreak, so a stale record must still yield a usable hop —
// dropping it entirely would leave a partition with no relay at all. The 2
// minute window only orders candidates; it does not filter them.
func TestAStaleLatencyRecordStillYieldsAUsableHop(t *testing.T) {
	const relay = "cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33"
	f := forwarderFixture(t)
	applyStaleLatency(t, f.rt.connMgr, relay, testNodeIDB, 12, 10*time.Minute)

	if !hasHop(f.findNextHops(testNodeIDB, testNodeIDA), relay) {
		t.Fatal("a 10-minute-old latency record yielded no hop — freshness is " +
			"being used as a FILTER rather than a tiebreak, so a partition with " +
			"only stale measurements has no relay at all")
	}
}

// The ranker fails CLOSED on peer-supplied transport labels: an unrecognised
// label must not be graded as though it named a good transport, because the
// label arrives from the peer and the grade decides the hop.
//
// MEASURED BEFORE (throwaway probe, values printed not inferred):
//
//	GradeA = 4 (best) … GradeF = 0 (worst); ProtoNoiseUDP == 0 == Protocol's ZERO
//	VALUE, and ParseProtocol's default returned it, so "" · "gossip-tls" ·
//	"route" · "garbage" all graded 4 — the BEST grade — while honest "wss"/"tls"
//	graded 2. Since `Transport` is `omitempty` (ledger/types.go:261), ABSENT is
//	the ordinary wire case: a peer that published nothing outranked a peer that
//	published honestly. An incentive inversion on hostile input.
//
// The fix is two separable pieces, and this test pins both plus the boundary
// between them:
//
//	(a) "gossip-tls" — a DOCUMENTED value of that field — joined the label set,
//	    so it now grades C like the rest of its transport class, everywhere.
//	(b) At the RANKING SITE ONLY, an unrecognised or absent label grades WORST.
//	    ParseProtocol's lossy default is deliberately UNCHANGED: five other
//	    callers depend on it and flipping it is a package-wide policy change.
func TestTheRankerGradesUnparseableTransportsWorstNotBest(t *testing.T) {
	// (b) Ranking-local: unknown and absent labels fail closed.
	for _, transport := range []string{"", "route", "garbage", "  "} {
		if got := remoteTransportGradeForRanking(transport); got != GradeF {
			t.Errorf("remoteTransportGradeForRanking(%q) = %d, want GradeF (%d) — a "+
				"peer that publishes an unparseable transport outranks one that "+
				"publishes honestly, and `omitempty` makes absent the ordinary case",
				transport, got, GradeF)
		}
	}

	// Honest labels are unaffected, including (a)'s new one.
	for _, tc := range []struct {
		transport string
		want      Grade
	}{
		{"noise-udp", GradeA},
		{"quic", GradeB},
		{"wss", GradeC},
		{"tls", GradeC},
		{"gossip-tls", GradeC}, // (a): was GradeA before the fix
	} {
		if got := remoteTransportGradeForRanking(tc.transport); got != tc.want {
			t.Errorf("remoteTransportGradeForRanking(%q) = %d, want %d",
				tc.transport, got, tc.want)
		}
	}

	// (a) is NOT ranking-local — it fixes the label set itself, so the shared
	// ParseProtocol path sees it too. This is what makes health_evaluator.go:344
	// inherit the correction for free.
	if got := GradeForProtocol(ParseProtocol("gossip-tls")); got != GradeC {
		t.Errorf("GradeForProtocol(ParseProtocol(\"gossip-tls\")) = %d, want GradeC "+
			"(%d) — the shared path still misgrades a documented transport value",
			got, GradeC)
	}

	// The scope boundary, pinned deliberately: ParseProtocol's default stays
	// lossy for genuinely unknown labels, because its other non-test callers
	// depend on that default. If this assertion fails, a ranking-local fix has
	// been widened into a package-wide grading-policy change.
	if got := GradeForProtocol(ParseProtocol("garbage")); got != GradeA {
		t.Errorf("ParseProtocol's default now grades %d for an unknown label, not "+
			"GradeA (%d) — the ranking-local fix has leaked into the shared "+
			"mapping its other callers depend on.", got, GradeA)
	}

	// GradeForProtocol's defensive `default: GradeF` arm remains unreachable via
	// ParseProtocol (it only returns the five named constants). The ranker names
	// GradeF directly rather than relying on that arm; the arm stays in place.
	if GradeForProtocol(Protocol(99)) != GradeF {
		t.Error("the defensive default arm no longer yields GradeF")
	}
}

// applyStaleLatency is applyLatency with a controllable MeasuredAt, so the
// freshness tiebreak can be exercised without waiting.
func applyStaleLatency(t *testing.T, m *ConnectionManager, from, to string, rttMs int64, age time.Duration) {
	t.Helper()
	b, _ := json.Marshal(lad.LatencyRecord{
		FromNode: from, ToNode: to, RTTMs: rttMs,
		MeasuredAt: time.Now().Add(-age), ExpiresAt: time.Now().Add(time.Hour),
	})
	if err := m.rt.cache.Apply(lad.Record{
		Topic: lad.TopicLatency, NodeID: from, Body: b, Timestamp: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
}
