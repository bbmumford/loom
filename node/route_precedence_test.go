/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"sort"
	"testing"
	"time"
)

// findNextHops documents the route engine as the authoritative source of next
// hops — dual-signed path advertisements with RFC2439 damping — and LAD latency
// records as the fallback for partitions no advertisement has reached.
//
// Route provenance used to travel as the transport string "route". The ranking
// sort passes transport to remoteTransportGradeForRanking, which parses it
// strictly and returns GradeF for anything that is not a transport name, so the
// authoritative candidate scored zero and sorted below every candidate whose
// transport happened to parse. fromRoute carries that provenance now.

// rankCandidates sorts through the PRODUCTION comparator. Re-implementing the
// comparison here would stay green even if the production sort lost the
// provenance clause entirely — verifying the mechanism instead of the wiring.
func rankCandidates(f *runtimeForwarder, in []nextHopCandidate) []nextHopCandidate {
	out := append([]nextHopCandidate(nil), in...)
	sort.Slice(out, func(i, j int) bool { return f.nextHopLess(out[i], out[j]) })
	return out
}

// 🔴 THE AUTHORITATIVE SOURCE MUST OUTRANK THE FALLBACK.
//
// 🔬 THE LAD CANDIDATE IS DELIBERATELY GIVEN THE BEST POSSIBLE TRANSPORT. If it
// carried a weak one the route candidate could win on score alone and the test
// would pass without provenance mattering at all. noise-udp grades A, so the
// only thing that can put the route candidate first is fromRoute.
func TestARouteEngineCandidateOutranksTheStrongestHeuristicOne(t *testing.T) {
	f := &runtimeForwarder{rt: &Runtime{}}

	got := rankCandidates(f, []nextHopCandidate{
		{nodeID: "lad-peer", transport: "noise-udp", rttMs: 5},
		{nodeID: "route-peer", transport: "", rttMs: 900, fromRoute: true},
	})

	if got[0].nodeID != "route-peer" {
		t.Errorf("ranking put %q first; the route-engine candidate must lead. Carrying "+
			"provenance in the transport string made it parse as an unknown transport and "+
			"score GradeF, so the source findNextHops calls authoritative sorted last",
			got[0].nodeID)
	}
}

// 🔬 THE CONTROL. Provenance must not become a blanket override of everything
// else: among candidates of the SAME provenance the transport grade still
// decides, or the change has simply replaced one inverted ordering with another.
func TestAmongRouteCandidatesTheOrdinaryScoreStillDecides(t *testing.T) {
	f := &runtimeForwarder{rt: &Runtime{}}

	got := rankCandidates(f, []nextHopCandidate{
		{nodeID: "slow", transport: "", rttMs: 900, fromRoute: true},
		{nodeID: "fast", transport: "", rttMs: 5, fromRoute: true},
	})

	if got[0].nodeID != "fast" {
		t.Errorf("ranking put %q first among two route candidates; with provenance equal "+
			"the ordinary RTT ordering must still apply", got[0].nodeID)
	}
}

// The heuristic candidates keep their existing relative order, so this changes
// which class leads without disturbing ranking inside the fallback class.
func TestHeuristicCandidatesKeepTheirTransportGradeOrder(t *testing.T) {
	f := &runtimeForwarder{rt: &Runtime{}}

	got := rankCandidates(f, []nextHopCandidate{
		{nodeID: "websocket-peer", transport: "websocket", age: time.Second},
		{nodeID: "noise-peer", transport: "noise-udp", age: time.Second},
	})

	if got[0].nodeID != "noise-peer" {
		t.Errorf("ranking put %q first; noise-udp grades A and websocket grades C, so the "+
			"noise peer must lead among heuristic candidates", got[0].nodeID)
	}
}

// A genuinely unknown transport must still fail closed to GradeF. The fix
// removed one sentinel from that bucket; it must not have opened the bucket.
func TestAnUnknownTransportStillGradesF(t *testing.T) {
	if got := remoteTransportGradeForRanking("definitely-not-a-transport"); got != GradeF {
		t.Errorf("an unparseable transport graded %v, want GradeF — the ranker no longer "+
			"fails closed on labels it cannot parse", got)
	}
	if got := remoteTransportGradeForRanking("route"); got != GradeF {
		t.Errorf("the retired \"route\" sentinel graded %v; it is no longer produced, and "+
			"if it reappears it is an unparseable transport like any other", got)
	}
}
