/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"sort"

	aether "github.com/ORBTR/aether"
)

// scoreRoute returns a route's composite cost — lower is better. Used by
// FindRoutes and the rpc_forward.go forwarder to compare direct vs
// relayed paths on like terms.
//
// Components:
//   - base: measured RTT (ms) to the first hop if known, else a
//     grade-derived synthetic latency (A=5, B=20, C=40, F=1000)
//   - hopPenalty: ~100ms per intermediate node in the route (direct=0)
//
// Cascade F (L3 #3): FindRoutes used to return routes in positional
// order — direct first, then relays sorted by their leg-RTT to the
// target. Parallel probes in HWPCaller.Call could let a low-RTT relay
// beat a high-RTT direct path. With this scoring the slice is sorted by
// composite cost, so a healthy direct path wins as long as its total
// RTT is comparable to the relay's combined two-leg RTT.
//
// rttByPeerMs is "first-hop RTT in ms" keyed by the first-hop's nodeID
// (peer the caller has direct connectivity to). Caller pre-computes
// from LAD latency records.
func scoreRoute(session aether.Session, hops int, firstHopRTTMs int64) int64 {
	g := SessionGrade(session)
	var base int64
	switch g {
	case GradeA:
		base = 5
	case GradeB:
		base = 20
	case GradeC:
		base = 40
	default:
		base = 1000
	}
	if firstHopRTTMs > 0 {
		base = firstHopRTTMs
	}
	hopPenalty := int64(hops) * 100
	return base + hopPenalty
}

// sortProbeRoutesByScore sorts the routes slice in-place by composite
// cost ascending (best first). rttByPeerMs maps a route's first-hop
// nodeID to its measured RTT in milliseconds; missing entries fall back
// to grade-derived synthetic latency.
//
// Tiebreak (equal score): higher grade wins.
func sortProbeRoutesByScore(routes []aether.ProbeRoute, rttByPeerMs map[string]int64) {
	if len(routes) < 2 {
		return
	}
	sort.SliceStable(routes, func(i, j int) bool {
		si := scoreRoute(routes[i].Session, len(routes[i].RouteList), rttByPeerMs[routes[i].NodeID])
		sj := scoreRoute(routes[j].Session, len(routes[j].RouteList), rttByPeerMs[routes[j].NodeID])
		if si != sj {
			return si < sj
		}
		return SessionGrade(routes[i].Session) > SessionGrade(routes[j].Session)
	})
}
