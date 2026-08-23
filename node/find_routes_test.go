/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	lad "github.com/bbmumford/ledger"
)

// COVERAGE of FindRoutes, 0.0% — the last uncovered function in
// mesh_session_finder.go.
//
// It computes up to 3 probe routes to a role's node: a DIRECT session first,
// then RELAYS discovered from latency records, then a blind-relay fallback.
// Every failure is a routing failure that still returns something: a route
// through a peer we cannot reach, or no route at all when one exists.
//
// 🙋 Fourth cost measured rather than estimated: every
// dependency already had a fixture — roleFixture for the role table, bind for
// sessions, and the cache accepts latency records directly. ~20 lines.

func applyLatency(t *testing.T, m *ConnectionManager, from, to string, rttMs int64) {
	t.Helper()
	b, _ := json.Marshal(lad.LatencyRecord{
		FromNode: from, ToNode: to, RTTMs: rttMs,
		MeasuredAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour),
	})
	if err := m.rt.cache.Apply(lad.Record{
		Topic: lad.TopicLatency, NodeID: from, Body: b, Timestamp: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
}

func TestFindRoutesReturnsNothingWhenNoNodeServesTheRole(t *testing.T) {
	m := roleFixture(t, "auth", testNodeIDB)

	if got := m.FindRoutes(context.Background(), "billing", ""); got != nil {
		t.Fatalf("FindRoutes returned %d routes for an unadvertised role, want "+
			"none — every one of them probes a node that never claimed it", len(got))
	}
}

// The direct route is route 1 and carries an EMPTY RouteList: it is the
// distinguishing mark of "no intermediate hops". A relay route that lost its
// RouteList would be indistinguishable from a direct one at the probe layer.
func TestFindRoutesPrefersTheDirectSessionAndMarksItAsDirect(t *testing.T) {
	m := roleFixture(t, "auth", testNodeIDB)
	bind(m, testNodeIDB, wsSession())

	routes := m.FindRoutes(context.Background(), "auth", "")

	if len(routes) == 0 {
		t.Fatal("FindRoutes returned nothing with a live session to the target " +
			"— the direct path is the best route available and was skipped")
	}
	if routes[0].NodeID != testNodeIDB {
		t.Fatalf("route 0 is %q, want the target %q — the direct session must "+
			"come first", routes[0].NodeID, testNodeIDB)
	}
	if len(routes[0].RouteList) != 0 {
		t.Fatalf("the direct route carries RouteList %v, want empty — a probe "+
			"layer reading that hop list would relay a call it could make "+
			"directly", routes[0].RouteList)
	}
	if routes[0].TargetNodeID != testNodeIDB {
		t.Fatalf("TargetNodeID = %q, want %q", routes[0].TargetNodeID, testNodeIDB)
	}
}

// 🔴 A RELAY IS ONLY USABLE IF WE HAVE A LIVE SESSION TO IT.
//
// Latency records name peers that can reach the target — but a relay we
// cannot reach ourselves is not a route. Proposing one produces a probe that
// cannot even be attempted, and it consumes one of the three route slots that
// a usable relay could have taken.
func TestFindRoutesSkipsRelaysWeHaveNoSessionTo(t *testing.T) {
	const relay = "cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33"
	m := roleFixture(t, "auth", testNodeIDB)
	// The relay CAN reach the target — but we have no session to the relay,
	// and none to the target either, so there is no direct route to mask it.
	applyLatency(t, m, relay, testNodeIDB, 12)

	routes := m.FindRoutes(context.Background(), "auth", "")

	for _, r := range routes {
		if r.NodeID == relay {
			t.Fatalf("a relay we have NO session to was offered as a route "+
				"(%v) — the probe cannot be attempted and it displaces a "+
				"usable candidate", routes)
		}
	}
}

// With a session to the relay, it becomes a real route — and it is marked as
// relayed via RouteList, which is what tells the probe layer to forward.
func TestFindRoutesOffersAReachableRelayMarkedAsRelayed(t *testing.T) {
	const relay = "cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33"
	m := roleFixture(t, "auth", testNodeIDB)
	applyLatency(t, m, relay, testNodeIDB, 12)
	bind(m, relay, wsSession()) // now reachable

	routes := m.FindRoutes(context.Background(), "auth", "")

	var found *struct{ hops []string }
	for _, r := range routes {
		if r.NodeID == relay {
			found = &struct{ hops []string }{hops: r.RouteList}
			if r.TargetNodeID != testNodeIDB {
				t.Errorf("relay route TargetNodeID = %q, want the target %q — "+
					"the forwarding peer would not know where to send it",
					r.TargetNodeID, testNodeIDB)
			}
		}
	}
	if found == nil {
		t.Fatalf("a reachable relay with latency to the target was not offered "+
			"(%d routes) — the only path to the target is being discarded", len(routes))
	}
	if len(found.hops) == 0 || found.hops[0] != testNodeIDB {
		t.Fatalf("relay route RouteList = %v, want [%s] — without the hop list "+
			"the probe layer treats a relayed route as direct and never "+
			"forwards", found.hops, testNodeIDB)
	}
}

// Never more than 3 routes: the probe layer fans out over what it is given,
// so an unbounded list is an unbounded fan-out.
func TestFindRoutesIsCappedAtThree(t *testing.T) {
	m := roleFixture(t, "auth", testNodeIDB)
	bind(m, testNodeIDB, wsSession()) // direct route
	for _, relay := range []string{
		"cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33",
		"dd44dd44dd44dd44dd44dd44dd44dd44dd44dd44dd44dd44dd44dd44dd44dd44",
		"ee55ee55ee55ee55ee55ee55ee55ee55ee55ee55ee55ee55ee55ee55ee55ee55",
		"ff66ff66ff66ff66ff66ff66ff66ff66ff66ff66ff66ff66ff66ff66ff66ff66",
	} {
		applyLatency(t, m, relay, testNodeIDB, 20)
		bind(m, relay, wsSession())
	}

	routes := m.FindRoutes(context.Background(), "auth", "")
	if len(routes) < 2 {
		t.Fatalf("premise wrong: only %d routes built, so the cap is untested",
			len(routes))
	}
	if len(routes) > 3 {
		t.Fatalf("FindRoutes returned %d routes, want at most 3 — the probe "+
			"layer fans out over every route it is handed", len(routes))
	}
}
