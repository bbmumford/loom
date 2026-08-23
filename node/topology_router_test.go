/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"testing"
)

// COVERAGE of the topology router's untested surface:
// DefaultTopologyRouterConfig (:47), GetExistingSession (:120),
// HasDirectRoute (:130), recordDirectHit (:169), recordLookupError (:189) —
// all at 0.0%.
//
// Checked for duplication against the existing suite first, because without
// not: `RouteToNode`'s grade gate is ALREADY covered by
// TestRouteToNodeNeverReachesProtocolForGradeBelowGradeB
// (grade_routing_and_budget_test.go:81), and `Stats`/`recordFallback`/
// `recordGradeMiss`/`shortID` are already at 100%. This file adds only what is
// genuinely uncovered and reuses that file's `gradeReporter` rather than
// introducing a second fake.

// 🔴 THE FINDING THIS FILE EXISTS TO PIN: `TopologyRouterConfig.MinGrade` HAS
// NO CONSUMER, AND THE THRESHOLD IT CONFIGURES IS HARDCODED TWICE.
//
//	topology_router.go:43   MinGrade Grade            — the field
//	topology_router.go:49   MinGrade: GradeB          — the documented default
//	topology_router.go:84   if conn.Grade < GradeB    — RouteToNode, HARDCODED
//	topology_router.go:135  conn.Grade >= GradeB      — HasDirectRoute, HARDCODED
//
// MEASURED: `NewTopologyRouter(reporter, peerMgr, identity)` takes NO config,
// `TopologyRouter` holds no config field, and `DefaultTopologyRouterConfig` has
// zero non-test callers. So the knob cannot be set, and an operator changing it
// would change nothing — while the value it names is duplicated in two
// comparisons that nothing forces to agree.
//
// This test pins the AGREEMENT between the two hardcoded sites, which is the
// property that actually protects routing. Same shape as resources.go's
// tier/max-roles pair: two expressions of one policy, no shared
// constant.
func TestRouteToNodeAndHasDirectRouteUseTheSameThreshold(t *testing.T) {
	for _, g := range []Grade{GradeF, GradeC, GradeB, GradeA} {
		tr := NewTopologyRouter(gradeReporter{info: ConnectionInfo{Grade: g}}, nil, nil)

		hasRoute := tr.HasDirectRoute(testNodeIDB)
		// RouteToNode with a nil connMgr returns (nil, nil) for BOTH a grade
		// miss and a "grade passed but no dialer" — so the observable that
		// distinguishes them is the grade-miss counter.
		before := tr.Stats().GradeMisses
		_, err := tr.RouteToNode(context.Background(), testNodeIDB)
		if err != nil {
			t.Fatalf("grade %v: RouteToNode returned an error: %v — it is documented to fall back, not fail", g, err)
		}
		routeRejectedOnGrade := tr.Stats().GradeMisses > before

		// If HasDirectRoute says a direct route exists, RouteToNode must NOT
		// have rejected it on grade — and vice versa.
		if hasRoute == routeRejectedOnGrade {
			t.Errorf("grade %v: HasDirectRoute=%v but RouteToNode grade-rejected=%v — the two "+
				"hardcoded GradeB comparisons (:135 and :84) have diverged. A peer would be "+
				"advertised as directly routable and then refused on grade, or refused while "+
				"reported reachable", g, hasRoute, routeRejectedOnGrade)
		}
	}
}

// The nil-reporter guard must fail toward fallback, not panic — the router is
// constructed before the reporter is necessarily wired.
func TestANilReporterFailsToFallbackNotPanic(t *testing.T) {
	tr := NewTopologyRouter(nil, nil, nil)

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("HasDirectRoute/RouteToNode panicked with a nil reporter: %v", r)
		}
	}()

	if tr.HasDirectRoute(testNodeIDB) {
		t.Error("HasDirectRoute claimed a direct route with NO reporter — callers would skip " +
			"their fallback dispatch path for a route that cannot exist")
	}
	session, err := tr.RouteToNode(context.Background(), testNodeIDB)
	if err != nil || session != nil {
		t.Errorf("RouteToNode with a nil reporter = (%v, %v), want (nil, nil) — it is documented "+
			"to return nil so the caller falls back", session, err)
	}
	if tr.Stats().Fallbacks == 0 {
		t.Error("the nil-reporter path did not record a fallback — the counter is how an operator " +
			"sees that direct routing is disabled rather than merely unused")
	}
}

// 🔴 MESH-G05, PINNED AS CHARACTERISATION. The doc comment at :111-119 warns
// that despite its name, GetExistingSession DIALS A FRESH transport session on
// every call and the caller MUST Close it — "treating it like a cached session
// leaks one transport connection (and defeats the connection budget) per call".
//
// I verified the CALL CHAIN rather than trusting the comment: GetExistingSession
// → RouteToNode → dialDirect → connMgr.dialWithProtocol. There is no session-
// table lookup on that path.
//
// With a nil connMgr the dial cannot happen, so the observable here is that the
// method returns nil rather than a reused session — i.e. it has no cache to fall
// back on. That is the half of MESH-G05 a test can reach without a live dialer.
func TestGetExistingSessionHasNoSessionCacheToReturn(t *testing.T) {
	// Grade A passes the threshold, so the ONLY reason to return nil is that
	// there is no dialer — proving no cached-session path exists.
	tr := NewTopologyRouter(gradeReporter{info: ConnectionInfo{Grade: GradeA}}, nil, nil)

	if got := tr.GetExistingSession(testNodeIDB); got != nil {
		t.Fatalf("GetExistingSession returned %v with a nil ConnectionManager — a session "+
			"appeared from somewhere other than a dial, which would contradict MESH-G05's "+
			"documented dial-fresh behaviour", got)
	}
	if tr.Stats().Fallbacks == 0 {
		t.Error("no fallback recorded — GetExistingSession did not travel the documented " +
			"RouteToNode path")
	}
}

// A lookup miss (no active connection) is distinct from a grade miss, and the
// counters must not conflate them — they mean different things to an operator:
// "peer not connected" versus "connected but too slow to route over".
func TestALookupMissAndAGradeMissAreCountedSeparately(t *testing.T) {
	miss := NewTopologyRouter(absentReporter{}, nil, nil)
	if _, err := miss.RouteToNode(context.Background(), testNodeIDB); err != nil {
		t.Fatalf("RouteToNode on a lookup miss returned an error: %v", err)
	}
	ms := miss.Stats()
	if ms.LookupErrors == 0 {
		t.Error("an absent connection did not record a lookup error")
	}
	if ms.GradeMisses != 0 {
		t.Error("an absent connection recorded a GRADE miss — an unconnected peer is being " +
			"reported as one whose connection was too slow")
	}

	low := NewTopologyRouter(gradeReporter{info: ConnectionInfo{Grade: GradeC}}, nil, nil)
	if _, err := low.RouteToNode(context.Background(), testNodeIDB); err != nil {
		t.Fatalf("RouteToNode on a grade miss returned an error: %v", err)
	}
	ls := low.Stats()
	if ls.GradeMisses == 0 {
		t.Error("a below-threshold connection did not record a grade miss")
	}
	if ls.LookupErrors != 0 {
		t.Error("a below-threshold connection recorded a LOOKUP error — a connected peer is " +
			"being reported as absent")
	}
}

// The default config is a wire-visible contract even though nothing consumes it
// today: GradeB means "A and B only". If it ever gets a consumer, this pins what
// it promises.
func TestTheDefaultConfigNamesGradeBAndNothingConsumesIt(t *testing.T) {
	cfg := DefaultTopologyRouterConfig()

	if cfg.MinGrade != GradeB {
		t.Errorf("DefaultTopologyRouterConfig().MinGrade = %v, want GradeB — the doc says "+
			"\"only A and B connections are used\"", cfg.MinGrade)
	}
	// Not asserted mechanically, recorded deliberately: NewTopologyRouter takes
	// no config and TopologyRouter holds none, so this value reaches nothing.
	// If a future change wires it, TestRouteToNodeAndHasDirectRouteUseTheSameThreshold
	// is the test that must then read cfg.MinGrade instead of GradeB.
}

// 🔴 THE `ok` FLAG IS LOAD-BEARING AND MUTATION PROVED MY SUITE DID NOT ASSERT IT.
// `HasDirectRoute` returns `ok && conn.Grade >= GradeB`. A mutant that drops the
// `ok` term SURVIVED my first pass — because absentReporter returns a ZERO-VALUE
// ConnectionInfo whose Grade is GradeF, which fails the grade test anyway.
//
// The flag only matters when a reporter returns ok=false ALONGSIDE non-zero info
// — a stale-read shape. Ignoring `ok` there reports a direct route to a peer with
// NO active connection, and the caller skips its fallback for a route that
// cannot carry traffic. Fail-OPEN, which is the direction that costs traffic.
func TestAStaleReadWithNoActiveConnectionIsNotADirectRoute(t *testing.T) {
	tr := NewTopologyRouter(staleReporter{}, nil, nil)

	if tr.HasDirectRoute(testNodeIDB) {
		t.Fatal("HasDirectRoute returned true for a reporter answering ok=FALSE with a " +
			"Grade-A payload — the `ok` term is being ignored, so a peer with no active " +
			"connection is advertised as directly routable and the caller skips its fallback")
	}
}

// staleReporter answers ok=false but leaves a high-grade payload behind — the
// only shape that distinguishes `ok && grade` from `grade` alone.
type staleReporter struct{ NilConnectionReporter }

func (staleReporter) ConnectionTo(string) (ConnectionInfo, bool) {
	return ConnectionInfo{Grade: GradeA}, false
}

var _ ConnectionReporter = staleReporter{}

// absentReporter reports no connection for any peer — the lookup-miss path.
// Embeds NilConnectionReporter for the rest of the interface, matching the
// shape gradeReporter already uses rather than reimplementing five methods.
type absentReporter struct{ NilConnectionReporter }

func (absentReporter) ConnectionTo(string) (ConnectionInfo, bool) {
	return ConnectionInfo{}, false
}

var _ ConnectionReporter = absentReporter{}
