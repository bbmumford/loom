/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"strings"
	"testing"
	"time"

	lad "github.com/bbmumford/ledger"
)

// Covers the 4-layer HealthEvaluator, whose consumers are:
//
//	NewHealthEvaluator   1 production caller   io/endpoints/help.orbtr.io/routes.go:540
//	AllServiceHealth     -> node/metrics_export.go:177  (the /metrics alert value)
//	ServiceHealth        -> help.orbtr.io/monitoring_api.go:1364  (the monitoring API)
//	MeshStatus           -> HSTLES Library/domain/monitoring/service/checker.go:177,
//	                        injected at routes.go:556 — a CROSS-REPO consumer
//
// This file exercises the producer of the strings that alert mapping consumes.
// Covering the mapping without the producer asserts on both endpoints of an
// edge and never on the edge itself.
//
// The status ladder under test, from the code:
//
//	direct connection (any grade)   -> healthy   (Layer 4)
//	gossip + reach                  -> healthy   (Layers 2+3)
//	gossip only                     -> degraded  (Layer 2)
//	reach only                      -> connecting(Layer 3)
//	member only                     -> connecting(Layer 1)
//	nothing                         -> unreachable

const evalSvc = "auth.hstles.com"

// evaluatorFor builds an evaluator over a fixed LAD snapshot and connection
// list, then runs ONE evaluation synchronously — no goroutine, no ticker, so
// the assertions read a settled cache rather than racing the loop.
func evaluatorFor(t *testing.T, snap *LADSnapshot, http map[string]string, conns ...ConnectionInfo) *meshHealthEvaluator {
	t.Helper()
	e := NewHealthEvaluatorWithSnapshot(
		cannedReporter{conns: conns},
		func() map[string]string { return http },
		func() map[string]string { return map[string]string{testNodeIDB: evalSvc} },
		func() *LADSnapshot { return snap },
		HealthEvaluatorConfig{CacheTTL: time.Minute, EvalInterval: time.Hour},
	).(*meshHealthEvaluator)
	e.evaluate()
	return e
}

func snapWith(t *testing.T) *LADSnapshot {
	t.Helper()
	return &LADSnapshot{BuiltAt: time.Now(), GossipLiveness: map[string]time.Time{}}
}

func reportFor(t *testing.T, e *meshHealthEvaluator) *ServiceHealthReport {
	t.Helper()
	h := e.ServiceHealth(evalSvc)
	if h == nil {
		t.Fatalf("no report for %q — the service was not discovered by ANY of "+
			"the four layers, so every status assertion below would be vacuous",
			evalSvc)
	}
	return h
}

// ── The status ladder ───────────────────────────────────────────────────────

// Layer 4 wins outright: a direct connection is healthy at ANY grade, and the
// comment is explicit that Grade C must not read as degraded because
// cross-org nodes can only ever use Grade C.
func TestDirectConnectionIsHealthyEvenAtTheLowestUsableGrade(t *testing.T) {
	e := evaluatorFor(t, snapWith(t), nil,
		ConnectionInfo{PeerNodeID: testNodeIDB, Transport: "websocket", Grade: GradeC, ConnCount: 1})

	h := reportFor(t, e)
	if h.MeshStatus != "healthy" {
		t.Fatalf("MeshStatus = %q for a live Grade-C connection, want healthy — "+
			"marking a working WebSocket peer degraded pages for every "+
			"cross-org node in the mesh", h.MeshStatus)
	}
	if h.BestTransport != "C" {
		t.Fatalf("BestTransport = %q, want C", h.BestTransport)
	}
	if h.Connections != 1 {
		t.Fatalf("Connections = %d, want 1", h.Connections)
	}
}

// Layers 2+3 without a direct connection: still healthy, and the transport
// label must say "gossip" so an operator can tell the two apart.
func TestGossipPlusReachIsHealthyAndLabelledAsGossip(t *testing.T) {
	snap := snapWith(t)
	snap.GossipLiveness[testNodeIDB] = time.Now()
	snap.Reach = []lad.ReachRecord{{NodeID: testNodeIDB}}

	h := reportFor(t, evaluatorFor(t, snap, nil))
	if h.MeshStatus != "healthy" {
		t.Fatalf("MeshStatus = %q for a gossip-alive service publishing reach "+
			"records, want healthy — it is reachable through the mesh with no "+
			"direct connection from THIS node", h.MeshStatus)
	}
	if h.BestTransport != "gossip" {
		t.Fatalf("BestTransport = %q, want gossip — an operator cannot "+
			"otherwise distinguish mesh-reachable from directly connected",
			h.BestTransport)
	}
}

// Gossip alive but nothing published: the node is up and not advertising.
func TestGossipWithoutReachIsDegraded(t *testing.T) {
	snap := snapWith(t)
	snap.GossipLiveness[testNodeIDB] = time.Now()

	h := reportFor(t, evaluatorFor(t, snap, nil))
	if h.MeshStatus != "degraded" {
		t.Fatalf("MeshStatus = %q for a gossip-alive service with no reach "+
			"records, want degraded — it is alive but nothing can dial it",
			h.MeshStatus)
	}
}

// 🔑 THE STALE-GOSSIP BOUNDARY. Liveness is a 5-minute window; an older
// timestamp must NOT count as alive, or a dead node stays "degraded" forever
// instead of decaying to unreachable.
func TestGossipOlderThanFiveMinutesDoesNotCountAsAlive(t *testing.T) {
	snap := snapWith(t)
	snap.GossipLiveness[testNodeIDB] = time.Now().Add(-6 * time.Minute)

	e := evaluatorFor(t, snap, nil)
	if h := e.ServiceHealth(evalSvc); h != nil {
		t.Fatalf("a service whose last gossip was 6 minutes ago was still "+
			"discovered (status %q) — stale liveness never decays and a dead "+
			"node reads as alive indefinitely", h.MeshStatus)
	}
}

// Reach records without gossip: addresses are published but nobody has heard
// from it — connecting, not healthy.
func TestReachWithoutGossipIsConnecting(t *testing.T) {
	snap := snapWith(t)
	snap.Reach = []lad.ReachRecord{{NodeID: testNodeIDB}}
	// Reach alone does not add the service to allServices, so it needs a
	// membership record to be discovered at all — that is Layer 1's job.
	snap.Members = []lad.MemberRecord{{NodeID: testNodeIDB, Attrs: map[string]string{"serviceName": evalSvc}}}

	h := reportFor(t, evaluatorFor(t, snap, nil))
	if h.MeshStatus != "connecting" {
		t.Fatalf("MeshStatus = %q for published addresses with no gossip, want "+
			"connecting — nothing has confirmed the service is actually up",
			h.MeshStatus)
	}
	if h.BestTransport != "reach" {
		t.Fatalf("BestTransport = %q, want reach", h.BestTransport)
	}
}

// Registered and nothing else. This is what a node looks like immediately
// after joining, and it must not read as unreachable.
func TestMembershipAloneIsConnecting(t *testing.T) {
	snap := snapWith(t)
	snap.Members = []lad.MemberRecord{{NodeID: testNodeIDB, Attrs: map[string]string{"serviceName": evalSvc}}}

	h := reportFor(t, evaluatorFor(t, snap, nil))
	if h.MeshStatus != "connecting" {
		t.Fatalf("MeshStatus = %q for a registered-but-quiet service, want "+
			"connecting — a node that just joined would otherwise page",
			h.MeshStatus)
	}
	if h.BestTransport != "member" {
		t.Fatalf("BestTransport = %q, want member", h.BestTransport)
	}
}

// The serviceName attribute wins over the nodeToSvc resolver, and a member
// with neither is skipped rather than filed under "".
func TestMemberWithNoResolvableServiceNameIsSkipped(t *testing.T) {
	snap := snapWith(t)
	snap.Members = []lad.MemberRecord{{NodeID: "unknown-node-id"}}

	e := evaluatorFor(t, snap, nil)
	if got := len(e.AllServiceHealth()); got != 0 {
		t.Fatalf("%d reports for a member with no resolvable service name — an "+
			"empty-string service would appear in /metrics as a label with no "+
			"value", got)
	}
}

// ── The combined mesh+HTTP matrix ───────────────────────────────────────────

// A mesh-connected service whose HTTP check is failing is degraded, not
// healthy: the process is reachable and not serving.
func TestMeshHealthyButHTTPFailingIsDegraded(t *testing.T) {
	e := evaluatorFor(t, snapWith(t), map[string]string{evalSvc: "major_outage"},
		ConnectionInfo{PeerNodeID: testNodeIDB, Transport: "websocket", Grade: GradeA, ConnCount: 1})

	h := reportFor(t, e)
	if h.CombinedStatus != "degraded" {
		t.Fatalf("CombinedStatus = %q for mesh-connected + HTTP major_outage, "+
			"want degraded — this is the exact state where the mesh looks fine "+
			"and the service is down", h.CombinedStatus)
	}
	if !strings.Contains(h.Detail, "down") {
		t.Fatalf("Detail = %q, want the human label for major_outage — the "+
			"detail string is what an operator reads first", h.Detail)
	}
}

// HTTP up, mesh gone: degraded. The service is serving traffic but is
// invisible to mesh routing, so RPCs to it will fail.
func TestHTTPUpButMeshUnreachableIsDegraded(t *testing.T) {
	e := evaluatorFor(t, snapWith(t), map[string]string{evalSvc: "operational"})

	h := reportFor(t, e)
	if h.MeshStatus != "unreachable" || h.CombinedStatus != "degraded" {
		t.Fatalf("mesh=%q combined=%q for HTTP-operational + no mesh presence, "+
			"want unreachable/degraded", h.MeshStatus, h.CombinedStatus)
	}
}

// Both gone is the only genuine unreachable.
func TestBothMeshAndHTTPDownIsUnreachable(t *testing.T) {
	e := evaluatorFor(t, snapWith(t), map[string]string{evalSvc: "major_outage"})

	h := reportFor(t, e)
	if h.CombinedStatus != "unreachable" {
		t.Fatalf("CombinedStatus = %q with no mesh presence and HTTP down, want "+
			"unreachable", h.CombinedStatus)
	}
}

// 🔴 SEVENTH ABSENT-vs-ZERO INSTANCE, AND IT REACHES THE OPERATOR AS A
// SENTENCE, NOT JUST A NUMBER.
//
//	httpOK := httpSt == "operational" || httpSt == ""
//
// An EMPTY status means no HTTP check has run for this service. It is folded
// into "operational", so a service with no mesh presence and NO HTTP DATA
// reports CombinedStatus "degraded" with the detail "HTTP responsive but mesh
// unreachable" — asserting responsiveness nothing measured.
//
// The switch's own `default` arm carries the correct message for this case,
// "No mesh or HTTP data", and CANNOT be reached: the four meshSt values times
// the httpOK boolean are exhausted by the cases above it. The right answer is
// present in the file and unreachable because absence was converted to
// "operational" three statements earlier.
//
// PINNED AS CURRENT BEHAVIOUR, not endorsed — changing it moves an alert
// value on a live surface, so it is reported to @R/@P rather than altered
// here. If the fold is removed, this test must change deliberately.
func TestAbsentHTTPDataIsReportedAsResponsive(t *testing.T) {
	// Discovered via Layer 1 only, with no HTTP entry at all.
	snap := snapWith(t)
	snap.Members = []lad.MemberRecord{{NodeID: testNodeIDB, Attrs: map[string]string{"serviceName": evalSvc}}}

	h := reportFor(t, evaluatorFor(t, snap, nil))
	if strings.Contains(h.Detail, "HTTP") && !strings.Contains(h.Detail, "Mesh connecting") {
		t.Fatalf("premise drifted: Detail = %q", h.Detail)
	}
	if h.HTTPStatus != "" {
		t.Fatalf("HTTPStatus = %q, want empty — the fixture supplies no HTTP "+
			"data and this test is about exactly that state", h.HTTPStatus)
	}
}

// The companion: no mesh AND no HTTP data reports "HTTP responsive", which is
// the sentence the absent case should never produce.
func TestNoMeshAndNoHTTPDataClaimsHTTPIsResponsive(t *testing.T) {
	// Discovered only because an HTTP entry exists with an EMPTY value —
	// which is how a configured-but-never-checked endpoint appears.
	e := evaluatorFor(t, snapWith(t), map[string]string{evalSvc: ""})

	h := reportFor(t, e)
	if h.CombinedStatus != "degraded" {
		t.Fatalf("CombinedStatus = %q, want degraded (current behaviour)",
			h.CombinedStatus)
	}
	if !strings.Contains(h.Detail, "HTTP responsive") {
		t.Fatalf("Detail = %q — this test pins the CURRENT wording so that "+
			"removing the absent-is-operational fold shows up as a deliberate "+
			"change and not a silent one", h.Detail)
	}
}

// ── Cache, accessors, lifecycle ─────────────────────────────────────────────

// ServiceHealth is TTL-bounded: a report older than CacheTTL is withheld
// rather than served stale. MeshStatus must then fall back to "unreachable" —
// the conservative answer, since a stale report is not evidence.
func TestExpiredReportsAreWithheldAndMeshStatusFallsBackToUnreachable(t *testing.T) {
	e := NewHealthEvaluatorWithSnapshot(
		cannedReporter{}, nil,
		func() map[string]string { return map[string]string{testNodeIDB: evalSvc} },
		func() *LADSnapshot { return snapWith(t) },
		HealthEvaluatorConfig{CacheTTL: time.Nanosecond, EvalInterval: time.Hour},
	).(*meshHealthEvaluator)

	// Seed a report by hand so the test controls its age exactly.
	e.cache[evalSvc] = &ServiceHealthReport{
		ServiceName: evalSvc, MeshStatus: "healthy", CombinedStatus: "healthy",
		EvaluatedAt: time.Now().Add(-time.Hour),
	}

	if h := e.ServiceHealth(evalSvc); h != nil {
		t.Fatalf("an hour-old report was served under a 1ns TTL (%q) — stale "+
			"health is worse than none: it reports healthy long after the "+
			"service died", h.CombinedStatus)
	}
	if got := e.MeshStatus(evalSvc); got != "unreachable" {
		t.Fatalf("MeshStatus = %q for an expired report, want unreachable — "+
			"this is the value the cross-repo monitoring checker reads", got)
	}
}

// MeshStatus for a service that was never evaluated is "unreachable", not "".
// The cross-repo consumer compares this string; an empty one matches nothing.
func TestMeshStatusForAnUnknownServiceIsUnreachable(t *testing.T) {
	e := evaluatorFor(t, snapWith(t), nil)
	if got := e.MeshStatus("never-heard-of-it"); got != "unreachable" {
		t.Fatalf("MeshStatus = %q for an unknown service, want unreachable", got)
	}
}

// LastEvaluation advances on evaluate() and is what SelfHealthMonitor reads to
// decide whether observability itself is stalled.
func TestLastEvaluationAdvancesAndStartStopRunAtLeastOnce(t *testing.T) {
	e := NewHealthEvaluatorWithSnapshot(
		cannedReporter{}, nil,
		func() map[string]string { return nil },
		func() *LADSnapshot { return snapWith(t) },
		HealthEvaluatorConfig{CacheTTL: time.Minute, EvalInterval: time.Hour},
	)
	if !e.LastEvaluation().IsZero() {
		t.Fatal("premise wrong: LastEvaluation is set before the first run")
	}

	before := time.Now()
	e.Start() // runs one evaluation immediately, then ticks hourly
	defer e.Stop()

	deadline := time.Now().Add(2 * time.Second)
	for e.LastEvaluation().IsZero() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if e.LastEvaluation().IsZero() {
		t.Fatal("Start() did not evaluate once immediately — with a 30s default " +
			"interval the node would report no health at all for its first " +
			"30 seconds, and SelfHealthMonitor reads exactly this timestamp")
	}
	if e.LastEvaluation().Before(before) {
		t.Fatal("LastEvaluation predates Start()")
	}
}

// The defaults are load-bearing: they are what the one production caller uses.
func TestDefaultConfigAndZeroValuesAreFilledIn(t *testing.T) {
	cfg := DefaultHealthEvaluatorConfig()
	if cfg.CacheTTL != 30*time.Second || cfg.EvalInterval != 30*time.Second {
		t.Fatalf("defaults changed: %+v", cfg)
	}

	// A zero config must be repaired, not honoured — a 0 TTL would expire
	// every report instantly and a 0 interval would spin the ticker.
	e := NewHealthEvaluatorWithSnapshot(nil, nil, func() map[string]string { return nil },
		nil, HealthEvaluatorConfig{}).(*meshHealthEvaluator)
	if e.cacheTTL == 0 || e.evalTicker == 0 {
		t.Fatalf("zero config was not repaired: ttl=%v interval=%v",
			e.cacheTTL, e.evalTicker)
	}
	if e.reporter == nil {
		t.Fatal("a nil reporter was not replaced with NilConnectionReporter — " +
			"evaluate() calls ActiveConnections() on it and would panic")
	}
	if e.snapshot == nil {
		t.Fatal("a nil snapshot source was not replaced — evaluate() calls it")
	}
}

// 🔴 THE ONE UNDEFENDED PARAMETER, AND IT IS THE ONE THAT PANICS.
//
// Both constructors substitute a default for a nil reporter, a nil snapshot
// source, a zero CacheTTL and a zero EvalInterval; evaluate() guards a nil
// httpSource at its call site. nodeToSvc is the single input with no defence,
// and evaluate() calls it unconditionally on its first line:
//
//	health_evaluator.go:263   nodeToSvc := e.nodeToSvc()
//
// A nil func value there is a SIGSEGV — measured, not reasoned: the probe
// panicked at exactly that line. And it panics inside the goroutine Start()
// launches, where an unrecovered panic takes the whole endpoint process down,
// not just the evaluation. The one production caller passes a real function
// today, so this is latent; it is also the kind of latency that ends as a
// crash loop on a deploy nobody connects to this file.
func TestNilNodeToServiceResolverDoesNotPanic(t *testing.T) {
	for _, tc := range []struct {
		name string
		make func() HealthEvaluator
	}{
		{"WithSnapshot", func() HealthEvaluator {
			return NewHealthEvaluatorWithSnapshot(nil, nil, nil, nil, HealthEvaluatorConfig{})
		}},
		{"plain", func() HealthEvaluator {
			return NewHealthEvaluator(nil, nil, nil, nil, HealthEvaluatorConfig{})
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := tc.make().(*meshHealthEvaluator)
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("evaluate() panicked with a nil nodeToSvc: %v — in "+
						"production this runs in the goroutine Start() spawns, so "+
						"the panic is unrecovered and the endpoint dies", r)
				}
			}()
			e.evaluate()
			if got := len(e.AllServiceHealth()); got != 0 {
				t.Fatalf("%d reports from an evaluator with no inputs at all — "+
					"services were invented from nothing", got)
			}
		})
	}
}

// NewHealthEvaluator with a nil directory must still produce a working
// evaluator: it installs an empty-snapshot source rather than leaving the
// field nil. This is the constructor the one production caller uses.
func TestPlainConstructorWithoutADirectoryStillEvaluates(t *testing.T) {
	e := NewHealthEvaluator(
		cannedReporter{conns: []ConnectionInfo{{
			PeerNodeID: testNodeIDB, Transport: "websocket", Grade: GradeB, ConnCount: 2,
		}}},
		func() map[string]string { return map[string]string{evalSvc: "operational"} },
		func() map[string]string { return map[string]string{testNodeIDB: evalSvc} },
		nil, // no LAD directory
		HealthEvaluatorConfig{CacheTTL: time.Minute, EvalInterval: time.Hour},
	).(*meshHealthEvaluator)
	e.evaluate()

	h := e.ServiceHealth(evalSvc)
	if h == nil {
		t.Fatal("no report with a live direct connection and an HTTP status — " +
			"the nil-directory path left the evaluator unable to see Layer 4, " +
			"which does not come from LAD at all")
	}
	if h.CombinedStatus != "healthy" {
		t.Fatalf("CombinedStatus = %q for a Grade-B connection plus operational "+
			"HTTP, want healthy", h.CombinedStatus)
	}
}

// Layer 4 also reads mesh-wide LAD latency records, so a service NOBODY here
// is connected to can still be graded from another node's edge. Without this
// the mesh-wide half of Layer 4 is dead and every remote service that this
// node happens not to dial reads as gossip-only.
func TestLatencyRecordsGradeAServiceWeHaveNoConnectionTo(t *testing.T) {
	snap := snapWith(t)
	snap.Latency = []lad.LatencyRecord{{
		FromNode: "some-other-node", ToNode: testNodeIDB, Transport: "websocket",
	}}

	h := reportFor(t, evaluatorFor(t, snap, nil)) // no local connections at all
	if h.MeshStatus != "healthy" {
		t.Fatalf("MeshStatus = %q for a service graded from another node's "+
			"latency edge, want healthy — the mesh-wide half of Layer 4 is "+
			"not contributing", h.MeshStatus)
	}
	if h.Connections != 0 {
		t.Fatalf("Connections = %d, want 0 — the edge belongs to another node "+
			"and must not be counted as ours", h.Connections)
	}
}

// A connection to a peer we cannot resolve to a service is skipped, not filed
// under the empty service name.
func TestConnectionToAnUnresolvablePeerIsIgnored(t *testing.T) {
	e := evaluatorFor(t, snapWith(t), nil,
		ConnectionInfo{PeerNodeID: "peer-with-no-service", Grade: GradeA, ConnCount: 1})

	if got := len(e.AllServiceHealth()); got != 0 {
		t.Fatalf("%d reports from a connection whose peer maps to no service — "+
			"an empty service label would appear in /metrics", got)
	}
}

// A mesh-degraded service with a failing HTTP check names BOTH faults; an
// operator reading one detail line must not have to guess at the other.
func TestMeshDegradedAndHTTPFailingNamesBothFaults(t *testing.T) {
	snap := snapWith(t)
	snap.GossipLiveness[testNodeIDB] = time.Now() // gossip only -> degraded

	h := reportFor(t, evaluatorFor(t, snap, map[string]string{evalSvc: "partial_outage"}))
	if h.CombinedStatus != "degraded" {
		t.Fatalf("CombinedStatus = %q, want degraded", h.CombinedStatus)
	}
	if !strings.Contains(h.Detail, "Mesh degraded") || !strings.Contains(h.Detail, "intermittent") {
		t.Fatalf("Detail = %q, want both the mesh fault and the HTTP label — "+
			"reporting one hides the other", h.Detail)
	}
}

// A connecting service appends its HTTP fault only when there IS one. The
// `httpSt != ""` guard here is the one place in evaluate() that treats an
// empty status as ABSENT rather than as operational — see
// TestTheCorrectWordsForTheAbsentCaseExistAndAreUnreachable.
func TestConnectingServiceAppendsAnHTTPFaultWhenPresent(t *testing.T) {
	snap := snapWith(t)
	snap.Members = []lad.MemberRecord{{NodeID: testNodeIDB, Attrs: map[string]string{"serviceName": evalSvc}}}

	h := reportFor(t, evaluatorFor(t, snap, map[string]string{evalSvc: "major_outage"}))
	if h.CombinedStatus != "connecting" {
		t.Fatalf("CombinedStatus = %q, want connecting", h.CombinedStatus)
	}
	if !strings.Contains(h.Detail, "down") {
		t.Fatalf("Detail = %q, want the major_outage label appended", h.Detail)
	}
}

// MeshStatus returns the cached mesh verdict for a live report — the positive
// half of the cross-repo contract whose fallback is pinned above.
func TestMeshStatusReturnsTheCachedVerdict(t *testing.T) {
	e := evaluatorFor(t, snapWith(t), nil,
		ConnectionInfo{PeerNodeID: testNodeIDB, Transport: "websocket", Grade: GradeA, ConnCount: 1})

	if got := e.MeshStatus(evalSvc); got != "healthy" {
		t.Fatalf("MeshStatus = %q for a directly connected service, want "+
			"healthy — this is the string HSTLES' monitoring checker branches "+
			"on", got)
	}
}

// The cross-repo boundary contract.
//
// MeshStatus is consumed OUTSIDE this repository —
// HSTLES Library/domain/monitoring/service/checker.go:177 branches on the
// exact literals "healthy" / "degraded" / "connecting" / "unreachable", and
// maps "unreachable" to partial_outage, which opens an auto-incident. So the
// string set is a published contract, not an internal label.
//
// This test pins the property that keeps the HTTP-fold safe for that consumer:
// MeshStatus is computed from Layers 1-4 ONLY and is independent of the HTTP
// status, so changing how absent HTTP data is folded cannot move it.
// If that independence ever breaks, the fold becomes a cross-repo change and
// must be routed rather than made.
func TestMeshStatusIsIndependentOfTheHTTPStatus(t *testing.T) {
	newSnap := func() *LADSnapshot {
		s := snapWith(t)
		s.GossipLiveness[testNodeIDB] = time.Now() // gossip only -> degraded
		return s
	}

	var seen []string
	for _, httpSt := range []string{"", "operational", "major_outage", "partial_outage"} {
		h := reportFor(t, evaluatorFor(t, newSnap(), map[string]string{evalSvc: httpSt}))
		seen = append(seen, h.MeshStatus)
	}
	for i, got := range seen {
		if got != "degraded" {
			t.Fatalf("MeshStatus = %q at HTTP variant %d, want degraded for all "+
				"of them — the mesh verdict has become HTTP-dependent, which "+
				"makes every change to the HTTP fold a cross-repo change to "+
				"HSTLES' incident thresholds", got, i)
		}
	}
}

// 🔴 THE FINDING, MADE INTO AN ASSERTION RATHER THAN A CLAIM.
//
// httpStatusLabel("") returns "not monitored" — the exactly right words for a
// service with no HTTP check. NOTHING IN evaluate() CAN EVER PRODUCE IT.
// Every call site sits behind `!httpOK`, and
//
//	httpOK := httpSt == "operational" || httpSt == ""
//
// folds the empty status into OK, so the label never sees "". The switch's
// `default` arm carries the same idea in different words — "No mesh or HTTP
// data" — and is dead for the same reason, since the four meshSt values times
// the httpOK boolean are exhausted by the cases above it.
//
// So the file contains TWO independent, correct renderings of the absent case
// and can emit NEITHER. That is the eighth instance of the absent-vs-zero
// class this session and the most instructive: nobody overlooked the case —
// two authors handled it, and one fold three statements earlier made both
// answers unreachable.
//
// This test pins the labels themselves (they are correct and worth keeping)
// and records the reachability fact next to them, so whoever removes the fold
// finds the words already written.
func TestTheCorrectWordsForTheAbsentCaseExistAndAreUnreachable(t *testing.T) {
	for status, want := range map[string]string{
		"operational":    "responsive",
		"degraded":       "slow",
		"partial_outage": "intermittent",
		"major_outage":   "down",
		"":               "not monitored", // correct, and unreachable from evaluate()
	} {
		if got := httpStatusLabel(status); got != want {
			t.Errorf("httpStatusLabel(%q) = %q, want %q", status, got, want)
		}
	}
	// An unrecognised status must not render as responsive.
	if got := httpStatusLabel("something-new"); got == "responsive" {
		t.Errorf("an unknown HTTP status rendered as %q — an unrecognised "+
			"value must never read as healthy in an operator-facing string", got)
	}

	// The reachability half, asserted rather than asserted-about: drive the
	// state that SHOULD say "not monitored" and confirm it does not.
	h := reportFor(t, evaluatorFor(t, snapWith(t), map[string]string{evalSvc: ""}))
	if strings.Contains(h.Detail, "not monitored") {
		t.Fatalf("Detail = %q — the fold has been removed and the absent case "+
			"now reaches its own label. That is an improvement; update this "+
			"test rather than leaving a stale claim that the words are "+
			"unreachable", h.Detail)
	}
}
