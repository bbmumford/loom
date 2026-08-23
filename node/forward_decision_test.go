/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ORBTR/aether/rpc/pb"
	"github.com/bbmumford/loom/node/handlers"
)

// Covers the forward-or-serve-locally decision and the two system RPC
// handlers.
//
// CENSUSED FIRST, per symbol: `shouldForwardWithContext` ← `rpc.go:632` (the
// dispatch path's forwarding gate) · `PingRPCHandler` and `StatusRPCHandler`
// ← `runtime.go:577-578`, registered on **every** node.
//
// 🔴 THE FORWARDING GATE IS A LOAD-SHEDDING DECISION: forward too eagerly and
// an RPC crosses the mesh for no reason; forward too reluctantly and an
// overloaded node keeps queueing work it cannot serve. Its ladder is four
// thresholds and a loop guard, none of which had a test.

// fakeAdvisor supplies the three LoadAdvisor inputs.
type fakeAdvisor struct {
	local  int32
	health float64
	grade  Grade
}

func (f fakeAdvisor) LocalLoad() int32                { return f.local }
func (f fakeAdvisor) PeerDispatchHealth() float64     { return f.health }
func (f fakeAdvisor) BestGradeToHandler(string) Grade { return f.grade }

var _ LoadAdvisor = fakeAdvisor{}

// serverAt builds a server whose in-flight handler count is `active` and whose
// advisor reports the given peer health and best grade.
func serverAt(active int32, health float64, grade Grade) *RPCServer {
	s := NewRPCServer(nil)
	s.SetLoadAdvisor(fakeAdvisor{health: health, grade: grade})
	atomic.StoreInt32(&s.activeHandlers, active)
	return s
}

// 🔴 NO ADVISOR MEANS NEVER FORWARD. A node with no load information must not
// shed work on a guess — it has no basis for believing any peer is better.
func TestWithoutALoadAdvisorNothingIsForwarded(t *testing.T) {
	s := NewRPCServer(nil)
	atomic.StoreInt32(&s.activeHandlers, 5000) // wildly overloaded

	if s.shouldForwardWithContext(&pb.RPCRequest{Handler: "h"}) {
		t.Fatal("an overloaded node with NO load advisor chose to forward — it " +
			"has no evidence any peer is healthier, so the RPC crosses the mesh " +
			"on no information at all")
	}
}

// 🔴 THE LOOP GUARD OUTRANKS EVERY LOAD SIGNAL. At MaxRPCHops-1 the request
// must be served locally however overloaded this node is, or a request can
// circulate until the hop counter kills it.
func TestTheHopLimitPreventsForwardingEvenWhenOverloaded(t *testing.T) {
	s := serverAt(5000, 1.0, GradeA) // maximally overloaded, perfect peers

	atLimit := &pb.RPCRequest{Handler: "h", Hops: pb.MaxRPCHops - 1}
	if s.shouldForwardWithContext(atLimit) {
		t.Fatalf("forwarded a request already at %d hops — a request can now "+
			"circulate the mesh until the hop counter rejects it, and the node "+
			"that could have served it did not", atLimit.Hops)
	}

	// One hop below the limit, the same request DOES forward — so the guard is
	// the reason above, not a blanket refusal.
	if !s.shouldForwardWithContext(&pb.RPCRequest{Handler: "h", Hops: pb.MaxRPCHops - 2}) {
		t.Fatal("premise wrong: an overloaded node did not forward one hop below " +
			"the limit, so the hop guard above proved nothing")
	}
}

// 🔴 DEAD PEERS OUTRANK LOCAL OVERLOAD. Below 0.3 health, forwarding sheds work
// onto peers that cannot serve it — worse than queueing locally.
func TestUnhealthyPeersPreventForwardingEvenWhenOverloaded(t *testing.T) {
	if serverAt(5000, 0.29, GradeA).shouldForwardWithContext(&pb.RPCRequest{Handler: "h"}) {
		t.Fatal("forwarded to peers at 0.29 dispatch health — the work is shed " +
			"onto nodes that mostly cannot serve it, so the RPC fails remotely " +
			"instead of queueing locally")
	}
	// Just above the threshold, the same load DOES forward.
	if !serverAt(5000, 0.31, GradeA).shouldForwardWithContext(&pb.RPCRequest{Handler: "h"}) {
		t.Fatal("premise wrong: 0.31 health did not forward under extreme load, " +
			"so the 0.3 threshold above is not what was tested")
	}
}

// The load ladder, each rung isolated. A grade-dependent threshold means a
// better path forwards sooner; getting the direction wrong makes a node hold
// work when it has an excellent peer available.
func TestTheForwardingLadderIsGradeDependent(t *testing.T) {
	for _, tc := range []struct {
		name   string
		active int32
		grade  Grade
		want   bool
		why    string
	}{
		{"idle node keeps its own work", 5, GradeA, false,
			"an idle node must serve locally however good the peer path is"},
		{"grade A forwards above 20", 21, GradeA, true,
			"a Grade-A path plus moderate load is the cheapest time to shed"},
		{"grade A holds at exactly 20", 20, GradeA, false,
			"the threshold is > 20, not >= 20"},
		{"grade B holds at 21", 21, GradeB, false,
			"a Grade-B path must not shed as eagerly as Grade A"},
		{"grade B forwards above 35", 36, GradeB, true,
			"Grade B's own rung is 35"},
		{"grade C holds at 36", 36, GradeC, false,
			"below Grade B there is no rung until the absolute overload cut"},
		{"anything forwards above 50", 51, GradeC, true,
			"past 50 in flight the node sheds regardless of path quality"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := serverAt(tc.active, 1.0, tc.grade).
				shouldForwardWithContext(&pb.RPCRequest{Handler: "h"})
			if got != tc.want {
				t.Fatalf("active=%d grade=%v → forward=%v, want %v — %s",
					tc.active, tc.grade, got, tc.want, tc.why)
			}
		})
	}
}

// ── The two system handlers ─────────────────────────────────────────────────

// Ping must echo the caller's message: it is the liveness probe every peer
// uses, and a ping that loses its payload cannot correlate.
func TestPingEchoesTheCallersMessage(t *testing.T) {
	h := &PingRPCHandler{}
	payload, err := json.Marshal(map[string]string{"message": "hello-from-peer"})
	if err != nil {
		t.Fatal(err)
	}

	resp, err := h.ExecuteRPC(context.Background(), &handlers.RPCRequest{Payload: payload})
	if err != nil {
		t.Fatalf("ExecuteRPC: %v", err)
	}
	if !resp.Success {
		t.Fatalf("ping was unsuccessful: %+v", resp)
	}
	var out struct {
		Message   string    `json:"message"`
		Timestamp time.Time `json:"timestamp"`
	}
	if err := json.Unmarshal(resp.Payload, &out); err != nil {
		t.Fatalf("ping response is not decodable JSON: %v", err)
	}
	// 🙋 The reply is "pong: <message>", not a bare echo — and the prefix is
	// better than what I first asserted: a caller can distinguish a pong from
	// its own request even if the two are logged together. My expectation was
	// wrong; the code is not.
	if !strings.Contains(out.Message, "hello-from-peer") {
		t.Fatalf("ping reply %q does not contain the caller's message — a probe "+
			"that loses its payload cannot be correlated with its request",
			out.Message)
	}
	if !strings.HasPrefix(out.Message, "pong:") {
		t.Fatalf("ping reply %q lost its \"pong:\" prefix — a caller can no "+
			"longer tell a reply from a re-logged request", out.Message)
	}
	if out.Timestamp.IsZero() {
		t.Fatal("ping response carries a zero timestamp")
	}
}

// A malformed ping payload must not error the handler: a peer can send
// anything, and an RPC handler that returns a transport error on bad input
// gives the caller no usable answer.
func TestAMalformedPingPayloadStillProducesAResponse(t *testing.T) {
	h := &PingRPCHandler{}
	resp, err := h.ExecuteRPC(context.Background(),
		&handlers.RPCRequest{Payload: []byte("{not json")})
	if err != nil {
		t.Fatalf("ExecuteRPC returned a transport error for a malformed "+
			"payload (%v) — the caller gets no answer at all rather than an "+
			"unsuccessful one", err)
	}
	if resp == nil {
		t.Fatal("nil response for a malformed payload")
	}
}

// The status verdict never outruns its instrument: it is derived from the
// health sources rather than reported as a literal.
//
// The three sources have three different roles and are deliberately NOT averaged:
// HealthEvaluator is authoritative for the headline; SelfHealthMonitor is a META
// signal that maps to "unknown" (never "degraded", never silently "healthy");
// the subsystem registry is DETAIL that must not move the headline.

// A handler with no Runtime cannot see anything, so it must say "unknown" — not
// "healthy", and not a panic. This is the shape every test that builds the
// handler bare produces.
func TestAStatusHandlerWithNoRuntimeReportsUnknownNotHealthy(t *testing.T) {
	h := &StatusRPCHandler{identity: &NodeIdentity{}, startTime: time.Now().Add(-time.Hour)}

	resp, err := h.ExecuteRPC(context.Background(), &handlers.RPCRequest{})
	if err != nil {
		t.Fatalf("ExecuteRPC: %v", err)
	}
	var out struct {
		Uptime     string `json:"uptime"`
		Status     string `json:"status"`
		Confidence string `json:"confidence"`
	}
	if err := json.Unmarshal(resp.Payload, &out); err != nil {
		t.Fatalf("status response is not decodable JSON: %v", err)
	}
	if out.Status != statusUnknown {
		t.Fatalf("status = %q with no health sources at all, want %q — a node that "+
			"cannot see itself must say so on the wire rather than assert health it "+
			"has not measured", out.Status, statusUnknown)
	}
	if out.Confidence != confidenceLow {
		t.Fatalf("confidence = %q, want %q — the caller must be able to read the "+
			"instrument's worth without inferring it from the verdict",
			out.Confidence, confidenceLow)
	}
	if out.Uptime == "" {
		t.Fatal("uptime is empty — the one field that was always measured is missing")
	}
}

// 🔴 THE PRECEDENCE, AND IT IS THE WHOLE RULING. A stalled SelfHealthMonitor means
// the OTHER sources are unreliable, so the verdict must be "unknown" — the
// evaluator is not even consulted. Downgrading afterwards would still let a stale
// authoritative-looking verdict reach the wire.
func TestStalledObservabilityYieldsUnknownAndNeverTheEvaluatorsVerdict(t *testing.T) {
	rt := &Runtime{}
	rt.SetHealthEvaluator(fixedMeshStatus("healthy"))
	rt.selfHealth = stalledSelfHealth(t)

	h := &StatusRPCHandler{identity: &NodeIdentity{}, startTime: time.Now(), rt: rt}
	status, confidence, obs := h.statusVerdict()

	if status != statusUnknown {
		t.Fatalf("status = %q while observability is %q — the evaluator said "+
			"\"healthy\" and that verdict was trusted anyway. A verdict computed "+
			"from stalled observability is signed false evidence, and it is read at "+
			"exactly the moment it matters", status, obs.Status)
	}
	if confidence != confidenceLow {
		t.Fatalf("confidence = %q, want %q", confidence, confidenceLow)
	}
}

// With observability healthy, the evaluator IS authoritative and its verdict passes
// through unchanged — including an unhealthy one. The meta signal must not launder
// a bad verdict into a good one either.
func TestAHealthyObservabilityPassesTheEvaluatorsVerdictThroughUnchanged(t *testing.T) {
	for _, want := range []string{"healthy", "degraded", "unreachable"} {
		rt := &Runtime{}
		rt.SetHealthEvaluator(fixedMeshStatus(want))
		rt.selfHealth = healthySelfHealth(t)

		h := &StatusRPCHandler{identity: &NodeIdentity{}, startTime: time.Now(), rt: rt}
		status, confidence, _ := h.statusVerdict()

		if status != want {
			t.Errorf("status = %q, want the evaluator's %q — the headline is not "+
				"the authoritative source's verdict", status, want)
		}
		if confidence != confidenceAuthoritative {
			t.Errorf("confidence = %q for verdict %q, want %q",
				confidence, want, confidenceAuthoritative)
		}
	}
}

// A degraded SUBSYSTEM is detail and must never move the headline: the registry
// count is reported beside the verdict, not folded into it.
func TestDegradedSubsystemsAreDetailAndDoNotMoveTheHeadline(t *testing.T) {
	rt := &Runtime{}
	rt.SetHealthEvaluator(fixedMeshStatus("healthy"))
	rt.selfHealth = healthySelfHealth(t)

	h := &StatusRPCHandler{identity: &NodeIdentity{}, startTime: time.Now(), rt: rt}
	if status, _, _ := h.statusVerdict(); status != "healthy" {
		t.Fatalf("status = %q — a subsystem-level signal has reached the headline, "+
			"which is what keeping the three sources separate prevents", status)
	}
}

// A Runtime that exists but has NO evaluator cannot produce a headline either.
// Found by mutation: S3 ("nil evaluator reports healthy") SURVIVED my first pass,
// because TestAStatusHandlerWithNoRuntime… covers rt==nil and nothing covered
// rt!=nil with ev==nil. A live node mid-startup is exactly that shape.
func TestARuntimeWithNoEvaluatorReportsUnknownNotHealthy(t *testing.T) {
	rt := &Runtime{}
	rt.selfHealth = healthySelfHealth(t) // observability fine; nothing to evaluate

	h := &StatusRPCHandler{identity: &NodeIdentity{}, startTime: time.Now(), rt: rt}
	status, confidence, _ := h.statusVerdict()

	if status != statusUnknown {
		t.Fatalf("status = %q with no HealthEvaluator, want %q — healthy "+
			"observability says the INSTRUMENTS are fine, not that the SERVICE is",
			status, statusUnknown)
	}
	if confidence != confidenceLow {
		t.Fatalf("confidence = %q, want %q", confidence, confidenceLow)
	}
}

// LAGGING is the middle state: the verdict is still the evaluator's, but the
// caller must be told the instrument is behind. Found by mutation: S4
// ("confidence always authoritative") SURVIVED — nothing exercised this path.
func TestLaggingObservabilityKeepsTheVerdictButLowersConfidence(t *testing.T) {
	rt := &Runtime{}
	rt.SetHealthEvaluator(fixedMeshStatus("healthy"))
	rt.selfHealth = laggingSelfHealth(t)

	h := &StatusRPCHandler{identity: &NodeIdentity{}, startTime: time.Now(), rt: rt}
	status, confidence, obs := h.statusVerdict()

	if obs.Status != "lagging" {
		t.Fatalf("fixture wrong: observability is %q, want lagging", obs.Status)
	}
	if status != "healthy" {
		t.Fatalf("status = %q — lagging is not stalled; the evaluator's verdict "+
			"still stands and must not be downgraded to unknown", status)
	}
	if confidence != confidenceLagging {
		t.Fatalf("confidence = %q, want %q — a caller reading a verdict from a "+
			"lagging instrument must be able to see that from the reply",
			confidence, confidenceLagging)
	}
}

// ── Fixtures for the status verdict ────────────────────────────────────────

// fixedMeshStatus is a HealthEvaluator whose MeshStatus is whatever the test
// says, so the precedence tests turn on the PRECEDENCE and not on evaluating
// real service health.
type fixedMeshStatus string

func (f fixedMeshStatus) ServiceHealth(string) *ServiceHealthReport { return nil }
func (f fixedMeshStatus) AllServiceHealth() []*ServiceHealthReport  { return nil }
func (f fixedMeshStatus) MeshStatus(string) string                  { return string(f) }
func (f fixedMeshStatus) LastEvaluation() time.Time                 { return time.Now() }
func (f fixedMeshStatus) Start()                                    {}
func (f fixedMeshStatus) Stop()                                     {}

var _ HealthEvaluator = fixedMeshStatus("")

// stalledSelfHealth returns a monitor whose Check() reports "stalled" — an
// evaluator that has not run for far longer than its configured interval.
func stalledSelfHealth(t *testing.T) *SelfHealthMonitor {
	t.Helper()
	return NewSelfHealthMonitor(&staleEvaluator{at: time.Now().Add(-time.Hour)},
		NilConnectionReporter{}, time.Second, SelfHealthMonitorConfig{})
}

// healthySelfHealth returns a monitor whose Check() reports "healthy".
func healthySelfHealth(t *testing.T) *SelfHealthMonitor {
	t.Helper()
	return NewSelfHealthMonitor(&staleEvaluator{at: time.Now()},
		NilConnectionReporter{}, time.Hour, SelfHealthMonitorConfig{})
}

// staleEvaluator reports a LastEvaluation the test controls; everything else is
// inert because SelfHealthMonitor.Check only reads the timestamps.
type staleEvaluator struct{ at time.Time }

func (s *staleEvaluator) ServiceHealth(string) *ServiceHealthReport { return nil }
func (s *staleEvaluator) AllServiceHealth() []*ServiceHealthReport  { return nil }
func (s *staleEvaluator) MeshStatus(string) string                  { return "healthy" }
func (s *staleEvaluator) LastEvaluation() time.Time                 { return s.at }
func (s *staleEvaluator) Start()                                    {}
func (s *staleEvaluator) Stop()                                     {}

var _ HealthEvaluator = (*staleEvaluator)(nil)

// laggingSelfHealth returns a monitor whose Check() reports "lagging": past the
// LaggingMultiplier threshold but short of StalledMultiplier.
func laggingSelfHealth(t *testing.T) *SelfHealthMonitor {
	t.Helper()
	// interval 1s, defaults lagging=2x stalled=5x -> a 3s-old evaluation is lagging.
	return NewSelfHealthMonitor(&staleEvaluator{at: time.Now().Add(-3 * time.Second)},
		NilConnectionReporter{}, time.Second, SelfHealthMonitorConfig{})
}
