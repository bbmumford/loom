/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"testing"
	"time"

	lad "github.com/bbmumford/ledger"
)

// The evaluator grades a peer-supplied transport label off gossiped latency
// records. ParseProtocol discards its ok flag and returns ProtoNoiseUDP for any
// unrecognised string, which GradeForProtocol scores GradeA — so before the
// strict parse an unlabelled edge outranked every correctly-labelled one in the
// max-selection that picks BestTransport.
//
// This node publishes "unknown" itself when it cannot name a transport
// (runtime.go:4656), so the label that graded best carried no information.

func gradeEvaluator(t *testing.T, recs ...lad.LatencyRecord) *ServiceHealthReport {
	t.Helper()
	snap := &LADSnapshot{BuiltAt: time.Now(), Latency: recs}
	e := NewHealthEvaluatorWithSnapshot(
		NilConnectionReporter{},
		nil,
		func() map[string]string { return map[string]string{"node-a": "svc", "node-b": "svc"} },
		func() *LADSnapshot { return snap },
		HealthEvaluatorConfig{CacheTTL: time.Hour, EvalInterval: time.Hour},
	)
	// ServiceHealth is cache-only — it never evaluates on demand, and Start
	// would spawn the ticker goroutine this test has no reason to run. One
	// direct pass is what fills the cache.
	e.(*meshHealthEvaluator).evaluate()
	return e.ServiceHealth("svc")
}

func latRec(from, to, transport string) lad.LatencyRecord {
	return lad.LatencyRecord{
		FromNode: from, ToNode: to, Transport: transport,
		MeasuredAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour),
	}
}

// 🔬 ANTI-CORRELATED BY CONSTRUCTION: one edge carries a real, LOW-grade label
// and the other carries an unrecognised one. If the unrecognised label still
// graded A it would win the max-selection and the report would name a better
// transport than any edge actually provides. With both edges labelled the test
// could not tell a strict parse from a lossy one.
func TestAnUnrecognisedTransportLabelDoesNotOutrankARealOne(t *testing.T) {
	got := gradeEvaluator(t,
		latRec("node-a", "node-b", "websocket"), // a real GradeC edge
		latRec("node-a", "node-b", "unknown"),   // the label this node emits itself
	)
	if got == nil {
		t.Fatal("no report produced")
	}

	if got.BestTransport == "A" {
		t.Error("an unrecognised transport label graded A and won the selection — the " +
			"service reports a better transport than any of its edges provides, and the " +
			"label that wins is the one this node writes when it cannot name a transport")
	}
	if got.BestTransport != "C" {
		t.Errorf("BestTransport = %q, want C (the websocket edge, the only labelled one)",
			got.BestTransport)
	}
}

// 🔴 LIVENESS MUST SURVIVE THE STRICTER GRADE. A latency record existing is
// evidence of an edge whatever its transport is spelled; downgrading the grade
// must not also erase the connection. Failing closed on the GRADE while failing
// closed on LIVENESS too would report a reachable service as unreachable.
func TestAnUnrecognisedLabelStillCountsAsAnEdge(t *testing.T) {
	got := gradeEvaluator(t, latRec("node-a", "node-b", "unknown"))
	if got == nil {
		t.Fatal("no report produced")
	}

	if got.Connections == 0 && got.MeshStatus == "unreachable" {
		t.Error("a service whose only edge carries an unrecognised transport label reads " +
			"as unreachable — the strict grade also erased the liveness evidence, so a " +
			"reachable service is reported down")
	}
}

// A recognised label must still grade at its own class, or the strict parse has
// simply broken grading for everyone.
func TestRecognisedLabelsStillGradeAtTheirOwnClass(t *testing.T) {
	for _, tc := range []struct{ transport, want string }{
		{"noise-udp", "A"},
		{"quic", "B"},
		{"websocket", "C"},
		{"gossip-tls", "C"},
	} {
		t.Run(tc.transport, func(t *testing.T) {
			got := gradeEvaluator(t, latRec("node-a", "node-b", tc.transport))
			if got == nil {
				t.Fatal("no report")
			}
			if got.BestTransport != tc.want {
				t.Errorf("%q graded %q, want %q", tc.transport, got.BestTransport, tc.want)
			}
		})
	}
}
