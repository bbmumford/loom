/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import "testing"

// COVERAGE of the ConnectionReporter's SELF-IDENTITY pair and the
// NilConnectionReporter fallback — the five methods still at 0.0%:
//
//	peerConnectionReporter.SelfRegion  (:212)   NilConnectionReporter.ConnectionTo (:224)
//	peerConnectionReporter.SelfNodeID  (:216)   NilConnectionReporter.SelfRegion   (:226)
//	                                            NilConnectionReporter.SelfNodeID   (:227)
//
// 🔴 CENSUS MODE 3, AND THIS TIME IT CHANGED THE ANSWER. These are
// interface methods, so no caller names them — and `ConnectionReporter` is
// EXPORTED, so the loom repo is NOT the population. Enumerating the roots first
// censused before publishing rather than after:
//
//	loom repo         .SelfNodeID() / .SelfRegion() call sites:  0
//	HSTLES root                                                  0
//	ORBTR root                                                   4  <- LIVE
//	  io/endpoints/help.orbtr.io/monitoring_api.go:1139 :1141 :1209 :1392
//	POSITIVE CONTROL: the same sweep finds .ActiveConnections() 16/5/13 across the roots.
//
// ⇒ A loom-only grep would have filed all five as DEAD. They are the mesh
// topology API's view of "which node am I".

// ── The real reporter ───────────────────────────────────────────────────────

// SelfRegion and SelfNodeID must read through to the manager, not a snapshot:
// the topology API calls them on every request, and a stale self-identity puts
// this node's own row in the wrong region or under the wrong ID.
func TestTheReporterReadsSelfIdentityThroughToTheManager(t *testing.T) {
	m := registerTestManager()
	m.selfID = testNodeIDA
	m.selfRegion = "syd"

	r := NewConnectionReporter(m)
	if got := r.SelfNodeID(); got != testNodeIDA {
		t.Fatalf("SelfNodeID() = %q, want %q", got, testNodeIDA)
	}
	if got := r.SelfRegion(); got != "syd" {
		t.Fatalf("SelfRegion() = %q, want %q", got, "syd")
	}

	// A later change must be visible: the reporter is a live view, and the
	// region in particular is set after construction on some paths.
	m.selfRegion = "iad"
	m.selfID = testNodeIDB
	if got := r.SelfRegion(); got != "iad" {
		t.Fatalf("SelfRegion() = %q after the manager moved region — the reporter "+
			"cached it, so the topology API reports this node in a region it left", got)
	}
	if got := r.SelfNodeID(); got != testNodeIDB {
		t.Fatalf("SelfNodeID() = %q after the manager's ID changed — a cached "+
			"self ID files this node's own row under a stale key", got)
	}
}

// ── The Nil fallback, and why its exact values are load-bearing ─────────────

// 🔴 THE MOST IMPORTANT TEST IN THIS FILE, AND THE REASON IS IN ANOTHER REPO.
//
// NilConnectionReporter is substituted whenever a ConnectionManager is absent —
// runtime.go:1654, health_evaluator.go:127 and :179, metrics_export.go:52. Its
// returns are not arbitrary placeholders: a consumer in a DIFFERENT REPO guards
// on them by value.
//
//	help.orbtr.io/monitoring_api.go:1139  if _, ok := machineMap[selfID]; !ok && selfID != ""
//	help.orbtr.io/monitoring_api.go:1141  if selfOwnRegion == "" { ...Platform.Region() }
//
// Both guards test for THE EMPTY STRING. If SelfNodeID() ever returned
// "unknown" or "nil-reporter" instead, the `selfID != ""` guard would pass and
// every reporter-less node would grow a PHANTOM MACHINE in the mesh topology
// view — and every test in loom would still be green, because the coupling is
// invisible from inside this repo.
//
// So: this test pins the sentinel values themselves, not merely "some zero value".
func TestTheNilReporterReturnsTheExactSentinelsItsConsumersGuardOn(t *testing.T) {
	var r ConnectionReporter = NilConnectionReporter{}

	if got := r.SelfNodeID(); got != "" {
		t.Fatalf("NilConnectionReporter.SelfNodeID() = %q, want \"\" — "+
			"help.orbtr.io/monitoring_api.go:1139 guards with `selfID != \"\"`, so a "+
			"non-empty sentinel inserts a phantom machine into the topology view of "+
			"every node that has no ConnectionManager", got)
	}
	if got := r.SelfRegion(); got != "" {
		t.Fatalf("NilConnectionReporter.SelfRegion() = %q, want \"\" — "+
			"help.orbtr.io/monitoring_api.go:1141 uses `selfOwnRegion == \"\"` to fall "+
			"back to the platform region, and a non-empty sentinel defeats that "+
			"fallback so the node reports a region it is not in", got)
	}

	if ci, ok := r.ConnectionTo(testNodeIDA); ok || ci != (ConnectionInfo{}) {
		t.Fatalf("NilConnectionReporter.ConnectionTo() = (%+v, %v), want (zero, false) "+
			"— a reporter with no manager cannot know about any connection, and "+
			"reporting ok=true would have callers read a zero ConnectionInfo as real", ci, ok)
	}
	// ⚠ DELIBERATELY A LENGTH CHECK, NOT `!= nil`. My first draft asserted nil
	// and a mutant returning `[]ConnectionInfo{}` killed it — which made that
	// mutant fail as a negative control and revealed the assertion was
	// over-specified. Nil and empty are indistinguishable to every consumer
	// (`len` and `range` behave identically, and monitoring_api.go builds its own
	// slice before marshalling, so the JSON null/[] difference never surfaces).
	// Pinning nil would only block a harmless refactor.
	if got := r.ActiveConnections(); len(got) != 0 {
		t.Fatalf("ActiveConnections() = %+v, want empty", got)
	}
	if got := r.ConnectedPeerCount(); got != 0 {
		t.Fatalf("ConnectedPeerCount() = %d, want 0", got)
	}
}

// ConnectionTo on the Nil reporter must answer false for EVERY key, including
// the empty one — it holds no state, so there is no input that should produce a
// hit. A map-backed stub that happened to return true for "" would satisfy a
// looser test.
func TestTheNilReporterKnowsNoConnectionForAnyKey(t *testing.T) {
	r := NilConnectionReporter{}
	for _, key := range []string{"", testNodeIDA, testNodeIDB, "not-a-node-id"} {
		if _, ok := r.ConnectionTo(key); ok {
			t.Fatalf("ConnectionTo(%q) reported a connection from a reporter with "+
				"no manager", key)
		}
	}
}

// 🔑 THE FALLBACK MUST BE INDISTINGUISHABLE FROM AN IDLE REAL REPORTER. That is
// what makes substituting it at the four call sites safe: on a node with a
// manager but no peers, both must give the same answers, so swapping in the
// fallback cannot change what an observer sees.
func TestTheNilReporterAgreesWithAnIdleRealReporter(t *testing.T) {
	m := registerTestManager()
	m.peers = map[string]*peerConn{}
	real_ := NewConnectionReporter(m)
	nil_ := NilConnectionReporter{}

	if a, b := real_.ConnectedPeerCount(), nil_.ConnectedPeerCount(); a != b {
		t.Fatalf("idle real reporter says %d connected peers, nil fallback says %d "+
			"— substituting the fallback changes the observed peer count", a, b)
	}
	if len(real_.ActiveConnections()) != len(nil_.ActiveConnections()) {
		t.Fatalf("idle real reporter lists %d active connections, nil fallback %d",
			len(real_.ActiveConnections()), len(nil_.ActiveConnections()))
	}
	_, aOK := real_.ConnectionTo(testNodeIDA)
	_, bOK := nil_.ConnectionTo(testNodeIDA)
	if aOK != bOK {
		t.Fatalf("idle real reporter ConnectionTo ok=%v, nil fallback ok=%v", aOK, bOK)
	}
}

// And the runtime's own accessor must hand back the fallback rather than a
// reporter wrapping a nil manager — the latter would panic on first use, at
// observability time, on a node that is otherwise healthy.
func TestARuntimeWithNoConnectionManagerYieldsTheNilReporter(t *testing.T) {
	rt := &Runtime{} // connMgr deliberately nil

	r := rt.ConnectionReporter()
	if r == nil {
		t.Fatal("ConnectionReporter() returned a nil interface — every caller " +
			"dereferences it without a check")
	}
	if _, isNil := r.(NilConnectionReporter); !isNil {
		t.Fatalf("ConnectionReporter() returned %T for a runtime with no manager; "+
			"want NilConnectionReporter — a reporter wrapping a nil manager panics "+
			"on first use, and it would do so on the observability path of an "+
			"otherwise healthy node", r)
	}

	// And it must actually be usable, not merely the right type.
	if got := r.SelfNodeID(); got != "" {
		t.Fatalf("SelfNodeID() = %q from the runtime fallback, want \"\"", got)
	}
	if n := r.ConnectedPeerCount(); n != 0 {
		t.Fatalf("ConnectedPeerCount() = %d from the runtime fallback, want 0", n)
	}
}
