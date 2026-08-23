/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import "testing"

// Covers the connection-priority surface: ConnectionPriority.String(),
// ConnectionBudget.SetPriority and ConnectionBudget.GetPriority.
//
// CENSUS, and it needed the name-collision test to come out right:
//
//	.SetPriority(  1 non-test caller (ConnectionManager.updatePriorities)
//	.GetPriority(  10 raw hits — and ZERO of them are ours. Every one is a
//	               PROTOBUF getter on an unrelated type (DnsAnswer, PolicyRecord,
//	               CreateGroupRequest…): p.GetPriority(), req.GetPriority(),
//	               in.GetPriority(). Measured: `budget.GetPriority(` / `b.GetPriority(` = 0.
//	               ⇒ ConnectionBudget.GetPriority has NO production caller. It is
//	               tested here as the read-side of a setter that IS called, and
//	               because it is exported on a published module.
//	String()       surfaces at peer_connections.go:1799 in the PeerStates map —
//	               an OPERATOR-FACING API field, not a log nicety.

// 🔴 String() IS AN API FIELD. peer_connections.go:1799 puts p.priority.String()
// into the operator-facing PeerStates map, so these five literals are a wire
// contract: a dashboard or alert rule keying on "critical" breaks silently if
// the label changes, and the peer that must never be drained looks ordinary.
func TestEveryPriorityHasItsOwnStableLabel(t *testing.T) {
	for _, tc := range []struct {
		p    ConnectionPriority
		want string
	}{
		{PriorityIdle, "idle"},
		{PriorityLow, "low"},
		{PriorityNormal, "normal"},
		{PriorityHigh, "high"},
		{PriorityCritical, "critical"},
	} {
		if got := tc.p.String(); got != tc.want {
			t.Errorf("ConnectionPriority(%d).String() = %q, want %q — this string is "+
				"an operator-facing API field (peer_connections.go:1799), so a "+
				"renamed label silently breaks whatever keys on it",
				int(tc.p), got, tc.want)
		}
	}

	// Distinctness is the property that matters most: two priorities sharing a
	// label makes drain order unreadable from the outside.
	seen := map[string]ConnectionPriority{}
	for _, p := range []ConnectionPriority{
		PriorityIdle, PriorityLow, PriorityNormal, PriorityHigh, PriorityCritical,
	} {
		if prev, dup := seen[p.String()]; dup {
			t.Fatalf("priorities %d and %d share the label %q — an operator cannot "+
				"tell a drainable peer from one that must never be drained",
				int(prev), int(p), p.String())
		}
		seen[p.String()] = p
	}
}

// String() must be TOTAL: an out-of-range value must yield "unknown", not an
// empty string. An empty label renders as a missing field rather than a wrong
// one, which is the harder failure to notice.
func TestAnOutOfRangePriorityRendersAsUnknownNotEmpty(t *testing.T) {
	for _, p := range []ConnectionPriority{-1, 5, 99} {
		got := p.String()
		if got == "" {
			t.Errorf("ConnectionPriority(%d).String() is EMPTY — the PeerStates "+
				"field disappears rather than showing a wrong value", int(p))
		}
		if got != "unknown" {
			t.Errorf("ConnectionPriority(%d).String() = %q, want \"unknown\"", int(p), got)
		}
	}
}

// 🔴 THE LAZY INIT IS LOAD-BEARING. ConnectionBudget is an exported struct with
// exported fields, so any composite-literal construction leaves `priorities`
// nil — and SetPriority's nil check is the only thing standing between that and
// an assignment-to-nil-map panic on a live peer-priority update.
func TestSetPriorityWorksOnACompositeLiteralBudget(t *testing.T) {
	// DELIBERATELY not DefaultConnectionBudget() — that path may initialise the
	// map, which would make this test prove nothing about the guard.
	b := &ConnectionBudget{MaxTotal: 50, MaxPerPeer: 2, MinPerPeer: 1}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("SetPriority panicked on a composite-literal budget: %v — "+
				"assignment to a nil map. Any caller constructing ConnectionBudget "+
				"by literal crashes on the first priority update", r)
		}
	}()
	b.SetPriority(testNodeIDA, PriorityCritical)

	if got := b.GetPriority(testNodeIDA); got != PriorityCritical {
		t.Fatalf("GetPriority = %v after SetPriority(critical) — the value did not "+
			"survive the lazy init()", got)
	}
}

// 🔑 LAST-WRITE-WINS, INCLUDING DEMOTION. updatePriorities recomputes priority
// from scratch on every tick, so demotion is the normal case — a max-merge would
// make every peer that was ever busy permanently un-drainable, and the budget
// would never reclaim capacity.
func TestSetPriorityDemotesAsWellAsPromotes(t *testing.T) {
	b := DefaultConnectionBudget()

	b.SetPriority(testNodeIDA, PriorityCritical)
	b.SetPriority(testNodeIDA, PriorityIdle)

	if got := b.GetPriority(testNodeIDA); got != PriorityIdle {
		t.Fatalf("GetPriority = %v after a critical→idle rewrite, want idle — "+
			"priority is being max-merged or accumulated, so a peer that was once "+
			"busy can never be drained again and the budget never reclaims capacity",
			got)
	}
}

// Priorities are per-node: setting one must not move another.
func TestPrioritiesAreScopedPerNode(t *testing.T) {
	b := DefaultConnectionBudget()
	b.SetPriority(testNodeIDA, PriorityCritical)
	b.SetPriority(testNodeIDB, PriorityLow)

	if got := b.GetPriority(testNodeIDA); got != PriorityCritical {
		t.Fatalf("node A = %v after setting node B, want critical — the map is not "+
			"keyed per node", got)
	}
	if got := b.GetPriority(testNodeIDB); got != PriorityLow {
		t.Fatalf("node B = %v, want low", got)
	}
}

// 🔴 CHARACTERISATION, AND IT IS THE SIXTH INSTANCE OF ONE FAMILY THIS SESSION.
//
// GetPriority returns PriorityIdle both when the map is nil AND when the node is
// simply absent — and PriorityIdle is 0, which is also the MOST DRAINABLE rank.
// So "never measured" and "measured as idle" are indistinguishable, and the
// default is the one that gets you drained first.
//
// That is a zero standing in for "unknown" inside an ordering where zero is
// also an extreme — the same shape as an unstamped event timestamp or an
// unmeasured transport grade.
//
// ⚠ Here it is BENIGN — an unknown peer being treated as drainable is the safe
// direction, unlike ranking an unmeasured transport BEST. Pinned so the choice
// stays visible rather than accidental.
func TestAnUnsetNodeIsIndistinguishableFromAMeasuredIdleNode(t *testing.T) {
	b := DefaultConnectionBudget()

	neverSeen := b.GetPriority("node-that-was-never-set")
	b.SetPriority(testNodeIDA, PriorityIdle)
	measuredIdle := b.GetPriority(testNodeIDA)

	if neverSeen != measuredIdle {
		t.Fatalf("unset node = %v but a measured-idle node = %v — absence has been "+
			"given its own representation. That is very likely an improvement; "+
			"update this test deliberately", neverSeen, measuredIdle)
	}
	if neverSeen != PriorityIdle {
		t.Fatalf("unset node = %v, want PriorityIdle", neverSeen)
	}
}

// 🔑 THE TWO NIL GUARDS ARE NOT EQUALLY LOAD-BEARING, AND MUTATION PROVED IT.
//
//	SetPriority's guard (:133-135)  REQUIRED — writing to a nil map PANICS.
//	                                Mutant B1 removes it and this suite goes red.
//	GetPriority's guard (:144-146)  INERT — READING a nil map already returns the
//	                                zero value, which IS PriorityIdle. Mutant B5
//	                                removes it and nothing changes: an EQUIVALENT
//	                                mutant, correctly surviving.
//
// The guard is harmless and self-documenting, so this is a note rather than a
// finding — but the asymmetry is the interesting part: identical-looking guards
// on the same field, one of which cannot fire.
//
// This test therefore pins the BEHAVIOUR (no panic, returns Idle), which holds
// with or without the guard, rather than pretending to pin the guard itself.
func TestGetPriorityOnACompositeLiteralBudgetDoesNotPanic(t *testing.T) {
	b := &ConnectionBudget{MaxTotal: 50}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("GetPriority panicked on a nil priorities map: %v", r)
		}
	}()
	if got := b.GetPriority(testNodeIDA); got != PriorityIdle {
		t.Fatalf("GetPriority = %v on an uninitialised budget, want PriorityIdle", got)
	}
}
