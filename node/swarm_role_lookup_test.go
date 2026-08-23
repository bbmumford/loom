/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"testing"
	"time"

	lad "github.com/bbmumford/ledger"

	"github.com/bbmumford/loom/ports"
	"github.com/bbmumford/loom/secrets"
)

// COVERAGE of LookupRoleViaSwarm (swarm_integration.go:409), which was 0.0%.
//
// 🔴 WHY THIS FUNCTION AND NOT ANOTHER 0.0% ACCESSOR. It computes `covered` —
// the single input that decides whether the takeover engine arms
// (role_takeover.go:446). Everything the previous slice measured about
// evaluateRole's state machine is downstream of this one number.
//
// 🔑 THE INTERESTING PART IS THE NIL RETURN. LookupRoleViaSwarm returns nil for
// "I cannot answer" — no swarm, or no role table. evaluateRole consumes that
// with len(), which turns "unknown" into "ZERO HOLDERS", which is the most
// extreme possible reading and the one that arms takeover. That is the
// "absent sorts as best" shape this lane has now hit five times: a sentinel
// meaning UNKNOWN consumed as an extreme value.
//
// ⚠ MEASURED, NOT ASSUMED — AND IT IS NOT REACHABLE TODAY. InitSwarm returns
// an error if NewRoleTable fails (swarm_integration.go:87-91), so a
// SwarmIntegration it returns always carries a non-nil RoleTable, and
// StartTakeover's own precondition rules out a nil rt.swarm. The failure
// direction is wrong but no production path reaches it. Recording that
// precondition is the point: a hazard whose reachability is stated is a
// different object from one that is merely asserted, and "a fallback that
// EXISTS is not one that FIRES" cuts both ways.
//
// ⛔ CREDENTIAL FENCE: role/coverage records only — no key material.

func lookupFixture(records map[string]lad.RoleRecord) *Runtime {
	return &Runtime{
		swarm: &SwarmIntegration{
			Node:      &stubSwarmNode{},
			RoleTable: &RoleTable{byRole: map[string]map[string]lad.RoleRecord{"auth": records}},
		},
	}
}

// The two "cannot answer" inputs. Both must return nil rather than panicking —
// a panic here would take down the takeover ticker for every role.
func TestLookupRoleViaSwarmReturnsNilWhenItCannotAnswer(t *testing.T) {
	for _, tc := range []struct {
		name string
		rt   *Runtime
	}{
		{"InitSwarm never ran", &Runtime{}},
		{"swarm present but no role table", &Runtime{swarm: &SwarmIntegration{Node: &stubSwarmNode{}}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.rt.LookupRoleViaSwarm("auth", ""); got != nil {
				t.Errorf("LookupRoleViaSwarm = %v, want nil", got)
			}
		})
	}
}

// 🔴 THE CONSUMER COUPLING, MADE EXECUTABLE. This is the test that says what
// the nil above MEANS once evaluateRole reads it, rather than leaving it to a
// reader of two files to notice.
//
// A node whose role table is missing cannot see ANY holder of ANY role. It does
// not conclude "I don't know"; it concludes "nothing is covered" and starts the
// corroboration window on every role it guards. If the condition persisted past
// the window, such a node would claim every role it is entitled to — on the
// strength of having no information at all.
//
// This asserts CURRENT behaviour, so it will fail if someone changes the
// coupling. That is deliberate: the change is a design decision (route to
// @R/DESIGN), not something a test lane should quietly make, and a red test is
// how the decision surfaces rather than being lost.
func TestAMissingRoleTableIsReadAsZeroCoverageNotAsUnknown(t *testing.T) {
	rt := &Runtime{
		identity:       &NodeIdentity{NodeID: "self"},
		swarm:          &SwarmIntegration{Node: &stubSwarmNode{}}, // no RoleTable
		roleActivation: &roleActivationManager{activators: map[string]ports.RoleActivator{}, active: map[string]bool{}},
	}
	e := &TakeoverEngine{
		rt:           rt,
		cfg:          TakeoverConfig{Roles: []string{"auth"}, MinReplicas: 2, MaxWinners: 1, CorroborationWindow: time.Minute, ClaimSettle: time.Minute, Entitled: func(string) error { return nil }},
		envelopes:    map[string]*secrets.Envelope{},
		claims:       map[string]map[string]roleClaim{},
		missingSince: map[string]time.Time{},
		claimedAt:    map[string]time.Time{},
	}

	e.evaluateRole("auth")

	if _, armed := e.missingSince["auth"]; !armed {
		t.Error("a node with NO role table did not arm the corroboration window. If you " +
			"are reading this failure after changing the coupling, the change is almost " +
			"certainly an IMPROVEMENT — distinguishing \"unknown\" from \"zero holders\" " +
			"is the safer reading. Update this test, and tell @R/DESIGN, because which " +
			"of the two a node should assume is a design decision and not a test-lane one.")
	}
}

// The populated path must carry through exactly the two fields the trimmed view
// promises. MaxGrade is the one that would go unnoticed if dropped — it is
// zero-valued, so a comparison against a fixture that also used 0 would pass
// with the field never assigned.
func TestLookupRoleViaSwarmCarriesNodeIDAndMaxGrade(t *testing.T) {
	rt := lookupFixture(map[string]lad.RoleRecord{
		"node-a": {NodeID: "node-a", MaxGrade: 3},
		"node-b": {NodeID: "node-b", MaxGrade: 1},
	})

	got := rt.LookupRoleViaSwarm("auth", "")

	if len(got) != 2 {
		t.Fatalf("got %d results, want 2 — the coverage count feeding evaluateRole is wrong", len(got))
	}
	// Lookup sorts by NodeID, so this order is defined.
	if got[0].NodeID != "node-a" || got[1].NodeID != "node-b" {
		t.Errorf("got node IDs %q,%q, want node-a,node-b", got[0].NodeID, got[1].NodeID)
	}
	// 🔬 Anti-correlated on purpose: the grades DISAGREE with each other and
	// neither is the zero value, so an unassigned MaxGrade cannot pass.
	if got[0].MaxGrade != 3 || got[1].MaxGrade != 1 {
		t.Errorf("got MaxGrade %d,%d, want 3,1 — the grade was not carried into the "+
			"trimmed view, so every peer reads as grade 0", got[0].MaxGrade, got[1].MaxGrade)
	}
}

// An unknown role must count as zero holders, not panic. This is the ordinary
// case on a node that guards a role nobody else runs yet.
func TestLookupRoleViaSwarmOnAnUnknownRoleCountsAsZero(t *testing.T) {
	rt := lookupFixture(map[string]lad.RoleRecord{"node-a": {NodeID: "node-a"}})

	if got := rt.LookupRoleViaSwarm("billing", ""); len(got) != 0 {
		t.Errorf("got %d holders for a role no node advertises, want 0", len(got))
	}
}

// The handler filter must EXCLUDE nodes that advertise the role without the
// requested handler. A filter that admitted them would inflate the coverage
// count with nodes that cannot serve the call — coverage that is not coverage.
func TestTheHandlerFilterExcludesNodesLackingThatHandler(t *testing.T) {
	rt := lookupFixture(map[string]lad.RoleRecord{
		"node-a": {NodeID: "node-a", Handlers: []lad.HandlerMetadata{{Name: "orbtr.ai.auth.Login"}}},
		"node-b": {NodeID: "node-b", Handlers: []lad.HandlerMetadata{{Name: "orbtr.ai.auth.Logout"}}},
	})

	got := rt.LookupRoleViaSwarm("auth", "orbtr.ai.auth.Login")

	if len(got) != 1 {
		t.Fatalf("got %d results for a handler exactly one node advertises, want 1 — "+
			"coverage is being counted from nodes that cannot serve the call", len(got))
	}
	if got[0].NodeID != "node-a" {
		t.Errorf("got %q, want node-a — the filter kept the wrong node", got[0].NodeID)
	}
}
