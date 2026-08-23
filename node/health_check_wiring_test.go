/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"errors"
	"testing"
	"time"

	lad "github.com/bbmumford/ledger"
	obshealth "github.com/bbmumford/loom/pkg/obshealth"
)

// Runtime.healthCheck is declared, read by three call sites, and — before the
// wiring in Initialize — never assigned. These tests cover what each reader
// does in both states, because the nil state is indistinguishable from a
// healthy node at every one of them:
//
//	IsHealthy       returns true on nil, so the answer is "healthy" either way
//	HealthCheck     hands out nil, so a caller cannot tell it apart from "off"
//	performHealthCheck  is the ONLY writer of the mesh.raft / mesh.ledger
//	                    entries in the registry, which mesh_subsystem_degraded
//	                    and /api/monitoring/subsystems read
//
// The registry assertions are the ones that matter: a field assignment proves
// nothing until the subsystem entry it feeds actually moves.

// degraded reports whether a subsystem currently has a degraded entry.
// The registry exposes a sorted Snapshot rather than per-name lookup.
func degraded(r *obshealth.Registry, id obshealth.SubsystemID) bool {
	for _, e := range r.Snapshot().Entries {
		if e.Name == id {
			return true
		}
	}
	return false
}

// probeLedger fails Head on demand so the ledger probe has something to find.
type probeLedger struct {
	lad.Ledger
	headErr error
	calls   int
}

func (l *probeLedger) Head(context.Context) (lad.CausalWatermark, error) {
	l.calls++
	return lad.CausalWatermark{}, l.headErr
}

// 🔴 AN UNREACHABLE LEDGER MUST REACH THE REGISTRY. This is the capability the
// unassigned field removed: without a HealthCheck nothing ever writes
// SubsystemMeshLedger, so the degraded-subsystem gauge reads clean while the
// ledger is unreachable.
func TestAFailedLedgerProbeMarksTheLedgerSubsystem(t *testing.T) {
	reg := obshealth.New(obshealth.AllowedSubsystems())
	ledger := &probeLedger{headErr: errors.New("ledger unreachable")}
	h := NewHealthCheck(HealthCheckDeps{Ledger: ledger, Registry: reg}, time.Hour)

	h.performHealthCheck()

	if ledger.calls != 1 {
		t.Fatalf("the ledger was probed %d times, want 1", ledger.calls)
	}
	if h.IsHealthy() {
		t.Error("IsHealthy() is true after the ledger probe failed — Runtime.IsHealthy " +
			"forwards this, so every caller reads the node as healthy")
	}
	if !degraded(reg, obshealth.SubsystemMeshLedger) {
		t.Error("the ledger subsystem is not marked degraded — performHealthCheck is the " +
			"only writer of this entry, so the mesh_subsystem_degraded gauge and " +
			"/api/monitoring/subsystems both keep reporting a healthy ledger")
	}
}

// The recovery direction. A mark that never clears is as useless as one that
// never fires: the node would stay degraded for the process lifetime after a
// single transient failure.
func TestARecoveredLedgerClearsTheSubsystem(t *testing.T) {
	reg := obshealth.New(obshealth.AllowedSubsystems())
	ledger := &probeLedger{headErr: errors.New("down")}
	h := NewHealthCheck(HealthCheckDeps{Ledger: ledger, Registry: reg}, time.Hour)

	h.performHealthCheck()
	if !degraded(reg, obshealth.SubsystemMeshLedger) {
		t.Fatal("fixture wrong: the subsystem was never marked, so a clear proves nothing")
	}

	ledger.headErr = nil
	h.performHealthCheck()

	if degraded(reg, obshealth.SubsystemMeshLedger) {
		t.Error("the ledger subsystem stayed degraded after a successful probe — one " +
			"transient failure pins the node degraded for the rest of the process")
	}
	if !h.IsHealthy() {
		t.Error("IsHealthy() is still false after recovery")
	}
}

// 🔑 AN UNWIRED DEPENDENCY MUST REPORT NOTHING, NOT HEALTH. The Runtime wires
// only the ledger; the raft hooks stay nil because it owns no raft. Marking
// raft healthy on that basis would be worse than the gap being fixed — it would
// assert a subsystem is fine on the strength of never having looked.
//
// 🔬 The fixture asserts on a subsystem that is NOT wired while another one IS,
// so "nothing was touched" is distinguishable from "the registry is inert".
func TestAnUnwiredSubsystemIsLeftUntouchedRatherThanMarkedHealthy(t *testing.T) {
	reg := obshealth.New(obshealth.AllowedSubsystems())
	h := NewHealthCheck(HealthCheckDeps{
		Ledger:   &probeLedger{},
		Registry: reg,
		// GetLeader/IsLeader/GetPeers deliberately nil: no raft here.
	}, time.Hour)

	h.performHealthCheck()

	if degraded(reg, obshealth.SubsystemMeshRaft) {
		t.Error("raft was marked degraded although no raft hook is wired — the check is " +
			"reporting on a subsystem it never observed")
	}
	// ⚠ WHAT THIS CANNOT ASSERT. The snapshot lists only DEGRADED entries, so an
	// absent name means "not marked" and covers both "observed and healthy" and
	// "never observed". The distinction an operator needs — checked-and-fine vs
	// never-checked — is not expressible through this surface, and the fact that
	// nil raft hooks and a healthy raft render identically is a reporting gap in
	// its own right rather than something this test can close.
	if degraded(reg, obshealth.SubsystemMeshLedger) {
		t.Error("the wired ledger probe succeeded but its subsystem reads degraded")
	}
}

// A HealthCheck with no registry must still work — the registry is documented
// as optional and the probe result still has to reach lastResult.
func TestTheProbeWorksWithNoRegistryWired(t *testing.T) {
	h := NewHealthCheck(HealthCheckDeps{
		Ledger: &probeLedger{headErr: errors.New("down")},
	}, time.Hour)

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("performHealthCheck panicked with no registry: %v — the registry is "+
				"documented as optional, so this runs on every endpoint that has not "+
				"wired one", r)
		}
	}()

	h.performHealthCheck()

	if h.IsHealthy() {
		t.Error("a failed ledger probe left the node healthy when no registry was wired — " +
			"the registry is a reporting sink, not the thing that decides health")
	}
}

// ⚠ WHAT THIS FILE STILL DOES NOT COVER, STATED SO A GREEN RUN DOES NOT IMPLY
// IT. These tests enter through startHealthCheck. Deleting the single call to
// it inside Initialize leaves every test here passing, because none of them run
// Initialize — that needs a full node config, a listener and a ledger backend,
// which is an integration concern. Extracting the seam moved the boundary as
// far as a unit test reaches: the seam's CONTENTS are covered (a nil ledger and
// a nil registry are both caught below), and exactly one line — the call site —
// is not.
//
// The wiring, entered through the seam Initialize calls. Every test above
// constructs a HealthCheck directly, so deleting the assignment in Initialize
// left them all green — they cover the probe and say nothing about whether the
// Runtime ever builds one.
//
// Consumes rt.ledger and rt.healthRegistry; feeds rt.healthCheck.
func TestStartHealthCheckWiresTheRuntimesOwnDependencies(t *testing.T) {
	reg := obshealth.New(obshealth.AllowedSubsystems())
	ledger := &probeLedger{headErr: errors.New("ledger unreachable")}
	rt := &Runtime{ledger: ledger, healthRegistry: reg}

	rt.startHealthCheck(time.Hour)
	t.Cleanup(rt.healthCheck.Stop)

	if rt.healthCheck == nil {
		t.Fatal("no health check was constructed, so Runtime.IsHealthy returns true " +
			"unconditionally and nothing writes the mesh.ledger subsystem entry")
	}
	if rt.HealthCheck() == nil {
		t.Error("the accessor hands out nil after a start")
	}

	// The probe must be reachable with the Runtime's own ledger, not merely
	// constructed: a HealthCheck wired to a nil ledger observes nothing.
	rt.healthCheck.performHealthCheck()

	if ledger.calls == 0 {
		t.Error("the Runtime's ledger was never probed — the health check was built " +
			"without the dependency it exists to watch")
	}
	if rt.IsHealthy() {
		t.Error("Runtime.IsHealthy() is true while the ledger probe is failing")
	}
	if !degraded(reg, obshealth.SubsystemMeshLedger) {
		t.Error("the Runtime's own registry never received the ledger mark")
	}
}

// An unobserved subsystem must not have another writer's mark cleared out from
// under it. SelfHealthMonitor and the endpoint's own probes share this
// registry, so a check that clears what it never looked at silently reports a
// broken subsystem as recovered.
//
// The fresh-registry tests cannot see this: Clear on an unmarked entry is a
// no-op, so a check that clears indiscriminately is indistinguishable from one
// that respects the guard until something else has marked the entry first.
func TestAnUnobservedSubsystemDoesNotClearAnotherWritersMark(t *testing.T) {
	reg := obshealth.New(obshealth.AllowedSubsystems())
	at := time.Now()
	if err := reg.Mark(obshealth.SubsystemMeshRaft, at, errors.New("no leader")); err != nil {
		t.Fatalf("fixture: %v", err)
	}

	// No raft hooks: this check observes nothing about raft.
	h := NewHealthCheck(HealthCheckDeps{Ledger: &probeLedger{}, Registry: reg}, time.Hour)
	h.performHealthCheck()

	if !degraded(reg, obshealth.SubsystemMeshRaft) {
		t.Error("a health check with no raft hook cleared an existing raft mark — it " +
			"reported a subsystem recovered on the strength of never having observed " +
			"it, so a leaderless raft renders healthy")
	}
}
