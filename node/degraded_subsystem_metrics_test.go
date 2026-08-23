/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	obshealth "github.com/bbmumford/loom/pkg/obshealth"
)

// COVERAGE of writeDegradedSubsystemMetrics, 38.5% → the branches
// that matter.
//
// 🔑 THE ASYMMETRY THIS FILE EXISTS TO PIN, because it looks like an
// inconsistency and is not:
//
//	writeServiceHealthMetrics  no evaluator -> emits NOTHING
//	writeDegradedSubsystemMetrics  no registry -> emits `..._total 0`
//
// Both are correct, for opposite reasons. "No opinion on a service's health"
// must not render as `0.00` = unreachable, which would page. But "zero
// subsystems degraded" IS a true statement when you hold no degradation
// records — and emitting it keeps the series continuous, so a Prometheus
// `rate()`/`absent()` rule does not fire merely because a node has not
// registered anything yet.

func degradedRegistry(t *testing.T, degraded ...obshealth.SubsystemID) *obshealth.Registry {
	t.Helper()
	r := obshealth.New(obshealth.AllowedSubsystems())
	for _, id := range degraded {
		if err := r.Mark(id, time.Now(), errors.New("test degradation")); err != nil {
			t.Fatalf("Mark(%q) rejected — the fixture uses a subsystem outside "+
				"AllowedSubsystems and the test below would be vacuous: %v", id, err)
		}
	}
	return r
}

// 🔴 THE SERIES MUST EXIST EVEN AT ZERO. An absent gauge and a zero gauge are
// different alerting facts: `absent(mesh_subsystem_degraded_total)` fires on
// the first, and a threshold rule reads the second as healthy.
func TestDegradedTotalIsEmittedAsZeroRatherThanOmittedWhenUnwired(t *testing.T) {
	e := NewMetricsExporter(MetricsExporterConfig{}) // no registry

	var buf bytes.Buffer
	e.writeDegradedSubsystemMetrics(&buf)
	out := buf.String()

	if !strings.Contains(out, "mesh_subsystem_degraded_total 0") {
		t.Fatalf("unwired registry did not emit a zero total — the series "+
			"disappears and an absent() rule fires on a healthy node:\n%s", out)
	}
	// And the per-subsystem gauge must NOT appear: absence of a label is how
	// a healthy subsystem is represented.
	if strings.Contains(out, "mesh_subsystem_degraded{") {
		t.Fatalf("a per-subsystem series was emitted with no registry — every "+
			"label there means DEGRADED:\n%s", out)
	}
}

// A healthy registry is the common case and must read as zero degraded, with
// no per-subsystem rows at all.
func TestHealthyRegistryEmitsZeroAndNoSubsystemRows(t *testing.T) {
	e := NewMetricsExporter(MetricsExporterConfig{HealthRegistry: degradedRegistry(t)})

	var buf bytes.Buffer
	e.writeDegradedSubsystemMetrics(&buf)
	out := buf.String()

	if !strings.Contains(out, "mesh_subsystem_degraded_total 0") {
		t.Fatalf("a healthy registry did not report 0 degraded:\n%s", out)
	}
	if strings.Contains(out, "mesh_subsystem_degraded{") {
		t.Fatalf("a healthy registry emitted per-subsystem rows — ABSENCE of a "+
			"label is how health is signalled, so every row here is a false "+
			"degradation:\n%s", out)
	}
}

// A degraded subsystem must appear BOTH in the aggregate and as its own
// labelled row: operators alert on the total and diagnose from the label.
func TestDegradedSubsystemAppearsInBothTheTotalAndItsOwnRow(t *testing.T) {
	allowed := obshealth.AllowedSubsystems()
	if len(allowed) == 0 {
		t.Skip("no allowed subsystems declared; nothing can be marked degraded")
	}
	id := allowed[0]
	e := NewMetricsExporter(MetricsExporterConfig{HealthRegistry: degradedRegistry(t, id)})

	var buf bytes.Buffer
	e.writeDegradedSubsystemMetrics(&buf)
	out := buf.String()

	if !strings.Contains(out, "mesh_subsystem_degraded_total 1") {
		t.Fatalf("one degraded subsystem did not raise the total to 1 — the "+
			"aggregate alert never fires:\n%s", out)
	}
	want := `mesh_subsystem_degraded{subsystem="` + string(id) + `"} 1`
	if !strings.Contains(out, want) {
		t.Fatalf("no labelled row for the degraded subsystem %q — the total "+
			"alerts but an operator cannot tell WHICH subsystem:\n%s", id, out)
	}
}

// Clearing a degradation must return the series to zero and remove the row.
// A stuck row is worse than no row: it pages forever on a healthy system.
func TestClearedSubsystemLeavesNoResidualRow(t *testing.T) {
	allowed := obshealth.AllowedSubsystems()
	if len(allowed) == 0 {
		t.Skip("no allowed subsystems declared")
	}
	id := allowed[0]
	r := degradedRegistry(t, id)

	var before bytes.Buffer
	NewMetricsExporter(MetricsExporterConfig{HealthRegistry: r}).writeDegradedSubsystemMetrics(&before)
	if !strings.Contains(before.String(), "mesh_subsystem_degraded_total 1") {
		t.Fatalf("premise wrong: the subsystem is not degraded to begin with:\n%s",
			before.String())
	}

	if err := r.Clear(id, time.Now()); err != nil {
		t.Fatal(err)
	}

	var after bytes.Buffer
	NewMetricsExporter(MetricsExporterConfig{HealthRegistry: r}).writeDegradedSubsystemMetrics(&after)
	out := after.String()
	if !strings.Contains(out, "mesh_subsystem_degraded_total 0") {
		t.Fatalf("a CLEARED subsystem still counts toward the total — the alert "+
			"never resolves:\n%s", out)
	}
	if strings.Contains(out, `subsystem="`+string(id)+`"`) {
		t.Fatalf("a CLEARED subsystem still has a labelled row — it pages "+
			"forever on a recovered system:\n%s", out)
	}
}
