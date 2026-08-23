/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

// COVERAGE of the service-health metric and its status→value mapping
// continuing the exporter coverage.
//
// This is the series operators ALERT on. `mesh_service_health` is a gauge
// whose meaning lives entirely in a string→float mapping, so a wrong mapping
// does not fail anything — it inverts the alert. A service reading 1.0 while
// unreachable is a page that never fires.
//
// 🙋 Sixth cost measured rather than estimated: HealthEvaluator is a 6-method
// interface and ServiceHealthReport is a plain struct, so the fake is ~20
// lines with no registry involved.

// fakeEvaluator serves canned reports. The unused methods satisfy the
// interface and are deliberately inert — this file is about the metric, not
// the evaluator.
type fakeEvaluator struct{ reports []*ServiceHealthReport }

func (f *fakeEvaluator) AllServiceHealth() []*ServiceHealthReport { return f.reports }
func (f *fakeEvaluator) ServiceHealth(name string) *ServiceHealthReport {
	for _, r := range f.reports {
		if r.ServiceName == name {
			return r
		}
	}
	return nil
}
func (f *fakeEvaluator) MeshStatus(string) string  { return "" }
func (f *fakeEvaluator) LastEvaluation() time.Time { return time.Time{} }
func (f *fakeEvaluator) Start()                    {}
func (f *fakeEvaluator) Stop()                     {}

var _ HealthEvaluator = (*fakeEvaluator)(nil)

// 🔴 THE MAPPING IS THE METRIC. Every value here is load-bearing for an
// alert rule, and a swap is invisible: the series still exists, still has the
// right labels, and reports the wrong health.
func TestServiceHealthValueMappingIsExact(t *testing.T) {
	cases := []struct {
		status string
		want   string // formatted to 2dp, as the exporter writes it
	}{
		{"healthy", "1.00"},
		{"degraded", "0.50"},
		{"connecting", "0.25"},
		{"unreachable", "0.00"},
		// An unrecognised status must read as UNREACHABLE, not healthy: a
		// typo or a new status added upstream must fail safe toward alerting,
		// never toward silence.
		{"some-new-status", "0.00"},
	}
	for _, tc := range cases {
		t.Run(tc.status, func(t *testing.T) {
			e := NewMetricsExporter(MetricsExporterConfig{
				Evaluator: &fakeEvaluator{reports: []*ServiceHealthReport{{
					ServiceName: "auth.hstles.com", CombinedStatus: tc.status,
					MeshStatus: "healthy", HTTPStatus: "operational", BestTransport: "A",
				}}},
			})

			var buf bytes.Buffer
			e.writeServiceHealthMetrics(&buf)
			out := buf.String()

			if !strings.Contains(out, "mesh_service_health{") {
				t.Fatalf("no mesh_service_health series emitted:\n%s", out)
			}
			if !strings.HasSuffix(strings.TrimSpace(out), " "+tc.want) {
				t.Fatalf("status %q produced a value other than %s — an alert "+
					"rule keyed on this gauge now fires wrongly, or does not "+
					"fire at all:\n%s", tc.status, tc.want, out)
			}
		})
	}
}

// The labels are the identity of the series. Losing one silently merges two
// services into a single time-series and an operator cannot tell which is
// unhealthy.
func TestServiceHealthCarriesItsIdentifyingLabels(t *testing.T) {
	e := NewMetricsExporter(MetricsExporterConfig{
		Evaluator: &fakeEvaluator{reports: []*ServiceHealthReport{{
			ServiceName: "auth.hstles.com", CombinedStatus: "degraded",
			MeshStatus: "connecting", HTTPStatus: "operational", BestTransport: "C",
		}}},
	})

	var buf bytes.Buffer
	e.writeServiceHealthMetrics(&buf)
	out := buf.String()

	for _, want := range []string{
		`service="auth.hstles.com"`, `mesh_status="connecting"`,
		`http_status="operational"`, `best_transport="C"`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("label %s missing — two services collapse into one "+
				"time-series and the unhealthy one becomes invisible:\n%s",
				want, out)
		}
	}
}

// Absent evaluator and empty report set both emit NOTHING — not a zero.
//
// That distinction is deliberate and worth pinning: a zero-valued
// mesh_service_health means "this service is unreachable" and would page. An
// absent series means "this node has no opinion", which is the truth when the
// evaluator is not wired or has not run yet.
func TestServiceHealthEmitsNothingRatherThanZeroWhenItHasNoOpinion(t *testing.T) {
	t.Run("no evaluator", func(t *testing.T) {
		e := NewMetricsExporter(MetricsExporterConfig{})
		var buf bytes.Buffer
		e.writeServiceHealthMetrics(&buf)
		if buf.Len() != 0 {
			t.Fatalf("emitted output with no evaluator — a 0.00 gauge reads as "+
				"UNREACHABLE and pages for a service this node never assessed:\n%s",
				buf.String())
		}
	})

	t.Run("evaluator with no reports", func(t *testing.T) {
		e := NewMetricsExporter(MetricsExporterConfig{Evaluator: &fakeEvaluator{}})
		var buf bytes.Buffer
		e.writeServiceHealthMetrics(&buf)
		if buf.Len() != 0 {
			t.Fatalf("emitted output for an evaluator with zero reports:\n%s",
				buf.String())
		}
	})
}
