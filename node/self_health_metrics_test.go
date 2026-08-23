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

// COVERAGE of writeSelfHealthMetrics + selfHealthStatusToFloat,
// 62.5% / 0.0%. This is the observability system reporting on ITSELF, so a
// wrong answer here hides every other metric's staleness.
//
// 🔑 THE THIRD VARIANT OF THE ZERO QUESTION, AND IT COMPLETES THE SET.
// Three writers, three different nil behaviours, all deliberate:
//
//	writeServiceHealthMetrics       nil -> emits NOTHING       ("no opinion")
//	writeDegradedSubsystemMetrics   nil -> 0 = NONE DEGRADED   (healthy)
//	writeSelfHealthMetrics          nil -> 0 = STALLED         (UNHEALTHY)
//
// The same numeral means healthy in one series and stalled in another. That
// is not sloppiness: an unwired self-health monitor genuinely IS stalled
// observability, so this one fails toward alerting — and it is worth a test
// precisely because a reader who learned the degraded-subsystem convention
// would "fix" it the wrong way.

// lagEvaluator drives Check() through its thresholds by controlling only
// LastEvaluation — the single input that decides healthy/lagging/stalled.
type lagEvaluator struct {
	fakeEvaluator
	last time.Time
}

func (l *lagEvaluator) LastEvaluation() time.Time { return l.last }

func selfHealthFor(t *testing.T, evalLag time.Duration) *SelfHealthMonitor {
	t.Helper()
	const interval = time.Second
	ev := &lagEvaluator{}
	if evalLag > 0 {
		ev.last = time.Now().Add(-evalLag)
	} else {
		ev.last = time.Now()
	}
	// LaggingMultiplier 2, StalledMultiplier 4 over a 1s interval ⇒
	// lagging above 2s, stalled above 4s.
	return NewSelfHealthMonitor(ev, NilConnectionReporter{}, interval,
		SelfHealthMonitorConfig{LaggingMultiplier: 2, StalledMultiplier: 4})
}

// 🔴 THE NIL PATH FAILS TOWARD ALERTING, AND THAT IS THE POINT.
func TestSelfHealthWithNoMonitorReportsStalledNotHealthy(t *testing.T) {
	e := NewMetricsExporter(MetricsExporterConfig{}) // no SelfHealth

	var buf bytes.Buffer
	e.writeSelfHealthMetrics(&buf)
	out := buf.String()

	if !strings.Contains(out, "mesh_self_health_status 0") {
		t.Fatalf("an unwired self-health monitor did not report 0 — and 0 here "+
			"means STALLED. Reporting anything else claims the observability "+
			"system is working when nothing is checking it:\n%s", out)
	}
	// It must still emit the HELP/TYPE preamble so the series exists.
	if !strings.Contains(out, "# TYPE mesh_self_health_status gauge") {
		t.Fatalf("the series preamble is missing, so the gauge has no type:\n%s", out)
	}
}

// The three statuses, driven through the REAL Check() rather than by calling
// the mapping directly — so the thresholds and the mapping are pinned
// together. A wrong threshold and a wrong mapping produce the same wrong
// gauge, and only an end-to-end assertion catches both.
func TestSelfHealthStatusAndValueTrackTheEvaluationLag(t *testing.T) {
	for _, tc := range []struct {
		name       string
		lag        time.Duration
		wantStatus string
		wantValue  string
	}{
		{"fresh evaluation", 0, "healthy", "1.00"},
		{"past the lagging threshold", 3 * time.Second, "lagging", "0.50"},
		{"past the stalled threshold", 10 * time.Second, "stalled", "0.00"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := NewMetricsExporter(MetricsExporterConfig{SelfHealth: selfHealthFor(t, tc.lag)})

			var buf bytes.Buffer
			e.writeSelfHealthMetrics(&buf)
			out := buf.String()

			if !strings.Contains(out, `status="`+tc.wantStatus+`"`) {
				t.Fatalf("lag %v produced a status other than %q — the label an "+
					"operator reads is wrong:\n%s", tc.lag, tc.wantStatus, out)
			}
			if !strings.HasSuffix(strings.TrimSpace(out), " "+tc.wantValue) {
				t.Fatalf("lag %v produced a value other than %s — an alert rule "+
					"on this gauge fires wrongly or stays silent:\n%s",
					tc.lag, tc.wantValue, out)
			}
		})
	}
}

// The default arm is UNREACHABLE through the exporter — Check() only ever
// returns healthy/lagging/stalled — so it is tested directly. It is not dead
// code: it is the landing site for a status string added to Check() later, and
// it must land on 0.0 (= stalled, pages) rather than on healthy silence. The
// same guard as healthStatusToFloat's.
func TestSelfHealthUnrecognisedStatusFailsTowardAlerting(t *testing.T) {
	for _, status := range []string{"", "degraded", "Healthy", "unknown"} {
		if got := selfHealthStatusToFloat(status); got != 0.0 {
			t.Errorf("selfHealthStatusToFloat(%q) = %v, want 0.0 — an unrecognised "+
				"status must read as stalled. Any other value lets a future status "+
				"string silently report the observability system as working",
				status, got)
		}
	}
	// Positive control: the mapping is not simply returning 0 for everything.
	if got := selfHealthStatusToFloat("healthy"); got != 1.0 {
		t.Fatalf(`selfHealthStatusToFloat("healthy") = %v, want 1.0 — the loop `+
			`above would pass vacuously against a mapping that returns 0 always`, got)
	}
}

// 🔴 THE FOURTH VARIANT, AND IT POINTS THE OTHER WAY.
//
// A WIRED monitor whose evaluator has NEVER run reports **healthy 1.00** —
// the deliberate zero-time guard at self_health.go:206-212, which exists
// because time.Since(zeroTime) overflows to a ~292-year junk lag.
//
// So the two "we know nothing" states disagree:
//
//	no monitor at all        -> 0    = stalled
//	monitor, evaluator never ran -> 1.00 = healthy
//
// This test pins the CURRENT behaviour rather than asserting it is right,
// because the guard has no expiry: it cannot distinguish "has not run YET"
// from "never ran", and the monitor records no start time to bound the grace
// window with. An evaluator that fails to start is reported healthy forever.
// Reported to @R as a finding; if the guard gains a bound, this test is the
// one that must change, and deliberately.
func TestSelfHealthReportsHealthyForAnEvaluatorThatHasNeverRun(t *testing.T) {
	// last stays the zero time: LastEvaluation() has never been set.
	monitor := NewSelfHealthMonitor(&lagEvaluator{}, NilConnectionReporter{}, time.Second,
		SelfHealthMonitorConfig{LaggingMultiplier: 2, StalledMultiplier: 4})
	e := NewMetricsExporter(MetricsExporterConfig{SelfHealth: monitor})

	var buf bytes.Buffer
	e.writeSelfHealthMetrics(&buf)
	out := buf.String()

	if !strings.Contains(out, `status="healthy"`) {
		t.Fatalf("the zero-LastEvaluation guard no longer reads as healthy. That "+
			"may be an improvement — but it is a DELIBERATE change to an alerting "+
			"series and must be made knowingly, not as a side effect:\n%s", out)
	}
	if !strings.Contains(out, `eval_lag_ms="0"`) {
		t.Fatalf("a never-run evaluator reported a non-zero lag — the zero-time "+
			"guard is gone and time.Since(zeroTime) is back, which renders as a "+
			"~292-year lag:\n%s", out)
	}
}

// The lag values are carried as labels so an operator can see HOW stale, not
// just that it is stale. Losing them leaves a binary signal with no diagnosis.
func TestSelfHealthCarriesTheLagLabels(t *testing.T) {
	e := NewMetricsExporter(MetricsExporterConfig{SelfHealth: selfHealthFor(t, 3*time.Second)})

	var buf bytes.Buffer
	e.writeSelfHealthMetrics(&buf)
	out := buf.String()

	for _, want := range []string{"eval_lag_ms=", "scan_lag_ms="} {
		if !strings.Contains(out, want) {
			t.Fatalf("label %s missing — the gauge says the observability system "+
				"is degraded but not by how much:\n%s", want, out)
		}
	}
	// And the eval lag must be non-zero for a 3s-stale evaluation, or the
	// label is present but carrying nothing.
	if strings.Contains(out, `eval_lag_ms="0"`) {
		t.Fatalf("eval_lag_ms is 0 for a 3s-stale evaluation — the label is "+
			"wired to the wrong quantity:\n%s", out)
	}
}
