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

// Covers writeConnectionEventMetrics + writeGossipMetrics, and pins the
// metric-name collision between them.
//
// The defect this file exists for: both writers can emit
// `# HELP mesh_connection_events_total`, with different meanings (all-time
// count vs a 5-minute window by type). The Prometheus text format permits
// exactly one HELP and one TYPE per metric name; prometheus/common's
// expfmt.TextParser answers a duplicate with
//
//	text format parsing error in line 4: second HELP line for metric name
//	"mesh_connection_events_total"
//
// and discards THE ENTIRE BODY — every other metric on the node with it.
// Measured against that parser offline, with a control that parsed clean.
//
// TestExporterOutputIsWellFormedPrometheusExposition does not catch it: that
// test validates the shape of each LINE and never compares lines to each other, so
// a per-line-perfect body that no parser will accept walked straight through
// it. TestExpositionDeclaresEachMetricNameExactlyOnce below is the assertion
// that was missing.

func eventLogFixture(t *testing.T, events ...ConnectionEvent) *ConnectionScaler {
	t.Helper()
	s := NewConnectionScaler(nil, nil)
	if s.EventLog() == nil {
		t.Fatal("premise wrong: a fresh scaler has no event log, so every " +
			"assertion below would be vacuous")
	}
	for _, ev := range events {
		s.EventLog().Append(ev)
	}
	return s
}

func connEvent(reason string, age time.Duration) ConnectionEvent {
	return ConnectionEvent{
		PeerNodeID: testNodeIDB,
		Transport:  "websocket",
		Reason:     reason,
		Timestamp:  time.Now().Add(-age),
	}
}

// 🔴 THE REGRESSION PIN. Two HELP lines for one name is not a cosmetic
// duplicate — it costs the whole scrape.
func TestExpositionDeclaresEachMetricNameExactlyOnce(t *testing.T) {
	m := reporterFixtureForMetrics(t)
	e := NewMetricsExporter(MetricsExporterConfig{
		Reporter: NewConnectionReporter(m),
		Budget:   DefaultConnectionBudget(),
		// Both event writers must be live, or the collision cannot appear
		// and this test is vacuous.
		Scaler: eventLogFixture(t, connEvent("connected", time.Second)),
	})

	var buf bytes.Buffer
	e.writeMetrics(&buf)
	out := buf.String()

	if !strings.Contains(out, "mesh_connection_events_total") ||
		!strings.Contains(out, "mesh_connection_events_recent") {
		t.Fatalf("premise wrong: both event writers did not fire, so this test "+
			"cannot see a collision between them:\n%s", out)
	}

	seenHelp, seenType := map[string]int{}, map[string]int{}
	for _, line := range strings.Split(out, "\n") {
		f := strings.Fields(line)
		if len(f) < 3 || f[0] != "#" {
			continue
		}
		switch f[1] {
		case "HELP":
			seenHelp[f[2]]++
		case "TYPE":
			seenType[f[2]]++
		}
	}
	if len(seenHelp) == 0 {
		t.Fatalf("premise wrong: no HELP lines parsed at all:\n%s", out)
	}
	for name, n := range seenHelp {
		if n > 1 {
			t.Errorf("%d HELP lines for metric name %q — a Prometheus text "+
				"parser rejects the SECOND one and drops the whole body, so "+
				"every metric this node exports is lost", n, name)
		}
	}
	for name, n := range seenType {
		if n > 1 {
			t.Errorf("%d TYPE lines for metric name %q — same failure: the "+
				"batch is discarded, not the line", n, name)
		}
	}
}

// The two series must stay distinguishable by name, because they answer
// different questions: how many events have EVER been logged, and what has
// happened in the last five minutes.
func TestAllTimeAndWindowedEventSeriesHaveDistinctNames(t *testing.T) {
	e := NewMetricsExporter(MetricsExporterConfig{
		Scaler: eventLogFixture(t, connEvent("connected", time.Second)),
	})

	var gossip, recent bytes.Buffer
	e.writeGossipMetrics(&gossip)
	e.writeConnectionEventMetrics(&recent)

	if !strings.Contains(gossip.String(), "mesh_connection_events_total 1") {
		t.Fatalf("the all-time gauge did not report the single logged event — "+
			"and this is the name io/tools/go/meshmon reads, so a change here "+
			"is a consumer break:\n%s", gossip.String())
	}
	if strings.Contains(recent.String(), "mesh_connection_events_total") {
		t.Fatalf("the WINDOWED writer is emitting the all-time metric name "+
			"again — the collision is back and the scrape body is "+
			"unparseable:\n%s", recent.String())
	}
	if !strings.Contains(recent.String(), `mesh_connection_events_recent{type="connected"} 1`) {
		t.Fatalf("the windowed series is missing its labelled sample:\n%s",
			recent.String())
	}
}

// 🔑 THE WINDOW IS THE WHOLE POINT OF THE SECOND SERIES. An event outside it
// must not appear — otherwise the "recent" series is just the all-time one
// with labels, and an operator watching for a reconnect storm sees a flat line
// that never decays.
func TestOnlyEventsInsideTheFiveMinuteWindowAreCountedAsRecent(t *testing.T) {
	e := NewMetricsExporter(MetricsExporterConfig{
		Scaler: eventLogFixture(t,
			connEvent("connected", 10*time.Second),    // inside
			connEvent("disconnected", 30*time.Minute), // outside
		),
	})

	var recent, gossip bytes.Buffer
	e.writeConnectionEventMetrics(&recent)
	e.writeGossipMetrics(&gossip)

	if !strings.Contains(recent.String(), `mesh_connection_events_recent{type="connected"} 1`) {
		t.Fatalf("an event 10s old is missing from the 5-minute window:\n%s",
			recent.String())
	}
	if strings.Contains(recent.String(), "disconnected") {
		t.Fatalf("a 30-MINUTE-old event appears in the 5-minute window — the "+
			"series never decays and a reconnect storm is indistinguishable "+
			"from an old one:\n%s", recent.String())
	}
	// The all-time gauge counts BOTH: that is the difference between the two
	// series, stated as an assertion.
	if !strings.Contains(gossip.String(), "mesh_connection_events_total 2") {
		t.Fatalf("the all-time gauge did not count the aged-out event — it is "+
			"supposed to be the lifetime figure, not a second window:\n%s",
			gossip.String())
	}
}

// An empty window emits NOTHING rather than a zero row. This is deliberate and
// differs from the all-time gauge, which always emits: a per-type breakdown
// has no types to name when nothing has happened, and inventing
// `{type="connected"} 0` would claim a vocabulary the node has not observed.
func TestEmptyWindowEmitsNoWindowedRowsButTheAllTimeGaugeStillReports(t *testing.T) {
	e := NewMetricsExporter(MetricsExporterConfig{
		Scaler: eventLogFixture(t, connEvent("connected", time.Hour)), // all aged out
	})

	var recent, gossip bytes.Buffer
	e.writeConnectionEventMetrics(&recent)
	e.writeGossipMetrics(&gossip)

	if recent.Len() != 0 {
		t.Fatalf("the windowed writer emitted rows for an empty window:\n%s",
			recent.String())
	}
	if !strings.Contains(gossip.String(), "mesh_connection_events_total 1") {
		t.Fatalf("the all-time gauge went silent too — it must keep reporting "+
			"so the series stays continuous:\n%s", gossip.String())
	}
}

// No scaler at all: the windowed writer says nothing, the all-time gauge says
// zero. Worth pinning because the two writers sit next to each other and are
// easy to "make consistent".
func TestWithoutAScalerTheAllTimeGaugeIsZeroAndTheWindowIsSilent(t *testing.T) {
	e := NewMetricsExporter(MetricsExporterConfig{})

	var recent, gossip bytes.Buffer
	e.writeConnectionEventMetrics(&recent)
	e.writeGossipMetrics(&gossip)

	if recent.Len() != 0 {
		t.Fatalf("windowed rows with no scaler:\n%s", recent.String())
	}
	if !strings.Contains(gossip.String(), "mesh_connection_events_total 0") {
		t.Fatalf("the all-time gauge did not fall back to 0 with no scaler — "+
			"the series vanishes and an absent() rule fires on a node that is "+
			"merely idle:\n%s", gossip.String())
	}
}
