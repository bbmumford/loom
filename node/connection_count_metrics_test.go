/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"bytes"
	"strings"
	"testing"
)

// COVERAGE of the last uncovered branch in the exporter:
// writeConnectionMetrics' `if ci.ConnCount == 0 { counts[key]++ }`, the only
// statement in metrics_export.go that no test reached.
//
// 🔑 AND IT IS A SIXTH ABSENT-vs-ZERO INSTANCE, POINTING THE OTHER WAY.
// The sibling exporters render an ABSENT input as a value.
// This one renders a ZERO input as ONE — a peer in ActiveConnections() with
// ConnCount == 0 is counted as a single connection rather than none.
//
// That is correct, and the reason is worth stating rather than assuming: a
// peer only appears in ActiveConnections() because it IS connected, so a zero
// ConnCount means "this reporter did not populate the field", never "this peer
// has no connections". Rendering it as 0 would report a connected mesh as
// empty. It is the one place in this file where turning absence into a
// concrete value is the right call — because the surrounding evidence
// (presence in the list) already establishes the answer.

// cannedReporter serves a fixed connection list. It embeds
// NilConnectionReporter so it satisfies the whole interface and only overrides
// the one method under test.
type cannedReporter struct {
	NilConnectionReporter
	conns []ConnectionInfo
}

func (c cannedReporter) ActiveConnections() []ConnectionInfo { return c.conns }
func (c cannedReporter) ConnectedPeerCount() int             { return len(c.conns) }

// 🔴 A CONNECTED PEER MUST NEVER RENDER AS ZERO CONNECTIONS.
func TestPeerWithUnsetConnCountIsCountedAsOneNotZero(t *testing.T) {
	e := NewMetricsExporter(MetricsExporterConfig{
		Reporter: cannedReporter{conns: []ConnectionInfo{{
			PeerNodeID: testNodeIDB, Transport: "websocket", Grade: GradeA,
			// ConnCount deliberately left unset.
		}}},
	})

	var buf bytes.Buffer
	e.writeConnectionMetrics(&buf)
	out := buf.String()

	want := `mesh_connections_total{transport="websocket",grade="A"} 1`
	if !strings.Contains(out, want) {
		t.Fatalf("a CONNECTED peer with an unset ConnCount was not counted as 1 "+
			"— it is in ActiveConnections(), so it is connected; reporting 0 "+
			"shows an empty mesh while the node is serving:\n%s", out)
	}
}

// Peers that DO report a count must be summed, not counted as one each — and
// the two rules must not collide when both kinds share a (transport, grade).
func TestReportedConnCountsAreSummedAndUnsetOnesAddExactlyOne(t *testing.T) {
	e := NewMetricsExporter(MetricsExporterConfig{
		Reporter: cannedReporter{conns: []ConnectionInfo{
			{PeerNodeID: testNodeIDB, Transport: "websocket", Grade: GradeA, ConnCount: 3},
			{PeerNodeID: testNodeIDA, Transport: "websocket", Grade: GradeA}, // unset -> 1
		}},
	})

	var buf bytes.Buffer
	e.writeConnectionMetrics(&buf)
	out := buf.String()

	want := `mesh_connections_total{transport="websocket",grade="A"} 4`
	if !strings.Contains(out, want) {
		t.Fatalf("3 reported connections plus one unset peer did not total 4 — "+
			"either the sum is being overwritten or the unset peer is adding "+
			"the wrong amount:\n%s", out)
	}
}

// Different transports and grades are different series. Collapsing them loses
// exactly the breakdown the metric exists to provide.
func TestConnectionsAreGroupedByTransportAndGrade(t *testing.T) {
	e := NewMetricsExporter(MetricsExporterConfig{
		Reporter: cannedReporter{conns: []ConnectionInfo{
			{PeerNodeID: testNodeIDB, Transport: "websocket", Grade: GradeA, ConnCount: 2},
			{PeerNodeID: testNodeIDA, Transport: "noise-udp", Grade: GradeC, ConnCount: 5},
		}},
	})

	var buf bytes.Buffer
	e.writeConnectionMetrics(&buf)
	out := buf.String()

	for _, want := range []string{
		`mesh_connections_total{transport="websocket",grade="A"} 2`,
		`mesh_connections_total{transport="noise-udp",grade="C"} 5`,
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing series %q — a transport or grade breakdown was "+
				"merged away and an operator cannot see which transport is "+
				"degraded:\n%s", want, out)
		}
	}
}

// The empty case emits a placeholder series rather than nothing, so the metric
// exists from boot. Note the transport/grade labels are the sentinel pair
// "none"/"F" — a real series can never carry transport="none", so the
// placeholder is distinguishable from a genuine measurement.
func TestNoConnectionsStillEmitsThePlaceholderSeries(t *testing.T) {
	e := NewMetricsExporter(MetricsExporterConfig{Reporter: cannedReporter{}})

	var buf bytes.Buffer
	e.writeConnectionMetrics(&buf)
	out := buf.String()

	if !strings.Contains(out, `mesh_connections_total{transport="none",grade="F"} 0`) {
		t.Fatalf("an idle node emitted no placeholder — the series does not "+
			"exist until the first connection, so absent() fires on a healthy "+
			"but quiet node:\n%s", out)
	}
	if !strings.Contains(out, "# TYPE mesh_connections_total gauge") {
		t.Fatalf("the placeholder has no TYPE declaration:\n%s", out)
	}
}
