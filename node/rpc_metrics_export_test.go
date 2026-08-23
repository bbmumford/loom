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

// COVERAGE of writeRPCMetrics, 7.1% — the largest remaining hole in
// the exporter and the widest-published one: /metrics is mounted at 28
// endpoints.
//
// 7.1% is one statement: the `e.rpcServer == nil` early return. Everything
// after it — per-handler calls/errors/forwards, the dedup counters, and the
// active-handler gauge — has never been executed by a test.
//
// 🔑 THE SHAPE THAT MATTERS HERE IS THE ORDER OF THE TWO EARLY RETURNS.
// `len(stats) == 0` means "no handler has been CALLED yet", which is every
// node between boot and its first RPC. That guard sits ABOVE the dedup and
// active-handler writes, so it withholds three series that do not depend on
// handler stats at all. See TestRPCDedupAndActiveHandlersSurviveAnIdleServer.

// idleRPCServer() is a server with a live response cache and metrics registry
// and no traffic — the state every endpoint is in at boot.
func idleRPCServer() *RPCServer { return NewRPCServer(nil) }

// A node with no RPC server has no RPC opinion, and silence is the right
// answer: a zeroed handler row would invent a handler that does not exist.
func TestRPCMetricsEmitNothingWithoutAnRPCServer(t *testing.T) {
	e := NewMetricsExporter(MetricsExporterConfig{})

	var buf bytes.Buffer
	e.writeRPCMetrics(&buf)

	if buf.Len() != 0 {
		t.Fatalf("emitted RPC metrics with no RPC server — every series here "+
			"describes handlers this node does not have:\n%s", buf.String())
	}
}

// 🔴 THE ONE THAT WAS WRONG. An idle server publishes NOTHING today, because
// the `len(stats) == 0` return fires before the dedup and active-handler
// writes. Those three series are meaningful at zero and independent of
// handler stats:
//
//   - mesh_rpc_active_handlers is a SATURATION gauge. Absent, an operator
//     cannot distinguish "idle" from "not scraping" — the exact question the
//     gauge exists to answer.
//   - the dedup counters are counters; a counter that begins existing at its
//     first non-zero value has no baseline, so the first rate() sample after
//     boot is lost.
//
// The node is also at its most interesting here: a node that has served zero
// RPCs since boot is either brand new or broken, and today it looks the same
// as a node with no RPC server at all.
func TestRPCDedupAndActiveHandlersSurviveAnIdleServer(t *testing.T) {
	e := NewMetricsExporter(MetricsExporterConfig{RPCServer: idleRPCServer()})

	var buf bytes.Buffer
	e.writeRPCMetrics(&buf)
	out := buf.String()

	for _, want := range []string{
		"mesh_rpc_active_handlers 0",
		"mesh_rpc_dedup_hits_total 0",
		"mesh_rpc_dedup_misses_total 0",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("an idle RPC server did not publish %q — the series does not "+
				"exist until the first RPC call, so a freshly booted node is "+
				"indistinguishable from one with no RPC server:\n%q", want, out)
		}
	}
	// The per-handler group must be absent ENTIRELY — not just its rows. A
	// bare HELP/TYPE preamble with no samples is inert to a scraper, so this
	// asserts more than Prometheus strictly requires; it is deliberate,
	// because a declared-but-empty series is exactly what a dashboard renders
	// as "this node has handlers and they are all at zero".
	if strings.Contains(out, "mesh_rpc_handler_") {
		t.Fatalf("the per-handler group was declared for a server with no "+
			"traffic — a zero row (or an empty declared series) names a handler "+
			"that has never run on this node:\n%s", out)
	}
}

// The three per-handler counters must be independently correct. They are
// written from one map and it is easy to make them all report `calls`.
func TestRPCPerHandlerCountersAreIndependent(t *testing.T) {
	srv := idleRPCServer()
	m := srv.Metrics()
	// 3 calls, 1 of them failed; plus 2 forwards.
	m.RecordLocal("orbtr.io.dhcp.ListLeases", true, time.Millisecond)
	m.RecordLocal("orbtr.io.dhcp.ListLeases", true, time.Millisecond)
	m.RecordLocal("orbtr.io.dhcp.ListLeases", false, time.Millisecond)
	m.RecordForward("orbtr.io.dhcp.ListLeases", time.Millisecond)
	m.RecordForward("orbtr.io.dhcp.ListLeases", time.Millisecond)

	e := NewMetricsExporter(MetricsExporterConfig{RPCServer: srv})
	var buf bytes.Buffer
	e.writeRPCMetrics(&buf)
	out := buf.String()

	const h = `{handler="orbtr.io.dhcp.ListLeases"}`
	for _, want := range []string{
		"mesh_rpc_handler_calls_total" + h + " 3",
		"mesh_rpc_handler_errors_total" + h + " 1",
		"mesh_rpc_handler_forwards_total" + h + " 2",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing or wrong: %q — an error rate computed from these "+
				"counters is wrong, and error rate is the alert:\n%s", want, out)
		}
	}
}

// RecordForwardFail counts against BOTH forwards and errors. A forward that
// failed is still a forward — dropping it from either total hides a peer that
// is accepting work and failing it.
func TestRPCForwardFailureCountsAsBothAForwardAndAnError(t *testing.T) {
	srv := idleRPCServer()
	srv.Metrics().RecordForwardFail("orbtr.ai.inference.Dispatch", time.Millisecond)

	e := NewMetricsExporter(MetricsExporterConfig{RPCServer: srv})
	var buf bytes.Buffer
	e.writeRPCMetrics(&buf)
	out := buf.String()

	const h = `{handler="orbtr.ai.inference.Dispatch"}`
	for _, want := range []string{
		"mesh_rpc_handler_forwards_total" + h + " 1",
		"mesh_rpc_handler_errors_total" + h + " 1",
		// It was never executed locally, so it is not a call.
		"mesh_rpc_handler_calls_total" + h + " 0",
	} {
		if !strings.Contains(out, want) {
			t.Fatalf("missing or wrong: %q — a failing forward must appear in both "+
				"totals and must not be counted as a local call:\n%s", want, out)
		}
	}
}

// The dedup counters come from a different subsystem (the response cache) than
// the handler stats, and they are written adjacently — a swap is silent.
func TestRPCDedupCountersComeFromTheResponseCache(t *testing.T) {
	srv := idleRPCServer()
	srv.Metrics().RecordLocal("orbtr.io.dhcp.ListLeases", true, time.Millisecond)
	// Two lookups that miss; no hits. Asymmetric on purpose: equal values
	// would let a hits/misses swap pass.
	srv.responseCache.Get("no-such-request")
	srv.responseCache.Get("no-such-request-either")

	e := NewMetricsExporter(MetricsExporterConfig{RPCServer: srv})
	var buf bytes.Buffer
	e.writeRPCMetrics(&buf)
	out := buf.String()

	if !strings.Contains(out, "mesh_rpc_dedup_misses_total 2") {
		t.Fatalf("dedup misses did not reach the exporter as 2:\n%s", out)
	}
	if !strings.Contains(out, "mesh_rpc_dedup_hits_total 0") {
		t.Fatalf("dedup hits is not 0 after two misses — hits and misses are "+
			"crossed, and the dedup ratio an operator reads is inverted:\n%s", out)
	}
}

// Every emitted series needs its HELP/TYPE preamble or a scraper rejects the
// batch. writeRPCMetrics writes three preambles up front and two more later,
// which is exactly the arrangement that loses one in an edit.
func TestRPCMetricsDeclareEverySeriesTheyEmit(t *testing.T) {
	srv := idleRPCServer()
	srv.Metrics().RecordLocal("orbtr.io.dhcp.ListLeases", true, time.Millisecond)

	e := NewMetricsExporter(MetricsExporterConfig{RPCServer: srv})
	var buf bytes.Buffer
	e.writeRPCMetrics(&buf)
	out := buf.String()

	declared := map[string]bool{}
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "# TYPE ") {
			if f := strings.Fields(line); len(f) >= 3 {
				declared[f[2]] = true
			}
		}
	}
	if len(declared) == 0 {
		t.Fatalf("premise wrong: no TYPE lines at all, so the check below is "+
			"vacuous:\n%s", out)
	}
	for _, line := range strings.Split(out, "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		name := strings.Fields(line)[0]
		if i := strings.IndexByte(name, '{'); i >= 0 {
			name = name[:i]
		}
		if !declared[name] {
			t.Fatalf("series %q is emitted with no # TYPE declaration — an "+
				"undeclared metric is dropped by strict scrapers:\n%s", name, out)
		}
	}
}
