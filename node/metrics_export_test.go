/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ORBTR/aether"
)

// COVERAGE of the Prometheus metrics exporter, 12 functions at 0.0%.
//
// Censused first, and this one is LIVE ON THE DEPLOYMENT SURFACE:
// `MetricsExporter().Handler()` is mounted at /metrics in NINE endpoints
// (bootstrap · monitoring · support · login · …). A panic here takes the
// scrape endpoint down; a malformed line makes a scraper drop the whole
// batch. Neither shows up as a loom test failure.
//
// 🙋 Fifth cost measured rather than estimated: every dependency is optional
// by construction (`nil components produce no metrics for their section`) and
// writeMetrics takes an io.Writer, so the fixture is one struct literal.

// 🔴 THE ONE THAT MATTERS MOST: an exporter with NOTHING wired must still
// serve. Runtime.MetricsExporter() builds from whatever happens to be
// initialised, so a node that scrapes before its subsystems are up hits
// exactly this shape — on a live HTTP path, in nine endpoints.
func TestExporterWithNoDependenciesServesWithoutPanicking(t *testing.T) {
	e := NewMetricsExporter(MetricsExporterConfig{}) // every dep nil

	var buf bytes.Buffer
	e.writeMetrics(&buf) // must not panic

	// The nil reporter is substituted with NilConnectionReporter, so the
	// connection section is present-and-zero rather than absent.
	if buf.Len() == 0 {
		t.Fatal("an exporter with no dependencies produced NO output at all — " +
			"a scraper sees an empty body and records nothing, which is " +
			"indistinguishable from a healthy node with zero connections")
	}
}

func TestHandlerSetsPrometheusContentTypeAndRecordsTheScrape(t *testing.T) {
	e := NewMetricsExporter(MetricsExporterConfig{})
	if !e.LastScrape().IsZero() {
		t.Fatal("premise wrong: LastScrape is set before any scrape")
	}
	before := time.Now()

	rec := httptest.NewRecorder()
	e.Handler()(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	ct := rec.Header().Get("Content-Type")
	if !strings.HasPrefix(ct, "text/plain") || !strings.Contains(ct, "version=0.0.4") {
		t.Fatalf("Content-Type = %q, want the Prometheus exposition type — "+
			"a scraper that content-negotiates will reject or mis-parse the "+
			"body", ct)
	}
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	if e.LastScrape().Before(before) {
		t.Fatal("LastScrape was not advanced by a scrape — the freshness " +
			"signal that tells an operator whether anything is scraping at " +
			"all is dead")
	}
}

// Prometheus exposition is line-oriented and a malformed line makes a scraper
// drop the batch. This pins the shape rather than the values: every non-blank
// line is a comment (# HELP / # TYPE) or a `name value` sample.
func TestExporterOutputIsWellFormedPrometheusExposition(t *testing.T) {
	m := reporterFixtureForMetrics(t)
	e := NewMetricsExporter(MetricsExporterConfig{
		Reporter: NewConnectionReporter(m),
		Budget:   DefaultConnectionBudget(),
	})

	var buf bytes.Buffer
	e.writeMetrics(&buf)
	out := buf.String()
	if out == "" {
		t.Fatal("premise wrong: no output, so the shape assertions below are vacuous")
	}

	sawSample := false
	for i, line := range strings.Split(out, "\n") {
		if line == "" {
			continue
		}
		if strings.HasPrefix(line, "#") {
			if !strings.HasPrefix(line, "# HELP ") && !strings.HasPrefix(line, "# TYPE ") {
				t.Fatalf("line %d is a comment but neither HELP nor TYPE: %q — "+
					"Prometheus rejects unknown comment forms", i+1, line)
			}
			continue
		}
		// A sample line is `metric_name[{labels}] value`.
		if !strings.Contains(line, " ") {
			t.Fatalf("line %d has no value separator: %q", i+1, line)
		}
		sawSample = true
	}
	if !sawSample {
		t.Fatal("output contained only comments and no samples — a scraper " +
			"records nothing from this node")
	}
}

// A connected peer must appear in the connection metrics. Without this the
// exporter can be well-formed and still say the mesh is empty.
func TestConnectionMetricsReflectAConnectedPeer(t *testing.T) {
	m := reporterFixtureForMetrics(t)
	e := NewMetricsExporter(MetricsExporterConfig{Reporter: NewConnectionReporter(m)})

	var buf bytes.Buffer
	e.writeConnectionMetrics(&buf)
	out := buf.String()

	if !strings.Contains(out, "mesh_") {
		t.Fatalf("connection metrics carry no mesh_ series: %q", out)
	}
	// The peer is connected, so at least one connection series must be
	// non-zero — an all-zero body is what an empty mesh looks like.
	nonZero := false
	for _, line := range strings.Split(out, "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if f := strings.Fields(line); len(f) >= 2 && f[len(f)-1] != "0" {
			nonZero = true
			break
		}
	}
	if !nonZero {
		t.Fatalf("every connection sample is 0 with a CONNECTED peer present — "+
			"the dashboard shows an empty mesh while the node is serving:\n%s", out)
	}
}

// reporterFixtureForMetrics reuses the connection-reporter fixture from
// One CONNECTED peer with a live WebSocket session.
func reporterFixtureForMetrics(t *testing.T) *ConnectionManager {
	t.Helper()
	m, _ := reporterFixture(aether.ProtoWebSocket, 5*time.Millisecond, false)
	return m
}
