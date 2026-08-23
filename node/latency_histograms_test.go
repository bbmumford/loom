/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"testing"
	"time"

	aethermetrics "github.com/ORBTR/aether/metrics"
)

// Covers the latency histogram READERS: `dispatchLatencyRegistry.TopN`,
// `bidiLatencyRegistry.Snapshots`, and the four RPCServer accessors wrapping
// them. Every one is consumed by MeshMetrics:
//
//	DispatchLatencyTopN    <- runtime.go:1889   dispatch_handler_latency_ms_*
//	BidiLatencySnapshots   <- runtime.go:1905   bidirpc_latency_ms_*
//	BidiTimeoutSnapshots   <- runtime.go:1917
//	BidiPhaseSnapshots     <- runtime.go:1931   marshal / send / wait
//
// The recorders that feed these are RecordBidiLatency and RecordBidiTimeout,
// firing from BidiRPC.Call's defer. Asserting on a recorder and on a reader
// separately does not exercise the edge between them, so each test below drives
// a recorder and asserts what the reader publishes:
// recorder → histogram → reader → MeshMetrics key.
//
// Buckets with zero samples are excluded so a freshly-started node does not
// surface placeholder zeros. That distinction is load-bearing: a published p50
// of 0 µs reads as an impossibly fast path rather than as "no data".

func recorded(t *testing.T, d ...time.Duration) *RPCServer {
	t.Helper()
	s := NewRPCServer(nil)
	for _, dur := range d {
		s.RecordBidiLatency("noise-udp", "same-origin", dur)
	}
	return s
}

// ── The zero-sample rule ────────────────────────────────────────────────────

// 🔴 A BUCKET THAT EXISTS BUT HAS NO SAMPLES MUST NOT BE PUBLISHED. Both
// readers create the histogram lazily on first Record, so an empty registry
// must simply produce nothing.
func TestReadersPublishNothingBeforeAnySamples(t *testing.T) {
	s := NewRPCServer(nil)

	if got := s.BidiLatencySnapshots(); len(got) != 0 {
		t.Fatalf("BidiLatencySnapshots = %+v on a fresh server — a freshly "+
			"started node would surface placeholder zeros, and a p50 of 0µs "+
			"reads as an impossibly fast path rather than as 'no data'", got)
	}
	if got := s.BidiTimeoutSnapshots(); len(got) != 0 {
		t.Fatalf("BidiTimeoutSnapshots = %+v on a fresh server", got)
	}
	m, sn, w := s.BidiPhaseSnapshots()
	if len(m)+len(sn)+len(w) != 0 {
		t.Fatalf("BidiPhaseSnapshots = %+v/%+v/%+v on a fresh server", m, sn, w)
	}
	if got := s.DispatchLatencyTopN(20); len(got) != 0 {
		t.Fatalf("DispatchLatencyTopN = %+v on a fresh server", got)
	}
}

// One recorded sample must appear, with its tag split correctly. This is the
// positive half — without it the test above passes against readers that always
// return nothing.
func TestARecordedSampleReachesTheReaderWithItsTagSplit(t *testing.T) {
	s := recorded(t, 5*time.Millisecond)

	got := s.BidiLatencySnapshots()
	if len(got) != 1 {
		t.Fatalf("BidiLatencySnapshots = %d entries after one Record, want 1 — "+
			"the recorder and the reader are not sharing a registry, so every "+
			"OBS-7 key stays absent no matter how much traffic flows", len(got))
	}
	if got[0].Transport != "noise-udp" || got[0].Scope != "same-origin" {
		t.Fatalf("tag = (%q, %q), want (noise-udp, same-origin) — the "+
			"'<transport>|<scope>' key is not being split, so every MeshMetrics "+
			"key is mislabelled", got[0].Transport, got[0].Scope)
	}
	if got[0].Count != 1 {
		t.Fatalf("Count = %d, want 1", got[0].Count)
	}
	if got[0].P50US <= 0 {
		t.Fatalf("P50US = %d for a 5ms sample, want > 0 — the percentile is not "+
			"being read out and every latency key publishes zero", got[0].P50US)
	}
}

// The four registries are SEPARATE. A sample recorded as a timeout must not
// appear in the success histogram: sharing them lets every 30s caller timeout
// inject a 30s sample, which pins the success p99 at the deadline value.
func TestTimeoutSamplesStayOutOfTheSuccessHistogram(t *testing.T) {
	s := NewRPCServer(nil)
	s.RecordBidiTimeout("websocket", "cross-org", 30*time.Second)

	if got := s.BidiLatencySnapshots(); len(got) != 0 {
		t.Fatalf("a TIMEOUT sample appeared in the success histogram (%+v) — "+
			"every caller timeout now pins the success p99 at the 30s "+
			"callerRequestTTL and the histogram is unreadable for real latency",
			got)
	}
	timeouts := s.BidiTimeoutSnapshots()
	if len(timeouts) != 1 || timeouts[0].Transport != "websocket" {
		t.Fatalf("BidiTimeoutSnapshots = %+v, want one websocket entry", timeouts)
	}

	// And the reverse direction: a success must not land in the timeout series.
	s.RecordBidiLatency("websocket", "cross-org", time.Millisecond)
	if got := s.BidiTimeoutSnapshots(); len(got) != 1 {
		t.Fatalf("BidiTimeoutSnapshots = %d after recording a SUCCESS, want 1 — "+
			"the two registries are the same one", len(got))
	}
}

// The three phase registries are also separate, and their order in the return
// tuple is load-bearing: runtime.go:1931 assigns them positionally to the
// marshal / send / wait MeshMetrics keys, so a swap mislabels all three.
func TestThePhaseRegistriesAreDistinctAndReturnedInOrder(t *testing.T) {
	s := NewRPCServer(nil)
	s.RecordBidiPhaseMarshal("noise-udp", "same-origin", 1*time.Millisecond)
	s.RecordBidiPhaseSend("noise-udp", "same-origin", 2*time.Millisecond)
	s.RecordBidiPhaseSend("noise-udp", "same-origin", 2*time.Millisecond)
	s.RecordBidiPhaseWait("noise-udp", "same-origin", 3*time.Millisecond)
	s.RecordBidiPhaseWait("noise-udp", "same-origin", 3*time.Millisecond)
	s.RecordBidiPhaseWait("noise-udp", "same-origin", 3*time.Millisecond)

	marshal, send, wait := s.BidiPhaseSnapshots()
	for name, got := range map[string][]bidiLatencySnapshot{
		"marshal": marshal, "send": send, "wait": wait,
	} {
		if len(got) != 1 {
			t.Fatalf("%s phase returned %d buckets, want 1", name, len(got))
		}
	}
	// Distinct sample counts identify each phase, so a positional swap shows up.
	if marshal[0].Count != 1 || send[0].Count != 2 || wait[0].Count != 3 {
		t.Fatalf("phase counts = marshal %d / send %d / wait %d, want 1/2/3 — the "+
			"return tuple is out of order and runtime.go:1931 assigns them "+
			"positionally, so all three MeshMetrics keys are mislabelled",
			marshal[0].Count, send[0].Count, wait[0].Count)
	}
}

// 🔑 NORMALISATION HAPPENS AT THE WRITER, AND THAT MAKES THE READER'S
// FALLBACK UNREACHABLE.
//
// `Record` replaces an empty transport or scope with "unknown" BEFORE building
// the key, and always joins with "|". So every key the only writer can produce
// contains a separator, and `Snapshots`'s `(unknown, unknown)` initialisers —
// documented as "invalid keys go to (unknown, unknown)" — describe a state no
// caller can reach.
//
// 🙋 My first version of this test tried to reach it through
// `Record("malformed-no-pipe", "", d)` and got ("malformed-no-pipe",
// "unknown"), because Record had already normalised the empty scope and
// inserted the pipe. The branch is defensive redundancy, not a live path — the
// same shape as any guard whose predicate the writer has already ensured. Both
// halves are pinned
// below: the reachable normalisation, and the unreachable fallback reached by
// writing a raw key past the writer.
func TestEmptyTagsAreNormalisedAtTheWriterAndTheReaderFallbackIsUnreachable(t *testing.T) {
	// Reachable: the writer normalises both components.
	s := NewRPCServer(nil)
	s.RecordBidiLatency("", "", 4*time.Millisecond)

	got := s.BidiLatencySnapshots()
	if len(got) != 1 {
		t.Fatalf("an empty-tag sample was dropped (%d entries)", len(got))
	}
	if got[0].Transport != "unknown" || got[0].Scope != "unknown" {
		t.Fatalf("empty tag became (%q, %q), want (unknown, unknown) — a "+
			"MeshMetrics key would carry a blank component",
			got[0].Transport, got[0].Scope)
	}

	// Unreachable through Record: store a separator-free key directly, which is
	// the only way to exercise the reader's fallback. If a future writer ever
	// bypasses Record's normalisation, this is the behaviour it inherits.
	raw := NewRPCServer(nil)
	h := &aethermetrics.DurationHist{}
	h.Record(7 * time.Millisecond)
	raw.bidiLatency.hists.Store("no-separator-at-all", h)

	rawGot := raw.bidiLatency.Snapshots()
	if len(rawGot) != 1 {
		t.Fatalf("a separator-free key produced %d entries, want 1 — the "+
			"samples exist and no key carries them", len(rawGot))
	}
	if rawGot[0].Transport != "unknown" || rawGot[0].Scope != "unknown" {
		t.Fatalf("separator-free key became (%q, %q), want (unknown, unknown)",
			rawGot[0].Transport, rawGot[0].Scope)
	}
}

// ── DispatchLatencyTopN ─────────────────────────────────────────────────────

// TopN sorts by sample count descending and truncates. Busy handlers must come
// first: the list is operator-facing and the cap is what keeps a node with
// hundreds of handlers from publishing hundreds of keys.
func TestDispatchTopNRanksBusyHandlersFirstAndHonoursTheCap(t *testing.T) {
	s := NewRPCServer(nil)
	for i := 0; i < 5; i++ {
		s.dispatchLatency.Record("busy.Handler", time.Millisecond, true)
	}
	for i := 0; i < 2; i++ {
		s.dispatchLatency.Record("medium.Handler", time.Millisecond, true)
	}
	s.dispatchLatency.Record("quiet.Handler", time.Millisecond, true)

	all := s.DispatchLatencyTopN(20)
	if len(all) != 3 {
		t.Fatalf("TopN(20) = %d handlers, want 3", len(all))
	}
	if all[0].Handler != "busy.Handler" || all[2].Handler != "quiet.Handler" {
		t.Fatalf("order = %q, %q, %q — TopN is not sorted by sample count "+
			"descending, so the truncation below keeps the wrong handlers",
			all[0].Handler, all[1].Handler, all[2].Handler)
	}

	capped := s.DispatchLatencyTopN(2)
	if len(capped) != 2 {
		t.Fatalf("TopN(2) = %d handlers, want 2 — the cap is what bounds the "+
			"MeshMetrics key count on a node with many handlers", len(capped))
	}
	if capped[0].Handler != "busy.Handler" || capped[1].Handler != "medium.Handler" {
		t.Fatalf("TopN(2) kept %q and %q — truncation happened before the sort, "+
			"so the busiest handlers are the ones dropped",
			capped[0].Handler, capped[1].Handler)
	}
}

// A non-positive n returns nil rather than everything: the caller asked for
// none, and returning all of them would publish every handler.
func TestDispatchTopNWithANonPositiveLimitReturnsNothing(t *testing.T) {
	s := NewRPCServer(nil)
	s.dispatchLatency.Record("some.Handler", time.Millisecond, true)

	for _, n := range []int{0, -1, -100} {
		if got := s.DispatchLatencyTopN(n); got != nil {
			t.Fatalf("TopN(%d) = %+v, want nil — a caller asking for no entries "+
				"must not receive every handler on the node", n, got)
		}
	}
}

// The registries must not confuse handlers: two handlers' samples stay in
// separate histograms, which is what makes a per-handler p99 meaningful.
func TestEachHandlerKeepsItsOwnHistogram(t *testing.T) {
	s := NewRPCServer(nil)
	s.dispatchLatency.Record("a.Handler", time.Millisecond, true)
	s.dispatchLatency.Record("b.Handler", time.Millisecond, true)
	s.dispatchLatency.Record("b.Handler", time.Millisecond, true)

	byName := map[string]int{}
	for _, snap := range s.DispatchLatencyTopN(20) {
		byName[snap.Handler] = snap.Count
	}
	if byName["a.Handler"] != 1 || byName["b.Handler"] != 2 {
		t.Fatalf("counts = %v, want a=1 b=2 — the samples are landing in one "+
			"shared histogram, so every per-handler percentile is the node's "+
			"aggregate", byName)
	}
}

// 🔴 A BUCKET THAT EXISTS WITH ZERO SAMPLES MUST BE SKIPPED — and reaching
// that state needs the writer bypassed, which is the point.
//
// 🙋 TWO MUTANTS SURVIVED MY FIRST PASS: deleting the `c == 0` guard from
// either reader changed nothing, because my only zero-sample test used a FRESH
// server whose sync.Map is EMPTY — there was no bucket to skip. `Record` calls
// `hist.Record(d)` immediately after LoadOrStore, so every bucket it creates
// has at least one sample and the guard is unreachable through the writer.
//
// ⚠ Which means the guard's stated rationale is slightly off: the doc says it
// exists "so a freshly-started node doesn't surface placeholder zeros", but a
// freshly-started node has an EMPTY map, not zero-sample buckets. The guard is
// real protection against a state only a future writer could create. Pinned
// here by creating that state directly.
func TestAnExistingBucketWithZeroSamplesIsSkippedByBothReaders(t *testing.T) {
	s := NewRPCServer(nil)

	// Store empty histograms past the writer, in both registries.
	s.bidiLatency.hists.Store("noise-udp|same-origin", &aethermetrics.DurationHist{})
	s.dispatchLatency.hists.Store("quiet.Handler", &aethermetrics.DurationHist{})

	if got := s.BidiLatencySnapshots(); len(got) != 0 {
		t.Fatalf("a zero-sample bucket was published (%+v) — its p50 is 0µs, "+
			"which an operator reads as an impossibly fast path rather than as "+
			"'no data'", got)
	}
	if got := s.DispatchLatencyTopN(20); len(got) != 0 {
		t.Fatalf("a zero-sample handler was published (%+v) — it also consumes "+
			"one of the 20 top-N slots that a busy handler should hold", got)
	}

	// Positive control: the same buckets DO appear once they carry a sample, so
	// the assertions above are about the zero-sample case and not about a
	// reader that ignores the map entirely.
	s.RecordBidiLatency("noise-udp", "same-origin", time.Millisecond)
	s.dispatchLatency.Record("quiet.Handler", time.Millisecond, true)
	if len(s.BidiLatencySnapshots()) != 1 || len(s.DispatchLatencyTopN(20)) != 1 {
		t.Fatalf("after recording one sample each: bidi=%d dispatch=%d, want 1/1 "+
			"— the readers are skipping populated buckets too",
			len(s.BidiLatencySnapshots()), len(s.DispatchLatencyTopN(20)))
	}
}
