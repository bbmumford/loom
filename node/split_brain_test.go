/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"bytes"
	"os"
	"regexp"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/bbmumford/loom/internal/debug"
)

// Covers split_brain.go's LamportClock.Merge and PartitionDetector.LogStatus.
//
// The detector itself is covered by partition_detector_test.go — tests over
// ObservePeer, RemovePeer, Detect, silence, eviction, and the leading-edge
// divergence rule. This file reuses that file's `detectorFixture()` and
// `observedAt` helpers and asserts nothing it already asserts.
//
// 🔑 WHY `Merge` IS WORTH TESTING DESPITE HAVING NO CALLERS, which is the
// opposite of the usual argument: its own doc block records that nothing calls
// it and nothing should (swarm.HLC.Observe is this estate's merging clock), and
// that it is retained because it is EXPORTED ON A PUBLISHED MODULE — so an
// in-tree caller count cannot bound its users. That is precisely the
// case where a test suite is the only regression protection an external caller
// has: there is no in-tree consumer whose failure would reveal a break.

// ─── LamportClock.Merge ──────────────────────────────────────────────────

// The documented contract is max(local, remote) + 1. The three orderings of
// (remote, local) are the whole domain, and the zero value must be usable
// because the doc block warns the clock "returns to 0 on restart" — so a
// freshly-constructed clock merging a live remote stamp is the ordinary case
// after a process restart, not an edge case.
func TestMergeAdvancesToMaxOfLocalAndRemotePlusOne(t *testing.T) {
	for _, tc := range []struct {
		name         string
		localTicks   int
		remote, want uint64
	}{
		{"remote behind local", 10, 3, 11},    // local wins: 10 + 1
		{"remote equals local", 10, 10, 11},   // tie: 10 + 1
		{"remote ahead of local", 10, 40, 41}, // remote wins: 40 + 1
		{"zero value clock after restart", 0, 900, 901},
		{"zero value clock, zero remote", 0, 0, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var lc LamportClock
			for i := 0; i < tc.localTicks; i++ {
				lc.Tick()
			}

			if got := lc.Merge(tc.remote); got != tc.want {
				t.Errorf("Merge(%d) with local=%d = %d, want %d — the clock must advance to "+
					"max(local, remote)+1 or causal ordering of LAD records breaks",
					tc.remote, tc.localTicks, got, tc.want)
			}
			if got := lc.Current(); got != tc.want {
				t.Errorf("after Merge the clock reads %d but Merge returned %d — the returned "+
					"value must be the value that was stored", got, tc.want)
			}
		})
	}
}

// 🔬 CONCURRENCY, AND `-race` CANNOT SEE THIS PROPERTY. Merge is a CAS retry
// loop, so it is data-race free by construction and `-race` will stay silent
// whatever the loop does. The property that actually matters is LOGICAL: every
// caller must receive a DISTINCT stamp, because two records sharing a Lamport
// value are unordered and the LAD merge rule keys on it.
//
// The loop guarantees this only because a successful CAS both (a) strictly
// increases the counter and (b) returns exactly the value it stored — so a
// second winner at the same starting value is impossible. Rewriting the loop
// to store and return separately would break uniqueness silently.
func TestConcurrentMergesEachReturnADistinctStamp(t *testing.T) {
	const goroutines, perGoroutine = 8, 200

	var lc LamportClock
	var wg sync.WaitGroup
	results := make([][]uint64, goroutines)

	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			out := make([]uint64, 0, perGoroutine)
			for i := 0; i < perGoroutine; i++ {
				// Vary the remote stamp so both branches of the comparison are
				// exercised concurrently: some merges advance past the counter,
				// others fall back to local+1.
				out = append(out, lc.Merge(uint64(g*i%97)))
			}
			results[g] = out
		}(g)
	}
	wg.Wait()

	seen := make(map[uint64]int, goroutines*perGoroutine)
	for g, out := range results {
		for _, v := range out {
			if prev, dup := seen[v]; dup {
				t.Fatalf("stamp %d was returned to two callers (goroutines %d and %d) — two "+
					"records would carry the same Lamport value and be causally unordered", v, prev, g)
			}
			seen[v] = g
		}
	}
	if got := len(seen); got != goroutines*perGoroutine {
		t.Fatalf("got %d distinct stamps from %d merges", got, goroutines*perGoroutine)
	}
	// Every stamp is <= the final counter, and the counter advanced at least
	// once per merge: the clock never went backwards under contention.
	if final := lc.Current(); final < uint64(goroutines*perGoroutine) {
		t.Errorf("final clock %d < %d merges — the CAS loop lost updates",
			final, goroutines*perGoroutine)
	}
}

// ─── PartitionDetector.LogStatus ─────────────────────────────────────────

// captureDebug redirects the debug logger to a buffer for the duration of a
// test and restores stderr afterwards. debug.Configure is process-global, so
// tests using it must not run in parallel with each other.
//
// 📌 MEASURED while writing this: debug.Configure has ZERO callers in the
// estate outside its own doc comment, and internal/debug is not importable
// from outside the module — so the documented "silence everything" control
// (`debug.Configure(false, nil)`) is currently reachable only from tests. Noted,
// not fixed: it is an ops-surface observation, not this file's subject.
func captureDebug(t *testing.T) *syncBuffer {
	t.Helper()
	buf := &syncBuffer{}
	debug.Configure(true, buf)
	t.Cleanup(func() { debug.Configure(false, os.Stderr) })
	return buf
}

// syncBuffer is mutex-guarded because the debug logger may be written by
// background goroutines belonging to other tests in this package.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (s *syncBuffer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *syncBuffer) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

// A healthy mesh must log NOTHING. LogStatus is called on a timer, so an
// unguarded log here is not cosmetic: it is a line per tick per node forever,
// and it trains operators to ignore the split-brain channel — the one channel
// that must be believed the moment it does fire.
func TestLogStatusIsSilentWhenThereIsNoPartition(t *testing.T) {
	buf := captureDebug(t)

	pd := detectorFixture()
	pd.ObservePeer("node-a", 100)
	pd.ObservePeer("node-b", 120)

	if st := pd.Detect(); st.Partitioned {
		t.Fatalf("fixture is already partitioned (%+v) — the silence assertion below would "+
			"pass for the wrong reason", st)
	}
	pd.LogStatus()

	if got := buf.String(); got != "" {
		t.Errorf("a healthy mesh logged %q — LogStatus must return early when "+
			"Partitioned is false", got)
	}
}

// LogStatus's `if !status.Partitioned { return }` guard is equivalent to the
// two length checks that follow it, not redundant with them by accident: a
// healthy mesh has empty DivergedPeers and SilentPeers, so both routes produce
// the same silence. This test drives the guard directly so that equivalence is
// asserted rather than assumed.
//
// Reading Detect settles which of the two it is: EVERY append to either slice is
// paired with `status.Partitioned = true`, so Partitioned==false implies both
// slices are empty and the early return is genuinely equivalent — not merely
// unreached by my fixture.
//
// 🔑 BUT THAT EQUIVALENCE IS INCIDENTAL, AND IT IS LOAD-BEARING IN ONE
// DIRECTION. If a future branch ever appends a diverged or silent peer WITHOUT
// setting Partitioned, the early return stops being redundant and starts
// SUPPRESSING a real partition report — the operator sees nothing during the
// event the detector exists to catch. So the right response to the surviving
// mutant is not another assertion about LogStatus; it is to pin the coupling
// that makes it safe.
func TestPartitionedIsSetWheneverEitherSymptomListIsNonEmpty(t *testing.T) {
	for _, tc := range []struct {
		name  string
		plant func(pd *PartitionDetector)
	}{
		{"a silent peer", func(pd *PartitionDetector) {
			pd.SilenceTimeout = time.Minute
			observedAt(pd, "node-quiet", 100, time.Now().Add(-5*time.Minute))
		}},
		{"a diverged peer", func(pd *PartitionDetector) {
			pd.DivergenceThreshold = 10
			now := time.Now()
			observedAt(pd, "node-leading", 500, now)
			observedAt(pd, "node-lagging", 400, now)
		}},
		{"a healthy mesh", func(pd *PartitionDetector) {
			pd.ObservePeer("node-a", 100)
			pd.ObservePeer("node-b", 120)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pd := detectorFixture()
			tc.plant(pd)

			st := pd.Detect()
			symptom := len(st.SilentPeers) > 0 || len(st.DivergedPeers) > 0

			if st.Partitioned != symptom {
				t.Errorf("Partitioned=%v but silent=%d diverged=%d — LogStatus returns early on "+
					"!Partitioned, so a symptom recorded without the flag is a partition that is "+
					"detected and never reported",
					st.Partitioned, len(st.SilentPeers), len(st.DivergedPeers))
			}
		})
	}
}

var clockLagLine = regexp.MustCompile(`delta=(\d+) \(ref=(\d+) peer=(\d+)`)

// 🔴 THE LOGGED LINE MUST BE SELF-CHECKING: delta == ref - peer.
//
// split_brain.go:278-281 states this as the reason all three numbers are
// printed — `ref=` is the operand delta is measured against, `local=` is
// carried for context and is deliberately NOT part of the arithmetic
// part of it. An operator reading the line is expected to be able to reproduce
// delta from the operands printed beside it.
//
// So this asserts the ARITHMETIC OF THE EMITTED TEXT rather than the struct
// Detect returned: a doc comment promising a self-checking line is only true if
// the format string actually prints those three fields in those three slots.
// Asserting the struct would leave the format string untested, which is exactly
// where the promise can break.
func TestTheLoggedDivergenceLineIsReproducibleFromItsOwnNumbers(t *testing.T) {
	buf := captureDebug(t)

	pd := detectorFixture()
	pd.DivergenceThreshold = 10
	// The local Lamport counter is deliberately set to a value that is neither
	// the reference nor the lag, so a format string that printed local= in the
	// ref= slot would produce arithmetic that does not close.
	for i := 0; i < 7; i++ {
		pd.local.Tick()
	}
	now := time.Now()
	observedAt(pd, "node-leading", 500, now)
	observedAt(pd, "node-lagging", 400, now)

	pd.LogStatus()

	out := buf.String()
	m := clockLagLine.FindStringSubmatch(out)
	if m == nil {
		t.Fatalf("no split-brain clock-lag line matched in output:\n%s", out)
	}
	delta, ref, peer := mustUint(t, m[1]), mustUint(t, m[2]), mustUint(t, m[3])

	if ref-peer != delta {
		t.Errorf("logged line is not self-checking: delta=%d but ref-peer = %d-%d = %d.\n"+
			"An operator cannot reproduce the divergence from the line, which is "+
			"the reason all three operands are printed.\nline: %s",
			delta, ref, peer, ref-peer, out)
	}
	if ref != 500 || peer != 400 || delta != 100 {
		t.Errorf("got ref=%d peer=%d delta=%d, want 500/400/100 — the leading edge is the "+
			"highest live peer clock (500), NOT this node's Lamport counter (7)",
			ref, peer, delta)
	}
}

// Silent peers are reported on their own line even when no peer has diverged:
// silence and divergence are independent partition symptoms, and a mesh whose
// peers have gone quiet without drifting must still be logged.
func TestSilentPeersAreLoggedWithoutAnyDivergence(t *testing.T) {
	buf := captureDebug(t)

	pd := detectorFixture()
	pd.SilenceTimeout = time.Minute
	observedAt(pd, "node-quiet", 100, time.Now().Add(-5*time.Minute))

	pd.LogStatus()

	out := buf.String()
	if !bytes.Contains([]byte(out), []byte("silent peers")) {
		t.Errorf("a peer silent past the timeout was not logged; output:\n%s", out)
	}
	if bytes.Contains([]byte(out), []byte("clock lag")) {
		t.Errorf("a clock-lag line was logged for a mesh with no diverged peer — silence is "+
			"being reported as divergence; output:\n%s", out)
	}
}

func mustUint(t *testing.T, s string) uint64 {
	t.Helper()
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		t.Fatalf("parsing %q from the log line: %v", s, err)
	}
	return v
}
