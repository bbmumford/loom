/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"bytes"
	"log"
	"strings"
	"sync"
	"testing"
	"time"
)

// Covers the mesh concurrency helpers safeGo and safeGoCh. safeGo wraps the
// RPC server's exported accept loop; safeGoCh wraps the forwarder's parallel
// probe fan-out.
//
// safeGo is the only thing between a panicking RPC handler and process death on
// an exported accept loop; safeGoCh is what stops a fan-out collector blocking
// forever on a result that exploded. Each fails silently, and each failure is an
// outage.

// ── safeGo ──────────────────────────────────────────────────────────────────

// 🔴 safeGo IS THE LAST THING BEFORE PROCESS DEATH. rpc.go:820 wraps the
// exported accept loop with it: an unrecovered panic in a handler goroutine
// takes down the whole node, not just the request.
func TestSafeGoContainsAPanicAndKeepsTheProcessAlive(t *testing.T) {
	var buf lockedBuffer
	old := log.Writer()
	log.SetOutput(&buf)
	defer log.SetOutput(old)

	deferRan := make(chan struct{})
	safeGo("unit-test-label", func() {
		defer close(deferRan) // fn's OWN defers must still run
		panic("boom")
	})

	select {
	case <-deferRan:
	case <-time.After(2 * time.Second):
		t.Fatal("the panicking function's own defer never ran — recovery is " +
			"happening before fn's defers, so callers cannot release resources")
	}

	// The recovery log is the only trace a panicked goroutine leaves; without the
	// label an operator cannot tell WHICH goroutine died.
	waitForLog(t, &buf, "unit-test-label", "the panic was contained but the label "+
		"never reached the log — a recovered panic with no identity is invisible")
}

// A non-panicking function must run normally and log nothing — otherwise the
// test above passes against a helper that logs unconditionally.
func TestSafeGoRunsANormalFunctionWithoutLogging(t *testing.T) {
	var buf lockedBuffer
	old := log.Writer()
	log.SetOutput(&buf)
	defer log.SetOutput(old)

	ran := make(chan struct{})
	safeGo("quiet", func() { close(ran) })

	select {
	case <-ran:
	case <-time.After(2 * time.Second):
		t.Fatal("safeGo never ran the function at all")
	}
	if strings.Contains(buf.String(), "panic") {
		t.Fatalf("a clean run logged a panic: %q", buf.String())
	}
}

// ── safeGoCh ────────────────────────────────────────────────────────────────

type probeResult struct{ err string }

// 🔴 THE FAN-OUT MUST NOT HANG. The forwarder's collector reads exactly one
// result per spawned path; if a panicking probe sends nothing, the collector
// waits for a result that will never arrive.
func TestSafeGoChDeliversItsPanicSentinelToTheCollector(t *testing.T) {
	ch := make(chan probeResult, 1)
	safeGoCh("probe", ch, probeResult{err: "panicked"}, func() {
		panic("probe exploded")
	})

	select {
	case got := <-ch:
		if got.err != "panicked" {
			t.Fatalf("collector received %+v, want the onPanic sentinel", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("a panicking probe sent NOTHING to the result channel — the " +
			"forwarder's collector reads one result per path and would block " +
			"forever on this one")
	}
}

// …and it must NOT send on the success path, or the collector reads a phantom
// extra result and mis-attributes it to another probe.
func TestSafeGoChSendsNothingWhenTheWorkSucceeds(t *testing.T) {
	ch := make(chan probeResult, 1)
	done := make(chan struct{})
	safeGoCh("probe", ch, probeResult{err: "panicked"}, func() { close(done) })

	<-done
	select {
	case got := <-ch:
		t.Fatalf("a SUCCESSFUL probe pushed %+v onto the result channel — the "+
			"sentinel is being sent unconditionally, so the collector counts a "+
			"failure for work that succeeded", got)
	case <-time.After(150 * time.Millisecond):
	}
}

// 🔴 CHARACTERISATION, NOT APPROVAL. The sentinel send is NON-BLOCKING
// (`select { case ch <- onPanic: default: }`), so if the channel has no spare
// capacity the sentinel is SILENTLY DROPPED. The comment calls this
// "best-effort … so a missing receiver doesn't hang the goroutine" — a
// deliberate trade — but it means the anti-hang guarantee holds only for a
// BUFFERED channel with room. This pins which half you get.
// ⚠ FIXTURE NOTE, because my first version of this test was RACY AND WRONG — the
// code was right and the test was not. I used an unbuffered channel and then had
// the test itself sit in `<-ch`: by the time the recover ran there WAS a waiting
// receiver, so the non-blocking send succeeded and the test failed claiming the
// behaviour had changed. "No spare capacity" has to be established by the
// CHANNEL, not by the test's timing.
//
// A pre-filled size-1 buffer is deterministic: the sentinel send has nowhere to
// go no matter when the recover executes, and no receiver is ever waiting.
func TestTheSentinelIsDroppedWhenTheChannelHasNoCapacity(t *testing.T) {
	ch := make(chan probeResult, 1)
	ch <- probeResult{err: "pre-existing"} // buffer is now FULL

	sent := make(chan struct{})
	safeGoCh("probe", ch, probeResult{err: "panicked"}, func() {
		defer close(sent)
		panic("probe exploded")
	})
	<-sent
	time.Sleep(50 * time.Millisecond) // let the recover's non-blocking send resolve

	got := <-ch
	if got.err != "pre-existing" {
		t.Fatalf("first value off the channel is %+v, want the pre-existing one — "+
			"the sentinel displaced it", got)
	}
	select {
	case extra := <-ch:
		t.Fatalf("the sentinel (%+v) was ALSO delivered into a full channel — the "+
			"send is no longer non-blocking. That is very likely an intentional "+
			"change; update this test deliberately and note that the anti-hang "+
			"guarantee now depends on the sender blocking", extra)
	default:
		// Dropped, as documented. The helper protects the GOROUTINE from hanging,
		// NOT the collector from waiting — callers must supply buffer capacity.
		// This is the half of the guarantee that is easy to misread.
	}
}

// lockedBuffer is a concurrency-safe log sink.
//
// ⚠ REQUIRED, and my first version got this wrong: a bare bytes.Buffer is NOT
// safe for concurrent use, and these tests deliberately have a safeGo goroutine
// writing to the log while the test goroutine polls it. `-count=200` passed
// clean; `-race` caught it immediately. Two instruments, two different defect
// classes — repetition finds flaky ASSERTIONS, the race detector finds unsafe
// SHARING, and neither subsumes the other.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// waitForLog polls a log sink until it contains want, so the assertion does not
// race the goroutine's own log write.
func waitForLog(t *testing.T, buf *lockedBuffer, want, msg string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(buf.String(), want) {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("%s (log did not contain %q; got %q)", msg, want, buf.String())
}
