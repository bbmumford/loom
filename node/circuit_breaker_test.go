/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"errors"
	"testing"
	"time"
)

// COVERAGE of CircuitBreaker (health.go:184-246). Reset (:256) and Failures
// (:266) were both at 0.0%, and the Call state machine had no test of its own.
//
// 🔴 MEASURED FIRST, AND IT CHANGES WHY THIS FILE EXISTS: THE BREAKER IS NEVER
// INVOKED. runtime.go:563 constructs one when cfg.CircuitBreaker.Enabled and
// logs "[NODE] circuit breaker enabled", and runtime.go:1343 exposes it via
// rt.CircuitBreaker. Measured across all three workspace roots (loom, ORBTR,
// HSTLES): ZERO non-test callers of CircuitBreaker.Call, and ZERO references
// to rt.CircuitBreaker. Positive control on the same search shape returns
// the two rt.circuitBreaker sites in runtime.go, so the search works.
//
// An operator who sets Enabled=true gets a log line asserting protection and
// receives none. That is worse than an absent feature: the absent one does not
// claim to be there. Same family as the takeover engine — wired to
// config, never called — which is now the second instance this lane has
// measured, in a different subsystem.
//
// ⚠ SO WHAT DO THESE TESTS PROVE? The MECHANISM, not the protection. Nothing
// here makes the breaker run in a deployment, and deciding which calls it
// should wrap is a production-authority question for @R/DESIGN, not this
// lane's. What this file does is make the semantics known BEFORE someone wires
// it — because one of them (see the accumulation test) is not what the name
// suggests, and discovering that after wiring means discovering it in prod.

var errBoom = errors.New("boom")

// fail/succeed keep the call-counting explicit at each use site.
func breakerFixture(threshold int, resetTime time.Duration) *CircuitBreaker {
	return NewCircuitBreaker(threshold, resetTime)
}

// The basic contract: the breaker opens on the threshold-th failure, not
// before. Opening early costs availability; opening late defeats the purpose.
func TestTheBreakerOpensExactlyOnTheThresholdFailure(t *testing.T) {
	cb := breakerFixture(3, time.Minute)

	for i := 1; i <= 2; i++ {
		_ = cb.Call(func() error { return errBoom })
		if got := cb.State(); got != CircuitBreakerClosed {
			t.Fatalf("after %d of 3 failures the breaker is %q, want closed — it opens "+
				"early, so a transient blip costs availability", i, got)
		}
	}

	_ = cb.Call(func() error { return errBoom })

	if got := cb.State(); got != CircuitBreakerOpen {
		t.Errorf("after 3 of 3 failures the breaker is %q, want open — the threshold "+
			"does not trip, so the breaker never protects anything", got)
	}
	if got := cb.Failures(); got != 3 {
		t.Errorf("Failures = %d, want 3", got)
	}
}

// 🔴 THE POINT OF THE WHOLE MECHANISM: an open breaker must NOT run the
// function. Returning an error while still calling through would leave the
// failing dependency under full load — the breaker would be pure overhead
// while appearing to work, and every test that only checked the error would
// pass.
func TestAnOpenBreakerDoesNotInvokeTheProtectedFunction(t *testing.T) {
	cb := breakerFixture(1, time.Minute)
	_ = cb.Call(func() error { return errBoom }) // trips it

	calls := 0
	err := cb.Call(func() error { calls++; return nil })

	if calls != 0 {
		t.Errorf("the protected function ran %d times while the breaker was open — the "+
			"failing dependency stays under full load and the breaker is pure overhead", calls)
	}
	if err == nil {
		t.Error("an open breaker returned a nil error, so the caller proceeds as though " +
			"the call succeeded")
	}
}

// After the reset window an open breaker must admit ONE trial call, and a
// success on that trial must close it and clear the count. Without this a
// breaker that opens once never recovers without a process restart.
func TestTheResetWindowAdmitsATrialCallAndASuccessCloses(t *testing.T) {
	cb := breakerFixture(1, time.Minute)
	_ = cb.Call(func() error { return errBoom })

	// Age the failure past the reset window rather than sleeping for it.
	cb.mu.Lock()
	cb.lastFailure = time.Now().Add(-2 * time.Minute)
	cb.mu.Unlock()

	calls := 0
	if err := cb.Call(func() error { calls++; return nil }); err != nil {
		t.Fatalf("the trial call returned %v, want nil", err)
	}
	if calls != 1 {
		t.Fatalf("the trial function ran %d times, want 1 — the breaker never leaves the "+
			"open state, so it can only be recovered by a restart", calls)
	}
	if got := cb.State(); got != CircuitBreakerClosed {
		t.Errorf("state after a successful trial = %q, want closed", got)
	}
	if got := cb.Failures(); got != 0 {
		t.Errorf("Failures = %d after recovery, want 0 — the old count survives, so the "+
			"next single failure re-opens immediately", got)
	}
}

// The other half of the trial: a failure during half-open must re-open, not
// close. Both directions matter, and a test of only one cannot tell a working
// transition from an unconditional one.
func TestAFailedTrialCallReopensTheBreaker(t *testing.T) {
	cb := breakerFixture(1, time.Minute)
	_ = cb.Call(func() error { return errBoom })
	cb.mu.Lock()
	cb.lastFailure = time.Now().Add(-2 * time.Minute)
	cb.mu.Unlock()

	_ = cb.Call(func() error { return errBoom })

	if got := cb.State(); got != CircuitBreakerOpen {
		t.Errorf("state after a FAILED trial = %q, want open — a still-broken dependency "+
			"is admitted back into service", got)
	}
}

// 🔴 CHARACTERISATION, NOT APPROVAL — the semantics most likely to surprise
// whoever wires this, and the reason to record them BEFORE it is wired.
//
// In the CLOSED state, cb.failures only ever increments. Nothing decays it:
// the sole reset paths are the half-open→closed recovery (health.go:239-243)
// and the manual Reset. So the count is CUMULATIVE-FOR-ALL-TIME, not
// consecutive and not windowed.
//
// The consequence for a long-lived breaker: threshold 3, one failure a day,
// thousands of successes in between — it opens on day three and blames "3
// failures". Every name and message here reads as though it meant consecutive
// failures ("Circuit breaker: OPEN (failures: %d)"), which is what most
// circuit breakers count.
//
// 🔬 The fixture is anti-correlated on purpose: the successes are interleaved
// BETWEEN the failures. A test that ran the failures consecutively would pass
// identically under both semantics and could not tell them apart.
//
// Routed as a question, not a change: whether this should count consecutive
// failures, decay over a window, or stay cumulative is a design decision for
// @R/DESIGN. This test pins today's answer so any change is deliberate — the
// same treatment gave the mapProtocol key collapse.
func TestFailuresAreCumulativeForAllTimeAndSuccessesDoNotDecayThem(t *testing.T) {
	cb := breakerFixture(3, time.Minute)

	for i := 0; i < 2; i++ {
		_ = cb.Call(func() error { return errBoom })
		for j := 0; j < 50; j++ {
			if err := cb.Call(func() error { return nil }); err != nil {
				t.Fatalf("a success returned %v while the breaker was closed", err)
			}
		}
	}

	if got := cb.State(); got != CircuitBreakerClosed {
		t.Fatalf("fixture wrong: breaker already %q before the third failure", got)
	}
	if got := cb.Failures(); got != 2 {
		t.Fatalf("Failures = %d after 2 failures and 100 successes, want 2 — if this is "+
			"0 the breaker now decays on success, which is very likely the FIX; update "+
			"this test deliberately and see the routing note above", got)
	}

	_ = cb.Call(func() error { return errBoom })

	if got := cb.State(); got != CircuitBreakerOpen {
		t.Fatalf("premise wrong: the third failure did not open the breaker (%q)", got)
	}
	t.Logf("pinned: 3 failures separated by 100 successes opened the breaker — the count "+
		"is cumulative for the process lifetime, not consecutive (Failures=%d)",
		cb.Failures())
}

// Reset is the only operator-facing escape hatch, and it has no caller today.
// It must both close the breaker AND clear the count — closing without
// clearing would re-open on the very next failure, which looks identical to
// the reset having done nothing.
func TestResetBothClosesTheBreakerAndClearsTheCount(t *testing.T) {
	cb := breakerFixture(2, time.Minute)
	_ = cb.Call(func() error { return errBoom })
	_ = cb.Call(func() error { return errBoom })
	if cb.State() != CircuitBreakerOpen {
		t.Fatal("fixture wrong: the breaker is not open, so Reset has nothing to undo")
	}

	cb.Reset()

	if got := cb.State(); got != CircuitBreakerClosed {
		t.Errorf("state after Reset = %q, want closed", got)
	}
	if got := cb.Failures(); got != 0 {
		t.Errorf("Failures = %d after Reset, want 0 — the breaker is closed but primed "+
			"to re-open on the next single failure, so the reset looks like a no-op to "+
			"an operator", got)
	}

	// And the reset must actually admit traffic again.
	calls := 0
	if err := cb.Call(func() error { calls++; return nil }); err != nil || calls != 1 {
		t.Errorf("after Reset the protected function ran %d times (err=%v), want 1 and nil",
			calls, err)
	}
}

// A closed breaker must pass the function's own error through unchanged —
// callers distinguish "the dependency failed" from "the breaker refused" by
// the error, and swallowing or wrapping it silently loses that distinction.
func TestAClosedBreakerReturnsTheFunctionsOwnError(t *testing.T) {
	cb := breakerFixture(99, time.Minute)

	err := cb.Call(func() error { return errBoom })

	if !errors.Is(err, errBoom) {
		t.Errorf("Call returned %v, want the function's own error — the caller cannot "+
			"tell a dependency failure from a breaker rejection", err)
	}
}
