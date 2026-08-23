/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"sync"
	"testing"
	"time"
)

// COVERAGE of SelfHealthMonitor.Stop() (self_health.go:201), which was 0.0%, and
// meshHealthEvaluator.Stop() (health_evaluator.go:227).
//
// The same guard is needed here as on SessionHealthMonitor.Stop(). A bare
// `close(stopCh)` panics with "close of closed channel" on a second Stop, which
// a sync.Once prevents. That method's comment records that
// "the sibling LADSnapshotCache.Stop() 200 lines away already guarded itself this
// way" — so at the time of the fix the pattern was already present in TWO
// places and the lane took the third.
//
// Measured now, across every Stop-shaped close in node/:
//
//	SessionHealthMonitor.Stop()  sync.Once          (fixed earlier)
//	LADSnapshotCache.Stop()      sync.Once
//	PeerPublisher.Stop()         select/default     (sequentially safe, racy)
//	SelfHealthMonitor.Stop()     BARE close         ← unguarded, fixed
//	meshHealthEvaluator.Stop()   BARE close         ← unguarded, fixed
//	HealthCheck.Stop()           BARE close         ← unguarded, fixed
//
// The fix reached three siblings and missed three. That is the shape worth
// recording: not "someone forgot a guard" but "a known fix stopped travelling",
// which is why the search for remaining instances has to be exhaustive rather
// than opportunistic.
//
// 🔬 AND MY OWN FIRST SWEEP UNDER-COVERED IT. Searching for `close(*.stopCh)`
// found five of the six; HealthCheck.Stop() names its field `stopChan` and was
// invisible to that pattern. It surfaced only on a second pass keyed on
// `func (…) Stop` bodies instead of on the field name — the same
// search-scope-vs-claim-scope error this lane keeps filing, committed here
// while enumerating instances of a lesson about incomplete enumeration.
//
// ⚠ REACHABILITY, STATED RATHER THAN IMPLIED. Runtime calls selfHealth.Stop()
// at runtime.go:1426 inside shutdown, so a double-Stop is not reachable from
// that path today. The guard is still required: Stop is exported on a published
// module, and "not reachable from the one caller we happen to have" is a
// property of the current callers, not of the method.

// stopTwice runs Stop, then Stop again, and reports what happened.
func stopTwice(t *testing.T, name string, stop func()) {
	t.Helper()
	stop()
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("%s.Stop() panicked on a second call: %v — Stop is exported on a "+
				"published module, so any consumer that stops twice (a retry, a "+
				"defer plus an explicit shutdown) crashes the process", name, r)
		}
	}()
	stop()
}

func TestSelfHealthMonitorStopIsIdempotent(t *testing.T) {
	m := NewSelfHealthMonitor(nil, nil, time.Minute, DefaultSelfHealthMonitorConfig())
	stopTwice(t, "SelfHealthMonitor", m.Stop)
}

func TestMeshHealthEvaluatorStopIsIdempotent(t *testing.T) {
	e := &meshHealthEvaluator{stopCh: make(chan struct{})}
	stopTwice(t, "meshHealthEvaluator", e.Stop)
}

func TestHealthCheckStopIsIdempotent(t *testing.T) {
	h := NewHealthCheck(HealthCheckDeps{}, time.Minute)
	stopTwice(t, "HealthCheck", h.Stop)
}

// 🔬 THE RACE, WHICH THE SEQUENTIAL TEST ABOVE CANNOT SEE. PeerPublisher.Stop()
// guards with a select/default rather than a sync.Once: sequentially that is
// correct, because after the close the `case <-p.stopCh` arm fires. But two
// goroutines can both reach `default` before either closes, and then both
// close.
//
// This is the reason "it has a guard" is not the same question as "the guard
// holds" — and why the check has to be run under -race with real concurrency
// rather than read off the source.
func TestConcurrentStopsAreSafe(t *testing.T) {
	for _, tc := range []struct {
		name string
		stop func() func() // returns a fresh Stop for each run
	}{
		{"SelfHealthMonitor", func() func() {
			return NewSelfHealthMonitor(nil, nil, time.Minute, DefaultSelfHealthMonitorConfig()).Stop
		}},
		{"meshHealthEvaluator", func() func() {
			return (&meshHealthEvaluator{stopCh: make(chan struct{})}).Stop
		}},
		{"HealthCheck", func() func() {
			return NewHealthCheck(HealthCheckDeps{}, time.Minute).Stop
		}},
		// PeerPublisher reached this list with a select/default guard rather than
		// a sync.Once. It passed under a plain run and panicked under -race, which
		// is why a passing concurrency test is "did not reproduce" and never
		// "cannot happen". It is guarded the same way as the others now.
		{"PeerPublisher", func() func() {
			return (&PeerPublisher{stopCh: make(chan struct{})}).Stop
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stop := tc.stop()
			var wg sync.WaitGroup
			var mu sync.Mutex
			var panicked any

			for i := 0; i < 8; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					defer func() {
						if r := recover(); r != nil {
							mu.Lock()
							panicked = r
							mu.Unlock()
						}
					}()
					stop()
				}()
			}
			wg.Wait()

			if panicked != nil {
				t.Errorf("%s.Stop() panicked under concurrent calls: %v — the guard is a "+
					"check-then-act, so two goroutines can both pass it and both close",
					tc.name, panicked)
			}
		})
	}
}
