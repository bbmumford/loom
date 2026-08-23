/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// DrainManager's onClosed callback is ConnectionManager.closeDrainedConnection,
// which releases a budget slot and cancels the peer's gossip. StartDrain leaves
// a monitor goroutine that invokes it after a grace window, so without a Stop
// the callback lands on a manager that shutdown has already finished with —
// up to a full grace period after Shutdown returned.
//
// These tests own the "no work after close" half of the lifecycle. The grace
// window is min(dm.timeout, drainGracePeriod), so the fixtures set a short
// timeout to keep the tests fast rather than sleeping for the real 5s.

func drainFixture(t *testing.T, grace time.Duration) (*DrainManager, *int32) {
	t.Helper()
	var fired int32
	dm := NewDrainManager(func(string, string, time.Time) { atomic.AddInt32(&fired, 1) })
	dm.timeout = grace
	return dm, &fired
}

// 🔴 THE PROPERTY THE MISSING Stop REMOVED. A drain still inside its grace
// window when shutdown begins must abandon the close, not deliver it later.
func TestStopAbandonsADrainStillInsideItsGraceWindow(t *testing.T) {
	dm, fired := drainFixture(t, time.Hour) // long enough that only Stop can end it

	dm.StartDrain("peer-a", "quic", "scale_down")
	dm.Stop()

	if got := atomic.LoadInt32(fired); got != 0 {
		t.Errorf("the close callback fired %d times during Stop — closeDrainedConnection "+
			"releases a budget slot and cancels peer gossip, so it ran against a manager "+
			"that shutdown had already torn down", got)
	}
}

// 🔴 Stop must WAIT, not merely signal. If it returned while a monitor was
// still between its select and its callback, the callback would land after
// Stop returned — which is the same defect the signal was added to prevent,
// just with a narrower window.
//
// 🔬 Asserting "fired == 0" after Stop cannot catch that on its own: a monitor
// that fires a microsecond later still reads 0 at the moment of the check. The
// discriminating step is to re-read after giving a leaked goroutine ample time.
func TestStopWaitsForItsMonitorsRatherThanOnlySignallingThem(t *testing.T) {
	dm, fired := drainFixture(t, time.Hour)
	for i := 0; i < 8; i++ {
		dm.StartDrain("peer-"+string(rune('a'+i)), "quic", "scale_down")
	}

	dm.Stop()
	time.Sleep(50 * time.Millisecond) // a leaked monitor has plenty of room here

	if got := atomic.LoadInt32(fired); got != 0 {
		t.Errorf("%d callbacks arrived after Stop returned — Stop signalled its monitors "+
			"without waiting for them, so shutdown races the close instead of excluding it", got)
	}
	if n := dm.DrainCount(); n != 0 {
		t.Errorf("%d drain entries survived Stop", n)
	}
}

// The normal path must still work: a drain left alone for its grace window
// closes. Without this, "no callback after Stop" is satisfiable by never
// calling the callback at all, which would disable scale-down entirely.
func TestADrainThatElapsesNormallyStillCloses(t *testing.T) {
	dm, fired := drainFixture(t, 10*time.Millisecond)

	dm.StartDrain("peer-a", "quic", "scale_down")

	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadInt32(fired) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := atomic.LoadInt32(fired); got != 1 {
		t.Fatalf("the close callback fired %d times for a drain left to elapse, want 1 — "+
			"suppressing the callback unconditionally would satisfy the Stop tests while "+
			"disabling scale-down", got)
	}
	dm.Stop()
}

// Stop is exported and reachable from shutdown paths that can run twice.
func TestDrainManagerStopIsIdempotentAndConcurrencySafe(t *testing.T) {
	dm, _ := drainFixture(t, time.Hour)
	dm.StartDrain("peer-a", "quic", "scale_down")

	var wg sync.WaitGroup
	var panicked any
	var mu sync.Mutex
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
			dm.Stop()
		}()
	}
	wg.Wait()

	if panicked != nil {
		t.Errorf("concurrent Stop panicked: %v", panicked)
	}
}

// 🔬 THE ONLY ASSERTION THAT MAKES wg.Wait() LOAD-BEARING. Removing it from
// Stop leaves every callback test green: the monitors exit through the stopCh
// branch and never invoke onClosed either way, so callback counts cannot see
// the difference. What Wait actually buys is that no monitor goroutine is still
// running when Stop returns — the "no work after close" half of the lifecycle,
// which is about goroutines rather than about callbacks.
//
// Measured at the instant Stop returns, with no sleep: that is the whole point.
// Without Wait the monitors are still unwinding at that moment and the count
// stays elevated; with it they are all done.
func TestStopReturnsWithNoMonitorGoroutineStillRunning(t *testing.T) {
	const drains = 50
	dm, _ := drainFixture(t, time.Hour)

	base := runtime.NumGoroutine()
	for i := 0; i < drains; i++ {
		dm.StartDrain(fmt.Sprintf("peer-%d", i), "quic", "scale_down")
	}
	if peak := runtime.NumGoroutine(); peak < base+drains/2 {
		t.Fatalf("fixture wrong: %d goroutines after starting %d drains (base %d) — the "+
			"monitors are not running, so this test cannot observe whether Stop waits",
			peak, drains, base)
	}

	dm.Stop()

	if after := runtime.NumGoroutine(); after > base+drains/5 {
		t.Errorf("%d goroutines still live the instant Stop returned (base %d, started "+
			"%d) — Stop signalled its monitors without waiting, so shutdown continues "+
			"while drain monitors are still running", after, base, drains)
	}
}
