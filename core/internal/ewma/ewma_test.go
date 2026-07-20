/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package ewma

import (
	"math"
	"sync"
	"testing"
)

func TestEWMA_SeedAndValue(t *testing.T) {
	e := New(0.5, 10)
	if got := e.Value(); got != 10 {
		t.Fatalf("seed: got %v, want 10", got)
	}
	e.Reset(3)
	if got := e.Value(); got != 3 {
		t.Fatalf("reset: got %v, want 3", got)
	}
}

func TestEWMA_UpdateConverges(t *testing.T) {
	// Feeding a constant sample must converge monotonically toward it.
	e := New(0.3, 0)
	const target = 100.0
	prev := e.Value()
	for i := 0; i < 200; i++ {
		e.Update(target)
		v := e.Value()
		if v < prev-1e-9 {
			t.Fatalf("EWMA moved away from target at step %d: %v -> %v", i, prev, v)
		}
		prev = v
	}
	if math.Abs(e.Value()-target) > 0.01 {
		t.Fatalf("did not converge: got %v, want ~%v", e.Value(), target)
	}
}

func TestEWMA_FirstUpdateMatchesFormula(t *testing.T) {
	// One update from a known seed equals alpha*sample + (1-alpha)*old.
	e := New(0.25, 4)
	e.Update(8)
	want := 0.25*8 + 0.75*4 // = 5
	if got := e.Value(); math.Abs(got-want) > 1e-12 {
		t.Fatalf("formula: got %v, want %v", got, want)
	}
}

// TestEWMA_ConcurrentUpdate is the race-detector guard: N goroutines hammer
// Update concurrently; the CAS loop must never panic and must leave the
// value bounded within the sample range (no corruption from torn writes).
// Run with -race.
func TestEWMA_ConcurrentUpdate(t *testing.T) {
	e := New(0.1, 50)
	const goroutines, iters = 16, 5000
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		// Half push toward 0, half toward 100 — value must stay in [0,100].
		sample := 0.0
		if g%2 == 0 {
			sample = 100.0
		}
		go func(s float64) {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				e.Update(s)
			}
		}(sample)
	}
	wg.Wait()
	if v := e.Value(); v < 0 || v > 100 || math.IsNaN(v) {
		t.Fatalf("concurrent update corrupted the value: %v", v)
	}
}
