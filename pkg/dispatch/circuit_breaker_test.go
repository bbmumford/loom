/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package dispatch

import (
	"testing"
	"time"
)

func TestCircuitBreaker_StartsClosedAllowsCalls(t *testing.T) {
	cb := NewCircuitBreaker(3, 30*time.Second, 10*time.Second)
	if !cb.Allow() {
		t.Fatal("expected Allow()=true on fresh breaker")
	}
	if cb.State() != CircuitClosed {
		t.Fatalf("expected CircuitClosed, got %v", cb.State())
	}
}

func TestCircuitBreaker_TripsAfterThreshold(t *testing.T) {
	cb := NewCircuitBreaker(3, 30*time.Second, 10*time.Second)
	cb.RecordFailure()
	cb.RecordFailure()
	if !cb.Allow() {
		t.Fatal("should still allow after 2 failures")
	}
	cb.RecordFailure() // 3rd failure → trips
	if cb.Allow() {
		t.Fatal("should NOT allow after 3 failures")
	}
	if cb.State() != CircuitOpen {
		t.Fatalf("expected CircuitOpen, got %v", cb.State())
	}
}

func TestCircuitBreaker_SuccessResets(t *testing.T) {
	cb := NewCircuitBreaker(3, 30*time.Second, 10*time.Second)
	cb.RecordFailure()
	cb.RecordFailure()
	cb.RecordSuccess()
	if cb.failures != 0 {
		t.Fatalf("expected failures=0 after success, got %d", cb.failures)
	}
}

func TestCircuitBreaker_HalfOpenAfterReset(t *testing.T) {
	cb := NewCircuitBreaker(3, 30*time.Second, 50*time.Millisecond)
	cb.RecordFailure()
	cb.RecordFailure()
	cb.RecordFailure()
	time.Sleep(60 * time.Millisecond)
	if !cb.Allow() {
		t.Fatal("should allow one probe after reset timeout")
	}
	if cb.State() != CircuitHalfOpen {
		t.Fatalf("expected CircuitHalfOpen, got %v", cb.State())
	}
	if cb.Allow() {
		t.Fatal("should NOT allow second call during half-open")
	}
}

func TestCircuitBreaker_HalfOpenSuccessCloses(t *testing.T) {
	cb := NewCircuitBreaker(3, 30*time.Second, 50*time.Millisecond)
	cb.RecordFailure()
	cb.RecordFailure()
	cb.RecordFailure()
	time.Sleep(60 * time.Millisecond)
	cb.Allow()
	cb.RecordSuccess()
	if cb.State() != CircuitClosed {
		t.Fatalf("expected CircuitClosed after half-open success, got %v", cb.State())
	}
}

func TestCircuitBreaker_HalfOpenFailureReopens(t *testing.T) {
	cb := NewCircuitBreaker(3, 30*time.Second, 50*time.Millisecond)
	cb.RecordFailure()
	cb.RecordFailure()
	cb.RecordFailure()
	time.Sleep(60 * time.Millisecond)
	cb.Allow()
	cb.RecordFailure()
	if cb.State() != CircuitOpen {
		t.Fatalf("expected CircuitOpen after half-open failure, got %v", cb.State())
	}
}

func TestCircuitBreaker_FailureWindowResets(t *testing.T) {
	// threshold=3, window=50ms — failures outside the window don't accumulate
	cb := NewCircuitBreaker(3, 50*time.Millisecond, 10*time.Second)
	cb.RecordFailure()
	cb.RecordFailure()
	// 2 failures, wait for window to expire
	time.Sleep(60 * time.Millisecond)
	// This failure should reset the counter (outside window), then count as 1
	cb.RecordFailure()
	if cb.State() != CircuitClosed {
		t.Fatalf("expected CircuitClosed (window reset), got %v", cb.State())
	}
	// Need 2 more to trip
	cb.RecordFailure()
	if cb.State() != CircuitClosed {
		t.Fatalf("expected still closed after 2, got %v", cb.State())
	}
	cb.RecordFailure()
	if cb.State() != CircuitOpen {
		t.Fatalf("expected CircuitOpen after 3 in new window, got %v", cb.State())
	}
}

func TestCircuitBreaker_String(t *testing.T) {
	if CircuitClosed.String() != "closed" {
		t.Fatalf("got %s", CircuitClosed.String())
	}
	if CircuitOpen.String() != "open" {
		t.Fatalf("got %s", CircuitOpen.String())
	}
	if CircuitHalfOpen.String() != "half-open" {
		t.Fatalf("got %s", CircuitHalfOpen.String())
	}
}
