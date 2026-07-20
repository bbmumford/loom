/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package dispatch

import (
	"errors"
	"sync"
	"time"
)

// CircuitState represents the state of a circuit breaker.
type CircuitState int

const (
	CircuitClosed   CircuitState = iota // normal — calls pass through
	CircuitOpen                         // tripped — calls fail fast
	CircuitHalfOpen                     // probing — one call allowed
)

func (s CircuitState) String() string {
	switch s {
	case CircuitClosed:
		return "closed"
	case CircuitOpen:
		return "open"
	case CircuitHalfOpen:
		return "half-open"
	default:
		return "unknown"
	}
}

// ErrCircuitOpen is returned when a call is rejected by an open circuit breaker.
var ErrCircuitOpen = errors.New("circuit breaker open")

// HandlerError wraps an error returned by the remote handler (vs. a
// transport/connectivity failure). The dispatch returns it when every
// probe reports the same application-level error so the bridge can
// classify it as 400 Bad Request rather than 502 Bad Gateway — the
// handler responded, it just didn't accept the input.
//
// Use errors.Is(err, ErrHandlerError) at boundaries (e.g. classifyMeshError).
type handlerErrorType struct{ msg string }

func (h *handlerErrorType) Error() string { return h.msg }
func (h *handlerErrorType) Is(target error) bool {
	_, ok := target.(*handlerErrorType)
	if ok {
		return true
	}
	return target == ErrHandlerError
}

// NewHandlerError wraps a handler-side error message so the bridge can
// classify it as 4xx instead of treating it as a transport failure.
func NewHandlerError(msg string) error { return &handlerErrorType{msg: msg} }

// ErrHandlerError is the sentinel used by errors.Is to detect any
// HandlerError instance regardless of message.
var ErrHandlerError = &handlerErrorType{msg: "handler error"}

// CircuitBreaker implements a per-role circuit breaker.
// Trip: failureThreshold consecutive failures within failureWindow.
// Reset: after resetTimeout, allow one probe (half-open).
// Close: probe succeeds → closed. Probe fails → re-open.
type CircuitBreaker struct {
	mu               sync.Mutex
	state            CircuitState
	failures         int
	failureThreshold int
	failureWindow    time.Duration
	resetTimeout     time.Duration
	lastFailureAt    time.Time
	openedAt         time.Time
}

// NewCircuitBreaker creates a circuit breaker with the given thresholds.
func NewCircuitBreaker(failureThreshold int, failureWindow, resetTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		state:            CircuitClosed,
		failureThreshold: failureThreshold,
		failureWindow:    failureWindow,
		resetTimeout:     resetTimeout,
	}
}

// State returns the current circuit state.
func (cb *CircuitBreaker) State() CircuitState {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state
}

// Allow returns true if a call should be attempted.
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	switch cb.state {
	case CircuitClosed:
		return true
	case CircuitOpen:
		if time.Since(cb.openedAt) > cb.resetTimeout {
			cb.state = CircuitHalfOpen
			return true
		}
		return false
	case CircuitHalfOpen:
		return false // only one probe per half-open period
	default:
		return true
	}
}

// RecordSuccess records a successful call. Resets failure count and closes the circuit.
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.failures = 0
	cb.state = CircuitClosed
}

// RecordFailure records a failed call. Trips the circuit after threshold consecutive failures.
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()
	// Reset failure count if outside the window
	if !cb.lastFailureAt.IsZero() && now.Sub(cb.lastFailureAt) > cb.failureWindow {
		cb.failures = 0
	}
	cb.lastFailureAt = now
	cb.failures++

	if cb.state == CircuitHalfOpen {
		cb.state = CircuitOpen
		cb.openedAt = now
		return
	}

	if cb.failures >= cb.failureThreshold {
		cb.state = CircuitOpen
		cb.openedAt = now
	}
}
