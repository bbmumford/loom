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

// CircuitBreaker is one availability circuit. HWPCaller keeps one per
// capability, destination node and session generation (breakerKey), so a
// failing capability or a dead session opens its own circuit and no other.
//
// Trip: failureThreshold failures within failureWindow. Only what HWPCaller
// classifies as an availability failure is recorded; its own refusals,
// handler rejections and the caller's cancellation are not.
// Reset: after resetTimeout, Allow admits one half-open probe and leases the
// circuit to it. Close: the probe's success closes it; its failure re-opens it.
// A probe that ends with no verdict calls Release, and a lease nobody reports
// on expires after resetTimeout, so a lost probe cannot hold the circuit
// half-open for ever.
type CircuitBreaker struct {
	mu               sync.Mutex
	state            CircuitState
	failures         int
	failureThreshold int
	failureWindow    time.Duration
	resetTimeout     time.Duration
	lastFailureAt    time.Time
	openedAt         time.Time

	// leased is true while a half-open probe holds the circuit; probeAt is
	// when the lease was granted, for its expiry.
	leased  bool
	probeAt time.Time

	// lastUsed is the last Allow, Record or Release, for idle eviction.
	lastUsed time.Time
}

// NewCircuitBreaker creates a circuit breaker with the given thresholds.
func NewCircuitBreaker(failureThreshold int, failureWindow, resetTimeout time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		state:            CircuitClosed,
		failureThreshold: failureThreshold,
		failureWindow:    failureWindow,
		resetTimeout:     resetTimeout,
		lastUsed:         time.Now(),
	}
}

// State returns the current circuit state.
func (cb *CircuitBreaker) State() CircuitState {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state
}

// Allow returns true if a call should be attempted. In the half-open state it
// returns true to exactly one caller at a time: the one that takes the lease.
func (cb *CircuitBreaker) Allow() bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()
	cb.lastUsed = now
	switch cb.state {
	case CircuitClosed:
		return true
	case CircuitOpen:
		if now.Sub(cb.openedAt) > cb.resetTimeout {
			cb.state = CircuitHalfOpen
			cb.leased = true
			cb.probeAt = now
			return true
		}
		return false
	case CircuitHalfOpen:
		if !cb.leased || now.Sub(cb.probeAt) > cb.resetTimeout {
			cb.leased = true
			cb.probeAt = now
			return true
		}
		return false
	default:
		return true
	}
}

// Release ends a half-open probe's lease without a verdict, so the next Allow
// admits a new probe. It is a no-op in any other state, so a caller that does
// not know whether its call was the probe may always call it.
func (cb *CircuitBreaker) Release() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.lastUsed = time.Now()
	if cb.state == CircuitHalfOpen {
		cb.leased = false
	}
}

// RecordSuccess records a successful call. Resets failure count and closes the circuit.
func (cb *CircuitBreaker) RecordSuccess() {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.lastUsed = time.Now()
	cb.failures = 0
	cb.state = CircuitClosed
	cb.leased = false
}

// RecordFailure records an availability failure. Trips the circuit after
// threshold failures within the window, and re-opens a half-open one.
func (cb *CircuitBreaker) RecordFailure() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	now := time.Now()
	cb.lastUsed = now
	// Reset failure count if outside the window
	if !cb.lastFailureAt.IsZero() && now.Sub(cb.lastFailureAt) > cb.failureWindow {
		cb.failures = 0
	}
	cb.lastFailureAt = now
	cb.failures++

	if cb.state == CircuitHalfOpen {
		cb.state = CircuitOpen
		cb.openedAt = now
		cb.leased = false
		return
	}

	if cb.failures >= cb.failureThreshold {
		cb.state = CircuitOpen
		cb.openedAt = now
	}
}

// idle reports whether the circuit carries no state a fresh one would not:
// closed with no failure inside the window, or untouched for longer than a
// failure window and a reset period together. HWPCaller drops idle circuits on
// its maintenance tick, which bounds its map although every new session
// generation keys new circuits.
func (cb *CircuitBreaker) idle(now time.Time) bool {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	if cb.state == CircuitClosed &&
		(cb.failures == 0 || now.Sub(cb.lastFailureAt) > cb.failureWindow) {
		return true
	}
	return now.Sub(cb.lastUsed) > cb.failureWindow+cb.resetTimeout
}
