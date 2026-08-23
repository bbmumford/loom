/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	lad "github.com/bbmumford/ledger"
	"github.com/bbmumford/loom/pkg/obshealth"
	"github.com/hashicorp/go-multierror"
)

// HealthCheckDeps holds dependencies needed for health checks
type HealthCheckDeps struct {
	Ledger    lad.Ledger
	IsLeader  func() bool
	GetLeader func() (string, error)
	GetPeers  func() ([]string, error)

	// Registry is the platform-wide subsystem health registry. When
	// non-nil, performHealthCheck marks / clears the mesh.raft and
	// mesh.ledger subsystems based on the per-check outcome so
	// downstream observers (meshmon, /api/monitoring/subsystems,
	// mesh_subsystem_degraded gauge) can see per-subsystem failure
	// modes instead of a single connected/disconnected bit.
	//
	// Wiring is optional so existing endpoints continue to work with a
	// nil registry — they simply lose the per-subsystem gradient signal
	// until an operator supplies a Registry. Mark / Clear are
	// namespace-bounded (fail-closed on unknown identifiers) so this
	// hook cannot accidentally grow the registry map.
	Registry *health.Registry
}

// HealthCheck performs comprehensive health monitoring
type HealthCheck struct {
	deps       HealthCheckDeps
	interval   time.Duration
	stopChan   chan struct{}
	stopOnce   sync.Once // Stop is exported; a second close would panic
	mu         sync.Mutex
	lastResult error
}

// NewHealthCheck creates a new health check monitor
func NewHealthCheck(deps HealthCheckDeps, interval time.Duration) *HealthCheck {
	return &HealthCheck{
		deps:     deps,
		interval: interval,
		stopChan: make(chan struct{}),
	}
}

// Start begins periodic health checks
func (h *HealthCheck) Start() {
	go func() {
		ticker := time.NewTicker(h.interval)
		defer ticker.Stop()

		for {
			select {
			case <-h.stopChan:
				log.Println("[HEALTH] Health check stopped")
				return
			case <-ticker.C:
				h.performHealthCheck()
			}
		}
	}()
	log.Printf("[HEALTH] Health check started (interval: %v)", h.interval)
}

// Stop terminates health checks. Idempotent.
//
// 🔴 The sync.Once is load-bearing — a bare close(h.stopChan) panics with
// "close of closed channel" on a second call. See the note on
// SelfHealthMonitor.Stop; this type names its field stopChan, not stopCh.
func (h *HealthCheck) Stop() {
	h.stopOnce.Do(func() {
		close(h.stopChan)
	})
}

// LastResult returns the result of the most recent health check
func (h *HealthCheck) LastResult() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.lastResult
}

// IsHealthy returns true if the last health check passed
func (h *HealthCheck) IsHealthy() bool {
	return h.LastResult() == nil
}

func (h *HealthCheck) performHealthCheck() {
	// Capture check start time BEFORE probing subsystems so the
	// Registry monotonic gate (see registry.Mark/Clear) sees a
	// deterministic timestamp per pass — a Mark or Clear stamped
	// mid-check would race with a concurrent SelfHealthMonitor pass
	// and could be dropped by the gate.
	at := time.Now()

	var result error
	var raftErr error
	raftChecked := h.deps.GetLeader != nil

	// Check Raft cluster health
	if raftChecked {
		if !h.deps.IsLeader() {
			leader, err := h.deps.GetLeader()
			if err != nil {
				raftErr = fmt.Errorf("raft: no leader elected: %w", err)
				result = multierror.Append(result, raftErr)
			} else {
				dbgNodeHealth.Printf("Raft follower (leader: %s)", leader)
			}
		} else {
			peers, err := h.deps.GetPeers()
			if err != nil {
				raftErr = fmt.Errorf("raft: cannot get peers: %w", err)
				result = multierror.Append(result, raftErr)
			} else {
				dbgNodeHealth.Printf("Raft leader (peers: %d)", len(peers))
			}
		}
	}

	// Check VL1 mesh connectivity (basic ping to ledger)
	var ledgerErr error
	ledgerChecked := h.deps.Ledger != nil
	if ledgerChecked {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		_, err := h.deps.Ledger.Head(ctx)
		if err != nil {
			ledgerErr = fmt.Errorf("lad: ledger unreachable: %w", err)
			result = multierror.Append(result, ledgerErr)
		}
	}

	// Reflect per-subsystem outcome into the platform-wide health
	// Registry. Only checked subsystems are touched — an unwired dep
	// (nil GetLeader, nil Ledger) is treated as "not observed" and
	// leaves the registry entry alone, so a health-check that intentionally
	// skips a subsystem cannot flap it back to healthy.
	if h.deps.Registry != nil {
		if raftChecked {
			if raftErr != nil {
				_ = h.deps.Registry.Mark(health.SubsystemMeshRaft, at, raftErr)
			} else {
				_ = h.deps.Registry.Clear(health.SubsystemMeshRaft, at)
			}
		}
		if ledgerChecked {
			if ledgerErr != nil {
				_ = h.deps.Registry.Mark(health.SubsystemMeshLedger, at, ledgerErr)
			} else {
				_ = h.deps.Registry.Clear(health.SubsystemMeshLedger, at)
			}
		}
	}

	// Store result
	h.mu.Lock()
	h.lastResult = result
	h.mu.Unlock()

	if result != nil {
		log.Printf("[HEALTH] Health check FAILED: %v", result)
	}
}

// CircuitBreakerState represents the state of a circuit breaker
type CircuitBreakerState string

const (
	CircuitBreakerClosed   CircuitBreakerState = "closed"
	CircuitBreakerOpen     CircuitBreakerState = "open"
	CircuitBreakerHalfOpen CircuitBreakerState = "half-open"
)

// CircuitBreaker protects against cascading failures
type CircuitBreaker struct {
	threshold   int
	failures    int
	lastFailure time.Time
	state       CircuitBreakerState
	resetTime   time.Duration
	mu          sync.Mutex
}

// NewCircuitBreaker creates a new circuit breaker
func NewCircuitBreaker(threshold int, resetTime time.Duration) *CircuitBreaker {
	return &CircuitBreaker{
		threshold: threshold,
		failures:  0,
		state:     CircuitBreakerClosed,
		resetTime: resetTime,
	}
}

// Call executes the given function with circuit breaker protection
func (cb *CircuitBreaker) Call(fn func() error) error {
	cb.mu.Lock()

	// Check if circuit is open
	if cb.state == CircuitBreakerOpen {
		if time.Since(cb.lastFailure) > cb.resetTime {
			cb.state = CircuitBreakerHalfOpen
			log.Println("[CIRCUIT] Circuit breaker: transitioning to half-open")
		} else {
			cb.mu.Unlock()
			return fmt.Errorf("circuit breaker open (failures: %d, threshold: %d)", cb.failures, cb.threshold)
		}
	}

	cb.mu.Unlock()

	// Execute function
	err := fn()

	cb.mu.Lock()
	defer cb.mu.Unlock()

	if err != nil {
		cb.failures++
		cb.lastFailure = time.Now()

		if cb.failures >= cb.threshold {
			cb.state = CircuitBreakerOpen
			log.Printf("[CIRCUIT] Circuit breaker: OPEN (failures: %d)", cb.failures)
		}

		return err
	}

	// Success - reset if half-open
	if cb.state == CircuitBreakerHalfOpen {
		cb.state = CircuitBreakerClosed
		cb.failures = 0
		log.Println("[CIRCUIT] Circuit breaker: closed (recovered)")
	}

	return nil
}

// State returns the current state of the circuit breaker
func (cb *CircuitBreaker) State() CircuitBreakerState {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.state
}

// Reset manually resets the circuit breaker to closed state
func (cb *CircuitBreaker) Reset() {
	cb.mu.Lock()
	defer cb.mu.Unlock()

	cb.state = CircuitBreakerClosed
	cb.failures = 0
	log.Println("[CIRCUIT] Circuit breaker: manually reset")
}

// Failures returns the current failure count
func (cb *CircuitBreaker) Failures() int {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	return cb.failures
}
