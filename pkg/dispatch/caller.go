/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

// Package dispatch provides the shared infrastructure for domain-level RPC dispatch.
//
// Each domain defines a service interface (e.g., PolicyService) with two implementations:
//   - Local: calls the domain handler directly (zero overhead)
//   - Mesh: serializes the request, sends over HWP mesh, deserializes response
//
// The Caller interface abstracts the mesh transport so domain dispatch implementations
// don't depend on HWP/pool internals.
package dispatch

import (
	"context"
	"sync"
)

// Caller sends an RPC request to a mesh node serving the given role.
// The payload is pre-serialized bytes (JSON or proto depending on the domain).
// Returns the response payload bytes and any transport/handler error.
type Caller interface {
	Call(ctx context.Context, role, method string, payload []byte) ([]byte, error)
	Close()
}

// ─── Global Caller Registry ─────────────────────────────────────────────────
// Replaces Library/clients/registry — endpoints call SetCaller() at boot,
// domain dispatch packages call GetCaller() to obtain a Caller.

var (
	globalCaller Caller
	globalDomain string
	globalMu     sync.RWMutex
)

// SetCaller sets the global dispatch caller (HWPCaller or LocalCaller).
func SetCaller(c Caller) {
	globalMu.Lock()
	defer globalMu.Unlock()
	globalCaller = c
}

// GetCaller returns the global dispatch caller.
func GetCaller() Caller {
	globalMu.RLock()
	defer globalMu.RUnlock()
	return globalCaller
}

// SetDomain sets the global domain (e.g. "hstles.com").
func SetDomain(d string) {
	globalMu.Lock()
	defer globalMu.Unlock()
	globalDomain = d
}

// GetDomain returns the global domain.
func GetDomain() string {
	globalMu.RLock()
	defer globalMu.RUnlock()
	return globalDomain
}

// IsInitialized returns true if the global caller and domain are set.
func IsInitialized() bool {
	globalMu.RLock()
	defer globalMu.RUnlock()
	return globalCaller != nil && globalDomain != ""
}
