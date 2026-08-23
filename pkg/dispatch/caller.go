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
	"errors"
	"sync"
)

// Caller sends an RPC request to a mesh node serving the given role.
// The payload is pre-serialized bytes (JSON or proto depending on the domain).
// Returns the response payload bytes and any transport/handler error.
type Caller interface {
	Call(ctx context.Context, role, method string, payload []byte) ([]byte, error)
	Close()
}

// ErrNoRouteToNode is returned by TargetedCaller.CallNode when the named node
// cannot be reached. It is deliberately an ERROR and never a fallback: the
// whole point of a targeted call is that reaching some OTHER node serving the
// same role is a wrong answer, not a degraded one.
var ErrNoRouteToNode = errors.New("dispatch: no route to target node")

// TargetedCaller is an OPTIONAL capability a Caller may also implement: dispatch
// to one NAMED node rather than to whichever node happens to serve a role.
//
// Why optional rather than a method on Caller: adding to Caller would break every
// implementor. loom already uses optional-capability type assertion in this exact
// shape elsewhere — ports.ScopeStamper on the auth validator (node/rpc.go), and
// aether.TicketCapable / RelayCapable / TenantAwareSession / ConnProvider on
// transports and sessions. A caller that does not implement TargetedCaller simply
// cannot be targeted, and the rpc layer reports that rather than silently
// downgrading to role dispatch.
//
// CONTRACT — this is the part that matters, and it is the opposite of the
// role-addressed path's:
//
//   - Reaching nodeID, or an error. Never another node.
//   - If nodeID is unreachable, return ErrNoRouteToNode. Do NOT fall back to
//     role resolution, and do NOT return a response from a different peer.
//
// That constraint exists because the underlying transport primitive
// (SessionFinder.CallViaBidi) reports "no bidi channel for this node" as an
// ordinary (nil, false, nil) — a signal its existing callers correctly treat as
// "take the untargeted arm". For placement-bound dispatch that fallback is
// exactly the defect, so implementations MUST convert it into ErrNoRouteToNode.
type TargetedCaller interface {
	CallNode(ctx context.Context, nodeID, role, method string, payload []byte) ([]byte, error)
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
