/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package compose

// FunctionID is the stable fully-qualified function id — the unified id
// space across the reflection RPC registry and the mesh handler registry
// (e.g. "hstles.auth.validate"). ≥2 dot segments (ExtractRole is strict).
type FunctionID string

// ResultKind is iii's 4-way function-result semantics.
type ResultKind int

const (
	// ResultSuccess — completed with a payload. The ONLY kind the RPC
	// response cache may store (only Success is cached today; the compose
	// layer must not widen that).
	ResultSuccess ResultKind = iota

	// ResultFailure — completed with an error; not cached, retryable per
	// the caller's policy.
	ResultFailure

	// ResultDeferred — accepted for asynchronous completion; the result
	// arrives later correlated by invocation ID (maps onto core/task
	// assignment + result flow).
	ResultDeferred

	// ResultNoResult — executed, nothing to return (fire-and-forget /
	// notification semantics).
	ResultNoResult
)

// FunctionResult is the uniform invocation outcome.
type FunctionResult struct {
	Kind    ResultKind
	Payload []byte // nil unless Kind == ResultSuccess (or a Deferred handle)
	Err     string // set when Kind == ResultFailure
}
