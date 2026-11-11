/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package scope

import "errors"

// ErrDenied is the shared sentinel wrapped by every dispatch surface when the
// canonical presence evaluator refuses a scope. The caller-facing rpc package
// re-exports this exact value as rpc.ErrScopeDenied.
var ErrDenied = errors.New("rpc: scope denied")

// ErrConflict is what a handler's conflict with the state it holds unwraps to:
// the write names a revision older than the stored one, would duplicate a
// record that must be unique, would pass a limit the stored state has reached,
// or meets a change of the same record already in flight. It refuses nothing
// about the caller's authority, so it is distinct from ErrDenied. A handler or
// a middleware's Before returns an error wrapping it; node/handlers' Conflict
// reads it into RPCResponse.Conflict, pkg/dispatch stamps it on the response a
// node answers over the mesh, and a caller answering HTTP maps it to 409 where
// it maps ErrDenied to 403. It is declared here, beside ErrDenied, because
// node/handlers must test for it and cannot import pkg/dispatch, which imports
// node/handlers; pkg/dispatch and pkg/rpc re-export this exact value as their
// ErrConflict.
var ErrConflict = errors.New("rpc: the request conflicts with the stored state")
