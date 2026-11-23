/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
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

// ErrStarting is what a node answers a call with while its handler registry is
// held until ready: the endpoint spine holds it from before the mesh node starts
// until every route, feature set and infrastructure route is registered and the
// node's catalogue is published, so the call ran nothing. node/handlers'
// registry refuses with it, pkg/dispatch carries it on the wire as its own
// refusal kind and treats it as a refusal that sent nothing (so a caller moves
// to its next route), and a bridge answers it 503 with Retry-After. It is
// declared here, beside ErrDenied, for the reason ErrConflict is: node/handlers
// must refuse with it and cannot import pkg/dispatch; pkg/dispatch and pkg/rpc
// re-export this exact value as their ErrStarting.
var ErrStarting = errors.New("rpc: the serving node is still starting and ran nothing")
