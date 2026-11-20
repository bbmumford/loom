/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package rpc

import "github.com/bbmumford/loom/pkg/rpc/scope"

// Coded is a handler's refusal that names its caller-facing status and a
// stable wire code (scope.Coded, this exact type): the HTTP bridge
// (rpc/http.classifyHandlerStatus) answers it with Status and the code beside
// the error text, so a client switches on the code and never on the sentence,
// whether the handler ran in the bridge's own process or on a node the call
// crossed the mesh to. A handler error that is not Coded and matches no other
// sentinel answers 500 with no detail.
type Coded = scope.Coded

// Refuse wraps cause as a Coded refusal (scope.Refuse). Status must be a 4xx
// (a refusal of the request, never a server fault) and code non-empty;
// anything else answers cause unchanged, so a mistaken call never turns a
// fault into a client error.
func Refuse(status int, code string, cause error) error { return scope.Refuse(status, code, cause) }

// CodedOf answers the Coded refusal err carries, if any.
func CodedOf(err error) (*Coded, bool) { return scope.CodedOf(err) }
