/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"github.com/bbmumford/loom/internal/securityctx"
	"github.com/bbmumford/loom/ports"
)

// DefaultAuthValidator returns the validator an RPC server uses when Config
// carries none. It reads and stamps loom's own context keys, so it needs no
// composition-root package and names no identity level.
//
// It exists so a consumer can SAY it wants the default. Config.AuthValidator
// may simply be left unset — SetAuthValidator ignores a nil, and the server is
// constructed with this validator already in place — but "the field is absent"
// and "the default is intended" then look identical in a composition root, and
// a later reader cannot tell which one a line meant. Passing this makes the
// choice explicit and survives someone adding a nil check.
//
// It gates the way an injected Config.AuthValidator is expected to: a handler that does not
// require auth passes; one that does needs an authenticated principal, a
// permitted auth type, and every required scope. Because it reads loom's keys,
// a caller whose authority was stamped somewhere else entirely produces a
// uniform DENY for those handlers rather than an admit — the direction a gate
// must fail when it cannot see what it is judging.
func DefaultAuthValidator() ports.AuthValidator { return securityctx.Default() }
