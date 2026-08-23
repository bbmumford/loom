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
