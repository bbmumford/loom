package rpchttp

import (
	"context"
	"errors"
	"strings"

	"github.com/bbmumford/loom/pkg/dispatch"
)

// classifyMeshError maps a mesh-dispatch error to an HTTP status code plus
// optional retry hint. Used by the bridge to surface accurate failure modes
// to gateway callers instead of collapsing every cause to 502 Bad Gateway.
//
//   - DeadlineExceeded            → 504 Gateway Timeout
//   - dispatch.ErrCircuitOpen     → 503 Service Unavailable + Retry-After: 1
//   - "session closed before send" → 502 + retry-eligible (the call never
//     reached the remote handler — Send hadn't started; safe to redo)
//   - "all N probes failed"       → 502, NOT retried. Probes can have lost
//     the response after Send succeeded, so a retry could double-execute a
//     mutating RPC. The dispatcher already invalidated its route cache; the
//     caller is welcome to retry idempotent ops at the application layer.
//   - "dispatch caller not init"  → 500 Internal Server Error (config bug,
//     not a gateway condition)
//   - everything else             → 502 Bad Gateway
//
// retryEligible signals to the bridge that one bounded re-dispatch is safe
// because the call never reached the remote handler (no side effects). We
// only set it for the strict "before send" case — every other path could
// have crossed the wire and lost only the response, so retry could double-
// execute. If a future per-handler idempotency flag is wired in, broader
// retry can be re-introduced gated on that flag.
func classifyMeshError(err error) (status int, retryAfter string, retryEligible bool) {
	if err == nil {
		return 500, "", false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return 504, "", false
	}
	if errors.Is(err, dispatch.ErrCircuitOpen) {
		return 503, "1", false
	}
	// HandlerError: the remote handler responded but reported an
	// application-level failure (validation, not-found, conflict). The
	// transport worked end-to-end — bucketing under 502 Bad Gateway is
	// misleading. Map to 400 Bad Request and surface the handler's own
	// message so callers can see the underlying complaint.
	if errors.Is(err, dispatch.ErrHandlerError) {
		return 400, "", false
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "session closed before send"):
		return 502, "", true
	case strings.Contains(msg, "all ") && strings.Contains(msg, "probes failed"):
		return 502, "", false
	case strings.Contains(msg, "session closed"):
		return 502, "", false
	case strings.Contains(msg, "dispatch caller not initialized"):
		return 500, "", false
	}
	return 502, "", false
}
