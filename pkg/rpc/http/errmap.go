package rpchttp

import (
	"context"
	"errors"
	"strings"

	"github.com/bbmumford/loom/pkg/dispatch"
	"github.com/bbmumford/loom/pkg/rpc"
)

// classifyMeshError maps a mesh-dispatch error to an HTTP status code plus
// optional retry hint. Used by the bridge to surface accurate failure modes
// to gateway callers instead of collapsing every cause to 502 Bad Gateway.
//
//   - dispatch.ErrAmbiguousEffect → 502, NOT retried: the call left and its
//     outcome is unknown, so a retry could run the operation a second time
//   - DeadlineExceeded            → 504 Gateway Timeout
//   - dispatch.ErrCircuitOpen     → 503 Service Unavailable + Retry-After: 1
//   - dispatch.ErrAnswerersDisagree → 503, NOT retried: the adverts of the
//     operation disagree on what it requires, nothing left this node, and no
//     resend changes that until the fleet's adverts do
//   - rpc.ErrScopeDenied           → 403, NOT retried: the handler, or its
//     registry's scope gate, refused the caller's authority, or the serving
//     node refused the principal the caller sent, on another node
//     (dispatch.StampDenial) or in this process (the node's local
//     dispatcher), as executeLocal answers the refusal made here
//   - dispatch.ErrHandlerError     → 400: the handler answered with a failure
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
// execute.
// TODO: allow a broader retry for an operation whose effect.Class is
// Repeatable; the bridge does not read the class.
func classifyMeshError(err error) (status int, retryAfter string, retryEligible bool) {
	if err == nil {
		return 500, "", false
	}
	// An ambiguous effect was sent and may have run; any resend could run it
	// again. It is checked before the deadline mapping because its cause is
	// often the deadline that ended the wait.
	if errors.Is(err, dispatch.ErrAmbiguousEffect) {
		return 502, "", false
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, dispatch.ErrRouteDeadline) {
		return 504, "", false
	}
	if errors.Is(err, dispatch.ErrCircuitOpen) {
		return 503, "1", false
	}
	if errors.Is(err, dispatch.ErrAnswerersDisagree) {
		return 503, "", false
	}
	// A refusal of the caller's authority is checked before HandlerError,
	// because the one a handler on another node made is a HandlerError too.
	if errors.Is(err, rpc.ErrScopeDenied) {
		return 403, "", false
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
