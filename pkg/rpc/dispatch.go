package rpc

import (
	"context"
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"
)

// ErrHandlerNotFound is returned by Dispatch when the FQN is not registered.
// Callers can errors.Is at HTTP/wire boundaries (→ 404).
var ErrHandlerNotFound = errors.New("rpc: handler not found")

// ErrProxyDispatchLocal is returned when Dispatch is called on a proxy
// handler (Func==nil). The receiver should never have routed a proxy-only
// handler to local dispatch — the caller should use the mesh path instead.
var ErrProxyDispatchLocal = errors.New("rpc: proxy handler cannot dispatch locally")

// Dispatch executes a handler from the registry against raw protobuf bytes.
// Enforces Scope before invoking the handler; returns ErrScopeDenied
// (wrapped) when the call fails the handler's declared scope tier.
//
// Dispatch runs scope, unmarshal, the webhook check and the handler for an
// in-process caller holding this Registry. The mesh RPC server does not call
// it: it reaches handlers exported by ExportToHandlerRegistry through
// registryShim.ExecuteRPC. HTTP/CLI callers that need per-handler Middleware
// should go through the HTTP bridge instead.
func (r *Registry) Dispatch(ctx context.Context, fqn string, payload []byte) ([]byte, error) {
	h, ok := r.Get(fqn)
	if !ok {
		return nil, fmt.Errorf("%w: %s", ErrHandlerNotFound, fqn)
	}
	if h.Func == nil {
		return nil, fmt.Errorf("%w: %s", ErrProxyDispatchLocal, fqn)
	}
	if err := EnforceScope(ctx, h, r.PresencePolicy(), r.AuthzPolicy()); err != nil {
		return nil, err
	}

	// Create zero-value request from type hint
	req := NewInstance(h.Request)
	if len(payload) > 0 {
		if err := proto.Unmarshal(payload, req); err != nil {
			return nil, fmt.Errorf("unmarshal %s request: %w", fqn, err)
		}
	}

	// Webhook-class pre-dispatch hook — runs signature verification
	// BEFORE handler.Func is invoked so handlers never see an
	// unauthenticated event. Fails closed on missing secret / bad sig.
	// Only runs for handlers that declared WebhookSignature on
	// registration; zero overhead for all other handlers.
	if h.WebhookSignature != nil {
		if err := verifyWebhookSignature(h.WebhookSignature, req); err != nil {
			return nil, fmt.Errorf("%s: %w", fqn, err)
		}
	}

	// Execute handler
	resp, err := h.Func(ctx, req)
	if err != nil {
		return nil, err
	}

	// Marshal response
	out, err := proto.Marshal(resp)
	if err != nil {
		return nil, fmt.Errorf("marshal %s response: %w", fqn, err)
	}
	return out, nil
}

// ExtractRole returns "namespace.domain" (everything before the last dot) from
// a "namespace.domain.Operation" FQN. It returns "" unless fqn has at least two
// dots, a non-empty segment before the first dot and after the last, and a
// non-empty stretch between them, so a malformed FQN such as "bogus" or "...."
// never becomes a routing role. Inner segments are not checked ("a.b..c"
// yields "a.b.").
func ExtractRole(fqn string) string {
	if fqn == "" {
		return ""
	}
	first, last := -1, -1
	for i := 0; i < len(fqn); i++ {
		if fqn[i] == '.' {
			if first < 0 {
				first = i
			}
			last = i
		}
	}
	// Need at least two distinct dots — the namespace dot and the operation
	// dot. A single dot ("ns.op" / ".x" / "x.") is malformed.
	if first < 0 || first == last {
		return ""
	}
	// No empty namespace, domain, or operation segments.
	if first == 0 || last == len(fqn)-1 || first+1 == last {
		return ""
	}
	return fqn[:last]
}
