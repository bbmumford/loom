package rpchttp

import (
	"encoding/json"
	"errors"
	"io"
	"log"
	"net/http"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/go-chi/chi/v5"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/bbmumford/loom/pkg/rpc"
)

// TenantInjector mutates the parsed request message to stamp tenant/org
// context derived from the HTTP request (typically session cookie or
// bearer). Bridges that don't need this leave it nil.
type TenantInjector = func(r *http.Request, msg proto.Message)

// Bridge provides a single HTTP handler for all RPC operations.
// Replaces all per-domain generated http_bridge.go files.
//
// Concurrency: tenantInjector and directoryAuth are held in
// atomic.Pointers so callers can rotate them under load without racing
// in-flight dispatch goroutines.
//
// directoryAuth is per-Bridge (per-Registry) — NOT a package-level global
// — so a process that mounts multiple registries (e.g. a user-tier proxy
// registry alongside a control-plane registry) picks its verbose-directory
// gating per-registry. See finding rev-080 for the process-global race
// and cross-registry bypass that motivated this design.
type Bridge struct {
	registry       *rpc.Registry
	tenantInjector atomic.Pointer[TenantInjector]
	directoryAuth  atomic.Pointer[DirectoryAuthCheck]
	maxBodySize    int64 // max request body size (default 1MB)
}

// defaultMaxBodySize is the default per-request body cap (1 MiB). Tunable
// via NewBridgeWithLimit when an endpoint needs larger uploads.
const defaultMaxBodySize int64 = 1 << 20

// NewBridge creates a new generic RPC HTTP bridge with the default body
// size limit (1 MiB).
func NewBridge(registry *rpc.Registry) *Bridge {
	return &Bridge{
		registry:    registry,
		maxBodySize: defaultMaxBodySize,
	}
}

// NewBridgeWithLimit creates a Bridge with a custom max body size. Use
// for endpoints that accept large uploads (e.g. import APIs). Caller is
// responsible for picking a sensible upper bound.
func NewBridgeWithLimit(registry *rpc.Registry, maxBodySize int64) *Bridge {
	if maxBodySize <= 0 {
		maxBodySize = defaultMaxBodySize
	}
	b := &Bridge{
		registry:    registry,
		maxBodySize: maxBodySize,
	}
	return b
}

// SetTenantInjector configures a callback that injects tenant/org context
// from the HTTP request into the proto message before dispatch. Safe to
// call concurrently with HandleRPC traffic — uses an atomic pointer swap.
// Pass nil to clear.
func (b *Bridge) SetTenantInjector(fn TenantInjector) {
	if fn == nil {
		b.tenantInjector.Store(nil)
		return
	}
	b.tenantInjector.Store(&fn)
}

// loadInjector returns the current injector or nil. Safe under concurrent
// SetTenantInjector calls.
func (b *Bridge) loadInjector() TenantInjector {
	if p := b.tenantInjector.Load(); p != nil {
		return *p
	}
	return nil
}

// SetDirectoryAuth installs the auth predicate consulted by
// Bridge.HandleDirectory (and by HandleRPC when routing the
// "_directory" wildcard meta-path). Safe to call concurrently with
// in-flight directory requests — uses an atomic pointer swap. Pass nil
// to revert to summary-only mode.
//
// Per-Bridge (per-Registry): each Bridge holds its own predicate. A
// process that mounts a user-tier proxy registry alongside a control-
// plane registry gates each independently. See finding rev-080 for
// the prior package-global design and why it was unsafe.
func (b *Bridge) SetDirectoryAuth(check DirectoryAuthCheck) {
	if check == nil {
		b.directoryAuth.Store(nil)
		return
	}
	b.directoryAuth.Store(&check)
}

// loadDirectoryAuth returns the current directory auth predicate or nil.
// Safe under concurrent SetDirectoryAuth calls.
func (b *Bridge) loadDirectoryAuth() DirectoryAuthCheck {
	if p := b.directoryAuth.Load(); p != nil {
		return *p
	}
	return nil
}

// HandleDirectory returns an http.HandlerFunc that renders the
// `/rpc/_directory` view for THIS bridge's registry, gated on THIS
// bridge's auth predicate. The predicate is loaded per-request via the
// atomic.Pointer, so callers can hot-swap it without draining traffic.
//
// nil predicate → summary-only (safer default). See rpchttp.HandleDirectory
// docstring for the response shapes.
func (b *Bridge) HandleDirectory() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		writeDirectory(w, r, b.registry, b.loadDirectoryAuth())
	}
}

// HandleRPC dispatches based on {handler} URL parameter.
// Used for wildcard routes: POST /rpc/{handler}.
//
// Special-cases the meta-path "_directory" (any HTTP method): returns
// the same JSON manifest as the GET /rpc/_directory route mounted by
// RegisterAll. Wildcard-routed gateways (app.orbtr.io and friends)
// don't call RegisterAll, so without this special case they'd dispatch
// "_directory" as a regular handler name and 404.
//
// CRITICAL: HandleRPC now applies the per-handler Middleware chain around
// the dispatch path. Prior versions bypassed Middleware entirely for the
// wildcard route — every gateway endpoint that used /rpc/{handler} skipped
// the auth/scope chain RegisterAll wires onto per-FQN routes. The fix
// composes the chain at request time so wildcard parity matches per-route
// parity exactly.
func (b *Bridge) HandleRPC(w http.ResponseWriter, r *http.Request) {
	handlerName := chi.URLParam(r, "handler")
	if handlerName == "" {
		writeError(w, http.StatusBadRequest, "handler name required")
		return
	}
	if handlerName == "_directory" {
		// Per-Bridge auth (rev-080): the directory view is gated on THIS
		// bridge's SetDirectoryAuth predicate, NOT a package-level global.
		// A wildcard-mounted bridge and a per-route mount on a sibling
		// registry can therefore diverge in verbose/summary gating.
		writeDirectory(w, r, b.registry, b.loadDirectoryAuth())
		return
	}
	h, ok := b.registry.Get(handlerName)
	if !ok {
		w.Header().Set("Connection", "close")
		writeError(w, http.StatusNotFound, "unknown handler: "+handlerName)
		return
	}

	// Compose the handler's middleware chain around dispatch. Empty chain →
	// dispatch runs directly. The chain is fixed for the lifetime of the
	// registered Handler — Registry.Register defensive-copies the slice so
	// caller mutations can't retroactively change what runs here.
	final := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b.dispatch(w, r, h)
	})
	composeMiddleware(h.Middleware, final).ServeHTTP(w, r)
}

// composeMiddleware applies the chain so chain[0] is the outermost wrapper
// (runs first, sees the request before any other middleware or the handler).
// Empty chain returns h unchanged.
func composeMiddleware(chain []rpc.Middleware, h http.Handler) http.Handler {
	for i := len(chain) - 1; i >= 0; i-- {
		h = chain[i](h)
	}
	return h
}

// dispatch handles JSON→proto unmarshaling, tenant injection, local/proxy
// dispatch, and proto→JSON response marshaling. The Handler pointer is
// supplied by HandleRPC (or by per-FQN routes) so we don't re-Get.
func (b *Bridge) dispatch(w http.ResponseWriter, r *http.Request, h *rpc.Handler) {
	// 1. Read body with size limit. We read maxBodySize+1 and reject if the
	//    extra byte got pulled — the LimitReader prevents unbounded allocation
	//    even on a malicious Content-Length.
	body, err := io.ReadAll(io.LimitReader(r.Body, b.maxBodySize+1))
	if err != nil {
		writeError(w, http.StatusBadRequest, "read body: "+err.Error())
		return
	}
	if int64(len(body)) > b.maxBodySize {
		writeError(w, http.StatusRequestEntityTooLarge, "request body too large")
		return
	}

	// 2. Parse into typed request. DiscardUnknown=false: an unknown field
	//    is an error, not a silent drop. Schema-drift surfaces immediately;
	//    attacker probing of internal fields fails fast.
	req := rpc.NewInstance(h.Request)
	if len(body) > 0 {
		opts := protojson.UnmarshalOptions{DiscardUnknown: false}
		if err := opts.Unmarshal(body, req); err != nil {
			writeError(w, http.StatusBadRequest, "invalid request: "+err.Error())
			return
		}
	}

	// 3. Tenant injection runs AFTER parse so the injector can overwrite
	//    any client-supplied tenant/org fields on the typed proto. The
	//    pointer load is atomic — safe under concurrent SetTenantInjector.
	if inj := b.loadInjector(); inj != nil {
		inj(r, req)
	}

	// 4. Execute local or proxy
	var out []byte
	if h.Func != nil {
		out, err = b.executeLocal(r, h, req)
	} else {
		out, err = b.executeProxy(r, h, req)
	}
	if err != nil {
		writeBridgeError(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(out)
}

// executeLocal runs the handler in-process. Returns the marshalled response
// or an error wrapped via classifyHandlerError for the caller to map.
//
// Mirrors Registry.Dispatch's pre-handler guards (rpc/dispatch.go:36 +
// rpc/dispatch.go:53-57) so the HTTP bridge enforces the same security
// surface as the mesh-side dispatcher:
//
//  1. EnforceScope fails closed on the handler's declared TenantScope
//     tier when the stamped ctx (tenantId/userId/orgId) doesn't
//     match. Without this the bridge silently bypassed the declared
//     scope on every handler reached through /rpc/{handler} or per-FQN
//     routes — see finding rev-081.
//  2. verifyWebhookSignature runs the webhook-class pre-dispatch hook
//     before handler.Func sees an unauthenticated event. We run this on
//     the LOCAL execution path only; the proxy path delegates to the
//     destination's Registry.Dispatch which always runs the same hook.
//
// classifyHandlerStatus maps ErrScopeDenied → 403 and
// ErrWebhookSignatureInvalid / ErrWebhookSecretUnset → 401, keeping
// status-code shaping single-sourced.
func (b *Bridge) executeLocal(r *http.Request, h *rpc.Handler, req proto.Message) ([]byte, error) {
	if err := rpc.EnforceScope(r.Context(), h, b.registry.PlatformTenants()); err != nil {
		return nil, &bridgeError{status: classifyHandlerStatus(err), cause: err, op: h.FQN()}
	}
	if h.WebhookSignature != nil {
		if err := rpc.VerifyWebhookSignature(h.WebhookSignature, req); err != nil {
			return nil, &bridgeError{status: classifyHandlerStatus(err), cause: err, op: h.FQN()}
		}
	}
	resp, err := h.Func(r.Context(), req)
	if err != nil {
		return nil, &bridgeError{status: classifyHandlerStatus(err), cause: err, op: h.FQN()}
	}
	out, err := protojson.MarshalOptions{EmitUnpopulated: false}.Marshal(resp)
	if err != nil {
		return nil, &bridgeError{status: http.StatusInternalServerError, cause: err, op: h.FQN(), where: "marshal response"}
	}
	return out, nil
}

// executeProxy dispatches over the mesh via CallRaw and unmarshals the wire
// proto into JSON. Implements the bounded one-shot retry for transient
// route failures (only "session closed before send" qualifies — see
// errmap.go for why other "all probes failed" returns can't safely retry).
//
// Scope enforcement caveat (finding rev-081, reviewer concern C3): gateway
// endpoints (app.orbtr.io, bootstrap.hstles.com — see
// project_v0_0_301_swarm_address_bridge) do NOT call SetPlatformTenants
// on their proxy registry. PlatformTenants() therefore returns nil, and
// IsPlatform always answers false. A ScopePlatform handler that is
// legitimately reachable at the destination would 403 on the gateway
// before the request ever crossed the mesh.
//
// We resolve this by NOT enforcing scope on the proxy path when the
// handler is ScopePlatform AND the proxy registry has no platforms
// configured — verification is the destination's responsibility, which
// always runs Registry.Dispatch and always enforces. Webhook-signature
// verification is also deferred to the destination (the gateway typically
// doesn't carry the secret env var; running it here would fail-closed on
// every signed event passing through a thin gateway).
//
// All non-Platform scopes (Tenant/Org/User/Profile) ARE enforced here —
// those tiers only need the wire-stamped ctx fields, which the gateway
// has after tenantInjector ran, and enforcing fail-closes a malformed
// ctx before we waste mesh bandwidth on a doomed call.
func (b *Bridge) executeProxy(r *http.Request, h *rpc.Handler, req proto.Message) ([]byte, error) {
	if h != nil && h.Scope != rpc.ScopePlatform {
		if err := rpc.EnforceScope(r.Context(), h, b.registry.PlatformTenants()); err != nil {
			return nil, &bridgeError{status: classifyHandlerStatus(err), cause: err, op: h.FQN()}
		}
	} else if h != nil && b.registry.PlatformTenants() != nil {
		// ScopePlatform AND this registry knows the platform set — enforce.
		// (Node endpoints that proxy ScopePlatform handlers DO configure
		// platforms; only the thin gateways with nil set are exempted.)
		if err := rpc.EnforceScope(r.Context(), h, b.registry.PlatformTenants()); err != nil {
			return nil, &bridgeError{status: classifyHandlerStatus(err), cause: err, op: h.FQN()}
		}
	}
	payload, err := proto.Marshal(req)
	if err != nil {
		return nil, &bridgeError{status: http.StatusBadRequest, cause: err, op: h.FQN(), where: "marshal request"}
	}
	respBytes, err := rpc.CallRaw(r.Context(), h.FQN(), payload)
	if err != nil {
		status, retryAfter, retryEligible := classifyMeshError(err)
		if retryEligible && r.Context().Err() == nil {
			if respBytes2, err2 := rpc.CallRaw(r.Context(), h.FQN(), payload); err2 == nil {
				respBytes = respBytes2
				goto unmarshal
			}
		}
		return nil, &bridgeError{status: status, cause: err, op: h.FQN(), retryAfter: retryAfter}
	}
unmarshal:
	resp := rpc.NewInstance(h.Response)
	if err := proto.Unmarshal(respBytes, resp); err != nil {
		return nil, &bridgeError{status: http.StatusInternalServerError, cause: err, op: h.FQN(), where: "unmarshal response"}
	}
	out, err := protojson.MarshalOptions{EmitUnpopulated: false}.Marshal(resp)
	if err != nil {
		return nil, &bridgeError{status: http.StatusInternalServerError, cause: err, op: h.FQN(), where: "marshal response"}
	}
	return out, nil
}

// classifyHandlerStatus maps a handler error to an HTTP status code.
// Known sentinels (ErrScopeDenied, ErrHandlerNotFound, webhook signature
// failures, context cancellations) get specific codes; unknown errors
// get 500 + the error msg.
//
// We don't sanitize the unknown-error message here — downstream callers
// (writeBridgeError) decide whether to expose the cause or substitute a
// generic body. That keeps the classifier pure.
func classifyHandlerStatus(err error) int {
	switch {
	case errors.Is(err, rpc.ErrScopeDenied):
		return http.StatusForbidden
	case errors.Is(err, rpc.ErrHandlerNotFound):
		return http.StatusNotFound
	case errors.Is(err, rpc.ErrProxyDispatchLocal):
		return http.StatusBadGateway
	case errors.Is(err, rpc.ErrWebhookSignatureInvalid),
		errors.Is(err, rpc.ErrWebhookSecretUnset):
		// Webhook signature failure (bad/missing/expired signature) and
		// unset signing secret both map to 401 Unauthorized — the request
		// did not present credentials this gateway can accept. Distinct
		// from 403 (scope denied) so dashboards and rate-limiters can
		// alarm on each cause separately.
		return http.StatusUnauthorized
	}
	return http.StatusInternalServerError
}

// bridgeError captures all the metadata needed to render an HTTP error
// response — status code, optional Retry-After, the underlying cause for
// logging, and a stable "operation" tag for log correlation.
type bridgeError struct {
	status     int
	cause      error
	op         string
	where      string
	retryAfter string
}

func (e *bridgeError) Error() string {
	if e.where != "" {
		return e.where + ": " + e.cause.Error()
	}
	return e.cause.Error()
}

func (e *bridgeError) Unwrap() error { return e.cause }

// writeBridgeError renders a bridgeError to the response. Sets Retry-After
// when present. Logs the full cause with a request-id for log correlation;
// the wire body carries only the classified status text (5xx) or the
// classified message verbatim (4xx). This prevents internal stack/SQL/
// database detail from leaking through 5xx response bodies.
func writeBridgeError(w http.ResponseWriter, err error) {
	var be *bridgeError
	if !errors.As(err, &be) {
		// Fallback for non-bridgeError causes (shouldn't happen, but stays
		// defensive). Logs the full cause; returns generic 500 body.
		log.Printf("[rpc.bridge] unclassified dispatch error: %v", err)
		writeError(w, http.StatusInternalServerError, "internal error")
		return
	}
	if be.retryAfter != "" {
		w.Header().Set("Retry-After", be.retryAfter)
	}
	if be.status >= 500 {
		// 5xx: log the cause, return generic body + request id for log
		// correlation. Prevents stack/SQL/internal detail from leaking
		// through the wire response.
		reqID := strconv.FormatInt(time.Now().UnixNano(), 36)
		log.Printf("[rpc.bridge] %s op=%s requestID=%s err=%v", http.StatusText(be.status), be.op, reqID, be.cause)
		writeError(w, be.status, http.StatusText(be.status)+" (requestID="+reqID+")")
		return
	}
	// 4xx: surface the classified cause so callers can debug their requests.
	writeError(w, be.status, be.cause.Error())
}

func writeError(w http.ResponseWriter, status int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	resp, _ := json.Marshal(map[string]string{"error": message})
	_, _ = w.Write(resp)
}
