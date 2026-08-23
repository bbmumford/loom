package rpchttp

import (
	"net/http"

	"github.com/bbmumford/loom/pkg/rpc"
	"github.com/go-chi/chi/v5"
)

// RegisterAll registers each handler from the rpc.Registry as an HTTP route.
//
// Route format: POST /rpc/{namespace}.{domain}.{Operation}
//
// Each handler's Middleware field is applied per-handler via the shared
// composeMiddleware helper — wildcard /rpc/{handler} routes go through the
// SAME composition path inside HandleRPC, so the two mount styles can't
// diverge on the chain semantics.
//
// Handlers with nil Func are proxied over mesh via rpc.CallRaw().
// Handlers with non-nil Func are executed locally.
//
// The introspection endpoint (`GET /rpc/_directory`) is mounted last and
// defaults to summary-only. Endpoints that want verbose directory access
// for authenticated callers should use RegisterAllWithBridge, pre-configure
// the Bridge via bridge.SetDirectoryAuth, and pass that bridge in — the
// auth predicate is per-Bridge (per-Registry), NOT a process-wide global.
// See finding rev-080.
func RegisterAll(reg *rpc.Registry, router chi.Router) {
	RegisterAllWithBridge(NewBridge(reg), router)
}

// RegisterAllWithBridge is the auth-aware variant of RegisterAll: the
// caller pre-builds a Bridge (optionally configuring TenantInjector +
// DirectoryAuth) and this function mounts the domain routes + the
// `/rpc/_directory` view that consults the bridge's own auth predicate.
//
// Use this instead of RegisterAll when:
//   - the endpoint hosts multiple Registries in the same binary and each
//     needs its own directory auth gating (per-registry isolation), OR
//   - the endpoint wants verbose directory access for authenticated
//     callers (bridge.SetDirectoryAuth(predicate)).
//
// The bridge's registry is the source of truth for the mounted handlers.
func RegisterAllWithBridge(bridge *Bridge, router chi.Router) {
	for _, h := range bridge.registry.All() {
		handler := h // bind by value so closure captures stable pointer
		final := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			bridge.dispatch(w, r, handler)
		})
		wrapped := composeMiddleware(handler.Middleware, final)
		router.Method("POST", "/rpc/"+handler.FQN(), wrapped)
	}
	router.Method("GET", "/rpc/_directory", bridge.HandleDirectory())
}
