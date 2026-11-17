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
// A handler declaring rpc.ExposureMeshOnly gets no route (bridged), so the
// router answers its path as it answers any path it does not hold.
//
// The introspection endpoint (`GET /rpc/_directory`) is mounted last and
// defaults to summary-only. Endpoints that want verbose directory access
// for authenticated callers should use RegisterAllWithBridge, pre-configure
// the Bridge via bridge.SetDirectoryAuth, and pass that bridge in — the
// auth predicate is per-Bridge (per-Registry), NOT a process-wide global.
func RegisterAll(reg *rpc.Registry, router chi.Router) {
	RegisterAllWithBridge(NewBridge(reg), router)
}

// RegisterAllWithBridge is the auth-aware variant of RegisterAll: the
// caller pre-builds a Bridge (optionally configuring IdentityInjector +
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
	for _, h := range bridged(bridge.registry) {
		handler := h // bind by value so closure captures stable pointer
		final := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			bridge.dispatch(w, r, handler)
		})
		wrapped := composeMiddleware(handler.Middleware, final)
		router.Method("POST", "/rpc/"+handler.FQN(), wrapped)
	}
	router.Method("GET", "/rpc/_directory", bridge.HandleDirectory())
}

// bridged is every handler of reg an HTTP surface may present: all of them but
// those declaring rpc.ExposureMeshOnly, sorted by FQN as reg.All returns them.
// RegisterAllWithBridge mounts exactly these and writeDirectory lists exactly
// these, and Bridge.HandleRPC answers a mesh-only handler as a miss, because a
// mesh-only operation (cortex's inference declarations) must not be reachable
// from any HTTP request, however the bridge is mounted.
func bridged(reg *rpc.Registry) []*rpc.Handler {
	all := reg.All()
	out := make([]*rpc.Handler, 0, len(all))
	for _, h := range all {
		if !h.MeshOnly() {
			out = append(out, h)
		}
	}
	return out
}
