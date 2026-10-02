# rpc — Namespace-Qualified Reflection Dispatch

Zero-codegen RPC dispatch: handlers self-register with namespace-qualified names; one generic HTTP bridge and mesh dispatcher serve every domain.

## Architecture

```
Handler Registration (startup):
  monitoring.Register(registry)  →  "example.monitoring.GetOverallStatus" → handler func

HTTP Request:
  POST /rpc/example.monitoring.GetOverallStatus {json body}
    → Router middleware the endpoint installs (recovery, logging, security headers, session)
    → Per-handler middleware (RequireAuth, RateLimit, RequireDeviceToken, etc.)
    → Generic bridge: lookup handler → protojson.Unmarshal → EnforceScope → call func (or forward over the mesh) → protojson.Marshal

Mesh RPC:
  rpc.Call[*pb.Resp](ctx, "example.monitoring.GetOverallStatus", req)
    → proto.Marshal
    → in-process dispatch when this process hosts the handler, otherwise
      dispatch.GetCaller().Call(ctx, "example.monitoring", FQN, bytes) (an HWPCaller on a mesh node)
    → RPCServer → HandlerRegistry (filled by Registry.ExportToHandlerRegistry)
    → scope check → proto.Unmarshal → webhook check (handlers that declare WebhookSignature) → call func → proto.Marshal

LAD Discovery:
  node-a.example.com publishes: roles=["example.identity", "example.monitoring", ...]
  node-b.example.com publishes: roles=["example.device", "example.policy", ...]
  node-c.example.com publishes: roles=["sample.monitoring"]  ← distinct from example.monitoring
```

## Namespace Convention

```
{namespace}.{domain}.{Operation}
```

The consumer chooses the namespace, and it may have more than one dot-separated
segment. The role a node publishes and routes by is `{namespace}.{domain}`
(everything before the last dot, see `ExtractRole`), so two namespaces can each
host a domain of the same name without their roles colliding.

## Handler Registration

Each domain has a `register.go` that self-registers handlers:

```go
package monitoring

func Register(reg *rpc.Registry) error {
    return reg.RegisterAll([]rpc.Handler{
        {Namespace: "example", Domain: "monitoring", Operation: "GetOverallStatus",
            Func: rpc.Wrap(handlers.GetOverallStatus),
            Request: (*pb.GetOverallStatusRequest)(nil),
            Response: (*pb.GetOverallStatusResponse)(nil),
            Scope: rpc.ScopeNone},
    })
}
```

A domain whose handlers are methods takes the receiver:

```go
package device

func Register(reg *rpc.Registry, h *handlers.Handler) error {
    return reg.RegisterAll([]rpc.Handler{
        {Namespace: "example", Domain: "device", Operation: "Heartbeat",
            Func: rpc.Wrap(h.Heartbeat),
            Request: (*pb.HeartbeatRequest)(nil),
            Response: (*pb.HeartbeatResponse)(nil),
            Scope: rpc.ScopePartition},
    })
}
```

A gateway that serves an operation another node executes registers the same
list with `Func` stripped through `Registry.RegisterProxy`; `rpctest.AssertRegisterTypesParity`
checks that a domain's `Register` and `Types` lists agree.

## Security: Middleware Chain (No Fixed Tiers)

Security is **pure composition** — each handler declares its own middleware chain.
There is no SecurityTier enum. Middleware is attached at endpoint boot time,
not at handler registration time.

### Why no fixed tiers?

Each consumer has its own auth types (session, service token, device token, API
key, ...). A fixed enum in a shared module would have to name every one of them;
a middleware chain lets each composition root attach the checks it owns.

### How it works

**Domain package (register.go)** — declares handler logic and its scope tag, no middleware:
```go
rpc.Handler{
    Namespace: "example", Domain: "device", Operation: "Heartbeat",
    Func: rpc.Wrap(h.Heartbeat),
    Scope: rpc.ScopePartition,
    // No Middleware — added by endpoint at boot time
}
```

**Endpoint (main.go)** — applies middleware by convention:
```go
reg := rpc.NewRegistry()
device.Register(reg, deviceHandler)

// Apply middleware by namespace.domain pattern:
for _, h := range reg.ByRole("example.device") {
    h.Middleware = []rpc.Middleware{
        devicemw.RequireDeviceToken(resolver),
    }
}
for _, h := range reg.ByNamespace("sample") {
    h.Middleware = []rpc.Middleware{
        authmw.RequireAuth("session", "bearer"),
    }
}

// Register all handlers with their middleware on the HTTP router:
rpchttp.RegisterAll(reg, router)
```

**HTTP routes** — wrap each handler with its middleware chain:
```go
// For each handler (pkg/rpc/http/routes.go):
final := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
    bridge.dispatch(w, r, handler)
})
wrapped := composeMiddleware(handler.Middleware, final)
router.Method("POST", "/rpc/"+handler.FQN(), wrapped)
```

### Example middleware compositions

```go
// Session-authenticated endpoint
h.Middleware = []rpc.Middleware{
    authmw.RequireAuth("session", "bearer"),
    authmw.RequireScopes("identity.write"),
    authmw.RateLimit(authmw.RateLimitOptions{ConfigName: "strict"}),
}

// Device/agent endpoint
h.Middleware = []rpc.Middleware{
    devicemw.RequireDeviceToken(resolver),
}

// Authenticated endpoint behind a feature gate
h.Middleware = []rpc.Middleware{
    authmw.RequireAuth("session", "bearer"),
    authmw.RequireSubject(),
    features.RequireFeature("policy.full"),
}

// Public (no auth) — empty middleware
h.Middleware = nil  // or []rpc.Middleware{}

// A consumer with its own auth type
h.Middleware = []rpc.Middleware{
    consentmw.RequireConsent(),
    authmw.RequireAuth("bearer"),
}
```

## Type-Safe Caller

```go
resp, err := rpc.Call[*pb.GetOverallStatusResponse](ctx, "example.monitoring.GetOverallStatus", req)
```

`Call` and `CallRaw` dispatch in-process when this process hosts the handler and
over the mesh otherwise. `CallNode` and `CallNodeRaw` send to one named node and
never short-circuit locally.

## Scope tiers

Each handler declares a `Scope` tag. `EnforceScope` judges it at the HTTP
bridge and in `Registry.Dispatch`. At mesh dispatch the `node/handlers`
registry judges the exported copy the same way with its own policy, and also
reconciles the transport-pinned axis against the transport binding.

loom treats a tag as an opaque label: the `PresencePolicy` installed on the
registry (`Registry.SetPresencePolicy` here) decides what it requires, and a
registry with no policy denies every handler above `ScopeNone`.
`scope.NewScopePresencePolicy` is the reference policy:

| Tag | The reference policy admits |
|-----|-----------------------------|
| `ScopeNone` | every caller; `EnforceScope` admits it without consulting a policy (liveness, public data) |
| `ScopeAllowlisted` | an identity whose partition is in the allowlist passed to `NewScopePresencePolicy` |
| `ScopePartition` | an identity with a non-empty partition |
| `ScopeWithinPartition` | evaluated as `ScopePartition` |
| `ScopeSubject` | an identity with a non-empty partition and a non-empty authenticated subject |

`ScopeUnset` (the zero value) and `ScopeUnknown` are refused at registration;
`EnforceScope` fails `ScopeUnset` closed and the reference policy denies
`ScopeUnknown`.

A handler that declares a `Requirement` (`scope.Open()`, `scope.Capability(cap)`,
`scope.Nexus()`) is judged by the registry's authorizer
(`Registry.SetAuthzPolicy` here) instead of its `Scope` tag.

`EnforceScope` reads only the typed identity a trusted transport adapter stamps
with `WithAuthenticatedScopeIdentity`. Request bodies and the `WithRPCContext`
map are never read as scope authority.

## Package Structure

```
pkg/rpc/
├── handler.go      — Handler struct, Middleware type, Scope* tags, Wrap[Req,Resp] generic
├── registry.go     — Thread-safe Registry with namespace-qualified lookup, RegisterProxy, StripFuncs
├── dispatch.go     — Registry.Dispatch (scope, unmarshal, webhook check, handler), ExtractRole
├── scope.go        — EnforceScope and the typed scope identity on the context
├── call.go         — Call[Resp], CallRaw, CallNode, CallNodeRaw
├── local.go        — the in-process short-circuit Call and CallRaw take
├── webhook_sig.go  — webhook-class signature verification
├── adapter.go      — exports handlers into node/handlers.HandlerRegistry
├── scope/          — ScopeTag, identity axes, PresencePolicy, Requirement
├── rpctest/        — AssertRegisterTypesParity for domain Register/Types tests
├── registrycheck/  — go/analysis pass that reports a discarded registration error
└── http/
    ├── bridge.go    — Single generic HTTP handler (protojson, body limits)
    ├── routes.go    — Auto-registration with per-handler middleware wrapping
    ├── directory.go — GET /rpc/_directory handler listing
    └── errmap.go    — mesh-dispatch error → HTTP status mapping
```
