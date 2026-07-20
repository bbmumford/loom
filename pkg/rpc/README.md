# rpc — Namespace-Qualified Reflection Dispatch

Zero-codegen RPC dispatch for the HSTLES platform. Handlers self-register at startup with namespace-qualified names. A single generic HTTP bridge and mesh dispatcher replace all per-domain generated code.

## Architecture

```
Handler Registration (startup):
  monitoring.Register(registry)  →  "hstles.monitoring.GetOverallStatus" → handler func

HTTP Request:
  POST /rpc/platform.monitoring.GetOverallStatus {json body}
    → Global middleware (Recovery, Logging, SecurityHeaders, Session)
    → Per-handler middleware (RequireAuth, RateLimit, RequireDeviceMJT, etc.)
    → Generic bridge: lookup handler → protojson.Unmarshal → call func → protojson.Marshal

Mesh RPC:
  rpc.Call[*pb.Resp](ctx, "hstles.monitoring.GetOverallStatus", req)
    → proto.Marshal → HWPCaller.Call(ctx, "platform.monitoring", FQN, bytes)
    → RPCServer → Registry.Dispatch → proto.Unmarshal → call func → proto.Marshal

LAD Discovery:
  node.hstles.com publishes: roles=["platform.identity", "platform.auth", ...]
  node.orbtr.io publishes:   roles=["app.device", "app.policy", ...]
  help.orbtr.io publishes:   roles=["app.monitoring"]  ← no collision
```

## Namespace Convention

```
{namespace}.{domain}.{Operation}

platform.*  = HSTLES core domains (identity, auth, billing, platform, support, notify, monitoring, storage, admin)
app.*       = ORBTR-specific domains (device, policy, job, authentication, remote_access, etc.)
custom.*    = Future: tenant-specific extensions
```

## Handler Registration

Each domain has a `register.go` that self-registers handlers:

```go
package monitoring

func Register(reg *rpc.Registry) error {
    return reg.RegisterAll([]rpc.Handler{
        {Namespace: "hstles", Domain: "monitoring", Operation: "GetOverallStatus",
            Func: rpc.Wrap(handlers.GetOverallStatus),
            Request: (*pb.GetOverallStatusRequest)(nil),
            Response: (*pb.GetOverallStatusResponse)(nil),
            Scope: rpc.ScopeNone},
    })
}
```

ORBTR handlers use struct receivers:

```go
package device

func Register(reg *rpc.Registry, h *handlers.Handler) error {
    return reg.RegisterAll([]rpc.Handler{
        {Namespace: "orbtr", Domain: "device", Operation: "Heartbeat",
            Func: rpc.Wrap(h.Heartbeat),
            Request: (*pb.HeartbeatRequest)(nil),
            Response: (*pb.HeartbeatResponse)(nil),
            Scope: rpc.ScopeTenant},
    })
}
```

## Security: Middleware Chain (No Fixed Tiers)

Security is **pure composition** — each handler declares its own middleware chain.
There is no SecurityTier enum. Middleware is attached at endpoint boot time,
not at handler registration time.

### Why no fixed tiers?

HSTLES has 4 auth types (Public, Session, Service, Mixed). ORBTR adds Device (MJT).
Future apps may add PatientConsent, APIKeyScoped, etc. Fixed enums don't scale.

### How it works

**Library (register.go)** — declares handler logic only, no security:
```go
rpc.Handler{
    Namespace: "orbtr", Domain: "device", Operation: "Heartbeat",
    Func: rpc.Wrap(h.Heartbeat),
    // No Middleware — added by endpoint at boot time
}
```

**Endpoint (main.go)** — applies middleware by convention:
```go
reg := rpc.NewRegistry()
device.Register(reg, deviceHandler)

// Apply middleware by namespace.domain pattern:
for _, h := range reg.ByRole("app.device") {
    h.Middleware = []rpc.Middleware{
        orbtr.RequireDeviceMJT(resolver),
    }
}
for _, h := range reg.ByNamespace("platform") {
    h.Middleware = []rpc.Middleware{
        hstles.RequireAuth("session", "bearer"),
    }
}

// Register all handlers with their middleware on the HTTP router:
rpchttp.RegisterAll(reg, securityRoutes)
```

**HTTP bridge** — wraps each handler with its middleware chain:
```go
// For each handler:
var wrapped http.Handler = bridge.HandleRPC
for i := len(h.Middleware) - 1; i >= 0; i-- {
    wrapped = h.Middleware[i](wrapped)
}
sr.Public.Handle("/rpc/"+h.FQN(), wrapped)
```

### Example middleware compositions

```go
// HSTLES: session-authenticated user endpoint
h.Middleware = []rpc.Middleware{
    middleware.RequireAuth("session", "bearer"),
    middleware.RequireScopes("identity.write"),
    middleware.RateLimit(middleware.RateLimitOptions{ConfigName: "strict"}),
}

// ORBTR: device/agent endpoint (MJT auth)
h.Middleware = []rpc.Middleware{
    orbtr.RequireDeviceMJT(resolver),
}

// ORBTR: user endpoint with feature gate
h.Middleware = []rpc.Middleware{
    middleware.RequireAuth("session", "bearer"),
    orbtr.RequireUserIdentity(),
    features.RequireFeature("policy.full"),
}

// Public (no auth) — empty middleware
h.Middleware = nil  // or []rpc.Middleware{}

// Future app with custom auth
h.Middleware = []rpc.Middleware{
    careledger.RequirePatientConsent(),
    middleware.RequireAuth("bearer"),
}
```

## Type-Safe Caller

Replaces all per-domain `dispatch.NewMesh(caller).Method(ctx, req)` patterns:

```go
// Before (generated):
svc := identityDispatch.NewMesh(meshCaller)
resp, err := svc.CreateUser(ctx, req)

// After (generic, zero codegen):
resp, err := rpc.Call[*pb.CreateUserResponse](ctx, "hstles.identity.CreateUser", req)
```

## Tenant Scope

Handler-level tenant isolation, enforced at mesh dispatch:

| Scope | Meaning |
|-------|---------|
| `ScopeNone` | No tenant restriction (health checks, public data) |
| `ScopePlatform` | HSTLES internal only (tenant management, key rotation) |
| `ScopeTenant` | Requires valid tenant_id in RPC context |
| `ScopeOrg` | Requires tenant + org membership |
| `ScopeUser` | Requires tenant + own user data only |

Platform tenant ("orbtr", "hstles") is propagated via `HWPCaller.SetPlatformTenant()`.
Org ID is in the proto request payload (separate channel, no cross-contamination).

## Package Structure

```
Library/rpc/
├── handler.go      — Handler struct, Middleware type, Wrap[Req,Resp] generic
├── registry.go     — Thread-safe Registry with namespace-qualified lookup
├── dispatch.go     — Generic proto reflection dispatch, ExtractRole
├── call.go         — Type-safe Call[Resp] + CallRaw for mesh RPC
├── adapter.go      — Bridge to existing HandlerRegistry (RPCServer compat)
├── registry_test.go
└── http/
    ├── bridge.go   — Single generic HTTP handler (protojson, body limits)
    └── routes.go   — Auto-registration with per-handler middleware wrapping
```
