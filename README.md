# loom

A standalone Go mesh runtime with CRDT-encrypted secret gossip and a
universal composition surface — the mesh, reusable.

```
go get github.com/bbmumford/loom
```

loom gives a service fleet a self-forming overlay network: peers discover
each other, dial over the best available transport, route RPCs to whichever
node advertises a handler, and converge on a shared directory of who's alive
and reachable — all without a central coordinator. On top of that it adds
encrypted role hand-off (a node can seal a role's config to eligible peers,
who assume the role automatically when it goes missing) and a small
composition layer that makes every capability registerable, callable,
triggerable, and observable.

## Features

- **Self-forming overlay** — bootstrap, NAT traversal (coordinated
  hole-punching), multipath dialing, and adaptive connection scaling across
  Noise/UDP, QUIC/gRPC, WebSocket, and HTTP transports, with automatic
  transport-grade upgrades.
- **RPC mesh** — a reflection RPC registry with local-dispatch
  short-circuit, cross-mesh forwarding to the advertising node, and
  bidirectional streaming.
- **Live directory** — typed, convergent membership / reachability / role /
  handler / latency projections backed by a signed δ-CRDT, with a
  deterministic snapshot fingerprint.
- **Durable journal** — a crash-consistent, append-only record of accepted
  state with gap-free replay and point-in-time snapshots.
- **Trust policy** — fail-closed authorization (NodeID↔key binding, tenant /
  topic / role / observer / secret-recipient gates) enforced before any
  record can affect state.
- **Encrypted role secrets** — per-role config bundles sealed with
  XChaCha20-Poly1305 and per-recipient sealed-box key wraps, gossiped over
  the CRDT; eligible nodes decrypt and take over a missing role via an
  anti-thundering-herd claim set.
- **Composition surface** — a function registry, a late-binding trigger
  registry (subscribe / cron / state / http kinds), and per-owner scope
  tracking for clean activation and teardown.
- **Observability** — a layered health evaluator, a Prometheus `/metrics`
  export, and a `/mesh/status` JSON endpoint.

## Quick start

```go
package main

import (
	"log"

	"github.com/bbmumford/loom/node"
	"github.com/bbmumford/loom/ports"
)

func main() {
	rt, err := node.Initialize(node.Config{
		ServiceName: "example",
		DataDir:     "/var/lib/example",
		Platform:    ports.DevPlatform(), // inject a cloud platform in production
		// NetworkKeys, VL1, LAD, Databases, … as needed
	})
	if err != nil {
		log.Fatal(err)
	}
	defer rt.Shutdown()

	// Run the connection manager (blocks); typically launched in a goroutine.
	go rt.ConnManager().Start(rt.Context())

	// rt now exposes RPC registration, dialing, directory reads,
	// health, and the role-activation / composition surfaces.
	select {}
}
```

`node.Initialize(cfg) (*Runtime, error)` is the sole entry point. `Config`
exposes injection seams (`Platform`, `AuthValidator`, `VerifyBootstrap`,
`MeshFallbackStats`, `RegisterDomains`) so the runtime stays decoupled from
any host application; zero-value seams fall back to safe local defaults
(`Platform` excepted — cloud deployments must inject a real platform for
correct UDP bind and origin detection).

## Packages

| Package | What it is |
|---|---|
| `node` | the runtime — `Runtime`, `Config`, RPC server, sessions, directory, health, role activation, composition bridge |
| `core` | directory/gossip codecs, well-known streams, the distributed task framework, per-tenant transport manager |
| `grade` | connection quality grades and operation classes |
| `contracts/pb` | mesh transport protobufs |
| `pkg/rpc` | reflection RPC registry (`Call`, `Registry`, HTTP bridge, tenant scopes) |
| `pkg/dispatch` | local-dispatch short-circuit and mesh-caller wiring |
| `pkg/trace` | e2e trace-ID context propagation |
| `pkg/obshealth` | subsystem-health registry |
| `ports` | the interface seams: `LiveDirectory`, `DurableJournal`, `TrustPolicy`, `RoleActivator`, `PlatformInfo`, `AuthValidator` |
| `directory` | fail-closed `TrustPolicy`, a Swarm-backed `LiveDirectory` projection, and a shadow-parity comparator |
| `journal` | a crash-consistent file-backed `DurableJournal` |
| `secrets` | the `role.secrets.<role>` encrypted-gossip envelope, sealer/opener, and coverage evaluator |
| `compose` | function, trigger, and scope-tracker composition surfaces |

## Design notes

- **Single live authority.** The δ-CRDT is the only merge authority; the
  journal is a durable record and recovery source, never a second one.
- **Fresh per-seal nonce.** Every seal on `role.secrets.<role>` uses a fresh
  random nonce and data key; the AEAD additional data binds the role and
  epoch so a ciphertext can't be replayed onto another role or generation.
- **Fail-closed trust.** Authorization runs before a record can affect
  roles, reachability, routing, handlers, or secret recipients; advertising
  a capability is never sufficient to receive its secrets.

## Testing

```
go build ./...
go vet ./...
go test ./...
```

## License

Proprietary — Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights
Reserved. See [LICENSE](LICENSE).
