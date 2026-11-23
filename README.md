# loom

loom is a Go mesh runtime: a self-forming overlay network for a fleet of
services, with RPC across the mesh, a shared live directory, encrypted role
hand-off and a small composition surface.

Peers discover each other, dial over the best transport available, route RPCs
to whichever node advertises a handler, and converge on a shared directory of
who is alive and reachable, without a central coordinator. A node can seal a
role's configuration to eligible peers, which take the role over when it goes
missing. Every capability a node offers can be registered, called, triggered
and observed through one composition layer.

- **Self-forming overlay.** Bootstrap with an admission check, NAT traversal
  (coordinated hole punching), multipath dialing and adaptive connection
  scaling over aether's Noise/UDP, QUIC/gRPC, WebSocket and HTTP transports,
  upgrading a connection when a better transport grade becomes available.
- **RPC mesh.** A reflection RPC registry (`pkg/rpc`) whose calls run locally
  when this node holds the handler and are forwarded to the advertising node
  otherwise, with bidirectional streaming, partition scopes and an HTTP bridge.
- **Live directory.** Typed, convergent views of membership, reachability,
  roles, handlers and latency, built on swarm's signed delta-state CRDT, with a
  deterministic snapshot fingerprint.
- **Durable journal.** A crash-consistent, append-only record of accepted state
  with gap-free replay and point-in-time snapshots.
- **Trust policy.** Fail-closed authorization (NodeID-to-key binding,
  partition, topic, role, observer and secret-recipient gates) that runs before
  any record can change state.
- **Encrypted role secrets.** Per-role configuration bundles sealed with
  XChaCha20-Poly1305 and wrapped per recipient (diststore's recipient wrap),
  gossiped over the CRDT; eligible nodes decrypt them and take over a missing
  role through a claim set that stops a thundering herd.
- **Composition surface.** A function registry, a late-binding trigger
  registry (subscribe, cron, state and HTTP triggers) and per-scope tracking
  for clean activation and teardown.
- **Distributed tasks.** A task queue with bidding, award rules, lease
  takeover, a gateway and artifact transfer (`core/task`).
- **Observability.** A layered health evaluator, a Prometheus `/metrics`
  export and a `/mesh/status` JSON endpoint.

## Use

```sh
go get github.com/bbmumford/loom
```

```go
rt, err := node.Initialize(node.Config{
	ServiceName: "example",
	DataDir:     "/var/lib/example",
	Platform:    ports.DevPlatform(), // inject a cloud platform in production
	// NetworkKeys, VL1, LAD, Databases and the other sections as needed
})
if err != nil {
	log.Fatal(err)
}
defer rt.Shutdown()

go rt.ConnManager().Start(rt.Context())
```

`node.Initialize(cfg) (*Runtime, error)` is the one entry point. `Config`
carries the injection seams (`Platform`, `AuthValidator`, `VerifyBootstrap`,
`MeshFallbackStats`, `RegisterDomains`), so the runtime stays independent of
the application hosting it. A zero-value seam falls back to a safe local
default, except `Platform`: a cloud deployment must inject a real platform for
the right UDP bind and origin detection. `Runtime.InitSwarm` and
`Runtime.InitRoute` start the swarm fabric and the route engine on it.

## Packages

| package | what it is |
|---|---|
| `node` | the runtime: `Runtime`, `Config`, the RPC server, sessions, the directory, health, role activation, the composition bridge |
| `node/handlers`, `node/capabilities`, `node/storage`, `node/meshstatus`, `node/audit` | the node's built-in handlers and scopes, its capability and role selection, its content storage, the `/mesh/status` view and its audit hooks |
| `pkg/rpc` | the reflection RPC registry (`Call`, `Registry`), with `pkg/rpc/scope` (the scope tags), `pkg/rpc/http` (the HTTP bridge), `pkg/rpc/effect` (an operation's declared effect class), `pkg/rpc/registrycheck` and `pkg/rpc/rpctest` |
| `pkg/dispatch` | the local-dispatch short cut and the mesh caller |
| `pkg/trace`, `pkg/obshealth`, `pkg/slog` | trace-id propagation, the subsystem health registry, subsystem-scoped structured logs |
| `ports` | the interface seams: `LiveDirectory`, `DurableJournal`, `TrustPolicy`, `RoleActivator`, `PlatformInfo`, `AuthValidator` |
| `directory` | the fail-closed `TrustPolicy`, the swarm-backed `LiveDirectory` and a parity comparator |
| `journal` | the crash-consistent, file-backed `DurableJournal` |
| `secrets` | the `role.secrets.<role>` envelope, its sealer and opener, and the coverage evaluator |
| `compose` | the function, trigger and scope-tracker registries |
| `anchor` | the bootstrap admission check a node asks an anchor-capable peer before admitting a joiner |
| `netpolicy` | turns live link signals (link type, RTT, loss, battery, churn) into the network profile every cadence reads |
| `grade` | connection quality grades and operation classes |
| `core/...` | directory and gossip codecs, the well-known streams, the task framework, the per-partition transport manager |
| `contracts/pb`, `anchor/pb` | the mesh transport and anchor protobufs |

## Design notes

- **One live authority.** The delta-state CRDT is the only merge authority;
  the journal is a durable record and recovery source, never a second one.
- **A fresh nonce per seal.** Every seal on `role.secrets.<role>` uses a fresh
  random nonce and data key; the AEAD additional data binds the role and epoch,
  so a ciphertext cannot be replayed onto another role or generation.
- **Fail-closed trust.** Authorization runs before a record can affect roles,
  reachability, routing, handlers or secret recipients; advertising a
  capability is never enough to receive its secrets.

## Dependencies

Other packages of this estate:

- `github.com/ORBTR/aether` (with `adapter`, `noise`, `grpc`, `multipath`,
  `nat`, `discovery`, `flow`, `metrics`): the transports and sessions.
- `github.com/bbmumford/swarm`: gossip, the CRDT store and anti-entropy.
- `github.com/bbmumford/route`: path selection over the swarm fabric.
- `github.com/bbmumford/reach`: publishing the node's own addresses.
- `github.com/bbmumford/ledger` (with `anchor`, `cache`, `storage`): the
  directory's record store.
- `github.com/bbmumford/whisper` (with `iblt`): topic gossip used by the
  directory's gossip layer.
- `github.com/bbmumford/tursoraft` (`localdb`, `manager`, `mesh`): the node's
  replicated databases.
- `github.com/bbmumford/policypool`: the fail-closed connection policy of the
  directory's local store (`node/lad_store.go`).
- `github.com/bbmumford/diststore/crypto`: the per-recipient key wrap.
- `github.com/bbmumford/weft` and `github.com/bbmumford/cfgload`: HTTP helpers
  and configuration loading. (`go.mod` also requires
  `github.com/bbmumford/envreq`, which no loom package imports.)

External modules: `google.golang.org/grpc`, `google.golang.org/protobuf`,
`github.com/hashicorp/raft`, `github.com/hashicorp/yamux`,
`github.com/hashicorp/go-multierror`, `github.com/go-chi/chi/v5`,
`github.com/gobwas/ws`, `github.com/soheilhy/cmux`, `github.com/google/uuid`,
`github.com/shirou/gopsutil/v3`, `filippo.io/edwards25519`,
`golang.org/x/crypto`, `golang.org/x/sync` and `golang.org/x/tools`.

## Used by

Importers in the ORBTR estate, mostly of `pkg/rpc`, `pkg/rpc/scope`,
`pkg/dispatch` and `node`:

- `github.com/ORBTR/Library` (41 packages), `github.com/ORBTR/cortex`
  (140 packages) and `github.com/ORBTR/constellation` (74 packages): every RPC
  domain registers on `pkg/rpc`, and the endpoint runtime starts a `node`.
- The AI endpoints `api`, `app`, `compute`, `fleet`, `node`, `runtime` and
  `worker.orbtr.ai` (`github.com/ORBTR/api.orbtr.ai`,
  `github.com/ORBTR/ai/platform/endpoints/...`) and `github.com/ORBTR/xyz`.
- The IO endpoints `app`, `devices`, `get`, `help`, `mcp`, `node` and
  `relay.orbtr.io`, `github.com/ORBTR/agent` (`pkg/dispatch`, `pkg/rpc`,
  `pkg/obshealth`, `core/streams`, `grade`) and `github.com/ORBTR/tools`.
- In this estate's packages: `github.com/bbmumford/atlas` (`ports`),
  `github.com/bbmumford/shuttle` (`ports`, `pkg/rpc/scope`) and
  `github.com/bbmumford/warrant` (`pkg/rpc/scope`).

## Platforms and build

Pure Go, no cgo. GOOS=js is not a build target: `node` and
`core/transport/manager` reach the network through aether's Noise/UDP and QUIC
transports, which have no js build. The module's `go` directive is 1.25.0.

```sh
go build ./...
go vet ./...
go test ./...
```

`go test` at the root runs a naming guard over the module's identifiers,
comments, literals, file names and `.proto` files.

## Licence

MIT. Copyright (c) 2026 Brandon Miller-Mumford. The full text is in
[LICENSE](LICENSE). `core/internal/ewma` is ported from pollen's
`pkg/observability/metrics/ewma.go` and keeps its Apache-2.0 origin, as its
package documentation says.

## Contributing

[AGENTS.md](AGENTS.md) holds the repository's two rules, for people and coding
agents alike: no temporary documents in the repository, and conventional commit
messages that stand on their own. [CLAUDE.md](CLAUDE.md) imports it for Claude
Code.
