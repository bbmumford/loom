# Orbtr Storage Module (mesh/storage)

Content-addressed storage primitives for the Orbtr mesh. This package provides
interfaces and helpers for chunking, publishing, and fetching immutable
artifacts over the VL1 overlay while anchoring manifests in a blockchain or
append-only control plane.

## Key Concepts

- **Manifests** — canonical description of a blob, including chunk hashes,
  metadata, and publisher signature. Stored/verified via `Manifest` helpers.
- **StoreBackend** — pluggable local persistence (disk provider included; S3, Pebble, in-memory forthcoming).
- **FetchBackend** — remote retrieval (p2p, relay, HTTP gateway, etc.).
- **ChainBackend** — lightweight abstraction to publish/resolve manifests from
  a trust anchor (EVM contract, Cosmos module, LAD topic).
- **Node** — publisher that deduplicates chunks, signs manifests, and pushes
  headers to the chain.
- **Leech** — client that hydrates content by hedging fetches across backends
  and verifying chunk hashes.

The design is documented in `storage-design.md`; this README focuses on how to
wire the code into a complete system.

## Getting Started

```go
import (
    "context"
    "os"

  storage "github.com/hstles/library/mesh/storage"
)

func publishFile(ctx context.Context, path string, node *storage.Node) error {
    f, err := os.Open(path)
    if err != nil {
        return err
    }
    defer f.Close()

    manifest, cid, err := node.Publish(ctx, f, storage.PublishOptions{ChunkSize: 4 << 20})
    if err != nil {
        return err
    }
    // Persist the manifest ID (cid) to LAD, task mesh, etc.
    _ = manifest
    _ = cid
    return nil
}
```

## Manager and Configuration

`storage.Manager` wires a store backend, chain client, and shared fetchers into
convenience helpers for publishers (`NewNode`) and leeches (`NewLeech`).

```go
cfg := storage.Config{
   Store: storage.StoreConfig{
      Provider: "disk",
      Disk: &storage.DiskStoreConfig{
        Root:       "/var/lib/orbtr/chunks",
        ShardDepth: 2,
        DirMode:    "0755",
        FileMode:   "0644",
      },
   },
}

chain := newMyChainBackend()          // implements storage.ChainBackend
fetch := newP2PFetcher(vl1Manager)    // implements storage.FetchBackend

manager, err := storage.NewManager(cfg, chain, storage.WithManagerFetchers(fetch))
if err != nil {
   log.Fatalf("storage init: %v", err)
}

node, _ := manager.NewNode()   // publishes using configured signer/fetchers
leech, _ := manager.NewLeech() // hydrates via local store + fetchers
```

### Configuration Checklist

1. Provide a `storage.Config` pointing at your desired `StoreBackend` (disk is
  available today; cloud/object stores can plug in later).
2. Register one or more `FetchBackend`s for remote retrieval (p2p, relay, HTTP).
3. Supply a `ChainBackend` implementation that publishes manifest headers and
  resolves names to manifest IDs.
4. Configure signing keys for publishers; verify keys during leech operations.
5. Optionally, implement `AnnounceProvides` to push reachability hints into LAD
  (`mesh/directory`).

## Integration with LAD and VL1

- **Reachability** — after publishing a manifest, call `lad.DirectoryCache.Apply`
  with a `ReachRecord` describing which node serves the content. VL1 peers can
  then discover the node using `PathSelector`.
- **Transport** — pair the storage `FetchBackend` with a VL1 session manager so
  chunk fetches can reuse Noise-encrypted UDP streams. For example, implement a
  `p2pFetchBackend` that dials `vl1.Manager.DialNode` and streams chunk bytes
  over the resulting session.
- **Task Mesh Hooks** — expose publish/retrieve via `mesh/task` handlers so
  gateways can request staged data migrations or deliver artifacts to executors.

## Typical Flows

### Publishing

1. Read input and chunk using `Chunker` helpers.
2. Call `StoreBackend.Put` for new chunk hashes.
3. Build a `Manifest`, sign, and send to `ChainBackend.PublishManifest`.
4. Optionally gossip manifest IDs via LAD or task offers.

### Retrieval

1. Resolve a name or manifest ID through the chain backend.
2. Load and validate the manifest using `Manifest.Validate`.
3. For each chunk hash, check the local store; if missing, execute parallel
   fetches via configured backends.
4. Reassemble the original payload or expose a random-access reader.

## Observability

The module exposes lightweight hooks for metrics:

- Wrap backends to collect cache hit ratios, fetch latency, and dedupe rates.
- Instrument leech operations with tracing spans rooted in the manifest ID.
- Emit structured events when publish completes, along with manifest size and
  chunk counts for audit.

### Disk Store

The bundled disk backend shards chunks into nested hexadecimal directories to
avoid hot folders and performs dedupe based on the content hash. It can be
configured via `DiskStoreConfig` in code or YAML (`dir_mode`, `file_mode`, and
`shard_depth`). Future providers (S3, libp2p-backed stores) will implement the
same `StoreBackend` contract.

## Next Steps

- Add additional store providers (S3, Pebble, libp2p) while reusing the manager.
- Integrate with the task module to automate publication and cache warming.
- Use LAD snapshots to bootstrap new nodes with the latest manifest pointers.
