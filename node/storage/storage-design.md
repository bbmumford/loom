# 🚀 Blockchain‑Backed Decentralized Storage Module — Build Plan (Canvas)

A Go module enabling content‑addressed chunk storage and retrieval over an unreliable, address‑less network. Uses a blockchain control plane for discovery, integrity, and versioning; libp2p for peer fabric; pluggable storage/fetch backends for data.

---

## 1) High‑Level Architecture

**Planes**

* **Data plane (chunks):** Content‑addressed blobs retrieved from nodes/peers/gateways/cloud.
* **Control plane (chain):** Blockchain stores manifests, name→manifest pointers, and optional provider attestations.
* **Peer fabric:** libp2p (QUIC, DHT, relays, WebRTC) for identity, discovery, NAT traversal.

**Module surfaces**

* `StoreBackend`, `FetchBackend`, `ChainBackend`, `Node`, `Leech`, `Manifest` (clean Go interfaces).

---

## 2) What Goes On‑Chain (Compact & Verifiable)

**Manifest record (header)**

* `manifest_id` (hash of canonical manifest bytes / CID)
* `size`, `chunk_size`
* `chunks_merkle_root` (commit to all chunk hashes)
* `publisher_pubkey`, `publisher_signature`
* `timestamp`, `prev_manifest_id`
* (Optional) `enc_hint` (e.g., key id/alg)

**Name registry (optional)**

* `name → manifest_id` (latest), settable by owner key; enables human‑readable addressing.

**Provider attestations (optional)**

* Minimal claims: `peer_id`, `content_bitmap_root` (Bloom/Merkle).
* Keep sparse on‑chain; rely on DHT/pubsub off‑chain for the live index.

**Chain choices**

* **EVM L2 contract:** fastest to bootstrap (abigen, event logs, indexers).
* **Cosmos SDK/Tendermint:** purpose‑built state machine, fast finality, clean modules.

---

## 3) Address‑less Discovery (No Static Endpoints)

* **Identity:** libp2p `PeerID` (pubkey‑derived).
* **Discovery:** Kademlia DHT keyed by `chunk_hash` + `manifest_id` → provider records (signed, TTL’d).
* **Transport:** QUIC/UDP with hole‑punch; fallback **circuit‑relay v2**.
* **Browser path:** libp2p WebRTC (DTLS DataChannels) with STUN/TURN; rendezvous via pubsub or chain ID.

---

## 4) Data Model (Go Types)

```go
type Hash = [32]byte // sha256

type ChunkRef struct {
    Index int
    Hash  Hash
    Size  uint32
}

type Manifest struct {
    ContentID      [32]byte
    Version        uint64
    ChunkSize      uint32
    Chunks         []ChunkRef
    PrevManifestID *[32]byte
    Meta           map[string]string // entrypoint, mime, etc.
    Signature      []byte            // over canonical encoding
}

type StoreBackend interface {
    Put(hash Hash, data []byte) error
    Get(hash Hash) ([]byte, error)
    Has(hash Hash) bool
    Evict(hash Hash) error
    Stats() map[string]any
}

type FetchBackend interface {
    Fetch(ctx context.Context, hash Hash) ([]byte, error)
}

type ChainBackend interface {
    PublishManifest(ctx context.Context, m *Manifest) ([32]byte, error)
    ResolveName(ctx context.Context, name string) ([32]byte, error)
    GetManifestHeader(ctx context.Context, id [32]byte) (*ManifestHeader, error)
    WatchUpdates(ctx context.Context, pointer string) (<-chan UpdateEvent, error)
    AnnounceProvides(ctx context.Context, peerID string, root Hash) error // optional
}
```

---

## 5) Pluggable Backends

**StoreBackend (seeders & cache)**

* `disk://path` → `./chunks/<hex(hash)>`
* `badger://` or `pebble://` (embedded KV)
* `s3://bucket/prefix` (with SSE or client‑side AES‑GCM)
* `ipfs://` (pin by CID mapped from chunk hash)
* `inmem://` (testing)

**FetchBackend (clients)**

* `p2p://` libp2p (DHT → direct QUIC stream)
* `relay://` circuit‑relay v2
* `webrtc://` browser transport
* `http(s)://` gateway fallback
* `ipfs://` get/pin

Policy: leech hedges parallel fetches across backends with jittered timeouts; first valid wins.

---

## 6) Protocol Flows

**Publishing (Node)**

1. Stream chunking: `Chunker.Split(r, size)` → `sha256` for `chunk_hash`.
2. `store.Put(chunk_hash, data)` if not present (dedupe by hash).
3. Build `Manifest{Chunks, ChunkSize, Prev, Meta}`, sign.
4. `chain.PublishManifest(manifest)` → returns `manifest_id`.
5. (Optional) `AnnounceProvides(peer_id, bloom_root)`.
6. Pubsub gossip on `manifests:<manifest_id>` for quick propagation.

**Retrieval (Leech)**

1. Resolve name → `manifest_id` via `chain.ResolveName` → fetch header.
2. Load full manifest via CAS (IPFS/http) and verify against header hash/signature.
3. For each `chunk_hash`:

   * Check local store; else issue hedged `Fetch` (p2p/relay/http/ipfs).
   * Verify `sha256(data) == chunk_hash`; then `store.Put`.
4. Reassemble file/stream. For DB files, expose a read‑through **virtual file** with random‑access chunk fetch.

---

## 7) NAT, Relays, and Reachability

* Enable libp2p **AutoNAT**, **Autorelay**, **relay‑hop** for stubborn NATs.
* Rotate signed multiaddrs; publish periodically via DHT/provider records.
* Browser clients: WebRTC + TURN fallback; gateway bridge for non‑peered networks.

---

## 8) Security Model

* **Integrity:** content addressing (sha256); manifest merkle + signature; header pinned on chain.
* **Authenticity:** publisher key controls name→manifest; clients verify signature matches on‑chain key.
* **Confidentiality (opt‑in):** per‑chunk or per‑manifest AES‑GCM; keys out‑of‑band or capabilities; enables anonymous storage on untrusted nodes.
* **Availability:** multi‑source fetch (p2p + gateways + pins). Optional incentives later.

---

## 9) Consistency & Updates

* **Immutable manifests:** append‑only; link via `prev_manifest_id`.
* **Latest pointer:** name registry maps to current `manifest_id`.
* **Partial updates:** unchanged chunk hashes reused; only new chunks uploaded.
* **GC:** LRU cache & pin‑sets in StoreBackend; chain policies can hint retain windows.

---

## 10) Incentives (Future)

* Serve receipts: peers sign short‑lived acknowledgements when serving chunks.
* Periodic micro‑settlement on‑chain or via payment channels.
* Not required for MVP.

---

## 11) Repository & Package Layout (Go)

```
module your.org/storage

/chain
  evm/        // abigen bindings, tx builder, event filters
  cosmos/     // tendermint light client
  mock/       // test doubles

/p2p
  host.go     // libp2p host builder (QUIC, relay, webrtc)
  dht.go
  fetcher.go  // FetchBackend (p2p)
  pubsub.go

/store
  disk/
  badger/
  s3/
  ipfs/
  inmem/

/manifest
  manifest.go // canonical CBOR/JSON, signing, verify
  merkle.go

/chunker
  chunker.go  // streaming chunker (io.Reader)
  ioctx.go

/node
  node.go     // AddContent, ServeChunk, Announce

/leech
  leech.go    // LoadManifest, FetchChunk, Reconstruct

/cmd
  storagenode/
  storageleech/

/internal/codec  // canonical encoders (CBOR preferred)
/internal/crypto // ed25519, aes-gcm, hkdf
```

---

## 12) Pseudocode — Streaming Publisher

```go
func (n *Node) Publish(ctx context.Context, r io.Reader, chunkSize int) (*Manifest, [32]byte, error) {
    var chunks []ChunkRef
    buf := make([]byte, chunkSize)
    idx := 0

    for {
        nRead, err := io.ReadFull(r, buf)
        if err == io.ErrUnexpectedEOF || err == io.EOF {
            if nRead == 0 { break }
        } else if err != nil && err != io.ErrUnexpectedEOF {
            return nil, [32]byte{}, err
        }

        data := buf[:nRead]
        ch := sha256.Sum256(data)
        if !n.store.Has(ch) {
            if err := n.store.Put(ch, data); err != nil { return nil, [32]byte{}, err }
        }
        chunks = append(chunks, ChunkRef{Index: idx, Hash: ch, Size: uint32(nRead)})
        idx++
    }

    m := Manifest{Version: timeNowVersion(), ChunkSize: uint32(chunkSize), Chunks: chunks}
    signManifest(&m, n.publisherKey)

    mid, err := n.chain.PublishManifest(ctx, &m)
    if err != nil { return nil, [32]byte{}, err }

    return &m, mid, nil
}
```

---

## 13) Testing & Observability

* Deterministic vectors for chunking/hashing; golden manifests.
* In‑proc libp2p net for NAT/relay regression.
* OTel tracing around fetch paths; metrics: cache hit/miss, latency, chunk repair rate, provider counts.

---

## 14) MVP Scope (90/10)

1. **Chain:** EVM L2 contract with `publishManifest(header)`, `setName(name, manifestId)`, events.
2. **P2P:** libp2p host (QUIC + relay), Kademlia DHT provider ads by `chunk_hash`.
3. **Storage:** Disk store; p2p fetcher; HTTP gateway fallback.
4. **CLI:** `storagenode publish <file>`, `storageleech get <name|manifestId> -o out`.
5. **Browser:** Minimal WebRTC leech → bridge to local node.

---

## 15) Key Trade‑offs & Defaults

* **Chain:** EVM L2 for speed to market; Cosmos later for bespoke control.
* **Chunk size:** 4–16 MiB (DBs prefer larger for throughput; web assets smaller). Power‑of‑two sizes.
* **Encoding:** Canonical CBOR for manifests (stable hashing), JSON for tooling UX.
* **Crypto:** ed25519 for publisher keys; AES‑GCM for encryption; HKDF for key derivation.
* **Hedged fetch:** p2p first, then relay, then gateway; cancel on first valid.

---

### ✅ Outcome

A robust Go module that:

* Operates without static endpoints
* Verifiably stores and retrieves content by hash
* Uses a blockchain to pin integrity and version pointers
* Works across nodes, relays, browsers, and cloud backends with clean, pluggable interfaces.
