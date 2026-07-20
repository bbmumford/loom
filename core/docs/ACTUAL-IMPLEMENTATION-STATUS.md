# HSTLES Mesh: Remaining Implementation & Optimization Roadmap

**Status**: Active Development
**Focus**: Security Hardening, Optimization, and Missing Critical Components

---

## 🔴 Missing Critical Components

### 1. Anchor Node Implementation
**Location**: `mesh/directory/anchor/` (Directory does not exist)

**Missing Functionality**:
- **Snapshot Serving**: HTTP/gRPC endpoints to serve LAD snapshots to bootstrapping nodes.
- **Snapshot Signing**: Cryptographic signing of snapshots by anchor nodes.
- **Quorum Verification**: Logic to verify snapshots against a quorum of anchor signatures (1/1, 2/3, 3/5 policies).

**Required Files**:
- `anchor.go`: Anchor node server logic.
- `snapshot.go`: Snapshot signing and serving.
- `quorum.go`: Quorum verification logic.

### 2. Gossip Peer Management
**Location**: `mesh/directory/gossip/`

**Status**: ✅ Implemented (Components ready, integration pending)
- **Dynamic Peer Discovery**: `discovery.go` implemented.
- **Peer Management**: `peer.go` implemented.
- **Gossip Protocol**: `protocol.go` implemented.

**Next Steps**:
- Wire up `PeerManager` and `DiscoveryService` in `Runtime`.
- Implement transport layer integration for `Send` function.

---

## 🟡 Partial / Incomplete Components

### 1. Bootstrap Protocol Security
**Location**: `mesh/transport/bootstrap/`

**Status**: Partial Implementation
- ✅ `connect.go`: Basic connection logic exists.
- ✅ `manifest.go`: Seed manifest types exist.
- ✅ `descriptor.go`: Root descriptor types exist.

**Missing / Needs Verification**:
- **Full Verification Logic**: Ensure `SecureBootstrap` fully implements the dual-signature verification (Tenant Root + Platform Root) for Root Descriptors.
- **Manifest Fetching**: Verify `FetchSeedManifest` correctly handles HTTPS fetching and signature validation.

### 2. Handler Metadata Publishing
**Location**: `runtime/node/runtime.go`

**Status**: ✅ Validated
- Confirmed `PublishHandlersToLAD` correctly serializes scopes and auth requirements to `lad.HandlerMetadata`.

### 3. Task Artifact Storage
**Location**: `mesh/task/artifacts/`

**Status**: ✅ Implemented (Manager integration)
- `manager.go`: Integrates `storage.Node` (upload) and `storage.Leech` (download).
- Uses `storage.DiskStore` backend.

---

## 🚀 Optimization Opportunities (Performance & Health)

### 1. Connection Pre-warming (Cold Start Mitigation)
**Status**: ✅ Implemented in `BridgeCore.Warmup()`
- Proactively queries LAD and dials critical nodes.

### 2. Parallel "Happy Eyeballs" Dialing
**Status**: ✅ Implemented in `Manager.dialPaths()`
- Races top 3 candidates for lowest latency.

### 3. Active Latency Probing
**Status**: ✅ Implemented in `BridgeCore.StartProbing()`
- Background task probes idle connections.

### 4. Load-Aware Path Selection
**Status**: ✅ Implemented in `PathScorer`
- `ReachRecord` now includes `LoadFactor`.
- `PathScorer` penalizes overloaded nodes.

---

## 📝 Documentation Debt

- **MESH-STRUCTURE-AND-FLOW.md**: Update to reflect the current package structure and implemented components.
- **API Documentation**: Ensure all new RPC endpoints and configuration options are documented.
