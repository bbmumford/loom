# LAD: Ledger-as-Directory

**Status**: ✅ Active / Production-Ready
**Package**: `library/mesh/directory`

LAD (Ledger-as-Directory) is the distributed service discovery and state propagation layer of the HSTLES mesh. It acts as a decentralized "DNS" and "Routing Table", storing information about nodes, their roles, and their reachability.

---

## Core Concepts

### 1. Records
LAD stores immutable **Records** which are appended to a local ledger.
*   **Topic**: The type of record (e.g., `TopicReach`, `TopicRole`, `TopicMember`).
*   **Body**: The JSON payload (e.g., IP address, port, capabilities).
*   **Signature**: All records are signed by the originating node's Ed25519 key.

### 2. Gossip Protocol
Nodes exchange records via a **Gossip Protocol** over VL1.
*   **Push/Pull**: Nodes periodically exchange Bloom filters to identify missing records.
*   **Epidemic Propagation**: Updates spread exponentially through the mesh.
*   **Eventual Consistency**: All nodes eventually converge on the same state.

### 3. Directory Cache
An in-memory, queryable view of the current mesh state.
*   **Reachability**: "Where is Node X?" (IP/Port).
*   **Capabilities**: "Who runs the `auth` service?"
*   **Health**: "Is Node Y online?"

---

## Components

*   **`directory/ledger`**: The persistent store. Uses **MeshDB** (local SQLite) to store records on disk.
*   **`directory/gossip`**: The synchronization engine. Handles the exchange of records with peers.
*   **`directory/cache`**: The in-memory query engine.

---

## Usage

```go
// Initialize
cache := lad.NewDirectoryCache()
ledger, _ := ledger.NewMeshDBLedger(db) // Local SQLite
syncer := gossip.NewSynchronizer(ledger, cache)

// Start Sync
go syncer.Sync(ctx, startWatermark, nil)

// Query
peers, err := cache.Reach(ctx, "hstles", lad.ReachQuery{
    Role: "auth",
})
```

## Integration with Bootstrap

LAD plays a critical role in the **Adaptive Bootstrap** process:
1.  **Snapshot**: The bootstrap snapshot is essentially a serialized dump of the LAD Cache from a Gateway.
2.  **Tunnel**: When tunneling, the node gossips its own `ReachRecord` to the Gateway, which then propagates it to the rest of the mesh.

---

## Data Model

### ReachRecord
```json
{
  "addrs": ["10.0.0.1:41641", "203.0.113.5:41641"],
  "protocol": "udp",
  "latency": 45
}
```

### RoleRecord
```json
{
  "roles": ["auth", "identity"],
  "capabilities": {
    "cpu": "high",
    "region": "us-east"
  }
}
```
