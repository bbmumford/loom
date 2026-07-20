# HSTLES Mesh Network Documentation

> **Status**: Active / Production-Ready
> **Protocol Version**: VL1 (Virtual Layer 1)
> **Last Updated**: December 15, 2025

> **Current implementation review (2026-07-18):** See [Mesh, Swarm, and Ledger Review Findings](../../MESH-SWARM-LEDGER-REVIEW-FINDINGS-2026-07-18.md) for verified connection-lifecycle, trust, convergence, persistence, and staged directory-cutover blockers. The production-ready label above describes the intended capability set and should not be used as a release-readiness assertion until those findings are reconciled.

## Overview

The **HSTLES Mesh** is a secure, self-organizing overlay network that provides connectivity, service discovery, and distributed state management for the HSTLES platform. It abstracts away the underlying network topology (cloud, edge, local) and presents a flat, secure address space for all services.

### Core Components

1.  **VL1 (Virtual Layer 1)**: The transport layer.
    *   Uses **UDP** for high-performance P2P communication.
    *   Uses **Noise Protocol (IK Pattern)** for encryption and mutual authentication.
    *   Implements **STUN** for NAT traversal and hole punching.
    *   Provides a "flat" IP-like addressing space using cryptographic Node IDs.

2.  **LAD (Ledger-as-Directory)**: The discovery layer.
    *   A distributed ledger that stores peer reachability (IP/Port) and service capabilities.
    *   Propagates updates via **Gossip** over the VL1 transport.
    *   Serves as the "DNS" and "Routing Table" of the mesh.

3.  **MeshDB**: The control plane.
    *   A distributed SQL database (SQLite/libraryQL) built on top of the mesh.
    *   Uses **Raft** consensus for strong consistency.
    *   Manages platform configuration, sessions, and critical state.

4.  **Runtime**: The node lifecycle manager.
    *   Orchestrates bootstrap, connection management, and component wiring.
    *   Implements the **Adaptive Bootstrap** logic.

---

## Architecture & Data Flow

### Node Identity
Every node is identified by a **NodeID**, which is derived from its Ed25519 public key.
*   **Identity**: `library/mesh/runtime/node/identity.go`
*   **Format**: `node_<base32_hash>`

### Security Model
The mesh uses a **Zero Trust** architecture enforced by cryptographic keys:

1.  **Network Keys (PSK)**:
    *   **Purpose**: Authorization & Encryption.
    *   **Usage**:
        *   Signs `X-Mesh-Auth` headers for Snapshot requests.
        *   Encrypts the Noise Handshake (Prologue) for VL1 connections.
    *   **Storage**: `config.NetworkKeys` (Never sent over the wire).
    *   **Rotation**: Supports multiple keys; nodes try all configured keys.

2.  **Anchor Keys (Ed25519)**:
    *   **Purpose**: Trust Root for Snapshots.
    *   **Usage**:
        *   Gateways sign snapshots with the **Private Key**.
        *   Nodes verify snapshots with the **Public Key**.
    *   **Storage**: `config.Anchor.PublicKey` / `config.Anchor.PrivateKey`.

---

## Adaptive Bootstrap Process

The mesh uses an **Adaptive Bootstrap** mechanism to maximize performance while ensuring reliability. It prefers a fast, stateless snapshot download but falls back to a secure HTTPS tunnel if the mesh is unreachable.

### Configuration
Nodes only need to know the **Bootstrap Hosts** (Gateways).
*   **Config**: `LADBootstrapHosts` (e.g., `login.hstles.com,api.hstles.com`)
*   **Note**: Explicit `Anchor.URLs` are **deprecated** and removed. The node uses `LADBootstrapHosts` for both snapshot fetching and tunneling.

### Scenario A: Active Mesh (Fast Path)
*Used when the Bootstrap Host has a valid list of peers in its LAD Database.*

```text
[ NEW NODE ]                                          [ BOOTSTRAP GATEWAY ]
     |                                                        (Login/API)
     | 1. LOAD CONFIG                                              |
     |    - LADBootstrapHosts (Source: Config)                     |
     |    - Network Key (Source: Config)                           |
     |                                                             |
     | 2. GENERATE AUTH TOKEN                                      |
     |    Token = HMAC(Timestamp, NetworkKey)                      |
     |    (Key is NEVER sent, only the signature)                  |
     |                                                             |
     | 3. REQUEST SNAPSHOT (HTTPS)                                 |
     |    GET /mesh/snapshot                                       |
     |    Header: X-Mesh-Auth: v1.<ts>.<sig>  -------------------> |
     |                                                             | 4. VERIFY AUTH
     |                                                             |    - Check HMAC using Local Network Key
     |                                                             |    - Check Timestamp (+/- 30s)
     |                                                             |
     |                                                             | 5. QUERY DATABASE
     |                                                             |    [ LAD DATABASE ]
     |                                                             |    | NodeA | 10.0.0.1 | 41641 |
     |                                                             |    | NodeB | 10.0.0.2 | 41641 |
     |                                                             |    +------------------------+
     |                                                             |
     | 6. RECEIVE SNAPSHOT                                         |
     | <---------------------------------------------------------- | 200 OK (JSON Payload)
     |    Payload: [ {NodeA, IP, Port}, ... ]                      | Signature: Signed with Anchor Priv Key
     |                                                             |
     | 7. VERIFY SNAPSHOT                                          |
     |    - Verify signature using Anchor Pub Key                  |
     |    - Populate Local Cache                                   |
     |                                                             |
     | 8. PARALLEL CONNECT (UDP)                                   |
     |    (Skips HTTPS Tunnel)                                     |
     |                                                             |
     +-----> [ EXISTING PEER A ]                                   |
     |       Handshake: Noise_IK (Encrypted with Network Key)      |
     |                                                             |
     +-----> [ EXISTING PEER B ]                                   |
             Handshake: Noise_IK (Encrypted with Network Key)
```

### Scenario B: Empty / Fallback (Slow Path)
*Used when the mesh is new, or the snapshot endpoint fails/returns empty.*

```text
[ NEW NODE ]                                          [ BOOTSTRAP GATEWAY ]
     |                                                        (Login/API)
     | 1. REQUEST SNAPSHOT (HTTPS)                                 |
     |    GET /mesh/snapshot                                       |
     |    Header: X-Mesh-Auth: ...            -------------------> |
     |                                                             |
     | <---------------------------------------------------------- | 404 Not Found OR Empty List []
     |                                                             |
     | 2. FALLBACK TO TUNNEL (HTTPS)                               |
     |    POST /mesh/vl1                                           |
     |    Upgrade: vl1                                             |
     |    X-VL1-Node-ID: <MyID>               -------------------> |
     |    X-VL1-UDP-Port: 41641                                    |
     |                                                             |
     |                                                             | 3. ACCEPT UPGRADE
     | <---------------------------------------------------------- | 101 Switching Protocols
     |                                                             | Connection: Upgrade
     |                                                             | X-VL1-Node-ID: <GatewayID>
     |                                                             |
     | =========================================================== |
     |                SECURE TCP TUNNEL ESTABLISHED                |
     | =========================================================== |
     |                                                             |
     | 4. NOISE HANDSHAKE (Inside Tunnel)                          |
     |    - Authenticates using Network Key (PSK)                  |
     |    - Establishes Session Encryption                         |
     |                                                             |
     | 5. GOSSIP (LAD SYNC)                                        |
     |    "I am Node <MyID> at <MyIP>:41641"  -------------------> |
     |                                                             | 6. UPDATE DATABASE
     |                                                             |    [ LAD DATABASE ]
     |                                                             |    + Insert: <MyID>
     |                                                             |    (Node is now discoverable by others)
     |                                                             |
     | 7. RECEIVE PEERS (If any exist)                             |
     | <---------------------------------------------------------- | "Here is Node C..."
     |                                                             |
     | 8. PUNCH UDP HOLES                                          |
     |    (Attempt to switch from TCP Tunnel to UDP P2P)           |
```

---

## Package Structure

```
library/mesh/
├── control/        # MeshDB (Raft + Turso)
├── directory/      # LAD (Ledger, Cache, Gossip)
├── docs/           # Documentation
├── rpc/            # Internal RPC framework
├── runtime/        # Node lifecycle & Bootstrap logic
│   ├── node/       # Main Runtime implementation
│   └── handlers/   # RPC Handlers
├── task/           # Distributed Task Execution
└── transport/      # VL1 (UDP + Noise)
```

## Configuration Reference

### Node Configuration (`node.Config`)

| Field | Description |
| :--- | :--- |
| `ServiceName` | Logical name of the service (e.g., "login"). |
| `NetworkKeys` | List of PSKs for authorization. |
| `LADBootstrapHosts` | Comma-separated list of bootstrap gateways (e.g., `login.hstles.com`). |
| `Anchor.PublicKey` | Ed25519 Public Key for verifying snapshots. |
| `Anchor.PrivateKey` | Ed25519 Private Key for signing snapshots (Gateways only). |
| `VL1UDPPort` | Local UDP port for mesh traffic (default: 41641). |

### Environment Variables

*   `MESH_NETWORK_KEYS`: Comma-separated list of hex-encoded keys.
*   `MESH_ANCHOR_PUBLIC_KEY`: Hex-encoded public key.
*   `MESH_BOOTSTRAP_HOSTS`: Comma-separated list of hostnames.

---

## Usage Example

```go
import "github.com/hstles/library/mesh/runtime/node"

func main() {
    cfg := node.Config{
        ServiceName: "my-service",
        NetworkKeys: []string{"..."},
        LADBootstrapHosts: "login.hstles.com,api.hstles.com",
        Anchor: node.AnchorConfig{
            PublicKey: "...",
        },
        VL1UDPPort: 41641,
    }

    // Initialize Runtime
    // This automatically handles:
    // 1. Identity loading/generation
    // 2. Adaptive Bootstrap (Snapshot -> Tunnel)
    // 3. VL1 Listener start
    // 4. Background Gossip
    rt, err := node.Initialize(cfg)
    if err != nil {
        log.Fatal(err)
    }
    defer rt.Shutdown()

    // Use the mesh...
}
```
