# VL1 Overlay Transport

**Status**: ✅ Active / Production-Ready
**Package**: `library/mesh/transport`

VL1 (Virtual Layer 1) is the secure, peer-to-peer transport layer for the HSTLES mesh. It provides a flat, encrypted address space that abstracts away the underlying network topology (NATs, firewalls, cloud/edge).

---

## Core Features

*   **Zero Trust Security**: All connections are mutually authenticated and encrypted using the **Noise Protocol Framework** (IK Pattern).
*   **NAT Traversal**: Integrated **STUN** client for discovering public IPs and hole-punching through NATs.
*   **Adaptive Bootstrap**: Seamlessly switches between direct UDP (Fast Path) and HTTPS Tunnels (Slow Path) based on network conditions.
*   **Path Selection**: Intelligently routes traffic based on latency and availability scores from LAD.

---

## Building Blocks

### 1. Noise Transport (`transport/noise`)
Implements the **Noise_IK_25519_ChaChaPoly_BLAKE2b** handshake.
*   **Static Keys**: Ed25519 (converted to Curve25519) for identity.
*   **Pre-Shared Keys (PSK)**: "Network Keys" used to authorize the connection and encrypt the handshake prologue.
*   **Encryption**: ChaCha20-Poly1305 for session data.

### 2. Bootstrap (`transport/bootstrap`)
Handles the initial connection to the mesh.
*   **Snapshot**: Fetches a signed list of peers via `GET /mesh/snapshot`.
*   **Tunnel**: Establishes a WebSocket/TCP tunnel via `POST /mesh/vl1` if UDP is blocked.

### 3. STUN (`transport/nat`)
Automatically discovers the node's public IP and NAT type (Full Cone, Symmetric, etc.) to facilitate P2P connections.

### 4. Manager (`transport/transport.go`)
Coordinates the lifecycle of sessions, handles dialing, and manages keep-alives.

---

## Configuration

```go
transportCfg := noise.NoiseTransportConfig{
    LocalNode:    nodeID,
    PrivateKey:   privKey,
    NetworkKeys:  []string{"..."}, // PSKs for auth
    STUNConfig:   vl1.DefaultSTUNConfig(),
}

transport, err := noise.NewNoiseTransport(transportCfg)
```

## Usage

```go
// Initialize Manager
manager := vl1.NewManager(transport, directory)

// Dial a peer by NodeID
session, err := manager.Dial(ctx, targetNodeID)
if err != nil {
    // Handle error (e.g., peer offline)
}

// Use the session (net.Conn compatible)
_, err = session.Write([]byte("hello"))
```

## Adaptive Bootstrap Flow

1.  **Init**: Node starts up with `LADBootstrapHosts` (e.g., `login.hstles.com`).
2.  **Snapshot**: Attempts to download a peer snapshot.
    *   **Auth**: `X-Mesh-Auth` header signed with Network Key.
    *   **Verify**: Snapshot signature verified with Anchor Public Key.
3.  **Connect**:
    *   **If Snapshot OK**: Dials peers directly via UDP.
    *   **If Snapshot Fails**: Opens HTTPS tunnel to Gateway.
4.  **Steady State**: Once connected, Gossip protocol syncs full mesh state.

---

## Production Considerations

*   **Firewalls**: UDP port 41641 should be open for best performance.
*   **Network Keys**: Must be rotated periodically. The transport supports multiple keys and tries them in order.
*   **MTU**: The transport handles fragmentation, but keeping payloads under 1200 bytes is recommended for UDP efficiency.
