# Mesh Runtime Layer

**Status**: ✅ Active / Production-Ready
**Package**: `library/mesh/runtime`

The **Runtime Layer** is the heart of a HSTLES mesh node. It orchestrates the lifecycle of the node, manages identity, handles bootstrapping, and executes RPC/Task workloads.

---

## Core Components

### 1. Node Runtime (`runtime/node`)
The `node` package provides the `Runtime` struct and `Initialize()` function. It is the entry point for any service joining the mesh.

**Responsibilities**:
*   **Identity**: Loads or generates Ed25519 keys (`node_<id>`).
*   **Bootstrap**: Performs **Adaptive Bootstrap** (Snapshot -> Tunnel) to join the mesh.
*   **Transport**: Initializes the VL1 overlay (UDP + Noise).
*   **Directory**: Manages the LAD cache and Gossip synchronizer.
*   **Database**: Initializes MeshDB (if enabled).
*   **RPC**: Starts the RPC server and registers handlers.

### 2. Role Management (`runtime/roles`)
Nodes are assigned **Roles** (e.g., `auth`, `identity`, `gateway`) via the `ROLES` environment variable (override mode). If not set, roles are auto-detected based on available configurations. The `roles` package manages the registration of domain-specific handlers based on these roles.

**Example**:
```go
// In main.go
roles := os.Getenv("ROLES") // "auth,identity" (override mode)
nodeCfg.Roles = strings.Split(roles, ",")
```

### 3. Handler Registry (`runtime/handlers`)
The registry maps RPC method names (e.g., `auth.login`) to executable handlers. It supports both:
*   **RPC Handlers**: Synchronous, low-latency requests.
*   **Task Handlers**: Asynchronous, long-running jobs.

### 4. Capabilities (`runtime/capabilities`)
Nodes advertise their capabilities (CPU, RAM, Roles) in the LAD directory. This allows the mesh to route tasks to the most suitable nodes.

### 5. Audit (`runtime/audit`)
Provides structured logging for security-critical events (e.g., login attempts, permission changes).

---

## Usage

### Initialization

```go
import "github.com/hstles/library/mesh/runtime/node"

func main() {
    cfg := node.Config{
        ServiceName: "my-service",
        // ... config ...
    }

    rt, err := node.Initialize(cfg)
    if err != nil {
        log.Fatal(err)
    }
    defer rt.Shutdown()

    // Block until shutdown signal
    <-rt.Done()
}
```

### Adding a Handler

Handlers are typically generated from Protobuf definitions, but can be manually registered:

```go
type MyHandler struct{}

func (h *MyHandler) Handle(ctx context.Context, method string, payload []byte) (*RPCResponse, error) {
    return &RPCResponse{Success: true, Data: []byte("pong")}, nil
}

// Register
rt.RPCServer().RegisterHandler("my.ping", &MyHandler{})
```

---

## Adaptive Bootstrap

The runtime automatically handles the connection to the mesh:

1.  **Snapshot (Fast Path)**: Tries to fetch a signed peer list from `LADBootstrapHosts` via HTTPS (`GET /mesh/snapshot`).
2.  **Tunnel (Slow Path)**: If snapshot fails, opens a secure HTTPS tunnel (`POST /mesh/vl1`) to gossip and discover peers.
3.  **P2P (Steady State)**: Once peers are known, switches to direct UDP communication.

See `library/mesh/docs/README.md` for the full architecture.
