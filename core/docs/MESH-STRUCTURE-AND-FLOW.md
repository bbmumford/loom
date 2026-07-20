# HSTLES Mesh Package Structure & Execution Flow

**Last Updated**: December 15, 2025
**Status**: ✅ Active / Production-Ready

---

## Document Status

This document reflects the actual implementation state as of December 2025.

**Key Features**:
- **Adaptive Bootstrap**: Fast Path (Snapshot) vs. Slow Path (Tunnel).
- **Zero Trust Security**: Network Keys (HMAC/Noise) + Anchor Keys (Ed25519).
- **Unified Runtime**: Single entry point for all mesh nodes.
- **Distributed State**: MeshDB (Raft) + LAD (Gossip).

---

## Package Structure Overview

```
mesh/
├── bridge/              # HTTP-to-mesh gateway (Legacy/Transition)
│   ├── core/           # Core bridge logic
│   └── generated/      # Generated HTTP handlers
│
├── control/            # MeshDB distributed database layer
│   ├── connectors/    # Database connectors (embedded/remote Turso)
│   ├── database/      # Database handle & replica management
│   ├── manager/       # Config & manager for MeshDB
│   ├── raft/          # Raft consensus for local LAD
│   └── turso/         # Turso client wrapper
│
├── directory/          # LAD (Ledger-as-Directory) implementation
│   ├── cache/         # In-memory directory cache
│   ├── gossip/        # Peer synchronization protocol
│   ├── ledger/        # Core ledger + MeshDB persistence
│   └── types.go       # Directory data structures (NodeRecord, ReachQuery, etc.)
│
├── docs/              # Package-level documentation
│   └── README.md      # **MAIN DOCUMENTATION ENTRY POINT**
│
├── internal/          # Private utilities
│   └── bloom/        # Bloom filters for deduplication
│
├── rpc/               # RPC infrastructure
│   ├── client/       # VL1-based RPC client
│   ├── executor/     # Handler execution logic (RPC + Task)
│   └── server/       # VL1 session RPC server
│
├── runtime/           # Node runtime components
│   ├── audit/        # Audit logging
│   ├── capabilities/ # Capability-based routing
│   ├── handlers/     # Base handler types & registry
│   │   ├── handler.go      # Handler, RPCRequest, RPCResponse, HandlerRegistry
│   │   └── base_handler.go # BaseHandler implementation
│   ├── node/         # **MAIN ENTRY POINT** - Node runtime
│   │   ├── runtime.go    # Runtime struct, Initialize(), Shutdown()
│   │   ├── config.go     # Config struct & validation
│   │   ├── rpc.go        # RPCServer, RPCHandler interface
│   │   ├── identity.go   # Ed25519 key management
│   │   ├── tls.go        # TLS certificate setup
│   │   ├── health.go     # Health check handler
│   │   ├── metrics.go    # Metrics endpoint
│   │   └── resources.go  # Resource cleanup
│   └── roles/        # Role management
│
├── storage/           # Distributed object storage
│   ├── manager.go    # Storage manager
│   ├── store_disk.go # Disk-based storage backend
│   └── leech.go      # Peer-to-peer data sync
│
├── task/              # Async task execution framework
│   ├── artifacts/    # Task artifacts (input/output storage)
│   ├── bidding/      # Task bidding system
│   ├── execution/    # Task executor
│   ├── gateway/      # Task gateway (submission/results)
│   ├── queue/        # Task queue management
│   ├── transport/    # Task transport over VL1
│   ├── domain.go     # Domain-specific task types
│   └── types.go      # Core task types
│
└── transport/         # VL1 overlay network transport
    ├── bootstrap/    # Seed discovery & connection
    ├── nat/          # STUN NAT detection
    │   └── stun.go   # STUNClient (package nat)
    ├── noise/        # Noise protocol encryption
    │   └── noise.go  # NoiseTransport (package noise)
    ├── config.go     # TransportConfig, STUNConfig, NATType, ReflexiveAddress
    ├── identity.go   # NodeID generation
    ├── keys.go       # Ed25519 ↔ Curve25519 conversion
    ├── paths.go      # Path selection & scoring
    └── transport.go  # Transport interface, Session, Manager
```

## Execution Flow: node.hstles.com Startup → Request Handling

### Phase 1: Initialization (main.go → runtime/node)

```
node.hstles.com/main.go
    │
    ├─► Load Configs (apps, platform, mesh, auth, session, smtp, encryption)
    │
    ├─► Detect Roles (from ROLES env var override, or auto-detect from configs)
    │
    ├─► Build NodeConfig
    │   ├─► Identity: Ed25519 keys
    │   ├─► Network: Listen address, UDP port
    │   ├─► Security: Network Keys (PSK), Anchor Keys (Ed25519)
    │   ├─► Bootstrap: LADBootstrapHosts (e.g., login.hstles.com)
    │   └─► Components: VL1, LAD, MeshDB, Audit
    │
    └─► node.Initialize(nodeCfg)  ──┐
                                     │
┌────────────────────────────────────┘
│
▼
runtime/node/runtime.go::Initialize()
    │
    ├─► Validate Config
    │
    ├─► Load/Generate Ed25519 Identity
    │
    ├─► Initialize VL1 Transport (if enabled)
    │   ├─► noise.NewNoiseTransport()
    │   │   └─► Noise protocol setup (XX handshake)
    │   │
    │   ├─► vl1.NewManager(transport, directory)
    │   │
    │   └─► Start UDP listener
    │
    ├─► Adaptive Bootstrap (The "Magic")
    │   ├─► Attempt Fast Path: GET /mesh/snapshot (Signed + HMAC)
    │   │   ├─► Success: Load peers, skip tunnel
    │   │   └─► Failure: Fallback to Slow Path
    │   │
    │   └─► Slow Path: POST /mesh/vl1 (HTTPS Tunnel)
    │       └─► Upgrade to WebSocket/TCP -> Noise Handshake -> Gossip
    │
    ├─► Initialize LAD Directory (if enabled)
    │   ├─► directory/cache.NewDirectory()
    │   ├─► directory/ledger.NewLedger(meshDB)
    │   └─► directory/gossip.NewSynchronizer()
    │
    ├─► Initialize MeshDB (if enabled)
    │   └─► control/manager.NewManager()
    │
    ├─► Initialize RPC Server & Handlers
    │
    └─► Start Background Services (LAD sync, Health, Metrics)
```

### Phase 2: Request Handling (VL1 Session → Handler Execution)

#### A. Incoming VL1 Connection

```
UDP packet arrives on :41641
    │
    ▼
transport/noise/noise.go::Listen()
    │
    ├─► Noise handshake (XX pattern)
    │   ├─► Authenticate using Network Key (PSK)
    │   └─► Establish ChaCha20-Poly1305 encrypted session
    │
    └─► Emit IncomingSession on channel
            │
            ▼
transport/transport.go::Manager.HandleIncoming()
    │
    └─► Hand off to RPC server
            │
            ▼
runtime/node/rpc.go::RPCServer.ServeSession()
    │
    ├─► Read RPC request frame
    │   └─► JSON: {id, method, payload, timeout}
    │
    ├─► Lookup handler in registry
    │   └─► handlers[method] (e.g., "auth.login")
    │
    └─► Call handler.Handle(ctx, method, payload)
```

#### B. Handler Execution

```
domain/auth/handlers/login.go::LoginHandler.ExecuteRPC()
    │
    ├─► Validate credentials (MeshDB lookup)
    ├─► Create session
    ├─► Log audit event
    └─► Return pb.LoginResponse
```

## Key Components by Purpose

### Transport Layer (VL1)
- **Purpose**: Encrypted P2P overlay network.
- **Files**: `transport/`, `transport/nat/`, `transport/noise/`.
- **Key Feature**: Adaptive Bootstrap (Snapshot vs Tunnel).

### Directory Layer (LAD)
- **Purpose**: Distributed service discovery.
- **Files**: `directory/`.
- **Key Feature**: Gossip protocol for peer synchronization.

### Control Layer (MeshDB)
- **Purpose**: Distributed database (Raft-backed).
- **Files**: `control/`.
- **Key Feature**: Strong consistency for configuration and sessions.

### Runtime Layer
- **Purpose**: Node lifecycle and orchestration.
- **Files**: `runtime/node/`.
- **Key Feature**: Unified binary entry point.

---
Maintainer: HSTLES Platform Team
