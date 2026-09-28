/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
// Package node provides the shared mesh node runtime for every service that
// participates in the loom mesh. A composition root builds one Runtime from
// Config and injects its platform, trust and authorization seams; the package
// owns the mesh lifecycle for that process.
//
// Key components:
//   - Node identity management (Ed25519 keypairs)
//   - VL1 peer-to-peer transport (Noise protocol over UDP)
//   - LAD service discovery (member/role/reach advertisement)
//   - MeshDB distributed database (Raft consensus with optional TLS)
//   - RPC protocol (structured messaging over VL1)
//   - Health monitoring (cluster health checks)
//   - Circuit breakers (fault isolation)
//   - Audit logging (security and operational events)
package node
