/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
// Package node provides unified mesh node runtime for HSTLES platform services.
//
// This package replaces service-specific mesh initialization code with a shared runtime
// that can be used by auth.hstles.com, identity.hstles.com, notify.hstles.com, and future
// mesh-participating services including ORBTR control plane nodes.
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
//
// See README.md for comprehensive usage guide and MIGRATION.md for migration from
// service-specific mesh_*.go files.
package node
