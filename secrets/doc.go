/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

// Package secrets defines the Phase-1 CRDT-encrypted secret-gossip model:
// a node holding a service role's configs seals them onto the swarm topic
// role.secrets.<role>; when the mesh detects the role missing, TrustPolicy-
// authorized, capability-matched nodes holding a wrapped DEK decrypt the
// bundle and assume the role (failover chain node.hstles.com →
// node.orbtr.io → thin-apps-as-emergency-thick-apps).
//
// It builds on the Phase-0.5-hardened swarm δ-CRDT — do NOT build a new
// CRDT. Phase 0.5 is a hard prerequisite: identity binding, sparse-topology
// convergence, observer propagation, raw-HLC validation, and immutable
// storage are not yet safe on current swarm.
//
// # The invariant that makes encryption a drop-in
//
// role.secrets.<role> is single-writer-per-(topic,nodeID): only the owning
// publisher seals+signs a record; every replica stores and re-gossips the
// received body+sig bytes VERBATIM. The Merkle leaf hashes SHA256(sig) over
// those received bytes and the HLC tie-break compares the same bytes, so all
// replicas compute identical leaves regardless of how the ciphertext was
// produced — convergence never requires deterministic sealing. The only
// hard rule: encryption must not touch topic/nodeID/HLC/tombstone/pubkey
// (the merge/merkle/sig keys — all cleartext).
//
// # Nonce rule (catastrophic if violated)
//
// Use a FRESH nonce per seal — random, or derived from a hash of the sealed
// content + recipient set. NEVER a fixed nonce per (topic,node,epoch): the
// owner re-sealing after a config change or a recipient-roster change
// without bumping epoch would reuse the nonce across two distinct
// plaintexts — an AEAD/NaCl-box nonce-reuse failure. Bump Epoch OR bind the
// nonce to content+recipients so distinct plaintexts never share one.
//
// DEK wrapping reuses diststore/crypto/recipient_wrap
// (WrapDataKeyForRecipient — NaCl-box / P256-AESKW, entitlement fail-closed).
// Recipient selection is gated by ports.TrustPolicy.AuthorizeSecretRecipient:
// capability advertisement alone NEVER makes a node a recipient.
package secrets
