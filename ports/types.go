/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package ports

// NodeID identifies a mesh node. Provisional loom-local type: aether.NodeID
// is also a string kind, so boundary conversion is free — but the Phase-0
// extraction must preserve type identity on the public Runtime surface
// (Appendix A), so exported mesh APIs keep aether.NodeID and adapters
// convert at the port boundary only.
type NodeID string

// Topic is a tenant-/network-scoped replicated-state topic string
// (e.g. "fleet.peer", "role.secrets.auth", "agent.keys.<tenant>").
type Topic string

// HLC is a hybrid logical clock value in the swarm layout: 48-bit
// milliseconds high / 16-bit counter low (swarm hlc.go). Raw remote HLCs
// participate in winner comparison — Phase 0.5 requires far-future values
// to be clamped/rejected BEFORE storage; the existing HLC.Observe clamp
// protects only the local clock.
type HLC uint64

// Watermark is a monotonic journal/stream position. Provisional: a scalar
// sequence; the transitional adapter maps lad.CausalWatermark onto it.
// Consumers resume subscriptions from a Watermark — implementations must
// guarantee no gap and no silent drop between history and live delivery.
type Watermark uint64

// Record is the canonical accepted-record envelope that flows through
// LiveDirectory subscriptions and DurableJournal append/replay. It carries
// every field the current mesh_ledger schema omits (plan B.4 correction):
// HLC + Lamport, tombstone + reason, expiry, author key, and blob CID.
//
// Provenance invariant: Body, AuthorPubKey, Signature, and Observer are the
// owner's bytes VERBATIM. Replicas and the journal never re-encode or
// re-sign them — the swarm Merkle leaf hashes SHA256(sig) over the received
// signature bytes, and re-signing a remote owner's fact (the current LAD
// bridge behaviour) destroys provenance and cannot pass the Phase-0.5
// NodeID↔author policy.
//
// 🛑 MEASURED LIMIT (#M-513): THIS ENVELOPE IS FAITHFUL FOR SWARM-SIGNED
// RECORDS AND LOSSY FOR LAD-SIGNED ONES. §0.5.4 requires owner-signed
// provenance to survive PROJECTION; it does for swarm, whose signable bytes
// are fields carried here, and it does NOT for LAD. `lad.signatureContent`
// additionally covers TenantID, Seq, Timestamp, and ExpiresAt at NANOSECOND
// precision — none of which exist here (ExpiresAtUnixMs is milliseconds).
// A LAD record projected into this type and mapped back FAILS
// lad.VerifyRecord; restoring exactly those four restores it, which is the
// complete loss set (directory.TestLADSignedRecordDoesNotSurviveThePortEnvelope
// pins it, with a positive control that the original record verified).
//
// ⇒ CONSEQUENCE FOR STAGE 2: `anchor.Generator.Generate([]lad.Record)` cannot
// be fed from a DirectorySnapshot until this is resolved — every record's
// signature would fail to verify. Resolving it means widening this type
// (a shared-type change affecting other lanes) or leaving the anchor path
// reading LAD directly, which §0.5.3 stage 1 forbids. That is an @R decision,
// not an adapter detail.
type Record struct {
	Topic  Topic
	NodeID NodeID // owner of the (topic,node,key) slot — single writer

	// Key sub-divides a node's presence on a topic, mirroring swarm's
	// Record.Key: the slot identity is (Topic, NodeID, Key), so one node can
	// hold many records on one topic — a latency observation per observed
	// peer, a record per content hash it serves.
	//
	// Key == "" is the classical single-slot-per-node form and is what every
	// record carries today, so nothing changes for existing producers.
	//
	// 🛑 EVERY slot-identity computation MUST include Key. Omitting it does
	// not merely mis-project — `journal.slotKey` drives COMPACTION, so two
	// records one node published under different keys would collapse to one
	// slot and the older ones would be deleted from the durable journal.
	// That is silent, permanent loss, and it is why this field exists here
	// before the cutover wires anything.
	//
	// (The per-observer latency topic convention — LatencyTopic() /
	// "fleet.latency.<from>" — predates this field and exists only because a
	// node could not previously hold multiple records on one topic. It stays
	// correct; it is no longer the only option.)
	Key string

	HLC     HLC
	Lamport uint64 // split-brain LamportClock stamp — a WIRE field (no-drift list)

	Tombstone       bool
	TombstoneReason string
	ExpiresAtUnixMs int64 // TTL expiry; 0 = no expiry

	Body         []byte // owner bytes, verbatim (ciphertext for role.secrets.*)
	AuthorPubKey []byte // ed25519 public key, verbatim
	Signature    []byte // ed25519 signature over the canonical signable bytes, verbatim

	// Observer is the raw third-party attestation segment, if any, verbatim.
	// Observer facts are never promoted to owner state; below-quorum
	// attestations must remain forwardable/reconcilable (Phase-0.5 blocker 3).
	Observer []byte

	// BlobCID references a content-addressed body for large payloads
	// (swarm ContentTopic: SHA256(Manifest)); empty when Body is inline.
	BlobCID string
}
