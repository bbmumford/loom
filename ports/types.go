/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package ports

// NodeID identifies a mesh node. It is a loom-local type: aether.NodeID is
// also a string kind, so boundary conversion is free, but the public Runtime
// surface keeps aether.NodeID for type identity with its consumers, so
// adapters convert at the port boundary only.
type NodeID string

// Topic is a partition-/network-scoped replicated-state topic string
// (e.g. "fleet.peer", "role.secrets.auth", "agent.keys.<partition>").
type Topic string

// HLC is a hybrid logical clock value in the swarm layout: 48-bit
// milliseconds high / 16-bit counter low (swarm hlc.go). Raw remote HLCs
// participate in winner comparison, so far-future values must be clamped or
// rejected BEFORE storage; the HLC.Observe clamp protects only the local
// clock.
type HLC uint64

// Watermark is a monotonic journal/stream position: a scalar sequence. The LAD
// projection derives it from a record's HLC, or from its timestamp when the
// HLC is absent (directory/ladproject.go ladWatermark).
// Consumers resume subscriptions from a Watermark — implementations must
// guarantee no gap and no silent drop between history and live delivery.
type Watermark uint64

// Record is the canonical accepted-record envelope that flows through
// LiveDirectory subscriptions and DurableJournal append/replay. Beside the
// slot identity (Topic, NodeID, Key) and the owner's Body it carries the
// ordering stamps (HLC and Lamport), the tombstone and its reason, the
// expiry, the author's public key and signature, any observer attestation,
// and the CID of a body held out of line.
//
// Provenance invariant: Body, AuthorPubKey, Signature, and Observer are the
// owner's bytes VERBATIM. Replicas and the journal never re-encode or
// re-sign them — the swarm Merkle leaf hashes SHA256(sig) over the received
// signature bytes, and re-signing a remote owner's fact destroys provenance
// and cannot pass the NodeID↔author binding TrustPolicy.VerifyNodeKey checks.
//
// 🛑 MEASURED LIMIT: THIS ENVELOPE IS FAITHFUL FOR SWARM-SIGNED RECORDS AND
// LOSSY FOR LAD-SIGNED ONES. Owner-signed provenance must survive
// PROJECTION; it does for swarm, whose signable bytes
// are fields carried here, and it does NOT for LAD. `lad.signatureContent`
// additionally covers Partition, Seq, Timestamp, and ExpiresAt at NANOSECOND
// precision — none of which exist here (ExpiresAtUnixMs is milliseconds).
// A LAD record projected into this type and mapped back FAILS
// lad.VerifyRecord; restoring exactly those four restores it, which is the
// complete loss set (directory.TestLADSignedRecordDoesNotSurviveThePortEnvelope
// pins it, with a positive control that the original record verified).
//
// ⇒ CONSEQUENCE: `anchor.Generator.Generate([]lad.Record)` cannot be fed
// from a DirectorySnapshot — every record's signature would fail to verify.
//
// TODO: widen this type with those four fields, or keep the anchor path
// reading LAD directly; either choice changes a shared type or the anchor
// input, so it is not an adapter detail.
type Record struct {
	Topic  Topic
	NodeID NodeID // owner of the (topic,node,key) slot — single writer

	// Key sub-divides a node's presence on a topic, mirroring swarm's
	// Record.Key: the slot identity is (Topic, NodeID, Key), so one node can
	// hold many records on one topic — a latency observation per observed
	// peer, a record per content hash it serves.
	//
	// Key == "" is the single-slot-per-node form.
	//
	// 🛑 EVERY slot-identity computation MUST include Key. Omitting it does
	// not merely mis-project — `journal.slotKey` drives COMPACTION, so two
	// records one node published under different keys would collapse to one
	// slot and the older ones would be deleted from the durable journal.
	// That is silent, permanent loss.
	//
	// (The per-observer latency topic convention — LatencyTopic() /
	// "fleet.latency.<from>" — gives each observer its own topic instead of a
	// Key per observed peer on one topic. Both forms are correct.)
	Key string

	HLC     HLC
	Lamport uint64 // split-brain LamportClock stamp — a WIRE field

	Tombstone       bool
	TombstoneReason string
	ExpiresAtUnixMs int64 // TTL expiry; 0 = no expiry

	Body         []byte // owner bytes, verbatim (ciphertext for role.secrets.*)
	AuthorPubKey []byte // ed25519 public key, verbatim
	Signature    []byte // ed25519 signature over the canonical signable bytes, verbatim

	// Observer is the raw third-party attestation segment, if any, verbatim.
	// Observer facts are never promoted to owner state; below-quorum
	// attestations must remain forwardable/reconcilable.
	Observer []byte

	// BlobCID references a content-addressed body for large payloads
	// (swarm ContentTopic: SHA256(Manifest)); empty when Body is inline.
	BlobCID string
}
