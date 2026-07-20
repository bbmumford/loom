/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package ports

import (
	"context"
	"io"
)

// DurableJournal is the narrow durability port (Phase 0.5.1): atomic ordered
// append of ACCEPTED records, monotonic watermarks, lossless replay, restart
// recovery, retention, and snapshots. Accepted Swarm changes are journaled
// AFTER signature, identity, tenant/topic authorization, clock, and resource
// validation — the journal is an audit/recovery log of canonical records,
// never a second live merge authority. Replay feeds accepted records back
// through the same projection path (LiveDirectory), not around it.
//
// Initial implementation: a hardened Ledger/MeshLedger adapter. Known gaps
// it must close first (plan B.4): the current schema omits the signed CRDT
// fields (Record carries them all); auto-seq Head-then-INSERT is not atomic;
// history is queried before live subscribe; slow subscribers are silently
// dropped; compaction can delete the latest state/tombstone. The Ledger
// package dependency is removed LAST (Phase 0.5.3 step 6) — removal selects
// a new journal implementation, it never deletes durability/audit semantics.
type DurableJournal interface {
	// Append durably appends one accepted record and returns its watermark.
	// Appends are atomic and totally ordered; watermarks are strictly
	// monotonic. Concurrent appenders must serialize inside the
	// implementation (no read-modify-write seq races).
	Append(ctx context.Context, rec Record) (Watermark, error)

	// BatchAppend appends all records atomically (all-or-nothing) in order.
	BatchAppend(ctx context.Context, recs []Record) (Watermark, error)

	// Head returns the highest durable watermark.
	Head(ctx context.Context) (Watermark, error)

	// Replay streams records with watermark > from for the given topics
	// (nil topics = all), history first then live, with no gap and no
	// duplicate-free guarantee violated at the handoff. Every signed field
	// round-trips byte-identical. Cancel ctx to stop.
	Replay(ctx context.Context, from Watermark, topics []Topic) (<-chan Record, error)

	// ReplayUntil streams records with from < watermark ≤ to and CLOSES
	// the channel — the terminating form projection layers use to rebuild
	// state at boot (to = Head()) without subscribing to the live tail.
	ReplayUntil(ctx context.Context, from, to Watermark, topics []Topic) (<-chan Record, error)

	// Snapshot returns a consistent serialized checkpoint. Restarting from
	// snapshot+journal must reproduce the same live projection and Merkle
	// root as an uninterrupted node (§0.5.4).
	Snapshot(ctx context.Context) (io.ReadCloser, error)

	// Compact applies retention up to a causally-stable checkpoint. It must
	// never delete a (topic,node) slot's latest state or an unacknowledged
	// tombstone — tombstone GC is governed by acknowledged checkpoints, not
	// wall-clock age.
	Compact(ctx context.Context, upTo Watermark) error

	// Close flushes and releases the journal.
	Close() error
}
