/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package ports

import (
	"context"
	"io"
)

// DurableJournal is the narrow durability port: atomic ordered
// append of ACCEPTED records, monotonic watermarks, lossless replay, restart
// recovery, retention, and snapshots. Accepted Swarm changes are journaled
// AFTER signature, identity, partition/topic authorization, clock, and resource
// validation — the journal is an audit/recovery log of canonical records,
// never a second live merge authority. Replay feeds accepted records back
// through the same projection path (LiveDirectory), not around it.
//
// journal.FileJournal is the standard implementation. An implementation must
// store every signed CRDT field Record carries, append atomically (no
// Head-then-INSERT sequence race), subscribe before it replays history, never
// silently drop records to a slow subscriber, and never let compaction delete
// a slot's latest state or tombstone. Replacing an implementation never
// removes these durability/audit semantics.
// TODO: FileJournal does not yet meet the slow-subscriber requirement: its
// live tail (fanoutLocked) drops a record when the subscriber's 256-slot
// buffer is full, and Record carries no watermark, so the subscriber cannot
// see the gap or know where to resume a Replay.
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
	// root as an uninterrupted node.
	Snapshot(ctx context.Context) (io.ReadCloser, error)

	// Compact applies retention up to a causally-stable checkpoint. It must
	// never delete a (topic,node) slot's latest state or an unacknowledged
	// tombstone — tombstone GC is governed by acknowledged checkpoints, not
	// wall-clock age.
	Compact(ctx context.Context, upTo Watermark) error

	// Close flushes and releases the journal.
	Close() error
}
