/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package dispatch

import (
	"context"
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ORBTR/aether"
)

// capExhaustedBackoff is the cooldown applied to Fill() / Get() after
// the underlying session's MaxConcurrentStreams cap is hit (detected
// via aether.ErrStreamCapExhausted from OpenStream). Without it, Get()
// on an empty pool would re-enter OpenStream immediately, the cap
// would refuse, and the busy-loop would burn CPU + inflate the
// session's streamRefused counter. The cooldown gives existing
// streams time to drain before the next refill attempt.
const capExhaustedBackoff = 250 * time.Millisecond

// pooledStream pairs a pre-opened stream with metadata so Get can
// reject stale streams (peer-FIN, age beyond reap window) without an
// extra round-trip. mintedAt feeds the age-based reaper.
type pooledStream struct {
	stream   aether.Stream
	mintedAt time.Time
}

// poolReapAge bounds how long a pre-opened stream stays in the pool
// before it's considered too stale to dispatch on. Per-stream window
// credit drifts between mint and use (peer may have closed, granted
// more credit, shrunk the window) so a multi-minute-old stream often
// fails Send on the first byte. Reaping aggressively keeps the pool
// "fresh" at the cost of one extra OpenStream per ttl window. Picked
// to match dedupRejectCooldown — a stream older than this is also
// older than any plausible burst-traffic correlation window.
const poolReapAge = 2 * time.Minute

// poolCanSendChecker is the interface a stream must implement to be
// admissible from the pool. CanSend is stricter than IsOpen — it
// rejects half-closed (peer-FIN) streams that IsOpen still accepts
// (the aether StreamStateMachine considers StreamHalfClosed "open"
// from one side's POV but Send fails immediately when peerFIN is set).
// Was the H-Dispatch-HalfClosedPool finding.
type poolCanSendChecker interface {
	CanSend() bool
}

// StreamPool maintains pre-opened dynamic streams for a session.
// Eliminates the OPEN frame round-trip on the hot path.
//
// Get always returns a stream that satisfies CanSend (if the stream
// type exposes it) — half-closed streams are silently discarded
// instead of being returned to a caller who would fail on Send.
//
// Pool entries that age past poolReapAge are reaped opportunistically
// by Get (on every poll) and by the per-session refill caller. No
// background goroutine, so a pool whose session is idle accumulates
// nothing extra — the refill code path already runs on every Get.
type StreamPool struct {
	mu       sync.Mutex
	streams  []pooledStream
	capacity int

	// capExhaustedUntil is set when an OpenStream call returned
	// aether.ErrStreamCapExhausted. Subsequent Fill() / Get() calls
	// short-circuit until time advances past it, avoiding the busy-loop
	// that would otherwise rapidly redial → refuse → redial. atomic.Int64
	// holds UnixNano so reads are lock-free.
	capExhaustedUntil atomic.Int64
	// capExhaustedHits counts how many times the pool observed a
	// cap-exhausted error. Surfaced for monitoring so operators can spot
	// a leaking caller without grepping logs.
	capExhaustedHits atomic.Uint64
}

// NewStreamPool creates a pool with the given capacity.
func NewStreamPool(capacity int) *StreamPool {
	return &StreamPool{
		capacity: capacity,
		streams:  make([]pooledStream, 0, capacity),
	}
}

// canDispatchOn returns true when the supplied stream is in a state where
// Send will not immediately fail. Streams whose concrete type does not
// implement CanSend fall back to IsOpen (preserves legacy behaviour).
func canDispatchOn(stream aether.Stream) bool {
	if cs, ok := stream.(poolCanSendChecker); ok {
		return cs.CanSend()
	}
	return stream.IsOpen()
}

// Fill pre-opens streams up to capacity on the given session.
//
// Cap-exhausted backoff: if a prior OpenStream returned
// aether.ErrStreamCapExhausted, Fill short-circuits until
// capExhaustedBackoff has elapsed. A session whose caller is leaking
// streams otherwise produces a steady cap-hit cycle — each Fill
// burning one OpenStream attempt every refill loop — and floods the
// session's streamRefused counter.
func (p *StreamPool) Fill(sess aether.Session) {
	if p.inCapBackoff() {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	for len(p.streams) < p.capacity {
		if sess.IsClosed() {
			return
		}
		stream, err := sess.OpenStream(context.Background(), aether.StreamConfig{
			StreamID:    nextDynamicStreamID(sess),
			Reliability: aether.ReliableOrdered,
			Priority:    128,
		})
		if err != nil {
			if errors.Is(err, aether.ErrStreamCapExhausted) {
				p.armCapBackoff()
				log.Printf("[dispatch.pool] OpenStream refused: stream cap exhausted; backing off %v (caller may be leaking streams)",
					capExhaustedBackoff)
			}
			return
		}
		p.streams = append(p.streams, pooledStream{stream: stream, mintedAt: time.Now()})
	}
}

// inCapBackoff returns true if the pool is currently in cap-exhausted
// backoff and should skip OpenStream attempts. Lock-free.
func (p *StreamPool) inCapBackoff() bool {
	until := p.capExhaustedUntil.Load()
	if until == 0 {
		return false
	}
	return time.Now().UnixNano() < until
}

// armCapBackoff records that an OpenStream call returned
// ErrStreamCapExhausted and sets the cooldown deadline.
func (p *StreamPool) armCapBackoff() {
	p.capExhaustedUntil.Store(time.Now().Add(capExhaustedBackoff).UnixNano())
	p.capExhaustedHits.Add(1)
}

// CapExhaustedHits returns the cumulative count of cap-exhausted
// OpenStream attempts observed by this pool. Operators can monitor
// this to detect a leaking caller.
func (p *StreamPool) CapExhaustedHits() uint64 {
	return p.capExhaustedHits.Load()
}

// Get returns a pre-opened stream from the pool, or opens a fresh one if
// the pool is empty. Drains stale entries (closed, half-closed, or aged
// past poolReapAge) as it walks the LIFO so the caller always receives
// a stream they can actually Send on.
func (p *StreamPool) Get(ctx context.Context, sess aether.Session) (aether.Stream, error) {
	now := time.Now()
	p.mu.Lock()
	for len(p.streams) > 0 {
		idx := len(p.streams) - 1
		entry := p.streams[idx]
		p.streams = p.streams[:idx]
		if now.Sub(entry.mintedAt) > poolReapAge {
			p.mu.Unlock()
			// Aged stream — close in the background; reacquire to continue
			// walking the pool. The pool mutex is hot; never hold across
			// Close which can do non-trivial work.
			go closeStreamQuiet(entry.stream)
			p.mu.Lock()
			continue
		}
		if !canDispatchOn(entry.stream) || sess.IsClosed() {
			p.mu.Unlock()
			go closeStreamQuiet(entry.stream)
			p.mu.Lock()
			continue
		}
		p.mu.Unlock()
		return entry.stream, nil
	}
	p.mu.Unlock()

	// Pool empty — open a fresh stream. If we're inside the cap-
	// exhausted backoff window, return the typed error immediately
	// instead of redialing into a guaranteed refusal. Avoids the
	// busy-loop Get → empty → OpenStream → refused → caller retries
	// → Get → empty → ... that would otherwise flood streamRefused
	// while no real progress is possible.
	if p.inCapBackoff() {
		return nil, aether.ErrStreamCapExhausted
	}
	stream, err := sess.OpenStream(ctx, aether.StreamConfig{
		StreamID:    nextDynamicStreamID(sess),
		Reliability: aether.ReliableOrdered,
		Priority:    128,
	})
	if err != nil && errors.Is(err, aether.ErrStreamCapExhausted) {
		p.armCapBackoff()
	}
	return stream, err
}

// closeStreamQuiet calls Close + ignores errors. Used to discard pool
// entries that fail admission — we don't want a stream-leak from a
// stale entry but we also don't care about the Close error path (the
// stream is already known-bad).
func closeStreamQuiet(s aether.Stream) {
	if s == nil {
		return
	}
	defer func() { _ = recover() }()
	_ = s.Close()
}

// Len returns the number of pre-opened streams available.
func (p *StreamPool) Len() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.streams)
}

// Drain closes all pre-opened streams. Called when the session is being
// torn down. Best-effort — errors swallowed since the session itself
// is going away.
func (p *StreamPool) Drain() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, entry := range p.streams {
		_ = entry.stream.Close()
	}
	p.streams = p.streams[:0]
}
