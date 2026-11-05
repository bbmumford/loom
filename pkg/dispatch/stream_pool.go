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

// streamBackoff is how long a session's dynamic-stream opens wait after the
// session or its peer answered one with backpressure (isStreamBackpressure).
// Without it, Get on an empty pool would re-enter OpenStream at once, be refused
// again, and burn CPU and inflate the session's streamRefused counter. The
// cooldown gives existing streams time to drain before the next open. Every
// allocator on the session reads it through streamIDsFor (openDynamicStreamOn).
const streamBackoff = 250 * time.Millisecond

// isStreamBackpressure reports whether err says the session or its peer has no room
// for another stream or more bytes, with the session still up. OpenStream
// returns aether.ErrStreamCapExhausted and aether.ErrStreamCreditRefused; Send and
// Receive return aether.ErrStreamRefused and aether.ErrStreamOverrun, on Noise and
// on byte-stream sessions alike, because the peer answers an admitted OPEN or a full
// queue with a RESET that arrives after OpenStream has returned.
func isStreamBackpressure(err error) bool {
	return errors.Is(err, aether.ErrStreamCapExhausted) ||
		errors.Is(err, aether.ErrStreamCreditRefused) ||
		errors.Is(err, aether.ErrStreamRefused) ||
		errors.Is(err, aether.ErrStreamOverrun)
}

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
// implement CanSend are judged by IsOpen alone.
func canDispatchOn(stream aether.Stream) bool {
	if cs, ok := stream.(poolCanSendChecker); ok {
		return cs.CanSend()
	}
	return stream.IsOpen()
}

// sessionCloser is the close signal aether.BaseSession exposes; the adapter
// sessions embed BaseSession, so a live mesh session satisfies it.
type sessionCloser interface {
	CloseSignal() <-chan struct{}
}

// sessionScope returns a context that ends when parent ends or when sess
// closes. A session without a close signal is bounded by parent alone. The
// returned cancel ends the watcher goroutine and must always be called.
func sessionScope(parent context.Context, sess aether.Session) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	if sc, ok := sess.(sessionCloser); ok {
		if closed := sc.CloseSignal(); closed != nil {
			go func() {
				select {
				case <-closed:
					cancel()
				case <-ctx.Done():
				}
			}()
		}
	}
	return ctx, cancel
}

// Fill pre-opens streams up to capacity on the given session.
//
// Every OpenStream runs under ctx narrowed to the session's lifetime
// (sessionScope): HWPCaller passes its own lifecycle context, so a fill
// blocked on a stalled peer ends when the session closes or when the caller
// closes. Fill holds the pool mutex across OpenStream, so an unbounded open
// would also stall every Get on this pool.
//
// Backoff: while the session is inside streamBackoff (isStreamBackpressure),
// Fill opens nothing. A session whose caller is leaking streams, or whose peer is
// refusing them, otherwise produces a steady cycle of one refused open every
// refill loop and floods the session's streamRefused counter.
func (p *StreamPool) Fill(ctx context.Context, sess aether.Session) {
	if streamIDsFor(sess).backoffErr() != nil {
		return
	}
	ctx, cancel := sessionScope(ctx, sess)
	defer cancel()
	p.mu.Lock()
	defer p.mu.Unlock()
	for len(p.streams) < p.capacity {
		if sess.IsClosed() || ctx.Err() != nil {
			return
		}
		stream, err := openDynamicStreamOn(ctx, sess, aether.StreamConfig{
			Reliability: aether.ReliableOrdered,
			Priority:    128,
		})
		if err != nil {
			if isStreamBackpressure(err) {
				p.noteCapExhausted(err)
				log.Printf("[dispatch.pool] OpenStream refused: %v; backing off %v", err, streamBackoff)
			}
			return
		}
		p.streams = append(p.streams, pooledStream{stream: stream, mintedAt: time.Now()})
	}
}

// noteCapExhausted counts an open the session's stream cap refused.
func (p *StreamPool) noteCapExhausted(err error) {
	if errors.Is(err, aether.ErrStreamCapExhausted) {
		p.capExhaustedHits.Add(1)
	}
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

	// Pool empty: open a fresh stream. Inside the session's backoff window Get
	// answers the typed error that armed it, so a caller retrying does not redial
	// into a refusal and flood streamRefused while no progress is possible, and
	// capExhaustedHits counts refusals rather than retries.
	if err := streamIDsFor(sess).backoffErr(); err != nil {
		return nil, err
	}
	stream, err := openDynamicStreamOn(ctx, sess, aether.StreamConfig{
		Reliability: aether.ReliableOrdered,
		Priority:    128,
	})
	p.noteCapExhausted(err)
	return stream, err
}

// closeStreamQuiet discards a pool entry that failed admission, or one the
// pool drains, by resetting it through releaseDynamicStream. A reset removes
// the stream from both sessions' stream tables, so its dynamic ID is free for
// the next allocation; a FIN would leave it half-closed and holding the ID
// until aether's idle GC. Errors are ignored: the stream is already known-bad
// or no longer wanted.
func closeStreamQuiet(s aether.Stream) {
	releaseDynamicStream(nil, s)
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
		closeStreamQuiet(entry.stream)
	}
	p.streams = p.streams[:0]
}
