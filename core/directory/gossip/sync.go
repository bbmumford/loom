/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package gossip

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	lad "github.com/bbmumford/ledger"
	"github.com/bbmumford/ledger/cache"
	"github.com/bbmumford/whisper"
)

// GossipResult, ExchangeMeta, and other gossip types are defined in
// github.com/bbmumford/whisper and re-exported via whisper_types.go.

// maxGossipPayload caps the size of a single gossip message (1 MB).
// With 11 nodes, typical exchanges are 15-50KB. 1MB provides generous headroom
// while catching corrupted length headers (previously 16MB let garbage through).
const maxGossipPayload = 1 << 20

// deltaSyncClockSkewSlack widens the delta-sync watermark to tolerate a lagging
// author clock (MESH-F08) — see the RecordWatermark call site.
const deltaSyncClockSkewSlack = 30 * time.Second

// gossipMagic is the 2-byte prefix on all gossip frames for corruption detection.
// Wire format: [2-byte magic 0x4731][4-byte big-endian length][JSON payload]
// If the magic doesn't match, the frame is corrupted (e.g., from WS proxy
// frame reassembly errors on Fly.io). The reader logs and closes the connection.
const gossipMagic uint16 = 0x4731 // "G1"
const gossipHeaderSize = 6        // 2 (magic) + 4 (length)

// gossipBufPoolMaxCap is the maximum buffer capacity retained in the pool.
// Buffers larger than this are discarded to prevent pool inflation from
// occasional large payloads (e.g., full sync). Typical gossip payloads are
// ~100KB; 128KB cap retains buffers for reuse without holding oversized ones.
const gossipBufPoolMaxCap = 128 * 1024

// creditFullSyncFloor is the minimum stream credit available before a full
// sync is attempted. A full-state gossip payload is observed at 80-150 KB
// (and can spike higher on large fleets), so 256 KB gives us a 2x safety
// margin plus room for a subsequent delta before the next WINDOW_UPDATE.
// Below this threshold, deltaExchange forces a delta-only cycle even when
// the delta tracker would otherwise pick a full sync — prevents the
// guaranteed ConsumeTimeout stall that drains goroutines and starves the
// HTTP accept path.
const creditFullSyncFloor int64 = 256 * 1024

// creditDeltaFloor is the minimum stream credit available before ANY gossip
// exchange (delta or full) is attempted. Observed delta payloads on the live
// fleet run 1-40 KB; we picked 64 KB so a single peer-WINDOW_UPDATE round
// trip is enough to recover, while still aborting the exchange if the credit
// genuinely cannot fit the payload. Without this, a delta exchange entered
// with ~1-22 KB of credit (real fleet symptom 2026-05-29) burns
// ConsumeTimeout = 10 s waiting for grants that won't arrive in time, then
// fails the gossip stream. Returning early lets the next tick attempt the
// exchange when grants have actually flowed.
const creditDeltaFloor int64 = 64 * 1024

// errInsufficientCredit is returned by deltaExchange when stream credit is
// below creditDeltaFloor. The outer loop matches on this sentinel to schedule
// a short retry (grants flow in ms; the default 60s idle backoff would be
// useless) and to keep the skip out of the fatal-error classifier.
var errInsufficientCredit = fmt.Errorf("gossip: insufficient credit — exchange skipped")

// gossipBufPool reuses byte buffers for JSON marshal/unmarshal to reduce
// per-exchange heap allocations that contribute to OOM under high gossip load.
var gossipBufPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, 0, 32*1024) // 32 KB initial capacity
		return &b
	},
}

// returnBufToPool returns a buffer to the pool, discarding oversized buffers
// to prevent pool inflation from occasional large payloads.
func returnBufToPool(bufPtr *[]byte, buf []byte) {
	if cap(buf) > gossipBufPoolMaxCap {
		b := make([]byte, 0, 32*1024)
		bufPtr = &b
	} else {
		*bufPtr = buf[:0]
	}
	gossipBufPool.Put(bufPtr)
}

// GossipOverConn performs one bidirectional cache exchange over conn.
// Both sides simultaneously send their cache dump and read the peer's dump.
// Returns the round-trip time measured from write-start to read-complete.
//
// If meta is non-nil, the payload is sent as a v2 envelope (JSON object with
// "meta" and "records" keys). If meta is nil, a v2 envelope with no meta is
// still sent (new nodes always send v2). The receiver auto-detects v1 vs v2
// by checking whether the payload starts with '[' (v1 array) or '{' (v2 object).
//
// The optional since parameter enables delta sync: when set, only records
// with Timestamp after the given time are sent. If omitted or zero, the
// full cache is dumped. Delta sync cuts steady-state gossip bandwidth by
// roughly 90%.
func GossipOverConn(ctx context.Context, conn net.Conn, c *cache.DirectoryCache, meta *ExchangeMeta, since ...time.Time) (GossipResult, error) {
	var result GossipResult

	// Serialize cache records: delta (since watermark) or full dump
	var records []lad.Record
	if len(since) > 0 && !since[0].IsZero() {
		records = c.DumpSince(since[0])
	} else {
		records = c.Dump()
	}
	result.RecordsSent = len(records)

	// Marshal as protobuf — ~50% smaller on the wire than JSON.
	payload, err := marshalEnvelope(meta, records)
	if err != nil {
		return result, fmt.Errorf("gossip: marshal cache dump: %w", err)
	}

	// Set deadline on socket (works for direct sockets, no-op for shared socket sessions)
	deadline := time.Now().Add(30 * time.Second)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	_ = conn.SetDeadline(deadline)

	exchangeStart := time.Now()

	// Prepare our header: [2-byte magic][4-byte length]
	hdr := make([]byte, gossipHeaderSize)
	binary.BigEndian.PutUint16(hdr[0:2], gossipMagic)
	binary.BigEndian.PutUint32(hdr[2:6], uint32(len(payload)))

	// Sequential write-then-read: no goroutines, no leaked goroutines.
	// Aether stream.Send() enqueues to the scheduler and returns immediately
	// (non-blocking). The writeLoop flushes within 1ms. The responder reads
	// first then writes — so there's no deadlock.
	//
	// This replaces the previous concurrent goroutine model which leaked
	// read goroutines on timeout, permanently starving the stream's recvCh.
	if _, err := conn.Write(hdr); err != nil {
		return result, fmt.Errorf("gossip: write header: %w", err)
	}
	if _, err := conn.Write(payload); err != nil {
		return result, fmt.Errorf("gossip: write payload: %w", err)
	}

	// Read peer's response header — SetDeadline (above) ensures this times out
	// via the per-exchange context on Aether streams.
	var peerHdr [gossipHeaderSize]byte
	for {
		if _, err := io.ReadFull(conn, peerHdr[:]); err != nil {
			return result, fmt.Errorf("gossip: read header: %w", err)
		}

		// Validate magic bytes
		magic := binary.BigEndian.Uint16(peerHdr[0:2])
		if magic != gossipMagic {
			return result, fmt.Errorf("gossip: invalid magic 0x%04X (expected 0x%04X, frame corrupted)", magic, gossipMagic)
		}

		peerLen := binary.BigEndian.Uint32(peerHdr[2:6])
		if peerLen == 0 {
			// Zero-length = keepalive ping — skip and read next header
			continue
		}
		if peerLen > maxGossipPayload {
			return result, fmt.Errorf("gossip: peer payload too large (%d bytes)", peerLen)
		}

		// Pool inbound buffer to reduce per-exchange heap allocations
		inbufPtr := gossipBufPool.Get().(*[]byte)
		inbuf := *inbufPtr
		if cap(inbuf) < int(peerLen) {
			inbuf = make([]byte, peerLen)
		} else {
			inbuf = inbuf[:peerLen]
		}
		if _, err := io.ReadFull(conn, inbuf); err != nil {
			returnBufToPool(inbufPtr, inbuf)
			return result, fmt.Errorf("gossip: read payload: %w", err)
		}

		result.NetworkRTT = time.Since(exchangeStart)
		result.RTT = result.NetworkRTT

		// Unmarshal protobuf envelope
		peerMeta, peerRecords, err := unmarshalEnvelope(inbuf)
		if err != nil {
			returnBufToPool(inbufPtr, inbuf)
			return result, fmt.Errorf("gossip: unmarshal peer envelope: %w", err)
		}
		returnBufToPool(inbufPtr, inbuf)
		result.PeerMeta = peerMeta

		seenSet := make(map[string]bool)
		for _, rec := range peerRecords {
			if rec.NodeID != "" {
				seenSet[rec.NodeID] = true
			}
			if err := c.Apply(rec); err != nil {
				log.Printf("[GOSSIP] Failed to apply record (topic=%s, node=%s): %v", rec.Topic, rec.NodeID, err)
				continue
			}
			result.RecordsApplied++
		}

		// Collect unique NodeIDs for gossip liveness tracking
		for nodeID := range seenSet {
			result.SeenNodeIDs = append(result.SeenNodeIDs, nodeID)
		}

		return result, nil
	}
}

// escalateBucketDigest runs one G2B bucketed-digest round on conn AFTER a G1
// exchange whose piggybacked cache fingerprints still disagree. It sends this
// node's per-bucket XOR digest; the responder compares it against its own,
// records which of the (default 64) buckets diverge, and replies with its
// digest. That recorded divergence makes the responder's NEXT G1 reply serve
// only the diverging buckets' records — its G1 handler drains the set via
// takeDivergingBuckets — so the caller's scoped follow-up GossipOverConn moves
// O(diverging buckets) records instead of a full re-dump, which is the whole
// point: it replaces the expensive full-sync a stale-watermark divergence would
// otherwise force.
//
// The round is a single write-then-read — the same deadlock-safe shape as
// GossipOverConn: Aether's stream.Send enqueues non-blocking and the writeLoop
// flushes within ~1ms, so writing before reading cannot self-deadlock. It reuses
// the deadline GossipOverConn set moments earlier and does NOT call SetDeadline
// mid-round, which on an Aether StreamConn would flush the read buffer.
//
// Sending the 0x4742 frame unconditionally is safe because the mesh redeploys
// end-to-end: every peer's responder auto-wires the bucket-digest handler from
// its G1 store (whisper's ensureDefaults), so there is no mixed-version window in
// which a peer treats 0x4742 as an unknown magic and tears the stream down. The
// peer's digest reply is drained even though this minimal escalation does not
// diff it locally — leaving an unread frame would desync the scoped follow-up.
func escalateBucketDigest(conn net.Conn, c *cache.DirectoryCache) error {
	// Must match the responder's bucket count. The responder does not override
	// WithDigestBuckets, so both sides use whisper's default of 64.
	const digestBuckets uint16 = 64

	if err := whisper.WriteBucketDigest(conn, c.BucketDigest(digestBuckets), 0); err != nil {
		return fmt.Errorf("write bucket digest: %w", err)
	}

	var magic [2]byte
	if _, err := io.ReadFull(conn, magic[:]); err != nil {
		return fmt.Errorf("read digest reply magic: %w", err)
	}
	if got := binary.BigEndian.Uint16(magic[:]); got != whisper.DigestBucketMagic {
		return fmt.Errorf("expected digest magic 0x%04X, got 0x%04X", whisper.DigestBucketMagic, got)
	}
	if _, _, err := whisper.ReadBucketDigestBody(conn); err != nil {
		return fmt.Errorf("read digest reply body: %w", err)
	}
	return nil
}

// Pinger is optionally implemented by connections that support keepalive pings.
type Pinger interface {
	SendPing() (uint32, error)
}

// RTTProvider is optionally implemented by connections that track ping/pong RTT.
// Preferred over gossip exchange RTT for accurate network latency measurement.
type RTTProvider interface {
	AvgRTT() time.Duration
}

// RunGossipLoop runs periodic gossip exchanges over conn until ctx is cancelled
// or the connection closes. onExchange is called after each successful exchange
// (e.g., to publish latency records). The interval defaults to 30 seconds.
// metaProvider, if non-nil, is called before each exchange to build ExchangeMeta
// to include in the outbound gossip envelope (carries connection-count
// signaling so the rebalancer can detect hotspots).
// runGossipLoopInternal is the initiator-side gossip loop. conn carries
// G1/G2/G3 traffic; reconcileConn is the dedicated wire used for G4 IBLT
// reconciliation and G5 snapshot frames. Both must be non-nil — the
// previous single-stream multiplexed mode caused abandoned-G4-read bytes
// to corrupt G1's read path, and is no longer supported.
func runGossipLoopInternal(ctx context.Context, conn net.Conn, reconcileConn net.Conn, c *cache.DirectoryCache, interval time.Duration, onExchange func(GossipResult), getMeta func() *ExchangeMeta, dt *DeltaTracker, peerNodeID string, bpMonitor *BackpressureMonitor, peerBackoff *PeerBackoff) {
	dbgGossip.Printf("Gossip loop started: peerNodeID=%s, interval=%v, connType=%T", peerNodeID, interval, conn)
	if interval <= 0 {
		interval = 30 * time.Second
	}

	// deltaExchange performs one gossip exchange using delta sync when available.
	// Sends a G2 digest probe first — if caches match, skips the full G1 exchange.
	deltaExchange := func(label string) (GossipResult, error) {
		var meta *ExchangeMeta
		if getMeta != nil {
			meta = getMeta()
		}

		// Track pending exchanges so the backpressure monitor can flag
		// congestion (too many in-flight) back to the peer.
		if bpMonitor != nil {
			overloaded := bpMonitor.Enter()
			defer bpMonitor.Exit()
			if meta == nil {
				meta = &ExchangeMeta{}
			}
			meta.BackpressureSignal = overloaded
			if overloaded {
				dbgGossip.Printf("Backpressure: %d pending exchanges, signaling busy", bpMonitor.Pending())
			}
		}

		// Digest optimization: piggyback our cache fingerprint in ExchangeMeta.
		// The responder compares fingerprints after the exchange. If they match
		// AFTER applying records, the next exchange can be skipped (delta watermark
		// is current). This avoids a separate G2 round-trip which deadlocks on
		// Aether streams (scheduler doesn't flush before the read blocks).
		if meta == nil {
			meta = &ExchangeMeta{}
		}
		meta.CacheFingerprint = c.Fingerprint()
		var since time.Time
		if dt != nil && peerNodeID != "" && !dt.ShouldFullSync(peerNodeID) {
			since = dt.RecordWatermark(peerNodeID)
			// MESH-F08: the watermark is a LOCAL clock value, but DumpSince
			// selects records by the author's wall-clock Timestamp. If a peer's
			// clock lags ours, its records fall just below `since` and are
			// excluded from every delta until a full G4 reconcile heals it. Widen
			// the window by a clock-skew slack so a lagging author's records are
			// still selected; the over-included records are de-duped on apply.
			// (A monotonic HLC/apply-seq watermark would remove the slack, but
			// that is a larger delta-protocol change.)
			if !since.IsZero() {
				since = since.Add(-deltaSyncClockSkewSlack)
			}
		}

		// G4 reconciliation as primary repair path when a full-sync
		// would otherwise be required (no watermark, or watermark
		// stale per ShouldFullSync). G4 carries O(|Δ|) bytes via
		// IBLT cells, replacing the multi-MB G1 full-snapshot that
		// reliably fries the gossip stream's credit window after
		// fresh redeploys.
		//
		// Falls back to G1 when:
		//   - No reconcile driver registered (legacy / test paths)
		//   - peerNodeID is empty (no per-peer state to track)
		//   - G4 round itself errors at the wire layer
		//
		// G4 frames travel on the same gossip stream as G1; the
		// peer's engine routes them by magic so capability gating
		// happens implicitly (peers without the handler installed
		// see G4 frames as unknown magic and fall back).
		if since.IsZero() && peerNodeID != "" {
			if rd := ReconcileDriver(); rd != nil {
				// G4 always rides the dedicated reconcile
				// stream — that's the whole point of the
				// stream split. The magic-byte dispatcher in
				// the engine routes G4 frames on the responder
				// side; the initiator writes/reads them here
				// on reconcileConn directly via the driver.
				rd.RegisterPeer(&whisper.ReconcilePeer{NodeID: peerNodeID, Conn: reconcileConn})
				roundStart := time.Now()
				applied, sent, rerr := rd.RunInitiatorRound(peerNodeID, 15*time.Second)
				if rerr == nil {
					if dt != nil {
						dt.UpdateWatermark(peerNodeID, time.Now())
					}
					rttMeasured := time.Since(roundStart)
					dbgGossip.Printf("%s (G4 reconcile): sent=%d applied=%d rtt=%v",
						label, sent, applied, rttMeasured)
					return GossipResult{
						RTT:            rttMeasured,
						RecordsSent:    sent,
						RecordsApplied: applied,
					}, nil
				}
				// G4 failed at the wire layer. Force a recent-
				// sentinel watermark so the G1 fallback below
				// ships a small delta instead of a 77 KB full-
				// sync that would re-fry the gossip stream's
				// credit window on every retry.
				//
				// G4-FAIL-TRACE: capture conn state at the
				// failure point so we can distinguish "stream
				// stuck mid-read" (cascade signal) from
				// "session was closed before the call started"
				// (race with teardown).
				connClosed := "?"
				if cc, ok := reconcileConn.(interface{ IsClosed() bool }); ok {
					connClosed = fmt.Sprintf("%v", cc.IsClosed())
				}
				roundElapsed := time.Since(roundStart)
				dbgGossip.Printf("G4 to %s failed (%v) — falling back to G1 with recent sentinel [G4-FAIL-TRACE elapsed=%v rcConnClosed=%s err_type=%T]",
					peerNodeID, rerr, roundElapsed, connClosed, rerr)
				since = time.Now().Add(-30 * time.Second)
			}
		}

		// Credit-aware self-throttle. Two tiers:
		//   1. If the stream has < creditFullSyncFloor (256 KB), force a
		//      delta-only exchange even if the tracker wants a full sync.
		//   2. If the stream has < creditDeltaFloor (64 KB), skip the
		//      exchange entirely — even a delta won't complete inside
		//      ConsumeTimeout, so we'd just burn a goroutine for 10 s.
		// Without (2), live fleet observed delta exchanges entered with
		// 1-22 KB of credit and produced four cascaded `insufficient stream
		// credit after 10s` failures per peer per minute on app.orbtr.io
		// (2026-05-29). Skipping the tick lets WINDOW_UPDATE flow naturally.
		if ca, ok := conn.(interface{ AvailableCredit() int64 }); ok {
			avail := ca.AvailableCredit()
			// Treat ONLY -1 as the "no flow control / unbounded" sentinel.
			// Any other non-positive value (zero, overshipped negatives from
			// grant races) genuinely cannot accept a payload, so skip.
			if avail != -1 && avail < creditDeltaFloor {
				dbgGossip.Printf("credit critically low (avail=%d < delta-floor=%d) to %s — skipping exchange",
					avail, creditDeltaFloor, peerNodeID)
				return GossipResult{}, errInsufficientCredit
			}
			// Force a delta when credit is below the full-sync floor.
			// Also fall back to the recent-sentinel when RecordWatermark
			// returns the zero time — a first-contact peer with no
			// watermark would otherwise still produce a full sync against
			// inadequate credit.
			if since.IsZero() && avail != -1 && avail < creditFullSyncFloor {
				if dt != nil && peerNodeID != "" {
					since = dt.RecordWatermark(peerNodeID)
				}
				if since.IsZero() {
					since = time.Now().Add(-30 * time.Second)
				}
				dbgGossip.Printf("credit low (avail=%d < full-floor=%d) to %s — forcing delta sync",
					avail, creditFullSyncFloor, peerNodeID)
			}
		}

		exchangeTime := time.Now()
		res, err := GossipOverConn(ctx, conn, c, meta, since)
		if err != nil {
			return res, err
		}

		// G2B bucketed-digest escalation. When the piggybacked cache
		// fingerprints still disagree after the G1 exchange, run one compact
		// digest round so the responder records the diverging buckets, then
		// re-exchange: the responder now serves only those buckets' records
		// instead of a full re-dump. Best-effort — any escalation error tears
		// the stream down so the next cycle starts clean; the G1 records applied
		// above are already durable. Gated on a real fingerprint mismatch, so a
		// converged pair never pays for the extra round-trip.
		if res.PeerMeta != nil && res.PeerMeta.CacheFingerprint != 0 &&
			res.PeerMeta.CacheFingerprint != c.Fingerprint() {
			if escErr := escalateBucketDigest(conn, c); escErr != nil {
				// MESH-F04: actually tear the stream down, as the comment above
				// promises. escalateBucketDigest may have written its request but
				// timed out on the reply, leaving that reply buffered on conn; the
				// next cycle's G1 io.ReadFull would then decode those leftover
				// bytes as a bad magic ("invalid magic") — which is NOT in the
				// fatal classifier — and retry forever on the same conn. Closing
				// conn forces the session to be genuinely rebuilt with clean state.
				_ = conn.Close()
				return res, fmt.Errorf("gossip: G2B escalation to %s: %w", peerNodeID, escErr)
			}
			scoped, scErr := GossipOverConn(ctx, conn, c, meta, since)
			if scErr != nil {
				return res, fmt.Errorf("gossip: G2B scoped follow-up to %s: %w", peerNodeID, scErr)
			}
			res.RecordsApplied += scoped.RecordsApplied
			res.RecordsSent += scoped.RecordsSent
			if scoped.PeerMeta != nil {
				res.PeerMeta = scoped.PeerMeta
			}
			dbgGossip.Printf("%s (G2B): scoped follow-up applied=%d sent=%d", label, scoped.RecordsApplied, scoped.RecordsSent)
		}

		// Process peer's backpressure signal to drive sender-side backoff
		if peerBackoff != nil {
			signaled := res.PeerMeta != nil && res.PeerMeta.BackpressureSignal
			peerBackoff.RecordSignal(signaled)
			if signaled {
				dbgGossip.Printf("Peer signaled backpressure, backing off %v", peerBackoff.CurrentBackoff())
			}
		}

		if dt != nil && peerNodeID != "" {
			dt.UpdateWatermark(peerNodeID, exchangeTime)
		}

		if rttProvider, ok := conn.(RTTProvider); ok {
			if avgRTT := rttProvider.AvgRTT(); avgRTT > 0 {
				res.NetworkRTT = avgRTT
			}
		}

		isDelta := !since.IsZero()
		if isDelta {
			dbgGossip.Printf("%s (delta): sent=%d applied=%d rtt=%v networkRTT=%v", label, res.RecordsSent, res.RecordsApplied, res.RTT, res.NetworkRTT)
		} else {
			dbgGossip.Printf("%s (full): sent=%d applied=%d rtt=%v networkRTT=%v", label, res.RecordsSent, res.RecordsApplied, res.RTT, res.NetworkRTT)
		}
		return res, nil
	}

	// Do an immediate exchange, then periodic
	if res, err := deltaExchange("Initial exchange"); err != nil {
		log.Printf("[GOSSIP] Initial exchange failed: %v", err)
	} else if onExchange != nil {
		onExchange(res)
	}

	// Adaptive gossip interval: backs off when idle (60s), accelerates during
	// convergence (2s), returns to base when stable. Reduces gossip bandwidth
	// by ~80% in steady state (60s vs 10s = 6× fewer exchanges).
	adaptive := NewAdaptiveInterval(interval, 2*time.Second, 60*time.Second)
	timer := time.NewTimer(interval)
	defer timer.Stop()

	// NOTE: Gossip-level keepalive pings removed. They wrote zero-length frames to
	// stream 0 (gossip) which interleaved with exchange reads on muxed connections,
	// causing the reader to consume the ping as an exchange header and orphaning the
	// real exchange data in the buffer (eventually filling the 256-slot buffer).
	// Connection liveness is now handled by mux-level keepalive on stream 2 (15s
	// interval, 3 missed → close). RTT is measured from exchange header timing.

	for {
		select {
		case <-ctx.Done():
			dbgGossip.Printf("Gossip loop exiting: ctx.Done (peerNodeID=%s, err=%v)", peerNodeID, ctx.Err())
			return
		case <-timer.C:
			// Skip this exchange entirely if the peer is currently
			// signalling backpressure and we're inside its backoff window.
			if peerBackoff != nil && peerBackoff.ShouldSkip() {
				dbgGossip.Printf("Skipping exchange: peer in backpressure backoff (%v remaining)", peerBackoff.CurrentBackoff())
				timer.Reset(adaptive.Current())
				continue
			}
			res, err := deltaExchange("Exchange")
			if err != nil {
				// Credit-skip is benign — log quietly and retry quickly.
				// Grants flow in ms; the default idle backoff (60 s) would
				// turn a 100 ms transient into a minute-long stall.
				if errors.Is(err, errInsufficientCredit) {
					dbgGossip.Printf("Exchange skipped (credit floor) — fast retry")
					next := adaptive.Current()
					if next > 2*time.Second {
						next = 2 * time.Second
					}
					timer.Reset(next)
					continue
				}
				log.Printf("[GOSSIP] Exchange failed (%T): %v", conn, err)
				errStr := err.Error()
				if strings.Contains(errStr, "EOF") || strings.Contains(errStr, "connection reset") ||
					strings.Contains(errStr, "broken pipe") || strings.Contains(errStr, "use of closed") ||
					strings.Contains(errStr, "session closed") {
					log.Printf("[GOSSIP] Fatal connection error, exiting loop: %v", err)
					return
				}
				// Gossip timeouts are transient — BidiRPC on Stream 1 and keepalive
				// on Stream 2 work independently. Don't exit the loop on timeouts;
				// exiting tears down the entire Aether session (cleanup: label),
				// killing BidiRPC mid-flight and causing payload corruption on
				// connections routing through this session.
				timer.Reset(adaptive.Current())
				continue
			}
			if onExchange != nil {
				onExchange(res)
			}
			nextInterval := adaptive.Next(res)
			dbgGossip.Printf("Adaptive interval: next=%v (applied=%d, sent=%d)", nextInterval, res.RecordsApplied, res.RecordsSent)
			timer.Reset(nextInterval)
		}
	}
}

// runGossipResponderLoop waits for the initiator to send gossip records, applies
// them, then responds with our own records. This is the acceptor-side counterpart
// to runGossipLoopInternal which is the initiator-side.
//
// On Aether streams, gossip must use initiator/responder roles because Send/Receive
// are message-oriented. If both sides independently initiate (write-then-read),
// they deadlock — each writes and waits for a response the other never sends
// because it's also waiting.
//
// Two whisper engines run in parallel:
//   - Gossip engine on conn — handles G1 (record exchange), G2 (digest probe),
//     G3 (rumor push) + G3-ACK.
//   - Reconcile engine on reconcileConn — handles G4 (IBLT) + G5 (snapshot).
//
// Splitting these onto separate streams isolates frame queues so an
// abandoned read on one protocol can never poison another's magic-byte
// dispatch. Both conns are required.
func runGossipResponderLoop(ctx context.Context, conn net.Conn, reconcileConn net.Conn, c *cache.DirectoryCache,
	onExchange func(GossipResult), getMeta func() *ExchangeMeta, dt *DeltaTracker, peerNodeID string) {

	dbgGossip.Printf("Gossip responder started: peerNodeID=%s, connType=%T", peerNodeID, conn)

	// Gossip engine: G1 (record exchange) + G2 (digest probe).
	// Reconcile/snapshot drivers are NOT attached here — they belong on
	// the reconcile engine below so G4/G5 frames are only ever read
	// from the reconcile stream. The rumor pump (G3) was retired in
	// Phase 2; receivers no longer subscribe to a RumorPusher and the
	// reach-freshness payload path was deleted alongside it.
	gossipEngine := whisper.NewEngine(
		whisper.WithDelta(dt),
		whisper.WithFirstFrameGrace(500*time.Millisecond),
		whisper.WithFatalErrorClassifier(isFatalGossipError),
		whisper.WithRumorApplyFn(ladRumorApplyFn(c, nil)),
		whisper.WithRumorDedupKeyFunc(ladRumorDedupKey),
		whisper.WithG1Store(newLADStateStore(c)),
		whisper.WithG1Codec(newLADG1Codec()),
		whisper.WithG1ExchangeObserver(func(res whisper.GossipResult, _ string) {
			if onExchange != nil {
				onExchange(res)
			}
		}),
		whisper.WithMetaProvider(getMeta),
		whisper.WithSeenIDTracking(true),
		whisper.WithBufferPool(&gossipBufPool),
		whisper.WithMaxPayloadSize(maxGossipPayload),
		whisper.WithExchangeDeadline(30*time.Second),
	)

	// Subscribe to EventDigestMatch so the peer's watermark gets
	// updated on every G2 hit — same behaviour as the hand-rolled
	// loop's `dt.UpdateWatermark(peerNodeID, time.Now())` branch.
	events := make(chan whisper.GossipEvent, 16)
	gossipEngine.SubscribeEvents(events)
	watermarkCtx, watermarkCancel := context.WithCancel(ctx)
	defer watermarkCancel()
	go func() {
		for {
			select {
			case <-watermarkCtx.Done():
				return
			case ev, ok := <-events:
				if !ok {
					return
				}
				if ev.Kind == whisper.EventDigestMatch && dt != nil && ev.PeerNodeID != "" {
					dt.UpdateWatermark(ev.PeerNodeID, time.Now())
				}
			}
		}
	}()

	// Reconcile engine: G4 (IBLT) + G5 (snapshot) only. No G1
	// store/codec, no rumor handlers — the initiator only ever
	// writes G4/G5 magics here, and any other magic on this stream
	// is a protocol error the engine will surface as "unknown frame
	// magic" rather than silently dispatch onto the wrong handler.
	rcOpts := []whisper.EngineOption{
		whisper.WithFirstFrameGrace(500 * time.Millisecond),
		whisper.WithFatalErrorClassifier(isFatalGossipError),
		whisper.WithMaxPayloadSize(maxGossipPayload),
	}
	if rd := ReconcileDriver(); rd != nil {
		rcOpts = append(rcOpts, whisper.WithReconcile(rd))
	}
	if sd := SnapshotDriver(); sd != nil {
		rcOpts = append(rcOpts, whisper.WithSnapshot(sd))
	}
	reconcileEngine := whisper.NewEngine(rcOpts...)
	var reconcileWG sync.WaitGroup
	reconcileWG.Add(1)
	go func() {
		defer reconcileWG.Done()
		dbgGossip.Printf("Reconcile responder started: peerNodeID=%s", peerNodeID)
		if err := reconcileEngine.RunResponder(ctx, reconcileConn, peerNodeID); err != nil {
			if ctx.Err() == nil && !isFatalGossipError(err) {
				log.Printf("[GOSSIP] Reconcile responder: %v", err)
			}
		}
		dbgGossip.Printf("Reconcile responder exited (peerNodeID=%s)", peerNodeID)
	}()

	if err := gossipEngine.RunResponder(ctx, conn, peerNodeID); err != nil {
		if ctx.Err() == nil && !isFatalGossipError(err) {
			log.Printf("[GOSSIP] Responder: %v", err)
		}
	}
	// Wait for reconcile responder to exit before returning so the
	// caller's lifecycle reasoning (close streams, recycle session)
	// stays linear. Cancellation of ctx will cause the reconcile
	// loop's RunResponder to return, so this won't deadlock.
	reconcileWG.Wait()
	dbgGossip.Printf("Gossip responder exiting (peerNodeID=%s)", peerNodeID)
}

// Synchronizer continuously applies ledger records to a directory cache.
type Synchronizer struct {
	ledger lad.Ledger
	cache  *cache.DirectoryCache
}

// NewSynchronizer wires a ledger and cache together.
func NewSynchronizer(ledger lad.Ledger, cache *cache.DirectoryCache) *Synchronizer {
	return &Synchronizer{ledger: ledger, cache: cache}
}

// Sync consumes records from the ledger stream until the context ends.
func (s *Synchronizer) Sync(ctx context.Context, from lad.CausalWatermark, topics []lad.Topic) error {
	stream, err := s.ledger.Stream(ctx, from, topics)
	if err != nil {
		return err
	}
	for {
		select {
		case rec, ok := <-stream:
			if !ok {
				return nil
			}
			_ = s.cache.Apply(rec)
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
