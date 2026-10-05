/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package gossip

import (
	"context"
	"strings"
	"time"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/adapter"
	"github.com/bbmumford/ledger/cache"
)

// RunGossipLoopWithPeer runs gossip exchanges natively over an Aether stream,
// with delta sync enabled for the given peer; it needs no net.Conn or
// PingableConn.
//
// The Aether stream handles reliability, ordering, and keepalive independently.
// No SetDeadline, no PingableConn, no gossip-level pings. Transient errors
// (timeout) continue the loop. Fatal errors (EOF, reset) exit.
//
// dt holds the per-peer watermarks delta sync reads and advances. The caller
// owns it: node's Runtime passes the one tracker of its runtime generation
// (Runtime.startDeltaTracker), so watermarks survive connection restarts
// within that generation, persist across process restarts where the runtime
// has durable storage, and are never shared with another runtime in the same
// process. A nil dt, or an empty peerNodeID, runs every exchange as a full
// sync.
//
// onExchange is called after each successful exchange with RTT and sync results.
// metaProvider, if non-nil, returns ExchangeMeta for connection signaling.
//
// stream carries G1/G2/G3 (record exchange, digest probe, rumor push).
// reconcileStream carries G4 IBLT reconciliation + G5 snapshot frames on a
// dedicated aether stream — separating these traffic classes eliminates the
// abandoned-read race where a timed-out G4 round's residual bytes poison G1's
// read path on a shared stream (surfacing as "invalid magic 0x4734").
// Both streams are required.
func RunGossipLoopWithPeer(ctx context.Context, stream aether.Stream, reconcileStream aether.Stream, c *cache.DirectoryCache,
	interval time.Duration, peerNodeID string, dt *DeltaTracker, onExchange func(GossipResult), metaProvider ...func() *ExchangeMeta) {

	if interval <= 0 {
		interval = 10 * time.Second
	}

	var getMeta func() *ExchangeMeta
	if len(metaProvider) > 0 && metaProvider[0] != nil {
		getMeta = metaProvider[0]
	}

	dbgGossipStream.Printf("Loop started: streamID=%d, reconcileID=%d, interval=%v, peer=%s, delta=%v",
		stream.StreamID(), reconcileStream.StreamID(), interval, peerNodeID, peerNodeID != "")

	// Use the unified StreamConn adapter — single net.Conn implementation
	// with per-exchange deadline support and parent context propagation.
	streamConn := adapter.NewStreamConn(stream, ctx)
	reconcileConn := adapter.NewStreamConn(reconcileStream, ctx)

	// Delta sync needs the peer's identity to key its watermark (it cuts an
	// exchange from ~100KB to ~1KB).
	if peerNodeID == "" {
		dt = nil
	}
	runGossipLoopInternal(ctx, streamConn, reconcileConn, c, interval, onExchange, getMeta, dt, peerNodeID, nil, nil)
	dbgGossipStream.Printf("Loop exited: streamID=%d, ctxErr=%v", stream.StreamID(), ctx.Err())
}

// RunGossipResponder listens for gossip exchanges on an Aether stream and
// responds with its own records. This is the counterpart to RunGossipLoopWithPeer —
// the initiator drives the exchange timing, the responder reacts.
//
// On Aether streams, both sides cannot independently initiate exchanges because
// Send/Receive are message-oriented (not byte-stream). The initiator writes
// header+payload then reads the response; the responder reads header+payload
// then writes its response. This avoids the deadlock where both sides write
// and wait for a response that never comes.
//
// reconcileStream is the dedicated aether stream for G4/G5 frames; a parallel
// responder loop dispatches G4 (IBLT) and G5 (snapshot) frames on that stream
// while G1/G2/G3 stay on `stream`. Both streams are required. dt is the
// caller's delta tracker, as for RunGossipLoopWithPeer.
func RunGossipResponder(ctx context.Context, stream aether.Stream, reconcileStream aether.Stream, c *cache.DirectoryCache,
	peerNodeID string, dt *DeltaTracker, onExchange func(GossipResult),
	metaProvider ...func() *ExchangeMeta) {

	var getMeta func() *ExchangeMeta
	if len(metaProvider) > 0 && metaProvider[0] != nil {
		getMeta = metaProvider[0]
	}

	dbgGossipStream.Printf("Responder started: streamID=%d, reconcileID=%d, peer=%s",
		stream.StreamID(), reconcileStream.StreamID(), peerNodeID)

	streamConn := adapter.NewStreamConn(stream, ctx)
	reconcileConn := adapter.NewStreamConn(reconcileStream, ctx)

	if peerNodeID == "" {
		dt = nil
	}

	runGossipResponderLoop(ctx, streamConn, reconcileConn, c, onExchange, getMeta, dt, peerNodeID)
	dbgGossipStream.Printf("Responder exited: streamID=%d, ctxErr=%v", stream.StreamID(), ctx.Err())
}

// isFatalGossipError returns true for errors that mean the connection is dead.
func isFatalGossipError(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "EOF") ||
		strings.Contains(s, "connection reset") ||
		strings.Contains(s, "broken pipe") ||
		strings.Contains(s, "use of closed") ||
		strings.Contains(s, "session closed")
}
