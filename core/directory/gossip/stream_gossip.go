/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package gossip

import (
	"context"
	"errors"
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
// stream carries the stream-0 frames (G1 record exchange, G2 digest probe,
// GB bucket digest, G3 rumor, G5 rumor ACK). reconcileStream carries G4
// set-difference reconciliation and G6 snapshot frames on a dedicated aether
// stream — separating these traffic classes eliminates the abandoned-read race
// where a timed-out G4 round's residual bytes poison G1's read path on a shared
// stream (surfacing as "invalid magic 0x4734"). Both streams are required.
//
// p is the gossip profile the session negotiated (NegotiatedProfile); every
// exchange is written and read in its codec and reconciliation profile.
func RunGossipLoopWithPeer(ctx context.Context, stream aether.Stream, reconcileStream aether.Stream, c *cache.DirectoryCache, p Profile,
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
	streamConn := newGossipConn(stream, ctx)
	reconcileConn := newGossipConn(reconcileStream, ctx)

	// Delta sync needs the peer's identity to key its watermark (it cuts an
	// exchange from ~100KB to ~1KB).
	if peerNodeID == "" {
		dt = nil
	}
	runGossipLoopInternal(ctx, streamConn, reconcileConn, c, p, interval, onExchange, getMeta, dt, peerNodeID, nil, nil)
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
// reconcileStream is the dedicated aether stream for G4/G6 frames; a parallel
// responder loop dispatches G4 (set-difference reconcile) and G6 (snapshot)
// frames on that stream while G1, G2 and GB stay on `stream`. Both streams are
// required. dt is the caller's delta tracker and p the session's negotiated
// gossip profile, as for RunGossipLoopWithPeer.
func RunGossipResponder(ctx context.Context, stream aether.Stream, reconcileStream aether.Stream, c *cache.DirectoryCache, p Profile,
	peerNodeID string, dt *DeltaTracker, onExchange func(GossipResult),
	metaProvider ...func() *ExchangeMeta) {

	var getMeta func() *ExchangeMeta
	if len(metaProvider) > 0 && metaProvider[0] != nil {
		getMeta = metaProvider[0]
	}

	dbgGossipStream.Printf("Responder started: streamID=%d, reconcileID=%d, peer=%s",
		stream.StreamID(), reconcileStream.StreamID(), peerNodeID)

	streamConn := newGossipConn(stream, ctx)
	reconcileConn := newGossipConn(reconcileStream, ctx)

	if peerNodeID == "" {
		dt = nil
	}

	runGossipResponderLoop(ctx, streamConn, reconcileConn, c, p, onExchange, getMeta, dt, peerNodeID)
	dbgGossipStream.Printf("Responder exited: streamID=%d, ctxErr=%v", stream.StreamID(), ctx.Err())
}

// gossipChunkSize bounds one Send of a gossip conn. Whisper and GossipOverConn write
// a frame's body with one Write, and a Write on a StreamConn is one Send, while a
// session limits one message below the frame caps here: a byte-stream session to the
// 1 MiB its delivery queue holds per stream, less the queue's 64 B entry cost, and a
// Noise session to its fragment count at the path's MSS. Readers use io.ReadFull, so where a body is split does not
// reach them.
const gossipChunkSize = 64 * 1024

// gossipConn is the net.Conn every gossip and reconcile engine runs on. A Write
// splits into Sends of at most gossipChunkSize. Close resets the stream as well as
// closing it, because the conn's reader is what Close stops: nothing would read the
// stream's queue again, and the session's gossip loop reopens an ended stream
// (node's gossipStreams), which a half-closed one would never become.
type gossipConn struct {
	*adapter.StreamConn
	stream aether.Stream
}

func newGossipConn(stream aether.Stream, ctx context.Context) *gossipConn {
	return &gossipConn{StreamConn: adapter.NewStreamConn(stream, ctx), stream: stream}
}

// Write implements net.Conn. It returns the bytes of the Sends that succeeded.
func (c *gossipConn) Write(p []byte) (int, error) {
	written := 0
	for written < len(p) {
		end := min(written+gossipChunkSize, len(p))
		n, err := c.StreamConn.Write(p[written:end])
		written += n
		if err != nil {
			return written, err
		}
	}
	return written, nil
}

// Close implements net.Conn.
func (c *gossipConn) Close() error {
	err := c.StreamConn.Close()
	_ = c.stream.Reset(aether.ResetCancel)
	return err
}

// streamEnded reports whether the stream under this conn has ended, which
// runGossipLoopInternal reads to leave a reconcile stream no G4 round can use.
func (c *gossipConn) streamEnded() bool { return !c.stream.IsOpen() }

// isFatalGossipError returns true for errors that mean the connection or its stream
// is dead. A stream ended with a typed reset (aether.ErrStreamOverrun,
// aether.ErrStreamRefused) returns its end from every further read at once, so read
// as transient it would spin whisper's RunResponder and fail every initiator
// exchange for the session's life; read as fatal it ends the loop and the session's
// gossip loop reopens the stream. aether.ErrSessionWriteFailed is the session's end.
func isFatalGossipError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, aether.ErrStreamOverrun) || errors.Is(err, aether.ErrStreamRefused) ||
		errors.Is(err, aether.ErrSessionWriteFailed) || errors.Is(err, aether.ErrSessionClosed) ||
		errors.Is(err, errGossipDesynchronised) {
		return true
	}
	s := err.Error()
	return strings.Contains(s, "EOF") ||
		strings.Contains(s, "connection reset") ||
		strings.Contains(s, "broken pipe") ||
		strings.Contains(s, "use of closed") ||
		strings.Contains(s, "session closed")
}
