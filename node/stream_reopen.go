/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/ORBTR/aether"
	"github.com/bbmumford/loom/core/streams"
)

// Pacing of a stream's reopen on the side that opens it. The first open waits the
// minimum so the peer's consumer can read what the old stream's queue still holds
// and claim the ID; each open in a row doubles the wait, because a peer whose consumer
// is not yet claiming answers RESET(ResetRefused) through serveUnclaimedStream, and a
// peer that does not run the consumer at all keeps answering it for the session's
// life, which the maximum holds to one OPEN per session in that time. A generation
// that lived past streamReopenHealthy says the peer claims, so the next reopen starts
// from the minimum again.
const (
	streamReopenMin     = 250 * time.Millisecond
	streamReopenMax     = 30 * time.Second
	streamReopenHealthy = 2 * streamReopenMax
)

// typedStreamEnd reports whether err from Send or Receive is a stream reset with a
// typed reason (aether.ErrStreamOverrun, aether.ErrStreamRefused): the stream's ID
// has left both ends and the session stays up, so its consumer opens the stream
// again. On Noise and on byte-stream sessions alike they come after everything the
// stream's queue held, so nothing the peer delivered is behind this error.
func typedStreamEnd(err error) bool {
	return errors.Is(err, aether.ErrStreamOverrun) || errors.Is(err, aether.ErrStreamRefused)
}

// streamReopener establishes a stream with a fixed ID again on a live session, in
// the role that opened or claimed it the first time: the opener calls OpenStream
// and the other side AcceptStreamByID. BidiRPC's readLoop, MeshSwarmTransport's
// readLoop and AcceptMeshConnection's gossip loops hold one each. Not safe for
// concurrent use; each consumer's one reader goroutine uses its own.
type streamReopener struct {
	session aether.Session
	opens   bool
	cfg     aether.StreamConfig
	wait    time.Duration // the opener's next wait; zero starts at streamReopenMin
	opened  time.Time     // when the opener last opened a generation
}

// reopen returns the stream's next generation. It ends with an error when ctx ends,
// the session closes, or OpenStream answers anything but a refusal that clears:
// aether.ErrDuplicateStreamID while the session still holds the ID, and
// aether.ErrStreamCapExhausted and aether.ErrStreamCreditRefused while other streams
// hold the session's room. A Noise session holds the ID for its quiet time after both
// CLOSEs, and an ID this side reset until the peer acknowledges the RESET and for the
// quiet time after that (NoiseSession.reopenQuietLocked). A byte-stream session holds
// the ID while the old stream is registered, and an ID this side reset until the peer's
// acknowledgment and no longer: the byte stream carries that acknowledgment behind
// every frame of the old stream (TCPSession.resetAcknowledged).
func (r *streamReopener) reopen(ctx context.Context) (aether.Stream, error) {
	if !r.opens {
		return r.session.AcceptStreamByID(ctx, r.cfg.StreamID)
	}
	if time.Since(r.opened) > streamReopenHealthy {
		r.wait = 0
	}
	for {
		if r.wait == 0 {
			r.wait = streamReopenMin
		}
		timer := time.NewTimer(r.wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
		r.wait = min(2*r.wait, streamReopenMax)
		if r.session.IsClosed() {
			return nil, aether.ErrSessionClosed
		}
		st, err := r.session.OpenStream(ctx, r.cfg)
		if err == nil {
			r.opened = time.Now()
			return st, nil
		}
		if !errors.Is(err, aether.ErrDuplicateStreamID) &&
			!errors.Is(err, aether.ErrStreamCapExhausted) &&
			!errors.Is(err, aether.ErrStreamCreditRefused) {
			return nil, err
		}
	}
}

// gossipStreams is one session's pair of gossip streams: stream 0 and the reconcile
// stream, each with its reopener. AcceptMeshConnection's gossip loops pass current
// to each run and call reopenEnded before it.
type gossipStreams struct {
	gossip, reconcile                 aether.Stream
	gossipReopener, reconcileReopener streamReopener
}

// reopenEnded establishes again whichever of the two streams has ended: a typed
// reset leaves the session up and the stream gone from both ends, and a gossip loop
// handed that stream again would fail every exchange for the session's life. A
// stream still open is left in place. It returns false when a reopen ended, which
// happens only with ctx or the session. The side that claims waits here for the
// opener's OPEN; the session's other streams run on their own goroutines.
func (g *gossipStreams) reopenEnded(ctx context.Context, nodeID string) bool {
	reopenOne := func(st *aether.Stream, r *streamReopener) bool {
		if (*st).IsOpen() {
			return true
		}
		_ = (*st).Reset(aether.ResetCancel)
		fresh, err := r.reopen(ctx)
		if err != nil {
			dbgAether.Printf("%s: gossip stream %d reopen ended: %v", truncID(nodeID), r.cfg.StreamID, err)
			return false
		}
		log.Printf("[AETHER] %s: gossip stream %d reopened", truncID(nodeID), r.cfg.StreamID)
		*st = fresh
		return true
	}
	return reopenOne(&g.gossip, &g.gossipReopener) && reopenOne(&g.reconcile, &g.reconcileReopener)
}

// gossipStreamConfig is stream 0's configuration. On a Noise session the initial
// credit is the window this stream holds for the peer, and MaxCredit the ceiling
// OpenStream checks it against. A byte-stream session runs no aether credit; its
// delivery queue holds 1 MiB per stream.
func gossipStreamConfig() aether.StreamConfig {
	return aether.StreamConfig{
		StreamID: streams.StreamGossip, Reliability: aether.BestEffort, Priority: 64,
		InitialCredit: 1 * 1024 * 1024,
		MaxCredit:     2 * 1024 * 1024,
	}
}

// rpcStreamConfig is Stream 1's configuration; RPC payloads are typically under
// 10 KB but can batch, and on a Noise session 256 KB is the window for them.
func rpcStreamConfig() aether.StreamConfig {
	return aether.StreamConfig{
		StreamID: streams.StreamRPC, Reliability: aether.ReliableOrdered, Priority: 128,
		InitialCredit: 256 * 1024,
	}
}

// keepaliveStreamConfig is stream 2's configuration. It runs no FEC: nothing is sent
// through this stream, because AcceptMeshConnection's keepalive goes through
// Session.Ping, whose PING frames FEC does not cover, and a lost ping is what that
// loop's retries are for. Its credit is small because pings are under 50 bytes.
func keepaliveStreamConfig() aether.StreamConfig {
	return aether.StreamConfig{
		StreamID: streams.StreamKeepalive, Reliability: aether.UnreliableSequenced, Priority: 255,
		LatencyClass:  aether.ClassREALTIME,
		InitialCredit: 16 * 1024,
	}
}

// reconcileStreamConfig is the stream carrying G4 set-difference cells and G6
// snapshot frames. Its priority class matches gossip's, below RPC, keepalive and
// control, because reconcile is bulk convergence traffic. Its credit is smaller than
// gossip's because a G4 round moves O(|delta|).
func reconcileStreamConfig() aether.StreamConfig {
	return aether.StreamConfig{
		StreamID: streams.StreamReconcile, Reliability: aether.BestEffort, Priority: 64,
		InitialCredit: 256 * 1024,
		MaxCredit:     1 * 1024 * 1024,
	}
}
