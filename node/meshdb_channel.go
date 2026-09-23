/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ORBTR/aether"
	"github.com/bbmumford/loom/core/streams"
	"github.com/hashicorp/yamux"
)

// meshDBStreamConfig is the aether stream a peer channel rides on:
// streams.StreamMeshDB, reliable and ordered, in the interactive class so
// Raft heartbeats do not queue behind bulk transfers.
func meshDBStreamConfig() aether.StreamConfig {
	return aether.StreamConfig{
		StreamID:     streams.StreamMeshDB,
		Reliability:  aether.ReliableOrdered,
		Priority:     128,
		LatencyClass: aether.ClassINTERACTIVE,
	}
}

// meshDBYamuxConfig is the yamux session configuration of a channel. The
// keepalive and write timeout are what detect a peer that stopped answering
// while its session stays up.
func meshDBYamuxConfig(t meshDBTiming) *yamux.Config {
	cfg := yamux.DefaultConfig()
	cfg.EnableKeepAlive = true
	cfg.KeepAliveInterval = t.keepAlive
	cfg.ConnectionWriteTimeout = t.writeTimeout
	cfg.StreamOpenTimeout = 30 * time.Second
	cfg.StreamCloseTimeout = time.Minute
	cfg.LogOutput = nil
	cfg.Logger = log.New(log.Writer(), "[MESHDB] ", log.Flags())
	return cfg
}

// meshDBHealthyChannel is how long a channel must have lived for the lower
// end to reopen at once after it closes; a channel that dies sooner doubles
// the backoff.
const meshDBHealthyChannel = 30 * time.Second

// meshDBPeer is the transport's binding to one peer's current session. Its
// goroutine (run) keeps a channel on that session for as long as the session
// lives: the lower NodeID opens StreamMeshDB, the higher accepts it, and
// either end brings a new channel up after the last one closes.
type meshDBPeer struct {
	t       *MeshDBTransport
	id      string
	session aether.Session
	lower   bool

	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}

	// pokeCh wakes the lower end to re-check whether to open the channel.
	pokeCh chan struct{}
	// wanted is set by a Dial waiting for a channel, so the lower end opens
	// one even before the peer's metadata has arrived.
	wanted atomic.Bool

	mu sync.Mutex
	ch *meshDBChannel
}

// meshDBChannel is one yamux session over the peer's StreamMeshDB.
type meshDBChannel struct {
	peer *meshDBPeer
	conn *meshDBStreamConn
	mux  *yamux.Session
}

func newMeshDBPeer(t *MeshDBTransport, id string, session aether.Session) *meshDBPeer {
	ctx, cancel := context.WithCancel(t.ctx)
	return &meshDBPeer{
		t:       t,
		id:      id,
		session: session,
		lower:   t.localID < id,
		ctx:     ctx,
		cancel:  cancel,
		done:    make(chan struct{}),
		pokeCh:  make(chan struct{}, 1),
	}
}

func (p *meshDBPeer) poke() {
	select {
	case p.pokeCh <- struct{}{}:
	default:
	}
}

func (p *meshDBPeer) want() {
	if p.wanted.CompareAndSwap(false, true) {
		p.poke()
	}
}

// current returns the live channel, or nil.
func (p *meshDBPeer) current() *meshDBChannel {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ch == nil || p.ch.mux.IsClosed() {
		return nil
	}
	return p.ch
}

// stop ends the binding and waits for its goroutine, which closes the
// channel and every stream on it.
func (p *meshDBPeer) stop() {
	p.cancel()
	p.mu.Lock()
	ch := p.ch
	p.mu.Unlock()
	if ch != nil {
		ch.mux.Close()
	}
	<-p.done
}

func (p *meshDBPeer) run() {
	defer p.t.wg.Done()
	defer close(p.done)
	defer p.t.dropPeer(p)
	backoff := p.t.timing.reopenMin
	for p.ctx.Err() == nil && !p.session.IsClosed() {
		st, err := p.establish()
		if err != nil {
			if p.ctx.Err() != nil || p.session.IsClosed() {
				return
			}
			log.Printf("[MESHDB] channel to %s: %v; retrying in %v", truncID(p.id), err, backoff)
			if !p.sleep(backoff) {
				return
			}
			backoff = min(2*backoff, p.t.timing.reopenMax)
			continue
		}
		if lived := p.serve(st); lived >= meshDBHealthyChannel {
			backoff = p.t.timing.reopenMin
		} else if p.lower {
			if !p.sleep(backoff) {
				return
			}
			backoff = min(2*backoff, p.t.timing.reopenMax)
		}
	}
}

// establish opens (lower end) or claims (higher end) the channel's aether
// stream. The higher end claims by ID, so the OPEN never reaches the dynamic
// accept loop that serves unclaimed streams as RPC.
func (p *meshDBPeer) establish() (aether.Stream, error) {
	if !p.lower {
		st, err := p.session.AcceptStreamByID(p.ctx, streams.StreamMeshDB)
		if err == nil && p.ctx.Err() != nil {
			_ = st.Reset(aether.ResetCancel)
			return nil, p.ctx.Err()
		}
		return st, err
	}
	if !p.awaitEligible() {
		return nil, context.Canceled
	}
	return p.session.OpenStream(p.ctx, meshDBStreamConfig())
}

// awaitEligible waits until the lower end has a reason to open the channel:
// the peer publishes under a cluster key, or a Dial is waiting for it. A peer
// that publishes nothing runs no Raft member this process could reach, and
// may not run this transport at all.
func (p *meshDBPeer) awaitEligible() bool {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	for {
		if p.wanted.Load() || p.t.dir.publishesAny(p.id) {
			return true
		}
		select {
		case <-p.ctx.Done():
			return false
		case <-p.pokeCh:
		case <-tick.C:
			if p.session.IsClosed() {
				return false
			}
		}
	}
}

func (p *meshDBPeer) sleep(d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return !p.session.IsClosed()
	case <-p.ctx.Done():
		return false
	}
}

// serve runs one channel over st until it closes, serving every inbound
// stream, and reports how long it lived.
func (p *meshDBPeer) serve(st aether.Stream) time.Duration {
	start := time.Now()
	conn := newMeshDBStreamConn(st, p.t.timing.writeTimeout)
	cfg := meshDBYamuxConfig(p.t.timing)
	var (
		mux *yamux.Session
		err error
	)
	if p.lower {
		mux, err = yamux.Client(conn, cfg)
	} else {
		mux, err = yamux.Server(conn, cfg)
	}
	if err != nil {
		conn.Close()
		log.Printf("[MESHDB] channel to %s: %v", truncID(p.id), err)
		return 0
	}
	ch := &meshDBChannel{peer: p, conn: conn, mux: mux}
	p.mu.Lock()
	if p.ctx.Err() != nil {
		p.mu.Unlock()
		mux.Close()
		return 0
	}
	p.ch = ch
	p.wanted.Store(false)
	p.mu.Unlock()
	p.t.notify()
	log.Printf("[MESHDB] channel to %s up (lower=%v)", truncID(p.id), p.lower)

	var inbound sync.WaitGroup
	for {
		s, err := mux.AcceptStream()
		if err != nil {
			break
		}
		inbound.Add(1)
		go func() {
			defer inbound.Done()
			p.t.serveInbound(ch, s)
		}()
	}
	mux.Close()
	p.mu.Lock()
	if p.ch == ch {
		p.ch = nil
	}
	p.mu.Unlock()
	inbound.Wait()
	p.t.notify()
	lived := time.Since(start)
	log.Printf("[MESHDB] channel to %s down after %v", truncID(p.id), lived.Round(time.Millisecond))
	return lived
}

// serveInbound reads an inbound stream's cluster key, authorizes it, and
// hands it to that key's listener; anything else closes the stream.
func (t *MeshDBTransport) serveInbound(ch *meshDBChannel, s *yamux.Stream) {
	remote := ch.peer.id
	key, err := readMeshDBHeader(s, time.Now().Add(t.timing.header))
	if err == nil {
		err = t.authorize(ch, key)
	}
	var ln *meshDBListener
	if err == nil {
		t.mu.Lock()
		ln = t.listeners[key]
		t.mu.Unlock()
		if ln == nil {
			err = fmt.Errorf("no listener for %s", key)
		}
	}
	if err != nil {
		t.refused.Add(1)
		log.Printf("[MESHDB] refused a stream from %s for %q: %v", truncID(remote), key, err)
		s.Close()
		return
	}
	c := &meshDBConn{Stream: s, local: t.localID, remote: remote, key: key}
	select {
	case ln.ch <- c:
	case <-ln.closed:
		s.Close()
	case <-ch.mux.CloseChan():
		s.Close()
	case <-t.ctx.Done():
		s.Close()
	}
}

// authorize admits a stream for key from the channel's peer when the channel
// is still the peer's current one, the peer publishes under key, and the
// service it names has a seat in key's raft block. Metadata and service
// arrive by gossip, so a stream from a peer this process has not yet heard
// from waits up to authWait for them; a known service without a seat is
// refused at once.
func (t *MeshDBTransport) authorize(ch *meshDBChannel, key string) error {
	seats, ok := t.allowed[key]
	if !ok {
		return fmt.Errorf("%w: %s", errMeshDBUnknownKey, key)
	}
	remote := ch.peer.id
	timer := time.NewTimer(t.timing.authWait)
	defer timer.Stop()
	for {
		wait := t.watch()
		t.mu.Lock()
		cur := t.peers[remote]
		t.mu.Unlock()
		if cur != ch.peer || ch.peer.current() != ch || ch.peer.session.IsClosed() {
			return errMeshDBStaleStream
		}
		svc := t.dir.service(remote)
		published := t.dir.published(key, remote)
		if svc != "" && !seats[svc] {
			return fmt.Errorf("service %q has no seat in %s", svc, key)
		}
		if published && svc != "" {
			return nil
		}
		select {
		case <-wait:
		case <-timer.C:
			if !published {
				return fmt.Errorf("%s does not publish under %s", truncID(remote), key)
			}
			return errors.New("the peer's service is unknown")
		case <-ch.mux.CloseChan():
			return errMeshDBStaleStream
		case <-t.ctx.Done():
			return errMeshDBStopped
		}
	}
}
