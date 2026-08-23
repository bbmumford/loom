/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"
	"time"

	"github.com/ORBTR/aether"
	"github.com/bbmumford/swarm"
)

// ErrPeerNotRegistered is returned by Send when the destination peer has no
// swarm stream on this transport — it was never attached, or was unregistered.
//
// It exists because the previous behaviour was to log "SKIP" and return nil,
// which reported a successful send for a frame that never reached the wire.
// That is worse than a swallowed error: a caller told it succeeded cannot
// retry, and the swarm layer's anti-entropy cannot repair what it believes
// was delivered. The common way to reach this state is the responder side of
// RegisterPeer, where a failed AcceptStreamByID(100) returns without attaching
// and without retrying, so the peer stays absent for the life of the session.
//
// Callers that legitimately treat an absent peer as a no-op should compare
// with errors.Is rather than ignoring every error from Send.
var ErrPeerNotRegistered = errors.New("swarm transport: peer not registered")

// MeshSwarmTransport bridges aether sessions to the swarm.Transport
// interface. It owns one dedicated aether stream per connected peer and
// routes swarm frames over it.
//
// Frame format on the wire: [length: uint32 BE][frame bytes]. The length
// prefix lets the reader recover from partial reads and stream boundaries.
type MeshSwarmTransport struct {
	localID swarm.NodeID

	mu       sync.RWMutex
	peers    map[swarm.NodeID]*meshSwarmPeer
	receiver func(from swarm.NodeID, frame []byte)
	onJoin   func(id swarm.NodeID)
	onLeave  func(id swarm.NodeID)

	// stopped is set by Stop under mu. attachPeer refuses once it is set:
	// spawning a read loop after Stop has begun would both leak the
	// goroutine past shutdown and race wg.Add against wg.Wait.
	stopped bool
	// wg counts live per-peer read loops so Stop can JOIN them rather than
	// merely signalling them. Without it Stop returns while a read loop is
	// still inside the OnReceive callback, delivering a frame into a swarm
	// engine the caller has already torn down.
	wg sync.WaitGroup

	cancel context.CancelFunc
	ctx    context.Context
}

// meshSwarmPeer is per-peer state inside MeshSwarmTransport. Each peer has
// exactly one dedicated swarm stream; reads happen on a per-peer goroutine.
type meshSwarmPeer struct {
	id      swarm.NodeID
	session aether.Session
	stream  aether.Stream
	cancel  context.CancelFunc
}

// NewMeshSwarmTransport constructs the transport for a node whose swarm
// NodeID is localID. Use RegisterPeer / UnregisterPeer to attach aether
// sessions as the ConnectionManager establishes them.
func NewMeshSwarmTransport(localID swarm.NodeID) *MeshSwarmTransport {
	ctx, cancel := context.WithCancel(context.Background())
	return &MeshSwarmTransport{
		localID: localID,
		peers:   make(map[swarm.NodeID]*meshSwarmPeer),
		ctx:     ctx,
		cancel:  cancel,
	}
}

// Stop shuts down all peer read loops, closes streams, and WAITS for the read
// loops to finish.
//
// The wait is the point. Cancelling a read loop's context only asks it to
// stop; the loop may be inside the OnReceive callback at that instant, and
// that callback reaches into the swarm engine the caller is shutting down.
// Returning before the join makes "transport stopped" a claim the transport
// cannot honour — the map is empty and the frame still lands afterwards.
// Same defect class as swarm's own blocker 1 ("join its goroutines"), on
// loom's side of the seam.
//
// Idempotent, and safe to call concurrently with itself.
func (t *MeshSwarmTransport) Stop() {
	t.cancel()
	t.mu.Lock()
	t.stopped = true
	for _, p := range t.peers {
		if p.cancel != nil {
			p.cancel()
		}
		if p.stream != nil {
			_ = p.stream.Close()
		}
	}
	t.peers = make(map[swarm.NodeID]*meshSwarmPeer)
	// Released BEFORE the join: a read loop's teardown
	// (unregisterPeerInstance) takes this same lock, so waiting while
	// holding it deadlocks shutdown outright.
	t.mu.Unlock()

	t.wg.Wait()
}

// LocalID implements swarm.Transport.
func (t *MeshSwarmTransport) LocalID() swarm.NodeID { return t.localID }

// Send implements swarm.Transport. Writes the framed payload to the peer's
// dedicated swarm stream.
//
// Returns ErrPeerNotRegistered when the peer has no stream on this transport.
// Returning nil there would make an unreachable peer indistinguishable from a
// delivered frame.
func (t *MeshSwarmTransport) Send(to swarm.NodeID, frame []byte) error {
	t.mu.RLock()
	p, ok := t.peers[to]
	known := ok
	streamOk := ok && p.stream != nil
	t.mu.RUnlock()
	if !known || !streamOk {
		log.Printf("[SWARM] Send: SKIP peer=%s known=%v streamOk=%v bytes=%d", truncID(string(to)), known, streamOk, len(frame))
		return fmt.Errorf("%w: peer=%s known=%v streamOk=%v", ErrPeerNotRegistered, truncID(string(to)), known, streamOk)
	}
	err := sendFramed(t.ctx, p.stream, frame)
	if err != nil {
		log.Printf("[SWARM] Send: FAILED peer=%s bytes=%d err=%v", truncID(string(to)), len(frame), err)
	} else {
		log.Printf("[SWARM] Send: ok peer=%s bytes=%d", truncID(string(to)), len(frame))
	}
	return err
}

// Broadcast implements swarm.Transport. Sends to every registered peer and
// returns the joined errors of any that failed; a peer whose stream is absent
// counts as ErrPeerNotRegistered rather than being skipped silently.
//
// NOTE ON REACH: this method currently has NO callers. The swarm engine
// fans out via repeated Send (plumtrees/merkle) and its own
// plumtreesEngine.Broadcast, never through the Transport's. It is
// implemented here only to satisfy swarm.Transport. It reports failures anyway
// so that whoever wires it first does not inherit a method that returns nil
// unconditionally while discarding every error.
func (t *MeshSwarmTransport) Broadcast(frame []byte) error {
	t.mu.RLock()
	peers := make([]*meshSwarmPeer, 0, len(t.peers))
	for _, p := range t.peers {
		peers = append(peers, p)
	}
	t.mu.RUnlock()
	log.Printf("[SWARM] Broadcast: bytes=%d targets=%d", len(frame), len(peers))

	var errs []error
	for _, p := range peers {
		if p.stream == nil {
			errs = append(errs, fmt.Errorf("%w: peer=%s", ErrPeerNotRegistered, truncID(string(p.id))))
			continue
		}
		if err := sendFramed(t.ctx, p.stream, frame); err != nil {
			errs = append(errs, fmt.Errorf("peer=%s: %w", truncID(string(p.id)), err))
		}
	}
	return errors.Join(errs...)
}

// Peers implements swarm.Transport.
func (t *MeshSwarmTransport) Peers() []swarm.NodeID {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]swarm.NodeID, 0, len(t.peers))
	for id := range t.peers {
		out = append(out, id)
	}
	return out
}

// OnReceive implements swarm.Transport. The callback is invoked from
// per-peer goroutines and must be safe for concurrent use.
func (t *MeshSwarmTransport) OnReceive(fn func(from swarm.NodeID, frame []byte)) {
	t.mu.Lock()
	t.receiver = fn
	t.mu.Unlock()
}

// OnPeerJoin implements swarm.Transport.
func (t *MeshSwarmTransport) OnPeerJoin(fn func(id swarm.NodeID)) {
	t.mu.Lock()
	t.onJoin = fn
	t.mu.Unlock()
}

// OnPeerLeave implements swarm.Transport.
func (t *MeshSwarmTransport) OnPeerLeave(fn func(id swarm.NodeID)) {
	t.mu.Lock()
	t.onLeave = fn
	t.mu.Unlock()
}

// RegisterPeer attaches an aether session for a peer and binds the
// dedicated swarm stream (well-known StreamID 100). Both sides of the
// session call RegisterPeer; deterministic role assignment by NodeID
// comparison decides which side calls OpenStream (pushes OPEN onto the
// wire) and which side calls AcceptStreamByID(100) (claims the stream
// on arrival).
//
// Both sides MUST arm the StreamID-specific path: if the responder
// falls through to the dynamic FIFO AcceptStream loop, the swarm OPEN
// frame is grabbed by the RPC dynamic-stream handler in
// mesh_connection.go, which then unmarshals swarm protobuf as an
// RPCRequest and produces "invalid UTF-8" errors plus silent
// loss of every swarm frame on that peer.
//
// Idempotent — registering the same peer twice replaces the previous
// stream/goroutine via attachPeer.
func (t *MeshSwarmTransport) RegisterPeer(ctx context.Context, peerID swarm.NodeID, session aether.Session) error {
	log.Printf("[SWARM] RegisterPeer: entry peer=%s session=%v", truncID(string(peerID)), session != nil)
	if session == nil {
		log.Printf("[SWARM] RegisterPeer: ERROR nil session peer=%s", truncID(string(peerID)))
		return fmt.Errorf("swarm transport: nil session for peer %s", peerID)
	}

	selfID := string(t.localID)
	peerIDStr := string(peerID)

	// Deterministic tie-break: the lower NodeID is the initiator and
	// calls OpenStream(100), which is what pushes the OPEN frame on
	// the wire. The higher NodeID is the responder and arms
	// AcceptStreamByID(100) so the OPEN is claimed by the
	// StreamID-specific waiter rather than the FIFO dynamic-stream
	// handler. If both sides ever opened, aether's duplicate-OPEN
	// handling collapses to a single stream object and attachPeer is
	// idempotent.
	if selfID < peerIDStr {
		// Initiator: OpenStream(100) is fast (it just pushes the OPEN frame),
		// so keep it synchronous.
		stream, err := session.OpenStream(ctx, swarmStreamConfig())
		if err != nil {
			log.Printf("[SWARM] RegisterPeer: OpenStream(100) FAILED peer=%s: %v", truncID(peerIDStr), err)
			return fmt.Errorf("swarm transport: open stream for %s: %w", peerID, err)
		}
		log.Printf("[SWARM] RegisterPeer: OpenStream(100) ok peer=%s streamID=%d (initiator)", truncID(peerIDStr), stream.StreamID())
		t.attachPeer(peerID, session, stream)
		log.Printf("[SWARM] RegisterPeer: attachPeer returned peer=%s", truncID(peerIDStr))
		return nil
	}

	// Responder — AcceptStreamByID(100) blocks up to 30s waiting for
	// the peer's OPEN(100). RegisterPeer fires from the session hook BEFORE this
	// session's keepalive/gossip/bidi service goroutines start, so blocking here
	// for a slow or older-binary peer stalled the entire session's startup (up
	// to 30s of no keepalive/gossip). Arm the accept + attach in a goroutine so
	// RegisterPeer returns immediately; the swarm stream binds whenever the OPEN
	// arrives. ctx is the long-lived runtime ctx, so the goroutine lives exactly
	// as long as it should and exits on session/runtime teardown.
	go func() {
		acceptCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		stream, err := session.AcceptStreamByID(acceptCtx, 100)
		if err != nil {
			log.Printf("[SWARM] RegisterPeer: AcceptStreamByID(100) FAILED peer=%s: %v", truncID(peerIDStr), err)
			return
		}
		log.Printf("[SWARM] RegisterPeer: AcceptStreamByID(100) ok peer=%s streamID=%d (responder, async)", truncID(peerIDStr), stream.StreamID())
		t.attachPeer(peerID, session, stream)
		log.Printf("[SWARM] RegisterPeer: attachPeer returned peer=%s (async)", truncID(peerIDStr))
	}()
	return nil
}

// AcceptPeer registers a peer using an already-accepted incoming swarm
// stream. Called by the consumer's AcceptStream loop when it identifies
// an incoming stream as belonging to the swarm protocol.
func (t *MeshSwarmTransport) AcceptPeer(peerID swarm.NodeID, session aether.Session, stream aether.Stream) {
	t.attachPeer(peerID, session, stream)
}

// UnregisterPeer closes the peer's swarm stream and removes it from the
// transport. Subsequent Send calls to this peer are silently dropped.
func (t *MeshSwarmTransport) UnregisterPeer(peerID swarm.NodeID) {
	t.mu.Lock()
	p, ok := t.peers[peerID]
	if !ok {
		t.mu.Unlock()
		return
	}
	delete(t.peers, peerID)
	onLeave := t.onLeave
	t.mu.Unlock()

	if p.cancel != nil {
		p.cancel()
	}
	if p.stream != nil {
		_ = p.stream.Close()
	}
	if onLeave != nil {
		onLeave(peerID)
	}
}

// unregisterPeerInstance removes the peer ONLY if the current map entry is
// still this exact *meshSwarmPeer. It is the teardown for readLoop.
//
// Why the identity check matters: a grade upgrade (WS → noise-UDP) promotes a
// new session for the same peerID. Its session hook calls RegisterPeer →
// attachPeer, which cancels THIS (old-WS) loop's ctx and installs the new noise
// peer at t.peers[peerID]. This loop then exits and runs its deferred teardown.
// The unconditional UnregisterPeer(peerID) would delete whatever now sits at
// peerID — the freshly-installed NOISE peer — and fire onLeave, dropping the
// promoted session from the transport: every subsequent Send SKIPs (known=false)
// and the δ-CRDT anti-entropy pump never writes a frame to the noise session.
// onLeave also drops the peer from the swarm gossip target sets, so its
// fleet.peer record stops propagating and the AddressTable loses the peer's
// noise-UDP dial candidate — the direct path is then never re-dialed (its
// upgrade-walker wake never fires). The promoted session does NOT self-recover:
// the stall detector gates on in-flight data, so a purely idle path never trips
// it. Guarding on instance identity leaves the newer peer intact. Mirrors the
// expected-session guard in unregisterMeshSession.
//
// The public UnregisterPeer keeps its force-remove semantics for the genuine
// session-LEAVE hook (swarm_integration.go), which intends to remove whatever
// session is current for the peer.
func (t *MeshSwarmTransport) unregisterPeerInstance(p *meshSwarmPeer) {
	t.mu.Lock()
	if cur, ok := t.peers[p.id]; !ok || cur != p {
		// A newer session already replaced this peer entry — leave it intact.
		t.mu.Unlock()
		return
	}
	delete(t.peers, p.id)
	onLeave := t.onLeave
	t.mu.Unlock()

	if p.cancel != nil {
		p.cancel()
	}
	if p.stream != nil {
		_ = p.stream.Close()
	}
	if onLeave != nil {
		onLeave(p.id)
	}
}

// attachPeer installs the peer + spawns its read goroutine. Replaces any
// existing entry for the same peerID (and tears down the prior stream).
func (t *MeshSwarmTransport) attachPeer(peerID swarm.NodeID, session aether.Session, stream aether.Stream) {
	t.mu.Lock()
	if t.stopped {
		// Attaching after Stop would leak a read loop past shutdown and race
		// wg.Add against the wg.Wait already in progress. Close the stream
		// we were handed rather than silently retaining it.
		t.mu.Unlock()
		if stream != nil {
			_ = stream.Close()
		}
		log.Printf("[SWARM] attachPeer: REFUSED peer=%s — transport stopped", truncID(string(peerID)))
		return
	}
	if existing, ok := t.peers[peerID]; ok {
		if existing.cancel != nil {
			existing.cancel()
		}
		if existing.stream != nil {
			_ = existing.stream.Close()
		}
	}
	pctx, pcancel := context.WithCancel(t.ctx)
	p := &meshSwarmPeer{
		id:      peerID,
		session: session,
		stream:  stream,
		cancel:  pcancel,
	}
	t.peers[peerID] = p
	onJoin := t.onJoin
	peerCount := len(t.peers)
	// Counted while still holding mu, so it cannot race the wg.Wait that a
	// concurrent Stop performs after releasing the lock.
	t.wg.Add(1)
	t.mu.Unlock()

	log.Printf("[SWARM] attachPeer: installed peer=%s onJoin=%v totalPeers=%d", truncID(string(peerID)), onJoin != nil, peerCount)
	if onJoin != nil {
		onJoin(peerID)
		log.Printf("[SWARM] attachPeer: onJoin callback fired peer=%s", truncID(string(peerID)))
	}
	go t.readLoop(pctx, p)
}

// readLoop is the per-peer reader. Receives whole frames from the
// aether stream and invokes the registered OnReceive callback.
//
// aether.Stream.Receive delivers payload-sized chunks already, so no
// length prefixing is needed at this layer — each Receive call yields
// one logical swarm frame.
func (t *MeshSwarmTransport) readLoop(ctx context.Context, p *meshSwarmPeer) {
	// Deferred FIRST so it runs LAST: Stop's join must not be released until
	// the peer teardown below has also finished.
	defer t.wg.Done()
	defer t.unregisterPeerInstance(p)
	log.Printf("[SWARM] readLoop: entry peer=%s streamID=%d", truncID(string(p.id)), p.stream.StreamID())

	firstFrame := true
	for {
		frame, err := p.stream.Receive(ctx)
		if err != nil {
			log.Printf("[SWARM] readLoop: Receive exit peer=%s: %v", truncID(string(p.id)), err)
			return
		}
		if firstFrame {
			log.Printf("[SWARM] readLoop: FIRST frame received peer=%s bytes=%d", truncID(string(p.id)), len(frame))
			firstFrame = false
		}
		log.Printf("[SWARM] readLoop: frame peer=%s bytes=%d", truncID(string(p.id)), len(frame))
		t.mu.RLock()
		fn := t.receiver
		t.mu.RUnlock()
		if fn != nil {
			fn(p.id, frame)
		} else {
			log.Printf("[SWARM] readLoop: WARNING no receiver fn registered peer=%s", truncID(string(p.id)))
		}
	}
}

// sendFramed wraps Stream.Send so future encoding changes (e.g. adding a
// per-frame envelope for compression hints) have a single chokepoint.
func sendFramed(ctx context.Context, s aether.Stream, payload []byte) error {
	return s.Send(ctx, payload)
}

// Suppress unused imports while the framing helpers are simplified.
var _ = io.EOF
var _ = binary.BigEndian
var _ = fmt.Sprint

// swarmStreamConfig returns the aether.StreamConfig used for the
// dedicated swarm protocol stream per peer. Application-stream ID 100
// (well-known IDs <100 are reserved by aether for gossip/RPC/keepalive/
// control). Reliable + interactive class so eager push, IHave and
// graft frames don't queue behind bulk content fetches.
func swarmStreamConfig() aether.StreamConfig {
	return aether.StreamConfig{
		StreamID:     100,
		Reliability:  aether.ReliableOrdered,
		Priority:     128,
		LatencyClass: aether.ClassINTERACTIVE,
	}
}
