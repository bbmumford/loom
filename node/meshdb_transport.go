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
	"net"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ORBTR/aether"
	"github.com/bbmumford/tursoraft/mesh"
)

// Errors a Dial, Listen or inbound stream can end with.
var (
	errMeshDBStopped     = errors.New("meshdb transport: stopped")
	errMeshDBListening   = errors.New("meshdb transport: a listener is already open for this key")
	errMeshDBUnknownKey  = errors.New("meshdb transport: no configured raft block for this cluster key")
	errMeshDBStaleStream = errors.New("meshdb transport: stream arrived on a channel that is no longer the peer's")
)

// meshDBTiming is every duration the transport runs by.
type meshDBTiming struct {
	// keepAlive is how often yamux pings the peer; a ping unanswered within
	// writeTimeout closes the channel. A stalled peer is therefore detected
	// within keepAlive + writeTimeout.
	keepAlive time.Duration
	// writeTimeout bounds each Send on the aether stream and each yamux
	// write and ping.
	writeTimeout time.Duration
	// header bounds reading an inbound stream's cluster-key header.
	header time.Duration
	// authWait is how long an inbound stream waits for its dialer's
	// metadata and service to arrive by gossip before it is refused: a
	// leader probing a member that has not yet received the leader's own
	// record would otherwise be refused and not retried for tursoraft's
	// probe retry (30 s).
	authWait time.Duration
	// reopenMin and reopenMax bound the lower NodeID's backoff between
	// attempts to open the channel stream.
	reopenMin, reopenMax time.Duration
}

// defaultMeshDBTiming is the production timing. tursoraft's Raft heartbeat
// and election timeout are 1 s and its stream handshake allows 5 s; a dead
// channel is found within 5 s here, and a header plus authorization fit
// inside the handshake.
func defaultMeshDBTiming() meshDBTiming {
	return meshDBTiming{
		keepAlive:    2 * time.Second,
		writeTimeout: 3 * time.Second,
		header:       4 * time.Second,
		authWait:     3 * time.Second,
		reopenMin:    250 * time.Millisecond,
		reopenMax:    30 * time.Second,
	}
}

// MeshDBTransport is tursoraft's mesh.Mesh over the endpoint mesh.
//
// Each peer pair shares one aether stream, streams.StreamMeshDB, on their
// session. The lower NodeID opens it and runs the yamux client; the higher
// keeps AcceptStreamByID(StreamMeshDB) armed for the session's life and runs
// the yamux server (the swarm pattern for StreamSwarm). Every Raft stream is
// a yamux stream on that channel and opens with a header naming its cluster
// key; the member proof then runs inside tursoraft over it.
//
// Consumes session join and leave events (ConnectionManager.AddSessionHook),
// the swarm membership view (meshDBDirectory) and the cluster keys this
// process's config declares. Consumed by tursoraft's manager
// (ManagerConfig.Mesh), which Runtime.Shutdown stops before this transport so
// its withdrawal and leadership handoff still reach the mesh.
//
// An inbound stream is served only when its channel is the current one for
// the peer, the dialing NodeID publishes under the stream's key, and its
// service has a seat in that key's raft block (a voter service or a job host
// service). The NodeID comes from the Noise-authenticated session; the
// service is the one the node names in its signed PeerRecord.
type MeshDBTransport struct {
	localID string
	service string
	timing  meshDBTiming
	// allowed maps each configured cluster key to the services with a seat.
	allowed map[string]map[string]bool
	dir     *meshDBDirectory

	// connect asks the connection manager for a session to a peer with none;
	// nil in tests.
	connect func(nodeID string)
	// currentSession returns the session the connection manager dispatches
	// to a peer on (ConnectionManager.GetMeshSession); rebind reads it once
	// the bound session is gone. Nil in tests that bind sessions by hand.
	currentSession func(nodeID string) (aether.Session, bool)
	// removeHook ends the session-hook subscription; nil in tests.
	removeHook func()

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu        sync.Mutex
	stopped   bool
	peers     map[string]*meshDBPeer
	listeners map[string]*meshDBListener
	subs      map[chan struct{}]struct{}
	// state is closed and replaced on every change a waiter might care
	// about: a channel up or down, a peer bound or unbound, a directory
	// change.
	state chan struct{}

	refused atomic.Uint64
}

var _ mesh.Mesh = (*MeshDBTransport)(nil)

// newMeshDBTransport builds a transport for localID running service, serving
// the cluster keys in allowed. The caller attaches the directory to swarm and
// subscribes bind/unbind to session events.
func newMeshDBTransport(localID, service string, allowed map[string]map[string]bool, timing meshDBTiming) *MeshDBTransport {
	ctx, cancel := context.WithCancel(context.Background())
	t := &MeshDBTransport{
		localID:   localID,
		service:   service,
		timing:    timing,
		allowed:   allowed,
		ctx:       ctx,
		cancel:    cancel,
		peers:     map[string]*meshDBPeer{},
		listeners: map[string]*meshDBListener{},
		subs:      map[chan struct{}]struct{}{},
		state:     make(chan struct{}),
	}
	t.dir = newMeshDBDirectory(localID, t.notify)
	return t
}

// LocalID implements mesh.Mesh.
func (t *MeshDBTransport) LocalID() string { return t.localID }

// LocalService implements mesh.Mesh.
func (t *MeshDBTransport) LocalService() string { return t.service }

// Publish implements mesh.Mesh.
func (t *MeshDBTransport) Publish(key string, meta map[string]string) error {
	if t.isStopped() {
		return errMeshDBStopped
	}
	return t.dir.publish(key, meta)
}

// Members implements mesh.Mesh. A member is listed when it publishes under
// key, this process holds a live session to it, and its service is known;
// this process is listed once it has published. A node whose session is gone
// drops out at once, so tursoraft's absence timers start when the peer
// becomes unreachable rather than never, which is what a record kept by
// gossip after its node died would give.
func (t *MeshDBTransport) Members(key string) []mesh.Member {
	recs := t.dir.records(key)
	out := make([]mesh.Member, 0, len(recs))
	for node, meta := range recs {
		m := mesh.Member{NodeID: node, Meta: meta}
		if node == t.localID {
			m.Service = t.service
		} else {
			if !t.hasLiveSession(node) {
				continue
			}
			if m.Service = t.dir.service(node); m.Service == "" {
				continue
			}
		}
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NodeID < out[j].NodeID })
	return out
}

// Changes implements mesh.Mesh.
func (t *MeshDBTransport) Changes() (<-chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	t.mu.Lock()
	t.subs[ch] = struct{}{}
	t.mu.Unlock()
	var once sync.Once
	return ch, func() {
		once.Do(func() {
			t.mu.Lock()
			delete(t.subs, ch)
			t.mu.Unlock()
		})
	}
}

// notify wakes every Changes subscriber and state waiter, and lets the lower
// end of each unconnected peer re-check whether it should open the channel.
func (t *MeshDBTransport) notify() {
	t.mu.Lock()
	for ch := range t.subs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	close(t.state)
	t.state = make(chan struct{})
	peers := make([]*meshDBPeer, 0, len(t.peers))
	for _, p := range t.peers {
		peers = append(peers, p)
	}
	t.mu.Unlock()
	for _, p := range peers {
		p.poke()
	}
}

// watch returns a channel closed at the next notify.
func (t *MeshDBTransport) watch() <-chan struct{} {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.state
}

// Dial implements mesh.Mesh: it opens a yamux stream on the peer's channel and
// writes the cluster-key header. With no channel yet it waits for one until
// ctx ends, asking the lower end to open it now, or the connection manager
// for a session when there is none.
func (t *MeshDBTransport) Dial(ctx context.Context, nodeID, key string) (mesh.Conn, error) {
	asked := false
	for {
		wait := t.watch()
		t.mu.Lock()
		stopped, p := t.stopped, t.peers[nodeID]
		t.mu.Unlock()
		if stopped {
			return nil, errMeshDBStopped
		}
		if p == nil && !asked && t.connect != nil {
			asked = true
			t.connect(nodeID)
		}
		if p != nil {
			p.want()
			if ch := p.current(); ch != nil {
				s, err := ch.mux.OpenStream()
				if err == nil {
					deadline, ok := ctx.Deadline()
					if !ok {
						deadline = time.Now().Add(t.timing.header)
					}
					if err := writeMeshDBHeader(s, key, deadline); err != nil {
						s.Close()
						return nil, err
					}
					return &meshDBConn{Stream: s, local: t.localID, remote: nodeID, key: key}, nil
				}
			}
		}
		select {
		case <-wait:
		case <-ctx.Done():
			return nil, fmt.Errorf("meshdb transport: dial %s: %w", truncID(nodeID), ctx.Err())
		case <-t.ctx.Done():
			return nil, errMeshDBStopped
		}
	}
}

// Listen implements mesh.Mesh.
func (t *MeshDBTransport) Listen(key string) (mesh.Listener, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.stopped {
		return nil, errMeshDBStopped
	}
	if _, ok := t.listeners[key]; ok {
		return nil, fmt.Errorf("%w: %s", errMeshDBListening, key)
	}
	ln := &meshDBListener{t: t, key: key, ch: make(chan mesh.Conn), closed: make(chan struct{})}
	t.listeners[key] = ln
	return ln, nil
}

// bindSession attaches session as nodeID's current session, replacing any
// other; the same session again is a no-op. It refuses a session whose
// authenticated NodeID is not nodeID.
func (t *MeshDBTransport) bindSession(nodeID string, session aether.Session) {
	if session == nil || nodeID == t.localID {
		return
	}
	if remote := aether.NormalizeNodeID(string(session.RemoteNodeID())); remote != aether.NormalizeNodeID(nodeID) {
		log.Printf("[MESHDB] refused to bind a session to %s: it authenticated %s", truncID(nodeID), truncID(remote))
		return
	}
	t.mu.Lock()
	if t.stopped {
		t.mu.Unlock()
		return
	}
	old := t.peers[nodeID]
	if old != nil && old.session == session {
		t.mu.Unlock()
		return
	}
	p := newMeshDBPeer(t, nodeID, session)
	t.peers[nodeID] = p
	t.wg.Add(1)
	t.mu.Unlock()
	if old != nil {
		old.stop()
	}
	go p.run()
	t.notify()
}

// unbindSession detaches nodeID's peer after a session leave. A leave carries
// no session, so a peer whose session is still open is kept: the leave then
// belongs to an older session that a newer one already replaced.
func (t *MeshDBTransport) unbindSession(nodeID string) {
	t.mu.Lock()
	p := t.peers[nodeID]
	if p == nil || !p.session.IsClosed() {
		t.mu.Unlock()
		return
	}
	delete(t.peers, nodeID)
	t.mu.Unlock()
	p.stop()
	t.notify()
	t.rebind(nodeID, p.session)
}

// dropPeer removes p if it is still nodeID's peer, once its session closed.
func (t *MeshDBTransport) dropPeer(p *meshDBPeer) {
	t.mu.Lock()
	if cur := t.peers[p.id]; cur != p {
		t.mu.Unlock()
		return
	}
	delete(t.peers, p.id)
	t.mu.Unlock()
	t.notify()
	t.rebind(p.id, p.session)
}

// rebind binds nodeID to the session the connection manager still holds for
// it once the bound session, gone, has closed. A join fires only from
// AcceptMeshConnection for a session the manager installs; when an upgraded
// session dies and the manager falls back to the session it replaced
// (unregisterMeshSessionWithoutHook's proving revert) or to another live
// multipath path, no join fires for that session, and the peer would stay
// unbound, absent from Members and undialable, until its next join.
func (t *MeshDBTransport) rebind(nodeID string, gone aether.Session) {
	if t.currentSession == nil {
		return
	}
	s, ok := t.currentSession(nodeID)
	if !ok || s == nil || s == gone || s.IsClosed() {
		return
	}
	t.bindSession(nodeID, s)
}

// onSession is the ConnectionManager session hook.
func (t *MeshDBTransport) onSession(nodeID string, session aether.Session, joined bool) {
	if joined {
		t.bindSession(nodeID, session)
		return
	}
	t.unbindSession(nodeID)
}

func (t *MeshDBTransport) hasLiveSession(nodeID string) bool {
	t.mu.Lock()
	p := t.peers[nodeID]
	t.mu.Unlock()
	return p != nil && !p.session.IsClosed()
}

func (t *MeshDBTransport) isStopped() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.stopped
}

// Stop closes every listener and channel and waits for every goroutine the
// transport started. Idempotent.
func (t *MeshDBTransport) Stop() {
	t.mu.Lock()
	if t.stopped {
		t.mu.Unlock()
		t.wg.Wait()
		return
	}
	t.stopped = true
	removeHook := t.removeHook
	peers := make([]*meshDBPeer, 0, len(t.peers))
	for _, p := range t.peers {
		peers = append(peers, p)
	}
	t.peers = map[string]*meshDBPeer{}
	lns := make([]*meshDBListener, 0, len(t.listeners))
	for _, ln := range t.listeners {
		lns = append(lns, ln)
	}
	t.mu.Unlock()
	if removeHook != nil {
		removeHook()
	}
	t.cancel()
	for _, ln := range lns {
		ln.Close()
	}
	for _, p := range peers {
		p.stop()
	}
	t.wg.Wait()
	t.dir.close()
}

// Refused reports how many inbound streams were refused.
func (t *MeshDBTransport) Refused() uint64 { return t.refused.Load() }

// meshDBListener yields the authorized inbound streams for one cluster key.
type meshDBListener struct {
	t      *MeshDBTransport
	key    string
	ch     chan mesh.Conn
	closed chan struct{}
	once   sync.Once
}

// Accept implements mesh.Listener.
func (l *meshDBListener) Accept() (mesh.Conn, error) {
	select {
	case c := <-l.ch:
		return c, nil
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

// Close implements mesh.Listener; it frees the key for a new listener.
func (l *meshDBListener) Close() error {
	l.once.Do(func() {
		close(l.closed)
		l.t.mu.Lock()
		if l.t.listeners[l.key] == l {
			delete(l.t.listeners, l.key)
		}
		l.t.mu.Unlock()
	})
	return nil
}

// Addr implements mesh.Listener.
func (l *meshDBListener) Addr() net.Addr { return meshDBAddr{l.t.localID, l.key} }
