/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/adapter"
	"github.com/bbmumford/loom/core/directory/gossip"
	"github.com/bbmumford/loom/core/streams"
)

// dialTestRuntime is a runtime with the connection manager that
// DialAndAcceptMesh's failure branch writes to: the connection table whose
// connecting state it clears and the quality tracker whose dial ladder it
// debits.
func dialTestRuntime(t *testing.T) (*Runtime, *ConnectionManager) {
	t.Helper()
	rt := wireTestRuntime(t)
	m := trackedManager()
	m.peers = make(map[string]*peerConn)
	m.budget = DefaultConnectionBudget()
	m.rt = rt
	m.selfID = string(rt.identity.NodeID)
	rt.connMgr = m
	return rt, m
}

// loopbackTCP returns the two ends of one loopback TCP connection.
func loopbackTCP(t *testing.T) (near, far net.Conn) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	near, err = net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	far, err = ln.Accept()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = near.Close(); _ = far.Close() })
	return near, far
}

// runPeerSession runs a byte-stream session of another build's offer on conn,
// as that build's mesh runtime does on its end of a connection, and reports
// its first gossip-stream accept: the mismatch it refused on, or an error that
// it accepted the stream.
func runPeerSession(ctx context.Context, conn net.Conn, peerID, localID aether.NodeID, offer aether.WireVersions) <-chan error {
	accepted := make(chan error, 1)
	go func() {
		opts := aether.DefaultSessionOptions()
		opts.WireVersions = offer
		peer := adapter.NewTCPSession(conn, peerID, localID, aether.ProtoTCP, opts)
		st, err := peer.AcceptStreamByID(ctx, streams.StreamGossip)
		if st != nil {
			err = errors.New("the peer accepted the gossip stream")
		}
		_ = peer.Close()
		accepted <- err
	}()
	return accepted
}

// sessionCounters reads both mesh session gauges.
func sessionCounters() (initiator, responder uint64) {
	return atomic.LoadUint64(&meshInitiatorSessions), atomic.LoadUint64(&meshResponderSessions)
}

// A byte-stream dial whose peer speaks another version of a loom entry is
// refused by SetupMeshSession once the preface exchange ends, before the
// session is counted, so DialAndAcceptMesh's failure branch runs: the dialled
// conn is closed by this side, the dial ladder the dial site credited is
// debited and the path suppressed, the connecting state connectPeer set is
// cleared, nothing is recorded for the peer, and neither session gauge moves.
// A second refusal re-arms the suppression. A dial that borrowed the
// connecting state is debited the same way and leaves the state to its owner.
// The peer's own refusal names the entry, so the pair refused at the preface
// and no stream existed on either end.
func TestARefusedByteStreamDialTakesTheSetupFailureBranch(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	rt, m := dialTestRuntime(t)
	peerRT := wireTestRuntime(t)
	peerID := string(peerRT.identity.NodeID)
	m.peers[peerID] = &peerConn{nodeID: peerID, state: PeerConnecting, reconnectDelay: baseCooldown}
	key := dialKeyFor(peerID, ProtoWebSocket)

	refusedDial := func(t *testing.T, owns ownsConnectingState) {
		t.Helper()
		// The dial site credits the transport dial the moment it returns a
		// conn, which clears the ladder; the mesh refusal must debit it.
		m.recordDialSuccess(peerID, ProtoWebSocket)
		if m.dialIsSuppressed(peerID, ProtoWebSocket) {
			t.Fatal("premise wrong: the path is suppressed before the refused dial, so a debit cannot be observed")
		}
		near, far := loopbackTCP(t)
		accepted := runPeerSession(ctx, far, peerRT.identity.NodeID, rt.identity.NodeID, withLoomEntryAt(t, gossip.WireVersionCodec, 2))
		initBefore, respBefore := sessionCounters()
		returned := make(chan struct{})
		go func() {
			rt.DialAndAcceptMesh(ctx, near, peerID, "", ProtoWebSocket, "", "", "", owns)
			close(returned)
		}()
		select {
		case <-returned:
		case <-ctx.Done():
			t.Fatal("DialAndAcceptMesh did not return for a refused peer")
		}
		var me *aether.WireVersionMismatchError
		if err := <-accepted; !errors.As(err, &me) || me.Name != gossip.WireVersionCodec {
			t.Fatalf("the peer's accept err = %v, want a mismatch naming %s", err, gossip.WireVersionCodec)
		}
		if _, err := near.Read(make([]byte, 1)); !errors.Is(err, net.ErrClosed) {
			t.Errorf("read on the dialled conn after the refusal: err = %v, want net.ErrClosed (this side closed it)", err)
		}
		if n := m.qualityTracker.DialFailureCount(key); n != 1 {
			t.Errorf("dial failures on %s after the refusal = %d, want 1", key, n)
		}
		if !m.dialIsSuppressed(peerID, ProtoWebSocket) {
			t.Error("the refused path is not suppressed: the refusal left the ladder as the transport success cleared it")
		}
		if initAfter, respAfter := sessionCounters(); initAfter != initBefore || respAfter != respBefore {
			t.Errorf("mesh session gauges moved by (%d, %d) for a refused session, want (0, 0)",
				initAfter-initBefore, respAfter-respBefore)
		}
		m.dispatchMu.RLock()
		_, registered := m.meshSessions[peerID]
		m.dispatchMu.RUnlock()
		if registered {
			t.Error("the refused session is in the dispatch table")
		}
		m.mu.Lock()
		connCount, transports := m.peers[peerID].connCount, len(m.peers[peerID].transports)
		m.mu.Unlock()
		if connCount != 0 || transports != 0 {
			t.Errorf("peer connCount=%d transports=%d after a refused session, want 0 and 0", connCount, transports)
		}
		if got := m.budget.CurrentTotal(); got != 0 {
			t.Errorf("budget holds %d slots after a refused session, want 0", got)
		}
	}

	refusedDial(t, dialOwnsConnectingState)
	if got := stateOf(m, peerID); got != PeerDisconnected {
		t.Errorf("state = %v after the owning dial was refused, want PeerDisconnected so scanAndConnect re-dials it", got)
	}

	// The next owning dial is refused the same way and the suppression is
	// re-armed, so the peer is never re-dialled at the base delay.
	m.mu.Lock()
	m.peers[peerID].state = PeerConnecting
	m.mu.Unlock()
	refusedDial(t, dialOwnsConnectingState)
	if got := stateOf(m, peerID); got != PeerDisconnected {
		t.Errorf("state = %v after the second refused dial, want PeerDisconnected", got)
	}

	// A borrowing dial (the walker, hole punch or multipath dialer) fails
	// while connectPeer holds the peer in PeerConnecting for its own attempt.
	m.mu.Lock()
	m.peers[peerID].state = PeerConnecting
	m.mu.Unlock()
	refusedDial(t, dialBorrowsConnectingState)
	if got := stateOf(m, peerID); got != PeerConnecting {
		t.Errorf("state = %v after a borrowing dial was refused, want PeerConnecting left to the connectPeer that set it", got)
	}
}

// An incoming byte-stream connection from a peer of another build is refused
// by SetupMeshSession before AcceptIncomingMeshSession registers anything:
// the conn is closed, the responder gauge does not move and the peer is not in
// the connection or dispatch table.
func TestARefusedIncomingByteStreamSessionIsClosedAndNotCounted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	rt, m := dialTestRuntime(t)
	peerRT := wireTestRuntime(t)
	peerID := string(peerRT.identity.NodeID)
	near, far := loopbackTCP(t)
	accepted := runPeerSession(ctx, near, peerRT.identity.NodeID, rt.identity.NodeID, withLoomEntryAt(t, streams.WireVersionRegistry, 2))
	initBefore, respBefore := sessionCounters()

	rt.AcceptIncomingMeshSession(ctx, far, peerID, ProtoWebSocket, "", "", "", "")

	var me *aether.WireVersionMismatchError
	if err := <-accepted; !errors.As(err, &me) || me.Name != streams.WireVersionRegistry {
		t.Fatalf("the peer's accept err = %v, want a mismatch naming %s", err, streams.WireVersionRegistry)
	}
	if _, err := far.Read(make([]byte, 1)); !errors.Is(err, net.ErrClosed) {
		t.Errorf("read on the accepted conn after the refusal: err = %v, want net.ErrClosed", err)
	}
	if initAfter, respAfter := sessionCounters(); initAfter != initBefore || respAfter != respBefore {
		t.Errorf("mesh session gauges moved by (%d, %d) for a refused incoming session, want (0, 0)",
			initAfter-initBefore, respAfter-respBefore)
	}
	m.mu.Lock()
	_, known := m.peers[peerID]
	m.mu.Unlock()
	if known {
		t.Error("the refused peer is in the connection table")
	}
	m.dispatchMu.RLock()
	_, registered := m.meshSessions[peerID]
	m.dispatchMu.RUnlock()
	if registered {
		t.Error("the refused session is in the dispatch table")
	}
}

// delayedWriteConn is a net.Conn whose first Write waits delay before the
// bytes reach the conn, so a session's own preface write is still in flight
// when its read side has read the peer's preface and refused it. Reads and
// Close pass straight through.
type delayedWriteConn struct {
	net.Conn
	delay time.Duration
	once  sync.Once
}

func (c *delayedWriteConn) Write(b []byte) (int, error) {
	c.once.Do(func() { time.Sleep(c.delay) })
	return c.Conn.Write(b)
}

// A refused byte-stream session's own preface reaches the peer before
// SetupMeshSession closes the conn, so the peer refuses on the same entry
// and never reads a bare end of stream. The near side reads the peer's
// preface and refuses while its own preface write is still held by the
// delayed conn; SetupMeshSession returns only after that write went out and
// the session closed itself, the peer's accept names the entry, the conn is
// closed by this side, and neither gauge moves.
func TestARefusedSessionsOwnPrefaceReachesThePeerBeforeTheConnCloses(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	rt := wireTestRuntime(t)
	peerRT := wireTestRuntime(t)
	const delay = 200 * time.Millisecond
	near, far := loopbackTCP(t)
	accepted := runPeerSession(ctx, far, peerRT.identity.NodeID, rt.identity.NodeID, withLoomEntryAt(t, gossip.WireVersionReconcile, 2))
	initBefore, respBefore := sessionCounters()
	started := time.Now()

	sess, err := rt.SetupMeshSession(ctx, &delayedWriteConn{Conn: near, delay: delay}, string(peerRT.identity.NodeID), ProtoWebSocket, true)
	if sess != nil || !errors.Is(err, gossip.ErrWireProfile) {
		t.Fatalf("SetupMeshSession = (%v, %v), want (nil, gossip.ErrWireProfile)", sess, err)
	}
	if waited := time.Since(started); waited < delay {
		t.Errorf("SetupMeshSession returned after %v, before the held preface write (%v) could reach the peer", waited, delay)
	}
	var me *aether.WireVersionMismatchError
	if err := <-accepted; !errors.As(err, &me) || me.Name != gossip.WireVersionReconcile {
		t.Fatalf("the peer's accept err = %v, want a mismatch naming %s", err, gossip.WireVersionReconcile)
	}
	// The session's own close ends the conn; the peer, refusing too, may have
	// sent its end of stream first, so the first read can see EOF before this
	// side's close lands. Within a bound, a read sees this side's close.
	deadline := time.Now().Add(2 * time.Second)
	for {
		_, err := near.Read(make([]byte, 1))
		if errors.Is(err, net.ErrClosed) {
			break
		}
		if time.Now().After(deadline) {
			t.Errorf("read on the dialled conn after the refusal: err = %v, want net.ErrClosed within 2s (the session closes it)", err)
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if initAfter, respAfter := sessionCounters(); initAfter != initBefore || respAfter != respBefore {
		t.Errorf("mesh session gauges moved by (%d, %d) for a refused session, want (0, 0)",
			initAfter-initBefore, respAfter-respBefore)
	}
}

// SetupMeshSession returns a byte-stream session only once its preface
// exchange chose a set: a peer of this build over TCP yields a session whose
// chosen set is loom's, counted once on the gauge the caller's role selects.
// A peer that never sends a preface holds the setup until the caller's
// context ends, which closes the session and returns the context's error
// with nothing counted.
func TestSetupMeshSessionWaitsForTheChosenSet(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	rt := wireTestRuntime(t)
	peerRT := wireTestRuntime(t)
	peerID := string(peerRT.identity.NodeID)

	t.Run("a peer of this build is admitted and counted", func(t *testing.T) {
		near, far := loopbackTCP(t)
		go func() {
			opts := aether.DefaultSessionOptions()
			opts.WireVersions = meshWireVersions(nil)
			peer := adapter.NewTCPSession(far, peerRT.identity.NodeID, rt.identity.NodeID, aether.ProtoTCP, opts)
			_, _ = peer.AcceptStreamByID(ctx, streams.StreamGossip)
			_ = peer.Close()
		}()
		initBefore, respBefore := sessionCounters()
		sess, err := rt.SetupMeshSession(ctx, near, peerID, ProtoWebSocket, false)
		if err != nil {
			t.Fatalf("SetupMeshSession: %v", err)
		}
		defer sess.Close()
		set, ok := aether.SessionWireVersions(sess)
		if !ok || !set.Equal(loomNegotiatedSet()) {
			t.Fatalf("chosen set %v, want %v", set, loomNegotiatedSet())
		}
		if initAfter, respAfter := sessionCounters(); initAfter != initBefore || respAfter != respBefore+1 {
			t.Errorf("mesh session gauges moved by (%d, %d) for an admitted responder session, want (0, 1)",
				initAfter-initBefore, respAfter-respBefore)
		}
	})

	t.Run("a silent peer holds the setup until the context ends", func(t *testing.T) {
		near, _ := loopbackTCP(t)
		shortCtx, shortCancel := context.WithTimeout(ctx, 300*time.Millisecond)
		defer shortCancel()
		initBefore, respBefore := sessionCounters()
		started := time.Now()
		sess, err := rt.SetupMeshSession(shortCtx, near, peerID, ProtoWebSocket, true)
		if sess != nil || !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("SetupMeshSession = (%v, %v), want (nil, context.DeadlineExceeded)", sess, err)
		}
		if waited := time.Since(started); waited > 5*time.Second {
			t.Errorf("SetupMeshSession returned after %v, want soon after the 300ms context ended", waited)
		}
		if _, err := near.Read(make([]byte, 1)); !errors.Is(err, net.ErrClosed) {
			t.Errorf("read on the conn after the context ended: err = %v, want net.ErrClosed (the session closed it)", err)
		}
		if initAfter, respAfter := sessionCounters(); initAfter != initBefore || respAfter != respBefore {
			t.Errorf("mesh session gauges moved by (%d, %d) for a setup the context ended, want (0, 0)",
				initAfter-initBefore, respAfter-respBefore)
		}
	})
}
