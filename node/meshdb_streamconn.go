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
	"net"
	"sync"
	"time"

	"github.com/ORBTR/aether"
	"github.com/hashicorp/yamux"
)

// errMeshDBWriteTimeout is what a channel write returns when one Send on the
// aether stream outlives the write timer.
var errMeshDBWriteTimeout = errors.New("meshdb transport: write to the peer timed out")

// meshDBMaxChunk bounds one aether Send. aether fragments a payload into at
// most 255 path-MTU datagrams and refuses a larger one, and yamux hands a
// whole data frame (up to its stream window) to one Write.
const meshDBMaxChunk = 32 * 1024

// meshDBStreamConn is the byte stream one peer channel's yamux session runs
// over: the aether stream StreamMeshDB of one session.
//
// Consumes an aether.Stream. Consumed by yamux, which reads it from its one
// receive goroutine through a bufio.Reader and writes it from its one send
// goroutine; rmu keeps a second reader from splitting a message anyway.
//
// It is an io.ReadWriteCloser, not a net.Conn: yamux sets no deadline on the
// connection it runs over, and the Raft streams tursoraft sees are yamux
// streams, whose deadlines yamux enforces. What yamux needs from below is that
// a write cannot block forever, and aether's own net.Conn view
// (adapter.StreamConn) ignores SetWriteDeadline. So every Send carries its own
// timer: a Send waiting on flow-control credit from a peer that stopped reading
// would otherwise hold yamux's send goroutine, and with it every Raft stream
// on the channel. A Send that times out resets the aether stream, which fails
// yamux's send loop and closes the channel.
//
// Close resets the stream instead of closing it. A reset removes the stream
// from both sessions' stream tables at once, so the peer's end fails and the
// ID is free for the next channel; a FIN leaves it half-closed until the peer
// closes too, and a stalled peer never does.
type meshDBStreamConn struct {
	stream       aether.Stream
	writeTimeout time.Duration

	ctx    context.Context
	cancel context.CancelFunc
	once   sync.Once

	rmu  sync.Mutex
	rbuf []byte

	// stalled, once closed, stops Read from handing bytes to yamux until the
	// conn closes: a test seam that makes this end behave as a stopped
	// process whose socket stays up. Nil outside tests.
	stallMu sync.Mutex
	stalled chan struct{}
}

func newMeshDBStreamConn(stream aether.Stream, writeTimeout time.Duration) *meshDBStreamConn {
	ctx, cancel := context.WithCancel(context.Background())
	return &meshDBStreamConn{stream: stream, writeTimeout: writeTimeout, ctx: ctx, cancel: cancel}
}

// Read implements io.Reader over the stream's message boundaries.
func (c *meshDBStreamConn) Read(p []byte) (int, error) {
	c.rmu.Lock()
	defer c.rmu.Unlock()
	for len(c.rbuf) == 0 {
		msg, err := c.stream.Receive(c.ctx)
		if err != nil {
			if c.ctx.Err() != nil {
				return 0, net.ErrClosed
			}
			return 0, err
		}
		c.rbuf = msg
	}
	if s := c.stallSignal(); s != nil {
		select {
		case <-s:
			<-c.ctx.Done()
			return 0, net.ErrClosed
		default:
		}
	}
	n := copy(p, c.rbuf)
	c.rbuf = c.rbuf[n:]
	return n, nil
}

// Write implements io.Writer. p is copied before each Send: aether keeps the
// sent slice for retransmission, and yamux reuses its buffer as soon as Write
// returns.
func (c *meshDBStreamConn) Write(p []byte) (int, error) {
	written := 0
	for written < len(p) {
		end := written + meshDBMaxChunk
		if end > len(p) {
			end = len(p)
		}
		chunk := append([]byte(nil), p[written:end]...)
		ctx, cancel := context.WithTimeout(c.ctx, c.writeTimeout)
		err := c.stream.Send(ctx, chunk)
		timedOut := ctx.Err() == context.DeadlineExceeded && c.ctx.Err() == nil
		cancel()
		if err != nil {
			if timedOut {
				c.closeWith(aether.ResetTimeout)
				return written, errMeshDBWriteTimeout
			}
			if c.ctx.Err() != nil {
				return written, net.ErrClosed
			}
			return written, err
		}
		written = end
	}
	return written, nil
}

// Close resets the stream and wakes any Read or Write in progress.
func (c *meshDBStreamConn) Close() error {
	c.closeWith(aether.ResetCancel)
	return nil
}

func (c *meshDBStreamConn) closeWith(reason aether.ResetReason) {
	c.once.Do(func() {
		c.cancel()
		_ = c.stream.Reset(reason)
	})
}

func (c *meshDBStreamConn) stallSignal() chan struct{} {
	c.stallMu.Lock()
	defer c.stallMu.Unlock()
	return c.stalled
}

// stall makes this end stop delivering what it receives, as a stopped
// process would. Test seam.
func (c *meshDBStreamConn) stall() {
	c.stallMu.Lock()
	defer c.stallMu.Unlock()
	if c.stalled == nil {
		c.stalled = make(chan struct{})
		close(c.stalled)
	}
}

// meshDBAddr names one end of a Raft stream: a NodeID and the cluster key.
type meshDBAddr struct{ node, key string }

func (a meshDBAddr) Network() string { return "meshdb" }
func (a meshDBAddr) String() string  { return a.node + "/" + a.key }

// meshDBConn is one Raft stream to a peer, as tursoraft's mesh.Conn.
//
// Consumes a yamux stream of the peer's channel; yamux enforces its read and
// write deadlines, which tursoraft sets on every handshake and Raft RPC.
// RemoteID is the NodeID the aether session authenticated (Noise), never a
// value the peer sent: tursoraft's member proof binds both NodeIDs.
type meshDBConn struct {
	*yamux.Stream
	local, remote, key string
}

func (c *meshDBConn) RemoteID() string     { return c.remote }
func (c *meshDBConn) LocalAddr() net.Addr  { return meshDBAddr{c.local, c.key} }
func (c *meshDBConn) RemoteAddr() net.Addr { return meshDBAddr{c.remote, c.key} }

// meshDBMaxKeyLen bounds the cluster-key header an inbound stream may send.
const meshDBMaxKeyLen = 512

// writeMeshDBHeader names the cluster key a new stream is for: a big-endian
// uint16 length, then the key.
func writeMeshDBHeader(s *yamux.Stream, key string, deadline time.Time) error {
	if len(key) == 0 || len(key) > meshDBMaxKeyLen {
		return fmt.Errorf("meshdb transport: cluster key length %d outside 1..%d", len(key), meshDBMaxKeyLen)
	}
	buf := make([]byte, 2+len(key))
	binary.BigEndian.PutUint16(buf, uint16(len(key)))
	copy(buf[2:], key)
	if err := s.SetWriteDeadline(deadline); err != nil {
		return err
	}
	if _, err := s.Write(buf); err != nil {
		return err
	}
	return s.SetWriteDeadline(time.Time{})
}

// readMeshDBHeader reads the cluster key an inbound stream opens with, under
// deadline and the length bound, so a peer that opens a stream and sends
// nothing, or a huge length, holds one goroutine for the deadline at most.
func readMeshDBHeader(s *yamux.Stream, deadline time.Time) (string, error) {
	if err := s.SetReadDeadline(deadline); err != nil {
		return "", err
	}
	var hdr [2]byte
	if _, err := io.ReadFull(s, hdr[:]); err != nil {
		return "", fmt.Errorf("meshdb transport: read header: %w", err)
	}
	n := int(binary.BigEndian.Uint16(hdr[:]))
	if n == 0 || n > meshDBMaxKeyLen {
		return "", fmt.Errorf("meshdb transport: cluster key length %d outside 1..%d", n, meshDBMaxKeyLen)
	}
	key := make([]byte, n)
	if _, err := io.ReadFull(s, key); err != nil {
		return "", fmt.Errorf("meshdb transport: read cluster key: %w", err)
	}
	if err := s.SetReadDeadline(time.Time{}); err != nil {
		return "", err
	}
	return string(key), nil
}
