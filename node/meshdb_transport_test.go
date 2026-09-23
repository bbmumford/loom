/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/adapter"
	meshdbcfg "github.com/bbmumford/tursoraft/manager"
	"github.com/bbmumford/tursoraft/mesh"
)

const (
	meshDBTestLow     = "vl1_meshdbtest_aaaa"
	meshDBTestHigh    = "vl1_meshdbtest_bbbb"
	meshDBTestService = "node.test"
)

var meshDBTestKey = meshdbcfg.MeshKey("control", "ctl")

// fastMeshDBTiming shortens every bound so the unit tests run in seconds.
func fastMeshDBTiming() meshDBTiming {
	return meshDBTiming{
		keepAlive:    200 * time.Millisecond,
		writeTimeout: 300 * time.Millisecond,
		header:       time.Second,
		authWait:     500 * time.Millisecond,
		reopenMin:    50 * time.Millisecond,
		reopenMax:    200 * time.Millisecond,
	}
}

// meshDBPair is two transports joined by one aether session over a pipe,
// whose directories deliver each other's publishes the way swarm would.
type meshDBPair struct {
	low, high         *MeshDBTransport
	lowSess, highSess aether.Session
}

func crossWire(from, to *MeshDBTransport) {
	from.dir.publishFn = func(key string, body []byte) error {
		var meta map[string]string
		if err := json.Unmarshal(body, &meta); err != nil {
			return err
		}
		to.dir.ingest(key, from.localID, meta)
		return nil
	}
	from.dir.withdrawFn = func(key string) error {
		to.dir.ingest(key, from.localID, nil)
		return nil
	}
}

func newMeshDBPair(t *testing.T, timing meshDBTiming, seats map[string]map[string]bool) *meshDBPair {
	t.Helper()
	low := newMeshDBTransport(meshDBTestLow, meshDBTestService, seats, timing)
	high := newMeshDBTransport(meshDBTestHigh, meshDBTestService, seats, timing)
	crossWire(low, high)
	crossWire(high, low)
	low.dir.setService(meshDBTestHigh, meshDBTestService)
	high.dir.setService(meshDBTestLow, meshDBTestService)

	lowConn, highConn := net.Pipe()
	opts := aether.DefaultSessionOptions()
	p := &meshDBPair{
		low:      low,
		high:     high,
		lowSess:  adapter.NewTCPSession(lowConn, meshDBTestLow, meshDBTestHigh, aether.ProtoTCP, opts),
		highSess: adapter.NewTCPSession(highConn, meshDBTestHigh, meshDBTestLow, aether.ProtoTCP, opts),
	}
	t.Cleanup(func() {
		low.Stop()
		high.Stop()
		p.lowSess.Close()
		p.highSess.Close()
	})
	low.bindSession(meshDBTestHigh, p.lowSess)
	high.bindSession(meshDBTestLow, p.highSess)
	return p
}

func defaultMeshDBSeats() map[string]map[string]bool {
	return map[string]map[string]bool{meshDBTestKey: {meshDBTestService: true}}
}

func waitMeshDB(t *testing.T, within time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", within, what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func channelUp(tr *MeshDBTransport, peer string) bool {
	tr.mu.Lock()
	p := tr.peers[peer]
	tr.mu.Unlock()
	return p != nil && p.current() != nil
}

func acceptAsync(ln mesh.Listener) <-chan mesh.Conn {
	out := make(chan mesh.Conn, 1)
	go func() {
		c, err := ln.Accept()
		if err == nil {
			out <- c
		}
		close(out)
	}()
	return out
}

// exchange writes from one end and reads it at the other.
func exchange(t *testing.T, from, to net.Conn, msg string) {
	t.Helper()
	if _, err := from.Write([]byte(msg)); err != nil {
		t.Fatalf("write: %v", err)
	}
	to.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, len(msg))
	if _, err := io.ReadFull(to, buf); err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(buf) != msg {
		t.Fatalf("read %q, want %q", buf, msg)
	}
}

// Both members publish under the key: the lower end opens the channel, and a
// stream dialled from either end reaches the other's listener with the
// session's NodeIDs on both sides.
func TestMeshDBTransportCarriesStreamsBothWays(t *testing.T) {
	p := newMeshDBPair(t, fastMeshDBTiming(), defaultMeshDBSeats())
	lnLow, err := p.low.Listen(meshDBTestKey)
	if err != nil {
		t.Fatal(err)
	}
	lnHigh, err := p.high.Listen(meshDBTestKey)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.low.Listen(meshDBTestKey); !errors.Is(err, errMeshDBListening) {
		t.Fatalf("a second listener on one key: err = %v, want errMeshDBListening", err)
	}
	if err := p.low.Publish(meshDBTestKey, map[string]string{"role": "voter"}); err != nil {
		t.Fatal(err)
	}
	if err := p.high.Publish(meshDBTestKey, map[string]string{"role": "voter"}); err != nil {
		t.Fatal(err)
	}
	waitMeshDB(t, 5*time.Second, "the channel", func() bool {
		return channelUp(p.low, meshDBTestHigh) && channelUp(p.high, meshDBTestLow)
	})

	for _, dir := range []struct {
		name     string
		dialer   *MeshDBTransport
		target   string
		listener mesh.Listener
	}{
		{"low dials high", p.low, meshDBTestHigh, lnHigh},
		{"high dials low", p.high, meshDBTestLow, lnLow},
	} {
		t.Run(dir.name, func(t *testing.T) {
			accepted := acceptAsync(dir.listener)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			c, err := dir.dialer.Dial(ctx, dir.target, meshDBTestKey)
			if err != nil {
				t.Fatalf("Dial: %v", err)
			}
			defer c.Close()
			var srv mesh.Conn
			select {
			case srv = <-accepted:
			case <-time.After(5 * time.Second):
				t.Fatal("the listener accepted nothing")
			}
			if srv == nil {
				t.Fatal("the listener closed")
			}
			defer srv.Close()
			if c.RemoteID() != dir.target || srv.RemoteID() != dir.dialer.LocalID() {
				t.Fatalf("RemoteID dialer=%s acceptor=%s, want %s and %s", c.RemoteID(), srv.RemoteID(), dir.target, dir.dialer.LocalID())
			}
			exchange(t, c, srv, "request")
			exchange(t, srv, c, "response")
		})
	}

	members := p.low.Members(meshDBTestKey)
	if len(members) != 2 || members[0].NodeID != meshDBTestLow || members[1].Service != meshDBTestService {
		t.Fatalf("Members = %+v, want both nodes with their service", members)
	}
}

// A stream from a peer that does not publish under the key, or whose service
// has no seat in the key's raft block, is closed without reaching the
// listener; the same stream from a seated, publishing peer is delivered.
func TestMeshDBTransportClosesUnauthorizedStreams(t *testing.T) {
	for _, tc := range []struct {
		name      string
		publish   bool
		service   string
		wantServe bool
	}{
		{"authorized", true, meshDBTestService, true},
		{"unpublished", false, meshDBTestService, false},
		{"service without a seat", true, "api.test", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := newMeshDBPair(t, fastMeshDBTiming(), defaultMeshDBSeats())
			p.high.dir.setService(meshDBTestLow, tc.service)
			ln, err := p.high.Listen(meshDBTestKey)
			if err != nil {
				t.Fatal(err)
			}
			if err := p.high.Publish(meshDBTestKey, map[string]string{"role": "voter"}); err != nil {
				t.Fatal(err)
			}
			if tc.publish {
				if err := p.low.Publish(meshDBTestKey, map[string]string{"role": "voter"}); err != nil {
					t.Fatal(err)
				}
			}
			accepted := acceptAsync(ln)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			c, err := p.low.Dial(ctx, meshDBTestHigh, meshDBTestKey)
			if err != nil {
				t.Fatalf("Dial: %v", err)
			}
			defer c.Close()

			if !tc.wantServe {
				c.SetReadDeadline(time.Now().Add(3 * time.Second))
				if _, readErr := c.Read(make([]byte, 1)); !errors.Is(readErr, io.EOF) {
					t.Fatalf("read on a refused stream: %v, want io.EOF", readErr)
				}
				if p.high.Refused() == 0 {
					t.Fatal("the refusal was not counted")
				}
				select {
				case srv := <-accepted:
					if srv != nil {
						t.Fatal("a refused stream reached the listener")
					}
				default:
				}
				return
			}
			var srv mesh.Conn
			select {
			case srv = <-accepted:
			case <-time.After(3 * time.Second):
			}
			if srv == nil {
				t.Fatal("the authorized stream never reached the listener")
			}
			defer srv.Close()
			exchange(t, srv, c, "ok")
			if p.high.Refused() != 0 {
				t.Fatalf("refused %d streams from an authorized peer", p.high.Refused())
			}
		})
	}
}

// A peer that stops reading while its session stays up, as a stopped process
// does, has its channel closed within the keepalive interval plus the write
// timeout.
func TestMeshDBTransportDetectsAStalledPeer(t *testing.T) {
	timing := fastMeshDBTiming()
	p := newMeshDBPair(t, timing, defaultMeshDBSeats())
	if err := p.high.Publish(meshDBTestKey, map[string]string{"role": "voter"}); err != nil {
		t.Fatal(err)
	}
	waitMeshDB(t, 5*time.Second, "the channel", func() bool {
		return channelUp(p.low, meshDBTestHigh) && channelUp(p.high, meshDBTestLow)
	})
	p.high.mu.Lock()
	stalled := p.high.peers[meshDBTestLow].ch
	p.high.mu.Unlock()
	p.low.mu.Lock()
	watched := p.low.peers[meshDBTestHigh].ch
	p.low.mu.Unlock()

	start := time.Now()
	stalled.conn.stall()
	bound := timing.keepAlive + timing.writeTimeout
	select {
	case <-watched.mux.CloseChan():
	case <-time.After(3 * bound):
		t.Fatalf("the channel to a stalled peer was still open after %v (bound %v)", 3*bound, bound)
	}
	if took := time.Since(start); took > bound+timing.keepAlive {
		t.Fatalf("detected the stall after %v, bound %v", took, bound)
	}
}

// Members lists a peer only while its session is live and its service known.
func TestMeshDBMembersFollowTheSession(t *testing.T) {
	p := newMeshDBPair(t, fastMeshDBTiming(), defaultMeshDBSeats())
	if err := p.high.Publish(meshDBTestKey, map[string]string{"role": "voter"}); err != nil {
		t.Fatal(err)
	}
	if got := p.low.Members(meshDBTestKey); len(got) != 1 || got[0].NodeID != meshDBTestHigh {
		t.Fatalf("Members = %+v, want the publishing peer only", got)
	}
	p.low.dir.setService(meshDBTestHigh, "")
	if got := p.low.Members(meshDBTestKey); len(got) != 0 {
		t.Fatalf("Members listed a peer whose service is unknown: %+v", got)
	}
	p.low.dir.setService(meshDBTestHigh, meshDBTestService)
	p.lowSess.Close()
	p.low.unbindSession(meshDBTestHigh)
	waitMeshDB(t, 2*time.Second, "the peer to leave", func() bool { return len(p.low.Members(meshDBTestKey)) == 0 })
}

// A leave that arrives after a newer session was bound belongs to the older
// session and leaves the peer bound.
func TestMeshDBLeaveForAReplacedSessionKeepsThePeer(t *testing.T) {
	p := newMeshDBPair(t, fastMeshDBTiming(), defaultMeshDBSeats())
	p.low.onSession(meshDBTestHigh, nil, false)
	if !p.low.hasLiveSession(meshDBTestHigh) {
		t.Fatal("a leave unbound a peer whose session is still open")
	}
}

func boundTo(tr *MeshDBTransport, peer string, s aether.Session) bool {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	p := tr.peers[peer]
	return p != nil && p.session == s
}

// When the bound session closes while the connection manager still
// dispatches to an earlier session of the same peer (a failed upgrade it
// reverted, or a surviving path), no join fires for that session; the peer is
// bound to it anyway and its channel comes back up there.
func TestMeshDBRebindsToTheSessionTheManagerKeeps(t *testing.T) {
	p := newMeshDBPair(t, fastMeshDBTiming(), defaultMeshDBSeats())
	if err := p.high.Publish(meshDBTestKey, map[string]string{"role": "voter"}); err != nil {
		t.Fatal(err)
	}
	p.low.currentSession = func(string) (aether.Session, bool) { return p.lowSess, true }
	p.high.currentSession = func(string) (aether.Session, bool) { return p.highSess, true }
	up := func(lowS, highS aether.Session) func() bool {
		return func() bool {
			return boundTo(p.low, meshDBTestHigh, lowS) && channelUp(p.low, meshDBTestHigh) &&
				boundTo(p.high, meshDBTestLow, highS) && channelUp(p.high, meshDBTestLow)
		}
	}
	waitMeshDB(t, 5*time.Second, "the channel on the first session", up(p.lowSess, p.highSess))

	lowConn, highConn := net.Pipe()
	opts := aether.DefaultSessionOptions()
	lowUp := adapter.NewTCPSession(lowConn, meshDBTestLow, meshDBTestHigh, aether.ProtoTCP, opts)
	highUp := adapter.NewTCPSession(highConn, meshDBTestHigh, meshDBTestLow, aether.ProtoTCP, opts)
	t.Cleanup(func() { lowUp.Close(); highUp.Close() })
	p.low.bindSession(meshDBTestHigh, lowUp)
	p.high.bindSession(meshDBTestLow, highUp)
	waitMeshDB(t, 5*time.Second, "the channel on the upgraded session", up(lowUp, highUp))

	lowUp.Close()
	highUp.Close()
	waitMeshDB(t, 5*time.Second, "the channel back on the first session", up(p.lowSess, p.highSess))
}

// Stop refuses further use and returns only once every goroutine it started
// has ended.
func TestMeshDBTransportStop(t *testing.T) {
	p := newMeshDBPair(t, fastMeshDBTiming(), defaultMeshDBSeats())
	if err := p.high.Publish(meshDBTestKey, map[string]string{"role": "voter"}); err != nil {
		t.Fatal(err)
	}
	waitMeshDB(t, 5*time.Second, "the channel", func() bool { return channelUp(p.low, meshDBTestHigh) })
	done := make(chan struct{})
	go func() {
		p.low.Stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop did not return")
	}
	if _, err := p.low.Dial(context.Background(), meshDBTestHigh, meshDBTestKey); !errors.Is(err, errMeshDBStopped) {
		t.Fatalf("Dial after Stop: %v", err)
	}
	if _, err := p.low.Listen("tursoraft/other/db"); !errors.Is(err, errMeshDBStopped) {
		t.Fatalf("Listen after Stop: %v", err)
	}
	p.low.bindSession(meshDBTestHigh, p.lowSess)
	if p.low.hasLiveSession(meshDBTestHigh) {
		t.Fatal("a session bound after Stop")
	}
}
