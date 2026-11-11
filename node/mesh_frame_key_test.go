/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/adapter"
)

// connectWireTestRuntimes dials b's VL1 listener from a and runs the mesh
// session lifecycle on it, as the connection manager does after a dial, then
// waits until each end holds a bidi for the other and returns the two ends'
// sessions.
func connectWireTestRuntimes(t *testing.T, a, b *Runtime, portB int) (sa, sb aether.Session) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	idA, idB := string(a.identity.NodeID), string(b.identity.NodeID)
	conn, err := a.dialNoiseUDP(ctx, aether.Target{NodeID: b.identity.NodeID, Address: fmt.Sprintf("127.0.0.1:%d", portB)})
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	bs, ok := installableMeshConn(conn)
	if !ok {
		t.Fatalf("dial returned %T", conn)
	}
	go a.DialAndAcceptMesh(a.ctx, bs.Conn, idB, "", ProtoNoiseUDP, "", "", "", dialBorrowsConnectingState)
	deadline := time.Now().Add(20 * time.Second)
	for {
		_, okA := a.connMgr.GetBidiRPC(idB)
		_, okB := b.connMgr.GetBidiRPC(idA)
		if okA && okB {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the two nodes did not both hold a bidi for the other")
		}
		time.Sleep(20 * time.Millisecond)
	}
	sa, _ = a.connMgr.GetMeshSession(idB)
	sb, _ = b.connMgr.GetMeshSession(idA)
	return sa, sb
}

// Two nodes on loopback VL1 carry a mesh session's frames under the Noise
// transport's encryption, with no per-frame key at either end: neither session
// reports a frame key after the lifecycle has run, and a call crosses the
// session each way.
func TestTwoMeshNodesExchangeTheirFramesWithNoFrameKey(t *testing.T) {
	if testing.Short() {
		t.Skip("boots two runtimes")
	}
	a, _ := startWireTestRuntime(t)
	b, portB := startWireTestRuntime(t)
	sa, sb := connectWireTestRuntimes(t, a, b, portB)
	for _, s := range []aether.Session{sa, sb} {
		if _, ok := s.(*adapter.NoiseSession); !ok {
			t.Fatalf("a mesh session over VL1 is %T, not a Noise session", s)
		}
	}
	idA, idB := string(a.identity.NodeID), string(b.identity.NodeID)
	bidiA, _ := a.connMgr.GetBidiRPC(idB)
	bidiB, _ := b.connMgr.GetBidiRPC(idA)
	callUntilAnswered(t, bidiA, "a to b")
	callUntilAnswered(t, bidiB, "b to a")
	if k := sa.SessionKey(); k != nil {
		t.Errorf("the dialling node's session holds a %d-byte frame key", len(k))
	}
	if k := sb.SessionKey(); k != nil {
		t.Errorf("the accepting node's session holds a %d-byte frame key", len(k))
	}
}
