/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"testing"

	"github.com/bbmumford/swarm"
)

// A grade upgrade (WS → noise-UDP) replaces a peer's transport entry via
// attachPeer while the OLD session's readLoop is still winding down (attachPeer
// cancels its ctx). That stale readLoop's deferred teardown must NOT delete the
// freshly-installed replacement peer.
//
// The bug this guards: the teardown used to call UnregisterPeer(peerID), which
// keys only on peerID and unconditionally deleted whatever now sat there — the
// promoted NOISE peer — and fired onLeave, so the δ-CRDT engine dropped the peer
// and every subsequent Send SKIPped (known=false). The promoted noise session
// then carried zero data until the stall detector reverted it. The fix guards
// the teardown on instance identity (unregisterPeerInstance).
func TestUnregisterPeerInstance_StaleTeardownKeepsReplacement(t *testing.T) {
	tr := NewMeshSwarmTransport(swarm.NodeID("self"))
	defer tr.Stop()
	id := swarm.NodeID("peerX")

	_, c1 := context.WithCancel(context.Background())
	oldWS := &meshSwarmPeer{id: id, cancel: c1}
	_, c2 := context.WithCancel(context.Background())
	newNoise := &meshSwarmPeer{id: id, cancel: c2}

	var leaveFired []swarm.NodeID
	tr.mu.Lock()
	tr.onLeave = func(n swarm.NodeID) { leaveFired = append(leaveFired, n) }
	tr.peers[id] = oldWS    // WS session attached
	tr.peers[id] = newNoise // upgrade: attachPeer replaced it with the noise peer
	tr.mu.Unlock()

	// The stale WS readLoop's deferred teardown runs AFTER the swap.
	tr.unregisterPeerInstance(oldWS)

	tr.mu.RLock()
	cur, ok := tr.peers[id]
	tr.mu.RUnlock()
	if !ok || cur != newNoise {
		t.Fatalf("stale teardown removed the promoted peer: peers[%q]=%p, want newNoise=%p", id, cur, newNoise)
	}
	if len(leaveFired) != 0 {
		t.Errorf("onLeave must not fire for a stale teardown that finds a replaced entry; fired=%v", leaveFired)
	}

	// A genuine teardown of the CURRENT peer removes it and fires onLeave once —
	// the identity guard must not suppress a real departure.
	tr.unregisterPeerInstance(newNoise)
	tr.mu.RLock()
	_, ok = tr.peers[id]
	tr.mu.RUnlock()
	if ok {
		t.Error("teardown of the current peer must remove it from the transport")
	}
	if len(leaveFired) != 1 || leaveFired[0] != id {
		t.Errorf("onLeave must fire exactly once for the current-peer teardown; fired=%v", leaveFired)
	}
}
