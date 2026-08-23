/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	lad "github.com/bbmumford/ledger"
	ladcache "github.com/bbmumford/ledger/cache"

	"github.com/bbmumford/loom/directory"
)

// §0.5.3 step 4, the "Address" half: converge's reach read asks only
// whether a node has ANY dial candidate before forcing a connection to it.
// This pins that behaviour so the read can be routed onto the port.
//
// 🛑 HOW THIS TESTS A FUNCTION WHOSE SUCCESS PATH DIALS.
//
// converge ends in ForceConnect, and ForceConnect dials — so a naive fixture
// either opens sockets or proves nothing. The way through is a peer state
// that is simultaneously:
//
//   - NOT connected, so step 2 leaves its service in the "reconnect" set and
//     step 4 actually reaches the reach read (that check is
//     `state == PeerConnected || hasActiveTransport()`); and
//   - NOT dialable, because ForceConnect only calls connectPeer when
//     `state == PeerDiscovered`, and it promotes PeerDisconnected into that.
//
// PeerConnecting satisfies both. And ForceConnect resets reconnectDelay to
// baseCooldown BEFORE that dial check — so the reset is an observable proof
// that the reach read found addresses, with no socket opened.

func convergeFixture(t *testing.T, nodeID, service string, withReach bool) (*ConnectionManager, *peerConn) {
	t.Helper()
	c := ladcache.NewDirectoryCache()

	mb, _ := json.Marshal(lad.MemberRecord{
		NodeID: nodeID, CreatedAt: time.Now(),
		// converge reads Attrs["serviceName"] — the producer's spelling.
		Attrs: map[string]string{"serviceName": service},
	})
	if err := c.Apply(lad.Record{
		Topic: lad.TopicMember, NodeID: nodeID, Body: mb, Timestamp: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	if withReach {
		rb, _ := json.Marshal(lad.ReachRecord{
			NodeID: nodeID, UpdatedAt: time.Now(),
			Addresses: []lad.ReachAddress{{Host: "203.0.113.9", Port: 443, Proto: "wss", Scope: "public"}},
		})
		if err := c.Apply(lad.Record{
			Topic: lad.TopicReach, NodeID: nodeID, Body: rb, Timestamp: time.Now(),
		}); err != nil {
			t.Fatal(err)
		}
	}

	ld, err := directory.NewLADDirectory(c)
	if err != nil {
		t.Fatalf("premise wrong: the LiveDirectory adapter would not build: %v", err)
	}

	// PeerConnecting: reaches step 4 (not "connected") but cannot dial
	// (ForceConnect requires PeerDiscovered). reconnectDelay starts escalated
	// so a reset to baseCooldown is unambiguous.
	p := &peerConn{nodeID: nodeID, state: PeerConnecting, reconnectDelay: maxCooldown}
	m := &ConnectionManager{
		peers:  map[string]*peerConn{nodeID: p},
		budget: DefaultConnectionBudget(),
		selfID: testNodeIDA,
		rt:     &Runtime{cache: c, liveDir: ld, liveDirRaw: ld},
	}
	return m, p
}

func TestConvergeForcesAConnectionWhenTheNodeHasDialCandidates(t *testing.T) {
	if maxCooldown == baseCooldown {
		t.Fatal("premise wrong: the two cooldowns are equal, so the reset is " +
			"not observable")
	}
	m, p := convergeFixture(t, testNodeIDB, "svc-alpha", true)

	m.converge(context.Background())

	if p.reconnectDelay != baseCooldown {
		t.Fatalf("reconnectDelay = %v, want %v — converge did not force a "+
			"connection to a node that HAS dial candidates, so a service with "+
			"a reachable provider is never reconnected",
			p.reconnectDelay, baseCooldown)
	}
	if p.state != PeerConnecting {
		t.Fatalf("peer state = %v, want it untouched at %v — the fixture was "+
			"supposed to be undialable and something advanced it",
			p.state, PeerConnecting)
	}
}

// The negative half, and it is what makes the positive meaningful: a node the
// directory knows but that advertises NO reach record must not be forced.
// Without this, the assertion above would pass for an implementation that
// ignored reach entirely and always called ForceConnect.
func TestConvergeSkipsANodeWithNoDialCandidates(t *testing.T) {
	m, p := convergeFixture(t, testNodeIDB, "svc-alpha", false)

	m.converge(context.Background())

	if p.reconnectDelay != maxCooldown {
		t.Fatalf("reconnectDelay = %v, want it left at %v — converge forced a "+
			"connection to a node with NO dial candidates, which burns the "+
			"backoff on a peer that cannot be reached",
			p.reconnectDelay, maxCooldown)
	}
}

// A peer already connected marks its service satisfied at step 2, so step 4
// never considers it. Pinned because it is the gate that decides whether the
// reach read runs at all — an implementation that lost it would re-force
// every healthy peer on every sweep.
func TestConvergeSkipsServicesAlreadyConnected(t *testing.T) {
	m, p := convergeFixture(t, testNodeIDB, "svc-alpha", true)
	p.state = PeerConnected

	m.converge(context.Background())

	if p.reconnectDelay != maxCooldown {
		t.Fatalf("reconnectDelay = %v, want it left at %v — a service with a "+
			"CONNECTED peer was treated as needing reconnection",
			p.reconnectDelay, maxCooldown)
	}
}
