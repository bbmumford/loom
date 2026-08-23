/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 */

package node

import (
	"crypto/ed25519"
	"testing"

	"github.com/ORBTR/aether"
	"github.com/bbmumford/swarm"
)

// TestSwarmTrustGate covers the three inbound-trust modes wired from
// Config.SwarmTrustMode. A "good" record's NodeID is the aether derivation of
// its own key; a "bad" record claims that NodeID but carries a different key —
// the exact "any keypair claims any slot" case the gate exists to catch.
func TestSwarmTrustGate(t *testing.T) {
	pubA, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	pubB, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	idA, err := aether.NewNodeID(pubA)
	if err != nil {
		t.Fatal(err)
	}
	// good publishes its own PeerRecord on the self-owned fleet.peer topic — the
	// baseline seed opens it, and the key binds. bad claims idA with a different
	// key. unruled binds correctly but publishes a topic no rule allows — the leg
	// the gate gained when it moved from bind-only to full AuthorizePublish.
	good := swarm.Record{Topic: "fleet.peer", NodeID: swarm.NodeID(string(idA)), PubKey: pubA}
	bad := swarm.Record{Topic: "fleet.peer", NodeID: swarm.NodeID(string(idA)), PubKey: pubB}
	unruled := swarm.Record{Topic: "rogue.topic", NodeID: swarm.NodeID(string(idA)), PubKey: pubA}

	// off: no gate wired.
	for _, m := range []string{"", "off", "bogus"} {
		if check, gate := newSwarmTrustGate(m); check != nil || gate != nil {
			t.Fatalf("mode %q must yield no gate", m)
		}
	}

	// observe: accepts all three, counts the key mismatch AND the unruled topic.
	obs, og := newSwarmTrustGate("observe")
	if obs == nil || og == nil {
		t.Fatal("observe must yield a gate")
	}
	if err := obs(good); err != nil {
		t.Fatalf("observe rejected a well-formed self-owned publish: %v", err)
	}
	if err := obs(bad); err != nil {
		t.Fatalf("observe must ACCEPT a mismatch (count only), got: %v", err)
	}
	if err := obs(unruled); err != nil {
		t.Fatalf("observe must ACCEPT an unruled-topic publish (count only), got: %v", err)
	}
	if c, wr := og.checked.Load(), og.wouldReject.Load(); c != 3 || wr != 2 {
		t.Fatalf("observe counters = %d checked / %d would-reject, want 3 / 2", c, wr)
	}

	// enforce: accepts the seeded self-owned publish, rejects both the key
	// mismatch and the unruled topic.
	enf, eg := newSwarmTrustGate("enforce")
	if err := enf(good); err != nil {
		t.Fatalf("enforce rejected a well-formed self-owned publish: %v", err)
	}
	if err := enf(bad); err == nil {
		t.Fatal("enforce must REJECT a NodeID↔key mismatch")
	}
	if err := enf(unruled); err == nil {
		t.Fatal("enforce must REJECT a publish on a topic with no rule")
	}
	if wr := eg.wouldReject.Load(); wr != 2 {
		t.Fatalf("enforce would-reject = %d, want 2", wr)
	}
}
