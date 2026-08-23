/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 */

package node

import (
	"context"
	"crypto/ed25519"
	"testing"
	"time"

	aether "github.com/ORBTR/aether"
	"github.com/bbmumford/loom/directory"
	"github.com/bbmumford/swarm"
)

func canonicalSwarmPeer(t *testing.T) swarm.Record {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	nid, err := aether.NewNodeID(pub)
	if err != nil {
		t.Fatal(err)
	}
	return swarm.Record{Topic: "fleet.peer", NodeID: swarm.NodeID(string(nid)), PubKey: pub}
}

func TestSwarmToPortsRecordMapping(t *testing.T) {
	r := swarm.Record{
		Topic: "fleet.peer", NodeID: "n1", HLC: 5, Key: "kk",
		Body: []byte("b"), PubKey: []byte("k"), Sig: []byte("s"), Tombstone: true,
	}
	p := swarmToPortsRecord(r)
	if p.Topic != "fleet.peer" || string(p.NodeID) != "n1" || p.HLC != 5 || p.Key != "kk" || !p.Tombstone {
		t.Fatalf("scalar fields mis-mapped: %+v", p)
	}
	if string(p.AuthorPubKey) != "k" || string(p.Signature) != "s" || string(p.Body) != "b" {
		t.Fatalf("byte fields mis-mapped: %+v", p)
	}
	if p.Observer != nil {
		t.Fatalf("non-attestation must have nil Observer, got %v", p.Observer)
	}

	// An observer attestation is a death declaration (Tombstone) authored by the
	// observer; the converter carries a non-empty Observer segment so the gate
	// binds the observer key, not the subject NodeID.
	obs := swarm.Record{Topic: "fleet.peer", NodeID: "subject", PubKey: []byte("obskey"), ObserverNodeID: "obs-node", Tombstone: true}
	po := swarmToPortsRecord(obs)
	if string(po.Observer) != "obs-node" {
		t.Fatalf("attestation must carry the observer NodeID in Observer, got %q", po.Observer)
	}
}

func TestTrustShadowObservesAndCounts(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sh, err := newTrustShadow(ctx, "observe", t.TempDir(), directory.BaselinePolicyConfig())
	if err != nil {
		t.Fatal(err)
	}
	if sh == nil {
		t.Fatal("observe mode must yield a shadow")
	}

	good := canonicalSwarmPeer(t) // canonical NodeID on a seeded topic → passes the gate
	badPub, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	bad := swarm.Record{Topic: "fleet.peer", NodeID: good.NodeID, PubKey: badPub} // claims good's NodeID, different key

	sh.Observe(good)
	sh.Observe(bad)

	if !waitFor(func() bool { c, _, _ := sh.Stats(); return c == 2 }, 2*time.Second) {
		t.Fatal("shadow worker did not drain both records within timeout")
	}
	c, rej, dropped := sh.Stats()
	if c != 2 || rej != 1 || dropped != 0 {
		t.Fatalf("stats checked=%d rejected=%d dropped=%d, want 2/1/0", c, rej, dropped)
	}
}

// TestSwarmIntegrationShadowAccessorsWhenOff pins the default (SwarmTrustMode
// off) runtime contract: the stats/parity accessors any debug surface calls must
// be safe when no shadow was constructed.
func TestSwarmIntegrationShadowAccessorsWhenOff(t *testing.T) {
	si := &SwarmIntegration{} // constructed with no shadow, as in every non-observe mode
	if c, r, d := si.ShadowStats(); c != 0 || r != 0 || d != 0 {
		t.Fatalf("no-shadow ShadowStats must be zero, got %d/%d/%d", c, r, d)
	}
	rep, err := si.ShadowParity(context.Background(), nil, "hstles", nil)
	if rep != nil || err != nil {
		t.Fatalf("no-shadow ShadowParity must be (nil,nil), got %v %v", rep, err)
	}
	var nilSI *SwarmIntegration
	if c, r, d := nilSI.ShadowStats(); c != 0 || r != 0 || d != 0 {
		t.Fatal("nil SwarmIntegration ShadowStats must be zero")
	}
}

func TestTrustShadowOffModeIsNilAndSafe(t *testing.T) {
	sh, err := newTrustShadow(context.Background(), "", t.TempDir(), directory.BaselinePolicyConfig())
	if err != nil || sh != nil {
		t.Fatalf("non-observe mode must yield (nil, nil), got shadow=%v err=%v", sh, err)
	}
	// The OnAccepted hook calls these on a possibly-nil shadow; none may panic.
	sh.Observe(swarm.Record{})
	if sh.Directory() != nil {
		t.Fatal("nil shadow Directory must be nil")
	}
	if c, r, d := sh.Stats(); c != 0 || r != 0 || d != 0 {
		t.Fatalf("nil shadow Stats must be zero, got %d/%d/%d", c, r, d)
	}
}
