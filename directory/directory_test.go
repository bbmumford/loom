/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package directory

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"testing"

	swarmpb "github.com/bbmumford/swarm/proto/pb"
	"google.golang.org/protobuf/proto"

	"github.com/bbmumford/loom/journal"
	"github.com/bbmumford/loom/ports"
)

func testIdentity(t *testing.T) (ports.NodeID, ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return ports.NodeID(hex.EncodeToString(pub)), pub, priv
}

func peerRecord(t *testing.T, id ports.NodeID, pub ed25519.PublicKey, hlc uint64, roles []string, service string) ports.Record {
	t.Helper()
	pr := &swarmpb.PeerRecord{
		NodeId: pub,
		Capabilities: &swarmpb.Capabilities{
			Roles: roles,
			Tags:  []string{"service=" + service, "region=syd", "tenant=hstles"},
		},
		RpcHandlers: []string{"hstles." + service + ".ping"},
		Addresses: []*swarmpb.Address{
			{Transport: swarmpb.Address_WEBSOCKET, Host: "203.0.113.7", Port: 443},
			{Transport: swarmpb.Address_NOISE_UDP, Host: "fdaa::7", Port: 41641},
		},
	}
	body, err := proto.Marshal(pr)
	if err != nil {
		t.Fatal(err)
	}
	return ports.Record{
		Topic:        FleetPeerTopic,
		NodeID:       id,
		HLC:          ports.HLC(hlc),
		Body:         body,
		AuthorPubKey: pub,
		Signature:    []byte("sig-" + string(id[:8])),
	}
}

func newTestDirectory(t *testing.T, policy ports.TrustPolicy) *SwarmDirectory {
	t.Helper()
	j, err := journal.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { j.Close() })
	d, err := NewSwarmDirectory(context.Background(), j, policy)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestSwarmDirectoryProjectionAndViews(t *testing.T) {
	ctx := context.Background()
	idA, pubA, _ := testIdentity(t)
	d := newTestDirectory(t, nil)

	if err := d.Ingest(ctx, peerRecord(t, idA, pubA, 100<<16, []string{"auth", "anchor"}, "auth-svc")); err != nil {
		t.Fatal(err)
	}

	members, _ := d.Members(ctx)
	if len(members) != 1 || members[0].ServiceName != "auth-svc" || members[0].Tenant != "hstles" {
		t.Fatalf("members = %+v", members)
	}
	nodes, _ := d.NodesByRole(ctx, "auth")
	if len(nodes) != 1 || nodes[0] != idA {
		t.Fatalf("auth nodes = %v", nodes)
	}
	reach, _ := d.Reach(ctx, idA)
	if len(reach) != 2 || reach[0].Protocol != "noise-udp" || reach[1].Protocol != "ws" {
		t.Fatalf("reach (priority order broken) = %+v", reach)
	}
	adverts, _ := d.HandlersByName(ctx, "hstles.auth-svc.ping")
	if len(adverts) != 1 || adverts[0].NodeID != idA {
		t.Fatalf("handler adverts = %+v", adverts)
	}

	// Tombstone removes everything for the node.
	tomb := ports.Record{Topic: FleetPeerTopic, NodeID: idA, HLC: 200 << 16, Tombstone: true, AuthorPubKey: pubA, Signature: []byte("sig-t")}
	if err := d.Ingest(ctx, tomb); err != nil {
		t.Fatal(err)
	}
	if members, _ := d.Members(ctx); len(members) != 0 {
		t.Fatalf("tombstoned member still present: %+v", members)
	}
	if adverts, _ := d.HandlersByName(ctx, "hstles.auth-svc.ping"); len(adverts) != 0 {
		t.Fatalf("tombstoned handlers still present: %+v", adverts)
	}
}

func TestSwarmDirectoryRestartReproducesState(t *testing.T) {
	ctx := context.Background()
	idA, pubA, _ := testIdentity(t)
	dir := t.TempDir()

	j, err := journal.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	d1, err := NewSwarmDirectory(ctx, j, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := d1.Ingest(ctx, peerRecord(t, idA, pubA, 100<<16, []string{"auth"}, "svc")); err != nil {
		t.Fatal(err)
	}
	fp1, _ := d1.Fingerprint(ctx)
	j.Close()

	// Restart: journal replay must reproduce the identical projection.
	j2, err := journal.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer j2.Close()
	d2, err := NewSwarmDirectory(ctx, j2, nil)
	if err != nil {
		t.Fatal(err)
	}
	fp2, _ := d2.Fingerprint(ctx)
	if fp1 != fp2 {
		t.Fatal("restart did not reproduce the projection fingerprint")
	}
	members, _ := d2.Members(ctx)
	if len(members) != 1 || members[0].NodeID != idA {
		t.Fatalf("restart members = %+v", members)
	}
}

func TestIngestPolicyGateFailClosed(t *testing.T) {
	ctx := context.Background()
	idA, pubA, _ := testIdentity(t)
	_, pubEvil, _ := testIdentity(t)

	policy := NewPolicy(PolicyConfig{
		OpenPublishPrefixes: []string{"fleet.peer"},
	})
	d := newTestDirectory(t, policy)

	// Well-bound record on an open-publish topic → accepted.
	if err := d.Ingest(ctx, peerRecord(t, idA, pubA, 100<<16, []string{"auth"}, "svc")); err != nil {
		t.Fatalf("bound record refused: %v", err)
	}
	// NodeID that doesn't bind to the author key → refused.
	spoofed := peerRecord(t, idA, pubA, 101<<16, []string{"auth"}, "svc")
	spoofed.AuthorPubKey = pubEvil
	if err := d.Ingest(ctx, spoofed); err == nil {
		t.Fatal("unbound record must be refused")
	}
	// Topic with no publish rule → refused (fail closed).
	unruly := peerRecord(t, idA, pubA, 102<<16, nil, "svc")
	unruly.Topic = "rogue.topic"
	if err := d.Ingest(ctx, unruly); err == nil {
		t.Fatal("unruled topic must be refused")
	}
	if d.Rejected() != 2 {
		t.Fatalf("rejected counter = %d, want 2", d.Rejected())
	}
}

func TestTrustPolicyChecks(t *testing.T) {
	ctx := context.Background()
	idA, pubA, _ := testIdentity(t)
	idObs, pubObs, _ := testIdentity(t)
	idEvil, pubEvil, _ := testIdentity(t)

	p := NewPolicy(PolicyConfig{
		Tenants:          []string{"hstles"},
		Observers:        []ports.NodeID{idObs},
		RoleEntitlements: map[string][]ports.NodeID{"auth": {idA}},
	})

	// Role entitlement: configured node passes, another does not.
	if err := p.AuthorizeRoleEntitlement(ctx, ports.Principal{NodeID: idA, PubKey: pubA}, "auth"); err != nil {
		t.Fatalf("entitled node refused: %v", err)
	}
	if err := p.AuthorizeSecretRecipient(ctx, ports.Principal{NodeID: idEvil, PubKey: pubEvil}, "auth"); err == nil {
		t.Fatal("unentitled recipient must be refused — advertisement is not authorization")
	}

	// Observer authorization.
	if err := p.AuthorizeObserver(ctx, ports.Principal{NodeID: idObs, PubKey: pubObs}, idA); err != nil {
		t.Fatalf("configured observer refused: %v", err)
	}
	if err := p.AuthorizeObserver(ctx, ports.Principal{NodeID: idEvil, PubKey: pubEvil}, idA); err == nil {
		t.Fatal("unconfigured observer must be refused")
	}
	if err := p.AuthorizeObserver(ctx, ports.Principal{NodeID: idObs, PubKey: pubObs}, idObs); err == nil {
		t.Fatal("self-attestation must be refused")
	}

	// Revocation flips KeyState and every check.
	if p.KeyState(pubA) != ports.KeyStatusActive {
		t.Fatal("want active")
	}
	p.RevokeKey(pubA)
	if p.KeyState(pubA) != ports.KeyStatusRevoked {
		t.Fatal("want revoked")
	}
	if err := p.AuthorizeRoleEntitlement(ctx, ports.Principal{NodeID: idA, PubKey: pubA}, "auth"); err == nil {
		t.Fatal("revoked key must fail every check")
	}
}

func TestShadowParity(t *testing.T) {
	ctx := context.Background()
	idA, pubA, _ := testIdentity(t)
	idB, pubB, _ := testIdentity(t)

	auth := newTestDirectory(t, nil)
	shadow := newTestDirectory(t, nil)

	recA := peerRecord(t, idA, pubA, 100<<16, []string{"auth"}, "svc-a")
	recB := peerRecord(t, idB, pubB, 100<<16, []string{"billing"}, "svc-b")
	for _, r := range []ports.Record{recA, recB} {
		if err := auth.Ingest(ctx, r); err != nil {
			t.Fatal(err)
		}
		if err := shadow.Ingest(ctx, r); err != nil {
			t.Fatal(err)
		}
	}

	rep, err := CompareDirectories(ctx, auth, shadow, []string{"auth", "billing"})
	if err != nil {
		t.Fatal(err)
	}
	if !rep.InParity() {
		t.Fatalf("expected parity, got mismatches: %v", rep.Mismatches)
	}
	if ok, _ := CompareFingerprints(ctx, auth, shadow); !ok {
		t.Fatal("fingerprints must match in parity")
	}

	// Diverge the shadow: drop B. The comparator must SURFACE it, never
	// silently prefer a side.
	tomb := ports.Record{Topic: FleetPeerTopic, NodeID: idB, HLC: 200 << 16, Tombstone: true, AuthorPubKey: pubB, Signature: []byte("s")}
	if err := shadow.Ingest(ctx, tomb); err != nil {
		t.Fatal(err)
	}
	rep2, err := CompareDirectories(ctx, auth, shadow, []string{"auth", "billing"})
	if err != nil {
		t.Fatal(err)
	}
	if rep2.InParity() {
		t.Fatal("divergence must be reported")
	}
	if ok, _ := CompareFingerprints(ctx, auth, shadow); ok {
		t.Fatal("fingerprints must diverge")
	}
}
