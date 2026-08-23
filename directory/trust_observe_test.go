/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package directory

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"testing"

	aether "github.com/ORBTR/aether"
	swarmpb "github.com/bbmumford/swarm/proto/pb"
	"google.golang.org/protobuf/proto"

	"github.com/bbmumford/loom/journal"
	"github.com/bbmumford/loom/ports"
)

// canonicalPeer builds a fleet.peer record whose NodeID is the aether NodeID for
// its key, so VerifyNodeKey passes and the tests exercise the AuthorizePublish
// (topic) leg — the one an unseeded PolicyConfig fails closed. tenant=hstles.
func canonicalPeer(t *testing.T, hlc uint64) (ports.Record, ports.NodeID) {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	nid, err := aether.NewNodeID(pub)
	if err != nil {
		t.Fatal(err)
	}
	id := ports.NodeID(string(nid))
	pr := &swarmpb.PeerRecord{
		NodeId: pub,
		Capabilities: &swarmpb.Capabilities{
			Roles: []string{"auth"},
			Tags:  []string{"service=auth-svc", "region=syd", "tenant=hstles"},
		},
		RpcHandlers: []string{"hstles.auth.ping"},
		Addresses:   []*swarmpb.Address{{Transport: swarmpb.Address_WEBSOCKET, Host: "203.0.113.7", Port: 443}},
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
		Signature:    []byte("sig"),
	}, id
}

// canonicalIdentity returns a key whose NodeID is the aether NodeID for it —
// the scheme VerifyNodeKey binds against. Tests that pass a principal or record
// through the trust gate must use this, not testIdentity (which returns hex, a
// NodeID that never binds and is only useful for the negative case).
func canonicalIdentity(t *testing.T) (ports.NodeID, ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	nid, err := aether.NewNodeID(pub)
	if err != nil {
		t.Fatal(err)
	}
	return ports.NodeID(string(nid)), pub, priv
}

func trustDir(t *testing.T, policy ports.TrustPolicy, observe bool) *SwarmDirectory {
	t.Helper()
	j, err := journal.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { j.Close() })
	d, err := newSwarmDirectory(context.Background(), j, policy, observe)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// TestZeroConfigPolicyRejectsSelfOwnedPublish is the #R-1528 hazard made concrete:
// an unseeded Policy denies a node publishing its OWN PeerRecord, so enforcing it
// across the fleet rejects every node.
func TestZeroConfigPolicyRejectsSelfOwnedPublish(t *testing.T) {
	ctx := context.Background()
	d := trustDir(t, NewPolicy(PolicyConfig{}), false) // enforce, zero config
	rec, _ := canonicalPeer(t, 100<<16)

	if err := d.Ingest(ctx, rec); err == nil {
		t.Fatal("zero-config enforcing gate must refuse a self-owned publish")
	}
	if d.Rejected() != 1 || d.Checked() != 1 {
		t.Fatalf("checked=%d rejected=%d, want 1/1", d.Checked(), d.Rejected())
	}
	if m, _ := d.Members(ctx, ports.Tenant("hstles")); len(m) != 0 {
		t.Fatalf("refused record must not project, got %d members", len(m))
	}
}

// TestBaselineSeedAllowsSelfOwnedPublish proves the seed resolves the hazard: with
// SelfOwnedPublishPrefixes open, the same self-owned publish passes and projects.
func TestBaselineSeedAllowsSelfOwnedPublish(t *testing.T) {
	ctx := context.Background()
	d := trustDir(t, NewPolicy(BaselinePolicyConfig()), false) // enforce, baseline seed
	rec, id := canonicalPeer(t, 100<<16)

	if err := d.Ingest(ctx, rec); err != nil {
		t.Fatalf("baseline-seeded gate must allow a self-owned publish: %v", err)
	}
	if d.Rejected() != 0 || d.Checked() != 1 {
		t.Fatalf("checked=%d rejected=%d, want 1/0", d.Checked(), d.Rejected())
	}
	m, _ := d.Members(ctx, ports.Tenant("hstles"))
	if len(m) != 1 || m[0].NodeID != id {
		t.Fatalf("allowed record must project as a member, got %+v", m)
	}
}

// TestObserveModeCountsButKeeps proves the shadow safety net: even with a policy
// that would reject (zero config), observe mode still journals + projects the
// record while counting the would-reject — so an operator can measure gate impact
// against live traffic before cutting to enforce.
func TestObserveModeCountsButKeeps(t *testing.T) {
	ctx := context.Background()
	d := trustDir(t, NewPolicy(PolicyConfig{}), true) // OBSERVE, zero config
	rec, id := canonicalPeer(t, 100<<16)

	if err := d.Ingest(ctx, rec); err != nil {
		t.Fatalf("observe mode must not refuse: %v", err)
	}
	if d.Rejected() != 1 || d.Checked() != 1 {
		t.Fatalf("checked=%d rejected=%d, want 1/1 (counted but kept)", d.Checked(), d.Rejected())
	}
	m, _ := d.Members(ctx, ports.Tenant("hstles"))
	if len(m) != 1 || m[0].NodeID != id {
		t.Fatalf("observe mode must still project the record, got %+v", m)
	}
}

// TestObserverAttestationBindsToObserverKey proves the observer path binds the
// author (observer) key under the aether scheme VerifyNodeKey checks — so a
// legitimately-signed attestation passes the enforcing gate. Before the fix the
// principal NodeID was hex-encoded, which VerifyNodeKey rejects, so an enforcing
// cut would have silently dropped every death-tombstone / liveness observation.
func TestObserverAttestationBindsToObserverKey(t *testing.T) {
	ctx := context.Background()
	// Observer key + its aether NodeID; the record's own NodeID is a DIFFERENT
	// node (the subject the attestation is about).
	obsPub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	obsNID, err := aether.NewNodeID(obsPub)
	if err != nil {
		t.Fatal(err)
	}
	_, subjectID := canonicalPeer(t, 1) // borrow a distinct canonical NodeID as the subject

	// Baseline seed opens fleet.peer, and the observer is authorized to attest —
	// so the ONLY thing that can reject is a mis-bound observer NodeID.
	cfg := BaselinePolicyConfig()
	cfg.Observers = []ports.NodeID{ports.NodeID(string(obsNID))}
	d := trustDir(t, NewPolicy(cfg), false) // enforce

	rec := ports.Record{
		Topic:        FleetPeerTopic,
		NodeID:       subjectID, // SUBJECT, not the author
		HLC:          ports.HLC(200 << 16),
		Body:         []byte("attestation-body"),
		AuthorPubKey: obsPub,      // the OBSERVER signed it
		Observer:     []byte{0x1}, // non-nil attestation segment
		Signature:    []byte("sig"),
	}
	if err := d.Ingest(ctx, rec); err != nil {
		t.Fatalf("a legitimately-signed observer attestation must pass the gate: %v", err)
	}
	if d.Rejected() != 0 || d.Checked() != 1 {
		t.Fatalf("checked=%d rejected=%d, want 1/0", d.Checked(), d.Rejected())
	}
}

// TestBaselineSeedLeavesRestrictedTopicsClosed confirms the seed opens ONLY the
// self-owned families: a role.secrets.<role> publish is still denied without an
// explicit per-role entitlement, so the baseline is not an accidental open door.
func TestBaselineSeedLeavesRestrictedTopicsClosed(t *testing.T) {
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	nid, err := aether.NewNodeID(pub)
	if err != nil {
		t.Fatal(err)
	}
	p := NewPolicy(BaselinePolicyConfig())
	pr := ports.Principal{NodeID: ports.NodeID(string(nid)), PubKey: pub}

	if err := p.AuthorizePublish(context.Background(), pr, ports.Topic("role.secrets.auth")); err == nil {
		t.Fatal("baseline seed must not authorize a restricted role.secrets publish without entitlement")
	}
	// The same node's self-owned publish is allowed — sanity that the seed works.
	if err := p.AuthorizePublish(context.Background(), pr, FleetPeerTopic); err != nil {
		t.Fatalf("baseline seed must authorize the self-owned fleet.peer publish: %v", err)
	}
}
