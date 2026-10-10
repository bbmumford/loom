/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package directory

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"testing"

	aether "github.com/ORBTR/aether"
	"github.com/bbmumford/loom/ports"
	"github.com/bbmumford/loom/secrets"
)

// These tests cover the TrustPolicy authorization paths AuthorizeRead,
// EntitleRole and checkPartition, and the topics a publish rule covers in
// AuthorizePublish. Every assertion below is about the
// fail-closed direction, because a trust gate that wrongly denies is an outage
// found in minutes and one that wrongly allows is a breach that may never be
// found.

// trustPrincipal builds a principal whose NodeID is the canonical aether NodeID
// of its key — the binding VerifyNodeKey enforces. Taking the ID from the key
// rather than inventing one is deliberate: a hand-written ID would fail the bind
// for a reason unrelated to the property under test.
func trustPrincipal(t *testing.T, partition string) (ports.Principal, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	id, err := aether.NewNodeID(pub)
	if err != nil {
		t.Fatal(err)
	}
	return ports.Principal{
		NodeID:      ports.NodeID(string(id)),
		PubKey:      pub,
		PartitionID: partition,
	}, priv
}

// TestVerifyNodeKeyBindsCanonicalRejectsLegacyHex proves the reconcile: the
// NodeID a key binds to is exactly aether.NewNodeID(pub); the prior hex(pub)
// identifier — a different scheme — no longer binds, so a claimed-victim record
// carrying the legacy id (or any non-canonical value) is refused.
func TestVerifyNodeKeyBindsCanonicalRejectsLegacyHex(t *testing.T) {
	p := NewPolicy(PolicyConfig{Partitions: []string{"hstles"}})
	pub, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	id, err := aether.NewNodeID(pub)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.VerifyNodeKey(ports.NodeID(string(id)), pub); err != nil {
		t.Fatalf("the canonical aether NodeID must bind to its key: %v", err)
	}
	if err := p.VerifyNodeKey(ports.NodeID(hex.EncodeToString(pub)), pub); err == nil {
		t.Fatal("a legacy hex(pub) NodeID must no longer bind — the parallel scheme is gone")
	}
	// A revoked key is refused even when the caller presents the canonical id.
	p.RevokeKey(pub)
	if err := p.VerifyNodeKey(ports.NodeID(string(id)), pub); err == nil {
		t.Fatal("a revoked key must be refused")
	}
}

func TestAuthorizeReadFailsClosedOnEveryRejectionAxis(t *testing.T) {
	ctx := context.Background()
	p := NewPolicy(PolicyConfig{Partitions: []string{"hstles"}})
	good, _ := trustPrincipal(t, "hstles")

	// Premise: the happy path is genuinely allowed, or every denial below is
	// satisfied by a policy that denies everything.
	if err := p.AuthorizeRead(ctx, good, ports.Topic("fleet.peer")); err != nil {
		t.Fatalf("premise wrong: a valid principal was denied: %v", err)
	}

	t.Run("unknown tenant is denied", func(t *testing.T) {
		bad, _ := trustPrincipal(t, "not-a-tenant")
		if err := p.AuthorizeRead(ctx, bad, ports.Topic("fleet.peer")); err == nil {
			t.Fatal("a principal stamped with an unregistered tenant was allowed " +
				"to read — tenant isolation is not enforced on the read path")
		}
	})

	t.Run("NodeID not bound to the presented key is denied", func(t *testing.T) {
		imposter, _ := trustPrincipal(t, "hstles")
		other, _ := trustPrincipal(t, "hstles")
		imposter.NodeID = other.NodeID // keeps its own PubKey
		if err := p.AuthorizeRead(ctx, imposter, ports.Topic("fleet.peer")); err == nil {
			t.Fatal("a principal claiming another node's ID with its own key was " +
				"allowed — 'any keypair can claim any NodeID' is open on reads")
		}
	})

	t.Run("revoked key is denied", func(t *testing.T) {
		revoked, _ := trustPrincipal(t, "hstles")
		if err := p.AuthorizeRead(ctx, revoked, ports.Topic("fleet.peer")); err != nil {
			t.Fatalf("premise wrong: this key must be valid BEFORE revocation: %v", err)
		}
		p.RevokeKey(revoked.PubKey)
		if err := p.AuthorizeRead(ctx, revoked, ports.Topic("fleet.peer")); err == nil {
			t.Fatal("a REVOKED key still authorizes reads — rotation and " +
				"compromise response do not take effect on this path")
		}
	})

	t.Run("malformed key is denied", func(t *testing.T) {
		short := ports.Principal{NodeID: good.NodeID, PubKey: []byte{1, 2, 3}, PartitionID: "hstles"}
		if err := p.AuthorizeRead(ctx, short, ports.Topic("fleet.peer")); err == nil {
			t.Fatal("a key of the wrong size authorized a read")
		}
	})
}

// ⚠ TWO DELIBERATE OPENINGS, PINNED SO THEY STAY DELIBERATE.
//
// Both look like holes and are documented choices; pinning them means a
// future reader cannot "tighten" one without the test stating what it is
// giving up, and cannot let one widen by accident.
func TestAuthorizeReadsDocumentedCoarsenessIsIntentional(t *testing.T) {
	ctx := context.Background()
	p := NewPolicy(PolicyConfig{Partitions: []string{"hstles"}})

	t.Run("read is TOPIC-BLIND by design", func(t *testing.T) {
		pr, _ := trustPrincipal(t, "hstles")
		// AuthorizeRead discards the topic (`_ = topic`). Its doc states why:
		// "secret payload access control is cryptographic (the DEK wrap), so
		// read authorization here is deliberately coarser than publish."
		for _, topic := range []string{"fleet.peer", "role.secrets.auth", "anything.at.all"} {
			if err := p.AuthorizeRead(ctx, pr, ports.Topic(topic)); err != nil {
				t.Fatalf("read of %q denied: %v — if topic-scoped reads are now "+
					"intended, the DEK-wrap premise this coarseness rests on must "+
					"be re-checked, not just this test updated", topic, err)
			}
		}
	})

	t.Run("an UNSTAMPED partition bypasses the partition check", func(t *testing.T) {
		// checkPartition returns nil for PartitionID == "" (an unstamped fleet
		// principal; partition scoping applies once stamped). So the gate is
		// only as strong as whatever stamps the partition upstream.
		unstamped, _ := trustPrincipal(t, "")
		if err := p.AuthorizeRead(ctx, unstamped, ports.Topic("fleet.peer")); err != nil {
			t.Fatalf("a tenantless principal was denied: %v — that is a stricter "+
				"policy than documented; confirm intent before adopting it", err)
		}
	})
}

// EntitleRole grants at runtime what PolicyConfig.RoleEntitlements grants at
// construction. Its map is lazily created, so the first grant for a role is
// the branch most likely to be wrong.
func TestEntitleRoleGrantsAndStaysScoped(t *testing.T) {
	ctx := context.Background()
	p := NewPolicy(PolicyConfig{Partitions: []string{"hstles"}})
	alice, _ := trustPrincipal(t, "hstles")
	bob, _ := trustPrincipal(t, "hstles")

	// Premise: not entitled before the grant, or the assertion after it is
	// satisfied by a policy that entitles everyone.
	if err := p.AuthorizeRoleEntitlement(ctx, alice, "auth"); err == nil {
		t.Fatal("premise wrong: the role was already entitled before EntitleRole")
	}

	p.EntitleRole("auth", alice.NodeID) // first grant for this role: lazy map init

	if err := p.AuthorizeRoleEntitlement(ctx, alice, "auth"); err != nil {
		t.Fatalf("the entitled node was denied its role: %v", err)
	}
	if err := p.AuthorizeRoleEntitlement(ctx, bob, "auth"); err == nil {
		t.Fatal("an UNENTITLED node passed the role check — EntitleRole grants " +
			"to every node rather than the one named")
	}
	if err := p.AuthorizeRoleEntitlement(ctx, alice, "billing"); err == nil {
		t.Fatal("entitlement to \"auth\" also granted \"billing\" — the grant is " +
			"not scoped to its role")
	}

	// A second grant on the now-initialised map must not clobber the first.
	p.EntitleRole("auth", bob.NodeID)
	if err := p.AuthorizeRoleEntitlement(ctx, alice, "auth"); err != nil {
		t.Fatalf("the second grant removed the first: %v", err)
	}
}

// checkPartition is reached from several authorize paths; its three branches are
// asserted directly through AuthorizeRead so the behaviour is pinned at the
// caller rather than at an unexported helper.
func TestCheckPartitionBranches(t *testing.T) {
	ctx := context.Background()
	p := NewPolicy(PolicyConfig{Partitions: []string{"hstles", "acme"}})

	for _, tc := range []struct {
		name, partition string
		wantErr         bool
	}{
		{"registered tenant", "hstles", false},
		{"second registered tenant", "acme", false},
		{"unregistered tenant", "ghost", true},
		{"unstamped tenant bypasses", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pr, _ := trustPrincipal(t, tc.partition)
			err := p.AuthorizeRead(ctx, pr, ports.Topic("fleet.peer"))
			if tc.wantErr && err == nil {
				t.Fatalf("tenant %q was accepted", tc.partition)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("tenant %q was rejected: %v", tc.partition, err)
			}
		})
	}

	// A policy with NO partitions configured must still reject a stamped one —
	// zero-value config means "nothing allowed", not "everything allowed".
	empty := NewPolicy(PolicyConfig{})
	pr, _ := trustPrincipal(t, "hstles")
	if err := empty.AuthorizeRead(ctx, pr, ports.Topic("fleet.peer")); err == nil {
		t.Fatal("a policy with no configured tenants accepted a stamped tenant — " +
			"construction-time openness has inverted into check-time permissiveness")
	}
}

// A publish rule covers the one topic it names, or the namespace beneath it
// when it ends in ".", and the empty rule covers every topic. A restricted
// rule's writers and an open rule's publishers therefore reach no topic whose
// name only starts with the rule's, and the longest covering restricted rule
// decides.
func TestAPublishRuleCoversOneTopicOrANamespace(t *testing.T) {
	ctx := context.Background()
	writer, _ := trustPrincipal(t, "")
	second, _ := trustPrincipal(t, "")
	other, _ := trustPrincipal(t, "")
	who := map[ports.NodeID]string{writer.NodeID: "the rule's writer", second.NodeID: "the longer rule's writer", other.NodeID: "another node"}

	type verdict struct {
		pr     ports.Principal
		topic  string
		accept bool
	}
	run := func(name string, p *Policy, cases []verdict) {
		t.Helper()
		for _, tc := range cases {
			err := p.AuthorizePublish(ctx, tc.pr, ports.Topic(tc.topic))
			if tc.accept && err != nil {
				t.Errorf("%s: %s was refused on %q: %v", name, who[tc.pr.NodeID], tc.topic, err)
			}
			if !tc.accept && err == nil {
				t.Errorf("%s: %s was admitted on %q", name, who[tc.pr.NodeID], tc.topic)
			}
		}
	}

	run("named topics and namespaces", NewPolicy(PolicyConfig{
		PublishPrefixes: map[string][]ports.NodeID{
			"role.claim.auth": {writer.NodeID},
			"keys.":           {writer.NodeID},
		},
		OpenPublishPrefixes: []string{"open.one", "open.family."},
	}), []verdict{
		{writer, "role.claim.auth", true},
		{other, "role.claim.auth", false},
		{writer, "role.claim.authz", false},
		{writer, "role.claim.auth.admin", false},
		{writer, "role.claim", false},
		{writer, "keys.a", true},
		{writer, "keys.a.b", true},
		{other, "keys.a", false},
		{writer, "keys", false},
		{writer, "keysmith", false},
		{other, "open.one", true},
		{other, "open.onetwo", false},
		{other, "open.one.two", false},
		{other, "open.family.a", true},
		{other, "open.family.a.b", true},
		{other, "open.family", false},
		{other, "open.familyx", false},
	})

	run("the longest covering restricted rule", NewPolicy(PolicyConfig{
		PublishPrefixes: map[string][]ports.NodeID{
			"a.":     {writer.NodeID},
			"a.b.":   {second.NodeID},
			"a.b.c":  {writer.NodeID},
			"a.name": {second.NodeID},
		},
	}), []verdict{
		{writer, "a.x", true},
		{second, "a.x", false},
		{second, "a.b.x", true},
		{writer, "a.b.x", false},
		{writer, "a.b.c", true},
		{second, "a.b.c", false},
		{second, "a.name", true},
		{writer, "a.name", false},
		{writer, "a.names", true},
		{second, "a.names", false},
	})

	run("the empty rule", NewPolicy(PolicyConfig{
		PublishPrefixes: map[string][]ports.NodeID{"": {writer.NodeID}},
	}), []verdict{
		{writer, "any.topic", true},
		{other, "any.topic", false},
	})
}

// WithRoleEntitlements gives a role's entitled nodes that role's claim and
// sealed-bundle topics and no topic of a role whose name only starts with it.
func TestWithRoleEntitlementsCoversOnlyTheNamedRole(t *testing.T) {
	ctx := context.Background()
	holder, _ := trustPrincipal(t, "")
	p := NewPolicy(WithRoleEntitlements(BaselinePolicyConfig(),
		map[string][]ports.NodeID{"auth": {holder.NodeID}}))

	for _, topic := range []ports.Topic{secrets.ClaimTopic("auth"), secrets.Topic("auth")} {
		if err := p.AuthorizePublish(ctx, holder, topic); err != nil {
			t.Fatalf("premise wrong: the entitled node was refused its role topic %q: %v", topic, err)
		}
	}
	for _, role := range []string{"authz", "authority", "auth.admin", "auth."} {
		for _, topic := range []ports.Topic{secrets.ClaimTopic(role), secrets.Topic(role)} {
			if err := p.AuthorizePublish(ctx, holder, topic); err == nil {
				t.Errorf("the node entitled to \"auth\" was admitted on %q, a topic of role %q", topic, role)
			}
		}
	}

	// A role name that is empty or ends in "." would register a rule ending
	// in ".", which covers a namespace of other roles' topics; it gets no
	// rule and no entitlement.
	wide := WithRoleEntitlements(BaselinePolicyConfig(),
		map[string][]ports.NodeID{"": {holder.NodeID}, "auth.": {holder.NodeID}})
	if len(wide.PublishPrefixes) != 0 || len(wide.RoleEntitlements) != 0 {
		t.Errorf("a role name that is empty or ends in \".\" was given rules %v and entitlements %v",
			wide.PublishPrefixes, wide.RoleEntitlements)
	}
	wp := NewPolicy(wide)
	for _, topic := range []ports.Topic{
		secrets.ClaimTopic("auth.admin"), secrets.Topic("auth.admin"),
		secrets.ClaimTopic("billing"), secrets.Topic("billing"),
	} {
		if err := wp.AuthorizePublish(ctx, holder, topic); err == nil {
			t.Errorf("a node entitled only under a role name ending in \".\" was admitted on %q", topic)
		}
	}
}
