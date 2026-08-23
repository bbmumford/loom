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
)

// COVERAGE of the Phase-0.5 TrustPolicy's untested authorization paths
// (#M-559 ④): AuthorizeRead 0.0%, EntitleRole 0.0%, checkTenant 28.6%.
//
// 🛑 THIS GATE IS NOT WIRED YET (#M-557 ⑥ holds it pending @R), so nothing is
// currently at risk. That is exactly why it is worth doing NOW: it becomes
// load-bearing the instant that decision lands, and an authorization path
// executed for the first time in production is the class that produced
// #I-466 and #C-1159.
//
// Every assertion below is about the FAIL-CLOSED direction, because a trust
// gate that wrongly denies is an outage you find in minutes and one that
// wrongly allows is a breach you may never find.

// trustPrincipal builds a principal whose NodeID is the canonical aether NodeID
// of its key — the binding VerifyNodeKey enforces. Taking the ID from the key
// rather than inventing one is deliberate: a hand-written ID would fail the bind
// for a reason unrelated to the property under test.
func trustPrincipal(t *testing.T, tenant string) (ports.Principal, ed25519.PrivateKey) {
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
		NodeID: ports.NodeID(string(id)),
		PubKey: pub,
		Tenant: tenant,
	}, priv
}

// TestVerifyNodeKeyBindsCanonicalRejectsLegacyHex proves the reconcile: the
// NodeID a key binds to is exactly aether.NewNodeID(pub); the prior hex(pub)
// identifier — a different scheme — no longer binds, so a claimed-victim record
// carrying the legacy id (or any non-canonical value) is refused.
func TestVerifyNodeKeyBindsCanonicalRejectsLegacyHex(t *testing.T) {
	p := NewPolicy(PolicyConfig{Tenants: []string{"hstles"}})
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
	p := NewPolicy(PolicyConfig{Tenants: []string{"hstles"}})
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
		short := ports.Principal{NodeID: good.NodeID, PubKey: []byte{1, 2, 3}, Tenant: "hstles"}
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
	p := NewPolicy(PolicyConfig{Tenants: []string{"hstles"}})

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

	t.Run("an UNSTAMPED tenant bypasses the tenant check", func(t *testing.T) {
		// checkTenant returns nil for Tenant == "" — "tenantless fleet
		// principals; tenant scoping applies when stamped". So the gate is
		// only as strong as whatever stamps the tenant upstream.
		tenantless, _ := trustPrincipal(t, "")
		if err := p.AuthorizeRead(ctx, tenantless, ports.Topic("fleet.peer")); err != nil {
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
	p := NewPolicy(PolicyConfig{Tenants: []string{"hstles"}})
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

// checkTenant is reached from several authorize paths; its three branches are
// asserted directly through AuthorizeRead so the behaviour is pinned at the
// caller rather than at an unexported helper.
func TestCheckTenantBranches(t *testing.T) {
	ctx := context.Background()
	p := NewPolicy(PolicyConfig{Tenants: []string{"hstles", "acme"}})

	for _, tc := range []struct {
		name, tenant string
		wantErr      bool
	}{
		{"registered tenant", "hstles", false},
		{"second registered tenant", "acme", false},
		{"unregistered tenant", "ghost", true},
		{"unstamped tenant bypasses", "", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pr, _ := trustPrincipal(t, tc.tenant)
			err := p.AuthorizeRead(ctx, pr, ports.Topic("fleet.peer"))
			if tc.wantErr && err == nil {
				t.Fatalf("tenant %q was accepted", tc.tenant)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("tenant %q was rejected: %v", tc.tenant, err)
			}
		})
	}

	// A policy with NO tenants configured must still reject a stamped one —
	// zero-value config means "nothing allowed", not "everything allowed".
	empty := NewPolicy(PolicyConfig{})
	pr, _ := trustPrincipal(t, "hstles")
	if err := empty.AuthorizeRead(ctx, pr, ports.Topic("fleet.peer")); err == nil {
		t.Fatal("a policy with no configured tenants accepted a stamped tenant — " +
			"construction-time openness has inverted into check-time permissiveness")
	}
}
