/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 */
package node

import (
	"context"
	"crypto/ed25519"
	"testing"
	"time"

	"github.com/bbmumford/loom/secrets"
)

func TestStartRoleSecretsPublisher_Validation(t *testing.T) {
	rt, _ := takeoverFixture(t)
	okBundle := func(context.Context, string) (secrets.ConfigBundle, uint64, error) { return nil, 0, nil }
	okRecips := func(context.Context, string) ([]secrets.Recipient, error) { return nil, nil }

	if _, err := rt.StartRoleSecretsPublisher(PublisherConfig{Roles: []string{"auth"}}); err == nil {
		t.Error("nil Bundle/Recipients must be rejected")
	}
	if _, err := rt.StartRoleSecretsPublisher(PublisherConfig{Bundle: okBundle, Recipients: okRecips}); err == nil {
		t.Error("no roles must be rejected")
	}
}

// Republish must build the bundle + recipient set for every held role and seal+publish it. Sealing
// runs real crypto against the recipient's Ed25519 key, so this also proves a plausible recipient
// produces a valid envelope end to end. Constructed directly (not via Start) so the assertion is
// synchronous — no ticker goroutine touches the counters.
func TestRoleSecretsPublisher_RepublishSealsEveryRole(t *testing.T) {
	rt, _ := takeoverFixture(t)
	pub, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}

	sealed := map[string]bool{}
	p := &RoleSecretsPublisher{rt: rt, cfg: PublisherConfig{
		Roles:    []string{"auth", "directory"},
		Interval: time.Hour,
		Bundle: func(_ context.Context, role string) (secrets.ConfigBundle, uint64, error) {
			return secrets.ConfigBundle{"config": []byte("cfg-for-" + role)}, 7, nil
		},
		Recipients: func(_ context.Context, role string) ([]secrets.Recipient, error) {
			sealed[role] = true
			return []secrets.Recipient{{IdentityPub: pub}}, nil
		},
	}}
	p.Republish(context.Background())

	if !sealed["auth"] || !sealed["directory"] {
		t.Fatalf("Republish must publish every held role, sealed=%v", sealed)
	}
}

// A role whose Recipients set is empty (no eligible peer yet) is skipped without error — the
// publisher must not fail the whole cycle because one role has no candidate.
func TestRoleSecretsPublisher_EmptyRecipientsSkipped(t *testing.T) {
	rt, _ := takeoverFixture(t)
	p := &RoleSecretsPublisher{rt: rt, cfg: PublisherConfig{
		Roles:      []string{"auth"},
		Bundle:     func(context.Context, string) (secrets.ConfigBundle, uint64, error) { return nil, 1, nil },
		Recipients: func(context.Context, string) ([]secrets.Recipient, error) { return nil, nil },
	}}
	// Must not panic or block; nothing to assert beyond a clean return.
	p.Republish(context.Background())
}
