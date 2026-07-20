/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package secrets

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"testing"

	dscrypto "github.com/bbmumford/loom/internal/recipientwrap"
)

func genKey(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return pub, priv
}

func testBundle() ConfigBundle {
	return ConfigBundle{
		"auth":     []byte(`{"jwtKey":"k1"}`),
		"session":  []byte(`{"ttl":30}`),
		"platform": []byte(`{"tenants":["hstles"]}`),
	}
}

func TestSealOpenRoundTrip(t *testing.T) {
	ctx := context.Background()
	pubA, privA := genKey(t)
	pubB, privB := genKey(t)

	env, err := NewSealer().Seal(ctx, "auth", 1, testBundle(),
		[]Recipient{{IdentityPub: pubA}, {IdentityPub: pubB}})
	if err != nil {
		t.Fatalf("seal: %v", err)
	}
	if len(env.PerRecipient) != 2 {
		t.Fatalf("wraps = %d, want 2", len(env.PerRecipient))
	}
	if got := env.ConfigKeys; len(got) != 3 || got[0] != "auth" || got[1] != "platform" || got[2] != "session" {
		t.Fatalf("config keys = %v", got)
	}

	// Wire round-trip through the record-body codec.
	body, err := EncodeEnvelope(env)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	decoded, err := DecodeEnvelope(body)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	for _, tc := range []struct {
		name string
		pub  ed25519.PublicKey
		priv ed25519.PrivateKey
	}{{"recipientA", pubA, privA}, {"recipientB", pubB, privB}} {
		got, err := NewOpener(tc.pub, tc.priv, dscrypto.EntitledAllowAll).Open(ctx, decoded)
		if err != nil {
			t.Fatalf("%s open: %v", tc.name, err)
		}
		if string(got["auth"]) != `{"jwtKey":"k1"}` || len(got) != 3 {
			t.Fatalf("%s bundle mismatch: %v", tc.name, got)
		}
	}
}

func TestOpenNonRecipientFails(t *testing.T) {
	ctx := context.Background()
	pubA, _ := genKey(t)
	pubC, privC := genKey(t)

	env, err := NewSealer().Seal(ctx, "auth", 1, testBundle(), []Recipient{{IdentityPub: pubA}})
	if err != nil {
		t.Fatal(err)
	}
	_, err = NewOpener(pubC, privC, dscrypto.EntitledAllowAll).Open(ctx, env)
	if !errors.Is(err, ErrNotRecipient) {
		t.Fatalf("want ErrNotRecipient, got %v", err)
	}
}

func TestEntitlementGateFailClosed(t *testing.T) {
	ctx := context.Background()
	pubA, privA := genKey(t)
	env, err := NewSealer().Seal(ctx, "auth", 1, testBundle(), []Recipient{{IdentityPub: pubA}})
	if err != nil {
		t.Fatal(err)
	}

	denied := errors.New("policy: not entitled")
	if _, err := NewOpener(pubA, privA, func() error { return denied }).Open(ctx, env); !errors.Is(err, denied) {
		t.Fatalf("want policy denial, got %v", err)
	}
	// nil gate must be rejected (diststore fail-closed contract), never allow.
	if _, err := NewOpener(pubA, privA, nil).Open(ctx, env); err == nil {
		t.Fatal("nil entitlement gate must fail closed")
	}
}

func TestAADBindsRoleAndEpoch(t *testing.T) {
	ctx := context.Background()
	pubA, privA := genKey(t)
	env, err := NewSealer().Seal(ctx, "auth", 7, testBundle(), []Recipient{{IdentityPub: pubA}})
	if err != nil {
		t.Fatal(err)
	}
	opener := NewOpener(pubA, privA, dscrypto.EntitledAllowAll)

	// Transplanting the ciphertext onto another role must fail decryption.
	crossRole := *env
	crossRole.Role = "billing"
	if _, err := opener.Open(ctx, &crossRole); err == nil {
		t.Fatal("cross-role transplant must fail AEAD open")
	}
	// Replaying under a different epoch must fail decryption.
	crossEpoch := *env
	crossEpoch.Epoch = 8
	if _, err := opener.Open(ctx, &crossEpoch); err == nil {
		t.Fatal("cross-epoch replay must fail AEAD open")
	}
}

func TestFreshNoncePerSeal(t *testing.T) {
	ctx := context.Background()
	pubA, _ := genKey(t)
	e1, err := NewSealer().Seal(ctx, "auth", 1, testBundle(), []Recipient{{IdentityPub: pubA}})
	if err != nil {
		t.Fatal(err)
	}
	e2, err := NewSealer().Seal(ctx, "auth", 1, testBundle(), []Recipient{{IdentityPub: pubA}})
	if err != nil {
		t.Fatal(err)
	}
	if string(e1.Nonce) == string(e2.Nonce) {
		t.Fatal("two seals of the same (role, epoch) must NOT share a nonce")
	}
}

func TestTopicHelpers(t *testing.T) {
	if Topic("auth") != "role.secrets.auth" {
		t.Fatalf("Topic = %s", Topic("auth"))
	}
	if ClaimTopic("auth") != "role.claim.auth" {
		t.Fatalf("ClaimTopic = %s", ClaimTopic("auth"))
	}
}
