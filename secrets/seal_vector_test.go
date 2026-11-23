/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
 */

package secrets

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"strings"
	"testing"

	dscrypto "github.com/bbmumford/diststore/crypto"
)

// storedEnvelope is a role.secrets record body sealed by this package's
// Sealer to vectorRecipient's key, with the recipient's public key left as
// @@PUB@@ (filled in by the test from the same label). Replicas store such
// bodies verbatim and an Opener must still open them, so it holds the wrap
// framing, the AEAD additional data and the bundle encoding together.
const storedEnvelope = `{"role":"vector-role","configKeys":["db","keys"],"epoch":3,"perRecipient` +
	`":[{"alg":"nacl-box","recipientPub":"@@PUB@@","wrapped":"o6Zc4znLIlJcX+a` +
	`OHzVDUJ2xtwPBX/XOHpz3d80nbH+5QYL4IcS5ALATdt+d/S0uyb2rJMR/zX3npHh6HFS4vta` +
	`CWiY9YDj5Lr/Z1XU89Yc="}],"nonce":"rmTN8gO4tX+KrsoYMbxZ9rfdCIXRj997","cip` +
	`hertext":"1QAM9VDubvsLw3q83CDgyu1/9c50CWNOi06beehAPrGZJyHPikSbrRYh6jLcVb` +
	`5dhUDSVA7DxlC9ZdIKsq2dgh2uSmLzdlTGH6F3UQVWBLFhExJOHkkbmPaNRtC993MuW7JZCA` +
	`6M31aTnqyGZzIYq/8qkZfRXSM="}`

func vectorRecipient() (ed25519.PublicKey, ed25519.PrivateKey) {
	seed := sha256.Sum256([]byte("loom secrets vector: recipient"))
	priv := ed25519.NewKeyFromSeed(seed[:])
	return priv.Public().(ed25519.PublicKey), priv
}

func TestStoredEnvelopeOpens(t *testing.T) {
	pub, priv := vectorRecipient()
	body := strings.Replace(storedEnvelope, "@@PUB@@", base64.StdEncoding.EncodeToString(pub), 1)
	env, err := DecodeEnvelope([]byte(body))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if env.Role != "vector-role" || env.Epoch != 3 || len(env.PerRecipient) != 1 || env.PerRecipient[0].Alg != dscrypto.WrapAlgNaClBox {
		t.Fatalf("decoded envelope = %+v", env)
	}
	got, err := NewOpener(pub, priv, dscrypto.EntitledAllowAll).Open(context.Background(), env)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if len(got) != 2 || string(got["db"]) != `{"dsn":"vector"}` || string(got["keys"]) != `{"kid":"vector"}` {
		t.Fatalf("bundle = %q", got)
	}
}
