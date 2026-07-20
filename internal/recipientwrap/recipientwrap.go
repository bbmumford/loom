/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

// Package recipientwrap is the per-recipient DEK sealing layer for
// role.secrets envelopes — vendored WIRE-COMPATIBLE from
// diststore/crypto/recipient_wrap.go (diststore is not a published module,
// so loom cannot depend on it and remain standalone-installable). The JSON
// struct shape, Alg tokens, and byte framings are identical to diststore's;
// a wrap produced by either package opens in the other. Behavioural changes
// here MUST stay wire-compatible or be made in both packages.
//
// Two schemes coexist on the wire, selected by Alg:
//   - nacl-box: X25519 + XSalsa20-Poly1305 anonymous sealed box against the
//     recipient's Ed25519 identity key (birationally mapped) — the default;
//     no second keypair, internal ephemeral, no nonce management.
//   - p256-aeskw: ECDH-P256 + HKDF-SHA256 + RFC-3394 AES-256 Key Wrap —
//     FIPS-approvable path for deployments that must avoid NaCl.
package recipientwrap

import (
	"crypto/aes"
	"crypto/ecdh"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/sha512"
	"errors"
	"fmt"
	"io"

	"filippo.io/edwards25519"
	"golang.org/x/crypto/chacha20poly1305"
	"golang.org/x/crypto/hkdf"
	"golang.org/x/crypto/nacl/box"
)

// WrapAlg selects the sealing scheme so NaCl and FIPS deployments coexist
// on the wire — the recipient reads Alg to pick the unwrap path.
type WrapAlg string

const (
	WrapAlgNaClBox   WrapAlg = "nacl-box"
	WrapAlgP256AESKW WrapAlg = "p256-aeskw"
)

// recipientKEKContext domain-separates the P-256 ECDH→HKDF KEK. The token
// is diststore's — shared deliberately for wire compatibility.
const recipientKEKContext = "diststore-recip-kek-v1"

// ErrRecipientUnwrap is returned when a sealed data key fails to open —
// wrong recipient key, tampered ciphertext, or integrity failure.
var ErrRecipientUnwrap = errors.New("recipientwrap: unwrap failed")

// ErrNoEntitlementGate is returned when the entitled gate is nil. A missing
// gate is fail-closed: callers that need no policy check pass
// EntitledAllowAll explicitly so every bypass is greppable and intentional.
var ErrNoEntitlementGate = errors.New("recipientwrap: nil entitlement gate (pass a real check or EntitledAllowAll)")

// EntitledAllowAll is the explicit, auditable "no entitlement gate"
// sentinel for self-wrap and test fixtures.
func EntitledAllowAll() error { return nil }

// RecipientWrappedDataKey is a data key sealed to ONE recipient's public
// key. JSON field names match diststore's exactly (wire contract).
type RecipientWrappedDataKey struct {
	Alg          WrapAlg `json:"alg"`
	RecipientPub []byte  `json:"recipientPub"` // ed25519 32B for NaCl; P-256 SEC1 for FIPS
	Wrapped      []byte  `json:"wrapped"`      // sealed DEK bytes (alg-specific framing)
}

// edPubToX25519Pub maps an Ed25519 public key to its X25519 (Montgomery)
// equivalent via the birational map, so the identity that signs records
// also receives sealed boxes.
func edPubToX25519Pub(edPub ed25519.PublicKey) (*[32]byte, error) {
	if len(edPub) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("recipientwrap: ed25519 pub size %d", len(edPub))
	}
	p, err := new(edwards25519.Point).SetBytes(edPub)
	if err != nil {
		return nil, fmt.Errorf("recipientwrap: ed25519 pub -> x25519: %w", err)
	}
	var x [32]byte
	copy(x[:], p.BytesMontgomery())
	return &x, nil
}

// edPrivToX25519Priv derives the X25519 private scalar from an Ed25519
// private key: RFC-8032 SHA-512 seed expansion, RFC-7748-clamped.
func edPrivToX25519Priv(edPriv ed25519.PrivateKey) (*[32]byte, error) {
	if len(edPriv) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("recipientwrap: ed25519 priv size %d", len(edPriv))
	}
	h := sha512.Sum512(edPriv.Seed())
	h[0] &= 248
	h[31] &= 127
	h[31] |= 64
	var x [32]byte
	copy(x[:], h[:32])
	return &x, nil
}

// WrapDataKeyForRecipient seals a 32-byte DEK to recipientEdPub using a
// NaCl anonymous sealed box (no sender key in the wrap; internal ephemeral
// keypair and nonce — no nonce-reuse risk at this layer).
func WrapDataKeyForRecipient(dek []byte, recipientEdPub ed25519.PublicKey) (RecipientWrappedDataKey, error) {
	if len(dek) != chacha20poly1305.KeySize {
		return RecipientWrappedDataKey{}, fmt.Errorf("recipientwrap: dek size %d, want %d", len(dek), chacha20poly1305.KeySize)
	}
	xPub, err := edPubToX25519Pub(recipientEdPub)
	if err != nil {
		return RecipientWrappedDataKey{}, err
	}
	sealed, err := box.SealAnonymous(nil, dek, xPub, rand.Reader)
	if err != nil {
		return RecipientWrappedDataKey{}, fmt.Errorf("recipientwrap: seal anonymous: %w", err)
	}
	return RecipientWrappedDataKey{
		Alg:          WrapAlgNaClBox,
		RecipientPub: append([]byte(nil), recipientEdPub...),
		Wrapped:      sealed,
	}, nil
}

// UnwrapToRecipient opens w using the recipient's Ed25519 keypair AFTER the
// entitled gate passes. entitled must be non-nil (fail-closed); it runs and
// any error returns verbatim BEFORE key material is touched.
func UnwrapToRecipient(w RecipientWrappedDataKey, recipientEdPub ed25519.PublicKey,
	recipientEdPriv ed25519.PrivateKey, entitled func() error) ([]byte, error) {
	if entitled == nil {
		return nil, ErrNoEntitlementGate
	}
	if err := entitled(); err != nil {
		return nil, err
	}
	if w.Alg != WrapAlgNaClBox {
		return nil, fmt.Errorf("recipientwrap: UnwrapToRecipient: alg %q is not %q", w.Alg, WrapAlgNaClBox)
	}
	xPub, err := edPubToX25519Pub(recipientEdPub)
	if err != nil {
		return nil, err
	}
	xPriv, err := edPrivToX25519Priv(recipientEdPriv)
	if err != nil {
		return nil, err
	}
	dek, ok := box.OpenAnonymous(nil, w.Wrapped, xPub, xPriv)
	if !ok {
		return nil, ErrRecipientUnwrap
	}
	return dek, nil
}

// WrapDataKeyForRecipientP256 is the FIPS path: fresh ephemeral ECDH-P256 →
// HKDF-SHA256 KEK → RFC-3394 AES-256 Key Wrap.
// Framing: Wrapped = ephemeralPubSEC1 || aesKeyWrap(KEK, dek).
func WrapDataKeyForRecipientP256(dek []byte, recipientP256Pub *ecdh.PublicKey) (RecipientWrappedDataKey, error) {
	if len(dek) != chacha20poly1305.KeySize {
		return RecipientWrappedDataKey{}, fmt.Errorf("recipientwrap: dek size %d, want %d", len(dek), chacha20poly1305.KeySize)
	}
	if recipientP256Pub == nil {
		return RecipientWrappedDataKey{}, errors.New("recipientwrap: nil recipient P-256 key")
	}
	eph, err := ecdh.P256().GenerateKey(rand.Reader)
	if err != nil {
		return RecipientWrappedDataKey{}, fmt.Errorf("recipientwrap: ephemeral p256: %w", err)
	}
	kek, err := p256KEK(eph, recipientP256Pub)
	if err != nil {
		return RecipientWrappedDataKey{}, err
	}
	wrapped, err := aesKeyWrap(kek, dek)
	if err != nil {
		return RecipientWrappedDataKey{}, err
	}
	ephPub := eph.PublicKey().Bytes()
	return RecipientWrappedDataKey{
		Alg:          WrapAlgP256AESKW,
		RecipientPub: append([]byte(nil), recipientP256Pub.Bytes()...),
		Wrapped:      append(ephPub, wrapped...),
	}, nil
}

// UnwrapToRecipientP256 reverses WrapDataKeyForRecipientP256 after the
// entitled gate passes (same fail-closed contract as UnwrapToRecipient).
func UnwrapToRecipientP256(w RecipientWrappedDataKey, recipientP256Priv *ecdh.PrivateKey, entitled func() error) ([]byte, error) {
	if entitled == nil {
		return nil, ErrNoEntitlementGate
	}
	if err := entitled(); err != nil {
		return nil, err
	}
	if w.Alg != WrapAlgP256AESKW {
		return nil, fmt.Errorf("recipientwrap: UnwrapToRecipientP256: alg %q is not %q", w.Alg, WrapAlgP256AESKW)
	}
	if recipientP256Priv == nil {
		return nil, errors.New("recipientwrap: nil recipient P-256 private key")
	}
	const p256PubLen = 65 // uncompressed SEC1: 0x04 || X32 || Y32
	if len(w.Wrapped) <= p256PubLen {
		return nil, fmt.Errorf("recipientwrap: p256 wrap too short (%d)", len(w.Wrapped))
	}
	ephPub, err := ecdh.P256().NewPublicKey(w.Wrapped[:p256PubLen])
	if err != nil {
		return nil, fmt.Errorf("recipientwrap: parse ephemeral p256 pub: %w", err)
	}
	kek, err := p256KEK(recipientP256Priv, ephPub)
	if err != nil {
		return nil, err
	}
	dek, err := aesKeyUnwrap(kek, w.Wrapped[p256PubLen:])
	if err != nil {
		return nil, ErrRecipientUnwrap
	}
	return dek, nil
}

func p256KEK(local *ecdh.PrivateKey, remote *ecdh.PublicKey) ([]byte, error) {
	secret, err := local.ECDH(remote)
	if err != nil {
		return nil, fmt.Errorf("recipientwrap: p256 ecdh: %w", err)
	}
	kek := make([]byte, 32)
	if _, err := io.ReadFull(hkdf.New(sha256.New, secret, nil, []byte(recipientKEKContext)), kek); err != nil {
		return nil, fmt.Errorf("recipientwrap: p256 hkdf: %w", err)
	}
	return kek, nil
}

// aesKeyWrap implements RFC-3394 AES Key Wrap with the default IV.
func aesKeyWrap(kek, plaintext []byte) ([]byte, error) {
	if len(plaintext)%8 != 0 || len(plaintext) < 16 {
		return nil, fmt.Errorf("recipientwrap: keywrap plaintext length %d", len(plaintext))
	}
	block, err := aes.NewCipher(kek)
	if err != nil {
		return nil, fmt.Errorf("recipientwrap: keywrap cipher: %w", err)
	}
	n := len(plaintext) / 8
	a := []byte{0xA6, 0xA6, 0xA6, 0xA6, 0xA6, 0xA6, 0xA6, 0xA6}
	r := make([]byte, len(plaintext))
	copy(r, plaintext)
	var buf [16]byte
	for j := 0; j < 6; j++ {
		for i := 0; i < n; i++ {
			copy(buf[:8], a)
			copy(buf[8:], r[i*8:i*8+8])
			block.Encrypt(buf[:], buf[:])
			copy(a, buf[:8])
			copy(r[i*8:i*8+8], buf[8:])
			xorRoundCount(a, uint64(n*j+i+1))
		}
	}
	out := make([]byte, 0, 8+len(plaintext))
	out = append(out, a...)
	return append(out, r...), nil
}

// aesKeyUnwrap reverses aesKeyWrap, verifying the RFC-3394 default IV.
func aesKeyUnwrap(kek, ciphertext []byte) ([]byte, error) {
	if len(ciphertext)%8 != 0 || len(ciphertext) < 24 {
		return nil, fmt.Errorf("recipientwrap: keyunwrap length %d", len(ciphertext))
	}
	block, err := aes.NewCipher(kek)
	if err != nil {
		return nil, fmt.Errorf("recipientwrap: keyunwrap cipher: %w", err)
	}
	n := len(ciphertext)/8 - 1
	a := make([]byte, 8)
	copy(a, ciphertext[:8])
	r := make([]byte, len(ciphertext)-8)
	copy(r, ciphertext[8:])
	var buf [16]byte
	for j := 5; j >= 0; j-- {
		for i := n - 1; i >= 0; i-- {
			xorRoundCount(a, uint64(n*j+i+1))
			copy(buf[:8], a)
			copy(buf[8:], r[i*8:i*8+8])
			block.Decrypt(buf[:], buf[:])
			copy(a, buf[:8])
			copy(r[i*8:i*8+8], buf[8:])
		}
	}
	for _, b := range a {
		if b != 0xA6 {
			return nil, errors.New("recipientwrap: keyunwrap integrity check failed")
		}
	}
	return r, nil
}

// xorRoundCount XORs the big-endian round counter into the A register.
func xorRoundCount(a []byte, t uint64) {
	a[0] ^= byte(t >> 56)
	a[1] ^= byte(t >> 48)
	a[2] ^= byte(t >> 40)
	a[3] ^= byte(t >> 32)
	a[4] ^= byte(t >> 24)
	a[5] ^= byte(t >> 16)
	a[6] ^= byte(t >> 8)
	a[7] ^= byte(t)
}
