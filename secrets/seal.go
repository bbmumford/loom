/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package secrets

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sort"

	dscrypto "github.com/bbmumford/loom/internal/recipientwrap"
	"golang.org/x/crypto/chacha20poly1305"
)

// ErrNotRecipient is returned by Open when none of the envelope's wraps is
// sealed to the opener's identity key. Callers treat it as "not eligible",
// never as corruption.
var ErrNotRecipient = errors.New("secrets: no wrap sealed to this identity")

// Recipient is an authorized wrap target. Callers MUST have vetted every
// recipient through TrustPolicy.AuthorizeSecretRecipient before sealing —
// capability advertisement alone never makes a node a recipient; the Sealer
// trusts its input list.
type Recipient struct {
	// IdentityPub is the recipient node's Ed25519 identity public key. The
	// DEK is sealed to its X25519 birational image (diststore NaCl path),
	// so recipients need no second keypair.
	IdentityPub ed25519.PublicKey
}

// Sealer produces envelopes. Implemented by the role's current config
// holder (the publisher).
type Sealer interface {
	Seal(ctx context.Context, role string, epoch uint64, bundle ConfigBundle, recipients []Recipient) (*Envelope, error)
}

// Opener recovers a bundle from an envelope using the local identity key.
type Opener interface {
	Open(ctx context.Context, env *Envelope) (ConfigBundle, error)
}

// NewSealer returns the standard Sealer.
func NewSealer() Sealer { return stdSealer{} }

// NewOpener returns an Opener for the given identity keypair. entitled is
// the per-open authorization gate (diststore contract: MUST be non-nil —
// wire TrustPolicy.AuthorizeSecretRecipient through it, or pass
// dscrypto.EntitledAllowAll explicitly in tests so every bypass is
// greppable). It runs before any key material is touched.
func NewOpener(pub ed25519.PublicKey, priv ed25519.PrivateKey, entitled func() error) Opener {
	return stdOpener{pub: pub, priv: priv, entitled: entitled}
}

// sealAAD binds the ciphertext to its role + epoch so a sealed bundle
// cannot be transplanted onto another role's topic or replayed under a
// different epoch: version tag ‖ len(role) ‖ role ‖ epoch (fixed framing —
// this is part of the wire contract once records exist).
func sealAAD(role string, epoch uint64) []byte {
	aad := make([]byte, 0, 24+len(role)+8)
	aad = append(aad, []byte("loom-role-secrets-v1")...)
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(role)))
	aad = append(aad, lenBuf[:]...)
	aad = append(aad, role...)
	var epochBuf [8]byte
	binary.BigEndian.PutUint64(epochBuf[:], epoch)
	return append(aad, epochBuf[:]...)
}

// canonicalBundle produces deterministic bytes for a bundle (sorted keys,
// length-prefixed segments). Determinism is NOT needed for convergence
// (replicas store the owner's bytes verbatim) — it exists so tests and
// audit tooling can compare plaintexts structurally.
func canonicalBundle(b ConfigBundle) ([]byte, error) {
	keys := make([]string, 0, len(b))
	for k := range b {
		if k == "" {
			return nil, errors.New("secrets: empty config key")
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	ordered := make([]struct {
		Key     string `json:"key"`
		Payload []byte `json:"payload"`
	}, 0, len(keys))
	for _, k := range keys {
		ordered = append(ordered, struct {
			Key     string `json:"key"`
			Payload []byte `json:"payload"`
		}{k, b[k]})
	}
	return json.Marshal(ordered)
}

func decodeBundle(raw []byte) (ConfigBundle, error) {
	var ordered []struct {
		Key     string `json:"key"`
		Payload []byte `json:"payload"`
	}
	if err := json.Unmarshal(raw, &ordered); err != nil {
		return nil, fmt.Errorf("secrets: bundle decode: %w", err)
	}
	out := make(ConfigBundle, len(ordered))
	for _, e := range ordered {
		out[e.Key] = e.Payload
	}
	return out, nil
}

type stdSealer struct{}

func (stdSealer) Seal(_ context.Context, role string, epoch uint64, bundle ConfigBundle, recipients []Recipient) (*Envelope, error) {
	if role == "" {
		return nil, errors.New("secrets: seal: empty role")
	}
	if len(bundle) == 0 {
		return nil, fmt.Errorf("secrets: seal %q: empty bundle", role)
	}
	if len(recipients) == 0 {
		return nil, fmt.Errorf("secrets: seal %q: no recipients (TrustPolicy returned none?)", role)
	}

	plaintext, err := canonicalBundle(bundle)
	if err != nil {
		return nil, err
	}

	// Fresh DEK per seal — rotating the DEK on every publish means a
	// recipient removed from the roster loses access at the next seal
	// without any epoch gymnastics.
	dek := make([]byte, chacha20poly1305.KeySize)
	if _, err := rand.Read(dek); err != nil {
		return nil, fmt.Errorf("secrets: dek: %w", err)
	}

	aead, err := chacha20poly1305.NewX(dek)
	if err != nil {
		return nil, fmt.Errorf("secrets: aead: %w", err)
	}
	// Fresh random nonce per seal (the package-doc nonce rule). XChaCha20's
	// 24-byte nonce makes random generation collision-safe.
	nonce := make([]byte, chacha20poly1305.NonceSizeX)
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("secrets: nonce: %w", err)
	}
	ciphertext := aead.Seal(nil, nonce, plaintext, sealAAD(role, epoch))

	wraps := make([]RecipientWrap, 0, len(recipients))
	seen := make(map[string]bool, len(recipients))
	for i, r := range recipients {
		if len(r.IdentityPub) != ed25519.PublicKeySize {
			return nil, fmt.Errorf("secrets: seal %q: recipient %d bad key size %d", role, i, len(r.IdentityPub))
		}
		if k := string(r.IdentityPub); seen[k] {
			continue // dedup identical recipients
		} else {
			seen[k] = true
		}
		w, err := dscrypto.WrapDataKeyForRecipient(dek, r.IdentityPub)
		if err != nil {
			return nil, fmt.Errorf("secrets: seal %q: wrap recipient %d: %w", role, i, err)
		}
		wraps = append(wraps, w)
	}

	keys := make([]string, 0, len(bundle))
	for k := range bundle {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	env := &Envelope{
		Role:         role,
		ConfigKeys:   keys,
		Epoch:        epoch,
		PerRecipient: wraps,
		Nonce:        nonce,
		Ciphertext:   ciphertext,
	}
	return env, env.Validate()
}

type stdOpener struct {
	pub      ed25519.PublicKey
	priv     ed25519.PrivateKey
	entitled func() error
}

func (o stdOpener) Open(_ context.Context, env *Envelope) (ConfigBundle, error) {
	if env == nil {
		return nil, errors.New("secrets: open: nil envelope")
	}
	if err := env.Validate(); err != nil {
		return nil, err
	}
	var wrap *RecipientWrap
	for i := range env.PerRecipient {
		if bytes.Equal(env.PerRecipient[i].RecipientPub, o.pub) {
			wrap = &env.PerRecipient[i]
			break
		}
	}
	if wrap == nil {
		return nil, ErrNotRecipient
	}
	// The entitlement gate runs inside UnwrapToRecipient BEFORE key
	// material is touched (diststore fail-closed contract; nil rejected).
	dek, err := dscrypto.UnwrapToRecipient(*wrap, o.pub, o.priv, o.entitled)
	if err != nil {
		return nil, fmt.Errorf("secrets: open %q: %w", env.Role, err)
	}
	aead, err := chacha20poly1305.NewX(dek)
	if err != nil {
		return nil, fmt.Errorf("secrets: open %q: aead: %w", env.Role, err)
	}
	plaintext, err := aead.Open(nil, env.Nonce, env.Ciphertext, sealAAD(env.Role, env.Epoch))
	if err != nil {
		return nil, fmt.Errorf("secrets: open %q: decrypt failed (tampered, wrong AAD, or corrupted): %w", env.Role, err)
	}
	return decodeBundle(plaintext)
}

// EncodeEnvelope serializes an envelope for a swarm record body.
func EncodeEnvelope(e *Envelope) ([]byte, error) {
	if err := e.Validate(); err != nil {
		return nil, err
	}
	return json.Marshal(e)
}

// DecodeEnvelope parses a swarm record body into an envelope.
func DecodeEnvelope(body []byte) (*Envelope, error) {
	var e Envelope
	if err := json.Unmarshal(body, &e); err != nil {
		return nil, fmt.Errorf("secrets: envelope decode: %w", err)
	}
	if err := e.Validate(); err != nil {
		return nil, err
	}
	return &e, nil
}
