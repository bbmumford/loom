/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package secrets

import (
	"errors"
	"fmt"

	dscrypto "github.com/bbmumford/loom/internal/recipientwrap"
	"github.com/bbmumford/loom/ports"
)

// TopicPrefix is the swarm topic family carrying sealed role config bundles,
// alongside the existing agent.keys.<tenant> / agent.network-psk.<tenant>.<epoch>
// taxonomy.
const TopicPrefix = "role.secrets."

// ClaimTopicPrefix carries takeover claims (the anti-thundering-herd
// claim-set CRDT — pollen's ClaimWorkload/ReleaseWorkload pattern: claim =
// live record, release = tombstone, first N by HLC+nodeID win, others back
// off). Non-negotiable: without it every capable node assumes the role at
// once.
const ClaimTopicPrefix = "role.claim."

// Topic returns the sealed-secrets topic for a service role.
func Topic(role string) ports.Topic { return ports.Topic(TopicPrefix + role) }

// ClaimTopic returns the takeover-claim topic for a service role.
func ClaimTopic(role string) ports.Topic { return ports.Topic(ClaimTopicPrefix + role) }

// RecipientWrap is one recipient's sealed copy of the DEK. It reuses
// diststore's RecipientWrappedDataKey verbatim (NaCl anonymous sealed box to
// the recipient's Ed25519 identity key via the X25519 birational map, or
// P256-AESKW for FIPS deployments). The recipient's public key IS the
// matching ID — an Opener scans for its own identity pub. The sealed blob
// carries its own internal ephemeral key; no envelope-level ephemeral or
// per-wrap nonce exists, which removes that whole nonce-management surface
// from the wrap layer.
type RecipientWrap = dscrypto.RecipientWrappedDataKey

// Envelope is the record BODY published on role.secrets.<role> (plan §1.1).
// It rides inside a normal signed swarm record — the swarm merge/merkle/sig
// keys (topic, nodeID, HLC, tombstone, pubkey) stay cleartext and untouched;
// only this body is opaque.
//
// Nonce rule: Nonce is a FRESH random XChaCha20-Poly1305 nonce generated at
// every Seal. It is never derived from (topic,node,epoch): a fixed per-epoch
// nonce reused across two distinct plaintexts (config change or roster
// change without an epoch bump) would be a catastrophic AEAD nonce-reuse
// failure. Epoch is metadata for ordering/rotation observability, not nonce
// input.
type Envelope struct {
	// Role is the service role whose config bundle is sealed here.
	Role string `json:"role"`

	// ConfigKeys lists the cfgload names inside the bundle — exactly the
	// role's capabilities.RoleRequirement.Configs (e.g. auth → ["auth",
	// "session", "platform"]). Cleartext by design: recipients decide
	// eligibility before decrypting.
	ConfigKeys []string `json:"configKeys"`

	// Epoch is the seal generation, bumped on config-bundle or recipient-
	// roster change.
	Epoch uint64 `json:"epoch"`

	// PerRecipient holds one sealed DEK per authorized recipient identity.
	PerRecipient []RecipientWrap `json:"perRecipient"`

	// Nonce is the XChaCha20-Poly1305 nonce for Ciphertext — fresh random
	// bytes every seal (24 bytes).
	Nonce []byte `json:"nonce"`

	// Ciphertext is the AEAD-encrypted canonical config bundle. The AEAD
	// additional data binds Role+Epoch (see sealAAD), so a ciphertext
	// cannot be transplanted onto a different role's record or replayed
	// under a different epoch without detection.
	Ciphertext []byte `json:"ciphertext"`
}

// Validate checks structural well-formedness (not cryptographic validity).
func (e *Envelope) Validate() error {
	switch {
	case e.Role == "":
		return errors.New("secrets: envelope missing role")
	case len(e.ConfigKeys) == 0:
		return fmt.Errorf("secrets: envelope for %q lists no config keys", e.Role)
	case len(e.PerRecipient) == 0:
		return fmt.Errorf("secrets: envelope for %q has no recipients", e.Role)
	case len(e.Nonce) == 0:
		return fmt.Errorf("secrets: envelope for %q missing nonce", e.Role)
	case len(e.Ciphertext) == 0:
		return fmt.Errorf("secrets: envelope for %q has empty ciphertext", e.Role)
	}
	for i, r := range e.PerRecipient {
		if len(r.RecipientPub) == 0 || len(r.Wrapped) == 0 {
			return fmt.Errorf("secrets: envelope for %q recipient %d incomplete", e.Role, i)
		}
	}
	return nil
}

// ConfigBundle maps cfgload config names to their payloads — the plaintext
// a Sealer encrypts and an Opener recovers.
type ConfigBundle map[string][]byte
