/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package ports

import (
	"context"
	"crypto/ed25519"
)

// Principal is the authenticated identity a trust decision is made about.
type Principal struct {
	NodeID NodeID
	PubKey ed25519.PublicKey
	Tenant string
}

// KeyStatus is the lifecycle state of an identity key.
type KeyStatus int

const (
	KeyStatusUnknown KeyStatus = iota
	KeyStatusActive
	KeyStatusRotated // superseded but within its grace window
	KeyStatusRevoked
)

// TrustPolicy is the fail-closed authorization port (Phase 0.5.1). It is the
// mandatory pre-store gate for Swarm and the pre-projection gate during
// shadow migration: no record may affect roles, reachability, routing,
// handlers, anchor snapshots, or secret recipients before passing it.
//
// It is rooted in configured tenant/anchor identity and explicit role
// entitlement. It explicitly does NOT derive from self-advertised state:
// RoleTable.IsAnchorWithPubKey (the current observer gate) is a
// compatibility filter, not a trust root — an attacker advertising a role
// must never become a secret recipient or an accepted observer through it.
//
// Every method returns nil to allow and a non-nil error to deny; ambiguity
// (unknown key, unknown tenant, missing entitlement data) DENIES.
type TrustPolicy interface {
	// VerifyNodeKey checks NodeID == DeriveNodeID(pub) and that the key is
	// not revoked. Signature verification against a record's embedded key
	// proves possession only — this binding is what makes it identity.
	VerifyNodeKey(id NodeID, pub ed25519.PublicKey) error

	// AuthorizePublish authorizes p to write the given topic (tenant scope +
	// topic ownership; role.secrets.<role> additionally requires the role's
	// publisher entitlement).
	AuthorizePublish(ctx context.Context, p Principal, topic Topic) error

	// AuthorizeRead authorizes p to read/subscribe the given topic.
	AuthorizeRead(ctx context.Context, p Principal, topic Topic) error

	// AuthorizeRoleEntitlement authorizes p to hold/assume a service role
	// (Phase-1 takeover consults this before activation).
	AuthorizeRoleEntitlement(ctx context.Context, p Principal, role string) error

	// AuthorizeObserver authorizes p to attest about subject (death
	// tombstones, liveness observations). Replaces the self-advertised
	// anchor check; K-of-N quorum is counted only over authorized observers.
	AuthorizeObserver(ctx context.Context, p Principal, subject NodeID) error

	// AuthorizeSecretRecipient authorizes p's key as a wrap target for
	// role.secrets.<role>. Capability advertisement is necessary but never
	// sufficient — this is the fence that keeps Sybil/self-advertised nodes
	// out of the recipient set (plan §1.2).
	AuthorizeSecretRecipient(ctx context.Context, p Principal, role string) error

	// KeyState reports rotation/revocation state for a key.
	KeyState(pub ed25519.PublicKey) KeyStatus
}
