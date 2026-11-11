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
	NodeID      NodeID
	PubKey      ed25519.PublicKey
	PartitionID string
}

// KeyStatus is the lifecycle state of an identity key.
type KeyStatus int

const (
	KeyStatusUnknown KeyStatus = iota
	KeyStatusActive
	KeyStatusRotated // superseded but within its grace window
	KeyStatusRevoked
)

// TrustPolicy is the fail-closed authorization port. It is the mandatory
// pre-store gate for Swarm and the pre-projection gate for the shadow
// directory: no record may affect roles, reachability, routing, handlers,
// anchor snapshots, or secret recipients before passing it.
//
// It is rooted in configured partition/anchor identity and explicit role
// entitlement. It explicitly does NOT derive from self-advertised state:
// RoleTable.IsAnchorWithPubKey reads a role any peer can advertise and is not
// a trust root — an attacker advertising a role must never become a secret
// recipient or an accepted observer through it. node's InitSwarm gates
// observer attestations on AuthorizeObserver instead.
//
// Every method returns nil to allow and a non-nil error to deny; ambiguity
// (unknown key, unknown partition, missing entitlement data) DENIES.
type TrustPolicy interface {
	// VerifyNodeKey checks NodeID == DeriveNodeID(pub) and that the key is
	// not revoked. Signature verification against a record's embedded key
	// proves possession only — this binding is what makes it identity.
	VerifyNodeKey(id NodeID, pub ed25519.PublicKey) error

	// AuthorizePublish authorizes p to write the given topic (partition scope +
	// the author's own topic slot; role.secrets.<role> additionally requires the role's
	// publisher entitlement).
	AuthorizePublish(ctx context.Context, p Principal, topic Topic) error

	// AuthorizeRead authorizes p to read/subscribe the given topic.
	AuthorizeRead(ctx context.Context, p Principal, topic Topic) error

	// AuthorizeRoleEntitlement authorizes p to hold/assume a service role.
	// TODO: role takeover must consult this before activation; no in-tree
	// caller outside the policy itself does.
	AuthorizeRoleEntitlement(ctx context.Context, p Principal, role string) error

	// AuthorizeObserver authorizes p to attest about subject (death
	// tombstones, liveness observations). It is the trust root the
	// self-advertised anchor check is not; K-of-N quorum is counted only over
	// authorized observers.
	AuthorizeObserver(ctx context.Context, p Principal, subject NodeID) error

	// AuthorizeSecretRecipient authorizes p's key as a wrap target for
	// role.secrets.<role>. Capability advertisement is necessary but never
	// sufficient — this is the fence that keeps Sybil/self-advertised nodes
	// out of the recipient set.
	AuthorizeSecretRecipient(ctx context.Context, p Principal, role string) error

	// KeyState reports rotation/revocation state for a key.
	KeyState(pub ed25519.PublicKey) KeyStatus
}
