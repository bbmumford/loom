/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package directory

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"

	"github.com/bbmumford/loom/ports"
)

// Policy is the standard fail-closed ports.TrustPolicy: rooted in
// CONFIGURED state (tenant set, anchor keys, explicit role entitlements,
// revocations), never in gossiped/self-advertised state. Everything not
// explicitly allowed is denied; every deny returns an error naming the
// failed check so operators can diagnose without code-diving.
//
// It replaces RoleTable.IsAnchorWithPubKey as the observer/secret-recipient
// fence (that check verifies against self-advertised swarm state — an
// attacker advertising the anchor role passes it; nothing passes here
// without configured entitlement).
type Policy struct {
	mu sync.RWMutex

	// tenants is the closed set of tenant IDs this mesh serves.
	tenants map[string]bool

	// roleEntitlements[role] = set of NodeIDs entitled to hold/assume the
	// role (and to receive its sealed secrets).
	roleEntitlements map[string]map[ports.NodeID]bool

	// observers = NodeIDs authorized to attest about other nodes
	// (death tombstones, liveness).
	observers map[ports.NodeID]bool

	// publishPrefixes[prefix] = set of NodeIDs allowed to publish topics
	// with that prefix; the empty-string prefix is the default rule.
	// A topic matches its LONGEST configured prefix.
	publishPrefixes map[string]map[ports.NodeID]bool

	// openPublish marks prefixes any bound identity may publish
	// (e.g. "fleet.peer" — every node publishes its own PeerRecord).
	openPublish map[string]bool

	// revoked keys, by hex encoding.
	revoked map[string]bool
}

// PolicyConfig seeds a Policy. Zero-value fields mean "nothing allowed" for
// that dimension — construction is where openness is granted, never at
// check time.
type PolicyConfig struct {
	Tenants          []string
	Observers        []ports.NodeID
	RoleEntitlements map[string][]ports.NodeID
	// OpenPublishPrefixes lists topic prefixes ANY key-bound node may
	// publish (self-owned records like fleet.peer).
	OpenPublishPrefixes []string
	// PublishPrefixes lists restricted prefixes and their allowed writers.
	PublishPrefixes map[string][]ports.NodeID
}

// NewPolicy builds the policy from configured trust material.
func NewPolicy(cfg PolicyConfig) *Policy {
	p := &Policy{
		tenants:          map[string]bool{},
		roleEntitlements: map[string]map[ports.NodeID]bool{},
		observers:        map[ports.NodeID]bool{},
		publishPrefixes:  map[string]map[ports.NodeID]bool{},
		openPublish:      map[string]bool{},
		revoked:          map[string]bool{},
	}
	for _, t := range cfg.Tenants {
		p.tenants[t] = true
	}
	for _, o := range cfg.Observers {
		p.observers[o] = true
	}
	for role, nodes := range cfg.RoleEntitlements {
		set := map[ports.NodeID]bool{}
		for _, n := range nodes {
			set[n] = true
		}
		p.roleEntitlements[role] = set
	}
	for _, prefix := range cfg.OpenPublishPrefixes {
		p.openPublish[prefix] = true
	}
	for prefix, nodes := range cfg.PublishPrefixes {
		set := map[ports.NodeID]bool{}
		for _, n := range nodes {
			set[n] = true
		}
		p.publishPrefixes[prefix] = set
	}
	return p
}

// RevokeKey marks a key revoked (rotation/compromise). Takes effect on the
// next check — no caching layer to invalidate.
func (p *Policy) RevokeKey(pub ed25519.PublicKey) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.revoked[hex.EncodeToString(pub)] = true
}

// EntitleRole grants role entitlement to a node at runtime (config reload,
// operator action).
func (p *Policy) EntitleRole(role string, id ports.NodeID) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.roleEntitlements[role] == nil {
		p.roleEntitlements[role] = map[ports.NodeID]bool{}
	}
	p.roleEntitlements[role][id] = true
}

// VerifyNodeKey implements ports.TrustPolicy: the NodeID must be the
// canonical hex encoding of pub (the swarm nodeIDFromPub scheme) and the
// key must not be revoked.
func (p *Policy) VerifyNodeKey(id ports.NodeID, pub ed25519.PublicKey) error {
	if len(pub) != ed25519.PublicKeySize {
		return fmt.Errorf("trust: key size %d", len(pub))
	}
	keyHex := hex.EncodeToString(pub)
	if string(id) != keyHex {
		return fmt.Errorf("trust: node %s does not bind to presented key", short(id))
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.revoked[keyHex] {
		return fmt.Errorf("trust: key for node %s is revoked", short(id))
	}
	return nil
}

// AuthorizePublish implements ports.TrustPolicy.
func (p *Policy) AuthorizePublish(_ context.Context, pr ports.Principal, topic ports.Topic) error {
	if err := p.VerifyNodeKey(pr.NodeID, pr.PubKey); err != nil {
		return err
	}
	if err := p.checkTenant(pr); err != nil {
		return err
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	prefix, restricted := p.longestPrefixLocked(string(topic))
	if restricted {
		if !p.publishPrefixes[prefix][pr.NodeID] {
			return fmt.Errorf("trust: node %s not authorized to publish %s", short(pr.NodeID), topic)
		}
		return nil
	}
	for open := range p.openPublish {
		if strings.HasPrefix(string(topic), open) {
			return nil
		}
	}
	return fmt.Errorf("trust: topic %s matches no publish rule (fail closed)", topic)
}

// AuthorizeRead implements ports.TrustPolicy. Reads are tenant-gated plus
// key-bound; secret payload access control is cryptographic (the DEK wrap),
// so read authorization here is deliberately coarser than publish.
func (p *Policy) AuthorizeRead(_ context.Context, pr ports.Principal, topic ports.Topic) error {
	if err := p.VerifyNodeKey(pr.NodeID, pr.PubKey); err != nil {
		return err
	}
	if err := p.checkTenant(pr); err != nil {
		return err
	}
	_ = topic
	return nil
}

// AuthorizeRoleEntitlement implements ports.TrustPolicy.
func (p *Policy) AuthorizeRoleEntitlement(_ context.Context, pr ports.Principal, role string) error {
	if err := p.VerifyNodeKey(pr.NodeID, pr.PubKey); err != nil {
		return err
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if !p.roleEntitlements[role][pr.NodeID] {
		return fmt.Errorf("trust: node %s not entitled to role %q", short(pr.NodeID), role)
	}
	return nil
}

// AuthorizeObserver implements ports.TrustPolicy.
func (p *Policy) AuthorizeObserver(_ context.Context, pr ports.Principal, subject ports.NodeID) error {
	if err := p.VerifyNodeKey(pr.NodeID, pr.PubKey); err != nil {
		return err
	}
	if pr.NodeID == subject {
		return fmt.Errorf("trust: self-attestation is the owner-tombstone path, not observation")
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if !p.observers[pr.NodeID] {
		return fmt.Errorf("trust: node %s is not an authorized observer", short(pr.NodeID))
	}
	return nil
}

// AuthorizeSecretRecipient implements ports.TrustPolicy: recipients must be
// role-entitled. Capability advertisement is checked by the SEALER as a
// necessary condition; this is the sufficient one.
func (p *Policy) AuthorizeSecretRecipient(ctx context.Context, pr ports.Principal, role string) error {
	return p.AuthorizeRoleEntitlement(ctx, pr, role)
}

// KeyState implements ports.TrustPolicy.
func (p *Policy) KeyState(pub ed25519.PublicKey) ports.KeyStatus {
	if len(pub) != ed25519.PublicKeySize {
		return ports.KeyStatusUnknown
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.revoked[hex.EncodeToString(pub)] {
		return ports.KeyStatusRevoked
	}
	return ports.KeyStatusActive
}

func (p *Policy) checkTenant(pr ports.Principal) error {
	if pr.Tenant == "" {
		return nil // tenantless fleet principals; tenant scoping applies when stamped
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if !p.tenants[pr.Tenant] {
		return fmt.Errorf("trust: unknown tenant %q", pr.Tenant)
	}
	return nil
}

// longestPrefixLocked returns the longest restricted prefix matching topic.
func (p *Policy) longestPrefixLocked(topic string) (string, bool) {
	best, found := "", false
	for prefix := range p.publishPrefixes {
		if strings.HasPrefix(topic, prefix) && len(prefix) >= len(best) {
			best, found = prefix, true
		}
	}
	return best, found
}

func short(id ports.NodeID) string {
	if len(id) > 12 {
		return string(id[:12])
	}
	return string(id)
}

var _ ports.TrustPolicy = (*Policy)(nil)
