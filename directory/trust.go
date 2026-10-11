/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package directory

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"strings"
	"sync"

	aether "github.com/ORBTR/aether"
	"github.com/bbmumford/loom/ports"
)

// Policy is the standard fail-closed ports.TrustPolicy: rooted in
// CONFIGURED state (partition set, anchor keys, explicit role entitlements,
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

	// partitions is the closed set of partition IDs this mesh serves.
	partitions map[string]bool

	// roleEntitlements[role] = set of NodeIDs entitled to hold/assume the
	// role (and to receive its sealed secrets).
	roleEntitlements map[string]map[ports.NodeID]bool

	// observers = NodeIDs authorized to attest about other nodes
	// (death tombstones, liveness).
	observers map[ports.NodeID]bool

	// publishPrefixes[rule] = set of NodeIDs allowed to publish the topics
	// that restricted rule covers (ruleCovers); the empty rule is the
	// default. The LONGEST restricted rule covering a topic decides it.
	publishPrefixes map[string]map[ports.NodeID]bool

	// openPublish marks the rules whose topics any bound identity may
	// publish (e.g. "fleet.peer" — every node publishes its own PeerRecord).
	openPublish map[string]bool

	// selfOwnedOpen holds the SelfOwnedPublishPrefixes rules this policy
	// opens. AuthorizePublish decides a topic one of them covers before it
	// reads publishPrefixes, so no restricted rule closes a topic every node
	// publishes about itself. A rule naming one topic admits that topic; a
	// namespace rule admits a node only on the topic its own id forms there.
	selfOwnedOpen []string

	// revoked keys, by canonical aether NodeID.
	revoked map[string]bool
}

// PolicyConfig seeds a Policy. Zero-value fields mean "nothing allowed" for
// that dimension — construction is where openness is granted, never at
// check time.
type PolicyConfig struct {
	Partitions       []string
	Observers        []ports.NodeID
	RoleEntitlements map[string][]ports.NodeID
	// OpenPublishPrefixes lists the rules whose topics ANY key-bound node
	// may publish (self-owned records like fleet.peer). A rule covers topics
	// as PublishPrefixes describes, except that a SelfOwnedPublishPrefixes
	// namespace admits a node only on the topic its own id forms there.
	OpenPublishPrefixes []string
	// PublishPrefixes lists restricted rules and their allowed writers. A
	// rule ending in "." covers every topic beneath that namespace, the
	// empty rule covers every topic, and any other rule covers only the
	// topic it names: "role.claim.auth" reaches neither "role.claim.authz"
	// nor "role.claim.auth.admin". A restricted rule decides before every
	// open rule except a SelfOwnedPublishPrefixes rule that
	// OpenPublishPrefixes lists, whose topics stay open under it.
	PublishPrefixes map[string][]ports.NodeID
}

// NewPolicy builds the policy from configured trust material.
func NewPolicy(cfg PolicyConfig) *Policy {
	p := &Policy{
		partitions:       map[string]bool{},
		roleEntitlements: map[string]map[ports.NodeID]bool{},
		observers:        map[ports.NodeID]bool{},
		publishPrefixes:  map[string]map[ports.NodeID]bool{},
		openPublish:      map[string]bool{},
		revoked:          map[string]bool{},
	}
	for _, t := range cfg.Partitions {
		p.partitions[t] = true
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
	for _, rule := range SelfOwnedPublishPrefixes {
		if p.openPublish[rule] {
			p.selfOwnedOpen = append(p.selfOwnedOpen, rule)
		}
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
	id, ok := canonicalNodeID(pub)
	if !ok {
		return // a wrong-length key can never be a valid NodeID, so nothing to revoke
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.revoked[id] = true
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

// VerifyNodeKey implements ports.TrustPolicy: the NodeID must be the canonical
// aether.NewNodeID value for pub, and the key must not be revoked.
func (p *Policy) VerifyNodeKey(id ports.NodeID, pub ed25519.PublicKey) error {
	if len(pub) != ed25519.PublicKeySize {
		return fmt.Errorf("trust: key size %d", len(pub))
	}
	derived, ok := canonicalNodeID(pub)
	if !ok || string(id) != derived {
		return fmt.Errorf("trust: node %s does not bind to presented key", short(id))
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.revoked[derived] {
		return fmt.Errorf("trust: key for node %s is revoked", short(id))
	}
	return nil
}

// AuthorizePublish implements ports.TrustPolicy. After the key and partition
// checks it reads the publish rules in one order: a self-owned rule this
// policy opens decides, admitting a topic it names and, under a namespace,
// only the publishing node's own topic; else the longest restricted rule
// covering the topic decides for its writers alone; else any open rule
// admits; else the topic matches no rule and is refused.
//
// The self-owned rules are read first because every node republishes its
// PeerRecord on FleetPeerTopic and node's enforcing swarm trust gate calls
// this on each one: a supplied restricted rule above or on such a topic (the
// empty rule, "fleet.") would otherwise refuse every node's record.
func (p *Policy) AuthorizePublish(_ context.Context, pr ports.Principal, topic ports.Topic) error {
	if err := p.VerifyNodeKey(pr.NodeID, pr.PubKey); err != nil {
		return err
	}
	if err := p.checkPartition(pr); err != nil {
		return err
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	for _, own := range p.selfOwnedOpen {
		if !ruleCovers(own, string(topic)) {
			continue
		}
		// A self-owned namespace holds one topic per node, the one that
		// node's id forms under it (LatencyTopic). SwarmDirectory.project
		// reads the owning node from the topic and replaces or deletes that
		// node's samples, so the namespace admits a node on its own topic
		// and refuses it every other topic beneath it.
		if strings.HasSuffix(own, ".") && string(topic) != own+string(pr.NodeID) {
			return fmt.Errorf("trust: node %s may publish under %s only on its own topic, not %s",
				short(pr.NodeID), own, topic)
		}
		return nil
	}
	rule, restricted := p.longestPrefixLocked(string(topic))
	if restricted {
		if !p.publishPrefixes[rule][pr.NodeID] {
			return fmt.Errorf("trust: node %s not authorized to publish %s", short(pr.NodeID), topic)
		}
		return nil
	}
	for open := range p.openPublish {
		if ruleCovers(open, string(topic)) {
			return nil
		}
	}
	return fmt.Errorf("trust: topic %s matches no publish rule (fail closed)", topic)
}

// AuthorizeRead implements ports.TrustPolicy. Reads are partition-gated plus
// key-bound; secret payload access control is cryptographic (the DEK wrap),
// so read authorization here is deliberately coarser than publish.
func (p *Policy) AuthorizeRead(_ context.Context, pr ports.Principal, topic ports.Topic) error {
	if err := p.VerifyNodeKey(pr.NodeID, pr.PubKey); err != nil {
		return err
	}
	if err := p.checkPartition(pr); err != nil {
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
	derived, ok := canonicalNodeID(pub)
	if !ok {
		return ports.KeyStatusUnknown
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.revoked[derived] {
		return ports.KeyStatusRevoked
	}
	return ports.KeyStatusActive
}

// canonicalNodeID maps an Ed25519 key to its NodeID through the single scheme
// aether.NewNodeID. VerifyNodeKey compares a claimed NodeID against this value
// and the revoked set is keyed by it, so a key's wire identity and its
// revocation entry derive from one authority and can never disagree. ok is
// false for a wrong-length key, which can never be a valid NodeID.
func canonicalNodeID(pub ed25519.PublicKey) (string, bool) {
	id, err := aether.NewNodeID(pub)
	if err != nil {
		return "", false
	}
	return string(id), true
}

func (p *Policy) checkPartition(pr ports.Principal) error {
	if pr.PartitionID == "" {
		return nil // an unstamped fleet principal; partition scoping applies once stamped
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if !p.partitions[pr.PartitionID] {
		return fmt.Errorf("trust: unknown partition %q", pr.PartitionID)
	}
	return nil
}

// longestPrefixLocked returns the longest restricted rule covering topic. A
// rule naming the topic is as long as the topic, so it wins over every
// namespace rule above it.
func (p *Policy) longestPrefixLocked(topic string) (string, bool) {
	best, found := "", false
	for rule := range p.publishPrefixes {
		if ruleCovers(rule, topic) && len(rule) >= len(best) {
			best, found = rule, true
		}
	}
	return best, found
}

// ruleCovers reports whether a publish rule covers topic, for AuthorizePublish
// and longestPrefixLocked. A rule ending in "." covers every topic beneath
// that namespace (LatencyTopicPrefix, which LatencyTopic extends with a node
// id), the empty rule covers every topic, and any other rule covers the one
// topic it names. A named rule is exact because WithRoleEntitlements registers
// secrets.ClaimTopic(role) and secrets.Topic(role) for each role, and role
// names share leading text ("auth", "authz", "auth.admin"): a rule matched by
// leading text gives one role's writers the topics of every such role.
func ruleCovers(rule, topic string) bool {
	if rule == "" || strings.HasSuffix(rule, ".") {
		return strings.HasPrefix(topic, rule)
	}
	return topic == rule
}

func short(id ports.NodeID) string {
	if len(id) > 12 {
		return string(id[:12])
	}
	return string(id)
}

var _ ports.TrustPolicy = (*Policy)(nil)
