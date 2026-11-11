/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package directory

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	aether "github.com/ORBTR/aether"
	"github.com/bbmumford/loom/ports"
)

// Policy is the standard fail-closed ports.TrustPolicy: rooted in
// CONFIGURED state (partition set, anchor keys, explicit role entitlements,
// revocations), never in gossiped/self-advertised state. Everything not
// explicitly allowed is denied; every deny returns an error naming the
// failed check so operators can diagnose without code-diving.
//
// It is the observer fence node's InitSwarm wires (anchorObserverPolicy) and
// the secret-recipient fence node's role takeover asks, never
// RoleTable.IsAnchorWithPubKey, which reads the anchor role an attacker can
// advertise; nothing passes here without configured entitlement or an
// endorsement under a configured anchor key.
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

	// anchorKeys holds the anchor public keys the signed mesh document
	// lists, one per anchor-key holder, keyed by the raw key bytes.
	// AuthorizeObserver admits a node whose fleet.peer record carries an
	// endorsement one of them signed (endorsements supplies the records).
	anchorKeys   map[string]bool
	endorsements AnchorEndorsements

	// verified caches each endorsement signature check by the record's
	// endorsementDigest, under verifiedMu: AuthorizeObserver judges every
	// attestation against the records that could outrank the observer's,
	// and fleet.peer is open to any node's publish, so without it a peer
	// publishing records with bogus tags under a listed key would cost one
	// ed25519 check per record per attestation. It holds the signature's
	// validity alone, which no key-list change or revocation alters; both
	// are read live. AuthorizeObserver drops the digests of records no
	// longer held once the cache outgrows them (pruneVerified).
	verifiedMu sync.Mutex
	verified   map[[sha256.Size]byte]bool

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
	Partitions []string
	Observers  []ports.NodeID
	// AnchorKeys are the anchor public keys the signed mesh document lists,
	// one per anchor-key holder. A node is an observer when its fleet.peer
	// record carries an endorsement one of them signed (SetAnchorEndorsements
	// supplies the records); each key counts once toward a K-of-N quorum.
	AnchorKeys       []ed25519.PublicKey
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
		anchorKeys:       map[string]bool{},
		verified:         map[[sha256.Size]byte]bool{},
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
	p.SetAnchorKeys(cfg.AnchorKeys)
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

// SetAnchorKeys replaces the anchor public keys whose endorsements admit an
// observer with keys (each of Ed25519 size; any other is left out). NewPolicy
// seeds them from PolicyConfig.AnchorKeys, and node's
// Runtime.SetAnchorPublicKeys calls it with the list a reloaded mesh document
// names, so a re-signed document's holders count without a restart. The
// next AuthorizeObserver reads the new set; cached signature checks stay
// valid because they do not depend on it.
func (p *Policy) SetAnchorKeys(keys []ed25519.PublicKey) {
	set := make(map[string]bool, len(keys))
	for _, k := range keys {
		if len(k) == ed25519.PublicKeySize {
			set[string(k)] = true
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.anchorKeys = set
}

// SetAnchorEndorsements names where AuthorizeObserver reads each peer's
// fleet.peer endorsement. node's InitSwarm passes its RoleTable. Unset, only
// the configured Observers are admitted.
func (p *Policy) SetAnchorEndorsements(src AnchorEndorsements) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.endorsements = src
}

// AuthorizeObserver implements ports.TrustPolicy. After the key bind and the
// self-attestation refusal it admits a configured observer, or a node whose
// latest fleet.peer record carries an endorsement that verifies under one of
// the configured anchor keys, provided no other node holds a valid
// endorsement under the same key that outranks it (outranks: a later
// IssuedAt, or the same IssuedAt and a lower NodeID). A record dated more
// than MaxIssuedAtSkew past this node's clock counts for nothing: its node is
// not admitted and it outranks no one.
//
// One node per anchor key is what makes the K-of-N quorum count distinct
// anchor keys: swarm's attestation accumulator keys its set by
// ObserverNodeID, so admitting one node per key is the only way a holder
// running several machines, or minting several node keys, counts once. The
// freshest endorsement is the one admitted because a holder's live machine
// republishes its record and so re-signs its endorsement, while a dead
// machine's last record only ages until the role table prunes it: a
// replacement machine counts at once, whatever its NodeID. Because the
// admitted node changes as records are republished, swarm asks this check
// again for every attestation it holds when it counts a quorum.
func (p *Policy) AuthorizeObserver(_ context.Context, pr ports.Principal, subject ports.NodeID) error {
	if err := p.VerifyNodeKey(pr.NodeID, pr.PubKey); err != nil {
		return err
	}
	if pr.NodeID == subject {
		return fmt.Errorf("trust: self-attestation is the owner-tombstone path, not observation")
	}
	p.mu.RLock()
	configured := p.observers[pr.NodeID]
	src := p.endorsements
	keys := len(p.anchorKeys)
	p.mu.RUnlock()
	if configured {
		return nil
	}
	if src == nil || keys == 0 {
		return fmt.Errorf("trust: node %s is not an authorized observer", short(pr.NodeID))
	}
	records := src.AnchorEndorsements()
	defer p.pruneVerified(records)
	own, ok := records[pr.NodeID]
	if !ok {
		return fmt.Errorf("trust: node %s is not an authorized observer: its fleet.peer record carries no anchor endorsement", short(pr.NodeID))
	}
	if !bytes.Equal(own.NodeKey, pr.PubKey) {
		return fmt.Errorf("trust: node %s is not an authorized observer: its fleet.peer record is signed by another key", short(pr.NodeID))
	}
	latest := time.Now().Add(MaxIssuedAtSkew).UnixNano()
	if own.IssuedAt > latest {
		return fmt.Errorf("trust: node %s is not an authorized observer: its fleet.peer record is dated more than %s past this node's clock", short(pr.NodeID), MaxIssuedAtSkew)
	}
	anchorKey, err := p.endorsedKey(pr.NodeID, own)
	if err != nil {
		return fmt.Errorf("trust: node %s is not an authorized observer: %w", short(pr.NodeID), err)
	}
	for id, rec := range records {
		if id == pr.NodeID || !outranks(id, rec, pr.NodeID, own, latest) || !bytes.Equal(rec.Endorsement.AnchorKey, anchorKey) {
			continue
		}
		if _, err := p.endorsedKey(id, rec); err == nil {
			return fmt.Errorf("trust: node %s is not counted as an observer: its anchor key counts through node %s, whose endorsement is fresher",
				short(pr.NodeID), short(id))
		}
	}
	return nil
}

// outranks reports whether node id's record rec is the one counted before
// node self's record own under one anchor key: rec was issued later, or at
// the same instant by a lower NodeID, so every node reading the same records
// admits the same one. A rec dated after latest (unix nanoseconds: the
// judging node's clock plus MaxIssuedAtSkew) outranks no one: ranked by a
// date its machine's clock or its holder's key put past the judging clock,
// it would displace every later machine of the same holder until that clock
// reached it. AuthorizeObserver calls it before any signature check, so only
// records that could displace the observer cost one.
func outranks(id ports.NodeID, rec EndorsedRecord, self ports.NodeID, own EndorsedRecord, latest int64) bool {
	if rec.IssuedAt > latest {
		return false
	}
	if rec.IssuedAt != own.IssuedAt {
		return rec.IssuedAt > own.IssuedAt
	}
	return id < self
}

// endorsementVerifies reports whether rec's endorsement is a valid signature
// by its own anchor key over (id, rec's identity key, rec's IssuedAt),
// checking each distinct record (endorsementDigest) once. endorsedKey calls
// it after the cheap checks (key bind, listed key) have passed.
func (p *Policy) endorsementVerifies(id ports.NodeID, rec EndorsedRecord) bool {
	d := endorsementDigest(id, rec)
	p.verifiedMu.Lock()
	ok, cached := p.verified[d]
	p.verifiedMu.Unlock()
	if cached {
		return ok
	}
	ok = verifyAnchorEndorsement(rec.Endorsement, id, rec.NodeKey, rec.IssuedAt)
	p.verifiedMu.Lock()
	p.verified[d] = ok
	p.verifiedMu.Unlock()
	return ok
}

// pruneVerified keeps the signature cache bounded by the records the
// endorsement source holds: once it holds more than twice their count (plus
// a floor, so a small table is not swept on every check) it keeps only the
// digests of records in records. AuthorizeObserver calls it after each
// check, so a peer churning bogus records grows the cache by at most that
// bound, which swarm's own record caps bound in turn.
func (p *Policy) pruneVerified(records map[ports.NodeID]EndorsedRecord) {
	const floor = 64
	p.verifiedMu.Lock()
	defer p.verifiedMu.Unlock()
	if len(p.verified) <= 2*len(records)+floor {
		return
	}
	kept := make(map[[sha256.Size]byte]bool, len(records))
	for id, rec := range records {
		d := endorsementDigest(id, rec)
		if ok, cached := p.verified[d]; cached {
			kept[d] = ok
		}
	}
	p.verified = kept
}

// endorsedKey returns the anchor key under which rec's endorsement admits id:
// id binds to rec's identity key and is not revoked (VerifyNodeKey), the
// endorsement's key is a configured anchor key, and its signature verifies
// over (id, the identity key, the record's IssuedAt).
func (p *Policy) endorsedKey(id ports.NodeID, rec EndorsedRecord) (ed25519.PublicKey, error) {
	if err := p.VerifyNodeKey(id, rec.NodeKey); err != nil {
		return nil, err
	}
	p.mu.RLock()
	listed := p.anchorKeys[string(rec.Endorsement.AnchorKey)]
	p.mu.RUnlock()
	if !listed {
		return nil, errors.New("its endorsement is signed by an anchor key the mesh document does not list")
	}
	if !p.endorsementVerifies(id, rec) {
		return nil, errors.New("its anchor endorsement does not verify")
	}
	return rec.Endorsement.AnchorKey, nil
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
