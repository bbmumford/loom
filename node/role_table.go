/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"log"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/bbmumford/swarm"
	swarmpb "github.com/bbmumford/swarm/proto/pb"
	lad "github.com/bbmumford/ledger"
	"google.golang.org/protobuf/proto"
)

// PeerInfo carries the broader PeerRecord projection — roles + grade
// from the RoleRecord plus the operator-facing service_name + region +
// edge_roles + tags pulled out of Capabilities. Topology readers need
// these fields after the LAD cutover; lad.RoleRecord's shape is fixed
// so RoleTable carries the richer view alongside.
type PeerInfo struct {
	NodeID      string
	Roles       []string
	EdgeRoles   []string
	ServiceName string
	Region      string
	Tags        map[string]string
	MaxGrade    int
	Updated     time.Time
	// PubKey is the Sig-verified Ed25519 public key carried on the
	// PeerRecord swarm.Record envelope. Captured so the observer-
	// tombstone gate (IsAnchorWithPubKey) can bind a claimed
	// ObserverNodeID to the Sig-verified pubkey on the attestation,
	// closing the Sybil hole where a malicious peer could mint a fake
	// (NodeID, PubKey) pair and have its attestations counted toward
	// the K-of-N quorum.
	PubKey []byte

	// Saturated reports that the peer advertised connection-budget
	// pressure (the "saturated=1" tag) and wants peers to stop opening
	// fresh paths to it. BackoffUntil is the unix deadline carried on the
	// "backoff_until" tag; consumers treat now >= BackoffUntil as cleared
	// even if the boolean tag lingers, bounding staleness if the peer dies
	// while saturated.
	Saturated    bool
	BackoffUntil int64
}

// RoleTable indexes PeerRecords by role for RPC dispatch lookup. It
// subscribes to the swarm "fleet.peer" topic and maintains an in-memory
// reverse index from role -> set of (NodeID + grade + handlers).
//
// The Lookup return shape matches lad.RoleRecord so callers like
// findNodeForRole in mesh_session_finder.go can swap source with minimal
// churn.
type RoleTable struct {
	mu       sync.RWMutex
	byRole   map[string]map[string]lad.RoleRecord // role -> nodeID -> record
	byNode   map[string]lad.RoleRecord            // nodeID -> latest record
	infoBy   map[string]PeerInfo                  // nodeID -> richer peer projection
	unsub    swarm.Unsubscribe
	selfNode string
}

// NewRoleTable creates a RoleTable subscribed to the given swarm Node's
// "fleet.peer" topic. The subscription is established immediately.
func NewRoleTable(node swarm.Node, selfNodeID string) (*RoleTable, error) {
	rt := &RoleTable{
		byRole:   make(map[string]map[string]lad.RoleRecord),
		byNode:   make(map[string]lad.RoleRecord),
		infoBy:   make(map[string]PeerInfo),
		selfNode: selfNodeID,
	}
	if node == nil {
		return rt, nil
	}
	unsub, err := node.Subscribe(topicFleetPeer, rt.onRecord)
	if err != nil {
		return nil, err
	}
	rt.unsub = unsub
	return rt, nil
}

// Close unsubscribes from the swarm.
func (t *RoleTable) Close() {
	if t.unsub != nil {
		t.unsub()
		t.unsub = nil
	}
}

// Lookup returns the set of nodes serving the given role. When handler is
// non-empty, only records whose RPC handler set includes the handler are
// returned. The return shape matches lad.RoleRecord to keep callers
// source-agnostic.
func (t *RoleTable) Lookup(role, handler string) []lad.RoleRecord {
	t.mu.RLock()
	defer t.mu.RUnlock()
	byNode, ok := t.byRole[role]
	if !ok {
		return nil
	}
	out := make([]lad.RoleRecord, 0, len(byNode))
	for _, rec := range byNode {
		if handler != "" && !sliceContains(handlerFQNs(rec), handler) {
			continue
		}
		out = append(out, rec)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NodeID < out[j].NodeID })
	return out
}

// All returns the latest PeerRecord-derived RoleRecord for every known peer,
// keyed by NodeID. Used by mesh-topology builders.
func (t *RoleTable) All() map[string]lad.RoleRecord {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make(map[string]lad.RoleRecord, len(t.byNode))
	for k, v := range t.byNode {
		out[k] = v
	}
	return out
}

// AllPeerInfo returns the richer per-peer projection (roles + edge_roles
// + service_name + region + tags + grade) for every known peer. Topology
// builders consume this so post-LAD cutover they can still surface
// operator-facing identifiers (service / region) that the legacy reach
// record used to carry in its Metadata map.
func (t *RoleTable) AllPeerInfo() map[string]PeerInfo {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make(map[string]PeerInfo, len(t.infoBy))
	for k, v := range t.infoBy {
		out[k] = v
	}
	return out
}

// PruneStale deletes every peer whose PeerRecord has not been refreshed since
// cutoff, from THIS node's local view only. Returns the number removed.
//
// This is the reliable, topology-independent cleanup for the ghost roster, and
// it is safe for one reason the ledger directory could never claim: the
// RoleTable is a pure LOCAL PROJECTION rebuilt from gossip, not a propagating
// directory. Deleting an entry here creates no tombstone and sends nothing —
// so it can never suppress a live node's re-announcement (the failure that made
// ledger's propagating liveness tombstones unusable). A live peer's next
// PeerRecord simply re-adds it via onRecord; a partitioned peer reappears when
// the partition heals; only a genuinely dead peer, which has stopped
// publishing, stays gone.
//
// A re-gossiped ghost cannot take root either: it arrives carrying its
// origin's original IssuedAt (swarm gossips the signed record unchanged), which
// is already past cutoff, so onRecord drops it born-stale and the next sweep
// would remove it regardless.
//
// Self is exempt: a quiet node still holds its own identity.
func (t *RoleTable) PruneStale(cutoff time.Time) int {
	stale := t.StaleNodes(cutoff)
	for _, nodeID := range stale {
		t.removeNode(nodeID)
	}
	return len(stale)
}

// StaleNodes returns every peer whose PeerRecord has not been refreshed since
// cutoff — the nodes this table believes in but has not heard from.
//
// WHY THIS EXISTS: a RoleTable entry has exactly one removal path — an inbound
// tombstone on fleet.peer (see onRecord). There is no TTL and no expiry. So an
// entry survives only two possible deaths: the owner's own graceful tombstone
// (never emitted when a deploy destroys the machine) or an observer tombstone,
// which until now was published solely by ConnectionManager.sweepZombieSessions
// — a sweep over dead SESSIONS, structurally blind to a node this process never
// held a session with. Every machine replaced by a deploy landed in exactly that
// gap: unreachable by every removal path, immortal.
//
// Measured on the live fleet: the LAD directory (which does have a TTL and a
// liveness sweep) reported a clean 8 records while this table held 40 against 11
// real machines — node-orbtr-io alone carried 3 anchor identities for 1 machine.
// mesh-topology merges both sources, so the corpses rendered as healthy peers
// and were dialled forever: noise-UDP hung to msg2 timeout, WebSocket took
// 401/404 and fell back to TLS.
//
// Staleness is judged on PeerInfo.Updated, which carries the record's own
// IssuedAt — the publisher's claim, not our receipt — so it is immune to local
// clock drift on the reader and to gossip relay delay.
//
// This is an OBSERVATION, not a verdict: callers must treat a returned node as
// "I have not heard from this peer", never as authority to evict it. The only
// safe consumer is an observer attestation, which needs K distinct anchors to
// independently agree before anything is removed anywhere.
//
// Self is always excluded — a node must never attest against itself.
func (t *RoleTable) StaleNodes(cutoff time.Time) []string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	var out []string
	for nodeID, info := range t.infoBy {
		if nodeID == "" || nodeID == t.selfNode {
			continue
		}
		// A record with no IssuedAt cannot be aged. Refuse to attest on
		// absent evidence rather than guessing — an unaged record reads as
		// "infinitely old" against any cutoff, which would attest against
		// every peer whose publisher omitted the field.
		if info.Updated.IsZero() {
			continue
		}
		if info.Updated.Before(cutoff) {
			out = append(out, nodeID)
		}
	}
	return out
}

// RolesOf returns the published role set for a single peer, or nil when
// the peer hasn't been observed in any PeerRecord yet. Convenience over
// PeerInfo(nodeID).Roles for callers that only need the role slice and
// would otherwise have to defensive-copy the entire PeerInfo struct.
func (t *RoleTable) RolesOf(nodeID string) []string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	info, ok := t.infoBy[nodeID]
	if !ok || len(info.Roles) == 0 {
		return nil
	}
	out := make([]string, len(info.Roles))
	copy(out, info.Roles)
	return out
}

// PeerInfo returns the rich projection for one peer, or zero-value with
// ok=false if the peer is not yet known.
func (t *RoleTable) PeerInfo(nodeID string) (PeerInfo, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	info, ok := t.infoBy[nodeID]
	return info, ok
}

// IsAnchorWithPubKey is the swarm observer-tombstone gate. It returns
// true when nodeID is in the RoleTable with the "anchor" role AND the
// supplied pubKey byte-for-byte matches the PubKey on that peer's most
// recent PeerRecord. Used by node.SetObserverRoleCheck to bind a claimed
// ObserverNodeID to its Sig-verified pubkey before counting the
// attestation toward the K-of-N quorum — without this bind, a Sybil
// attacker could claim any NodeID and supply their own keypair.
//
// Returns false when the peer is unknown, has no anchor role, the
// pubkeys don't match exactly, or the supplied pubKey is empty (refuse
// the obvious bypass attempt).
func (t *RoleTable) IsAnchorWithPubKey(nodeID swarm.NodeID, pubKey []byte) bool {
	if len(pubKey) == 0 {
		return false
	}
	t.mu.RLock()
	defer t.mu.RUnlock()
	info, ok := t.infoBy[string(nodeID)]
	if !ok {
		return false
	}
	hasAnchor := false
	for _, r := range info.Roles {
		if r == "anchor" {
			hasAnchor = true
			break
		}
	}
	if !hasAnchor {
		return false
	}
	if len(info.PubKey) != len(pubKey) {
		return false
	}
	for i := range pubKey {
		if info.PubKey[i] != pubKey[i] {
			return false
		}
	}
	return true
}

func (t *RoleTable) onRecord(r swarm.Record) error {
	if r.Tombstone {
		log.Printf("[SWARM] RoleTable.onRecord: tombstone node=%s", truncID(string(r.NodeID)))
		t.removeNode(string(r.NodeID))
		return nil
	}
	pr := &swarmpb.PeerRecord{}
	if err := proto.Unmarshal(r.Body, pr); err != nil {
		log.Printf("[SWARM] RoleTable.onRecord: proto.Unmarshal FAILED node=%s: %v", truncID(string(r.NodeID)), err)
		return nil // drop malformed records silently
	}
	rec := peerRecordToRoleRecord(pr, string(r.NodeID))
	info := peerRecordToInfo(pr, string(r.NodeID))

	// Reject a born-stale record: its IssuedAt is already older than the
	// liveness window, so it is a dead node's original PeerRecord being
	// re-gossiped by a peer that has not pruned it yet. Adding it would let the
	// ghost re-root between sweeps and thrash. A genuinely live node always
	// republishes with a fresh IssuedAt (every 5 min), so this never drops a
	// live peer. Self is exempt — never drop our own record. Zero IssuedAt is
	// left alone (cannot be aged; a different guard's problem).
	if info.NodeID != t.selfNode && !info.Updated.IsZero() &&
		info.Updated.Before(time.Now().Add(-roleStaleThreshold)) {
		log.Printf("[SWARM] RoleTable.onRecord: DROP born-stale node=%s issuedAgo=%s",
			truncID(string(r.NodeID)), time.Since(info.Updated).Round(time.Second))
		return nil
	}
	// Capture the Sig-verified pubkey on the record envelope so the
	// observer-tombstone gate can bind a claimed observer NodeID to its
	// pubkey on attestations — see IsAnchorWithPubKey.
	if len(r.PubKey) > 0 {
		info.PubKey = append([]byte(nil), r.PubKey...)
	}
	log.Printf("[SWARM] RoleTable.onRecord: node=%s roles=%v handlers=%d service=%s region=%s",
		truncID(string(r.NodeID)), rec.Roles, len(rec.Handlers), info.ServiceName, info.Region)
	t.applyRecord(rec, info)
	return nil
}

func (t *RoleTable) applyRecord(rec lad.RoleRecord, info PeerInfo) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Remove from old role indices first
	if old, ok := t.byNode[rec.NodeID]; ok {
		for _, role := range old.Roles {
			if byNode, ok := t.byRole[role]; ok {
				delete(byNode, rec.NodeID)
				if len(byNode) == 0 {
					delete(t.byRole, role)
				}
			}
		}
	}
	t.byNode[rec.NodeID] = rec
	t.infoBy[rec.NodeID] = info
	for _, role := range rec.Roles {
		byNode, ok := t.byRole[role]
		if !ok {
			byNode = make(map[string]lad.RoleRecord)
			t.byRole[role] = byNode
		}
		byNode[rec.NodeID] = rec
	}
}

func (t *RoleTable) removeNode(nodeID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	old, ok := t.byNode[nodeID]
	if !ok {
		return
	}
	delete(t.byNode, nodeID)
	delete(t.infoBy, nodeID)
	for _, role := range old.Roles {
		if byNode, ok := t.byRole[role]; ok {
			delete(byNode, nodeID)
			if len(byNode) == 0 {
				delete(t.byRole, role)
			}
		}
	}
}

// peerRecordToInfo projects the swarm PeerRecord onto the richer
// PeerInfo shape. service_name + region come from the "service=" and
// "region=" entries in Capabilities.tags; all other tag entries
// preserved in the Tags map for operators who add custom tags.
func peerRecordToInfo(pr *swarmpb.PeerRecord, nodeIDStr string) PeerInfo {
	info := PeerInfo{
		NodeID:   nodeIDStr,
		Tags:     make(map[string]string),
		MaxGrade: int(pr.MaxGrade),
	}
	// MESH-G03: map IssuedAt==0 to the zero time, not 1970. time.Unix(0,0) is
	// 1970-01-01 — which is NOT time.Time{}.IsZero(), so the "zero IssuedAt is
	// left alone" guards in onRecord / StaleNodes never fired and a peer
	// publishing IssuedAt==0 was dropped as born-stale and never became routable.
	if pr.IssuedAt != 0 {
		info.Updated = time.Unix(0, pr.IssuedAt).UTC()
	}
	if pr.Capabilities != nil {
		info.Roles = append([]string(nil), pr.Capabilities.Roles...)
		info.EdgeRoles = append([]string(nil), pr.Capabilities.EdgeRoles...)
		for _, tag := range pr.Capabilities.Tags {
			k, v, ok := splitTag(tag)
			if !ok {
				continue
			}
			info.Tags[k] = v
			switch k {
			case "service":
				info.ServiceName = v
			case "region":
				info.Region = v
			}
		}
		info.Saturated, info.BackoffUntil = parseSaturationTags(pr.Capabilities.Tags)
	}
	return info
}

// parseSaturationTags extracts the saturation signal from a PeerRecord's
// Capabilities.Tags: "saturated=1" sets the bit, "backoff_until=<unix>" carries
// the flat-TTL deadline. Shared by the role table (typed PeerInfo projection)
// and the address table (per-DialCandidate stamp) so both read the bit
// identically. A malformed deadline parses to 0 (no backoff), never a panic.
func parseSaturationTags(tags []string) (saturated bool, backoffUntil int64) {
	for _, t := range tags {
		k, v, ok := splitTag(t)
		if !ok {
			continue
		}
		switch k {
		case "saturated":
			saturated = v == "1"
		case "backoff_until":
			if n, err := strconv.ParseInt(v, 10, 64); err == nil {
				backoffUntil = n
			}
		}
	}
	return saturated, backoffUntil
}

// splitTag parses a "key=value" tag string. Trailing whitespace is not
// trimmed — operators must pass clean tags.
func splitTag(s string) (string, string, bool) {
	for i := 0; i < len(s); i++ {
		if s[i] == '=' {
			return s[:i], s[i+1:], true
		}
	}
	return "", "", false
}

// ---------- helpers ----------

func peerRecordToRoleRecord(pr *swarmpb.PeerRecord, nodeIDStr string) lad.RoleRecord {
	roles := []string{}
	if pr.Capabilities != nil {
		roles = append(roles, pr.Capabilities.Roles...)
	}
	// Build HandlerMetadata from the PeerRecord's rpc_handlers list.
	handlers := make([]lad.HandlerMetadata, 0, len(pr.RpcHandlers))
	for _, fqn := range pr.RpcHandlers {
		handlers = append(handlers, lad.HandlerMetadata{
			Name:      fqn,
			TimeoutMs: 5000,
		})
	}
	return lad.RoleRecord{
		NodeID:   nodeIDStr,
		Roles:    roles,
		Handlers: handlers,
		MaxGrade: int(pr.MaxGrade),
		Updated:  time.Unix(0, pr.IssuedAt).UTC(),
	}
}

func handlerFQNs(r lad.RoleRecord) []string {
	out := make([]string, len(r.Handlers))
	for i, h := range r.Handlers {
		out[i] = h.Name
	}
	return out
}

func sliceContains(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}
