/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

// Package directory implements the Phase-0.5 cutover ports: the fail-closed
// TrustPolicy (trust.go), the Swarm-backed LiveDirectory projection
// (swarmlive.go), and the shadow parity comparator (shadow.go).
package directory

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	swarmpb "github.com/bbmumford/swarm/proto/pb"
	"google.golang.org/protobuf/proto"

	"github.com/bbmumford/loom/ports"
)

// Topic families the projection decodes into typed views.
const (
	// FleetPeerTopic carries swarmpb.PeerRecord bodies — membership,
	// roles, reach addresses, and RPC handler adverts.
	FleetPeerTopic = ports.Topic("fleet.peer")

	// LatencyTopicPrefix carries JSON ports.LatencySample bodies on
	// composite topics "fleet.latency.<fromNodeID>" — the topic-encoded
	// composite-key pattern (one slot per (from,observer) pair without
	// any swarm schema change).
	LatencyTopicPrefix = "fleet.latency."
)

// LatencyTopic returns the composite latency topic for an observing node.
func LatencyTopic(from ports.NodeID) ports.Topic {
	return ports.Topic(LatencyTopicPrefix + string(from))
}

// SwarmDirectory is the Swarm-backed ports.LiveDirectory: immutable typed
// projections fed by ONE accepted-record stream (Ingest), with every
// accepted record journaled first — restart replays the journal through
// the same projection path, reproducing identical state (§0.5.4).
//
// Wiring: swarm's Config.OnAccepted → Ingest (v0.0.9 hardened swarm), or
// per-topic swarm.Node.Subscribe callbacks → Ingest on v0.0.8. During the
// shadow phase, records may also be fed from the LAD side; the optional
// TrustPolicy re-gates every record pre-projection so an unhardened source
// cannot pollute the views.
type SwarmDirectory struct {
	journal ports.DurableJournal
	policy  ports.TrustPolicy // optional pre-projection gate

	mu       sync.RWMutex
	members  map[ports.NodeID]ports.Member
	reach    map[ports.NodeID][]ports.ReachAddress
	handlers map[string][]ports.HandlerAdvert            // FQN → adverts
	latency  map[ports.NodeID]map[ports.NodeID]ports.LatencySample // from → to → sample
	records  map[ports.Topic]map[ports.NodeID]ports.Record         // raw accepted slots
	// liveness overrides (live-session signal wins over gossip staleness)
	overrides map[ports.NodeID]livenessOverride

	rejected uint64 // records refused by the pre-projection policy gate
}

type livenessOverride struct {
	alive   bool
	expires time.Time
}

// NewSwarmDirectory builds the projection over a journal and rebuilds state
// by replaying it (crash/restart reproduces the live projection).
func NewSwarmDirectory(ctx context.Context, j ports.DurableJournal, policy ports.TrustPolicy) (*SwarmDirectory, error) {
	d := &SwarmDirectory{
		journal:   j,
		policy:    policy,
		members:   map[ports.NodeID]ports.Member{},
		reach:     map[ports.NodeID][]ports.ReachAddress{},
		handlers:  map[string][]ports.HandlerAdvert{},
		latency:   map[ports.NodeID]map[ports.NodeID]ports.LatencySample{},
		records:   map[ports.Topic]map[ports.NodeID]ports.Record{},
		overrides: map[ports.NodeID]livenessOverride{},
	}
	head, err := j.Head(ctx)
	if err != nil {
		return nil, err
	}
	if head > 0 {
		ch, err := j.ReplayUntil(ctx, 0, head, nil)
		if err != nil {
			return nil, err
		}
		for r := range ch {
			d.project(r) // already journaled — project only
		}
	}
	return d, nil
}

// Ingest is the single accepted-record entry point: policy gate →
// journal append → projection. Records that fail the gate never reach
// either. Idempotent per slot: an older-or-equal HLC for a known slot is
// projected last-write-wins by the CRDT upstream, so Ingest trusts its
// caller's ordering (swarm's store already merged).
func (d *SwarmDirectory) Ingest(ctx context.Context, r ports.Record) error {
	if d.policy != nil {
		pr := ports.Principal{NodeID: r.NodeID, PubKey: ed25519.PublicKey(r.AuthorPubKey)}
		if r.Observer != nil {
			// Observer attestations are authored by the observer; the
			// record's NodeID is the SUBJECT. Bind the author key to the
			// observer identity carried in the Observer segment.
			pr.NodeID = ports.NodeID(hex.EncodeToString(r.AuthorPubKey))
		}
		if err := d.policy.VerifyNodeKey(pr.NodeID, pr.PubKey); err != nil {
			d.bumpRejected()
			return fmt.Errorf("directory: ingest refused: %w", err)
		}
		if err := d.policy.AuthorizePublish(ctx, pr, r.Topic); err != nil {
			d.bumpRejected()
			return fmt.Errorf("directory: ingest refused: %w", err)
		}
	}
	if _, err := d.journal.Append(ctx, r); err != nil {
		return err
	}
	d.project(r)
	return nil
}

func (d *SwarmDirectory) bumpRejected() {
	d.mu.Lock()
	d.rejected++
	d.mu.Unlock()
}

// Rejected reports how many records the pre-projection gate refused.
func (d *SwarmDirectory) Rejected() uint64 {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.rejected
}

// project applies one accepted record to the typed views. Tombstones remove.
func (d *SwarmDirectory) project(r ports.Record) {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Raw slot view (KeyOps/Quorum/secrets consumers).
	slots := d.records[r.Topic]
	if slots == nil {
		slots = map[ports.NodeID]ports.Record{}
		d.records[r.Topic] = slots
	}
	slots[r.NodeID] = r

	switch {
	case r.Topic == FleetPeerTopic:
		if r.Tombstone {
			delete(d.members, r.NodeID)
			delete(d.reach, r.NodeID)
			d.dropHandlersLocked(r.NodeID)
			return
		}
		d.projectPeerLocked(r)
	case strings.HasPrefix(string(r.Topic), LatencyTopicPrefix):
		from := ports.NodeID(strings.TrimPrefix(string(r.Topic), LatencyTopicPrefix))
		if r.Tombstone {
			delete(d.latency, from)
			return
		}
		var s ports.LatencySample
		if err := json.Unmarshal(r.Body, &s); err != nil {
			return
		}
		if d.latency[from] == nil {
			d.latency[from] = map[ports.NodeID]ports.LatencySample{}
		}
		d.latency[from][s.To] = s
	}
}

// projectPeerLocked decodes a fleet.peer PeerRecord into the typed views,
// mirroring the mesh RoleTable/AddressTable conventions (service=/region=
// tags, Capabilities.Roles, RpcHandlers). Caller holds d.mu.
func (d *SwarmDirectory) projectPeerLocked(r ports.Record) {
	var pr swarmpb.PeerRecord
	if err := proto.Unmarshal(r.Body, &pr); err != nil {
		return
	}
	// LastSeen derives from the record HLC's wall part (48-bit ms high).
	hlcWall := int64(uint64(r.HLC) >> 16)

	m := ports.Member{
		NodeID:         r.NodeID,
		LastSeenUnixMs: hlcWall,
	}
	if pr.Capabilities != nil {
		m.Roles = append([]string(nil), pr.Capabilities.Roles...)
		for _, tag := range pr.Capabilities.Tags {
			if i := strings.IndexByte(tag, '='); i > 0 {
				k, v := tag[:i], tag[i+1:]
				switch k {
				case "service":
					m.ServiceName = v
				case "region":
					m.Region = v
				case "tenant":
					m.Tenant = v
				}
			}
		}
	}
	d.members[r.NodeID] = m

	addrs := make([]ports.ReachAddress, 0, len(pr.Addresses))
	for _, a := range pr.Addresses {
		addrs = append(addrs, ports.ReachAddress{
			Protocol: transportString(a.Transport),
			Address:  fmt.Sprintf("%s:%d", a.Host, a.Port),
			Scope:    a.Scope,
			Priority: transportPriority(a.Transport),
		})
	}
	sort.SliceStable(addrs, func(i, j int) bool { return addrs[i].Priority < addrs[j].Priority })
	d.reach[r.NodeID] = addrs

	d.dropHandlersLocked(r.NodeID)
	for _, fqn := range pr.RpcHandlers {
		d.handlers[fqn] = append(d.handlers[fqn], ports.HandlerAdvert{
			Name:   fqn,
			NodeID: r.NodeID,
			Roles:  m.Roles,
		})
	}
}

func (d *SwarmDirectory) dropHandlersLocked(id ports.NodeID) {
	for fqn, adverts := range d.handlers {
		kept := adverts[:0]
		for _, a := range adverts {
			if a.NodeID != id {
				kept = append(kept, a)
			}
		}
		if len(kept) == 0 {
			delete(d.handlers, fqn)
		} else {
			d.handlers[fqn] = kept
		}
	}
}

// transportString mirrors the mesh address-table mapping — "noise-udp" >
// "ws" > "grpc" > "http" (the noise-udp↔reach-"udp" naming stays consistent
// with the mesh's transportString).
func transportString(t swarmpb.Address_Transport) string {
	switch t {
	case swarmpb.Address_NOISE_UDP:
		return "noise-udp"
	case swarmpb.Address_WEBSOCKET:
		return "ws"
	case swarmpb.Address_GRPC:
		return "grpc"
	case swarmpb.Address_HTTP:
		return "http"
	default:
		return "unknown"
	}
}

func transportPriority(t swarmpb.Address_Transport) int {
	switch t {
	case swarmpb.Address_NOISE_UDP:
		return 0
	case swarmpb.Address_WEBSOCKET:
		return 1
	case swarmpb.Address_GRPC:
		return 2
	case swarmpb.Address_HTTP:
		return 3
	default:
		return 9
	}
}

// ---------- ports.LiveDirectory ----------

func (d *SwarmDirectory) Members(_ context.Context) ([]ports.Member, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	out := make([]ports.Member, 0, len(d.members))
	for _, m := range d.members {
		out = append(out, cloneMember(m))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NodeID < out[j].NodeID })
	return out, nil
}

func (d *SwarmDirectory) Member(_ context.Context, id ports.NodeID) (ports.Member, bool, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	m, ok := d.members[id]
	return cloneMember(m), ok, nil
}

func (d *SwarmDirectory) NodesByRole(_ context.Context, role string) ([]ports.NodeID, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	var out []ports.NodeID
	for id, m := range d.members {
		for _, r := range m.Roles {
			if r == role {
				out = append(out, id)
				break
			}
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

func (d *SwarmDirectory) Reach(_ context.Context, id ports.NodeID) ([]ports.ReachAddress, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return append([]ports.ReachAddress(nil), d.reach[id]...), nil
}

func (d *SwarmDirectory) Latency(_ context.Context, from, to ports.NodeID) (ports.LatencySample, bool, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	s, ok := d.latency[from][to]
	return s, ok, nil
}

func (d *SwarmDirectory) HandlersByName(_ context.Context, name string) ([]ports.HandlerAdvert, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return append([]ports.HandlerAdvert(nil), d.handlers[name]...), nil
}

func (d *SwarmDirectory) RecordsByTopic(_ context.Context, topic ports.Topic) ([]ports.Record, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	slots := d.records[topic]
	out := make([]ports.Record, 0, len(slots))
	for _, r := range slots {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NodeID < out[j].NodeID })
	return out, nil
}

// OverrideLiveness implements the live-session liveness override: a
// directly-connected peer is alive regardless of gossip staleness.
func (d *SwarmDirectory) OverrideLiveness(id ports.NodeID, alive bool, ttlMs int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if ttlMs <= 0 {
		delete(d.overrides, id)
		return
	}
	d.overrides[id] = livenessOverride{alive: alive, expires: time.Now().Add(time.Duration(ttlMs) * time.Millisecond)}
}

// LivenessOverride reports the current (unexpired) override for a node.
func (d *SwarmDirectory) LivenessOverride(id ports.NodeID) (alive, ok bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	o, present := d.overrides[id]
	if !present || time.Now().After(o.expires) {
		return false, false
	}
	return o.alive, true
}

// Snapshot returns the deterministic point-in-time view.
func (d *SwarmDirectory) Snapshot(ctx context.Context) (ports.DirectorySnapshot, error) {
	head, err := d.journal.Head(ctx)
	if err != nil {
		return ports.DirectorySnapshot{}, err
	}
	d.mu.RLock()
	var recs []ports.Record
	for _, slots := range d.records {
		for _, r := range slots {
			recs = append(recs, r)
		}
	}
	d.mu.RUnlock()
	sort.Slice(recs, func(i, j int) bool {
		if recs[i].Topic != recs[j].Topic {
			return recs[i].Topic < recs[j].Topic
		}
		return recs[i].NodeID < recs[j].NodeID
	})
	return ports.DirectorySnapshot{
		Watermark:   head,
		Fingerprint: fingerprintRecords(recs),
		Records:     recs,
	}, nil
}

func (d *SwarmDirectory) Fingerprint(_ context.Context) ([32]byte, error) {
	d.mu.RLock()
	var recs []ports.Record
	for _, slots := range d.records {
		for _, r := range slots {
			recs = append(recs, r)
		}
	}
	d.mu.RUnlock()
	sort.Slice(recs, func(i, j int) bool {
		if recs[i].Topic != recs[j].Topic {
			return recs[i].Topic < recs[j].Topic
		}
		return recs[i].NodeID < recs[j].NodeID
	})
	return fingerprintRecords(recs), nil
}

// Subscribe delegates to the journal — the accepted-record stream IS the
// subscription surface, watermark-resumable with a lossless history→live
// handoff (see journal.FileJournal.Replay).
func (d *SwarmDirectory) Subscribe(ctx context.Context, topics []ports.Topic, from ports.Watermark) (<-chan ports.Record, error) {
	return d.journal.Replay(ctx, from, topics)
}

// fingerprintRecords hashes sorted (topic, nodeID, hlc, sha256(sig)) tuples
// — the same shape as the swarm Merkle leaf, so two directories with the
// same accepted set agree byte-for-byte.
func fingerprintRecords(recs []ports.Record) [32]byte {
	h := sha256.New()
	for _, r := range recs {
		th := sha256.Sum256([]byte(r.Topic))
		h.Write(th[:])
		nh := sha256.Sum256([]byte(r.NodeID))
		h.Write(nh[:])
		var hlcb [8]byte
		binary.BigEndian.PutUint64(hlcb[:], uint64(r.HLC))
		h.Write(hlcb[:])
		sh := sha256.Sum256(r.Signature)
		h.Write(sh[:])
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out
}

func cloneMember(m ports.Member) ports.Member {
	m.Roles = append([]string(nil), m.Roles...)
	return m
}

var _ ports.LiveDirectory = (*SwarmDirectory)(nil)
