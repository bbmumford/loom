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
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sort"
	"strconv"
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

// recordSlot is the raw-slot identity within a topic: (NodeID, Key), matching
// swarm's own store keying. Key == "" is the classical single-slot-per-node
// form. Keying these views by NodeID alone would silently drop every keyed
// record a node holds beyond the first — see ports.Record.Key.
type recordSlot struct {
	NodeID ports.NodeID
	Key    string
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
	handlers map[string][]ports.HandlerAdvert                      // FQN → adverts
	latency  map[ports.NodeID]map[ports.NodeID]ports.LatencySample // from → to → sample
	records  map[ports.Topic]map[recordSlot]ports.Record           // raw accepted slots
	// liveness overrides (live-session signal wins over gossip staleness)
	overrides map[ports.NodeID]livenessOverride

	// observe makes the policy gate count failures without dropping the
	// record: Ingest still journals and projects it. This is the staged
	// step before an enforcing gate — deploy with observe, watch Rejected()
	// against Checked() for legitimate traffic, and only construct the
	// enforcing directory once the ratio is zero. With no policy wired the
	// flag is irrelevant (the gate is skipped entirely).
	observe bool

	checked  uint64 // records that reached the policy gate (0 when no policy)
	rejected uint64 // gate failures: refused when enforcing, would-refuse when observing
}

type livenessOverride struct {
	alive   bool
	expires time.Time
}

// NewSwarmDirectory builds the projection over a journal and rebuilds state
// by replaying it (crash/restart reproduces the live projection). When policy
// is non-nil the gate ENFORCES: a record that fails is dropped, never journaled
// or projected. Pass nil to skip the gate entirely.
func NewSwarmDirectory(ctx context.Context, j ports.DurableJournal, policy ports.TrustPolicy) (*SwarmDirectory, error) {
	return newSwarmDirectory(ctx, j, policy, false)
}

// NewSwarmDirectoryObserving builds the projection with the policy gate in
// OBSERVE mode: every record is checked and gate failures are counted + logged
// (see Rejected / Checked), but the record is STILL journaled and projected.
// This is the staged step that de-risks turning the fail-closed gate on across a
// live fleet — a seeded-but-imperfect PolicyConfig would otherwise drop every
// record whose topic it forgot to allow. Deploy this, confirm Rejected() stays
// zero for legitimate traffic, then cut to the enforcing NewSwarmDirectory.
func NewSwarmDirectoryObserving(ctx context.Context, j ports.DurableJournal, policy ports.TrustPolicy) (*SwarmDirectory, error) {
	return newSwarmDirectory(ctx, j, policy, true)
}

func newSwarmDirectory(ctx context.Context, j ports.DurableJournal, policy ports.TrustPolicy, observe bool) (*SwarmDirectory, error) {
	d := &SwarmDirectory{
		journal:   j,
		policy:    policy,
		observe:   observe,
		members:   map[ports.NodeID]ports.Member{},
		reach:     map[ports.NodeID][]ports.ReachAddress{},
		handlers:  map[string][]ports.HandlerAdvert{},
		latency:   map[ports.NodeID]map[ports.NodeID]ports.LatencySample{},
		records:   map[ports.Topic]map[recordSlot]ports.Record{},
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
			// Observer attestations are authored by the observer; the record's
			// NodeID is the SUBJECT. Bind the author key to the observer's own
			// NodeID under the SAME scheme VerifyNodeKey checks (canonicalNodeID
			// = aether). Encoding it any other way makes VerifyNodeKey reject
			// every legitimately-signed attestation, which under an enforcing
			// gate silently drops death-tombstone and liveness observations. A
			// wrong-length key leaves the subject NodeID in place, which the same
			// VerifyNodeKey call then refuses on the key-size check.
			if id, ok := canonicalNodeID(ed25519.PublicKey(r.AuthorPubKey)); ok {
				pr.NodeID = ports.NodeID(id)
			}
		}
		if refused := d.gate(ctx, pr, r.Topic); refused != nil {
			// Enforcing: drop the record before it is journaled or projected.
			// Observing: gate() has already counted + logged and returns nil,
			// so the record falls through to journal + project below.
			return refused
		}
	}
	if _, err := d.journal.Append(ctx, r); err != nil {
		return err
	}
	d.project(r)
	return nil
}

// gate runs the pre-projection trust checks for principal pr publishing topic.
// It counts every checked record and, on a check failure, counts a rejection.
// When enforcing it returns the wrapped error so Ingest drops the record; when
// observing it logs the would-reject and returns nil so the record still
// projects — the mode difference lives ONLY here.
func (d *SwarmDirectory) gate(ctx context.Context, pr ports.Principal, topic ports.Topic) error {
	d.mu.Lock()
	d.checked++
	d.mu.Unlock()
	if err := d.policy.VerifyNodeKey(pr.NodeID, pr.PubKey); err != nil {
		return d.refuse("verify", topic, pr.NodeID, err)
	}
	if err := d.policy.AuthorizePublish(ctx, pr, topic); err != nil {
		return d.refuse("publish", topic, pr.NodeID, err)
	}
	return nil
}

// refuse records a gate failure. In enforce mode it returns the wrapped error;
// in observe mode it logs the would-reject (rate-limited) and returns nil so the
// caller keeps the record. The rejected counter advances in BOTH modes so the
// observe-mode ratio is the evidence for whether an enforcing cut is safe.
func (d *SwarmDirectory) refuse(stage string, topic ports.Topic, id ports.NodeID, err error) error {
	d.mu.Lock()
	n := d.rejected + 1
	d.rejected = n
	d.mu.Unlock()
	if !d.observe {
		return fmt.Errorf("directory: ingest refused (%s): %w", stage, err)
	}
	if n <= 10 || n%100 == 0 {
		log.Printf("[DIR-TRUST-observe] %s topic=%q node=%s: %v (would-reject #%d)",
			stage, topic, short(id), err, n)
	}
	return nil
}

// bumpRejected advances the gate-failure counter without a policy check — used
// by projection paths that reject a record for a non-policy reason.
func (d *SwarmDirectory) bumpRejected() {
	d.mu.Lock()
	d.rejected++
	d.mu.Unlock()
}

// Rejected reports gate failures: records refused (enforce) or would-refuse
// (observe). Read against Checked to judge whether an enforcing cut is safe.
func (d *SwarmDirectory) Rejected() uint64 {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.rejected
}

// Checked reports how many records reached the policy gate (0 with no policy).
func (d *SwarmDirectory) Checked() uint64 {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.checked
}

// project applies one accepted record to the typed views. Tombstones remove.
func (d *SwarmDirectory) project(r ports.Record) {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Raw slot view (KeyOps/Quorum/secrets consumers). Keyed by
	// (NodeID, Key): a node may hold many records on one topic, and keying
	// by NodeID alone would let its keyed records overwrite each other with
	// last-writer-wins.
	slots := d.records[r.Topic]
	if slots == nil {
		slots = map[recordSlot]ports.Record{}
		d.records[r.Topic] = slots
	}
	slots[recordSlot{NodeID: r.NodeID, Key: r.Key}] = r

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
		// ports.Member.Roles is contractually lexicographic (#R-1607 ④).
		// Sorted HERE, at the write, rather than in Member()/Members() — the
		// same idiom this file already uses for reach addresses (sorted by
		// Priority immediately before the only store below), so the verbatim
		// returns on the read side are correct rather than accidental, and
		// the cost is paid once per projection instead of once per read.
		sort.Strings(m.Roles)
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
			// net.JoinHostPort, NOT "%s:%d". An IPv6 literal must be
			// bracketed or the result is not a dial string — and the fleet's
			// private addresses are IPv6 (Fly 6PN, fdaa::/48), so the naive
			// form produced "fdaa::7:41641" for exactly the addresses mesh
			// peers use to reach each other. Found by comparing this
			// projection against the independent LAD adapter, which had used
			// JoinHostPort all along (ladlive.go Reach).
			Address:  net.JoinHostPort(a.Host, strconv.Itoa(int(a.Port))),
			Scope:    a.Scope,
			Priority: transportPriority(a.Transport),
			// Raw tier. Swarm's producer vocabulary IS the normalised one —
			// the transport is an enum, so RawProtocol == Protocol here. That
			// equality is correct and not a missing projection: only the LAD
			// side has a second spelling to preserve.
			RawProtocol: transportString(a.Transport),
			Host:        a.Host,
			Port:        int(a.Port),
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

// Members filters by tenant. Before #R-1455 this returned EVERY tenant's
// members regardless of the caller, which the tenant-implicit port could not
// express — a cross-tenant leak the signature made invisible.
func (d *SwarmDirectory) Members(_ context.Context, tenant ports.Tenant) ([]ports.Member, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	out := make([]ports.Member, 0, len(d.members))
	for _, m := range d.members {
		if ports.Tenant(m.Tenant) != tenant {
			continue
		}
		out = append(out, cloneMember(m))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NodeID < out[j].NodeID })
	return out, nil
}

func (d *SwarmDirectory) Member(_ context.Context, tenant ports.Tenant, id ports.NodeID) (ports.Member, bool, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	m, ok := d.members[id]
	if !ok || ports.Tenant(m.Tenant) != tenant {
		return ports.Member{}, false, nil
	}
	return cloneMember(m), true, nil
}

// RoleAdverts implements ports.RoleEnumerator: every node advertising any
// role, one entry per node.
//
// Swarm carries roles inside the peer record, so unlike the LAD side there is
// no separate role record that could exist without a member — the projection
// is the only source. Roles are already sorted at projection (#R-1607 ④); the
// clone below preserves that and the result is ordered by NodeID so the two
// implementations are comparable.
func (d *SwarmDirectory) RoleAdverts(_ context.Context, tenant ports.Tenant) ([]ports.RoleAdvert, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	out := make([]ports.RoleAdvert, 0, len(d.members))
	for id, m := range d.members {
		if ports.Tenant(m.Tenant) != tenant {
			continue
		}
		if len(m.Roles) == 0 {
			continue
		}
		out = append(out, ports.RoleAdvert{
			NodeID: id,
			Roles:  append([]string(nil), m.Roles...),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NodeID < out[j].NodeID })
	return out, nil
}

func (d *SwarmDirectory) NodesByRole(_ context.Context, tenant ports.Tenant, role string) ([]ports.NodeID, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	var out []ports.NodeID
	for id, m := range d.members {
		if ports.Tenant(m.Tenant) != tenant {
			continue
		}
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

func (d *SwarmDirectory) Reach(_ context.Context, tenant ports.Tenant, id ports.NodeID) ([]ports.ReachAddress, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if m, ok := d.members[id]; ok && ports.Tenant(m.Tenant) != tenant {
		return nil, nil
	}
	return append([]ports.ReachAddress(nil), d.reach[id]...), nil
}

func (d *SwarmDirectory) Latency(_ context.Context, from, to ports.NodeID) (ports.LatencySample, bool, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	s, ok := d.latency[from][to]
	return s, ok, nil
}

func (d *SwarmDirectory) HandlersByName(_ context.Context, tenant ports.Tenant, name string) ([]ports.HandlerAdvert, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	var out []ports.HandlerAdvert
	for _, a := range d.handlers[name] {
		if m, ok := d.members[a.NodeID]; ok && ports.Tenant(m.Tenant) != tenant {
			continue
		}
		out = append(out, a)
	}
	return out, nil
}

// HandlerFQNs implements HandlerEnumerator: the set of handler names this
// directory currently indexes, for shadow-parity comparison-set building.
func (d *SwarmDirectory) HandlerFQNs(_ context.Context, _ ports.Tenant) ([]string, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	out := make([]string, 0, len(d.handlers))
	for fqn := range d.handlers {
		out = append(out, fqn)
	}
	sort.Strings(out)
	return out, nil
}

func (d *SwarmDirectory) RecordsByTopic(_ context.Context, topic ports.Topic) ([]ports.Record, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	slots := d.records[topic]
	out := make([]ports.Record, 0, len(slots))
	for _, r := range slots {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return lessSlot(out[i], out[j]) })
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
		return lessSlot(recs[i], recs[j])
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
		return lessSlot(recs[i], recs[j])
	})
	return fingerprintRecords(recs), nil
}

// Subscribe delegates to the journal — the accepted-record stream IS the
// subscription surface, watermark-resumable with a lossless history→live
// handoff (see journal.FileJournal.Replay).
func (d *SwarmDirectory) Subscribe(ctx context.Context, topics []ports.Topic, from ports.Watermark) (<-chan ports.Record, error) {
	return d.journal.Replay(ctx, from, topics)
}

func cloneMember(m ports.Member) ports.Member {
	m.Roles = append([]string(nil), m.Roles...)
	return m
}

var _ ports.LiveDirectory = (*SwarmDirectory)(nil)

// lessSlot is the canonical record order within a topic: NodeID, then Key.
//
// 🛑 Key IS PART OF THE ORDER, not a nicety. With composite keys one node
// holds several records on a topic, so ordering by NodeID alone leaves their
// relative order to Go's map iteration — and Fingerprint() hashes this exact
// sequence. A fingerprint that reorders run-to-run makes shadow parity
// (§0.5.3 stage 3) fail at random, or worse, mask a real divergence behind
// noise everyone learns to ignore.
//
// Members() deliberately does NOT use this: a member is per-node, so NodeID
// alone is its full identity.
func lessSlot(a, b ports.Record) bool {
	if a.NodeID != b.NodeID {
		return a.NodeID < b.NodeID
	}
	return a.Key < b.Key
}
