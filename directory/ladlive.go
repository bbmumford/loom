/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package directory

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"
	"sync"
	"time"

	lad "github.com/bbmumford/ledger"
	ladcache "github.com/bbmumford/ledger/cache"
	"github.com/bbmumford/loom/ports"
)

// LADDirectory is the transitional ports.LiveDirectory over the LAD
// DirectoryCache — implementation (1) named in ports/directory.go, and the
// side the Swarm projection is compared against during shadow mode
// (§0.5.3 stage 3). Without it the shadow phase has one implementation and
// nothing to compare it to.
//
// It is READ-ONLY over the cache. Records enter LAD through the existing
// gossip/apply paths; this type never writes state, with one deliberate
// exception — OverrideLiveness, which the port defines as a write and which
// the connection reporter needs.
//
// 🛑 THE TWO SIDES DO NOT SHARE A RECORD MODEL, and the cutover depends on
// knowing that. LAD stores a node's identity, roles and reachability as
// THREE typed records on three topics; the Swarm projection carries one
// signed fleet.peer record per node. This adapter therefore projects LAD's
// three onto one loom topic using the composite key (Key = "member" | "role"
// | "reach") — which is what Record.Key exists for, and which keeps all three
// instead of collapsing them last-writer-wins.
//
// The consequence is structural and MUST NOT be papered over: two sides
// holding the SAME FACTS still produce different raw-record sets, so their
// Fingerprints differ by construction. Cross-implementation parity is
// asserted on the TYPED projections (CompareDirectories), never on
// Fingerprint/Snapshot. CompareFingerprints refuses the pairing rather than
// returning a mismatch a lane might read as real divergence — see
// RecordModel.
type LADDirectory struct {
	cache *ladcache.DirectoryCache

	mu        sync.Mutex
	overrides map[ports.NodeID]*time.Timer
	closed    bool
}

// LAD record-model keys: one node's three typed LAD records share the loom
// fleet.peer topic and are separated by composite key.
const (
	ladKeyMember = "member"
	ladKeyRole   = "role"
	ladKeyReach  = "reach"
)

// NewLADDirectory wraps a live DirectoryCache as a ports.LiveDirectory for
// the given tenant. The cache is borrowed, never owned: Close releases only
// this adapter's own resources (liveness-override timers).
// NewLADDirectory wraps a live DirectoryCache as a ports.LiveDirectory. The
// cache is borrowed, never owned: Close releases only this adapter's own
// resources (liveness-override timers).
//
// 🔑 IT TAKES NO TENANT. An earlier version fixed one at construction, which
// #R-1455 ③(b) rejected: a tenant appearing after wiring would silently get no
// adapter — trading a visible hardcode for an invisible lifecycle dependency,
// which is a NEW silent-empty of exactly the class this file spent two rounds
// closing. The tenant is now a parameter of each operation that needs one.
func NewLADDirectory(c *ladcache.DirectoryCache) (*LADDirectory, error) {
	if c == nil {
		return nil, fmt.Errorf("directory: LAD directory needs a cache")
	}
	return &LADDirectory{
		cache:     c,
		overrides: make(map[ports.NodeID]*time.Timer),
	}, nil
}

// ErrTopicNotProjected is returned for a topic this directory cannot see, as
// opposed to one it can see and finds empty. The distinction is the point: a
// consumer asking "have any keys been revoked?" must never receive a confident
// "no" from a source that does not carry key operations at all.
var ErrTopicNotProjected = errors.New("directory: topic not projected by this implementation")

// RecordModel implements RecordModeler: the raw-record shape this directory
// exposes. Two directories may only be compared record-for-record when their
// models agree.
func (d *LADDirectory) RecordModel() string { return RecordModelLAD }

// Close stops the adapter's liveness-override timers. The wrapped cache is
// untouched — it outlives the adapter.
func (d *LADDirectory) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.closed = true
	for id, t := range d.overrides {
		t.Stop()
		delete(d.overrides, id)
	}
	return nil
}

// ---------- ports.LiveDirectory ----------

// Members joins LAD's three identity sources into the typed membership view:
// MemberRecord for identity, RoleRecord for service roles, ReachRecord for
// region. LAD's own Members() already synthesises members from signed reach
// metadata for nodes with no explicit member record, so this inherits that.
func (d *LADDirectory) Members(ctx context.Context, tenant ports.Tenant) ([]ports.Member, error) {
	recs, err := d.cache.Members(ctx, string(tenant))
	if err != nil {
		return nil, fmt.Errorf("directory: LAD members: %w", err)
	}
	roles, err := d.rolesByNode(ctx, tenant)
	if err != nil {
		return nil, err
	}
	reach, err := d.reachByNode(ctx, tenant)
	if err != nil {
		return nil, err
	}

	out := make([]ports.Member, 0, len(recs))
	for _, m := range recs {
		out = append(out, d.member(m, roles, reach))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NodeID < out[j].NodeID })
	return out, nil
}

func (d *LADDirectory) Member(ctx context.Context, tenant ports.Tenant, id ports.NodeID) (ports.Member, bool, error) {
	ms, err := d.Members(ctx, tenant)
	if err != nil {
		return ports.Member{}, false, err
	}
	for _, m := range ms {
		if m.NodeID == id {
			return m, true, nil
		}
	}
	return ports.Member{}, false, nil
}

func (d *LADDirectory) member(m lad.MemberRecord, roles map[string][]string, reach map[string]lad.ReachRecord) ports.Member {
	out := ports.Member{
		NodeID:      ports.NodeID(m.NodeID),
		Tenant:      m.TenantID,
		ServiceName: attrServiceName(m.Attrs),
		Region:      m.Attrs["region"],
	}
	if len(m.Attrs) > 0 {
		out.Attrs = make(map[string]string, len(m.Attrs))
		for k, v := range m.Attrs {
			out.Attrs[k] = v
		}
	}
	if r, ok := roles[m.NodeID]; ok {
		out.Roles = append([]string(nil), r...)
	} else if s := m.Attrs["roles"]; s != "" {
		// Reach-synthesised members carry roles as a comma-separated attr.
		out.Roles = splitAttrList(s)
	}
	last := m.CreatedAt
	if r, ok := reach[m.NodeID]; ok {
		if r.Region != "" {
			out.Region = r.Region
		}
		if r.UpdatedAt.After(last) {
			last = r.UpdatedAt
		}
	}
	if !last.IsZero() {
		out.LastSeenUnixMs = last.UnixMilli()
	}
	sort.Strings(out.Roles)
	return out
}

func (d *LADDirectory) NodesByRole(ctx context.Context, tenant ports.Tenant, role string) ([]ports.NodeID, error) {
	rs, err := d.cache.Roles(ctx, string(tenant), ladcache.RoleQuery{Role: role})
	if err != nil {
		return nil, fmt.Errorf("directory: LAD roles: %w", err)
	}
	out := make([]ports.NodeID, 0, len(rs))
	for _, r := range rs {
		out = append(out, ports.NodeID(r.NodeID))
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out, nil
}

// RoleAdverts implements ports.RoleEnumerator: every node advertising any
// role, one entry per node.
//
// It queries the ROLE records directly (an empty RoleQuery), NOT Members —
// LAD stores roles on their own records, so a node with a role record and no
// member record is invisible to Members and must not be invisible here
// (#M-626). Roles are sorted to match ports.Member.Roles (#R-1607 ④), and the
// result is ordered by NodeID so two directories are comparable.
func (d *LADDirectory) RoleAdverts(ctx context.Context, tenant ports.Tenant) ([]ports.RoleAdvert, error) {
	rs, err := d.cache.Roles(ctx, string(tenant), ladcache.RoleQuery{})
	if err != nil {
		return nil, fmt.Errorf("directory: LAD role adverts: %w", err)
	}
	out := make([]ports.RoleAdvert, 0, len(rs))
	for _, r := range rs {
		roles := append([]string(nil), r.Roles...)
		sort.Strings(roles)
		out = append(out, ports.RoleAdvert{NodeID: ports.NodeID(r.NodeID), Roles: roles})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NodeID < out[j].NodeID })
	return out, nil
}

// Reach flattens the node's signed ReachRecord addresses into priority-ordered
// dial candidates. The reach layer names the Noise transport "udp" while the
// mesh address table names it "noise-udp"; that mapping is normalised here and
// must stay consistent with transportString (plan B.5).
func (d *LADDirectory) Reach(ctx context.Context, tenant ports.Tenant, id ports.NodeID) ([]ports.ReachAddress, error) {
	rs, err := d.cache.Reach(ctx, string(tenant), ladcache.ReachQuery{NodeID: string(id)})
	if err != nil {
		return nil, fmt.Errorf("directory: LAD reach: %w", err)
	}
	var out []ports.ReachAddress
	for _, r := range rs {
		for _, a := range r.Addresses {
			proto := normaliseReachProto(a.Proto)
			out = append(out, ports.ReachAddress{
				Protocol: proto,
				Address:  net.JoinHostPort(a.Host, strconv.Itoa(a.Port)),
				Scope:    a.Scope,
				Priority: reachPriority(proto),
				NATClass: r.NATType,
				// Raw tier: the producer's own vocabulary, unnormalised.
				//
				// 🛑 normaliseReachProto RENAMES "udp" -> "noise-udp". A
				// consumer matching the reach layer's own name — forwarder.go
				// selects 6PN candidates with `Proto != "udp"` — matches
				// NOTHING against Protocol, and nothing errors: direct
				// dialling simply stops and every peer falls back to relay.
				// Compare against RawProtocol, never Protocol, when you mean
				// the producer's name.
				RawProtocol: a.Proto,
				Host:        a.Host,
				Port:        a.Port,
			})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Priority != out[j].Priority {
			return out[i].Priority < out[j].Priority
		}
		return out[i].Address < out[j].Address
	})
	return out, nil
}

// Latency reads the freshest observation the observing node published. LAD
// indexes latency by observer, so this is a direct lookup — no composite key
// needed on the LAD side.
func (d *LADDirectory) Latency(_ context.Context, from, to ports.NodeID) (ports.LatencySample, bool, error) {
	for _, r := range d.cache.Latency(string(from)) {
		if r.ToNode != string(to) {
			continue
		}
		return ports.LatencySample{
			From:             from,
			To:               to,
			RTTMs:            float64(r.RTTMs),
			ObservedAtUnixMs: r.MeasuredAt.UnixMilli(),
		}, true, nil
	}
	return ports.LatencySample{}, false, nil
}

// HandlersByName answers which nodes advertise an RPC handler FQN. LAD stores
// handler metadata on RoleRecord (and on MemberRecord for nodes that publish
// it there), so both are consulted.
func (d *LADDirectory) HandlersByName(ctx context.Context, tenant ports.Tenant, name string) ([]ports.HandlerAdvert, error) {
	rs, err := d.cache.Roles(ctx, string(tenant), ladcache.RoleQuery{Handler: name})
	if err != nil {
		return nil, fmt.Errorf("directory: LAD handler roles: %w", err)
	}
	var out []ports.HandlerAdvert
	for _, r := range rs {
		for _, h := range r.Handlers {
			if h.Name != name {
				continue
			}
			out = append(out, ports.HandlerAdvert{
				Name:   name,
				NodeID: ports.NodeID(r.NodeID),
				Roles:  append([]string(nil), r.Roles...),
			})
			break
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].NodeID < out[j].NodeID })
	return out, nil
}

// HandlerFQNs implements HandlerEnumerator so a LAD↔Swarm comparison compares
// the handler axis instead of degrading it.
func (d *LADDirectory) HandlerFQNs(ctx context.Context, tenant ports.Tenant) ([]string, error) {
	rs, err := d.cache.Roles(ctx, string(tenant), ladcache.RoleQuery{})
	if err != nil {
		return nil, fmt.Errorf("directory: LAD handler enumeration: %w", err)
	}
	set := map[string]bool{}
	for _, r := range rs {
		for _, h := range r.Handlers {
			if h.Name != "" {
				set[h.Name] = true
			}
		}
	}
	ms, err := d.cache.Members(ctx, string(tenant))
	if err != nil {
		return nil, fmt.Errorf("directory: LAD handler enumeration (members): %w", err)
	}
	for _, m := range ms {
		for _, h := range m.Handlers {
			if h.Name != "" {
				set[h.Name] = true
			}
		}
	}
	out := make([]string, 0, len(set))
	for n := range set {
		out = append(out, n)
	}
	sort.Strings(out)
	return out, nil
}

// RecordsByTopic is the escape hatch for consumers without a typed projection
// (KeyOps, Quorum). Topics are the LAD names ("member", "role", "reach",
// "keyops", "quorum", "latency"); the loom fleet.peer topic maps onto LAD's
// member/role/reach trio, separated by composite key.
func (d *LADDirectory) RecordsByTopic(_ context.Context, topic ports.Topic) ([]ports.Record, error) {
	if ladDeclaredButUnprojected[lad.Topic(topic)] {
		return nil, fmt.Errorf("%w: %q — LAD declares this topic but its cache "+
			"does not retain it (applyCore ignores it, ChangesSinceHLC has no branch), "+
			"so an empty result would be indistinguishable from a measured absence",
			ErrTopicNotProjected, topic)
	}
	var out []ports.Record
	for _, lt := range ladTopicsFor(topic) {
		recs, err := d.cache.ChangesSinceHLC(lt, 0)
		if err != nil {
			return nil, fmt.Errorf("directory: LAD records %q: %w", lt, err)
		}
		for _, r := range recs {
			out = append(out, ladToPortRecord(r))
		}
	}
	sort.Slice(out, func(i, j int) bool { return lessSlot(out[i], out[j]) })
	return out, nil
}

// OverrideLiveness applies the live-session liveness override.
//
// 🔴 LAD's SetGossipLivenessOverride has NO expiry — it is set until something
// clears it. The port's contract is "ttlMs bounds the override; 0 clears it",
// and an unbounded override fails OPEN: a peer whose transport dropped stays
// permanently exempt from liveness eviction. The TTL is therefore implemented
// HERE, by an adapter-owned timer that clears the cache override when it
// expires. Dropping the ttlMs argument on the floor would have satisfied the
// signature and silently disabled eviction for every overridden node.
func (d *LADDirectory) OverrideLiveness(id ports.NodeID, alive bool, ttlMs int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if t, ok := d.overrides[id]; ok {
		t.Stop()
		delete(d.overrides, id)
	}
	if d.closed || ttlMs <= 0 || !alive {
		d.cache.SetGossipLivenessOverride(string(id), false)
		return
	}
	d.cache.SetGossipLivenessOverride(string(id), true)
	d.overrides[id] = time.AfterFunc(time.Duration(ttlMs)*time.Millisecond, func() {
		d.mu.Lock()
		defer d.mu.Unlock()
		delete(d.overrides, id)
		d.cache.SetGossipLivenessOverride(string(id), false)
	})
}

// Snapshot returns this side's deterministic view. See RecordModel: the
// Fingerprint here is comparable only against another LAD-model directory.
func (d *LADDirectory) Snapshot(ctx context.Context) (ports.DirectorySnapshot, error) {
	recs, wm, err := d.allRecords(ctx)
	if err != nil {
		return ports.DirectorySnapshot{}, err
	}
	return ports.DirectorySnapshot{
		Watermark:   wm,
		Fingerprint: fingerprintRecords(recs),
		Records:     recs,
	}, nil
}

func (d *LADDirectory) Fingerprint(ctx context.Context) ([32]byte, error) {
	recs, _, err := d.allRecords(ctx)
	if err != nil {
		return [32]byte{}, err
	}
	return fingerprintRecords(recs), nil
}

// allRecords materialises every topic's records in canonical order, and the
// highest watermark seen.
//
// ⚠ The LAD watermark domain is NOT the Swarm journal's. LAD derives ordering
// from each record's HLC when present and its timestamp otherwise (see
// ChangesSinceHLC, whose typed-topic branches compare UnixNano and whose
// LamportClock is documented "best-effort: exact per-record LC not stored on
// typed records"). A Watermark from this side is resumable against this side
// only; it is not a journal position and must never be handed to the Swarm
// directory as one.
func (d *LADDirectory) allRecords(_ context.Context) ([]ports.Record, ports.Watermark, error) {
	var out []ports.Record
	var wm ports.Watermark
	for _, lt := range ladProjectedTopics {
		recs, err := d.cache.ChangesSinceHLC(lt, 0)
		if err != nil {
			return nil, 0, fmt.Errorf("directory: LAD snapshot %q: %w", lt, err)
		}
		for _, r := range recs {
			pr := ladToPortRecord(r)
			out = append(out, pr)
			if w := ladWatermark(r); w > wm {
				wm = w
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Topic != out[j].Topic {
			return out[i].Topic < out[j].Topic
		}
		return lessSlot(out[i], out[j])
	})
	return out, wm, nil
}

// ---------- projection helpers ----------

func (d *LADDirectory) rolesByNode(ctx context.Context, tenant ports.Tenant) (map[string][]string, error) {
	rs, err := d.cache.Roles(ctx, string(tenant), ladcache.RoleQuery{})
	if err != nil {
		return nil, fmt.Errorf("directory: LAD roles: %w", err)
	}
	out := make(map[string][]string, len(rs))
	for _, r := range rs {
		out[r.NodeID] = r.Roles
	}
	return out, nil
}

func (d *LADDirectory) reachByNode(ctx context.Context, tenant ports.Tenant) (map[string]lad.ReachRecord, error) {
	rs, err := d.cache.Reach(ctx, string(tenant), ladcache.ReachQuery{})
	if err != nil {
		return nil, fmt.Errorf("directory: LAD reach: %w", err)
	}
	out := make(map[string]lad.ReachRecord, len(rs))
	for _, r := range rs {
		out[r.NodeID] = r
	}
	return out, nil
}

var (
	_ ports.LiveDirectory = (*LADDirectory)(nil)
	_ HandlerEnumerator   = (*LADDirectory)(nil)
	_ RecordModeler       = (*LADDirectory)(nil)
)
