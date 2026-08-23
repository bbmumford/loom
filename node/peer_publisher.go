/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"log"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/bbmumford/loom/pkg/rpc"
	"github.com/bbmumford/swarm"
	swarmpb "github.com/bbmumford/swarm/proto/pb"
	"google.golang.org/protobuf/proto"
)

// PeerPublisher publishes this fleet endpoint's unified PeerRecord to the
// "fleet.peer" swarm topic. The PeerRecord carries identity + addresses +
// RPC handlers + capabilities + grade — all atomic per publish.
//
// Receivers index the records via role_table.go (role -> nodes) and
// address_table.go (NodeID -> dial candidates). One PeerPublisher carries
// both the reach and the role content in a single record.
type PeerPublisher struct {
	node swarm.Node
	rt   *Runtime
	reg  *rpc.Registry

	mu          sync.Mutex
	stopOnce    sync.Once // Stop is exported; concurrent callers must not double-close
	roles       []string
	addresses   []*swarmpb.Address
	maxGrade    swarmpb.Grade
	serviceName string
	region      string
	// extras is the free-form capability bag published in
	// Capabilities.extras. Copied on set and on read so a caller cannot
	// mutate what a concurrent publish is marshalling.
	extras      map[string]string
	republishCh chan struct{}
	// lastSatEmitted is the saturation boolean carried in the most recent
	// publish. The pressure poll compares it against the live state to
	// republish promptly on a transition (the 5-min TTL is too slow to
	// advertise/clear saturation), without publishing on every poll tick.
	lastSatEmitted bool

	// rtts supplies per-address RTT samples that populate
	// Address.rtt_estimate_ms in every published record. nil = leave
	// rtt_estimate_ms at whatever the caller passed to SetAddresses.
	rtts AddressRTTProvider

	epoch atomic.Uint64

	stopCh chan struct{}
}

// SetServiceName updates the operator-facing service identifier
// emitted in every PeerRecord (e.g. "help-orbtr-io", "node-hstles-com").
// Carried inside Capabilities.tags as "service=<name>" so the typed
// proto stays stable while topology readers can still pick it up.
func (p *PeerPublisher) SetServiceName(name string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.serviceName = name
	p.PublishNow()
}

// SetRegion updates the geo/region identifier (e.g. "iad", "syd").
// Same Capabilities.tags carrier as service_name. Used by the topology
// builder when the local connection-reporter can't fill in region for a
// distant peer.
// SetCapabilityExtras replaces the free-form capability bag and republishes.
//
// The bag rides in the signed PeerRecord's `Capabilities.extras`, so peers
// receive it through the same authenticated channel as roles and addresses —
// a consumer that trusts the record's roles can trust these to the same degree.
//
// ⚠ Records are capped at 16 KB. An oversized bag makes the whole record
// unpublishable, which would take roles and addresses down with it — so this
// drops the bag and keeps the node advertising rather than failing the publish.
// Silence about capabilities is recoverable; disappearing from the mesh is not.
func (p *PeerPublisher) SetCapabilityExtras(extras map[string]string) {
	copied := make(map[string]string, len(extras))
	size := 0
	for key, value := range extras {
		size += len(key) + len(value) + 2
		copied[key] = value
	}
	if size > maxCapabilityExtrasBytes {
		log.Printf("[SWARM] capability extras %d B exceeds the %d B budget; "+
			"dropping them so the record still publishes", size, maxCapabilityExtrasBytes)
		copied = nil
	}
	p.mu.Lock()
	p.extras = copied
	p.mu.Unlock()
	p.PublishNow()
}

// maxCapabilityExtrasBytes bounds the free-form bag well inside the 16 KB
// Record ceiling, leaving room for addresses, handlers and the signature.
const maxCapabilityExtrasBytes = 4096

func (p *PeerPublisher) SetRegion(region string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.region = region
	p.PublishNow()
}

// NewPeerPublisher creates a PeerPublisher. Call Start to begin republishing
// on input changes; the PeerPublisher otherwise holds no state.
func NewPeerPublisher(node swarm.Node, rt *Runtime, reg *rpc.Registry) *PeerPublisher {
	return &PeerPublisher{
		node:        node,
		rt:          rt,
		reg:         reg,
		republishCh: make(chan struct{}, 1),
		stopCh:      make(chan struct{}),
	}
}

// SetRegistry updates the rpc.Registry the publisher reads handler FQNs
// from. Needed because InitSwarm is idempotent — the swarm/publisher are
// constructed once at Runtime.Initialize with reg=nil (no registry yet),
// then the endpoint later calls PublishRPCHandlersToLAD with its
// registry. Without this setter, the late registry would be discarded
// and every PeerRecord would carry an empty handler list. Triggers an
// immediate republish so the new handler set propagates without waiting
// for the 5-minute TTL refresh.
func (p *PeerPublisher) SetRegistry(reg *rpc.Registry) {
	p.mu.Lock()
	p.reg = reg
	p.mu.Unlock()
	p.PublishNow()
}

// Start begins the publisher loop. Republishes are triggered by:
// - input changes (roles, addresses, grade) via PublishNow signals
// - periodic refresh every 5 minutes (TTL refresh)
// - explicit calls to PublishNow
//
// The run-loop is spawned via Runtime.Go so it is enrolled in rt.wg
// (drained by Shutdown), wrapped in a panic recover, and named in any
// crash log. Falls back to a bare `go` only when p.rt is nil, which no
// production construction produces.
func (p *PeerPublisher) Start(ctx context.Context) {
	if p.rt != nil {
		p.rt.Go("swarm.peer_publisher.run", func() { p.run(ctx) })
	} else {
		go p.run(ctx)
	}
	p.PublishNow() // initial publish
}

// Stop halts the publisher loop. Idempotent and safe under concurrent callers.
//
// 🔴 THE sync.Once REPLACES A CHECK-THEN-ACT GUARD THAT DID NOT HOLD. The
// previous select/default read p.stopCh and closed it only if the read did not
// succeed. Sequentially that is correct; concurrently two callers can both take
// the default branch before either close lands, and the second close panics
// with "close of closed channel". Stop is exported, so the number of concurrent
// callers is not this file's to bound.
func (p *PeerPublisher) Stop() {
	p.stopOnce.Do(func() {
		close(p.stopCh)
	})
}

// PublishNow signals an immediate republish on the next loop iteration.
// Coalesces multiple rapid calls into a single publish.
func (p *PeerPublisher) PublishNow() {
	select {
	case p.republishCh <- struct{}{}:
	default:
		// channel already pending; coalesced
	}
}

// SetRoles updates the role set advertised by this publisher.
func (p *PeerPublisher) SetRoles(roles []string) {
	p.mu.Lock()
	p.roles = append(p.roles[:0], roles...)
	sort.Strings(p.roles)
	p.mu.Unlock()
	// Drop the affinity cache on the connection manager so the next
	// Rebalance recomputes per-peer dispatch-target bonuses against the
	// new local-role set. Safe to call with no locks held.
	if p.rt != nil && p.rt.connMgr != nil {
		p.rt.connMgr.InvalidateLocalRoleCache()
	}
	p.PublishNow()
}

// Roles returns a defensive copy of the locally-advertised role set.
// Used by the role-affinity scorer to determine which peer roles are
// "unique remote capability" vs "redundancy with us". Empty slice when
// no roles have been set (boot phase before PublishRPCHandlersToLAD).
func (p *PeerPublisher) Roles() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.roles) == 0 {
		return nil
	}
	out := make([]string, len(p.roles))
	copy(out, p.roles)
	return out
}

// SetAddresses updates the address set advertised by this publisher.
// Copies the input slice so subsequent mutations on the caller's side
// don't affect what's published.
func (p *PeerPublisher) SetAddresses(addrs []*swarmpb.Address) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.addresses = append(p.addresses[:0], addrs...)
	p.PublishNow()
}

// Addresses returns a snapshot of the currently-advertised address set.
// Callers that need to add or remove a single entry should read,
// modify, and call SetAddresses with the new slice — the publisher
// has no in-place mutation API to keep the proto.Address messages
// safe to share with publishOnce.
func (p *PeerPublisher) Addresses() []*swarmpb.Address {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]*swarmpb.Address, len(p.addresses))
	copy(out, p.addresses)
	return out
}

// AddressRTTProvider supplies the most recent RTT observation for an
// advertised local address. Used by publishOnce to populate Address
// rtt_estimate_ms so peers can pick the lowest-latency address from
// the set instead of probing every one. Returns 0 when no recent
// measurement exists; the publisher leaves rtt_estimate_ms at zero in
// that case so receivers treat it as "unknown".
type AddressRTTProvider interface {
	AddressRTTMs(transport swarmpb.Address_Transport, host string, port uint32) uint32
}

// SetRTTProvider wires the provider that fills per-address RTT samples
// into every published PeerRecord. Idempotent.
func (p *PeerPublisher) SetRTTProvider(provider AddressRTTProvider) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rtts = provider
}

// SetMaxGrade updates the advertised max-grade.
func (p *PeerPublisher) SetMaxGrade(g swarmpb.Grade) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.maxGrade = g
	p.PublishNow()
}

// State is the mutable PeerRecord input set exposed to Update callers.
// Mirrors the publisher's internal fields one-to-one; the Update closure
// mutates this struct directly and the publisher copies the values back
// under lock before a single PublishNow signal. Callers that only need
// to change one field should keep using the SetX helpers — Update exists
// to coalesce a multi-field change (e.g. roles + grade + service +
// region at boot) into a single publish.
type State struct {
	Roles       []string
	Addresses   []*swarmpb.Address
	MaxGrade    swarmpb.Grade
	ServiceName string
	Region      string
}

// Update applies a batched mutation to the publisher's input state.
// The closure runs under the publisher lock; mutate s in place. A
// single PublishNow fires after the closure returns, so the receiver
// indexes one PeerRecord that reflects every change rather than four
// in rapid succession.
//
// Roles are sorted on store to keep the published list deterministic
// (matches SetRoles' behaviour). Addresses copy the slice header so
// later caller mutations don't leak into the publisher.
func (p *PeerPublisher) Update(fn func(s *State)) {
	if fn == nil {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	s := State{
		Roles:       append([]string(nil), p.roles...),
		Addresses:   append([]*swarmpb.Address(nil), p.addresses...),
		MaxGrade:    p.maxGrade,
		ServiceName: p.serviceName,
		Region:      p.region,
	}
	fn(&s)
	p.roles = append(p.roles[:0], s.Roles...)
	sort.Strings(p.roles)
	p.addresses = append(p.addresses[:0], s.Addresses...)
	p.maxGrade = s.MaxGrade
	p.serviceName = s.ServiceName
	p.region = s.Region
	p.PublishNow()
}

func (p *PeerPublisher) run(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	// Pressure poll: bounds advertise/clear latency for the saturation bit to
	// 30s without publishing on every tick (it republishes only on a
	// transition). The flat TTL refresh still rides the 5-min ticker.
	satPoll := time.NewTicker(30 * time.Second)
	defer satPoll.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.stopCh:
			return
		case <-ticker.C:
			p.publishOnce()
		case <-p.republishCh:
			p.publishOnce()
		case <-satPoll.C:
			p.maybeRepublishOnSaturationChange()
		}
	}
}

// maybeRepublishOnSaturationChange republishes only when the local saturation
// boolean has flipped since the last publish, so the bit advertises/clears
// within the 30s poll window without adding steady-state publish traffic.
func (p *PeerPublisher) maybeRepublishOnSaturationChange() {
	if p.rt == nil {
		return
	}
	sat, _ := p.rt.localSaturation()
	p.mu.Lock()
	changed := sat != p.lastSatEmitted
	p.lastSatEmitted = sat
	p.mu.Unlock()
	if changed {
		p.publishOnce()
	}
}

func (p *PeerPublisher) publishOnce() {
	log.Printf("[SWARM] publishOnce: entry")
	p.mu.Lock()
	roles := append([]string(nil), p.roles...)
	addrs := append([]*swarmpb.Address(nil), p.addresses...)
	maxGrade := p.maxGrade
	serviceName := p.serviceName
	region := p.region
	rtts := p.rtts
	// Copied under the lock like every other field here: the marshal below
	// runs outside it, and proto.Marshal over a map another goroutine is
	// writing is a data race, not merely a stale read.
	var extras map[string]string
	if len(p.extras) > 0 {
		extras = make(map[string]string, len(p.extras))
		for key, value := range p.extras {
			extras[key] = value
		}
	}
	// Snapshot reg under the lock so SetRegistry (called from
	// PublishRPCHandlersToLAD after Runtime.Initialize wired the
	// publisher with reg=nil) doesn't race the publishOnce reader.
	reg := p.reg
	p.mu.Unlock()

	// Stamp the freshest RTT observation onto each advertised address
	// so peers can pick the lowest-latency endpoint without an extra
	// probe. We rebuild the Address proto here rather than mutating
	// the cached value because proto messages carry a MessageState
	// lock and reused mutation would race the next publishOnce.
	if rtts != nil {
		stamped := make([]*swarmpb.Address, len(addrs))
		for i, a := range addrs {
			if a == nil {
				continue
			}
			rtt := rtts.AddressRTTMs(a.Transport, a.Host, a.Port)
			if rtt == 0 {
				stamped[i] = a
				continue
			}
			stamped[i] = &swarmpb.Address{
				Transport:     a.Transport,
				Host:          a.Host,
				Port:          a.Port,
				Sni:           a.Sni,
				RttEstimateMs: rtt,
				RegionHint:    a.RegionHint,
				Scope:         a.Scope,
			}
		}
		addrs = stamped
	}

	// Auto-include "anchor" role if anchor-capable.
	if p.rt != nil && p.rt.isAnchorCapable() {
		hasAnchor := false
		for _, r := range roles {
			if r == "anchor" {
				hasAnchor = true
				break
			}
		}
		if !hasAnchor {
			roles = append(roles, "anchor")
			sort.Strings(roles)
		}
	}

	// Gather handler FQNs from rpc.Registry (snapshot taken under lock
	// above — reg may be nil when Runtime.Initialize wired the publisher
	// before any handler registry was published).
	var handlers []string
	if reg != nil {
		registered := reg.All() // sorted by FQN
		handlers = make([]string, 0, len(registered))
		for _, handler := range registered {
			handlers = append(handlers, handler.FQN())
		}
	}

	// Operator-facing metadata (service name + region) rides Capabilities.tags.
	// Topology builders read these to identify peers we're not directly
	// connected to. The "service=" / "region=" convention is a stable
	// string contract — receivers parse on the "=" boundary.
	var tags []string
	if serviceName != "" {
		tags = append(tags, "service="+serviceName)
	}
	if region != "" {
		tags = append(tags, "region="+region)
	}

	// conn_count: how many peers this node currently holds an open session
	// with. Receivers feed it into ConnectionMap via the fleet.peer ingest,
	// and ConnectionMap.IsHotspot is what adjustForGlobalBalance consults to
	// steer new connections away from a node already carrying more than twice
	// the mesh average. With no writer the map stays empty, MeshAverage()
	// answers 0, IsHotspot answers false for every peer, and the damping
	// branch never reduces a target.
	//
	// Computed here, on the periodic republish, rather than pushed through a
	// setter: a setter would republish on every connect and disconnect, and
	// SetCapabilityExtras replaces the whole extras bag that reach_adapter
	// owns. ActivePeers takes m.mu and this runs after p.mu is released, so
	// the two locks are never held together.
	if p.rt != nil && p.rt.connMgr != nil {
		tags = append(tags, connCountMetaKey+"="+strconv.Itoa(len(p.rt.connMgr.ActivePeers())))
	}

	// nat_class: peers consume this via DecodeNATBehaviour to decide
	// hole-punch method (PunchDirect / PunchPortPrediction / ...) at
	// dial time without waiting for the per-call PunchRequest payload.
	// LAD bridge already reads "nat_class=" from Tags; without this
	// writer the field stays empty in every receiver's view and the
	// punch picker falls back to its conservative default for every
	// dial. encodeNATBehaviour returns "" when the classifier hasn't
	// finished — the publish call is harmless either way.
	if p.rt != nil {
		if enc := encodeNATBehaviour(p.rt.NATBehaviour()); enc != "" {
			tags = append(tags, natClassMetaKey+"="+enc)
		}
	}

	// http_port: receivers consume this via peerHTTPPort to dial the
	// peer's same-org 6PN-direct WebSocket on its actual HTTP port
	// instead of falling back to the hard-coded defaultMeshHTTPPort.
	// All current fleet endpoints listen on 8080, so the fallback hides
	// the loss today; advertising the real port future-proofs against
	// any endpoint that uses a non-default port. meshHTTPPort reads
	// $PORT first so the publish reflects the actual bind.
	if hp := meshHTTPPort(); hp != "" && hp != defaultMeshHTTPPort {
		tags = append(tags, "http_port="+hp)
	}

	// saturated / backoff_until: advertise connection-budget pressure so peers
	// stop opening fresh paths to us. The bit is recomputed here (not cached on
	// the struct), so it is automatically correct on the 5-min TTL refresh and
	// on every PublishNow. backoff_until is a flat deadline re-stamped each
	// republish while still saturated.
	if p.rt != nil {
		if sat, until := p.rt.localSaturation(); sat {
			tags = append(tags, "saturated=1")
			tags = append(tags, "backoff_until="+strconv.FormatInt(until, 10))
		}
	}

	// Guard the p.rt deref. run/Start explicitly support p.rt == nil
	// (bare-goroutine fallback) and publishOnce nil-checks p.rt in three places,
	// but this NodeId read did not — so a publisher constructed with rt==nil
	// panicked on its first publish. Production always passes a Runtime, so this
	// only affects that fallback / test paths.
	// The identity nil-check is the second half of . The original fix
	// guarded p.rt and stopped there, so a publisher holding a HALF-BUILT
	// Runtime — non-nil but with identity not yet assigned — still panicked on
	// its first publish, which is exactly the fallback/test shape the comment
	// above says the guard exists to protect. Production assigns identity
	// during Initialize before swarm_integration.go:100 constructs the
	// publisher, so this changes no live behaviour.
	var nodeIDBytes []byte
	if p.rt != nil && p.rt.identity != nil {
		nodeIDBytes = []byte(p.rt.identity.NodeID)
	}
	// Build the PeerRecord
	rec := &swarmpb.PeerRecord{
		NodeId:      nodeIDBytes,
		Epoch:       p.epoch.Add(1),
		IssuedAt:    time.Now().UnixNano(),
		Addresses:   addrs,
		RpcHandlers: handlers,
		Grade:       maxGrade,
		MaxGrade:    maxGrade,
		Capabilities: &swarmpb.Capabilities{
			Roles:  roles,
			Tags:   tags,
			Extras: extras,
		},
	}

	body, err := proto.Marshal(rec)
	if err != nil {
		log.Printf("[SWARM] publishOnce: proto.Marshal FAILED: %v", err)
		return
	}
	if p.node == nil {
		log.Printf("[SWARM] publishOnce: WARNING node nil — skipping publish")
		return
	}
	pubErr := p.node.Publish(topicFleetPeer, body)
	log.Printf("[SWARM] publishOnce: Publish ok=%v topic=%s bodySize=%d roles=%v addrs=%d handlers=%d service=%s region=%s",
		pubErr == nil, string(topicFleetPeer), len(body), roles, len(addrs), len(handlers), serviceName, region)
}

// topicFleetPeer is the swarm topic that carries fleet PeerRecords.
const topicFleetPeer = swarm.Topic("fleet.peer")
