/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"log"
	"sort"
	"sync"

	"github.com/bbmumford/route"
	"github.com/bbmumford/swarm"
	swarmpb "github.com/bbmumford/swarm/proto/pb"
	"google.golang.org/protobuf/proto"
)

// AddressTable indexes PeerRecords by NodeID and returns ordered dial
// candidates for peer_connections.go::pickPath. It subscribes to the
// swarm "fleet.peer" topic and maintains an in-memory map of NodeID ->
// []DialCandidate.
//
// Dial order: transport preference (NOISE_UDP > WEBSOCKET > GRPC > HTTP)
// then RTT estimate ascending.
//
// When a Router is wired via SetRouter, AddressTable notifies the route
// engine on first sight of a peer (Advertise) and on tombstone (Withdraw).
// This closes the gap where a peer learned via gossip — but with no live
// session yet — would otherwise stay unrouted: SessionHook only fires on
// session establishment, leaving gossip-only peers invisible to the
// route engine until they happened to dial in.
type AddressTable struct {
	mu     sync.RWMutex
	byNode map[string][]DialCandidate
	unsub  swarm.Unsubscribe

	routerMu sync.RWMutex
	router   route.Router

	// wakeCb fires after onRecord stores a PeerRecord that added a
	// Tier-0 (6PN ULA noise-UDP) address for the peer. Wired by
	// ConnectionManager via SetWakeCallback so the upgrade walker can
	// run immediately when a fresh same-org private endpoint shows
	// up rather than waiting for the next 30s ticker. Kept as an
	// opaque func so AddressTable doesn't import the node package
	// and create a cycle.
	wakeCbMu sync.RWMutex
	wakeCb   func()
}

// DialCandidate is one dial address for a peer.
type DialCandidate struct {
	NodeID    string
	Transport string // "noise-udp" | "websocket" | "grpc" | "http"
	Host      string
	Port      uint32
	SNI       string
	RTTEstMs  uint32
	Region    uint32
	// Scope advertises the network reachability class — "public" or
	// "private". Receivers use Scope:"private" entries to route via
	// the same-fabric private network (e.g. Fly 6PN) when the origin
	// prefix matches, and Scope:"public" entries for cross-fabric or
	// fallback dials. Empty values are treated as "public" for safety.
	Scope string
	// Saturated / BackoffUntil carry the peer-level saturation signal from
	// the PeerRecord's Capabilities.Tags. They are identical across all of a
	// peer's candidates (saturation is a node property, not a per-address
	// one); stamping them here lets the dial path read the bit without a
	// second RoleTable lookup.
	Saturated    bool
	BackoffUntil int64
}

// NewAddressTable creates an AddressTable subscribed to the given swarm
// Node's "fleet.peer" topic.
func NewAddressTable(node swarm.Node) (*AddressTable, error) {
	at := &AddressTable{
		byNode: make(map[string][]DialCandidate),
	}
	if node == nil {
		return at, nil
	}
	unsub, err := node.Subscribe(topicFleetPeer, at.onRecord)
	if err != nil {
		return nil, err
	}
	at.unsub = unsub
	return at, nil
}

// Close unsubscribes from the swarm.
func (a *AddressTable) Close() {
	if a.unsub != nil {
		a.unsub()
		a.unsub = nil
	}
}

// SetRouter wires a route.Router so subsequent onRecord calls emit
// Advertise on first sight of a peer and Withdraw on tombstone. Pass
// nil to detach. Safe to call concurrently with onRecord — the router
// handle is guarded by its own RWMutex so PeerRecord ingest never
// blocks on route engine state.
//
// Callers (route_integration.go InitRoute) must also seed-Advertise
// every existing entry after SetRouter — onRecord only emits on the
// transition from "unseen" to "seen", so entries indexed before the
// router was wired would otherwise stay unrouted.
func (a *AddressTable) SetRouter(r route.Router) {
	a.routerMu.Lock()
	a.router = r
	a.routerMu.Unlock()
}

// Router returns the currently-wired route.Router, or nil.
func (a *AddressTable) Router() route.Router {
	a.routerMu.RLock()
	defer a.routerMu.RUnlock()
	return a.router
}

// SetWakeCallback registers a function the AddressTable invokes after
// indexing a PeerRecord that delivered a Tier-0 (6PN ULA noise-UDP)
// address — i.e. an address with Scope="private" + Transport=NOISE_UDP.
// The expected wiring is ConnectionManager.SignalWalkerWake so the
// proactive upgrade walker fires immediately when a same-org private
// endpoint becomes dialable, rather than waiting up to
// upgradeWalkInterval (30s) for the next ticker.
//
// The callback must be non-blocking and goroutine-safe — onRecord runs
// on the swarm subscriber dispatcher, and a blocking callback would
// stall all subsequent fleet.peer records. SignalWalkerWake meets both
// constraints (non-blocking select on a buffered channel).
//
// Pass nil to detach. Safe to call concurrently with onRecord — the
// callback handle is guarded by its own RWMutex so PeerRecord ingest
// never blocks on subscriber registration state.
func (a *AddressTable) SetWakeCallback(cb func()) {
	a.wakeCbMu.Lock()
	a.wakeCb = cb
	a.wakeCbMu.Unlock()
}

// wakeCallback returns the currently-registered wake callback or nil.
func (a *AddressTable) wakeCallback() func() {
	a.wakeCbMu.RLock()
	defer a.wakeCbMu.RUnlock()
	return a.wakeCb
}

// Get returns the dial candidates for a peer, ordered by transport
// preference then RTT estimate.
func (a *AddressTable) Get(nodeID string) []DialCandidate {
	a.mu.RLock()
	defer a.mu.RUnlock()
	cands := a.byNode[nodeID]
	out := make([]DialCandidate, len(cands))
	copy(out, cands)
	return out
}

// All returns the union of every known peer's dial candidates.
func (a *AddressTable) All() map[string][]DialCandidate {
	a.mu.RLock()
	defer a.mu.RUnlock()
	out := make(map[string][]DialCandidate, len(a.byNode))
	for k, v := range a.byNode {
		cp := make([]DialCandidate, len(v))
		copy(cp, v)
		out[k] = cp
	}
	return out
}

func (a *AddressTable) onRecord(r swarm.Record) error {
	nodeID := string(r.NodeID)
	if r.Tombstone {
		log.Printf("[SWARM] AddressTable.onRecord: tombstone node=%s", truncID(nodeID))
		a.mu.Lock()
		_, existed := a.byNode[nodeID]
		delete(a.byNode, nodeID)
		a.mu.Unlock()
		// Mirror the Address/Route table coupling on the way out so the
		// route engine retracts the path as soon as gossip retires the
		// peer. Only emit when we actually had the entry to retract —
		// duplicate-tombstone churn shouldn't generate spurious
		// Withdrawals.
		if existed {
			if rtr := a.Router(); rtr != nil {
				_ = rtr.Withdraw(route.NodeID(nodeID), "")
			}
		}
		return nil
	}
	pr := &swarmpb.PeerRecord{}
	if err := proto.Unmarshal(r.Body, pr); err != nil {
		log.Printf("[SWARM] AddressTable.onRecord: proto.Unmarshal FAILED node=%s: %v", truncID(nodeID), err)
		return nil
	}
	// The saturation bit is peer-level — parse it once and stamp every
	// candidate. onRecord otherwise discards Capabilities; this extracts the
	// single bit the dial path needs while leaving the heavy projection to
	// RoleTable.
	var saturated bool
	var backoffUntil int64
	if pr.Capabilities != nil {
		saturated, backoffUntil = parseSaturationTags(pr.Capabilities.Tags)
	}
	cands := make([]DialCandidate, 0, len(pr.Addresses))
	for _, addr := range pr.Addresses {
		if addr == nil {
			continue
		}
		cands = append(cands, DialCandidate{
			NodeID:       nodeID,
			Transport:    transportString(addr.Transport),
			Host:         addr.Host,
			Port:         addr.Port,
			SNI:          addr.Sni,
			RTTEstMs:     addr.RttEstimateMs,
			Region:       addr.RegionHint,
			Scope:        addr.Scope,
			Saturated:    saturated,
			BackoffUntil: backoffUntil,
		})
	}
	sort.SliceStable(cands, func(i, j int) bool {
		pi := transportPriority(cands[i].Transport)
		pj := transportPriority(cands[j].Transport)
		if pi != pj {
			return pi > pj // higher priority first
		}
		return cands[i].RTTEstMs < cands[j].RTTEstMs
	})
	a.mu.Lock()
	prev, existed := a.byNode[nodeID]
	a.byNode[nodeID] = cands
	a.mu.Unlock()
	log.Printf("[SWARM] AddressTable.onRecord: indexed node=%s candidates=%d", truncID(nodeID), len(cands))
	// Detect a freshly-added Tier-0 (6PN ULA noise-UDP) candidate: an
	// entry with Scope=="private" + Transport=="noise-udp" that was not
	// present in the previous snapshot for this peer. Tier-0 is the
	// only path that can promote a same-org peer from WS Grade C to
	// noise-UDP Grade A, so its arrival is exactly the signal the
	// proactive upgrade walker needs to drop its 30s tick latency to
	// "next select iteration". Cross-org peers don't carry Scope=
	// "private" addresses (their 6PN ULAs are not routable across the
	// org boundary) so the trigger is naturally scoped to the same-org
	// upgrade case.
	//
	// We only wake when a NEW Tier-0 address appears (set-difference,
	// not membership) so spurious republications of the same record
	// don't burn walker cycles — the walker still runs every
	// upgradeWalkInterval as a backstop.
	if addedTier0Address(prev, cands) {
		if cb := a.wakeCallback(); cb != nil {
			cb()
		}
	}
	// First sight of this peer — notify the route engine so it can emit a
	// direct-path advertisement onto fleet.route. Without this, peers
	// learned via gossip-only (no session established yet) would never
	// appear in route_paths and dispatch would skip the role-based lane
	// for them. Subsequent updates skip the call because a path is
	// already advertised; the route engine handles refresh internally.
	if !existed {
		if rtr := a.Router(); rtr != nil {
			_ = rtr.Advertise(route.NodeID(nodeID), nil, "")
		}
	}
	return nil
}

// addedTier0Address returns true if `next` contains a Tier-0 address
// (Scope=="private" + Transport=="noise-udp") that does NOT appear in
// `prev`. Identity for "same entry" is the (Host, Port) pair — RTT and
// Region drift across republications and must not suppress the wake.
//
// Used by onRecord to fire the upgrade-walker wake only when a fresh
// 6PN ULA candidate actually lands, not on every PeerRecord. Sized for
// O(N+M) under the loop without an intermediate map: PeerRecord
// Addresses lists are short (typically 1–8 entries) so a linear scan
// is faster than a map allocation here.
func addedTier0Address(prev, next []DialCandidate) bool {
	hasOld := func(host string, port uint32) bool {
		for i := range prev {
			if prev[i].Host == host && prev[i].Port == port &&
				prev[i].Scope == "private" && prev[i].Transport == "noise-udp" {
				return true
			}
		}
		return false
	}
	for i := range next {
		if next[i].Scope != "private" || next[i].Transport != "noise-udp" {
			continue
		}
		if !hasOld(next[i].Host, next[i].Port) {
			return true
		}
	}
	return false
}

func transportString(t swarmpb.Address_Transport) string {
	switch t {
	case swarmpb.Address_NOISE_UDP:
		return "noise-udp"
	case swarmpb.Address_WEBSOCKET:
		return "websocket"
	case swarmpb.Address_GRPC:
		return "grpc"
	case swarmpb.Address_HTTP:
		return "http"
	default:
		return "unknown"
	}
}

func transportPriority(s string) int {
	switch s {
	case "noise-udp":
		return 4
	case "websocket":
		return 3
	case "grpc":
		return 2
	case "http":
		return 1
	default:
		return 0
	}
}

// swarmTransportToReachProto maps the swarm AddressTable transport string
// (the Address_Transport enum, stringified by transportString above) to the
// lad.ReachAddress.Proto value the multipath dialer expects. Returns ""
// for transports the dialer cannot consume so callers can skip the entry.
func swarmTransportToReachProto(t string) string {
	switch t {
	case "noise-udp":
		return "udp"
	case "websocket":
		return "wss"
	case "grpc":
		return "grpc"
	case "http":
		return "http"
	default:
		return ""
	}
}
