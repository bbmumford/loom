/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"log"

	"github.com/bbmumford/route"
	"github.com/bbmumford/swarm"
)

// fleetRouteTopic carries route.PathAdvertisement and PathWithdrawal
// records onto the fleet swarm fabric. Subscribers index incoming
// records into the local Router; the local Router publishes its own
// originated advertisements onto this topic via SwarmRoutePublisher.
//
// Each Record body is a JSON envelope with a "kind" discriminator so
// the subscriber can route to IngestAdvertisement vs IngestWithdrawal.
// JSON keeps the wire format protocol-agnostic — a future swap to the
// generated pb types is a transparent encoding change.
const fleetRouteTopic = swarm.Topic("fleet.route")

// RouteIntegration aggregates the route engine + the swarm topic
// subscription + the dial-time path consult helpers. Runtime owns one
// per fabric (HSTLES fleet here; the agent has its own integration).
type RouteIntegration struct {
	Router    route.Router
	publisher *swarmRoutePublisher
	unsub     swarm.Unsubscribe
}

// ErrSwarmNotWired is returned when InitRoute is called before
// InitSwarm. The route engine depends on swarm.Node for its topic
// publishing, so the caller must wire swarm first.
var ErrSwarmNotWired = errors.New("node: route requires InitSwarm first")

// InitRoute wires the route.Router onto the Runtime's swarm fabric.
// Returns the integration on success. Idempotent — subsequent calls
// return the existing integration. transit configures whether this
// node forwards multi-hop traffic for other peers.
//
// The Router's background reconvergence loop is spawned here and
// registered with rt.wg so Shutdown's wg.Wait blocks until it drains —
// mirrors the swarm.Node engine spawn pattern in InitSwarm. The loop
// periodically re-emits locally-originated advertisements (closing the
// gap where a peer joined just after the initial Advertise published)
// and decays damping penalty so suppressed paths become eligible again.
func (rt *Runtime) InitRoute(ctx context.Context, transit route.TransitMode) (*RouteIntegration, error) {
	if existing := rt.route.Load(); existing != nil {
		return existing, nil
	}
	if rt.swarm == nil || rt.swarm.Node == nil {
		return nil, ErrSwarmNotWired
	}
	if rt.identity == nil || rt.identity.PrivateKey == nil {
		return nil, ErrSwarmIdentityUnset
	}

	r, err := route.NewRouter(route.Config{
		NodeID:  route.NodeID(rt.identity.NodeID),
		PrivKey: ed25519.PrivateKey(rt.identity.PrivateKey),
		Transit: transit,
	})
	if err != nil {
		return nil, err
	}

	pub := &swarmRoutePublisher{node: rt.swarm.Node}
	if impl, ok := r.(interface {
		SetPublisher(route.AdvertisementPublisher)
	}); ok {
		impl.SetPublisher(pub)
	} else {
		// route.NewRouter must return something that implements
		// SetPublisher(route.AdvertisementPublisher). If it doesn't —
		// route module upgrade dropped the method, interface signature
		// changed, or a mock router was wired in — every Advertise()
		// will skip publishing (publisher==nil) and route_paths will
		// stay empty forever. Log loudly rather than failing silently, so
		// operators can correlate this to "swarm route paths never
		// populate".
		log.Printf("[ROUTE] WARNING: route.Router (%T) does not implement SetPublisher(route.AdvertisementPublisher) — advertisements will not publish onto fleet.route topic; route_paths will remain empty", r)
	}

	unsub, err := rt.swarm.Node.Subscribe(fleetRouteTopic, func(rec swarm.Record) error {
		return ingestRouteRecord(r, rec)
	})
	if err != nil {
		return nil, err
	}

	integration := &RouteIntegration{
		Router:    r,
		publisher: pub,
		unsub:     unsub,
	}
	rt.route.Store(integration)

	// One-shot backfill: SessionHook may have fired for peers already
	// connected at the moment InitRoute lands (race between
	// PublishRPCHandlersToLAD calling InitRoute and ConnectionManager
	// having registered live peers before the route engine existed).
	// Walk the currently-active session list and emit a direct-path
	// advertisement per peer so route_paths is populated immediately,
	// not deferred to the next reconnect cycle.
	if rt.connMgr != nil {
		for _, peerID := range rt.connMgr.ActivePeers() {
			r.Advertise(route.NodeID(peerID), nil, "")
		}
	}

	// Second backfill: peers learned via gossip-only (PeerRecord
	// indexed in the swarm AddressTable but no session established
	// yet) are invisible to SessionHook entirely. Wire the AddressTable
	// to the Router so future onRecord calls emit Advertise / Withdraw
	// in step with gossip, and seed the engine now with every entry
	// already cached. containsLoop on the route side filters the
	// self-entry, so it's safe to walk the whole map without a
	// special-case check here.
	if rt.swarm != nil && rt.swarm.AddressTable != nil {
		rt.swarm.AddressTable.SetRouter(r)
		for nodeID := range rt.swarm.AddressTable.All() {
			r.Advertise(route.NodeID(nodeID), nil, "")
		}
	}

	// Spawn the router's background reconvergence loop. Registered with
	// rt.wg so Shutdown's wg.Wait blocks until the loop exits — mirrors
	// the swarm.Node engine spawn in InitSwarm. Router.Start blocks on
	// rtCtx; cancelling rt.ctx (via Shutdown) unblocks it.
	rtCtx := rt.Context()
	rt.Go("route.router.start", func() {
		log.Printf("[ROUTE] InitRoute: starting router reconvergence loop")
		_ = rt.route.Load().Router.Start(rtCtx)
		log.Printf("[ROUTE] InitRoute: router reconvergence loop exited")
	})

	return integration, nil
}

// Route returns the wired RouteIntegration, or nil if InitRoute hasn't
// been called. Uses atomic.Pointer.Load so callers running on arbitrary
// goroutines (e.g. the swarm SessionHook) read a consistent pointer
// without coordinating with InitRoute.
func (rt *Runtime) Route() *RouteIntegration {
	return rt.route.Load()
}

// routeEnvelope is the JSON wire format for records on fleetRouteTopic.
// The "kind" field distinguishes advertisements from withdrawals — a
// single topic carries both so a peer leaving the fabric can withdraw
// without needing a second subscription.
type routeEnvelope struct {
	Kind          string                   `json:"kind"` // "advert" | "withdraw"
	Advertisement *route.PathAdvertisement `json:"advert,omitempty"`
	Withdrawal    *route.PathWithdrawal    `json:"withdraw,omitempty"`
}

// swarmRoutePublisher implements route.AdvertisementPublisher by
// emitting JSON envelopes onto fleetRouteTopic.
type swarmRoutePublisher struct {
	node swarm.Node
}

func (p *swarmRoutePublisher) PublishAdvertisement(ad *route.PathAdvertisement) error {
	return p.publish(routeEnvelope{Kind: "advert", Advertisement: ad})
}

func (p *swarmRoutePublisher) PublishWithdrawal(w *route.PathWithdrawal) error {
	return p.publish(routeEnvelope{Kind: "withdraw", Withdrawal: w})
}

// publish is the shared encode + emit path for both advert/withdraw
// flows. Keeping the marshal in one place lets a future swap to proto
// (or any other deterministic encoding) be a single-site change rather
// than two near-identical bodies.
func (p *swarmRoutePublisher) publish(env routeEnvelope) error {
	body, err := json.Marshal(env)
	if err != nil {
		return err
	}
	return p.node.Publish(fleetRouteTopic, body)
}

// ingestRouteRecord decodes a Record body and dispatches it to the
// Router's IngestAdvertisement or IngestWithdrawal. Tombstones are
// ignored — withdrawals carry their own signed envelope.
func ingestRouteRecord(r route.Router, rec swarm.Record) error {
	if rec.Tombstone {
		return nil
	}
	var env routeEnvelope
	if err := json.Unmarshal(rec.Body, &env); err != nil {
		return nil
	}
	switch env.Kind {
	case "advert":
		if env.Advertisement == nil {
			return nil
		}
		if err := r.IngestAdvertisement(env.Advertisement); err != nil {
			// Damping, loops, stale epochs and verification failures are
			// expected operational conditions — log at debug only when
			// the Library debug tag is enabled.
			log.Printf("[ROUTE] ingest advert: %v", err)
		}
	case "withdraw":
		if env.Withdrawal == nil {
			return nil
		}
		if err := r.IngestWithdrawal(env.Withdrawal); err != nil {
			log.Printf("[ROUTE] ingest withdrawal: %v", err)
		}
	}
	return nil
}
