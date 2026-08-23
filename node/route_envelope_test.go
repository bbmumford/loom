/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/bbmumford/route"
	"github.com/bbmumford/swarm"
)

// COVERAGE of the route-fabric wire envelope: `ingestRouteRecord`,
// `swarmRoutePublisher.publish` and both publish methods, all at 0.0%.
//
// 🔑 A CENSUS REFINEMENT THIS SLICE FORCED ON ME. My name-grep found ZERO
// callers for `PublishAdvertisement`/`PublishWithdrawal` and I nearly filed
// them as dead. They are LIVE — `swarmRoutePublisher` satisfies
// `route.AdvertisementPublisher`, and the router calls them **through the
// interface** at `route/router_impl.go:300` and `:324`. ⇒ **A method that
// exists to satisfy an interface has no direct callers by name, and a
// name-grep cannot see that edge.** That is a third census failure mode
// alongside per-pair scope and one-level reachability.
//
// 🔴 AND THE PROPERTY THAT MAKES THIS WORTH TESTING: advertisements and
// withdrawals share ONE topic, distinguished only by the envelope's `kind`
// string. Get it wrong and a peer LEAVING the fabric is ingested as one
// JOINING — the route table keeps a path to a node that has gone.

// capturingNode records what was published instead of touching a real swarm.
type capturingNode struct {
	swarm.Node
	topic swarm.Topic
	body  []byte
	err   error
}

func (c *capturingNode) Publish(topic swarm.Topic, body []byte) error {
	if c.err != nil {
		return c.err
	}
	c.topic, c.body = topic, body
	return nil
}

// recordingRouter captures which ingest path a record took. Only the two
// methods ingestRouteRecord calls are meaningful; the rest satisfy the
// interface and are deliberately inert.
type recordingRouter struct {
	route.Router
	adverts     []*route.PathAdvertisement
	withdrawals []*route.PathWithdrawal
	advertErr   error
}

func (r *recordingRouter) IngestAdvertisement(ad *route.PathAdvertisement) error {
	r.adverts = append(r.adverts, ad)
	return r.advertErr
}

func (r *recordingRouter) IngestWithdrawal(w *route.PathWithdrawal) error {
	r.withdrawals = append(r.withdrawals, w)
	return nil
}

// ── Publish side ────────────────────────────────────────────────────────────

// 🔴 THE `kind` DISCRIMINATOR IS THE WHOLE PROTOCOL. Both flows go through one
// `publish`, so a wrong or missing kind silently inverts join and leave.
func TestAdvertAndWithdrawalCarryDistinctKindsOnTheSameTopic(t *testing.T) {
	advertNode := &capturingNode{}
	if err := (&swarmRoutePublisher{node: advertNode}).
		PublishAdvertisement(&route.PathAdvertisement{}); err != nil {
		t.Fatalf("PublishAdvertisement: %v", err)
	}

	withdrawNode := &capturingNode{}
	if err := (&swarmRoutePublisher{node: withdrawNode}).
		PublishWithdrawal(&route.PathWithdrawal{}); err != nil {
		t.Fatalf("PublishWithdrawal: %v", err)
	}

	if advertNode.topic != withdrawNode.topic {
		t.Fatalf("advert went to %q and withdrawal to %q — they must share one "+
			"topic so a leaving peer can withdraw without a second subscription",
			advertNode.topic, withdrawNode.topic)
	}
	if advertNode.topic != fleetRouteTopic {
		t.Fatalf("published to %q, want %q", advertNode.topic, fleetRouteTopic)
	}

	var ad, wd routeEnvelope
	if err := json.Unmarshal(advertNode.body, &ad); err != nil {
		t.Fatalf("advert envelope is not decodable: %v", err)
	}
	if err := json.Unmarshal(withdrawNode.body, &wd); err != nil {
		t.Fatalf("withdrawal envelope is not decodable: %v", err)
	}
	if ad.Kind != "advert" {
		t.Fatalf("advert kind = %q, want \"advert\" — the receiver's switch has "+
			"no arm for this and the advertisement is silently dropped", ad.Kind)
	}
	if wd.Kind != "withdraw" {
		t.Fatalf("withdrawal kind = %q, want \"withdraw\" — a peer LEAVING the "+
			"fabric is not distinguished from one joining", wd.Kind)
	}
	if ad.Kind == wd.Kind {
		t.Fatal("both flows published the same kind — join and leave are " +
			"indistinguishable on the wire")
	}
}

// Each envelope must carry its own payload and omit the other, or the receiver
// hits its nil guard and drops the record.
func TestEachEnvelopeCarriesOnlyItsOwnPayload(t *testing.T) {
	n := &capturingNode{}
	if err := (&swarmRoutePublisher{node: n}).
		PublishAdvertisement(&route.PathAdvertisement{}); err != nil {
		t.Fatal(err)
	}
	var env routeEnvelope
	if err := json.Unmarshal(n.body, &env); err != nil {
		t.Fatal(err)
	}
	if env.Advertisement == nil {
		t.Fatal("the advert envelope has a nil Advertisement — the receiver's " +
			"nil guard drops it and the path is never learned")
	}
	if env.Withdrawal != nil {
		t.Fatal("the advert envelope also carries a Withdrawal — omitempty is " +
			"not doing its job and the receiver may act on the wrong field")
	}
}

// A transport failure must reach the caller: the router at
// route/router_impl.go:300 checks the error, and swallowing it would leave the
// router believing it had advertised a path that never went out.
func TestAPublishFailureIsReturnedToTheRouter(t *testing.T) {
	boom := errors.New("topic unavailable")
	p := &swarmRoutePublisher{node: &capturingNode{err: boom}}

	if err := p.PublishAdvertisement(&route.PathAdvertisement{}); !errors.Is(err, boom) {
		t.Fatalf("PublishAdvertisement returned %v, want the transport error — "+
			"the router would believe a path was advertised that never left "+
			"this node", err)
	}
	if err := p.PublishWithdrawal(&route.PathWithdrawal{}); !errors.Is(err, boom) {
		t.Fatalf("PublishWithdrawal returned %v, want the transport error — a "+
			"withdrawal that silently failed leaves every peer routing to a "+
			"node that has gone", err)
	}
}

// ── Ingest side ─────────────────────────────────────────────────────────────

// The two kinds must dispatch to their own ingest path. A crossed switch turns
// a withdrawal into an advertisement, which is the worst direction: the path
// is re-learned at the moment it was meant to be dropped.
func TestIngestDispatchesEachKindToItsOwnRouterPath(t *testing.T) {
	r := &recordingRouter{}

	advert, err := json.Marshal(routeEnvelope{
		Kind: "advert", Advertisement: &route.PathAdvertisement{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := ingestRouteRecord(r, swarm.Record{Body: advert}); err != nil {
		t.Fatalf("ingest advert: %v", err)
	}
	if len(r.adverts) != 1 || len(r.withdrawals) != 0 {
		t.Fatalf("an advert produced %d adverts and %d withdrawals — the kind "+
			"switch is crossed", len(r.adverts), len(r.withdrawals))
	}

	withdraw, err := json.Marshal(routeEnvelope{
		Kind: "withdraw", Withdrawal: &route.PathWithdrawal{},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := ingestRouteRecord(r, swarm.Record{Body: withdraw}); err != nil {
		t.Fatalf("ingest withdrawal: %v", err)
	}
	if len(r.withdrawals) != 1 {
		t.Fatalf("a withdrawal produced %d withdrawals, want 1 — a peer leaving "+
			"the fabric is not being dropped from the route table",
			len(r.withdrawals))
	}
	if len(r.adverts) != 1 {
		t.Fatalf("a withdrawal also produced an advertisement (%d total) — the "+
			"path is RE-LEARNED at the moment it was meant to be dropped",
			len(r.adverts))
	}
}

// 🔑 EVERY MALFORMED INPUT MUST BE SURVIVABLE AND SILENT-BY-RETURN. This is a
// gossip topic: any peer can publish anything onto it, so a decode failure must
// not error the subscriber (which would tear down the subscription) and must
// not reach the router.
func TestMalformedAndEmptyRecordsAreDroppedWithoutTouchingTheRouter(t *testing.T) {
	for _, tc := range []struct {
		name string
		rec  swarm.Record
	}{
		{"not JSON at all", swarm.Record{Body: []byte("{not json")}},
		{"empty body", swarm.Record{}},
		{"unknown kind", swarm.Record{Body: mustJSON(t, routeEnvelope{Kind: "sabotage"})}},
		{"advert kind with no advert", swarm.Record{Body: mustJSON(t, routeEnvelope{Kind: "advert"})}},
		{"withdraw kind with no withdrawal", swarm.Record{Body: mustJSON(t, routeEnvelope{Kind: "withdraw"})}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := &recordingRouter{}
			if err := ingestRouteRecord(r, tc.rec); err != nil {
				t.Fatalf("returned %v — a subscriber that errors on a malformed "+
					"gossip record risks tearing down the subscription, and any "+
					"peer can publish onto this topic", err)
			}
			if len(r.adverts) != 0 || len(r.withdrawals) != 0 {
				t.Fatalf("a malformed record reached the router (%d adverts, %d "+
					"withdrawals)", len(r.adverts), len(r.withdrawals))
			}
		})
	}
}

// A tombstone is ignored on purpose — withdrawals carry their own signed
// envelope, so acting on a tombstone would drop a path on an unsigned signal.
func TestATombstoneIsIgnoredBecauseWithdrawalsCarryTheirOwnEnvelope(t *testing.T) {
	r := &recordingRouter{}
	body := mustJSON(t, routeEnvelope{Kind: "withdraw", Withdrawal: &route.PathWithdrawal{}})

	if err := ingestRouteRecord(r, swarm.Record{Body: body, Tombstone: true}); err != nil {
		t.Fatal(err)
	}
	if len(r.withdrawals) != 0 {
		t.Fatalf("a TOMBSTONE was ingested as a withdrawal (%d) — a path would "+
			"be dropped on an unsigned signal rather than on the signed "+
			"withdrawal envelope", len(r.withdrawals))
	}
}

// An ingest error from the router must not propagate: damping, loops, stale
// epochs and verification failures are expected operational conditions on this
// topic, and returning them would surface routine gossip as subscriber errors.
func TestARouterIngestErrorIsLoggedRatherThanReturned(t *testing.T) {
	r := &recordingRouter{advertErr: errors.New("damped")}
	body := mustJSON(t, routeEnvelope{Kind: "advert", Advertisement: &route.PathAdvertisement{}})

	if err := ingestRouteRecord(r, swarm.Record{Body: body}); err != nil {
		t.Fatalf("returned %v for a DAMPED advertisement — damping is an "+
			"expected condition, and surfacing it as a subscriber error makes "+
			"routine gossip look like a transport fault", err)
	}
	if len(r.adverts) != 1 {
		t.Fatalf("the advertisement did not reach the router (%d)", len(r.adverts))
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
