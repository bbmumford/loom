/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"testing"

	"github.com/bbmumford/swarm"
	swarmpb "github.com/bbmumford/swarm/proto/pb"
	"google.golang.org/protobuf/proto"
)

// The per-address RTT hint is CONSUMED by loom and PRODUCED only when a
// provider is set:
//
//	AddressRTTProvider      declared          node/peer_publisher.go
//	SetRTTProvider          callers in loom   none
//	publishOnce             stamps RttEstimateMs only with a provider set
//	address_table.go        READS it          RTTEstMs: addr.RttEstimateMs
//	address_table.go        RANKS on it       cands[i].RTTEstMs < cands[j].RTTEstMs
//
// 🔴 So with no provider every address a loom node publishes arrives with
// RttEstimateMs == 0, and the ranking is ASCENDING — 0 sorts first, i.e.
// "best". The setter's doc ("so peers can pick the lowest-latency address from
// the set instead of probing every one") holds only once a caller sets one.
//
// ⚠ BOUNDED: `cands` are the addresses of ONE peer record, so these zeroes tie
// only among that peer's own addresses — they never sort against a different
// peer's real measurements. Within a transport-priority tier, a loom peer's
// addresses are ordered by wire arrival order, and the intended latency
// preference never applies.
//
// A publisher given an AddressRTTProvider (PeerPublisher.SetRTTProvider) stamps
// `RttEstimateMs: rtt` on each address; nothing in this module supplies one.
//
// These tests pin the CURRENT behaviour, including the zero-tie, so that
// wiring a provider turns them red in the place that explains why.

func addrOf(transport swarmpb.Address_Transport, host string, port uint32, rtt uint32) *swarmpb.Address {
	return &swarmpb.Address{
		Transport: transport, Host: host, Port: port, RttEstimateMs: rtt,
	}
}

// indexAddresses drives the REAL onRecord — the sort under test lives inside
// it, so building an AddressTable with pre-sorted candidates (as
// address_table_test.go does for its accessor tests) would test nothing here.
func indexAddresses(t *testing.T, addrs ...*swarmpb.Address) []DialCandidate {
	t.Helper()
	body, err := proto.Marshal(&swarmpb.PeerRecord{
		NodeId: []byte(testNodeIDB), Addresses: addrs,
	})
	if err != nil {
		t.Fatal(err)
	}
	a := &AddressTable{byNode: map[string][]DialCandidate{}}
	if err := a.onRecord(swarm.Record{NodeID: swarm.NodeID(testNodeIDB), Body: body}); err != nil {
		t.Fatalf("onRecord: %v", err)
	}
	got := a.Get(testNodeIDB)
	if len(got) != len(addrs) {
		t.Fatalf("indexed %d candidates from %d addresses — onRecord dropped "+
			"some, so the ordering assertions below are not about what they "+
			"claim", len(got), len(addrs))
	}
	return got
}

// 🔴 THE RANKING IS REAL — it does prefer a lower RTT when one is present.
// Pinned first so the zero-tie test below cannot be satisfied by a sort that
// ignores RTT entirely.
func TestAddressRankingPrefersTheLowerRTTWithinATransportTier(t *testing.T) {
	got := indexAddresses(t,
		addrOf(swarmpb.Address_WEBSOCKET, "slow.example", 443, 250),
		addrOf(swarmpb.Address_WEBSOCKET, "fast.example", 443, 5),
	)
	if got[0].Host != "fast.example" {
		t.Fatalf("ranked %q first, want fast.example — the RTT hint is not "+
			"ordering addresses within a transport tier, so the whole "+
			"rtt_estimate_ms mechanism is inert even when populated", got[0].Host)
	}
}

// A measured address must outrank an unmeasured one.
//
// 0 is not "0 ms" to this sort, it is "UNMEASURED", and loom's publisher never
// stamps the field at all, so a plain ascending sort ranks every address a loom
// node publishes as best-in-class.
func TestAMeasuredAddressOutranksAnUnmeasuredOne(t *testing.T) {
	got := indexAddresses(t,
		addrOf(swarmpb.Address_WEBSOCKET, "unmeasured.example", 443, 0),
		addrOf(swarmpb.Address_WEBSOCKET, "measured.example", 443, 5), // real 5ms
	)
	if got[0].Host != "measured.example" {
		t.Fatalf("ranked %q first, want measured.example — an ABSENT RTT hint is "+
			"outranking a real 5ms measurement. Absent must not sort as the "+
			"smallest value", got[0].Host)
	}
}

// …and when NOTHING is measured, nothing changes: all candidates tie and
// SliceStable preserves arrival order. Ranking-last is safe where a filter
// would not be — no address is ever dropped, so a peer whose addresses are all
// unmeasured still gets its full candidate set in its published order.
func TestWithNoMeasurementsAtAllArrivalOrderIsPreserved(t *testing.T) {
	got := indexAddresses(t,
		addrOf(swarmpb.Address_WEBSOCKET, "first.example", 443, 0),
		addrOf(swarmpb.Address_WEBSOCKET, "second.example", 443, 0),
	)
	if len(got) != 2 {
		t.Fatalf("got %d candidates, want 2 — the fail-closed ordering must never "+
			"DROP an unmeasured address, only rank it", len(got))
	}
	if got[0].Host != "first.example" {
		t.Fatalf("ranked %q first, want first.example — with no measurement to "+
			"discriminate, the stable sort must leave arrival order alone", got[0].Host)
	}
}

// The discriminator must not override transport priority: a measured WebSocket
// address must still lose to an unmeasured noise-UDP one, or "fixing" the tie
// would silently demote the mesh's best transport behind its fallback.
func TestTheRTTDiscriminatorNeverOutranksTransportPriority(t *testing.T) {
	got := indexAddresses(t,
		addrOf(swarmpb.Address_WEBSOCKET, "measured-ws.example", 443, 5),
		addrOf(swarmpb.Address_NOISE_UDP, "unmeasured-udp.example", 41641, 0),
	)
	if got[0].Host != "unmeasured-udp.example" {
		t.Fatalf("ranked %q first, want unmeasured-udp.example — the RTT "+
			"discriminator has been allowed to reorder across transport tiers, "+
			"which demotes noise-UDP behind WebSocket", got[0].Host)
	}
}
