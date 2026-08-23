/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package gossip

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"sort"
	"testing"
	"time"

	lad "github.com/bbmumford/ledger"
	"github.com/bbmumford/ledger/cache"
	"github.com/bbmumford/whisper"
)

// seedTime is a fixed record timestamp. Convergence is a property of record
// identity and set union, not of wall-clock, so the harness pins the clock to
// keep runs deterministic and to avoid the zero-time tombstone edge in Apply.
var seedTime = time.Unix(1_700_000_000, 0)

// seedMember applies one member record to a cache — a node that has learned of a
// single peer. It is the unit the convergence tests move across the exchange; it
// goes in through the same Apply the responder uses, so a seeded record is
// indistinguishable from a gossiped one.
func seedMember(t *testing.T, c *cache.DirectoryCache, tenant, node string) {
	t.Helper()
	body, err := json.Marshal(lad.MemberRecord{TenantID: tenant, NodeID: node, CreatedAt: seedTime})
	if err != nil {
		t.Fatalf("marshal member %s/%s: %v", tenant, node, err)
	}
	if err := c.Apply(lad.Record{
		Topic:     lad.TopicMember,
		TenantID:  tenant,
		NodeID:    node,
		Body:      body,
		Timestamp: seedTime,
	}); err != nil {
		t.Fatalf("apply member %s/%s: %v", tenant, node, err)
	}
}

// memberKeys is the set of "tenant/node" member identities a cache holds, read
// from the same Dump() the exchange serializes — so the assertion checks exactly
// what the wire would carry, not a private projection.
func memberKeys(c *cache.DirectoryCache) map[string]bool {
	keys := map[string]bool{}
	for _, rec := range c.Dump() {
		if rec.Topic == lad.TopicMember {
			keys[rec.TenantID+"/"+rec.NodeID] = true
		}
	}
	return keys
}

func keysSorted(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// gossipRound runs one initiator↔responder anti-entropy exchange between two
// caches: the real GossipOverConn writer against the real runGossipResponderLoop
// reader, over in-memory net.Pipe connections (one gossip stream, one reconcile
// stream, as the live responder expects). It blocks until the responder has
// applied the initiator's records and both responder goroutines have exited, so a
// caller can assert cache state immediately after it returns. A single round is
// bidirectional: the initiator sends its records and receives the peer's, and the
// responder applies the initiator's — after it returns both caches hold the union.
func gossipRound(t *testing.T, initiator, responder *cache.DirectoryCache, respID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	gInit, gResp := net.Pipe() // G1 record-exchange stream
	rInit, rResp := net.Pipe() // reconcile stream (unused here; kept open so the responder can read it)

	done := make(chan struct{})
	go func() {
		defer close(done)
		runGossipResponderLoop(ctx, gResp, rResp, responder, nil,
			func() *ExchangeMeta { return &ExchangeMeta{} },
			whisper.NewDeltaTracker(), respID)
	}()

	_, err := GossipOverConn(ctx, gInit, initiator, &ExchangeMeta{})

	// The initiator's single round-trip is complete (or errored). Close the
	// initiator ends so the responder's next blocking reads on both streams return
	// EOF and its loops exit; runGossipResponderLoop then returns and closes done.
	_ = gInit.Close()
	_ = rInit.Close()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		cancel()
		<-done
		t.Fatalf("responder %s did not exit after exchange", respID)
	}
	if err != nil {
		t.Fatalf("gossip exchange to %s: %v", respID, err)
	}
}

// TestGossip_TwoNodesConvergeInOneRound proves a single anti-entropy exchange is
// bidirectional: after it, the initiator has the responder's record AND the
// responder has the initiator's.
func TestGossip_TwoNodesConvergeInOneRound(t *testing.T) {
	a := cache.NewDirectoryCache()
	b := cache.NewDirectoryCache()
	seedMember(t, a, "t", "node-a")
	seedMember(t, b, "t", "node-b")

	gossipRound(t, a, b, "node-b")

	for name, c := range map[string]*cache.DirectoryCache{"a": a, "b": b} {
		got := memberKeys(c)
		if !got["t/node-a"] || !got["t/node-b"] {
			t.Fatalf("cache %s did not converge after one round: has %v, want t/node-a and t/node-b",
				name, keysSorted(got))
		}
	}
}

// TestGossip_EpidemicConvergesAcrossFleet is the convergence harness the gossip
// loop's cadence work needs: N nodes each seeded with a distinct member reach a
// single shared view under repeated pairwise anti-entropy. It exercises the real
// GossipOverConn/responder record path — so a change to the loop's cadence (the
// interval it drives GossipOverConn at) can be validated against actual epidemic
// spread rather than a mock.
func TestGossip_EpidemicConvergesAcrossFleet(t *testing.T) {
	const n = 6
	caches := make([]*cache.DirectoryCache, n)
	ids := make([]string, n)
	for i := range caches {
		caches[i] = cache.NewDirectoryCache()
		ids[i] = fmt.Sprintf("node-%d", i)
		seedMember(t, caches[i], "t", ids[i])
	}

	// Each round, every node initiates one exchange with its ring successor.
	// Within a round the caches mutate in place and the sweep runs i=0..n-1 in
	// order, so a record chains forward many hops in a single round; three rounds
	// over a ring of n is a comfortable convergence bound (and keeps the harness's
	// per-exchange grace cost bounded).
	const rounds = 3
	for round := 0; round < rounds; round++ {
		for i := 0; i < n; i++ {
			j := (i + 1) % n
			gossipRound(t, caches[i], caches[j], ids[j])
		}
	}

	want := make(map[string]bool, n)
	for _, id := range ids {
		want["t/"+id] = true
	}
	for i, c := range caches {
		got := memberKeys(c)
		for k := range want {
			if !got[k] {
				t.Fatalf("%s did not converge: missing %s, has %v", ids[i], k, keysSorted(got))
			}
		}
	}
}
