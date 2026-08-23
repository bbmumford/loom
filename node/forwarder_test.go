/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	aether "github.com/ORBTR/aether"
	lad "github.com/bbmumford/ledger"
	ladcache "github.com/bbmumford/ledger/cache"

	"github.com/bbmumford/loom/directory"
)

// The 6PN forwarder lookup had NO test before this, and it is the path whose
// failure mode is silent: if the filter selects nothing, direct dialling stops
// and every peer falls back to relay with no error logged anywhere.
//
// It now reads through the LiveDirectory seam, which makes the filter's
// operand load-bearing: the port NORMALISES the reach layer's "udp" to
// "noise-udp", so the comparison must be against RawProtocol. Matching
// Protocol would compile, pass vet, and disable direct dialling fleet-wide.
func newForwarderRuntime(t *testing.T) (*Runtime, *ladcache.DirectoryCache) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	c := ladcache.NewDirectoryCache()
	ld, err := directory.NewLADDirectory(c)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ld.Close() })

	return &Runtime{ctx: ctx, cache: c, liveDir: ld, liveDirRaw: ld}, c
}

func publishReach(t *testing.T, c *ladcache.DirectoryCache, nodeID string, addrs []lad.ReachAddress) {
	t.Helper()
	body, err := json.Marshal(lad.ReachRecord{
		TenantID: "", NodeID: nodeID, Addresses: addrs,
		UpdatedAt: time.Now(), ExpiresAt: time.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Apply(lad.Record{
		Topic: lad.TopicReach, TenantID: "", NodeID: nodeID,
		Body: body, Timestamp: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
}

func TestForwarderLookupSelectsFly6PNThroughThePort(t *testing.T) {
	rt, c := newForwarderRuntime(t)
	// A Fly 6PN ULA published the way the reach layer publishes it: the
	// producer's protocol name is "udp", NOT the address table's "noise-udp".
	publishReach(t, c, "target-node", []lad.ReachAddress{
		{Host: "fdaa:0:1::7", Port: 41641, Proto: "udp", Scope: "private"},
	})

	udp, ok := rt.forwarderLookup()(aether.NodeID("target-node"))
	if !ok {
		t.Fatal("6PN candidate not selected — direct dialling would be disabled " +
			"and every peer would silently fall back to relay")
	}
	if udp.Port != 41641 || udp.IP.String() != "fdaa:0:1::7" {
		t.Fatalf("resolved to %v, want fdaa:0:1::7:41641", udp)
	}
}

// The discriminations the filter must keep making, so "it returns something"
// is not mistaken for "it returns the right thing".
func TestForwarderLookupRejectsNon6PNAndNonUDP(t *testing.T) {
	cases := []struct {
		name string
		addr lad.ReachAddress
	}{
		{"tailscale ULA is not Fly 6PN", lad.ReachAddress{Host: "fd7a::1", Port: 41641, Proto: "udp", Scope: "private"}},
		{"public scope is not a hairpin target", lad.ReachAddress{Host: "fdaa:0:1::7", Port: 41641, Proto: "udp", Scope: "public"}},
		{"non-udp transport", lad.ReachAddress{Host: "fdaa:0:1::7", Port: 443, Proto: "ws", Scope: "private"}},
		{"ipv4 private", lad.ReachAddress{Host: "10.0.0.5", Port: 41641, Proto: "udp", Scope: "private"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rt, c := newForwarderRuntime(t)
			publishReach(t, c, "target-node", []lad.ReachAddress{tc.addr})
			if _, ok := rt.forwarderLookup()(aether.NodeID("target-node")); ok {
				t.Fatalf("accepted %+v as a 6PN forwarder target", tc.addr)
			}
		})
	}
}

// An absent node must not resolve, and a nil seam must fail closed rather
// than panic — the lookup runs on the dial path.
func TestForwarderLookupFailsClosed(t *testing.T) {
	rt, _ := newForwarderRuntime(t)
	if _, ok := rt.forwarderLookup()(aether.NodeID("never-published")); ok {
		t.Fatal("an unknown node resolved to a forwarder target")
	}

	bare := &Runtime{ctx: context.Background()}
	if _, ok := bare.forwarderLookup()(aether.NodeID("x")); ok {
		t.Fatal("a runtime with no directory resolved a forwarder target")
	}
}
