/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"testing"

	swarmpb "github.com/bbmumford/swarm/proto/pb"
)

// A private-only node must still advertise.
//
// advertiseSwarmListeners skips only the entries that need a public name when
// publicHost is empty; the PRIVATE 6PN entry and the interface-derived NIC
// candidates do not. A node with no PublicDomain and no STUN-discovered
// publicIP that published an EMPTY address set would leave same-fabric peers
// with no candidate at all.
//
// The requirements this holds to:
//
//   - transport is GRADED, "an ordered preference from a fast datagram
//     protocol down to a universally-reachable one", which has nothing to
//     order if the node publishes zero candidates;
//   - a standalone partition mesh runs "without the central service", which
//     is precisely the class of node with no public hostname;
//   - the one node class the mesh denies an OVERLAY ADDRESS is the control
//     plane, and a publicHost check does not express that.
func TestAPrivateOnlyNodeStillAdvertisesItsPrivateCandidates(t *testing.T) {
	rt := &Runtime{}
	rt.cfg.VL1.UDPPort = 4242
	// No PublicDomain, no publicIP: a standalone partition mesh's node.
	rt.cfg.PublicDomain = ""
	rt.publicIP = ""

	si := &SwarmIntegration{Publisher: &PeerPublisher{}}
	rt.advertiseSwarmListeners(si)

	got := si.Publisher.Addresses()
	if len(got) == 0 {
		t.Fatal("a node with no public hostname advertised NOTHING — every " +
			"advertisement is gated behind publicHost, so a standalone " +
			"partition mesh publishes an empty address set and the graded " +
			"transport has nothing to order")
	}

	// Everything advertised must be a genuine, dialable candidate — and none of
	// it may be a public entry, because there is no public name to advertise.
	for _, a := range got {
		if a.Host == "" {
			t.Errorf("advertised an empty host: %+v", a)
		}
		if a.Scope == "public" {
			t.Errorf("advertised a PUBLIC-scoped candidate %q with no public "+
				"hostname or IP available — that address cannot be reached", a.Host)
		}
		if a.Transport != swarmpb.Address_NOISE_UDP {
			t.Errorf("%s advertised as %v; with no public DNS name only the "+
				"noise-UDP candidates are valid (the 443 entries need a name for "+
				"TLS validation)", a.Host, a.Transport)
		}
	}
}

// The admission twin, so the test above cannot pass against a function that
// advertises indiscriminately: WITH a public name, the public entry must appear.
func TestAPublicNodeStillAdvertisesItsPublicEntry(t *testing.T) {
	rt := &Runtime{}
	rt.cfg.VL1.UDPPort = 4242
	rt.cfg.PublicDomain = "node.example.test"

	si := &SwarmIntegration{Publisher: &PeerPublisher{}}
	rt.advertiseSwarmListeners(si)

	var sawPublicUDP, sawWS, sawHTTP bool
	for _, a := range si.Publisher.Addresses() {
		switch {
		case a.Transport == swarmpb.Address_NOISE_UDP && a.Host == "node.example.test":
			sawPublicUDP = true
		case a.Transport == swarmpb.Address_WEBSOCKET:
			sawWS = true
		case a.Transport == swarmpb.Address_HTTP:
			sawHTTP = true
		}
	}
	if !sawPublicUDP {
		t.Error("the public noise-UDP entry disappeared for a node that HAS a " +
			"public domain — the publicHost guard was moved too far down")
	}
	if !sawWS || !sawHTTP {
		t.Errorf("the 443 entries are missing (ws=%v http=%v) — a publicly named "+
			"node must still advertise its edge-served listeners", sawWS, sawHTTP)
	}
}

// A node with no UDP listener bound advertises no noise-UDP candidate at all,
// public or private — the port would point at nothing.
func TestNoUDPListenerMeansNoNoiseUDPCandidates(t *testing.T) {
	rt := &Runtime{}
	rt.cfg.VL1.UDPPort = 0
	rt.cfg.PublicDomain = ""

	si := &SwarmIntegration{Publisher: &PeerPublisher{}}
	rt.advertiseSwarmListeners(si)

	for _, a := range si.Publisher.Addresses() {
		if a.Transport == swarmpb.Address_NOISE_UDP {
			t.Errorf("advertised noise-UDP candidate %s:%d with no UDP listener bound",
				a.Host, a.Port)
		}
	}
}
