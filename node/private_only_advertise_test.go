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
// advertiseSwarmListeners used to early-return whenever publicHost was empty,
// and EVERY address advertisement in the package sits downstream of that point
// — the public noise-UDP entry, the PRIVATE 6PN entry, the interface-derived
// NIC candidates, and both 443 entries. So a node with no PublicDomain and no
// STUN-discovered publicIP published an EMPTY address set and same-fabric peers
// had no candidate at all.
//
// The governing requirement, rather than a preference:
//
//	DM-P09  transport is GRADED — "an ordered preference from a fast datagram
//	        protocol down to a universally-reachable one" — which has nothing to
//	        order if the node publishes zero candidates.
//	DM-P07  SOVEREIGNTY: a standalone tenant mesh runs "without the central
//	        service", i.e. precisely the class of node with no public hostname.
//	DM-P03  the design's actual vocabulary for "advertise nothing" is to forbid a
//	        node an OVERLAY ADDRESS, and it applies that to exactly one class
//	        (the control plane). A publicHost check is not that vocabulary.
//	        MEASURED: the word "public" appears 0 times in design docs 14 and 18.
func TestAPrivateOnlyNodeStillAdvertisesItsPrivateCandidates(t *testing.T) {
	rt := &Runtime{}
	rt.cfg.VL1.UDPPort = 4242
	// No PublicDomain, no publicIP — the private-only node of DM-P07.
	rt.cfg.PublicDomain = ""
	rt.publicIP = ""

	si := &SwarmIntegration{Publisher: &PeerPublisher{}}
	rt.advertiseSwarmListeners(si)

	got := si.Publisher.Addresses()
	if len(got) == 0 {
		t.Fatal("a node with no public hostname advertised NOTHING — every " +
			"advertisement is still gated behind publicHost, so a standalone " +
			"tenant mesh (DM-P07) publishes an empty address set and DM-P09's " +
			"graded transport has nothing to order")
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
