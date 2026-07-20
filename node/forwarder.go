/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"context"
	"log"
	"net"
	"strconv"
	"time"

	aether "github.com/ORBTR/aether"
	"github.com/ORBTR/aether/noise"
	lad "github.com/bbmumford/ledger"
	ladcache "github.com/bbmumford/ledger/cache"
)

// Cross-org noise-UDP forwarder wiring.
//
// The aether/noise package ships an IntraOrgForwarder that can hairpin a
// preambled msg1 from the anycast-receiving machine (M_r) to the target
// machine (M_t) over 6PN, then unwrap M_t's reply and write it back
// from M_r's listener socket so Fly's edge 5-tuple flow stays intact.
//
// Library's job here is to:
//
//  1. Provide a ForwarderLookup callback that resolves a target NodeID
//     to a same-org 6PN UDP address using the LAD reach cache.
//  2. Attach an IntraOrgForwarder to each NoiseTransport at startup so
//     the listener path branches into the forwarder before normal
//     classify+dispatch.
//
// The dialer-side preamble emission lives in peer_connections.go — the
// connection manager decides per-dial whether to set the cross-org flag
// on aether.Target.Metadata based on peer.crossOrigin + the chosen
// address being a public IPv4.

// forwarderLookup returns a ForwarderLookup closure backed by the
// runtime's LAD reach cache. The closure scans a target NodeID's reach
// addresses for a Scope:"private" IPv6 6PN entry advertised on the
// noise-UDP port, and returns it as a *net.UDPAddr.
//
// Returns (nil, false) when:
//   - the runtime has no cache (boot-time race)
//   - the target NodeID is not in the directory
//   - the target's reach has no 6PN/udp entry
//
// All three are "drop the packet"-class failures from the forwarder's
// POV — the dialer's WS fallback will still establish a session.
// forwarderLookupTimeout caps the per-packet Reach() lookup so a
// hot-path UDP hairpin can't be wedged by in-memory writer-mutex
// contention. Reach() is a read against bbmumford/ledger v0.0.10's
// in-memory map — fast in the steady state — but during a writer
// burst (gossip apply storm, cache-rebuild) reads can queue behind
// the writer's exclusive lock. 100 ms is a conservative ceiling
// well above any healthy-path latency; on miss we return (nil, false)
// and the dialer falls back to WS, so the forwarder never hangs.
// Was the H6 finding.
const forwarderLookupTimeout = 100 * time.Millisecond

func (rt *Runtime) forwarderLookup() noise.ForwarderLookup {
	return func(target aether.NodeID) (*net.UDPAddr, bool) {
		if rt == nil || rt.cache == nil {
			return nil, false
		}
		lookupCtx, cancel := context.WithTimeout(rt.Context(), forwarderLookupTimeout)
		defer cancel()
		recs, err := rt.cache.Reach(lookupCtx, "", ladcache.ReachQuery{NodeID: string(target)})
		if err != nil || len(recs) == 0 {
			return nil, false
		}
		for _, rec := range recs {
			for _, a := range rec.Addresses {
				if a.Scope != "private" || a.Proto != "udp" {
					continue
				}
				udp, ok := parseSixPNUDPAddr(a)
				if !ok {
					continue
				}
				return udp, true
			}
		}
		return nil, false
	}
}

// parseSixPNUDPAddr converts a LAD ReachAddress into a *net.UDPAddr if
// the address is a Fly 6PN ULA (fdaa::/8) — anything else (Docker
// bridge IPv4, link-local, non-Fly ULA like Tailscale fd7a::) is not a
// valid intra-org forwarder target.
//
// L2 #8 fix: previous implementation only checked `ip.IsPrivate()`
// which covers all of fc00::/7 (the full IPv6 ULA range). A node
// running Tailscale on the same host could publish a fd7a:.. address
// with Scope:"private",Proto:"udp"; forwarderLookup would accept it
// and the hairpin would route into Tailscale's overlay instead of
// Fly's 6PN, where the target machine isn't reachable. Strict
// fdaa::/8 match aligns with extractOriginPrefix's policy.
func parseSixPNUDPAddr(a lad.ReachAddress) (*net.UDPAddr, bool) {
	host := a.Host
	port := a.Port
	// Reach records sometimes encode host as "[ipv6]:port" — split if so.
	if h, p, err := net.SplitHostPort(host); err == nil {
		host = h
		if p != "" && port == 0 {
			if pn, perr := strconv.Atoi(p); perr == nil {
				port = pn
			}
		}
	}
	ip := net.ParseIP(host)
	if ip == nil || ip.To4() != nil {
		return nil, false
	}
	if !isFly6PN(ip) {
		return nil, false
	}
	if port <= 0 {
		return nil, false
	}
	return &net.UDPAddr{IP: ip, Port: port}, true
}

// isFly6PN reports whether ip is a Fly 6PN address — IPv6 ULA in the
// fdaa::/8 subnet specifically. Non-Fly ULAs (Tailscale fd7a::, k8s
// Cilium fd9c::, etc.) are excluded — they share fc00::/7 with Fly but
// don't route inside the Fly mesh.
func isFly6PN(ip net.IP) bool {
	if ip == nil || ip.To4() != nil {
		return false
	}
	// First byte 0xfd, second byte 0xaa → fdaa::/8 within fc00::/7.
	v6 := ip.To16()
	return v6 != nil && v6[0] == 0xfd && v6[1] == 0xaa
}

// attachIntraOrgForwarder builds an IntraOrgForwarder for the given
// NoiseTransport and installs it via tr.SetForwarder. The forwarder's
// sweeper goroutine exits when NoiseTransport.Close runs (transport
// owns the forwarder lifecycle).
//
// A double-attach (a transport that already has a forwarder) is a NO-OP
// — but logs a WARNING because that almost certainly indicates a config-
// reload path re-entering startup without tearing the old transport down.
// The old forwarder's lookup closure is then retained and the new lookup
// is silently dropped; the warning surfaces that ambiguity to operators.
func (rt *Runtime) attachIntraOrgForwarder(tr *noise.NoiseTransport, label string) {
	if tr == nil {
		return
	}
	if tr.Forwarder() != nil {
		log.Printf("[NODE] WARN: VL1 cross-org forwarder already attached to %s transport — retaining existing instance; new lookup closure DROPPED (config reload without transport rebuild?)", label)
		return
	}
	fwd := noise.NewIntraOrgForwarder(rt.identity.NodeID, rt.forwarderLookup())
	tr.SetForwarder(fwd)
	log.Printf("[NODE] VL1 cross-org forwarder attached to %s transport", label)
}
