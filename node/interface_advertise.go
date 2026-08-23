/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"log"
	"net"
	"sort"

	swarmpb "github.com/bbmumford/swarm/proto/pb"
)

// netInterfaces and interfaceAddrs are seams over net.Interfaces and
// net.Interface.Addrs. Production always uses the real implementations;
// they exist because every filter in enumerateLocalInterfaces is otherwise
// verifiable only against whichever NICs the host running the tests happens
// to have — which is to say, not verifiable at all. Overridden only by tests,
// which restore them immediately.
var (
	netInterfaces  = net.Interfaces
	interfaceAddrs = func(ifc net.Interface) ([]net.Addr, error) { return ifc.Addrs() }
)

// swarmpbAddressNoiseUDP isolates the proto enum value at one site so
// the rest of this file doesn't pull in swarmpb. Trivial wrapper kept
// here purely for symmetry with other Address_TRANSPORT references in
// swarm_integration.go.
func swarmpbAddressNoiseUDP() swarmpb.Address_Transport {
	return swarmpb.Address_NOISE_UDP
}

// localInterfaceAddresses enumerates every routable interface address on
// the host so a multi-homed node advertises every plausible private dial
// target instead of just the one Platform.PrivateIP() reports.
//
// A node with multiple NICs (several 6PN families, dual VPNs, a private LAN
// plus a Fly 6PN ULA) is reduced to a single private address by a lone
// Platform.PrivateIP() call. Same-org peers that happen to share a different
// prefix then see no
// reachable private candidate and degraded to the public anycast path
// — which then hits the cross-region UDP unreliability the connection
// log shows.
//
// Filters:
//   - Skip interfaces that are not up.
//   - Skip loopback (lo / lo0). Loopback never reaches a peer.
//   - Skip link-local unicast (169.254.0.0/16, fe80::/10). Not routable
//     between machines.
//   - Skip multicast and unspecified addresses.
//   - Keep both v4 and v6, both RFC1918 private and routable public
//     addresses observed on the interface — receivers choose based on
//     Scope tags.
//   - Deduplicate by string so identical addresses across aliased
//     interfaces produce a single advertisement.
//
// Returns (host string, scope string) pairs. Scope is inferred via
// scopeForHost so the same rules apply as the swarm-merge fallback:
// 6PN ULA / RFC1918 / IPv6 ULA → "private"; everything else
// → "public".
//
// This enumeration does NOT include the platform-reported PrivateIP. That
// defensive fallback — for containerised runtimes that hide the host's
// NICs — is emitted by the caller at swarm_integration.go:528-531, two
// statements before it calls advertiseLocalInterfaces. Stated here because
// the previous wording read as if this function supplied it, which would
// make an enumeration failure look survivable from inside this file alone.
type localInterfaceAddress struct {
	Host  string
	Scope string
}

// enumerateLocalInterfaces walks net.Interfaces() and returns every
// routable address found, defensively deduplicating. The returned slice
// is stable-sorted by (scope, host) so the resulting PeerRecord doesn't
// flap between publishes solely due to OS-level interface ordering.
//
// On error (interface enumeration failed) returns nil; caller must fall
// back to the platform-reported PrivateIP.
func enumerateLocalInterfaces() []localInterfaceAddress {
	ifaces, err := netInterfaces()
	if err != nil {
		return nil
	}
	seen := make(map[string]struct{})
	out := make([]localInterfaceAddress, 0, 8)
	for _, ifc := range ifaces {
		if ifc.Flags&net.FlagUp == 0 {
			continue
		}
		if ifc.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := interfaceAddrs(ifc)
		if err != nil {
			continue
		}
		for _, raw := range addrs {
			var ip net.IP
			switch v := raw.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			default:
				continue
			}
			if ip == nil || ip.IsUnspecified() || ip.IsLoopback() {
				continue
			}
			if ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
				continue
			}
			if ip.IsMulticast() {
				continue
			}
			host := ip.String()
			if _, dup := seen[host]; dup {
				continue
			}
			seen[host] = struct{}{}
			out = append(out, localInterfaceAddress{
				Host:  host,
				Scope: scopeForHost(host),
			})
		}
	}
	// The (scope, host) ordering this function's doc promises. Without it the
	// slice carries whatever order the OS reported its interfaces in, which is
	// not stable across calls — and since the advertised address list becomes
	// the published PeerRecord, a reordering alone republishes the record and
	// gossips a "change" that changed nothing. "private" sorts before "public",
	// which also puts the same-fabric candidates first.
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Scope != out[j].Scope {
			return out[i].Scope < out[j].Scope
		}
		return out[i].Host < out[j].Host
	})
	return out
}

// advertiseLocalInterfaces emits a noise-UDP candidate per interface
// address discovered. The platform-reported PrivateIP is advertised by the
// caller (swarm_integration.go:528-531), not here — so if enumeration
// returns nothing this function advertises nothing and logs nothing, and
// the private candidate a peer needs comes from that separate call.
// Idempotent — AdvertiseLocalAddress dedups by
// {transport, host, port}, so calling this on every
// advertiseSwarmListeners tick is safe. Skips when udpPort is zero
// (no listener bound).
func (rt *Runtime) advertiseLocalInterfaces(si *SwarmIntegration, udpPort int) {
	if si == nil || si.Publisher == nil || udpPort <= 0 {
		return
	}
	added := 0
	for _, addr := range enumerateLocalInterfaces() {
		si.AdvertiseLocalAddress(swarmpbAddressNoiseUDP(), addr.Host, uint32(udpPort), "", addr.Scope)
		added++
	}
	if added > 0 {
		log.Printf("[SWARM] advertiseLocalInterfaces: emitted %d interface-derived noise-UDP candidates", added)
	}
}
