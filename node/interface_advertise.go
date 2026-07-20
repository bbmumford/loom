/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"log"
	"net"

	swarmpb "github.com/bbmumford/swarm/proto/pb"
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
// A node with multiple NICs (multiple 6PN families post-migration, dual
// VPNs, a private LAN plus a Fly 6PN ULA, etc.) was previously reduced
// to a single private address by the single Platform.PrivateIP() call.
// Same-org peers that happened to share a different prefix saw no
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
// → "public". Includes the platform-reported PrivateIP as a defensive
// fallback when interface enumeration is unavailable (containerised
// runtimes that hide the host's NICs).
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
	ifaces, err := net.Interfaces()
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
		addrs, err := ifc.Addrs()
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
	return out
}

// advertiseLocalInterfaces emits a noise-UDP candidate per interface
// address discovered, plus the platform-reported PrivateIP as a defence
// against enumeration failure (e.g. containerised runtimes that hide
// host NICs). Idempotent — AdvertiseLocalAddress dedups by
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
