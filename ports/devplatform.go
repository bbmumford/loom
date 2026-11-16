/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package ports

import (
	"net"
	"os"
)

// DevPlatform returns the safe generic PlatformInfo used when Config.
// Platform is nil. It is deliberately minimal: no cloud detection, no
// special bind semantics. Cloud deployments (Fly in particular) MUST inject
// their real platform implementation — this default cannot provide
// fly-global-services UDP bind, 6PN origin detection, or region codes, so a
// nil Platform field does NOT keep host behaviour the way the other nil seams
// do.
func DevPlatform() PlatformInfo { return devPlatform{} }

type devPlatform struct{}

// Region honours a REGION env override so multi-node dev topologies can
// still exercise region-aware paths; otherwise "unknown" (the same sentinel
// host.Detect uses when detection fails).
func (devPlatform) Region() string {
	if r := os.Getenv("REGION"); r != "" {
		return r
	}
	return "unknown"
}

func (devPlatform) MachineID() string {
	h, _ := os.Hostname()
	return h
}

func (devPlatform) AppName() string  { return "" }
func (devPlatform) PublicIP() string { return "" } // callers fall back to STUN

// PrivateIP returns "": the dev platform has no authoritative source for the
// private mesh address, so node.Runtime's discoverPrivateIP answers from its
// own tiers (hostname, interfaces, .internal DNS, outbound probe), each
// filtered to routable private addresses with Docker bridges excluded, as it
// does for atlas's dev platform. Its tier 0 returns this answer unfiltered
// whenever it is non-empty, so an interface walk here would hand a public or
// Docker-bridge address to the bootstrap headers, the cross-origin
// forwarder's origin and the private swarm entry.
func (devPlatform) PrivateIP() string { return "" }

func (devPlatform) ResolveUDPBind(port int) string {
	return ":" + itoa(port)
}

func (devPlatform) ResolveUDPBindDualStack(int) (string, string) { return "", "" }

func (devPlatform) OpenUDPListener(network, address string) (*net.UDPConn, error) {
	udpAddr, err := net.ResolveUDPAddr(network, address)
	if err != nil {
		return nil, err
	}
	return net.ListenUDP(network, udpAddr)
}

func (devPlatform) IsCloud() bool              { return false }
func (devPlatform) Provider() string           { return "dev" }
func (devPlatform) EnvTags() map[string]string { return map[string]string{} }
func (devPlatform) PreferredSources() []AddressSource {
	// dev ordering per the host package: interface first, STUN primary
	// discovery on bare metal, UPnP last resort.
	return []AddressSource{SourceInterface, SourceSTUN, SourceUPnP}
}

// itoa avoids strconv for this one hot-free call site.
func itoa(v int) string {
	if v == 0 {
		return "0"
	}
	neg := v < 0
	if neg {
		v = -v
	}
	var buf [12]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
