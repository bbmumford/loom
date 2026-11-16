/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"net"
	"testing"

	"github.com/bbmumford/loom/ports"
	swarmpb "github.com/bbmumford/swarm/proto/pb"
)

// privateIPPlatform answers PrivateIP with a fixed address and everything
// else as the dev platform does, standing in for a cloud platform whose
// metadata names the node's private address.
type privateIPPlatform struct {
	ports.PlatformInfo
	privateIP string
}

func (p privateIPPlatform) PrivateIP() string { return p.privateIP }

// onPrivateIPHost points discoverPrivateIP's lookups at a stated host: a
// hostname that resolves to nothing (so tiers 1 and 3 are silent) and the
// given interface addresses. Interface enumeration for
// advertiseLocalInterfaces fails, so an advert carries only the private entry
// advertiseSwarmListeners writes itself.
func onPrivateIPHost(t *testing.T, addrs ...net.Addr) {
	t.Helper()
	oldHostname, oldLookup, oldAddrs := privateIPHostname, privateIPLookupHost, privateIPInterfaceAddrs
	oldIfaces := netInterfaces
	t.Cleanup(func() {
		privateIPHostname, privateIPLookupHost, privateIPInterfaceAddrs = oldHostname, oldLookup, oldAddrs
		netInterfaces = oldIfaces
	})
	privateIPHostname = func() (string, error) { return "ladder-test-host", nil }
	privateIPLookupHost = func(name string) ([]string, error) {
		return nil, &net.DNSError{Err: "no such host", Name: name, IsNotFound: true}
	}
	privateIPInterfaceAddrs = func() ([]net.Addr, error) { return addrs, nil }
	netInterfaces = func() ([]net.Interface, error) { return nil, net.UnknownNetworkError("enumeration off") }
}

// A node composed without a platform runs on ports.DevPlatform. Its private
// address is the ladder's filtered answer: the Docker bridge and the public
// address on the host are passed over for the routable LAN address.
func TestANodeOnTheDevPlatformTakesTheFilteredPrivateAddress(t *testing.T) {
	onPrivateIPHost(t,
		mustCIDR(t, "172.17.0.1/16"),
		mustCIDR(t, "203.0.113.7/24"),
		mustCIDR(t, "10.254.77.9/16"),
	)
	rt := &Runtime{}
	rt.cfg.Platform = ports.DevPlatform()

	if got := rt.discoverPrivateIP(); got != "10.254.77.9" {
		t.Fatalf("discoverPrivateIP on the dev platform = %q, want the routable LAN address 10.254.77.9: "+
			"an unfiltered platform answer bypassed the ladder's filters", got)
	}
}

// The interface tier for any family runs last, so it answers only when the
// hostname, the ULA walk, the .internal name and the outbound probe are all
// silent, and a ULA on the same host still wins over IPv4.
func TestThePrivateIPLadderPrefersAULAAndFallsBackToAnIPv4Interface(t *testing.T) {
	rt := &Runtime{}
	rt.cfg.Platform = ports.DevPlatform()

	onPrivateIPHost(t, mustCIDR(t, "10.1.2.3/8"), mustCIDR(t, "fd12:3456:789a::1/64"))
	if got := rt.discoverPrivateIP(); got != "fd12:3456:789a::1" {
		t.Fatalf("with a ULA and an IPv4 LAN address, discoverPrivateIP = %q, want the ULA", got)
	}

	onPrivateIPHost(t, mustCIDR(t, "192.168.40.12/24"))
	if got := rt.discoverPrivateIP(); got != "192.168.40.12" {
		t.Fatalf("with only an IPv4 LAN address, discoverPrivateIP = %q, want 192.168.40.12", got)
	}
}

// The filters hold on the last tier as on the others: a host whose only
// addresses are a Docker bridge, a public address and a link-local address
// has no private address to state.
func TestThePrivateIPLadderStatesNoAddressForAHostWithoutOne(t *testing.T) {
	rt := &Runtime{}
	rt.cfg.Platform = ports.DevPlatform()
	onPrivateIPHost(t,
		mustCIDR(t, "172.17.0.1/16"),
		mustCIDR(t, "203.0.113.7/24"),
		mustCIDR(t, "169.254.10.10/16"),
		mustCIDR(t, "127.0.0.1/8"),
	)
	if got := rt.discoverPrivateIP(); got != "" {
		t.Fatalf("discoverPrivateIP = %q on a host with no routable private address, want \"\"", got)
	}
}

// Config.PrivateIP comes first and a platform's own answer comes before every
// discovery tier.
func TestThePrivateIPLadderHonoursTheOverrideAndThePlatform(t *testing.T) {
	onPrivateIPHost(t, mustCIDR(t, "10.1.2.3/8"))

	rt := &Runtime{}
	rt.cfg.Platform = privateIPPlatform{PlatformInfo: ports.DevPlatform(), privateIP: "fdaa:0:1:a7b::2"}
	if got := rt.discoverPrivateIP(); got != "fdaa:0:1:a7b::2" {
		t.Fatalf("discoverPrivateIP = %q, want the platform's fdaa:0:1:a7b::2", got)
	}

	rt.cfg.PrivateIP = "10.9.9.9"
	if got := rt.discoverPrivateIP(); got != "10.9.9.9" {
		t.Fatalf("discoverPrivateIP = %q, want the Config.PrivateIP override 10.9.9.9", got)
	}
}

func privateNoiseUDPHosts(si *SwarmIntegration) map[string]bool {
	out := map[string]bool{}
	for _, a := range si.Publisher.Addresses() {
		if a.Transport == swarmpb.Address_NOISE_UDP && a.Scope == "private" {
			out[a.Host] = true
		}
	}
	return out
}

// The private swarm entry states the address the bootstrap headers and the
// cross-origin forwarder read, so a dev-platform node advertises the filtered
// LAN address and nothing the ladder refused.
func TestThePrivateSwarmEntryIsTheLadderAnswer(t *testing.T) {
	onPrivateIPHost(t, mustCIDR(t, "172.17.0.1/16"), mustCIDR(t, "10.254.77.9/16"))
	rt := &Runtime{}
	rt.cfg.VL1.UDPPort = 4242
	rt.cfg.Platform = ports.DevPlatform()

	si := &SwarmIntegration{Publisher: &PeerPublisher{}}
	rt.advertiseSwarmListeners(si)

	got := privateNoiseUDPHosts(si)
	if len(got) != 1 || !got["10.254.77.9"] {
		t.Fatalf("private noise-UDP candidates = %v, want exactly 10.254.77.9", got)
	}
}

// With Config.PrivateIP set on a cloud platform, the advert carries the
// override and keeps the platform's own private address beside it.
func TestThePrivateSwarmEntryCarriesTheOverrideAndThePlatformAddress(t *testing.T) {
	onPrivateIPHost(t)
	rt := &Runtime{}
	rt.cfg.VL1.UDPPort = 4242
	rt.cfg.PrivateIP = "10.9.9.9"
	rt.cfg.Platform = privateIPPlatform{PlatformInfo: ports.DevPlatform(), privateIP: "fdaa:0:1:a7b::2"}

	si := &SwarmIntegration{Publisher: &PeerPublisher{}}
	rt.advertiseSwarmListeners(si)

	got := privateNoiseUDPHosts(si)
	if len(got) != 2 || !got["10.9.9.9"] || !got["fdaa:0:1:a7b::2"] {
		t.Fatalf("private noise-UDP candidates = %v, want the override 10.9.9.9 and the platform's fdaa:0:1:a7b::2", got)
	}
}
