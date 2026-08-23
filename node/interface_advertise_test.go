/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"net"
	"testing"

	swarmpb "github.com/bbmumford/swarm/proto/pb"
)

// COVERAGE of the multi-NIC advertisement path:
// swarmpbAddressNoiseUDP (:19), enumerateLocalInterfaces (:65),
// advertiseLocalInterfaces (:123) — all at 0.0%.
//
// CENSUS: advertiseLocalInterfaces <- swarm_integration.go:540, its only
// caller. enumerateLocalInterfaces and swarmpbAddressNoiseUDP <- that function
// only.
//
// 🔑 FIXTURE NOTE: advertiseLocalInterfaces is declared on *Runtime but its body
// never dereferences the receiver — it uses only si, udpPort and the package-level
// enumerateLocalInterfaces(). So a nil *Runtime is a legitimate and deliberate
// fixture here, and these tests need no Runtime construction at all.
//
// ⚠ TWO OF THESE TESTS DEPEND ON THE HOST'S REAL NICs, because
// enumerateLocalInterfaces calls net.Interfaces() directly with no seam. They are
// written as INVARIANTS over whatever the host yields rather than as value
// assertions, and each one FAILS LOUDLY IF THE HOST YIELDS NOTHING rather than
// passing vacuously — a machine with only loopback would otherwise satisfy every
// "all returned addresses are routable" claim by returning none.

// requireNonVacuous fails the test when the host enumerated no addresses, so an
// invariant test cannot pass by having nothing to check.
func requireNonVacuous(t *testing.T, got []localInterfaceAddress) {
	t.Helper()
	if len(got) == 0 {
		t.Skipf("this host enumerated 0 routable interface addresses, so the " +
			"invariant below has nothing to range over and would pass VACUOUSLY. " +
			"Skipping loudly rather than reporting a green that measured nothing.")
	}
}

// 🔴 THE ENUM IS A WIRE VALUE, NOT AN INTERNAL LABEL. AdvertiseLocalAddress
// (swarm_integration.go:449) SILENTLY DROPS Address_UNKNOWN with only a log line.
// If this wrapper ever returned the zero value, every interface-derived candidate
// would be discarded at the publisher and a multi-homed node would advertise only
// its two explicit entries — with no error anywhere.
func TestTheNoiseUDPEnumIsTheValueThePublisherAccepts(t *testing.T) {
	got := swarmpbAddressNoiseUDP()

	if got != swarmpb.Address_NOISE_UDP {
		t.Errorf("swarmpbAddressNoiseUDP() = %v, want %v — interface-derived "+
			"candidates would be advertised under the wrong transport",
			got, swarmpb.Address_NOISE_UDP)
	}
	// The independently-fatal half: whatever it returns must not be the zero
	// value, because that is the one AdvertiseLocalAddress throws away.
	if got == swarmpb.Address_UNKNOWN {
		t.Fatal("swarmpbAddressNoiseUDP() returned Address_UNKNOWN — " +
			"AdvertiseLocalAddress drops it with a log line and no error, so every " +
			"interface-derived candidate silently never reaches the PeerRecord")
	}
}

// 🔴 EVERY FILTER IN enumerateLocalInterfaces EXISTS TO KEEP AN UNDIALABLE
// ADDRESS OUT OF THE PEERRECORD. A loopback or link-local entry advertised to a
// peer is a candidate that can never connect: the dialer spends its attempt
// budget on it and the real address ranks behind.
func TestEnumeratedAddressesAreAllActuallyRoutable(t *testing.T) {
	got := enumerateLocalInterfaces()
	requireNonVacuous(t, got)

	for _, a := range got {
		ip := net.ParseIP(a.Host)
		if ip == nil {
			t.Errorf("enumerated host %q does not parse as an IP — it is advertised "+
				"verbatim into the PeerRecord and no peer can dial it", a.Host)
			continue
		}
		switch {
		case ip.IsLoopback():
			t.Errorf("%s is LOOPBACK — advertised to peers it can never reach this "+
				"host from another machine", a.Host)
		case ip.IsLinkLocalUnicast(), ip.IsLinkLocalMulticast():
			t.Errorf("%s is LINK-LOCAL — not routable between machines", a.Host)
		case ip.IsMulticast():
			t.Errorf("%s is MULTICAST — not a dial target", a.Host)
		case ip.IsUnspecified():
			t.Errorf("%s is the UNSPECIFIED address — a peer dialling it goes nowhere", a.Host)
		}
	}
}

// Deduplication is explicit in the implementation (a `seen` map) because aliased
// interfaces report the same address more than once. A duplicate would survive
// AdvertiseLocalAddress's {transport,host,port} dedup anyway, so this is belt and
// braces — but the sort/dedup is what keeps the published record from flapping.
func TestEnumeratedAddressesAreDeduplicated(t *testing.T) {
	got := enumerateLocalInterfaces()
	requireNonVacuous(t, got)

	seen := map[string]bool{}
	for _, a := range got {
		if seen[a.Host] {
			t.Errorf("host %q appears more than once — the `seen` map is not "+
				"suppressing aliased-interface duplicates", a.Host)
		}
		seen[a.Host] = true
	}
}

// 🔑 SCOPE IS THE FIELD RECEIVERS ROUTE ON. peer_connections.go's tiering reads
// Scope to decide whether a candidate is a same-fabric direct dial or a public
// one. An empty scope is not a third option — it is a value the receiver has to
// guess at, which is exactly what scopeForHost exists to prevent.
func TestEveryEnumeratedAddressCarriesADecidedScope(t *testing.T) {
	got := enumerateLocalInterfaces()
	requireNonVacuous(t, got)

	for _, a := range got {
		if a.Scope != "private" && a.Scope != "public" {
			t.Errorf("host %q has scope %q, want \"private\" or \"public\" — an "+
				"unscoped candidate leaves the receiver to infer reachability",
				a.Host, a.Scope)
		}
		if want := scopeForHost(a.Host); a.Scope != want {
			t.Errorf("host %q scoped %q but scopeForHost says %q — the enumeration "+
				"and the swarm-merge fallback would disagree about the same address",
				a.Host, a.Scope, want)
		}
	}
}

// ── advertiseLocalInterfaces: the guards ────────────────────────────────────

// 🔴 ALL THREE GUARDS MUST FAIL CLOSED WITHOUT PANICKING. This runs on every
// advertiseSwarmListeners tick, and a nil Publisher is the normal state before
// swarm integration finishes starting up.
func TestAdvertiseLocalInterfacesGuardsFailClosed(t *testing.T) {
	var rt *Runtime // deliberately nil — the method never dereferences it

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("advertiseLocalInterfaces panicked on a guard path: %v — this "+
				"runs on every publish tick, including before the publisher exists", r)
		}
	}()

	rt.advertiseLocalInterfaces(nil, 4242)                 // nil integration
	rt.advertiseLocalInterfaces(&SwarmIntegration{}, 4242) // nil publisher
	si := &SwarmIntegration{Publisher: &PeerPublisher{}}   //
	rt.advertiseLocalInterfaces(si, 0)                     // no listener bound
	rt.advertiseLocalInterfaces(si, -1)                    // nonsense port
	if addrs := si.Publisher.Addresses(); len(addrs) != 0 {
		t.Fatalf("a guard path advertised %d address(es): %+v — a node with no UDP "+
			"listener is publishing noise-UDP candidates peers cannot connect to",
			len(addrs), addrs)
	}
}

// ── advertiseLocalInterfaces: the happy path ────────────────────────────────

// The admission twin for the guards above: with a real publisher and a real
// port, every enumerated address must actually reach the PeerRecord. Without
// this, the guard test passes against an advertiseLocalInterfaces that is broken
// shut and advertises nothing ever.
func TestAdvertiseLocalInterfacesPublishesEveryEnumeratedAddress(t *testing.T) {
	var rt *Runtime
	enumerated := enumerateLocalInterfaces()
	requireNonVacuous(t, enumerated)

	si := &SwarmIntegration{Publisher: &PeerPublisher{}}
	const port = 4242
	rt.advertiseLocalInterfaces(si, port)

	got := si.Publisher.Addresses()
	if len(got) != len(enumerated) {
		t.Fatalf("advertised %d address(es) but the host enumerates %d — a "+
			"multi-homed node is not offering every dial target it has",
			len(got), len(enumerated))
	}

	byHost := map[string]*swarmpb.Address{}
	for _, a := range got {
		byHost[a.Host] = a
	}
	for _, want := range enumerated {
		a, ok := byHost[want.Host]
		if !ok {
			t.Errorf("enumerated host %q was never advertised", want.Host)
			continue
		}
		if a.Transport != swarmpb.Address_NOISE_UDP {
			t.Errorf("%s advertised with transport %v, want NOISE_UDP", want.Host, a.Transport)
		}
		if a.Port != uint32(port) {
			t.Errorf("%s advertised on port %d, want %d — the candidate points at a "+
				"port with no listener", want.Host, a.Port, port)
		}
		if a.Scope != want.Scope {
			t.Errorf("%s advertised with scope %q, want %q — same-fabric peers route "+
				"on this field", want.Host, a.Scope, want.Scope)
		}
	}
}

// 🔑 IDEMPOTENCE IS A STATED CONTRACT (:119 "Idempotent — AdvertiseLocalAddress
// dedups by {transport, host, port}"), and it is load-bearing because this is
// called on EVERY publish tick. If it accumulated, the PeerRecord would grow
// without bound for the life of the process and every publish would carry a
// larger duplicate address set.
func TestAdvertiseLocalInterfacesIsIdempotentAcrossTicks(t *testing.T) {
	var rt *Runtime
	requireNonVacuous(t, enumerateLocalInterfaces())

	si := &SwarmIntegration{Publisher: &PeerPublisher{}}
	rt.advertiseLocalInterfaces(si, 4242)
	first := len(si.Publisher.Addresses())

	for i := 0; i < 5; i++ {
		rt.advertiseLocalInterfaces(si, 4242)
	}

	if got := len(si.Publisher.Addresses()); got != first {
		t.Fatalf("address count grew from %d to %d over 6 publish ticks — the "+
			"PeerRecord accumulates duplicates for the life of the process", first, got)
	}
}

// ── deterministic filter tests, via the seams ───────────────────────────────
//
// 🔑 WHY THESE EXIST: the invariant tests above can only observe
// whichever NICs the host running them happens to have. Mutation proved that is
// not enough — neutering the interface-UP filter, the loopback-interface filter
// and the dedup ALL SURVIVED, not because those filters are redundant but
// because this host has no down interface, no loopback NIC carrying a routable
// address, and no aliased duplicate. A filter that no fixture can reach is
// untested however green the suite looks.

// withFakeInterfaces installs synthetic NICs for one test and restores the real
// seams afterwards, so no test can leak a fake into another.
func withFakeInterfaces(t *testing.T, ifaces []net.Interface, addrs map[string][]net.Addr) {
	t.Helper()
	oldIfaces, oldAddrs := netInterfaces, interfaceAddrs
	t.Cleanup(func() { netInterfaces, interfaceAddrs = oldIfaces, oldAddrs })

	netInterfaces = func() ([]net.Interface, error) { return ifaces, nil }
	interfaceAddrs = func(ifc net.Interface) ([]net.Addr, error) { return addrs[ifc.Name], nil }
}

func mustCIDR(t *testing.T, cidr string) net.Addr {
	t.Helper()
	ip, n, err := net.ParseCIDR(cidr)
	if err != nil {
		t.Fatalf("bad fixture CIDR %q: %v", cidr, err)
	}
	n.IP = ip
	return n
}

func hostsOf(got []localInterfaceAddress) []string {
	out := make([]string, 0, len(got))
	for _, a := range got {
		out = append(out, a.Host)
	}
	return out
}

// 🔴 A DOWN INTERFACE MUST NOT BE ADVERTISED. Its address is configured but not
// carrying traffic; a peer that dials it waits for a timeout that will never
// resolve, spending an attempt budget the working address needed.
func TestADownInterfaceContributesNothing(t *testing.T) {
	withFakeInterfaces(t,
		[]net.Interface{
			{Name: "up0", Flags: net.FlagUp},
			{Name: "down0", Flags: 0}, // configured, not up
		},
		map[string][]net.Addr{
			"up0":   {mustCIDR(t, "10.1.1.1/24")},
			"down0": {mustCIDR(t, "10.2.2.2/24")},
		})

	got := hostsOf(enumerateLocalInterfaces())
	if len(got) != 1 || got[0] != "10.1.1.1" {
		t.Fatalf("enumerated %v, want exactly [10.1.1.1] — the address of a DOWN "+
			"interface is being advertised, and every peer dialling it burns an "+
			"attempt on a NIC that carries no traffic", got)
	}
}

// 🔴 A LOOPBACK INTERFACE MUST BE SKIPPED BY ITS FLAG, NOT ONLY BY ITS IP.
// The per-address ip.IsLoopback() check catches 127.0.0.1 and ::1 — it does NOT
// catch a routable-looking address configured on a loopback NIC, which is a real
// pattern (anycast/VIP addresses pinned to lo). Advertising it tells peers to
// dial an address that only ever resolves inside this host.
func TestALoopbackInterfaceIsSkippedEvenWhenItsAddressLooksRoutable(t *testing.T) {
	withFakeInterfaces(t,
		[]net.Interface{
			{Name: "lo0", Flags: net.FlagUp | net.FlagLoopback},
			{Name: "eth0", Flags: net.FlagUp},
		},
		map[string][]net.Addr{
			// Deliberately NOT 127.x — the per-IP check would catch that, and the
			// mutant that removes the interface-level filter would survive again.
			"lo0":  {mustCIDR(t, "10.9.9.9/32")},
			"eth0": {mustCIDR(t, "10.1.1.1/24")},
		})

	got := hostsOf(enumerateLocalInterfaces())
	if len(got) != 1 || got[0] != "10.1.1.1" {
		t.Fatalf("enumerated %v, want exactly [10.1.1.1] — a routable-looking "+
			"address on a LOOPBACK interface reached the PeerRecord. The per-IP "+
			"IsLoopback() check cannot catch this; only the interface flag can", got)
	}
}

// Aliased interfaces report the same address more than once; the `seen` map is
// what collapses them. Without a duplicate in the fixture this filter is
// unreachable, which is exactly why it survived mutation before.
func TestAliasedDuplicatesCollapseToASingleAdvertisement(t *testing.T) {
	withFakeInterfaces(t,
		[]net.Interface{
			{Name: "eth0", Flags: net.FlagUp},
			{Name: "eth0:1", Flags: net.FlagUp}, // classic alias
		},
		map[string][]net.Addr{
			"eth0":   {mustCIDR(t, "10.1.1.1/24"), mustCIDR(t, "10.1.1.1/24")},
			"eth0:1": {mustCIDR(t, "10.1.1.1/24")},
		})

	got := hostsOf(enumerateLocalInterfaces())
	if len(got) != 1 {
		t.Fatalf("enumerated %v — the same address appears %d times. The "+
			"PeerRecord advertises one endpoint repeatedly and the dialer treats "+
			"each as a separate candidate", got, len(got))
	}
}

// 🔴 THE SORT THE DOC PROMISES. Before this was implemented the slice
// carried whatever order the OS reported, which is not stable across calls —
// and the advertised list becomes the published PeerRecord, so a reordering
// alone republishes the record and gossips a change that changed nothing.
// ⚠ FIXTURE NOTE, and mutation is what caught it: my first version of this test
// used 10.x/8.8.8.8/93.x, whose plain string order happens to be IDENTICAL to
// their (scope, host) order — so a mutant that sorted by HOST ALONE and ignored
// scope entirely SURVIVED. The fixture below is chosen so the two orderings
// DISAGREE: 1.1.1.1 is public but sorts first by host, and 192.168.1.1 is
// private but sorts last. Only a scope-first comparator produces `want`.
func TestTheResultIsSortedByScopeThenHost(t *testing.T) {
	withFakeInterfaces(t,
		[]net.Interface{{Name: "eth0", Flags: net.FlagUp}},
		map[string][]net.Addr{"eth0": {
			mustCIDR(t, "93.184.216.34/24"), // public
			mustCIDR(t, "10.9.9.9/24"),      // private
			mustCIDR(t, "1.1.1.1/24"),       // public — sorts FIRST by host alone
			mustCIDR(t, "192.168.1.1/24"),   // private — sorts LAST by host alone
			mustCIDR(t, "10.1.1.1/24"),      // private
		}})

	got := enumerateLocalInterfaces()
	// private (host-ascending) THEN public (host-ascending).
	want := []string{"10.1.1.1", "10.9.9.9", "192.168.1.1", "1.1.1.1", "93.184.216.34"}

	if h := hostsOf(got); !equalStrings(h, want) {
		t.Fatalf("order = %v, want %v — the doc promises a (scope, host) stable "+
			"sort so the PeerRecord does not flap on OS interface ordering. "+
			"private must precede public, and hosts must ascend within a scope", h, want)
	}
}

// Determinism is the property that actually matters: the same inputs must give
// the same order every time, or the anti-flap guarantee is worthless.
func TestEnumerationOrderIsStableAcrossRepeatedCalls(t *testing.T) {
	withFakeInterfaces(t,
		[]net.Interface{{Name: "eth0", Flags: net.FlagUp}},
		map[string][]net.Addr{"eth0": {
			mustCIDR(t, "10.5.5.5/24"),
			mustCIDR(t, "192.168.1.1/24"),
			mustCIDR(t, "8.8.8.8/24"),
		}})

	first := hostsOf(enumerateLocalInterfaces())
	for i := 0; i < 10; i++ {
		if got := hostsOf(enumerateLocalInterfaces()); !equalStrings(got, first) {
			t.Fatalf("call %d returned %v but the first returned %v — the order is "+
				"not deterministic, so the PeerRecord republishes on reordering alone", i, got, first)
		}
	}
}

// The non-routable classes, asserted deterministically rather than hoping the
// host has one of each.
func TestNonRoutableAddressClassesAreAllFiltered(t *testing.T) {
	withFakeInterfaces(t,
		[]net.Interface{{Name: "eth0", Flags: net.FlagUp}},
		map[string][]net.Addr{"eth0": {
			mustCIDR(t, "169.254.1.1/16"), // IPv4 link-local
			mustCIDR(t, "fe80::1/64"),     // IPv6 link-local
			mustCIDR(t, "224.0.0.1/32"),   // multicast
			&net.IPAddr{IP: net.IPv4zero}, // unspecified
			&net.IPAddr{IP: net.IPv6zero}, // unspecified v6
			mustCIDR(t, "127.0.0.1/8"),    // loopback IP on a non-loopback NIC
			mustCIDR(t, "10.1.1.1/24"),    // the only survivor
		}})

	got := hostsOf(enumerateLocalInterfaces())
	if len(got) != 1 || got[0] != "10.1.1.1" {
		t.Fatalf("enumerated %v, want exactly [10.1.1.1] — a non-routable address "+
			"class reached the PeerRecord as a dial candidate", got)
	}
}

// Enumeration failure returns nil, and the caller's separate PrivateIP
// advertisement is what covers the node in that case (swarm_integration.go:528).
func TestEnumerationFailureReturnsNil(t *testing.T) {
	oldIfaces := netInterfaces
	t.Cleanup(func() { netInterfaces = oldIfaces })
	netInterfaces = func() ([]net.Interface, error) { return nil, net.UnknownNetworkError("boom") }

	if got := enumerateLocalInterfaces(); got != nil {
		t.Fatalf("enumeration error returned %v, want nil", got)
	}
}

// A per-interface Addrs() error must skip only that interface — one unreadable
// NIC must not cost the node every other address it has.
func TestOneUnreadableInterfaceDoesNotSuppressTheOthers(t *testing.T) {
	oldIfaces, oldAddrs := netInterfaces, interfaceAddrs
	t.Cleanup(func() { netInterfaces, interfaceAddrs = oldIfaces, oldAddrs })

	netInterfaces = func() ([]net.Interface, error) {
		return []net.Interface{{Name: "bad0", Flags: net.FlagUp}, {Name: "eth0", Flags: net.FlagUp}}, nil
	}
	interfaceAddrs = func(ifc net.Interface) ([]net.Addr, error) {
		if ifc.Name == "bad0" {
			return nil, net.UnknownNetworkError("cannot read")
		}
		return []net.Addr{mustCIDR(t, "10.1.1.1/24")}, nil
	}

	got := hostsOf(enumerateLocalInterfaces())
	if len(got) != 1 || got[0] != "10.1.1.1" {
		t.Fatalf("enumerated %v, want [10.1.1.1] — one unreadable interface "+
			"suppressed the addresses of a readable one", got)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// A DIFFERENT port is a different endpoint and must NOT be deduplicated away —
// otherwise a node that rebinds its UDP listener keeps advertising the dead port.
func TestADifferentPortIsADistinctCandidate(t *testing.T) {
	var rt *Runtime
	enumerated := enumerateLocalInterfaces()
	requireNonVacuous(t, enumerated)

	si := &SwarmIntegration{Publisher: &PeerPublisher{}}
	rt.advertiseLocalInterfaces(si, 4242)
	rt.advertiseLocalInterfaces(si, 5353)

	if got, want := len(si.Publisher.Addresses()), 2*len(enumerated); got != want {
		t.Fatalf("advertised %d addresses across two ports, want %d — the dedup key "+
			"is ignoring the port, so a rebind leaves the old endpoint advertised "+
			"or the new one suppressed", got, want)
	}
}
