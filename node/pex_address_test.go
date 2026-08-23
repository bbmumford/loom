/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"testing"

	lad "github.com/bbmumford/ledger"
)

// Covers all four functions in pex_address.go. Censused per symbol, one level
// out, and checked for interface satisfaction. All four are UNEXPORTED, so
// package node is the entire possible caller population and a repo-rooted
// search has scope equal to the claim:
//
//	scopeForHost          1 <- interface_advertise.go:109 (+ scopeForSwarmHost)
//	scopeForSwarmHost     4 <- reach_resync.go:122, peer_connections.go:2390 :2637 :3340
//	parseHostPort         0 <- DEAD at HEAD. 2 total occurrences: its doc and its own
//	                           declaration. Confirmed with `git grep HEAD` too.
//	describeReachAddress  0 <- DEAD at HEAD, same evidence.
//	no interface declares any of them.
//
// 🔑 SCOPE INFERENCE IS TIER PLACEMENT. scopeForHost's answer decides whether an
// address lands in Tier 0 (same-org direct dial) or Tier 1/2 (anycast/FQDN), so
// a wrong answer does not fail loudly — it silently files a dialable 6PN peer
// under the wrong tier and the direct path is never tried.

// ── Scope inference (LIVE) ──────────────────────────────────────────────────

// The scope table, kept in one place because both live functions must agree on
// every row (see TestBothScopeFunctionsAgreeOnEveryHostShape).
var scopeCases = []struct {
	host, want, why string
}{
	// 🔴 The load-bearing row: 6PN ULA must be private or it drops out of Tier 0a.
	{"fdaa::1", "private", "6PN ULA — the fly private network; public would drop it from Tier 0a"},
	{"fc00::1", "private", "IPv6 ULA fc00::/7"},
	{"10.0.0.5", "private", "RFC1918"},
	{"192.168.1.1", "private", "RFC1918"},
	{"172.16.0.1", "private", "RFC1918"},

	// 🔑 DELIBERATE AND SURPRISING: loopback and link-local are "public", not
	// "private". The doc gives the reason — they are meant to be dropped by
	// Tier 0a's post-parse filter, and calling them private would file them as
	// same-org direct-dial candidates instead.
	{"127.0.0.1", "public", "loopback — public so Tier 0a's filter drops it rather than mis-scoping it"},
	{"::1", "public", "IPv6 loopback, same reason"},
	{"fe80::1", "public", "IPv6 link-local, same reason"},
	{"169.254.1.1", "public", "IPv4 link-local, same reason"},

	{"8.8.8.8", "public", "global unicast"},
	{"2606:4700::1", "public", "global unicast IPv6"},
	{"node.hstles.com", "public", "FQDN — not an IP, resolved later on the dial path"},
	{"", "public", "empty — ParseIP fails, treated as a name"},
	{"100.64.0.1", "public", "CGNAT 100.64/10 is NOT IsPrivate() in Go"},
}

func TestScopeForHostPlacesEachHostShapeInTheRightTier(t *testing.T) {
	for _, tc := range scopeCases {
		if got := scopeForHost(tc.host); got != tc.want {
			t.Errorf("scopeForHost(%q) = %q, want %q\n    why: %s",
				tc.host, got, tc.want, tc.why)
		}
	}
}

// 🔴 THE PROPERTY THAT PROTECTS BOTH CALL PATHS. scopeForSwarmHost's doc says
// "Identical rule as scopeForHost — kept as a distinct symbol so each call site
// documents its intent independently." Two symbols with one rule is fine until
// someone edits one of them: the swarm AddressTable merge (4 call sites) and the
// interface-advertise path (1) would then disagree about the scope of the same
// host, and the same peer would be filed in different tiers depending on which
// path learned it. This test is the thing that fails when they diverge.
func TestBothScopeFunctionsAgreeOnEveryHostShape(t *testing.T) {
	for _, tc := range scopeCases {
		direct, swarm := scopeForHost(tc.host), scopeForSwarmHost(tc.host)
		if direct != swarm {
			t.Errorf("scopeForHost(%q)=%q but scopeForSwarmHost(%q)=%q — the "+
				"advertise path and the swarm merge now disagree, so the same peer "+
				"is filed in different tiers depending on which path learned it",
				tc.host, direct, tc.host, swarm)
		}
	}
}

// ── Address parsing (DEAD at HEAD — see the census above) ───────────────────
//
// parseHostPort has no callers. It is tested anyway because it is a correct,
// centralised parser that open-coded net.SplitHostPort sites in this package
// duplicate by hand. Pinning its real behaviour is what makes it safe to adopt,
// and the characterisation test below is the part an adopter would otherwise
// get wrong, because the doc promises more than the code delivers.

func TestParseHostPortHandlesEveryDocumentedAddressForm(t *testing.T) {
	const defPort = 41641
	for _, tc := range []struct {
		raw      string
		wantHost string
		wantPort uint16
		why      string
	}{
		{"", "", 0, "empty is the documented sentinel"},
		{"1.2.3.4:41641", "1.2.3.4", defPort, "IPv4 with explicit port"},
		{"1.2.3.4", "1.2.3.4", defPort, "IPv4, no port -> default"},
		{"[fdaa::1]:41641", "fdaa::1", defPort, "bracketed IPv6 with port — brackets stripped"},
		{"[fdaa::1]", "fdaa::1", defPort, "bracketed IPv6, no port — brackets still stripped"},
		{"node.hstles.com:41641", "node.hstles.com", defPort, "FQDN with port"},
		{"node.hstles.com", "node.hstles.com", defPort, "FQDN, no port"},
	} {
		h, p := parseHostPort(tc.raw, defPort)
		if h != tc.wantHost || p != tc.wantPort {
			t.Errorf("parseHostPort(%q) = (%q, %d), want (%q, %d)\n    why: %s",
				tc.raw, h, p, tc.wantHost, tc.wantPort, tc.why)
		}
	}
}

// 🔑 A BAD PORT IS REJECTED OUTRIGHT, and this half of the doc promise holds.
// Returning the host with the default port instead would silently redirect a
// peer that explicitly asked for a different port.
func TestParseHostPortRejectsAnUnusablePort(t *testing.T) {
	for _, raw := range []string{
		"1.2.3.4:0",     // port 0 is not dialable
		"1.2.3.4:99999", // overflows uint16
		"1.2.3.4:abc",   // not a number
		"1.2.3.4:-1",    // negative
	} {
		if h, p := parseHostPort(raw, 41641); h != "" || p != 0 {
			t.Errorf("parseHostPort(%q) = (%q, %d), want (\"\", 0) — an unusable "+
				"port must reject the whole entry, not fall back to the default "+
				"and silently dial a port the publisher did not ask for", raw, h, p)
		}
	}
}

// Characterisation, not approval: this is the gap an adopter would inherit.
//
// The doc says: "Returns ("", 0) on parse failure so the caller can skip the
// entry without polluting peer.addresses with garbage."
//
// MEASURED: that holds for a bad PORT (test above) and NOT for a bad HOST. Any
// string SplitHostPort rejects falls through to the no-embedded-port branch,
// which returns `raw` VERBATIM with the default port. So garbage does reach
// peer.addresses — the exact outcome the comment says it prevents.
//
// Pinned so the behaviour is visible and any change is deliberate.
func TestParseHostPortCurrentlyPassesGarbageHostsThrough(t *testing.T) {
	const defPort = 41641
	for _, raw := range []string{
		"1.2.3.4:41641:extra", // too many colons -> becomes a HOST with that name
		"not a host at all",
		"[unclosed",
		"!!!garbage!!!",
		"  ",
		":::::",
	} {
		h, p := parseHostPort(raw, defPort)
		if h != raw || p != defPort {
			t.Errorf("parseHostPort(%q) = (%q, %d) — it no longer passes garbage "+
				"through verbatim. That is very likely the FIX; update this test "+
				"deliberately", raw, h, p)
		}
	}
}

// describeReachAddress is debug-log formatting, and the only thing worth pinning
// is that every field reaches the output: a summary that silently drops the port
// or the scope is worse than no summary, because it is read during dial triage.
func TestDescribeReachAddressIncludesEveryField(t *testing.T) {
	got := describeReachAddress(lad.ReachAddress{
		Proto: "noise-udp", Host: "fdaa::1", Port: 41641, Scope: "private",
	})

	for _, want := range []string{"noise-udp", "fdaa::1", "41641", "private"} {
		if !containsSubstring(got, want) {
			t.Errorf("describeReachAddress() = %q, missing %q — a dial-triage log "+
				"line that drops a field sends the reader looking for the wrong "+
				"address", got, want)
		}
	}
}

func containsSubstring(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
