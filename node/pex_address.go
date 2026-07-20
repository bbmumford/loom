/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"fmt"
	"net"
	"strconv"

	lad "github.com/bbmumford/ledger"
)

// parseHostPort accepts both "host" and "host:port" forms and returns
// (host, port). When no port is embedded the default is used. Handles
// bracketed IPv6 ("[fdaa::]:41641") via net.SplitHostPort. Returns
// ("", 0) on parse failure so the caller can skip the entry without
// polluting peer.addresses with garbage.
func parseHostPort(raw string, defaultUDPPort uint16) (string, uint16) {
	if raw == "" {
		return "", 0
	}
	// SplitHostPort accepts IPv6 ("[fdaa::]:41641"), IPv4
	// ("1.2.3.4:41641"), and FQDN ("node.hstles.com:41641") variants
	// uniformly. When it errors there's no embedded port.
	if h, p, err := net.SplitHostPort(raw); err == nil {
		if n, perr := strconv.ParseUint(p, 10, 16); perr == nil && n > 0 {
			return h, uint16(n)
		}
		return "", 0
	}
	// No embedded port. Strip IPv6 brackets if present, keep IPv4 / FQDN
	// as-is. Bracket stripping handles "[fdaa::]"-style hostnames a
	// previous publisher may have stringified.
	host := raw
	if len(host) >= 2 && host[0] == '[' && host[len(host)-1] == ']' {
		host = host[1 : len(host)-1]
	}
	return host, defaultUDPPort
}

// scopeForHost infers the swarm/LAD Scope tag from the host shape so
// the address lands in the right bestAddress tier — Tier 0 expects
// Scope="private" for same-org direct dials, Tier 1/2 expect
// Scope="public" for anycast / FQDN entries. Rules:
//
//   - Non-IP host (FQDN, parse failure) → "public" — the dial path
//     does DNS resolution later.
//   - Loopback or link-local → "public" so they're skipped by Tier 0a's
//     post-parse filter rather than mis-scoped.
//   - 6PN ULA (fdaa::/8) or any IsPrivate() IP (RFC1918, IPv6 ULA
//     fc00::/7) → "private".
//   - Anything else → "public".
func scopeForHost(host string) string {
	ip := net.ParseIP(host)
	if ip == nil {
		return "public"
	}
	if ip.IsLoopback() || ip.IsLinkLocalUnicast() {
		return "public"
	}
	if ip.IsPrivate() {
		return "private"
	}
	return "public"
}

// scopeForSwarmHost is the swarm-merge-side analog of scopeForHost.
// Used by the swarm AddressTable merge to default an unscoped
// DialCandidate based on host shape rather than blanket-coercing to
// "public", which would drop 6PN ULAs from Tier 0a coverage.
// Identical rule as scopeForHost — kept as a distinct symbol so each
// call site documents its intent independently.
func scopeForSwarmHost(host string) string {
	return scopeForHost(host)
}

// describeReachAddress returns a short single-line summary suitable for
// debug logging. Kept here so this file owns the lad.ReachAddress
// formatting and the helper string doesn't leak into peer_connections.go.
func describeReachAddress(a lad.ReachAddress) string {
	return fmt.Sprintf("%s://%s:%d scope=%s", a.Proto, a.Host, a.Port, a.Scope)
}
