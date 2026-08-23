/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"errors"
	"fmt"
	"net"
	"testing"
)

// COVERAGE of the Fly-internal DNS fallback path's pure helpers, all
// at 0.0%: isTransientDNSError (:3617), isDNSError (:3641), stripURLHost
// (:3662), mergeMetadata (:3687), flyInternalAddress (:3718).
//
// ⚠ DUPLICATION CHECK FIRST: no existing test in node/ mentions any of the five
// (measured by name across node/*_test.go). Each has exactly ONE production
// call site, so they are wired, not dead — a break here changes real dial
// behaviour rather than nothing.
//
// These five are one story: when the public resolver fails, the dialer decides
// whether the failure is DNS at all, whether it is worth retrying, derives the
// .flycast address to try instead, and carries the original hostname forward as
// the TLS SNI hint. Every step is a pure function, which is why they are worth
// pinning precisely.

// ─── the classification hierarchy ────────────────────────────────────────

// 🔑 THE CONTAINMENT PROPERTY, WHICH NOTHING IN THE CODE ENFORCES: every
// TRANSIENT DNS error must also be a DNS error. The two predicates are written
// independently, over two separate substring lists, and they gate two different
// behaviours — retry (transient) and the Fly-internal fallback (any DNS error).
//
// If containment broke, an error could be "worth retrying" while not being "a
// DNS failure": the caller would retry the public resolver forever and never
// try the separate 6PN plane that exists precisely for that case. The lists are
// what drift — isDNSError's has four entries and isTransientDNSError's has two.
func TestEveryTransientDNSErrorIsAlsoADNSError(t *testing.T) {
	for _, err := range dnsErrorCorpus() {
		if isTransientDNSError(err) && !isDNSError(err) {
			t.Errorf("isTransientDNSError=true but isDNSError=false for %q — the caller would "+
				"retry the public resolver and never reach the Fly-internal fallback",
				err)
		}
	}
}

// dnsErrorCorpus() spans both branches of both predicates: the typed *net.DNSError
// path and the substring path the dialer leaves behind when it loses the
// wrapping.
func dnsErrorCorpus() []error {
	return []error{
		nil,
		errors.New("connection refused"),
		errors.New("dial tcp: lookup node.hstles.com: i/o timeout"),
		errors.New("dial tcp: lookup node.hstles.com: server misbehaving"),
		errors.New("dial tcp: lookup node.hstles.com: no such host"),
		errors.New("Temporary failure in name resolution"),
		errors.New("lookup node.hstles.com: Temporary failure in name resolution"),
		&net.DNSError{Err: "i/o timeout", Name: "node.hstles.com", IsTimeout: true},
		&net.DNSError{Err: "server misbehaving", Name: "node.hstles.com", IsTemporary: true},
		&net.DNSError{Err: "no such host", Name: "node.hstles.com", IsNotFound: true},
		fmt.Errorf("dialing peer: %w", &net.DNSError{Err: "i/o timeout", IsTimeout: true}),
	}
}

// 🔴 "no such host" IS PERMANENT AND MUST NOT BE CALLED TRANSIENT — the comment
// on the substring fallback says so explicitly, and it is the one exclusion that
// list makes. Treating NXDOMAIN as transient means retrying a hostname that will
// never resolve, on every dial tick, for the life of the process.
//
// But it IS a DNS error, and isDNSError's doc explains why that matters:
// "NXDOMAIN can indicate a DNS plane that's lying about a real hostname
// (observed during regional resolver outages on Fly)" — so the Fly-internal
// fallback SHOULD fire for it. The two predicates disagreeing on this one input
// is the entire design, and this test is what pins the disagreement.
func TestNoSuchHostIsPermanentButStillWorthTheInternalFallback(t *testing.T) {
	for _, err := range []error{
		errors.New("dial tcp: lookup node.hstles.com: no such host"),
		&net.DNSError{Err: "no such host", Name: "node.hstles.com", IsNotFound: true},
	} {
		if isTransientDNSError(err) {
			t.Errorf("NXDOMAIN classified TRANSIENT for %q — the dialer would retry a hostname "+
				"that will never resolve, forever", err)
		}
		if !isDNSError(err) {
			t.Errorf("NXDOMAIN not classified as a DNS error for %q — the Fly-internal 6PN "+
				"fallback would not fire, and its doc names exactly this case", err)
		}
	}
}

func TestNeitherPredicateFiresOnANilOrNonDNSError(t *testing.T) {
	for _, err := range []error{nil, errors.New("connection refused"), errors.New("EOF")} {
		if isTransientDNSError(err) {
			t.Errorf("isTransientDNSError(%v) = true", err)
		}
		if isDNSError(err) {
			t.Errorf("isDNSError(%v) = true — a non-DNS failure would divert the dialer onto "+
				"the Fly-internal resolver path, which cannot help it", err)
		}
	}
}

// A typed *net.DNSError that is neither timeout nor temporary must not be
// transient — the typed branch returns IsTimeout||IsTemporary and must not fall
// through to the substring list, which would classify by message text instead.
func TestATypedDNSErrorIsClassifiedByItsFlagsNotItsText(t *testing.T) {
	// Message text says "i/o timeout" (which the substring list WOULD match)
	// while the flags say permanent. Flags win.
	err := &net.DNSError{Err: "lookup failed: i/o timeout", Name: "x", IsNotFound: true}

	if isTransientDNSError(err) {
		t.Error("a typed DNSError with IsTimeout=false and IsTemporary=false was classified " +
			"transient — the typed branch is falling through to substring matching, so a " +
			"permanent failure whose text mentions a timeout would be retried forever")
	}
	if !isDNSError(err) {
		t.Error("a typed *net.DNSError was not recognised as a DNS error")
	}
}

// ─── host derivation ─────────────────────────────────────────────────────

func TestStripURLHostHandlesEveryFormCallersPassAround(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"", ""},
		{"node.hstles.com", "node.hstles.com"},
		{"node.hstles.com:443", "node.hstles.com"},
		{"wss://node.hstles.com/mesh", "node.hstles.com"},
		{"wss://node.hstles.com:443/mesh", "node.hstles.com"},
		{"https://node.hstles.com:8443/", "node.hstles.com"},
		{"ws://node.hstles.com", "node.hstles.com"},
		{"http://node.hstles.com?x=1", "node.hstles.com"},
		{"https://node.hstles.com#frag", "node.hstles.com"},
	} {
		if got := stripURLHost(tc.in); got != tc.want {
			t.Errorf("stripURLHost(%q) = %q, want %q — this value becomes the TLS SNI hint, so "+
				"a wrong host fails certificate validation on the fallback path", tc.in, got, tc.want)
		}
	}
}

// 🔑 THE PAIRING THAT MAKES THE FALLBACK WORK, and it is stated in
// flyInternalAddress's own doc: the caller dials the .flycast address but keeps
// the ORIGINAL public hostname for TLS SNI, "so cert validation still works".
// Two functions, one contract, nothing forcing them to agree — so this asserts
// them together rather than separately.
func TestTheFlycastAddressAndTheSNIHintAreDerivedFromTheSameHost(t *testing.T) {
	for _, tc := range []struct{ in, wantSNI, wantFlycast string }{
		{"node.hstles.com", "node.hstles.com", "node-hstles-com.flycast"},
		{"wss://app.orbtr.io:443/mesh", "app.orbtr.io", "app-orbtr-io.flycast"},
		{"bootstrap.hstles.com:8443", "bootstrap.hstles.com", "bootstrap-hstles-com.flycast"},
	} {
		sni := stripURLHost(tc.in)
		flycast := flyInternalAddress(sni)

		if sni != tc.wantSNI {
			t.Errorf("stripURLHost(%q) = %q, want %q", tc.in, sni, tc.wantSNI)
		}
		if flycast != tc.wantFlycast {
			t.Errorf("flyInternalAddress(%q) = %q, want %q", sni, flycast, tc.wantFlycast)
		}
	}
}

// Returns "" — meaning "no Fly equivalent, do not attempt the fallback" — for
// inputs that already bypass public DNS or have no app slug. Returning a
// non-empty address for any of these would send the dialer at a name that
// cannot resolve, replacing one failure with a slower one.
func TestFlyInternalAddressDeclinesInputsWithNoFlyEquivalent(t *testing.T) {
	for _, in := range []string{
		"",
		"10.0.0.1", "10.0.0.1:443", // raw IPv4, with and without port
		"::1", "[fdaa::3]:443", // raw IPv6, with and without brackets
		"printer.local",
		"app.internal",
		"app-orbtr-io.flycast", // already in flycast form — must not double-convert
	} {
		if got := flyInternalAddress(in); got != "" {
			t.Errorf("flyInternalAddress(%q) = %q, want \"\" — the dialer would attempt a "+
				"name with no Fly equivalent instead of declining the fallback", in, got)
		}
	}
}

// ─── metadata ────────────────────────────────────────────────────────────

// "Never mutates base" is the documented contract, and it is the one that bites:
// base is peer metadata held in the connection map, so a mutating merge would
// write a single dial's overrides into shared peer state.
func TestMergeMetadataNeverMutatesBaseAndOverridesWin(t *testing.T) {
	base := map[string]string{"region": "syd", "org": "orbtr"}
	overrides := map[string]string{"region": "fra", "sni": "node.hstles.com"}

	got := mergeMetadata(base, overrides)

	if got["region"] != "fra" {
		t.Errorf("overrides did not win: region = %q, want %q", got["region"], "fra")
	}
	if got["org"] != "orbtr" || got["sni"] != "node.hstles.com" {
		t.Errorf("merged map lost entries: %v", got)
	}
	if base["region"] != "syd" {
		t.Errorf("BASE WAS MUTATED: region = %q, want %q — a single dial's override has been "+
			"written into shared peer metadata", base["region"], "syd")
	}
	if len(base) != 2 {
		t.Errorf("base grew to %v — merge added a key to the caller's map", base)
	}
	// Aliasing check: the result must not BE either input.
	got["probe"] = "x"
	if _, leaked := base["probe"]; leaked {
		t.Error("the merged map aliases base — writing to the result writes through to peer state")
	}
	if _, leaked := overrides["probe"]; leaked {
		t.Error("the merged map aliases overrides")
	}
}

func TestMergeMetadataIsNilSafeOnEitherSide(t *testing.T) {
	if got := mergeMetadata(nil, nil); got == nil || len(got) != 0 {
		t.Errorf("mergeMetadata(nil, nil) = %v, want an empty non-nil map", got)
	}
	if got := mergeMetadata(nil, map[string]string{"a": "1"}); got["a"] != "1" {
		t.Errorf("mergeMetadata(nil, overrides) lost the overrides: %v", got)
	}
	if got := mergeMetadata(map[string]string{"a": "1"}, nil); got["a"] != "1" {
		t.Errorf("mergeMetadata(base, nil) lost the base: %v", got)
	}
}
