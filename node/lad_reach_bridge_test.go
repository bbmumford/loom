/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"testing"

	swarmpb "github.com/bbmumford/swarm/proto/pb"
)

// 🔑 THIS FILE PINS THE PRODUCER SIDE OF A CONTRACT THE CONSUMER ALREADY
// DEPENDS ON.
//
// directory/normaliseReachProto carries an explicitly-labelled compatibility
// shim mapping "wss"->"ws" and "https"->"http". That shim is
// correct ONLY as long as this file keeps emitting exactly those strings —
// and nothing tested that. A change here would silently re-open the ranking
// drift where unmapped transports fall into the unknown rank and
// gRPC outranked WebSocket.
//
// So: the exact output vocabulary is asserted here, and directory's
// reachvocab_test.go asserts those same literals normalise correctly. The two
// tests are the contract; neither is meaningful alone.

func TestAddressProtoToReachEmitsTheDocumentedWireVocabulary(t *testing.T) {
	cases := []struct {
		in   swarmpb.Address_Transport
		want string
	}{
		// 🛑 NOISE_UDP MUST be "udp", never "noise-udp" — forwarder.go's 6PN
		// filter, the cross-origin classification and bestAddress all key on
		// Proto == "udp". This is the function's own documented invariant.
		{swarmpb.Address_NOISE_UDP, "udp"},
		{swarmpb.Address_WEBSOCKET, "wss"},
		{swarmpb.Address_GRPC, "grpc"},
		{swarmpb.Address_HTTP, "https"},
		// The zero value maps to nothing dialable, deliberately.
		{swarmpb.Address_UNKNOWN, ""},
	}
	for _, tc := range cases {
		if got := addressProtoToReach(tc.in); got != tc.want {
			t.Errorf("addressProtoToReach(%v) = %q, want %q — directory's "+
				"compatibility shim maps the expected string and will drop this "+
				"one into the unknown priority rank, silently reordering dial "+
				"candidates", tc.in, got, tc.want)
		}
	}
}

// 🔴 THE DEFECT THIS WAS WRITTEN AGAINST: an Address whose Transport is unset
// would otherwise be stored with an EMPTY Proto.
//
// Address_UNKNOWN is the enum's ZERO VALUE, so this needs no malice and no
// corruption — a producer that omits the field, or any build that does not set
// it, gets there by default. The resulting entry is undialable by every path
// while still counting as one of the peer's addresses.
func TestReachAddrsFromPBSkipsUnmappableTransports(t *testing.T) {
	in := []*swarmpb.Address{
		{Transport: swarmpb.Address_NOISE_UDP, Host: "fdaa:0:1234:a7b::2", Port: 41641, Scope: "private"},
		{Transport: swarmpb.Address_UNKNOWN, Host: "203.0.113.9", Port: 443, Scope: "public"},
		nil,
		{Transport: swarmpb.Address_WEBSOCKET, Host: "devices.orbtr.io", Port: 443, Scope: "public"},
	}

	got := reachAddrsFromPB(testNodeIDA, in)

	if len(got) != 2 {
		t.Fatalf("got %d addresses, want 2 (the udp and wss entries) — %+v", len(got), got)
	}
	for _, a := range got {
		if a.Proto == "" {
			t.Fatalf("an address with an EMPTY Proto was stored: %+v — it is "+
				"undialable by every path yet counts as one of the peer's "+
				"addresses, so the peer looks reachable and is not", a)
		}
	}

	// Control: the mappable entries are preserved intact, or "skip everything"
	// would satisfy the assertion above.
	if got[0].Proto != "udp" || got[0].Host != "fdaa:0:1234:a7b::2" || got[0].Port != 41641 {
		t.Fatalf("the noise-udp entry was not preserved: %+v", got[0])
	}
	if got[1].Proto != "wss" || got[1].Host != "devices.orbtr.io" {
		t.Fatalf("the websocket entry was not preserved: %+v", got[1])
	}
	if got[0].Scope != "private" || got[1].Scope != "public" {
		t.Fatalf("scope was not carried through: %q / %q", got[0].Scope, got[1].Scope)
	}
}

// An all-unmappable record must yield an empty slice, not a slice of blanks.
func TestReachAddrsFromPBReturnsEmptyRatherThanBlanks(t *testing.T) {
	got := reachAddrsFromPB(testNodeIDA, []*swarmpb.Address{
		{Transport: swarmpb.Address_UNKNOWN, Host: "203.0.113.9", Port: 443},
		{Transport: swarmpb.Address_Transport(99), Host: "203.0.113.10", Port: 443},
	})
	if len(got) != 0 {
		t.Fatalf("got %d addresses from an all-unmappable record, want 0 — %+v "+
			"(an unknown FUTURE enum value takes the same path as UNKNOWN, which "+
			"is what makes this forward-compatible rather than merely defensive)",
			len(got), got)
	}
}
