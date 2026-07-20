/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"context"
	"testing"

	"github.com/ORBTR/aether/nat"
	"github.com/bbmumford/reach"
)

// TestNATClassifyIdempotent verifies that classifyNATBehaviour is "first
// success wins": once natStrategy is populated, a subsequent call is a
// no-op and must not overwrite the existing strategy. This mirrors the
// ORBTR agent's DiscoverNAT idempotency guard.
func TestNATClassifyIdempotent(t *testing.T) {
	rt := &Runtime{}

	// Simulate a prior successful classification.
	want := nat.NewStrategy(nat.StrategyConfig{})
	rt.natBehaviourMu.Lock()
	rt.natStrategy = want
	rt.natBehaviour = nat.NATBehaviour{Mapping: nat.MappingEndpointIndependent}
	rt.natBehaviourMu.Unlock()

	// A second classification pass must short-circuit on the guard and
	// leave the stored strategy + behaviour untouched — even though
	// listenTransports is empty (no STUN client reachable).
	rt.classifyNATBehaviour(context.Background())

	if got := rt.NATStrategy(); got != want {
		t.Fatalf("classifyNATBehaviour overwrote natStrategy: got %p, want %p (idempotency guard missing)", got, want)
	}
	if got := rt.NATBehaviour().Mapping; got != nat.MappingEndpointIndependent {
		t.Fatalf("classifyNATBehaviour overwrote natBehaviour: got mapping %s, want %s", got, nat.MappingEndpointIndependent)
	}
}

// TestNATClassifyNoTransportsSafe verifies classifyNATBehaviour is safe to
// call when no noise transport / STUN client is available (the common
// case on a freshly-constructed runtime) — it must not panic and must
// leave natStrategy nil so callers can detect "classification pending".
func TestNATClassifyNoTransportsSafe(t *testing.T) {
	rt := &Runtime{}

	rt.classifyNATBehaviour(context.Background())

	if rt.NATStrategy() != nil {
		t.Fatalf("expected natStrategy to remain nil with no transports, got non-nil")
	}
}

// TestNATBehaviourAdvertiseRoundTrip verifies that a node's detected
// nat.NATBehaviour survives the publish→decode round trip through a
// reach record's Metadata: encodeNATBehaviour writes a compact class
// string into the metadata map (the same map startReachPublisher folds
// into the signed ReachRecord, and which the ledger cache surfaces as
// lad.MemberRecord.Attrs), and DecodeNATBehaviour recovers the identical
// classification from a peer's record. This is Phase-1 data-only — it
// only proves publish + decode, no dial logic.
func TestNATBehaviourAdvertiseRoundTrip(t *testing.T) {
	want := nat.NATBehaviour{
		Mapping:   nat.MappingAddressPortDependent,
		Filtering: nat.FilteringAddressDependent,
	}

	// Simulate the publish side: a fleet node folds its NAT class into
	// the reach metadata map exactly the way startReachPublisher does.
	meta := reach.Metadata{
		"serviceName": "devices.orbtr.io",
		"http_port":   "8080",
	}
	if enc := encodeNATBehaviour(want); enc != "" {
		meta[natClassMetaKey] = enc
	}

	// Simulate a peer's dialer reading the record back. reach.Metadata
	// flows verbatim into lad.MemberRecord.Attrs, so the decoder takes a
	// plain map[string]string and works for either carrier.
	got := DecodeNATBehaviour(meta)
	if got != want {
		t.Fatalf("NAT class did not round-trip: got %s, want %s", got, want)
	}
}

// TestNATBehaviourDecodeMissingKey verifies forward compatibility: a
// peer whose record predates the nat_class key (older code) decodes to
// the zero-value NATBehaviour (Unknown/Unknown) rather than panicking or
// producing a misleading classification.
func TestNATBehaviourDecodeMissingKey(t *testing.T) {
	if got := DecodeNATBehaviour(reach.Metadata{"http_port": "8080"}); got != (nat.NATBehaviour{}) {
		t.Fatalf("absent nat_class key must decode to zero-value, got %s", got)
	}
	if got := DecodeNATBehaviour(nil); got != (nat.NATBehaviour{}) {
		t.Fatalf("nil metadata must decode to zero-value, got %s", got)
	}
}

// TestNATBehaviourDecodeMalformed verifies the decoder is robust to a
// garbled or out-of-range nat_class value (a peer running a future
// schema, or corruption) — it must fall back to the zero-value rather
// than panic or return an invalid enum.
func TestNATBehaviourDecodeMalformed(t *testing.T) {
	cases := []string{"", "garbage", "1", "1:", ":2", "1:2:3", "99:99", "-1:0", "x:y"}
	for _, c := range cases {
		if got := DecodeNATBehaviour(reach.Metadata{natClassMetaKey: c}); got != (nat.NATBehaviour{}) {
			t.Fatalf("malformed nat_class %q must decode to zero-value, got %s", c, got)
		}
	}
}
