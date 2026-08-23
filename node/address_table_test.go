/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"sync"
	"testing"

	swarmpb "github.com/bbmumford/swarm/proto/pb"
)

// Covers the AddressTable accessors and transport mapping.
//
// transportString feeds swarmTransportToReachProto exactly as addressProtoToReach
// feeds directory's normaliseReachProto. Such a pair drifts apart silently —
// unmapped transports fall into the unknown priority rank, which ranks gRPC
// above WebSocket — so this pins the two halves against each other.

func TestTransportStringAndReachProtoAgreeOnEveryEnumValue(t *testing.T) {
	cases := []struct {
		enum      swarmpb.Address_Transport
		transport string // what the address table calls it
		reach     string // what the dialer consumes
	}{
		{swarmpb.Address_NOISE_UDP, "noise-udp", "udp"},
		{swarmpb.Address_WEBSOCKET, "websocket", "wss"},
		{swarmpb.Address_GRPC, "grpc", "grpc"},
		{swarmpb.Address_HTTP, "http", "http"},
		// The zero value must produce a name that maps to NOTHING dialable,
		// so an unset Transport is skipped rather than stored as a candidate.
		{swarmpb.Address_UNKNOWN, "unknown", ""},
	}

	for _, tc := range cases {
		got := transportString(tc.enum)
		if got != tc.transport {
			t.Errorf("transportString(%v) = %q, want %q", tc.enum, got, tc.transport)
		}
		// 🔑 THE COUPLING: every name this producer emits must be a name the
		// consumer recognises, or the candidate is silently dropped.
		if r := swarmTransportToReachProto(got); r != tc.reach {
			t.Errorf("swarmTransportToReachProto(transportString(%v)) = %q, want %q "+
				"— the address table emits a transport name its own dialer does "+
				"not consume, so every candidate of this type is skipped and the "+
				"peer silently loses that transport", tc.enum, r, tc.reach)
		}
	}
}

// ⚠ A KNOWN, DELIBERATE DIVERGENCE, PINNED SO IT STAYS DELIBERATE.
//
// For Address_HTTP the two producers disagree: this path yields "http" while
// lad_reach_bridge.go's addressProtoToReach yields "https". They feed
// DIFFERENT sinks — this one goes to peer.addresses, that one into LAD reach
// records — and directory's normaliseReachProto maps both onto "http", so
// they converge where it matters and nothing compares them where it does not.
//
// It is recorded because it looks like a mapping drift and is not one: if a
// future consumer ever compares the two sinks' Proto values directly, this is
// the line that explains why they differ.
func TestTheHTTPNamingDivergenceBetweenTheTwoProducersIsKnown(t *testing.T) {
	viaTable := swarmTransportToReachProto(transportString(swarmpb.Address_HTTP))
	viaBridge := addressProtoToReach(swarmpb.Address_HTTP)
	if viaTable == viaBridge {
		t.Fatalf("the two producers now AGREE on Address_HTTP (%q) — that is not "+
			"a failure, but this test documents a divergence that no longer "+
			"exists and should be deleted along with the comments citing it",
			viaTable)
	}
	if viaTable != "http" || viaBridge != "https" {
		t.Fatalf("the divergence changed shape: table=%q bridge=%q, was http/https "+
			"— directory's normaliseReachProto maps both to \"http\"; if either "+
			"side moved, check that the shim still covers it", viaTable, viaBridge)
	}
}

// transportPriority is the address table's OWN scale and it is INVERTED
// relative to directory's reachPriority: here higher wins and onRecord sorts
// with `>`; there lower wins and Reach sorts with `<`. Both are internally
// consistent, they live in different packages, and nothing catches a reader
// who carries the intuition across.
func TestTransportPriorityIsHigherIsBetterAndRanksNoiseUDPFirst(t *testing.T) {
	udp := transportPriority("noise-udp")
	ws := transportPriority("websocket")
	grpc := transportPriority("grpc")
	http := transportPriority("http")
	unknown := transportPriority("smoke-signal")

	if !(udp > ws && ws > grpc && grpc > http && http > unknown) {
		t.Fatalf("priority order is udp=%d ws=%d grpc=%d http=%d unknown=%d — "+
			"onRecord sorts with `>` (higher first), so this ordering decides "+
			"which transport is dialled first", udp, ws, grpc, http, unknown)
	}
	if unknown != 0 {
		t.Fatalf("unknown transport scores %d, want 0 — on a higher-is-better "+
			"scale anything above 0 lets an unrecognised transport outrank a "+
			"known one", unknown)
	}
}

// All must return a DEEP copy. It is called while other goroutines hold the
// table, so a caller that mutates the result must not be able to corrupt the
// live index — and the inner slices are the easy half to get wrong.
func TestAllReturnsADeepCopy(t *testing.T) {
	at := &AddressTable{byNode: map[string][]DialCandidate{
		testNodeIDA: {{NodeID: testNodeIDA, Transport: "noise-udp", Host: "fdaa:0:1234:a7b::2", Port: 41641}},
	}}

	snap := at.All()
	if len(snap) != 1 || len(snap[testNodeIDA]) != 1 {
		t.Fatalf("premise wrong: snapshot did not contain the seeded entry: %+v", snap)
	}

	// Mutate both levels of the returned structure.
	snap[testNodeIDA][0].Host = "corrupted"
	snap["injected"] = []DialCandidate{{NodeID: "injected"}}

	live := at.All()
	if live[testNodeIDA][0].Host != "fdaa:0:1234:a7b::2" {
		t.Fatalf("mutating the snapshot changed the live table: host is now %q — "+
			"All shares its inner slices, so any caller can corrupt the dial "+
			"index for every peer", live[testNodeIDA][0].Host)
	}
	if _, injected := live["injected"]; injected {
		t.Fatal("a key added to the snapshot appeared in the live table")
	}
}

func TestAllOnAnEmptyTableIsSafe(t *testing.T) {
	at := &AddressTable{}
	if got := at.All(); len(got) != 0 {
		t.Fatalf("All on an empty table returned %d entries", len(got))
	}
}

// The wake callback and router handles are set and read under separate locks
// from the record path, precisely so PeerRecord ingest never blocks on
// registration state. Round-trip plus detach.
func TestWakeCallbackRoundTripsAndDetaches(t *testing.T) {
	at := &AddressTable{}
	if at.wakeCallback() != nil {
		t.Fatal("a fresh table already has a wake callback")
	}

	var mu sync.Mutex
	calls := 0
	at.SetWakeCallback(func() { mu.Lock(); calls++; mu.Unlock() })

	cb := at.wakeCallback()
	if cb == nil {
		t.Fatal("SetWakeCallback did not register — the upgrade walker would " +
			"never be woken by a fresh Tier-0 address and would wait for its " +
			"30s ticker instead")
	}
	cb()
	mu.Lock()
	got := calls
	mu.Unlock()
	if got != 1 {
		t.Fatalf("the registered callback ran %d times, want 1", got)
	}

	at.SetWakeCallback(nil)
	if at.wakeCallback() != nil {
		t.Fatal("passing nil did not detach the callback — the documented way " +
			"to unwire it does not work")
	}
}

func TestRouterRoundTripsAndDefaultsToNil(t *testing.T) {
	at := &AddressTable{}
	if at.Router() != nil {
		t.Fatal("a fresh table already has a router")
	}
	// nil is the documented detached state; setting it must stay a no-op
	// rather than panicking, since InitSwarm may run before routing exists.
	at.SetRouter(nil)
	if at.Router() != nil {
		t.Fatal("SetRouter(nil) produced a non-nil router")
	}
}
