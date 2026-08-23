/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"testing"
)

// Covers the grade→protocol map and the connection budget gate:
// ProtocolForGrade, CanConnect, EffectiveMaxPerPeer.
//
// The budget gate is the only thing standing between the mesh and unbounded
// connection growth, and every one of its failure modes is silent.

// ─── ProtocolForGrade: a PARTIAL inverse, and its sharp edge ────────────────
//
// GradeForProtocol is many-to-one (WebSocket→C and TLS→C; QUIC→B and gRPC→B),
// so ProtocolForGrade can only pick one representative per grade. That is
// fine for the grades it is used on. The sharp edge is its `default` arm:
// GradeF — "no connection at all" — yields ProtoWebSocket, a real, dialable
// Grade-C protocol.

func TestProtocolForGradeRoundTripsForTheGradesItIsUsedOn(t *testing.T) {
	// Every grade the router actually forwards must survive
	// grade → protocol → grade unchanged, or the dial targets a transport
	// class other than the one the connection was measured at.
	for _, g := range []Grade{GradeA, GradeB} {
		if got := GradeForProtocol(ProtocolForGrade(g)); got != g {
			t.Errorf("round trip of %v yielded %v via protocol %v — a direct "+
				"route measured at one grade would be dialled at another",
				g, got, ProtocolForGrade(g))
		}
	}
}

func TestProtocolForGradeDefaultArmReturnsADialableProtocolForNoConnection(t *testing.T) {
	// This is NOT a defect report — it is the hazard being written down so the
	// guard test below has something to be a guard AGAINST. The function's own
	// comment says the arm "shouldn't be reached for A/B routing", and no live
	// caller reaches it. This pins what would happen if one ever did.
	got := ProtocolForGrade(GradeF)
	if got != ProtoWebSocket {
		t.Fatalf("premise changed: ProtocolForGrade(GradeF) = %v, not %v — "+
			"re-read the guard coupling below before trusting it", got, ProtoWebSocket)
	}
	if GradeForProtocol(got) == GradeF {
		t.Fatal("premise wrong: the default arm round-trips, so there is no " +
			"sharp edge here and the guard test below is unnecessary")
	}
	// GradeF means "not connected". ProtoWebSocket is a live transport class.
	// Reaching this arm therefore converts "no connection" into "dial a
	// WebSocket", which is why the call site must never let it happen.
}

// gradeReporter serves one canned ConnectionInfo. It embeds
// NilConnectionReporter so it satisfies the whole interface while overriding
// only the method under test — adding a method to ConnectionReporter must not
// break this fixture.
type gradeReporter struct {
	NilConnectionReporter
	info ConnectionInfo
}

func (g gradeReporter) ConnectionTo(string) (ConnectionInfo, bool) { return g.info, true }

// 🔴 THE COUPLING TEST, AND IT SPANS TWO FILES.
//
// ProtocolForGrade's default arm is safe ONLY because its sole live caller,
// topology_router.go, returns early on `conn.Grade < GradeB`. The function and
// the thing
// that makes it safe are in different files, so relaxing that threshold to
// admit Grade-C routing would silently feed GradeF into the default arm and
// dial peers the reporter says are NOT CONNECTED.
//
// This test fails if that guard is loosened.
func TestRouteToNodeNeverReachesProtocolForGradeBelowGradeB(t *testing.T) {
	ctx := context.Background()

	for _, tc := range []struct {
		name       string
		grade      Grade
		wantMiss   bool // rejected by the guard, ProtocolForGrade NOT reached
		wantThruTo bool // passed the guard, ProtocolForGrade reached
	}{
		{"GradeF is not a connection at all", GradeF, true, false},
		{"GradeC is below the direct-route threshold", GradeC, true, false},
		{"GradeB is routable", GradeB, false, true},
		{"GradeA is routable", GradeA, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// connMgr is nil deliberately: it makes "got past the grade guard"
			// observable as a Fallback without needing a dialable transport.
			tr := NewTopologyRouter(gradeReporter{info: ConnectionInfo{Grade: tc.grade}}, nil, nil)

			session, err := tr.RouteToNode(ctx, testNodeIDB)
			if err != nil {
				t.Fatalf("RouteToNode returned an error: %v — it is documented to "+
					"fall back gracefully rather than propagate", err)
			}
			if session != nil {
				t.Fatal("premise wrong: a session was returned with a nil connMgr")
			}

			st := tr.Stats()
			if tc.wantMiss && st.GradeMisses != 1 {
				t.Fatalf("grade %v was NOT rejected by the direct-route guard "+
					"(GradeMisses=%d, Fallbacks=%d). ProtocolForGrade now receives "+
					"grades it was never meant to see — and at GradeF that arm "+
					"converts 'peer is not connected' into 'dial it over WebSocket'",
					tc.grade, st.GradeMisses, st.Fallbacks)
			}
			if tc.wantThruTo && st.Fallbacks != 1 {
				t.Fatalf("grade %v did not reach the protocol selection "+
					"(GradeMisses=%d, Fallbacks=%d) — direct routing is off for a "+
					"grade that should have it", tc.grade, st.GradeMisses, st.Fallbacks)
			}
		})
	}
}

// ─── The budget gate ───────────────────────────────────────────────────────

// CanConnect has two independent rejection axes. A test that only saturates
// one of them cannot tell a working gate from one that dropped the other
// check — and dropping either fails OPEN, admitting connections forever.
func TestCanConnectRejectsOnBothAxesIndependently(t *testing.T) {
	t.Run("total cap", func(t *testing.T) {
		b := &ConnectionBudget{MaxPerPeer: 2, MaxTotal: 2, priorities: map[string]ConnectionPriority{}}
		if !b.CanConnect(0) {
			t.Fatal("premise wrong: a fresh budget refused the first connection")
		}
		for i := 0; i < 2; i++ {
			if !b.Acquire() {
				t.Fatalf("premise wrong: slot %d could not be taken", i)
			}
		}
		if b.CanConnect(0) {
			t.Fatal("the TOTAL cap was reached and CanConnect still said yes, for " +
				"a peer with zero connections of its own — MaxTotal is not enforced")
		}
	})

	t.Run("per-peer cap", func(t *testing.T) {
		b := &ConnectionBudget{MaxPerPeer: 2, MaxTotal: 50, priorities: map[string]ConnectionPriority{}}
		if !b.CanConnect(b.MaxPerPeer - 1) {
			t.Fatal("a peer one below its own cap was refused while the total " +
				"budget was nearly empty — the boundary is off by one")
		}
		if b.CanConnect(b.MaxPerPeer) {
			t.Fatal("a peer AT its per-peer cap was allowed another connection " +
				"while total headroom remained — MaxPerPeer is not enforced, so a " +
				"single flapping peer can consume the whole budget")
		}
	})
}

// The cross-region bonus is the one thing that makes the per-peer cap
// conditional. If it applied unconditionally every peer would silently get
// the higher cap; if it never applied, cross-region peers would be held to
// the same-region limit and lose their redundancy.
func TestEffectiveMaxPerPeerAppliesTheBonusOnlyAcrossRegions(t *testing.T) {
	b := &ConnectionBudget{MaxPerPeer: 2, MaxTotal: 50, CrossRegionBonus: 1,
		priorities: map[string]ConnectionPriority{}}

	if b.CrossRegionBonus == 0 {
		t.Fatal("premise wrong: a zero bonus makes both branches identical")
	}
	if got, want := b.EffectiveMaxPerPeer(false), b.MaxPerPeer; got != want {
		t.Fatalf("same-region cap = %d, want %d — the cross-region bonus is "+
			"being applied to every peer", got, want)
	}
	if got, want := b.EffectiveMaxPerPeer(true), b.MaxPerPeer+b.CrossRegionBonus; got != want {
		t.Fatalf("cross-region cap = %d, want %d — cross-region peers are held "+
			"to the same-region limit and lose their redundancy", got, want)
	}
}
