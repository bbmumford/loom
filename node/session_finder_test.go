/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"strings"
	"testing"

	"github.com/ORBTR/aether"
)

// COVERAGE of the dispatch session-selection surface, all 0.0%:
// FindSession's three paths, FindAnySession, and IsSupersededByUpgrade.
//
// Censused first: every one is driven, and they implement
// dispatch.SessionFinder — so a wrong answer here is a wrong ROUTE, not a
// wrong number. Each failure mode below is silent: the call still succeeds,
// against the wrong peer or over a worse transport.

// finderManager supplies a Runtime because findNodeForRole's own guard
// dereferences it before testing it — `if m.rt.swarm == nil` panics on a nil
// rt. Production never builds a ConnectionManager without a Runtime, so this
// is a fixture requirement, not a missing guard (the same
// conclusion about a sibling and correctly fixed the FIXTURE).
//
// swarm is left nil on purpose: findNodeForRole then returns "" — "no node
// for this role" — which is the state paths 2 and 3 below exercise.
func finderManager(t *testing.T) *ConnectionManager {
	t.Helper()
	m := registerTestManager()
	m.selfID = testNodeIDA
	m.rt = &Runtime{}
	return m
}

// bind registers a session for a node the way registerMeshSession would,
// without the proving/upgrade machinery — this file is about SELECTION, not
// registration, which mesh_connection_register_test.go already covers.
func bind(m *ConnectionManager, nodeID string, s *probeSession) {
	s.remote = aether.NodeID(nodeID)
	m.dispatchMu.Lock()
	if m.meshSessions == nil {
		m.meshSessions = map[string]aether.Session{}
	}
	m.meshSessions[nodeID] = s
	m.dispatchMu.Unlock()
}

// 🔴 IsSupersededByUpgrade decides whether the caller DROPS a cached session.
// Both directions cost: too eager and a healthy session is evicted on every
// equal-grade re-register (churn); too lax and a cold-start cross-region WS
// session sticks for the full 5-minute idle TTL after a same-region noise-UDP
// session has arrived — the L3 #7 case this exists for.
//
// The SAME-grade row is the one that pins `>` rather than `>=`.
func TestIsSupersededByUpgradeGradeComparison(t *testing.T) {
	cases := []struct {
		name    string
		cached  func() *probeSession
		current func() *probeSession
		want    bool
	}{
		{"current is HIGHER grade — cached is superseded", wsSession, noiseSession, true},
		{"current is LOWER grade — cached stays", noiseSession, wsSession, false},
		{"current is the SAME grade — cached stays", wsSession, wsSession, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := finderManager(t)
			cached := tc.cached()
			cached.remote = aether.NodeID(testNodeIDB)
			current := tc.current()
			bind(m, testNodeIDB, current)

			if SessionGrade(cached) == SessionGrade(current) && tc.want {
				t.Fatal("premise wrong: equal grades cannot supersede")
			}

			if got := m.IsSupersededByUpgrade(cached); got != tc.want {
				t.Fatalf("IsSupersededByUpgrade = %v, want %v (cached=%v current=%v). "+
					"A false positive evicts a healthy session on every equal-grade "+
					"re-register; a false negative pins a worse transport for the "+
					"full idle TTL after a better one arrives",
					got, tc.want, SessionGrade(cached), SessionGrade(current))
			}
		})
	}
}

// The guards, each of which must answer "not superseded" rather than panic or
// evict. A wrong answer on any of these drops a session for a reason that has
// nothing to do with grade.
func TestIsSupersededByUpgradeGuardsFailClosed(t *testing.T) {
	m := finderManager(t)

	if m.IsSupersededByUpgrade(nil) {
		t.Error("a nil session was reported superseded")
	}

	closed := wsSession()
	closed.remote = aether.NodeID(testNodeIDB)
	closed.closed = true
	bind(m, testNodeIDB, noiseSession()) // a strictly better current session
	if m.IsSupersededByUpgrade(closed) {
		t.Error("a CLOSED session was reported superseded — it is already gone, " +
			"and reporting it supersedable invites a second eviction path")
	}

	noRemote := wsSession() // remote left empty
	if m.IsSupersededByUpgrade(noRemote) {
		t.Error("a session with no RemoteNodeID was reported superseded — there " +
			"is no peer to compare against")
	}

	unknown := wsSession()
	unknown.remote = aether.NodeID("dd44dd44dd44dd44dd44dd44dd44dd44dd44dd44dd44dd44dd44dd44dd44dd44")
	if m.IsSupersededByUpgrade(unknown) {
		t.Error("a session whose peer has NO registered session was reported " +
			"superseded — absence is being read as a better path")
	}

	// The identity case: the cached session IS the current one.
	same := wsSession()
	bind(m, testNodeIDB, same)
	if m.IsSupersededByUpgrade(same) {
		t.Error("a session was reported superseded BY ITSELF")
	}
}

func TestFindAnySessionDelegatesAndFailsClosed(t *testing.T) {
	m := finderManager(t)

	if _, ok := m.FindAnySession(); ok {
		t.Fatal("premise wrong: a manager with no sessions returned one")
	}

	bind(m, testNodeIDB, wsSession())
	got, ok := m.FindAnySession()
	if !ok || got == nil {
		t.Fatal("FindAnySession found nothing with a session registered — mesh " +
			"forwarding has no path even though a peer is connected")
	}

	var nilMgr *ConnectionManager
	if _, ok := nilMgr.FindAnySession(); ok {
		t.Error("a nil ConnectionManager returned a session")
	}
}

// FindSession's three paths, and the two error messages are load-bearing:
// they distinguish "the role has no node" from "the node has no session",
// which are different operational faults with different fixes.
func TestFindSessionFallsBackToForwardingThenFailsWithTheRightReason(t *testing.T) {
	ctx := context.Background()

	t.Run("no node for role AND no session — names the ROLE", func(t *testing.T) {
		m := finderManager(t)
		_, err := m.FindSession(ctx, "auth", "hstles.auth.ping")
		if err == nil {
			t.Fatal("expected an error with neither a target node nor any session")
		}
		if !strings.Contains(err.Error(), "no nodes found for role") {
			t.Fatalf("error = %q, want it to name the ROLE — an operator seeing "+
				"\"no session to <node>\" would chase a connection when the real "+
				"fault is an empty role table", err)
		}
	})

	t.Run("no node for role but a session exists — FORWARDS", func(t *testing.T) {
		m := finderManager(t)
		bind(m, testNodeIDB, wsSession())

		got, err := m.FindSession(ctx, "auth", "hstles.auth.ping")
		if err != nil {
			t.Fatalf("FindSession errored with a forwardable session available: %v "+
				"— path 2 exists precisely so a peer can forward to a target we "+
				"cannot resolve locally", err)
		}
		if got == nil {
			t.Fatal("FindSession returned no session and no error")
		}
	})

	t.Run("nil manager", func(t *testing.T) {
		var nilMgr *ConnectionManager
		if _, err := nilMgr.FindSession(ctx, "auth", "h"); err == nil {
			t.Error("a nil ConnectionManager returned no error")
		}
	})
}
