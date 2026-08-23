/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"testing"
	"time"

	lad "github.com/bbmumford/ledger"
	ladcache "github.com/bbmumford/ledger/cache"
)

// COVERAGE of findNodeForRole, 6.7% → the selection logic.
//
// This picks WHICH NODE SERVES A ROLE. Every failure is silent: a call
// succeeds against a worse peer, or against a peer chosen alphabetically.
//
// 🙋 I ESTIMATED THIS NEEDED "a swarm.RoleTable fixture, a bigger build than
// this slice" AND THAT WAS WRONG — measured, RoleTable is a plain
// in-package struct with a nil-tolerant constructor (`NewRoleTable(nil, id)`)
// and an in-package `applyRecord`. The fixture below is ~15 lines. Estimating
// a cost and then acting on the estimate is the thing this session keeps
// catching; measuring it took one command.

// roleFixture builds a manager whose swarm RoleTable holds the given nodes
// under one role, with peerGrade wired through rt.connMgr → peers.
func roleFixture(t *testing.T, role string, nodeIDs ...string) *ConnectionManager {
	t.Helper()
	table, err := NewRoleTable(nil, testNodeIDA) // nil swarm.Node: no subscription
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range nodeIDs {
		table.applyRecord(lad.RoleRecord{
			NodeID: id, Roles: []string{role}, Updated: time.Now(),
		}, PeerInfo{})
	}

	m := registerTestManager()
	m.selfID = testNodeIDA
	m.peers = map[string]*peerConn{}
	// cache must be REAL, not nil: findNodeForRole calls
	// rt.cache.LatencyBetween, whose first act is c.inMemMu.RLock() — a nil
	// *DirectoryCache panics rather than returning a zero RTT. An EMPTY cache
	// is what gives 0 for every pair, which is the state the grade tiebreak
	// below needs.
	m.rt = &Runtime{swarm: &SwarmIntegration{RoleTable: table}, cache: ladcache.NewDirectoryCache()}
	// peerGrade reads rt.connMgr.peers — point it back at this manager.
	m.rt.connMgr = m
	return m
}

// gradePeer registers a peer holding one transport of the given grade, so
// rt.peerGrade(nodeID) answers with it.
func gradePeer(m *ConnectionManager, nodeID string, proto Protocol, g Grade) {
	p := &peerConn{nodeID: nodeID, state: PeerConnected, reconnectDelay: baseCooldown}
	p.addTransport(&transportConn{protocol: proto, grade: g, connectedAt: time.Now()})
	m.mu.Lock()
	m.peers[nodeID] = p
	m.mu.Unlock()
}

func TestFindNodeForRoleReturnsTheSoleCandidate(t *testing.T) {
	m := roleFixture(t, "auth", testNodeIDB)

	if got := m.findNodeForRole(context.Background(), "auth", ""); got != testNodeIDB {
		t.Fatalf("findNodeForRole = %q, want the only node advertising the role "+
			"(%q) — a role with exactly one provider is unroutable", got, testNodeIDB)
	}
}

func TestFindNodeForRoleReturnsEmptyWhenNothingAdvertisesTheRole(t *testing.T) {
	m := roleFixture(t, "auth", testNodeIDB)

	if got := m.findNodeForRole(context.Background(), "billing", ""); got != "" {
		t.Fatalf("findNodeForRole = %q for an unadvertised role, want \"\" — a "+
			"non-empty answer sends the call to a node that never claimed the "+
			"role", got)
	}
}

// 🔴 THE L3 #4 FIX, WHICH IS THE REASON THIS FUNCTION IS MORE THAN A MAP READ.
//
// When NEITHER candidate has a latency measurement the old code fell through
// to nodes[0] — i.e. whichever the map walk produced, effectively arbitrary.
// It now tiebreaks on the locally-measured connection GRADE: a peer we are
// already connected to at Grade A beats an unknown one even with no latency
// data.
func TestFindNodeForRolePrefersTheBetterGradeWhenNeitherHasLatency(t *testing.T) {
	const other = "cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33"
	m := roleFixture(t, "auth", testNodeIDB, other)

	// The cache is EMPTY, so LatencyBetween is 0 for both pairs and the grade
	// tiebreak is the ONLY discriminator.
	gradePeer(m, testNodeIDB, ProtoWebSocket, GradeC)
	gradePeer(m, other, ProtoNoiseUDP, GradeA)

	if m.rt.peerGrade(other) <= m.rt.peerGrade(testNodeIDB) {
		t.Fatalf("premise wrong: grades did not straddle (%v vs %v), so the "+
			"tiebreak has nothing to choose between",
			m.rt.peerGrade(other), m.rt.peerGrade(testNodeIDB))
	}

	got := m.findNodeForRole(context.Background(), "auth", "")
	if got != other {
		t.Fatalf("findNodeForRole = %q, want the Grade-A peer %q. With no "+
			"latency data the selection has fallen back to map/alphabetical "+
			"order, which is the L3 #4 defect: a Grade-C WebSocket peer serves "+
			"the role while a Grade-A noise-UDP peer sits idle", got, other)
	}
}

// The handler-specific lookup is tried FIRST and only falls back to the
// role-wide one when it yields nothing. Losing that order would route a
// handler-qualified call to a node that serves the role but not the handler.
func TestFindNodeForRolePrefersTheHandlerQualifiedLookup(t *testing.T) {
	table, err := NewRoleTable(nil, testNodeIDA)
	if err != nil {
		t.Fatal(err)
	}
	const handlerNode = "dd44dd44dd44dd44dd44dd44dd44dd44dd44dd44dd44dd44dd44dd44dd44dd44"
	// One node serves the role AND declares the handler; another only the role.
	table.applyRecord(lad.RoleRecord{
		NodeID: handlerNode, Roles: []string{"auth"}, Updated: time.Now(),
		Handlers: []lad.HandlerMetadata{{Name: "hstles.auth.ping"}},
	}, PeerInfo{})
	table.applyRecord(lad.RoleRecord{
		NodeID: testNodeIDB, Roles: []string{"auth"}, Updated: time.Now(),
	}, PeerInfo{})

	m := registerTestManager()
	m.selfID = testNodeIDA
	m.peers = map[string]*peerConn{}
	m.rt = &Runtime{swarm: &SwarmIntegration{RoleTable: table}, cache: ladcache.NewDirectoryCache()}
	m.rt.connMgr = m

	// Premise: the handler lookup must actually resolve, or this test is
	// only re-testing the role-wide path under a different name.
	if got := table.Lookup("auth", "hstles.auth.ping"); len(got) == 0 {
		t.Skip("RoleTable does not index this handler shape; the handler-first " +
			"ordering cannot be exercised through applyRecord alone")
	}

	if got := m.findNodeForRole(context.Background(), "auth", "hstles.auth.ping"); got != handlerNode {
		t.Fatalf("findNodeForRole = %q, want the handler-declaring node %q — a "+
			"handler-qualified call is being routed to a node that serves the "+
			"role but not the handler", got, handlerNode)
	}
}
