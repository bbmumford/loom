/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"testing"
	"time"

	lad "github.com/bbmumford/ledger"
)

// newStaleTestTable builds a RoleTable with no swarm subscription — StaleNodes
// reads infoBy only, so the maps can be populated directly and the test stays
// free of a live swarm Node.
func newStaleTestTable(self string) *RoleTable {
	return &RoleTable{
		byRole:   map[string]map[string]lad.RoleRecord{},
		byNode:   map[string]lad.RoleRecord{},
		infoBy:   map[string]PeerInfo{},
		selfNode: self,
	}
}

// TestRoleTableStaleNodes_FindsGhostAndSparesLive is the regression guard for
// immortal ghosts in the mesh topology.
//
// A RoleTable entry has ONE removal path: an inbound tombstone on fleet.peer
// (onRecord). No TTL, no expiry. So it dies only by the owner's graceful
// tombstone — never emitted when a deploy destroys the machine — or by an
// observer tombstone, which was published solely by sweepZombieSessions: a
// sweep over dead SESSIONS, structurally blind to a peer this process never
// held a session with. Every deploy-replaced machine landed in that gap.
//
// Measured live: 40 entries against 11 real machines, while the LAD directory
// (which HAS a TTL) sat clean at 8 beside it. mesh-topology merges both, so the
// corpses rendered as healthy peers and got dialled forever — noise-UDP hanging
// to msg2 timeout, WebSocket taking 401/404 and falling back to TLS.
//
// StaleNodes is the witness the session sweep cannot be. The live-peer case is
// the half that matters most: attesting against a live peer is the cascade the
// K-of-N quorum exists to prevent, and it is strictly worse than the ghost.
func TestRoleTableStaleNodes_FindsGhostAndSparesLive(t *testing.T) {
	const (
		self  = "vl1_self"
		ghost = "vl1_ghost"
		live  = "vl1_live"
	)
	now := time.Now()
	tbl := newStaleTestTable(self)

	// Ghost: silent for 3+ republish cycles (publisher refreshes every 5 min).
	tbl.infoBy[ghost] = PeerInfo{NodeID: ghost, Updated: now.Add(-2 * roleStaleThreshold)}
	// Live: refreshed within the window.
	tbl.infoBy[live] = PeerInfo{NodeID: live, Updated: now.Add(-30 * time.Second)}

	got := tbl.StaleNodes(now.Add(-roleStaleThreshold))

	if len(got) != 1 || got[0] != ghost {
		t.Fatalf("StaleNodes = %v, want [%s] exactly — a live peer in this list "+
			"becomes an attestation that it is dead", got, ghost)
	}
}

// TestRoleTableStaleNodes_NeverReportsSelf: a node must never attest against
// itself. swarm refuses a self-target anyway, but relying on that would make
// this table's contract depend on a downstream guard.
func TestRoleTableStaleNodes_NeverReportsSelf(t *testing.T) {
	const self = "vl1_self"
	now := time.Now()
	tbl := newStaleTestTable(self)

	// Self, arbitrarily stale — a quiet node still holds its own identity.
	tbl.infoBy[self] = PeerInfo{NodeID: self, Updated: now.Add(-10 * roleStaleThreshold)}

	if got := tbl.StaleNodes(now.Add(-roleStaleThreshold)); len(got) != 0 {
		t.Fatalf("StaleNodes reported SELF: %v", got)
	}
}

// TestRoleTableStaleNodes_RefusesUnagedRecord: a record with no IssuedAt cannot
// be aged. A zero time is BEFORE every cutoff, so treating it as data would
// read "never heard from" as "infinitely old" and attest against every peer
// whose publisher omitted the field. Absent evidence is not evidence of death.
func TestRoleTableStaleNodes_RefusesUnagedRecord(t *testing.T) {
	const self = "vl1_self"
	now := time.Now()
	tbl := newStaleTestTable(self)

	tbl.infoBy["vl1_noclock"] = PeerInfo{NodeID: "vl1_noclock"} // Updated zero

	if got := tbl.StaleNodes(now.Add(-roleStaleThreshold)); len(got) != 0 {
		t.Fatalf("StaleNodes attested on an unaged record: %v — zero time is before "+
			"every cutoff, so this would kill every peer with no IssuedAt", got)
	}
}

// TestRoleStaleThreshold_ClearsThreeRepublishCycles pins the relationship the
// threshold depends on. PeerPublisher republishes every 5 minutes; if that ever
// drops below roleStaleThreshold/3, a merely-slow peer starts tripping the
// sweep and anchors begin attesting against live nodes.
func TestRoleStaleThreshold_ClearsThreeRepublishCycles(t *testing.T) {
	const republishInterval = 5 * time.Minute // peer_publisher TTL refresh
	if roleStaleThreshold < 3*republishInterval {
		t.Fatalf("roleStaleThreshold %v is under 3 republish cycles (%v) — a peer "+
			"that misses two refreshes would be attested dead while alive",
			roleStaleThreshold, 3*republishInterval)
	}
	// The sweep must also run often enough that K anchors land inside swarm's
	// corroboration window; attestations older than it are pruned before quorum
	// is counted, so anchors sweeping further apart never corroborate.
	if roleSweepInterval >= 5*time.Minute {
		t.Fatalf("roleSweepInterval %v is not comfortably inside the 5m "+
			"corroboration window — anchors may never corroborate", roleSweepInterval)
	}
}

// TestRoleTablePruneStale_RemovesGhostSparesLiveAndSelf is the regression guard
// for the local staleness prune — the reliable, topology-independent cleanup
// that actually converges machines[] to the real fleet.
//
// Unlike the observer-tombstone path (which needs K anchors to agree and whose
// synthesised tombstone is consumer-local, so it never reliably reached nodes a
// hop away — the live fleet plateaued in the mid-30s against 11 real machines),
// a plain local delete is always safe: the RoleTable is a projection of gossip,
// so deleting a stale entry propagates nothing and a live peer's next
// PeerRecord re-adds it.
func TestRoleTablePruneStale_RemovesGhostSparesLiveAndSelf(t *testing.T) {
	const (
		self  = "vl1_self"
		ghost = "vl1_ghost"
		live  = "vl1_live"
	)
	now := time.Now()
	tbl := newStaleTestTable(self)

	tbl.applyRecord(lad.RoleRecord{NodeID: ghost, Roles: []string{"relay"}},
		PeerInfo{NodeID: ghost, Updated: now.Add(-2 * roleStaleThreshold)})
	tbl.applyRecord(lad.RoleRecord{NodeID: live, Roles: []string{"relay"}},
		PeerInfo{NodeID: live, Updated: now.Add(-30 * time.Second)})
	tbl.applyRecord(lad.RoleRecord{NodeID: self, Roles: []string{"anchor"}},
		PeerInfo{NodeID: self, Updated: now.Add(-10 * roleStaleThreshold)}) // self, arbitrarily stale

	removed := tbl.PruneStale(now.Add(-roleStaleThreshold))

	if removed != 1 {
		t.Fatalf("PruneStale removed %d, want 1 (ghost only)", removed)
	}
	if _, ok := tbl.PeerInfo(ghost); ok {
		t.Fatal("ghost survived the prune — stays in machines[] as a healthy peer forever")
	}
	if _, ok := tbl.PeerInfo(live); !ok {
		t.Fatal("live peer was pruned — dropped a healthy node from the local view")
	}
	if _, ok := tbl.PeerInfo(self); !ok {
		t.Fatal("self was pruned — a node erased its own identity from its view")
	}
}

// TestRoleTablePruneStale_Idempotent: a second prune with nothing stale removes
// nothing. Guards against a prune that churns or mis-accounts on a clean table.
func TestRoleTablePruneStale_Idempotent(t *testing.T) {
	const self = "vl1_self"
	now := time.Now()
	tbl := newStaleTestTable(self)
	tbl.applyRecord(lad.RoleRecord{NodeID: "vl1_live", Roles: []string{"relay"}},
		PeerInfo{NodeID: "vl1_live", Updated: now})

	if n := tbl.PruneStale(now.Add(-roleStaleThreshold)); n != 0 {
		t.Fatalf("first prune of a fresh table removed %d, want 0", n)
	}
	if n := tbl.PruneStale(now.Add(-roleStaleThreshold)); n != 0 {
		t.Fatalf("second prune removed %d, want 0", n)
	}
}
