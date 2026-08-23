/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package directory

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	lad "github.com/bbmumford/ledger"
	ladcache "github.com/bbmumford/ledger/cache"
)

// ports.RoleEnumerator (#M-626, directed by #R-1616) closes the all-roles
// capability gap recorded at #M-619 ⑤.
//
// 🔑 THE FIRST TEST IS THE JUSTIFICATION FOR THE CAPABILITY EXISTING AT ALL.
// The tempting substitution is "Members() + filter on len(Roles)>0" — which
// would need no new interface. That is only valid if every role-advertising
// node also has a member record. This measures whether it does, instead of
// asserting it: if Members already saw such a node, RoleEnumerator would be
// redundant and this file should be deleted rather than kept.

func ladDirWithRecords(t *testing.T, apply func(c *ladcache.DirectoryCache)) *LADDirectory {
	t.Helper()
	c := ladcache.NewDirectoryCache()
	apply(c)
	d, err := NewLADDirectory(c)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func applyRoleOnly(t *testing.T, c *ladcache.DirectoryCache, nodeID string, roles []string) {
	t.Helper()
	rb, _ := json.Marshal(lad.RoleRecord{NodeID: nodeID, Roles: roles, Updated: time.Now()})
	if err := c.Apply(lad.Record{
		Topic: lad.TopicRole, NodeID: nodeID, Body: rb, Timestamp: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
}

// 🔴 THE CAPABILITY'S REASON TO EXIST, MEASURED.
//
// A node that published a ROLE record but no MEMBER record: LAD keeps roles on
// their own records, so Members has nothing to project it from. Counting
// members-with-roles is therefore a different quantity from counting role
// advertisements — silently different, which is why node/runtime.go's
// mesh-status count stayed on the raw cache rather than being approximated.
func TestRoleAdvertsSeesARoleOnlyNodeThatMembersDoesNot(t *testing.T) {
	const roleOnly = "aa11aa11aa11aa11aa11aa11aa11aa11aa11aa11aa11aa11aa11aa11aa11aa11"
	d := ladDirWithRecords(t, func(c *ladcache.DirectoryCache) {
		applyRoleOnly(t, c, roleOnly, []string{"anchor"})
	})
	ctx := context.Background()

	adverts, err := d.RoleAdverts(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(adverts) != 1 || string(adverts[0].NodeID) != roleOnly {
		t.Fatalf("RoleAdverts = %v, want exactly the role-only node — the "+
			"capability does not see the case it exists for", adverts)
	}

	members, err := d.Members(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range members {
		if string(m.NodeID) == roleOnly {
			t.Fatalf("PREMISE REFUTED: Members() already returns the role-only "+
				"node (%d members). Then Members+filter WOULD have served this "+
				"and ports.RoleEnumerator is redundant — delete it rather than "+
				"keep a second way to ask one question", len(members))
		}
	}
}

// Both implementations must agree on shape and order, or a consumer's count
// changes with whichever directory is bound — the class #M-620 ④ found in
// Member.Roles.
func TestRoleAdvertsAgreesAcrossBothImplementations(t *testing.T) {
	ctx := context.Background()
	id, pub, _ := testIdentity(t)
	roles := []string{"identity", "auth", "anchor"} // deliberately unsorted

	sd := newRoleOrderSwarmDir(t)
	if err := sd.Ingest(ctx, peerRecord(t, id, pub, 1, roles, "svc")); err != nil {
		t.Fatal(err)
	}
	swarmAdverts, err := sd.RoleAdverts(ctx, fixtureTenant)
	if err != nil {
		t.Fatal(err)
	}

	ld := ladDirWithRecords(t, func(c *ladcache.DirectoryCache) {
		rb, _ := json.Marshal(lad.RoleRecord{
			TenantID: string(fixtureTenant), NodeID: string(id), Roles: roles, Updated: time.Now(),
		})
		if err := c.Apply(lad.Record{
			Topic: lad.TopicRole, TenantID: string(fixtureTenant), NodeID: string(id),
			Body: rb, Timestamp: time.Now(),
		}); err != nil {
			t.Fatal(err)
		}
	})
	ladAdverts, err := ld.RoleAdverts(ctx, fixtureTenant)
	if err != nil {
		t.Fatal(err)
	}

	if len(swarmAdverts) != 1 || len(ladAdverts) != 1 {
		t.Fatalf("premise wrong: swarm=%d lad=%d adverts, want 1 each — the "+
			"comparison below would be vacuous", len(swarmAdverts), len(ladAdverts))
	}
	if !rolesEqual(swarmAdverts[0].Roles, wantThreeSorted()) {
		t.Errorf("SwarmDirectory roles = %q, want %q", swarmAdverts[0].Roles, wantThreeSorted())
	}
	if !rolesEqual(ladAdverts[0].Roles, wantThreeSorted()) {
		t.Errorf("LADDirectory roles = %q, want %q", ladAdverts[0].Roles, wantThreeSorted())
	}
	if !rolesEqual(swarmAdverts[0].Roles, ladAdverts[0].Roles) {
		t.Fatalf("THE TWO IMPLEMENTATIONS DISAGREE: swarm=%q lad=%q",
			swarmAdverts[0].Roles, ladAdverts[0].Roles)
	}
}

func wantThreeSorted() []string { return []string{"anchor", "auth", "identity"} }

// Results are NodeID-ordered so two directories are comparable and a caller
// can diff them.
//
// 🛑 THIS IS TESTED ON THE SWARM SIDE ON PURPOSE, AND THE FIRST VERSION WAS
// VACUOUS FOR TESTING IT ON THE LAD SIDE (#M-626 ⑤).
//
// LADDirectory's ordering is INHERITED: ladcache.Roles already sorts by
// NodeID (cache/directory.go:1625), so removing LADDirectory's own sort.Slice
// changes nothing observable and a mutant SURVIVED the LAD version of this
// test. SwarmDirectory builds its result by ranging a map, where iteration
// order is randomised per run — so the sort is load-bearing there and only
// there.
//
// Six nodes: an unsorted map walk landing in ascending order by chance is
// 1/6! ≈ 0.14%, so the mutant dies reliably rather than flakily.
func TestRoleAdvertsIsOrderedByNodeID(t *testing.T) {
	ctx := context.Background()
	sd := newRoleOrderSwarmDir(t)
	const n = 6
	for i := 0; i < n; i++ {
		id, pub, _ := testIdentity(t) // random keypair ⇒ unordered insertion
		if err := sd.Ingest(ctx, peerRecord(t, id, pub, uint64(i+1), []string{"auth"}, "svc")); err != nil {
			t.Fatal(err)
		}
	}

	adverts, err := sd.RoleAdverts(ctx, fixtureTenant)
	if err != nil {
		t.Fatal(err)
	}
	if len(adverts) != n {
		t.Fatalf("got %d adverts, want %d — the fixture did not land, so the "+
			"ordering assertion below is vacuous", len(adverts), n)
	}
	for i := 1; i < len(adverts); i++ {
		if adverts[i-1].NodeID >= adverts[i].NodeID {
			t.Fatalf("adverts are not NodeID-ordered at %d: %s >= %s — the "+
				"result varies per map walk, so two directories cannot be "+
				"diffed and no caller can rely on the order",
				i, adverts[i-1].NodeID, adverts[i].NodeID)
		}
	}
}
