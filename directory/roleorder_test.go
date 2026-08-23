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

	"github.com/bbmumford/loom/journal"

	"github.com/bbmumford/loom/ports"
)

// ports.Member.Roles is contractually LEXICOGRAPHIC (#R-1607 ④). Before that
// ruling the port promised nothing and the two implementations disagreed:
// LADDirectory sorted at read, SwarmDirectory returned producer order
// (#M-620 ④, verified at the writer in #M-621 ①).
//
// 🔑 THIS IS THE INSTRUMENT A TWO-IMPLEMENTATION PORT EXISTS TO HAVE: feed
// BOTH the same roles in a deliberately unsorted order and require the same
// answer. A per-implementation test cannot catch a divergence — only a
// comparison can, and the divergence is what made a consumer's Roles[0]
// implementation-dependent.

// unsortedRoles is deliberately NOT in lexicographic order, and "anchor" is
// deliberately not first: a fixture already sorted, or one whose first
// element is the interesting one, cannot distinguish "sorted" from "returned
// as given" (#M-620 ③ — that exact fixture bug survived a mutant).
var unsortedRoles = []string{"identity", "auth", "anchor", "billing"}

// fixtureTenant matches the "tenant=hstles" tag peerRecord stamps. The port
// documents that a record stored under a tenant returns nothing for a query
// on "" — so both implementations must be asked the SAME tenant or the
// comparison is between a hit and a miss.
const fixtureTenant ports.Tenant = "hstles"

func wantSortedRoles() []string { return []string{"anchor", "auth", "billing", "identity"} }

func rolesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// newRoleOrderSwarmDir builds a SwarmDirectory over a temp-dir journal, the
// same shape compositekey_test.go uses. The journal is closed by t.Cleanup so
// a failing assertion cannot leak the file handle.
func newRoleOrderSwarmDir(t *testing.T) *SwarmDirectory {
	t.Helper()
	j, err := journal.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = j.Close() })
	d, err := NewSwarmDirectory(context.Background(), j, nil)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

// ladMemberRoles projects the roles through LADDirectory.
func ladMemberRoles(t *testing.T, id, tenant string, roles []string) []string {
	t.Helper()
	c := ladcache.NewDirectoryCache()
	mb, _ := json.Marshal(lad.MemberRecord{NodeID: id, TenantID: tenant, CreatedAt: time.Now()})
	if err := c.Apply(lad.Record{Topic: lad.TopicMember, TenantID: tenant, NodeID: id, Body: mb, Timestamp: time.Now()}); err != nil {
		t.Fatal(err)
	}
	rb, _ := json.Marshal(lad.RoleRecord{NodeID: id, TenantID: tenant, Roles: roles, Updated: time.Now()})
	if err := c.Apply(lad.Record{Topic: lad.TopicRole, TenantID: tenant, NodeID: id, Body: rb, Timestamp: time.Now()}); err != nil {
		t.Fatal(err)
	}
	d, err := NewLADDirectory(c)
	if err != nil {
		t.Fatal(err)
	}
	m, ok, err := d.Member(context.Background(), ports.Tenant(tenant), ports.NodeID(id))
	if err != nil || !ok {
		t.Fatalf("LADDirectory does not know the fixture node (ok=%v err=%v)", ok, err)
	}
	return m.Roles
}

func TestBothDirectoriesReturnRolesInTheSameOrder(t *testing.T) {
	// Premise: the fixture must not already be sorted, or neither half of
	// this comparison proves anything.
	if rolesEqual(unsortedRoles, wantSortedRoles()) {
		t.Fatal("premise wrong: the fixture roles are already in sorted order")
	}

	id, pub, _ := testIdentity(t)
	sd := newRoleOrderSwarmDir(t)
	if err := sd.Ingest(context.Background(), peerRecord(t, id, pub, 1, unsortedRoles, "svc")); err != nil {
		t.Fatal(err)
	}
	sm, ok, err := sd.Member(context.Background(), fixtureTenant, id)
	if err != nil || !ok {
		t.Fatalf("SwarmDirectory does not know the fixture node (ok=%v err=%v)", ok, err)
	}

	ladRoles := ladMemberRoles(t, string(id), string(fixtureTenant), unsortedRoles)

	if !rolesEqual(sm.Roles, wantSortedRoles()) {
		t.Errorf("SwarmDirectory returned %q, want %q — producer order is "+
			"reaching consumers, so Roles[0] and any equality/hash over Member "+
			"depend on which implementation is bound", sm.Roles, wantSortedRoles())
	}
	if !rolesEqual(ladRoles, wantSortedRoles()) {
		t.Errorf("LADDirectory returned %q, want %q", ladRoles, wantSortedRoles())
	}
	if !rolesEqual(sm.Roles, ladRoles) {
		t.Fatalf("THE TWO IMPLEMENTATIONS DISAGREE: swarm=%q lad=%q. A port with "+
			"two implementations that answer differently has no contract, only "+
			"a default", sm.Roles, ladRoles)
	}
}

// The clone path must not undo the ordering — Member() and Members() both
// hand out copies, and a copy that reordered would reintroduce the bug at the
// read side after it was fixed at the write side.
func TestSwarmRoleOrderSurvivesTheCloneOnEveryReadPath(t *testing.T) {
	id, pub, _ := testIdentity(t)
	sd := newRoleOrderSwarmDir(t)
	if err := sd.Ingest(context.Background(), peerRecord(t, id, pub, 1, unsortedRoles, "svc")); err != nil {
		t.Fatal(err)
	}

	one, ok, err := sd.Member(context.Background(), fixtureTenant, id)
	if err != nil || !ok {
		t.Fatalf("Member: ok=%v err=%v", ok, err)
	}
	all, err := sd.Members(context.Background(), fixtureTenant)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) == 0 {
		t.Fatal("premise wrong: Members returned nothing, so the second half " +
			"of this test is vacuous")
	}

	if !rolesEqual(one.Roles, wantSortedRoles()) {
		t.Errorf("Member() roles = %q, want %q", one.Roles, wantSortedRoles())
	}
	for _, m := range all {
		if string(m.NodeID) != string(id) {
			continue
		}
		if !rolesEqual(m.Roles, wantSortedRoles()) {
			t.Errorf("Members() roles = %q, want %q — the two read paths "+
				"disagree with each other", m.Roles, wantSortedRoles())
		}
	}
}
