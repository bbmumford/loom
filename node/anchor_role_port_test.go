/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	lad "github.com/bbmumford/ledger"
	ladcache "github.com/bbmumford/ledger/cache"

	"github.com/bbmumford/loom/directory"
	"github.com/bbmumford/loom/ports"
)

// §0.5.3 step 4, "Role/Address first": isAnchorNode is one of the two
// remaining direct role READS outside the adapters, and the only routable one.
//
// 🔑 THIS TEST IS THE EQUIVALENCE PROOF FOR THAT CUT, AND ITS VALUE DEPENDS ON
// BEING WRITTEN BEFORE IT.
//
// The fixture populates ONE DirectoryCache and hands the manager BOTH the
// cache and a LiveDirectory built over that same cache — exactly as
// runtime.go does. So the assertions hold whether isAnchorNode reads
// rt.cache.Roles or rt.liveDir.Member: it passed against the pre-cut
// implementation, and a pass afterwards is evidence the cut changed nothing
// rather than a test rewritten to match new behaviour.

// anchorFixture builds a manager whose directory holds one anchor node, one
// non-anchor node, and nothing at all for a third.
func anchorFixture(t *testing.T) *ConnectionManager {
	t.Helper()
	c := ladcache.NewDirectoryCache()

	apply := func(nodeID string, roles []string) {
		t.Helper()
		mb, _ := json.Marshal(lad.MemberRecord{
			NodeID: nodeID, CreatedAt: time.Now(),
			// Producer key per node/lad_reach_bridge.go — not the reader's.
			Attrs: map[string]string{"serviceName": "svc"},
		})
		if err := c.Apply(lad.Record{
			Topic: lad.TopicMember, NodeID: nodeID, Body: mb, Timestamp: time.Now(),
		}); err != nil {
			t.Fatal(err)
		}
		rb, _ := json.Marshal(lad.RoleRecord{
			NodeID: nodeID, Roles: roles, Updated: time.Now(),
		})
		if err := c.Apply(lad.Record{
			Topic: lad.TopicRole, NodeID: nodeID, Body: rb, Timestamp: time.Now(),
		}); err != nil {
			t.Fatal(err)
		}
	}
	apply(testNodeIDA, []string{"anchor", "bootstrap"})
	apply(testNodeIDB, []string{"auth"})

	ld, err := directory.NewLADDirectory(c)
	if err != nil {
		t.Fatalf("premise wrong: the LiveDirectory adapter would not build: %v", err)
	}
	// BOTH seams populated from the same cache — that is what lets one test
	// bind either implementation.
	return &ConnectionManager{rt: &Runtime{cache: c, liveDir: ld, liveDirRaw: ld}}
}

func TestIsAnchorNodeAnswersFromTheDirectory(t *testing.T) {
	m := anchorFixture(t)

	if !m.isAnchorNode(testNodeIDA) {
		t.Fatal("a node advertising the \"anchor\" role was not recognised — " +
			"anchors are pinned to PriorityCritical, so every anchor silently " +
			"loses its protection from drain selection")
	}
	if m.isAnchorNode(testNodeIDB) {
		t.Fatal("a node advertising only \"auth\" was reported as an anchor — " +
			"the role match is not checking the role name")
	}
	if m.isAnchorNode("cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33cc33") {
		t.Fatal("a node with NO directory record was reported as an anchor — " +
			"absence is being read as membership")
	}
}

// The multi-role case is the one a naive "first role wins" implementation
// gets wrong.
//
// 🛑 THE ROLE NAMES HERE ARE CHOSEN, NOT ARBITRARY, AND THE FIRST VERSION OF
// THIS TEST WAS VACUOUS BECAUSE THEY WERE NOT.
//
// ports.Member.Roles comes back SORTED, not in producer order — measured:
// writing {"auth","identity","anchor"} yields ["anchor" "auth" "identity"].
// So "anchor" landed at index 0 anyway and a mutant that checked ONLY
// Roles[0] SURVIVED this test. The fixture must therefore use a role that
// sorts BEFORE "anchor" ("admin" < "anchor"), which is the only way the
// assertion actually exercises a later position.
func TestIsAnchorNodeFindsTheRoleAnywhereInTheList(t *testing.T) {
	m := anchorFixture(t)
	c := m.rt.cache

	const multi = "dd44dd44dd44dd44dd44dd44dd44dd44dd44dd44dd44dd44dd44dd44dd44dd44"
	mb, _ := json.Marshal(lad.MemberRecord{NodeID: multi, CreatedAt: time.Now()})
	if err := c.Apply(lad.Record{Topic: lad.TopicMember, NodeID: multi, Body: mb, Timestamp: time.Now()}); err != nil {
		t.Fatal(err)
	}
	rb, _ := json.Marshal(lad.RoleRecord{
		NodeID: multi, Roles: []string{"admin", "anchor"}, Updated: time.Now(),
	})
	if err := c.Apply(lad.Record{Topic: lad.TopicRole, NodeID: multi, Body: rb, Timestamp: time.Now()}); err != nil {
		t.Fatal(err)
	}

	// Premise: "anchor" must NOT be first after the port's sort, or this test
	// cannot distinguish "checks the whole list" from "checks Roles[0]".
	mem, ok, err := m.rt.liveDir.Member(context.Background(), "", ports.NodeID(multi))
	if err != nil || !ok {
		t.Fatalf("premise wrong: the port does not know this node (ok=%v err=%v)", ok, err)
	}
	if len(mem.Roles) == 0 || mem.Roles[0] == "anchor" {
		t.Fatalf("premise wrong: roles came back as %q — \"anchor\" is first, so "+
			"a first-role-only implementation would pass this test", mem.Roles)
	}

	if !m.isAnchorNode(multi) {
		t.Fatal("\"anchor\" in a later position was missed — the check reads " +
			"only part of the role list")
	}
}

// Fail-closed when the directory seam is absent. An unavailable directory must
// not promote every peer to anchor: PriorityCritical exempts a peer from drain
// selection, so a fail-OPEN answer here disables scale-down fleet-wide.
func TestIsAnchorNodeFailsClosedWithoutADirectory(t *testing.T) {
	bare := &ConnectionManager{rt: &Runtime{}} // no cache, no liveDir

	if bare.isAnchorNode(testNodeIDA) {
		t.Fatal("a manager with NO directory reported a node as an anchor — " +
			"with no directory every peer becomes PriorityCritical and drain " +
			"selection can never choose one")
	}
}
