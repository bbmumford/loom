/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"encoding/json"
	"sort"
	"testing"
	"time"

	lad "github.com/bbmumford/ledger"
	ladcache "github.com/bbmumford/ledger/cache"

	"github.com/bbmumford/loom/directory"
	"github.com/bbmumford/loom/ports"
)

// The stage-1 wiring claim is "expected behaviour delta ZERO", and a claim
// like that is worth exactly as much as the check behind it. This measures the
// delta directly: for the reads now routed through the LiveDirectory seam, the
// port must return what the direct cache call returned.
//
// ⚠ The delta is zero CONDITIONAL on callers passing the tenant the records
// were stored under. Nine of loom's ten sites pass "" and loom's own appends
// leave TenantID at the zero value, so "" is the right bucket today — but
// whether any ENDPOINT publishes a non-empty tenant is unmeasured and owned by
// @C/@P. This test pins the equivalence for the tenant it is given
// and deliberately also exercises a non-empty one, so the conditional is
// visible rather than assumed.
func TestSeamMatchesDirectCacheReads(t *testing.T) {
	ctx := context.Background()

	for _, tenant := range []string{"", "hstles"} {
		t.Run("tenant="+tenant, func(t *testing.T) {
			c := ladcache.NewDirectoryCache()
			ld, err := directory.NewLADDirectory(c)
			if err != nil {
				t.Fatal(err)
			}
			defer ld.Close()

			for i, nodeID := range []string{"node-a", "node-b", "node-c"} {
				roles := []string{"auth"}
				if i == 2 {
					roles = []string{"billing"}
				}
				mb, _ := json.Marshal(lad.MemberRecord{
					TenantID: tenant, NodeID: nodeID, CreatedAt: time.Now(),
					// Producer key, per node/lad_reach_bridge.go:156 — not the reader's.
					Attrs: map[string]string{"serviceName": "svc"},
				})
				if err := c.Apply(lad.Record{
					Topic: lad.TopicMember, TenantID: tenant, NodeID: nodeID,
					Body: mb, Timestamp: time.Now(),
				}); err != nil {
					t.Fatal(err)
				}
				rb, _ := json.Marshal(lad.RoleRecord{
					TenantID: tenant, NodeID: nodeID, Roles: roles, Updated: time.Now(),
				})
				if err := c.Apply(lad.Record{
					Topic: lad.TopicRole, TenantID: tenant, NodeID: nodeID,
					Body: rb, Timestamp: time.Now(),
				}); err != nil {
					t.Fatal(err)
				}
			}

			// --- the routed read: NodesByRole vs cache.Roles(RoleQuery{Role}) ---
			direct, err := c.Roles(ctx, tenant, ladcache.RoleQuery{Role: "auth"})
			if err != nil {
				t.Fatal(err)
			}
			wantIDs := make([]string, 0, len(direct))
			for _, r := range direct {
				wantIDs = append(wantIDs, r.NodeID)
			}
			sort.Strings(wantIDs)
			if len(wantIDs) == 0 {
				t.Fatal("the direct call returned nothing — the comparison would be vacuous")
			}

			viaPort, err := ld.NodesByRole(ctx, ports.Tenant(tenant), "auth")
			if err != nil {
				t.Fatal(err)
			}
			gotIDs := make([]string, 0, len(viaPort))
			for _, id := range viaPort {
				gotIDs = append(gotIDs, string(id))
			}
			sort.Strings(gotIDs)

			if len(gotIDs) != len(wantIDs) {
				t.Fatalf("NodesByRole returned %d nodes, direct cache.Roles returned %d "+
					"— the routed read is not equivalent: %v vs %v",
					len(gotIDs), len(wantIDs), gotIDs, wantIDs)
			}
			for i := range wantIDs {
				if gotIDs[i] != wantIDs[i] {
					t.Fatalf("routed read diverged at %d: %q vs %q (%v vs %v)",
						i, gotIDs[i], wantIDs[i], gotIDs, wantIDs)
				}
			}

			// Control: the port discriminates by role rather than returning
			// everything, or the equality above would be trivially satisfiable.
			billing, err := ld.NodesByRole(ctx, ports.Tenant(tenant), "billing")
			if err != nil {
				t.Fatal(err)
			}
			if len(billing) != 1 || string(billing[0]) != "node-c" {
				t.Fatalf("control: NodesByRole(billing) = %v, want exactly node-c", billing)
			}

			// --- the routed read: Members count ---
			directM, err := c.Members(ctx, tenant)
			if err != nil {
				t.Fatal(err)
			}
			portM, err := ld.Members(ctx, ports.Tenant(tenant))
			if err != nil {
				t.Fatal(err)
			}
			if len(portM) != len(directM) {
				t.Fatalf("Members via port = %d, direct = %d — the status counter "+
					"would report a different number after wiring", len(portM), len(directM))
			}
			if len(directM) == 0 {
				t.Fatal("no members — the Members comparison would be vacuous")
			}
		})
	}
}

// The seam's lifecycle must actually be owned. Close is wired into Shutdown
// because the adapter holds liveness-override timers that reference the cache;
// this pins that Close is safe to call and idempotent, so the shutdown path
// cannot acquire a panic from being run twice.
func TestSeamCloseIsSafeAndIdempotent(t *testing.T) {
	c := ladcache.NewDirectoryCache()
	ld, err := directory.NewLADDirectory(c)
	if err != nil {
		t.Fatal(err)
	}
	ld.OverrideLiveness(ports.NodeID("n1"), true, 60_000)
	if err := ld.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := ld.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
}

// The routed reads must FAIL CLOSED when the seam is absent, not panic.
//
// The state this builds is the one that is actually reachable, and getting it
// wrong makes the test VACUOUS. Every routed site begins with
// `if m.rt.cache == nil { return … }`, so a runtime with no cache returns
// before touching the seam and proves nothing — such a test passes
// with the guard deleted.
//
// The reachable danger is cache PRESENT and seam ABSENT: runtime construction
// logs and leaves liveDir nil if NewLADDirectory fails, and `rt.liveDir` is an
// INTERFACE, so a call on a nil one panics unconditionally where the nil
// *DirectoryCache it replaced would not.
func TestRoutedReadsFailClosedWithNoSeam(t *testing.T) {
	// Cache present, seam absent — the state runtime construction can produce.
	m := &ConnectionManager{rt: &Runtime{
		ctx:   context.Background(),
		cache: ladcache.NewDirectoryCache(),
	}}
	if m.rt.cache == nil || m.rt.liveDir != nil {
		t.Fatal("premise wrong: this test needs cache PRESENT and liveDir ABSENT")
	}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("a routed read panicked with no seam: %v — the nil-interface "+
				"guard is missing from at least one site", r)
		}
	}()

	if got := m.peerServiceHostname("some-node"); got != "" {
		t.Fatalf("peerServiceHostname = %q, want \"\"", got)
	}
	if got := m.peerServiceName("some-node"); got != "" {
		t.Fatalf("peerServiceName = %q, want \"\"", got)
	}
	if got := m.peerHTTPPort("some-node"); got != defaultMeshHTTPPort {
		t.Fatalf("peerHTTPPort = %q, want the default %q", got, defaultMeshHTTPPort)
	}
}

// A standing per-site check: EVERY routed read must name what it
// does when the seam is nil, and be asserted on the REACHABLE state — cache
// present, seam absent — because that is the state runtime construction can
// actually produce, and because a nil-interface call panics unconditionally.
//
// This covers the routed sites that TestRoutedReadsFailClosedWithNoSeam does
// not: BestGradeToHandler (runtime) and the mesh-status member count. The
// forwarder lookup has its own (TestForwarderLookupFailsClosed, mutation-
// verified); the three peer_* readers are covered above.
func TestRoutedRuntimeReadsFailClosedWithNoSeam(t *testing.T) {
	rt := &Runtime{ctx: context.Background(), cache: ladcache.NewDirectoryCache()}
	if rt.cache == nil || rt.liveDir != nil {
		t.Fatal("premise wrong: this test needs cache PRESENT and liveDir ABSENT")
	}

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("a routed runtime read panicked with no seam: %v", r)
		}
	}()

	// The grade lookup must degrade to GradeF, which is what an absent
	// directory has always meant here — not panic on the dispatch path.
	if g := rt.BestGradeToHandler("hstles.auth.ping"); g != GradeF {
		t.Fatalf("BestGradeToHandler with no seam = %v, want GradeF", g)
	}
}
