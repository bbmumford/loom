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

// TestAHandlerNoPeerAdvertisesIsForwardedNotSentToAPeerOfTheRole: a peer of the
// role orbtr.provisioning advertises ProvisionPartition and not CreatePartitionDatabase,
// and a second peer of no such role holds the best session. A create names no
// target, offers no route to the role's peer, and leaves on the forwarding
// session; ProvisionPartition, which the peer does advertise, still routes to it, and a
// call naming no handler still finds the role's peer.
func TestAHandlerNoPeerAdvertisesIsForwardedNotSentToAPeerOfTheRole(t *testing.T) {
	const (
		role      = "orbtr.provisioning"
		provision = "orbtr.provisioning.ProvisionPartition"
		create    = "orbtr.provisioning.CreatePartitionDatabase"
		forwarder = "ee55ee55ee55ee55ee55ee55ee55ee55ee55ee55ee55ee55ee55ee55ee55ee55"
	)
	table, err := NewRoleTable(nil, testNodeIDA)
	if err != nil {
		t.Fatal(err)
	}
	table.applyRecord(lad.RoleRecord{
		NodeID: testNodeIDB, Roles: []string{role}, Updated: time.Now(),
		Handlers: []lad.HandlerMetadata{{Name: provision}},
	}, PeerInfo{})
	table.applyRecord(lad.RoleRecord{
		NodeID: forwarder, Roles: []string{"anchor"}, Updated: time.Now(),
	}, PeerInfo{})

	m := registerTestManager()
	m.selfID = testNodeIDA
	m.peers = map[string]*peerConn{}
	m.rt = &Runtime{swarm: &SwarmIntegration{RoleTable: table}, cache: ladcache.NewDirectoryCache()}
	m.rt.connMgr = m
	bind(m, testNodeIDB, wsSession())
	bind(m, forwarder, noiseSession())
	ctx := context.Background()

	if got := m.findNodeForRole(ctx, role, create); got != "" {
		t.Errorf("findNodeForRole(%s) = %q, want \"\": no peer advertises the create, and the "+
			"peer of the role that was picked refuses it as not found", create, got)
	}
	for _, r := range m.FindRoutes(ctx, role, create) {
		if r.NodeID == testNodeIDB || r.TargetNodeID == testNodeIDB {
			t.Errorf("FindRoutes(%s) offers a route to the peer of the role lacking it: %+v", create, r)
		}
	}
	sess, err := m.FindSession(ctx, role, create)
	if err != nil {
		t.Fatalf("FindSession(%s): %v; want the forwarding session", create, err)
	}
	if got := string(sess.RemoteNodeID()); got != forwarder {
		t.Errorf("FindSession(%s) leaves on %q, want the forwarding session to %q", create, got, forwarder)
	}

	if got := m.findNodeForRole(ctx, role, provision); got != testNodeIDB {
		t.Errorf("findNodeForRole(%s) = %q, want the peer advertising it %q", provision, got, testNodeIDB)
	}
	if got := m.findNodeForRole(ctx, role, ""); got != testNodeIDB {
		t.Errorf("findNodeForRole with no handler = %q, want the peer of the role %q", got, testNodeIDB)
	}
}
