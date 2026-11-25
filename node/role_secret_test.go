/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
 */

package node

import (
	"crypto/ed25519"
	"errors"
	"testing"
	"time"

	"github.com/bbmumford/loom/directory"
	"github.com/bbmumford/loom/ports"
	"github.com/bbmumford/swarm"
	swarmpb "github.com/bbmumford/swarm/proto/pb"
	"google.golang.org/protobuf/proto"
)

// secretRuntime is a Runtime holding r's endorsements for node id, as
// openMeshDB leaves it after releasing the group orbtr and r's secrets.
func secretRuntime(t *testing.T, r *RoleRelease, id ports.NodeID, nodeKey ed25519.PublicKey) *Runtime {
	t.Helper()
	endorsed, err := r.release([]string{"orbtr"}, id, nodeKey)
	if err != nil {
		t.Fatal(err)
	}
	return &Runtime{cfg: Config{Databases: &DatabasesConfig{Roles: r}}, roleEndorsements: &roleEndorsements{set: endorsed}}
}

func TestANamedSecretIsReleasedOnlyUnderACurrentEndorsement(t *testing.T) {
	clock := time.Unix(1_700_000_000, 0)
	r, nodeKey, _ := roleRelease(t, &clock)
	r.SecretRoles = map[string]string{"turso-organisation": "control-db"}
	rt := secretRuntime(t, r, "node-1", nodeKey)

	if err := rt.SecretReleased("turso-organisation"); err != nil {
		t.Fatalf("a secret whose role holds a current endorsement is refused: %v", err)
	}
	if err := rt.SecretReleased("turso-partition-group"); !errors.Is(err, ErrSecretNotReleased) {
		t.Errorf("a secret no SecretRoles entry names answered %v", err)
	}
	clock = clock.Add(4 * r.Keys.RepublishPeriod)
	if err := rt.SecretReleased("turso-organisation"); !errors.Is(err, ErrSecretNotReleased) {
		t.Errorf("an expired endorsement still releases the secret: %v", err)
	}
	if err := (*Runtime)(nil).SecretReleased("turso-organisation"); !errors.Is(err, ErrSecretNotReleased) {
		t.Errorf("a node with no release answered %v", err)
	}
}

func TestASecretsRoleThatCannotBeEndorsedRefusesTheRelease(t *testing.T) {
	clock := time.Unix(1_700_000_000, 0)
	r, nodeKey, _ := roleRelease(t, &clock)
	r.SecretRoles = map[string]string{"turso-organisation": "orbtr-tenants"}
	if _, err := r.release(nil, "node-1", nodeKey); err == nil {
		t.Fatal("a secret released under a role this node holds no key of was endorsed")
	}
	r.SecretRoles = map[string]string{"turso-organisation": ""}
	if err := r.validate(); err == nil {
		t.Fatal("a secret naming an empty role passed validation")
	}
}

func TestALapsedRoleNamesItsSecretsAsWellAsItsGroups(t *testing.T) {
	clock := time.Unix(1_700_000_000, 0)
	r, nodeKey, lapsed := roleRelease(t, &clock)
	r.SecretRoles = map[string]string{"turso-organisation": "control-db"}
	held := &roleEndorsements{set: map[string]directory.RoleEndorsement{}}
	endorsed, err := r.release([]string{"orbtr"}, "node-1", nodeKey)
	if err != nil {
		t.Fatal(err)
	}
	held.set = endorsed
	r.Keys.Keys = map[string][]ed25519.PublicKey{}
	clock = clock.Add(4 * r.Keys.RepublishPeriod)
	r.renewOnce(held, "node-1", nodeKey)
	var secretTold bool
	for _, l := range *lapsed {
		if l.group == "turso-organisation" && l.role == "control-db" {
			secretTold = true
		}
	}
	if !secretTold {
		t.Fatalf("Lapsed was told %v, not the secret its role releases", *lapsed)
	}
}

// The node's published tags carry each current endorsement, and the table a
// peer builds from the record reads back only an endorsement of the asked role
// for the node and key that signed the record.
func TestAPublishedRoleEndorsementReadsBackForThatNodeOnly(t *testing.T) {
	clock := time.Now()
	r, _, _ := roleRelease(t, &clock)
	id, nodeKey := canonicalTestNode(t)
	rt := secretRuntime(t, r, ports.NodeID(string(id)), nodeKey)
	tags := rt.roleEndorsementTags()
	if len(tags) != 1 {
		t.Fatalf("tags %v, want one endorsement", tags)
	}
	table, err := NewRoleTable(nil, "self")
	if err != nil {
		t.Fatal(err)
	}
	record := func(id swarm.NodeID, key ed25519.PublicKey) swarm.Record {
		body, err := proto.Marshal(&swarmpb.PeerRecord{NodeId: []byte(id), IssuedAt: time.Now().UnixNano(), Capabilities: &swarmpb.Capabilities{Tags: tags}})
		if err != nil {
			t.Fatal(err)
		}
		return swarm.Record{NodeID: id, Body: body, PubKey: key}
	}
	if err := table.onRecord(record(id, nodeKey)); err != nil {
		t.Fatal(err)
	}
	e, ok := table.RoleEndorsement(ports.NodeID(string(id)), "control-db")
	if !ok {
		t.Fatal("the published endorsement did not read back")
	}
	if err := directory.VerifyRoleEndorsement(e, r.Keys, time.Now()); err != nil {
		t.Errorf("the read-back endorsement does not verify under the release's keys: %v", err)
	}
	if _, ok := table.RoleEndorsement(ports.NodeID(string(id)), "orbtr-tenants"); ok {
		t.Error("an endorsement of another role read back")
	}
	otherID, otherKey := canonicalTestNode(t)
	if err := table.onRecord(record(otherID, otherKey)); err != nil {
		t.Fatal(err)
	}
	if _, ok := table.RoleEndorsement(ports.NodeID(string(otherID)), "control-db"); ok {
		t.Error("a record replaying another node's endorsement reads back as its own")
	}
}
