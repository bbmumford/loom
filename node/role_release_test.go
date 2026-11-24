/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
 */

package node

import (
	"crypto/ed25519"
	"strings"
	"testing"
	"time"

	"github.com/bbmumford/loom/directory"
	"github.com/bbmumford/loom/ports"
	meshdbcfg "github.com/bbmumford/tursoraft/manager"
)

type lapse struct{ group, role string }

func roleRelease(t *testing.T, clock *time.Time) (*RoleRelease, ed25519.PublicKey, *[]lapse) {
	t.Helper()
	rolePub, rolePriv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	nodePub, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	lapsed := &[]lapse{}
	r := &RoleRelease{
		Keys:       directory.RoleKeys{Keys: map[string][]ed25519.PublicKey{"control-db": {rolePub}}, RepublishPeriod: time.Minute},
		Held:       map[string]ed25519.PrivateKey{"control-db": rolePriv},
		GroupRoles: map[string]string{"orbtr": "control-db", "orbtr-sessions": "control-db"},
		Lapsed:     func(group, role string, _ error) { *lapsed = append(*lapsed, lapse{group, role}) },
		Now:        func() time.Time { return *clock },
	}
	return r, nodePub, lapsed
}

func TestAGroupsKeyIsReleasedOnlyUnderAVerifiedEndorsementOfItsRole(t *testing.T) {
	clock := time.Unix(1_800_000_000, 0)
	r, nodeKey, _ := roleRelease(t, &clock)
	got, err := r.release([]string{"orbtr", "orbtr-sessions", "fleet"}, "node-1", nodeKey)
	if err != nil {
		t.Fatal(err)
	}
	e, ok := got["control-db"]
	if len(got) != 1 || !ok {
		t.Fatalf("endorsements = %v, want control-db's alone (fleet names no role)", got)
	}
	if err := directory.VerifyRoleEndorsement(e, r.Keys, clock); err != nil || e.NodeID != "node-1" {
		t.Fatalf("the released endorsement does not verify for this node: %v", err)
	}

	unheld := *r
	unheld.Held = nil
	if _, err := unheld.release([]string{"orbtr"}, "node-1", nodeKey); err == nil || !strings.Contains(err.Error(), `"orbtr"`) {
		t.Fatalf("a group whose role this node holds no key for = %v, want the refusal naming the group", err)
	}
	revoked := *r
	revoked.Keys.Keys = map[string][]ed25519.PublicKey{}
	if _, err := revoked.release([]string{"orbtr"}, "node-1", nodeKey); err == nil {
		t.Fatal("a group whose role key left the document was released")
	}
	if got, err := (*RoleRelease)(nil).release([]string{"orbtr"}, "node-1", nodeKey); err != nil || len(got) != 0 {
		t.Fatalf("no role release = %v, %v; want every group released as configured", got, err)
	}
}

func TestALapsedEndorsementTellsEveryGroupItReleases(t *testing.T) {
	clock := time.Unix(1_800_000_000, 0)
	r, nodeKey, lapsed := roleRelease(t, &clock)
	set, err := r.release([]string{"orbtr", "orbtr-sessions"}, "node-1", nodeKey)
	if err != nil {
		t.Fatal(err)
	}
	held := &roleEndorsements{set: set}
	first := set["control-db"]

	clock = clock.Add(time.Minute)
	r.renewOnce(held, "node-1", nodeKey)
	if len(*lapsed) != 0 || held.set["control-db"].IssuedAt == first.IssuedAt {
		t.Fatalf("a renewal under a listed key lapsed %v or kept the old endorsement", *lapsed)
	}

	// A renewal that fails while the current endorsement still verifies is
	// tried again; nothing lapses.
	key := r.Held["control-db"]
	r.Held = nil
	clock = clock.Add(time.Minute)
	r.renewOnce(held, "node-1", nodeKey)
	if len(*lapsed) != 0 {
		t.Fatalf("a failed renewal of a still-valid endorsement lapsed %v", *lapsed)
	}

	// Past NotAfter with no renewal, every group the role releases lapses.
	clock = clock.Add(3 * time.Minute)
	r.renewOnce(held, "node-1", nodeKey)
	if len(*lapsed) != 2 {
		t.Fatalf("lapses = %v, want orbtr and orbtr-sessions", *lapsed)
	}

	// A role key that leaves the document lapses at the next renewal, however
	// long the endorsement has left.
	*lapsed = nil
	r.Held = map[string]ed25519.PrivateKey{"control-db": key}
	set, err = r.release([]string{"orbtr"}, "node-1", nodeKey)
	if err != nil {
		t.Fatal(err)
	}
	held = &roleEndorsements{set: set}
	r.Keys.Keys = map[string][]ed25519.PublicKey{}
	r.renewOnce(held, ports.NodeID("node-1"), nodeKey)
	if len(*lapsed) != 2 {
		t.Fatalf("lapses after revocation = %v, want both groups", *lapsed)
	}
}

func TestARoleReleaseNeedsItsLapseHandlerAndPeriod(t *testing.T) {
	clock := time.Unix(1_800_000_000, 0)
	r, _, _ := roleRelease(t, &clock)
	noHandler := *r
	noHandler.Lapsed = nil
	if err := noHandler.validate(); err == nil {
		t.Fatal("a role release with no Lapsed handler was accepted")
	}
	noPeriod := *r
	noPeriod.Keys.RepublishPeriod = 0
	if err := noPeriod.validate(); err == nil {
		t.Fatal("a role release with no republish period was accepted")
	}
	if err := r.validate(); err != nil {
		t.Fatal(err)
	}
}

func TestTheTokenResolverSuppliesEachGroupsTokenBesideItsKey(t *testing.T) {
	d := &DatabasesConfig{
		Groups:        []meshdbcfg.GroupConfig{{GroupName: "orbtr", AuthToken: "from-config"}, {GroupName: "fleet", AuthToken: "kept"}},
		KeyResolver:   func(group string) (string, string, error) { return "key-" + group, "", nil },
		TokenResolver: func(group string) (string, error) { return map[string]string{"orbtr": "from-vault"}[group], nil },
	}
	groups, err := d.resolvedGroups()
	if err != nil {
		t.Fatal(err)
	}
	if groups[0].AuthToken != "from-vault" || groups[0].EncryptionKey != "key-orbtr" {
		t.Fatalf("orbtr = %+v, want the resolved token and key", groups[0])
	}
	if groups[1].AuthToken != "kept" {
		t.Fatalf("a group the token resolver answers nothing for lost its token: %q", groups[1].AuthToken)
	}
	if d.Groups[0].AuthToken != "from-config" {
		t.Fatal("resolvedGroups changed the configured groups")
	}
}
