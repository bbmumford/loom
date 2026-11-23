/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
 */

package directory

import (
	"crypto/ed25519"
	"errors"
	"testing"
	"time"

	"github.com/bbmumford/loom/ports"
)

func roleFixture(t *testing.T) (ed25519.PrivateKey, ed25519.PublicKey, RoleKeys) {
	t.Helper()
	rolePub, rolePriv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	nodePub, _, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatal(err)
	}
	keys := RoleKeys{Keys: map[string][]ed25519.PublicKey{"control-db": {rolePub}}, RepublishPeriod: time.Minute}
	return rolePriv, nodePub, keys
}

func TestARoleEndorsementVerifiesForThreePeriodsUnderAListedKey(t *testing.T) {
	rolePriv, nodeKey, keys := roleFixture(t)
	now := time.Unix(1_800_000_000, 0)
	e, err := SignRoleEndorsement(rolePriv, "control-db", ports.NodeID("node-1"), nodeKey, now, keys)
	if err != nil {
		t.Fatal(err)
	}
	if got := time.Duration(e.NotAfter - e.IssuedAt); got != 3*time.Minute {
		t.Fatalf("validity = %s, want three republish periods", got)
	}
	if err := VerifyRoleEndorsement(e, keys, now.Add(2*time.Minute)); err != nil {
		t.Fatalf("inside its validity: %v", err)
	}
	if err := VerifyRoleEndorsement(e, keys, now.Add(3*time.Minute)); !errors.Is(err, ErrRoleEndorsementExpired) {
		t.Fatalf("at NotAfter = %v, want ErrRoleEndorsementExpired", err)
	}
	if err := VerifyRoleEndorsement(e, keys, now.Add(-MaxIssuedAtSkew-time.Second)); !errors.Is(err, ErrRoleEndorsementNotYetValid) {
		t.Fatalf("before its issue past the skew = %v, want ErrRoleEndorsementNotYetValid", err)
	}
	parsed, err := ParseRoleEndorsement(e.Encode())
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyRoleEndorsement(parsed, keys, now); err != nil {
		t.Fatalf("an endorsement through Encode and Parse: %v", err)
	}
}

func TestARoleEndorsementIsRefusedForAnythingItDoesNotSay(t *testing.T) {
	rolePriv, nodeKey, keys := roleFixture(t)
	now := time.Unix(1_800_000_000, 0)
	e, err := SignRoleEndorsement(rolePriv, "control-db", ports.NodeID("node-1"), nodeKey, now, keys)
	if err != nil {
		t.Fatal(err)
	}
	otherRole := e
	otherRole.Role = "session-store"
	keys.Keys["session-store"] = keys.Keys["control-db"]
	otherNode := e
	otherNode.NodeID = "node-2"
	longer := e
	longer.NotAfter += int64(time.Minute)
	for name, bad := range map[string]RoleEndorsement{"another role": otherRole, "another node": otherNode, "a longer validity": longer} {
		if err := VerifyRoleEndorsement(bad, keys, now); !errors.Is(err, ErrRoleEndorsementSignature) {
			t.Errorf("%s = %v, want ErrRoleEndorsementSignature", name, err)
		}
	}
	revoked := RoleKeys{Keys: map[string][]ed25519.PublicKey{}, RepublishPeriod: time.Minute}
	if err := VerifyRoleEndorsement(e, revoked, now); !errors.Is(err, ErrRoleKeyNotListed) {
		t.Fatalf("after the role key left the document = %v, want ErrRoleKeyNotListed", err)
	}
	_, strayPriv, _ := ed25519.GenerateKey(nil)
	if _, err := SignRoleEndorsement(strayPriv, "control-db", "node-1", nodeKey, now, keys); !errors.Is(err, ErrRoleKeyNotListed) {
		t.Fatalf("signing under an unlisted key = %v, want ErrRoleKeyNotListed", err)
	}
}

func TestARoleEndorsementNeverOutlivesItsDocument(t *testing.T) {
	rolePriv, nodeKey, keys := roleFixture(t)
	now := time.Unix(1_800_000_000, 0)
	keys.DocumentNotAfter = now.Add(90 * time.Second)
	e, err := SignRoleEndorsement(rolePriv, "control-db", "node-1", nodeKey, now, keys)
	if err != nil {
		t.Fatal(err)
	}
	if e.NotAfter != keys.DocumentNotAfter.UnixNano() {
		t.Fatalf("NotAfter = %d, want the document's expiry %d", e.NotAfter, keys.DocumentNotAfter.UnixNano())
	}
	tighter := keys
	tighter.DocumentNotAfter = now.Add(time.Minute)
	if err := VerifyRoleEndorsement(e, tighter, now); !errors.Is(err, ErrRoleEndorsementSpan) {
		t.Fatalf("an endorsement past a document's expiry = %v, want ErrRoleEndorsementSpan", err)
	}
	shorter := keys
	shorter.DocumentNotAfter = time.Time{}
	shorter.RepublishPeriod = 20 * time.Second
	if err := VerifyRoleEndorsement(e, shorter, now); !errors.Is(err, ErrRoleEndorsementSpan) {
		t.Fatalf("an endorsement longer than three of the document's periods = %v, want ErrRoleEndorsementSpan", err)
	}
	expired := keys
	expired.DocumentNotAfter = now
	if _, err := SignRoleEndorsement(rolePriv, "control-db", "node-1", nodeKey, now, expired); !errors.Is(err, ErrRoleEndorsementSpan) {
		t.Fatalf("signing under an expired document = %v, want ErrRoleEndorsementSpan", err)
	}
	if _, err := ParseRoleEndorsement("not.an.endorsement"); err == nil {
		t.Fatal("a malformed value parsed")
	}
}
