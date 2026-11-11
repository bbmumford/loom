/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"strings"
	"testing"
	"time"

	"github.com/bbmumford/swarm"
)

// The anchor public keys a running node's observer gate admits endorsements
// under follow the list a reloaded mesh document names: a holder the booted
// document did not list is refused, admitted once SetAnchorPublicKeys lists
// it, and a holder the new list drops is refused at once. An entry that does
// not decode is named in the error and left out while the keys that decode
// apply, as the boot reading treats them, and a runtime with no observer gate
// refuses the call.
func TestAReloadedAnchorKeyListReachesTheRunningObserverGate(t *testing.T) {
	a, b := newAnchorTestHolder(t), newAnchorTestHolder(t)
	rt, si := initSwarmGate(t, AnchorConfig{PublicKey: a.pubHex()})
	idA, pubA := canonicalTestNode(t)
	idB, pubB := canonicalTestNode(t)
	now := time.Now().UnixNano()
	for _, r := range []swarm.Record{
		{NodeID: idA, Body: endorsedPeerBodyAt(t, idA, pubA, a, now), PubKey: pubA},
		{NodeID: idB, Body: endorsedPeerBodyAt(t, idB, pubB, b, now), PubKey: pubB},
	} {
		if err := si.RoleTable.onRecord(r); err != nil {
			t.Fatal(err)
		}
	}
	if err := admitObserver(si, idB, pubB); err == nil {
		t.Fatal("a holder the booted document does not list is admitted")
	}

	if err := rt.SetAnchorPublicKeys([]string{a.pubHex(), b.pubHex()}); err != nil {
		t.Fatalf("SetAnchorPublicKeys: %v", err)
	}
	for who, n := range map[string]struct {
		id  swarm.NodeID
		pub []byte
	}{"A": {idA, pubA}, "B": {idB, pubB}} {
		if err := admitObserver(si, n.id, n.pub); err != nil {
			t.Fatalf("holder %s is refused once the reloaded list names it: %v", who, err)
		}
	}

	if err := rt.SetAnchorPublicKeys([]string{b.pubHex()}); err != nil {
		t.Fatalf("SetAnchorPublicKeys: %v", err)
	}
	if err := admitObserver(si, idA, pubA); err == nil {
		t.Fatal("a holder the reloaded list drops is still admitted")
	}

	err := rt.SetAnchorPublicKeys([]string{b.pubHex(), "not-a-key"})
	if err == nil || !strings.Contains(err.Error(), "PublicKeys[1]") || strings.Contains(err.Error(), "not-a-key") {
		t.Fatalf("SetAnchorPublicKeys with an undecodable entry returned %v; want an error naming PublicKeys[1] and not its value", err)
	}
	if err := admitObserver(si, idB, pubB); err != nil {
		t.Fatalf("the decodable key beside an undecodable entry does not apply: %v", err)
	}

	if err := (&Runtime{}).SetAnchorPublicKeys([]string{a.pubHex()}); err == nil {
		t.Fatal("a runtime with no observer gate accepts an anchor key list")
	}
}
