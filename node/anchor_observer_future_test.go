/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"testing"
	"time"

	"github.com/bbmumford/loom/directory"
	"github.com/bbmumford/loom/ports"
	"github.com/bbmumford/swarm"
)

// A holder's dead machine whose last fleet.peer record is dated an hour ahead
// of this node's clock (a machine whose clock ran fast) does not outrank its
// replacement: the role table refuses the record, so the replacement's
// endorsement under the same anchor key is the one admitted at once and the
// dead machine is not admitted, and the dead machine is not held as a peer
// either, so the staleness prune that ages records by IssuedAt is not what
// removes it.
func TestARecordDatedBeyondTheClockAllowanceDoesNotOutrankTheReplacement(t *testing.T) {
	a := newAnchorTestHolder(t)
	_, si := initSwarmGate(t, AnchorConfig{PublicKeys: []string{a.pubHex()}})
	deadID, deadPub := canonicalTestNode(t)
	liveID, livePub := canonicalTestNode(t)
	now := time.Now()

	if err := si.RoleTable.onRecord(swarm.Record{NodeID: deadID,
		Body: endorsedPeerBodyAt(t, deadID, deadPub, a, now.Add(time.Hour).UnixNano()), PubKey: deadPub}); err != nil {
		t.Fatalf("onRecord of the future-dated record: %v", err)
	}
	if err := si.RoleTable.onRecord(swarm.Record{NodeID: liveID,
		Body: endorsedPeerBodyAt(t, liveID, livePub, a, now.UnixNano()), PubKey: livePub}); err != nil {
		t.Fatalf("onRecord of the replacement's record: %v", err)
	}

	if err := admitObserver(si, liveID, livePub); err != nil {
		t.Errorf("the replacement is refused beside a record dated an hour ahead: %v", err)
	}
	if err := admitObserver(si, deadID, deadPub); err == nil {
		t.Error("the dead machine whose record is dated an hour ahead is admitted as an observer")
	}
	if _, held := si.RoleTable.AnchorEndorsements()[ports.NodeID(string(deadID))]; held {
		t.Error("the role table holds the record dated an hour ahead")
	}
}

// A record dated ahead of this node's clock by less than the allowance is a
// clock difference the fleet runs with: the role table holds it, and under one
// anchor key it outranks an older record as the freshest rule says, until the
// other machine's next record is dated later.
func TestARecordDatedAheadWithinTheClockAllowanceIsHeld(t *testing.T) {
	a := newAnchorTestHolder(t)
	_, si := initSwarmGate(t, AnchorConfig{PublicKeys: []string{a.pubHex()}})
	aheadID, aheadPub := canonicalTestNode(t)
	otherID, otherPub := canonicalTestNode(t)
	now := time.Now()
	ahead := now.Add(directory.MaxIssuedAtSkew - time.Minute)

	if err := si.RoleTable.onRecord(swarm.Record{NodeID: aheadID,
		Body: endorsedPeerBodyAt(t, aheadID, aheadPub, a, ahead.UnixNano()), PubKey: aheadPub}); err != nil {
		t.Fatal(err)
	}
	if err := si.RoleTable.onRecord(swarm.Record{NodeID: otherID,
		Body: endorsedPeerBodyAt(t, otherID, otherPub, a, now.UnixNano()), PubKey: otherPub}); err != nil {
		t.Fatal(err)
	}
	if _, held := si.RoleTable.AnchorEndorsements()[ports.NodeID(string(aheadID))]; !held {
		t.Fatal("the role table refuses a record dated ahead by less than the allowance")
	}
	if err := admitObserver(si, aheadID, aheadPub); err != nil {
		t.Errorf("the record dated ahead within the allowance is not the freshest: %v", err)
	}
	if err := admitObserver(si, otherID, otherPub); err == nil {
		t.Error("the older record under the same anchor key is admitted beside the freshest")
	}

	if err := si.RoleTable.onRecord(swarm.Record{NodeID: otherID,
		Body: endorsedPeerBodyAt(t, otherID, otherPub, a, ahead.Add(time.Second).UnixNano()), PubKey: otherPub}); err != nil {
		t.Fatal(err)
	}
	if err := admitObserver(si, otherID, otherPub); err != nil {
		t.Errorf("the machine whose next record is dated later is not admitted: %v", err)
	}
}
