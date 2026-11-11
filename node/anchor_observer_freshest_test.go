/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	aether "github.com/ORBTR/aether"
	"github.com/bbmumford/loom/directory"
	"github.com/bbmumford/loom/ports"
	"github.com/bbmumford/swarm"
	swarmpb "github.com/bbmumford/swarm/proto/pb"
	"google.golang.org/protobuf/proto"
)

// initSwarmGate runs InitSwarm on a runtime configured with anchor and
// returns the runtime and its SwarmIntegration, whose observers field is the
// policy the wired observer gate asks.
func initSwarmGate(t *testing.T, anchor AnchorConfig) (*Runtime, *SwarmIntegration) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	aid, err := aether.NewNodeID(pub)
	if err != nil {
		t.Fatal(err)
	}
	rt := &Runtime{
		ctx:      context.Background(),
		identity: &NodeIdentity{NodeID: aid, PublicKey: pub, PrivateKey: priv},
		cfg:      Config{Anchor: anchor},
	}
	si, err := rt.InitSwarm(nil)
	if err != nil {
		t.Fatalf("InitSwarm: %v", err)
	}
	t.Cleanup(func() { _ = si.Node.Stop() })
	if si.observers == nil {
		t.Fatal("InitSwarm built no observer policy")
	}
	return rt, si
}

// endorsedPeerBodyAt is endorsedPeerBody issued at issuedAt (unix nanoseconds).
func endorsedPeerBodyAt(t *testing.T, id swarm.NodeID, nodeKey ed25519.PublicKey, holder anchorTestHolder, issuedAt int64) []byte {
	t.Helper()
	v, err := directory.SignAnchorEndorsement(holder.priv, ports.NodeID(string(id)), nodeKey, issuedAt)
	if err != nil {
		t.Fatal(err)
	}
	body, err := proto.Marshal(&swarmpb.PeerRecord{NodeId: []byte(id), IssuedAt: issuedAt,
		Capabilities: &swarmpb.Capabilities{Tags: []string{directory.AnchorEndorsementTag + "=" + v}}})
	if err != nil {
		t.Fatal(err)
	}
	return body
}

func admitObserver(si *SwarmIntegration, id swarm.NodeID, pub ed25519.PublicKey) error {
	return si.observers.AuthorizeObserver(context.Background(),
		ports.Principal{NodeID: ports.NodeID(string(id)), PubKey: pub}, ports.NodeID(string(gateTestSubject)))
}

// A holder's machine replaced by another counts at once: while the dead
// machine's last endorsed record is still held (issued ten minutes ago, inside
// the role table's staleness window), the replacement's fresher endorsement
// under the same anchor key is the one admitted, whichever of the two NodeIDs
// is lower, and the dead machine's is not.
func TestAHoldersReplacementMachineCountsAtOnceBesideItsDeadMachinesRecord(t *testing.T) {
	for _, order := range []string{"replacement has the higher NodeID", "replacement has the lower NodeID"} {
		t.Run(order, func(t *testing.T) {
			a := newAnchorTestHolder(t)
			_, si := initSwarmGate(t, AnchorConfig{PublicKey: a.pubHex()})
			deadID, deadPub := canonicalTestNode(t)
			liveID, livePub := canonicalTestNode(t)
			if (liveID > deadID) != (order == "replacement has the higher NodeID") {
				deadID, deadPub, liveID, livePub = liveID, livePub, deadID, deadPub
			}
			now := time.Now()
			for _, r := range []swarm.Record{
				{NodeID: deadID, Body: endorsedPeerBodyAt(t, deadID, deadPub, a, now.Add(-10*time.Minute).UnixNano()), PubKey: deadPub},
				{NodeID: liveID, Body: endorsedPeerBodyAt(t, liveID, livePub, a, now.UnixNano()), PubKey: livePub},
			} {
				if err := si.RoleTable.onRecord(r); err != nil {
					t.Fatal(err)
				}
			}
			if err := admitObserver(si, liveID, livePub); err != nil {
				t.Fatalf("the holder's replacement machine is refused beside its dead machine's held record: %v", err)
			}
			if err := admitObserver(si, deadID, deadPub); err == nil {
				t.Fatal("the dead machine's staler endorsement still counts beside its replacement's")
			}
		})
	}
}

// Through swarm's real accumulator: a holder whose dead machine's record is
// still held attests through its replacement, and with a second holder's
// machine the K=2 quorum evicts the target at once.
func TestAReplacementMachineAndASecondHolderEvictAtOnce(t *testing.T) {
	holderA, holderB := newAnchorTestHolder(t), newAnchorTestHolder(t)
	hub := &observerHub{nodes: map[swarm.NodeID]*observerHubTransport{}}
	receiver := newObserverHubNode(t, hub)
	target := newObserverHubNode(t, hub)
	deadA := newObserverHubNode(t, hub)
	liveA := newObserverHubNode(t, hub)
	if liveA.id < deadA.id {
		deadA, liveA = liveA, deadA
	}
	machineB := newObserverHubNode(t, hub)
	hub.joinAll()

	table, err := NewRoleTable(receiver.node, string(receiver.id))
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{Anchor: AnchorConfig{PublicKeys: []string{holderA.pubHex(), holderB.pubHex()}}}
	receiver.node.SetObserverRoleCheck(observerCheck(anchorObserverPolicy(cfg, table)))

	now := time.Now()
	for _, p := range []struct {
		n    observerHubNode
		body []byte
	}{
		{target, endorsedPeerBody(t, target.id, target.pub, []string{"auth"}, nil)},
		{deadA, endorsedPeerBodyAt(t, deadA.id, deadA.pub, holderA, now.Add(-10*time.Minute).UnixNano())},
		{liveA, endorsedPeerBodyAt(t, liveA.id, liveA.pub, holderA, now.UnixNano())},
		{machineB, endorsedPeerBodyAt(t, machineB.id, machineB.pub, holderB, now.UnixNano())},
	} {
		if err := p.n.node.Publish(topicFleetPeer, p.body); err != nil {
			t.Fatal(err)
		}
	}
	waitHeld(t, receiver, table, target.id, deadA.id, liveA.id, machineB.id)

	for _, n := range []observerHubNode{liveA, machineB} {
		if err := n.node.PublishObserverTombstone(topicFleetPeer, target.id); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, ok := receiver.node.Get(topicFleetPeer, target.id); !ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("a holder's replacement machine and a second holder did not evict %s while the dead machine's record is held", target.id)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// One holder's two live machines, each counted while its own endorsement is
// the freshest under the holder's key, never reach the K=2 quorum together:
// machine 1 attests, machine 2 then publishes a fresher record and attests, and
// the target stays; a second holder's attestation then evicts it, two distinct
// anchor keys.
func TestOneHoldersAlternatingMachinesNeverReachAQuorumOfTwo(t *testing.T) {
	holderA, holderB := newAnchorTestHolder(t), newAnchorTestHolder(t)
	hub := &observerHub{nodes: map[swarm.NodeID]*observerHubTransport{}}
	receiver := newObserverHubNode(t, hub)
	target := newObserverHubNode(t, hub)
	machineA1 := newObserverHubNode(t, hub)
	machineA2 := newObserverHubNode(t, hub)
	machineB := newObserverHubNode(t, hub)
	hub.joinAll()

	table, err := NewRoleTable(receiver.node, string(receiver.id))
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{Anchor: AnchorConfig{PublicKeys: []string{holderA.pubHex(), holderB.pubHex()}}}
	receiver.node.SetObserverRoleCheck(observerCheck(anchorObserverPolicy(cfg, table)))

	now := time.Now()
	for _, p := range []struct {
		n    observerHubNode
		body []byte
	}{
		{target, endorsedPeerBody(t, target.id, target.pub, []string{"auth"}, nil)},
		{machineA1, endorsedPeerBodyAt(t, machineA1.id, machineA1.pub, holderA, now.UnixNano())},
		{machineB, endorsedPeerBodyAt(t, machineB.id, machineB.pub, holderB, now.UnixNano())},
	} {
		if err := p.n.node.Publish(topicFleetPeer, p.body); err != nil {
			t.Fatal(err)
		}
	}
	waitHeld(t, receiver, table, target.id, machineA1.id, machineB.id)
	if err := machineA1.node.PublishObserverTombstone(topicFleetPeer, target.id); err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)

	if err := machineA2.node.Publish(topicFleetPeer, endorsedPeerBodyAt(t, machineA2.id, machineA2.pub, holderA, time.Now().UnixNano())); err != nil {
		t.Fatal(err)
	}
	waitHeld(t, receiver, table, target.id, machineA2.id)
	if err := machineA2.node.PublishObserverTombstone(topicFleetPeer, target.id); err != nil {
		t.Fatal(err)
	}
	time.Sleep(2 * time.Second)
	if _, ok := receiver.node.Get(topicFleetPeer, target.id); !ok {
		t.Fatalf("one holder's two machines evicted %s: each counted while its endorsement was the freshest, so one anchor key counted twice toward K=2", target.id)
	}

	if err := machineB.node.PublishObserverTombstone(topicFleetPeer, target.id); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		if _, ok := receiver.node.Get(topicFleetPeer, target.id); !ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("attestations under two distinct listed anchor keys did not evict %s", target.id)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// waitHeld waits until the receiver holds target's fleet.peer record and its
// role table every listed peer's.
func waitHeld(t *testing.T, receiver observerHubNode, table *RoleTable, target swarm.NodeID, peers ...swarm.NodeID) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		held := 0
		for _, id := range peers {
			if _, ok := table.PeerInfo(string(id)); ok {
				held++
			}
		}
		_, targetHeld := receiver.node.Get(topicFleetPeer, target)
		if held == len(peers) && targetHeld {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("the receiver held %d of %d peer records (target held %v)", held, len(peers), targetHeld)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// The gate InitSwarm wires is handed each attestation's subject: a listed
// holder's machine attesting about another node is admitted, and the same
// machine attesting about itself is refused, as swarm's sender-side refusal
// does not bind a crafted attestation.
func TestTheWiredObserverGateRefusesAHolderAttestingAboutItself(t *testing.T) {
	a := newAnchorTestHolder(t)
	_, si := initSwarmGate(t, AnchorConfig{PublicKey: a.pubHex()})
	id, pub := canonicalTestNode(t)
	if err := si.RoleTable.onRecord(swarm.Record{NodeID: id, Body: endorsedPeerBodyAt(t, id, pub, a, time.Now().UnixNano()), PubKey: pub}); err != nil {
		t.Fatal(err)
	}
	gate := observerCheck(si.observers)
	if !gate(id, pub, gateTestSubject) {
		t.Fatal("a listed holder's machine attesting about another node is refused")
	}
	if gate(id, pub, id) {
		t.Fatal("a listed holder's machine attesting about itself passes the gate")
	}
}
