/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"strings"
	"sync"
	"testing"
	"time"

	aether "github.com/ORBTR/aether"
	"github.com/bbmumford/ledger/anchor"
	"github.com/bbmumford/loom/directory"
	"github.com/bbmumford/loom/ports"
	"github.com/bbmumford/swarm"
	swarmpb "github.com/bbmumford/swarm/proto/pb"
	"google.golang.org/protobuf/proto"
)

// anchorTestHolder is one anchor-key holder's pair, generated per test.
type anchorTestHolder struct {
	pub  ed25519.PublicKey
	priv ed25519.PrivateKey
}

func newAnchorTestHolder(t *testing.T) anchorTestHolder {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return anchorTestHolder{pub: pub, priv: priv}
}

func (h anchorTestHolder) pubHex() string { return hex.EncodeToString(h.pub) }

// observerHub delivers swarm frames between in-process swarm nodes, each on
// its own goroutine, so a test drives swarm's real attestation accumulator
// with the gate InitSwarm builds.
type observerHub struct {
	mu    sync.Mutex
	nodes map[swarm.NodeID]*observerHubTransport
}

type observerHubTransport struct {
	hub  *observerHub
	id   swarm.NodeID
	mu   sync.Mutex
	recv func(swarm.NodeID, []byte)
	join func(swarm.NodeID)
}

func (t *observerHubTransport) LocalID() swarm.NodeID { return t.id }
func (t *observerHubTransport) Send(to swarm.NodeID, frame []byte) error {
	t.hub.mu.Lock()
	dst := t.hub.nodes[to]
	t.hub.mu.Unlock()
	if dst == nil {
		return nil
	}
	dst.mu.Lock()
	fn := dst.recv
	dst.mu.Unlock()
	if fn != nil {
		cp := append([]byte(nil), frame...)
		go fn(t.id, cp)
	}
	return nil
}
func (t *observerHubTransport) Broadcast(frame []byte) error {
	for _, p := range t.Peers() {
		_ = t.Send(p, frame)
	}
	return nil
}
func (t *observerHubTransport) Peers() []swarm.NodeID {
	t.hub.mu.Lock()
	defer t.hub.mu.Unlock()
	var out []swarm.NodeID
	for id := range t.hub.nodes {
		if id != t.id {
			out = append(out, id)
		}
	}
	return out
}
func (t *observerHubTransport) OnReceive(fn func(swarm.NodeID, []byte)) {
	t.mu.Lock()
	t.recv = fn
	t.mu.Unlock()
}
func (t *observerHubTransport) OnPeerJoin(fn func(swarm.NodeID)) {
	t.mu.Lock()
	t.join = fn
	t.mu.Unlock()
}
func (t *observerHubTransport) OnPeerLeave(func(swarm.NodeID)) {}

// observerHubNode is one in-process swarm node with a canonical aether NodeID.
type observerHubNode struct {
	node swarm.Node
	id   swarm.NodeID
	pub  ed25519.PublicKey
}

func newObserverHubNode(t *testing.T, hub *observerHub) observerHubNode {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	aid, err := aether.NewNodeID(pub)
	if err != nil {
		t.Fatal(err)
	}
	id := swarm.NodeID(string(aid))
	n, err := swarm.New(swarm.Config{NodeID: id, PrivKey: priv, TreeDegree: 4, MaxRecordBytes: peerRecordMaxBytes})
	if err != nil {
		t.Fatal(err)
	}
	tp := &observerHubTransport{hub: hub, id: id}
	hub.mu.Lock()
	hub.nodes[id] = tp
	hub.mu.Unlock()
	if err := swarm.Wire(n, tp); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = n.Start(ctx) }()
	t.Cleanup(func() { cancel(); _ = n.Stop() })
	return observerHubNode{node: n, id: id, pub: pub}
}

func (h *observerHub) joinAll() {
	h.mu.Lock()
	var all []*observerHubTransport
	for _, tp := range h.nodes {
		all = append(all, tp)
	}
	h.mu.Unlock()
	for _, a := range all {
		for _, b := range all {
			if a == b {
				continue
			}
			a.mu.Lock()
			fn := a.join
			a.mu.Unlock()
			if fn != nil {
				fn(b.id)
			}
		}
	}
}

// endorsedPeerBody is a fleet.peer record for id advertising roles, issued
// now, carrying holder's endorsement of (id, nodeKey) when holder is non-nil.
func endorsedPeerBody(t *testing.T, id swarm.NodeID, nodeKey ed25519.PublicKey, roles []string, holder *anchorTestHolder) []byte {
	t.Helper()
	issuedAt := time.Now().UnixNano()
	var tags []string
	if holder != nil {
		v, err := directory.SignAnchorEndorsement(holder.priv, ports.NodeID(string(id)), nodeKey, issuedAt)
		if err != nil {
			t.Fatal(err)
		}
		tags = append(tags, directory.AnchorEndorsementTag+"="+v)
	}
	body, err := proto.Marshal(&swarmpb.PeerRecord{
		NodeId:       []byte(id),
		IssuedAt:     issuedAt,
		Capabilities: &swarmpb.Capabilities{Roles: roles, Tags: tags},
	})
	if err != nil {
		t.Fatal(err)
	}
	return body
}

// A holder producing two attestations counts once: one holder's two machines
// (two node identities, both endorsed by the holder's one anchor key) attest a
// live target and the receiver's K=2 quorum is not met; a second holder's
// machine then attests and the target is evicted, two distinct anchor keys.
// Every node here is a real swarm node and the receiver's gate is the one
// InitSwarm wires (observerCheck over anchorObserverPolicy).
func TestOneHoldersTwoAttestationsCountOnceTowardTheQuorum(t *testing.T) {
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

	for _, p := range []struct {
		n      observerHubNode
		roles  []string
		holder *anchorTestHolder
	}{
		{target, []string{"auth"}, nil},
		{machineA1, []string{"anchor"}, &holderA},
		{machineA2, []string{"anchor"}, &holderA},
		{machineB, []string{"anchor"}, &holderB},
	} {
		if err := p.n.node.Publish(topicFleetPeer, endorsedPeerBody(t, p.n.id, p.n.pub, p.roles, p.holder)); err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		held := 0
		for _, n := range []observerHubNode{machineA1, machineA2, machineB} {
			if _, ok := table.PeerInfo(string(n.id)); ok {
				held++
			}
		}
		_, targetHeld := receiver.node.Get(topicFleetPeer, target.id)
		if held == 3 && targetHeld {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the receiver never held the four fleet.peer records")
		}
		time.Sleep(20 * time.Millisecond)
	}

	for _, n := range []observerHubNode{machineA1, machineA2} {
		if err := n.node.PublishObserverTombstone(topicFleetPeer, target.id); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(2 * time.Second)
	if _, ok := receiver.node.Get(topicFleetPeer, target.id); !ok {
		t.Fatalf("one holder's two machines evicted %s: their two attestations counted twice toward K=2", target.id)
	}

	if err := machineB.node.PublishObserverTombstone(topicFleetPeer, target.id); err != nil {
		t.Fatal(err)
	}
	deadline = time.Now().Add(10 * time.Second)
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

// observerGateFixture is a RoleTable fed fleet.peer records directly and the
// gate InitSwarm builds over it for the listed holders.
func observerGateFixture(t *testing.T, listed ...anchorTestHolder) (*RoleTable, func(swarm.NodeID, []byte, swarm.NodeID) bool) {
	t.Helper()
	table, err := NewRoleTable(nil, "self")
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	for _, h := range listed {
		keys = append(keys, h.pubHex())
	}
	return table, observerCheck(anchorObserverPolicy(Config{Anchor: AnchorConfig{PublicKeys: keys}}, table))
}

// gateTestSubject is the node the gate fixture's attestations are about: a
// node other than every observer asked, so only the observer's standing
// decides.
const gateTestSubject = swarm.NodeID("attested-target")

// canonicalTestNode is a fresh node identity whose NodeID derives from its key.
func canonicalTestNode(t *testing.T) (swarm.NodeID, ed25519.PublicKey) {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	aid, err := aether.NewNodeID(pub)
	if err != nil {
		t.Fatal(err)
	}
	return swarm.NodeID(string(aid)), pub
}

// Asked of the gate InitSwarm wires: a node holding no anchor key that
// advertises the "anchor" role on its fleet.peer record is not an observer,
// whether or not its NodeID derives from its key.
func TestASelfAdvertisedAnchorIsNotAnObserver(t *testing.T) {
	configured := newAnchorTestHolder(t)
	table, gate := observerGateFixture(t, configured)

	impostorPub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	impostorID := swarm.NodeID("impostor-" + hex.EncodeToString(impostorPub[:4]))
	canonicalID, canonicalPub := canonicalTestNode(t)
	for _, n := range []struct {
		id  swarm.NodeID
		pub ed25519.PublicKey
	}{{impostorID, impostorPub}, {canonicalID, canonicalPub}} {
		if err := table.onRecord(swarm.Record{NodeID: n.id, Body: endorsedPeerBody(t, n.id, n.pub, []string{"anchor"}, nil), PubKey: n.pub}); err != nil {
			t.Fatal(err)
		}
		if gate(n.id, n.pub, gateTestSubject) {
			t.Fatalf("an attestation from %s, which advertises the anchor role and holds no configured anchor key, passes the observer gate", n.id)
		}
	}
}

// Only an endorsement by a listed anchor key, over this node's own ID and the
// key that signed its record, admits it.
func TestTheObserverGateAdmitsOnlyAListedHoldersEndorsementOfThatNode(t *testing.T) {
	listed, unlisted := newAnchorTestHolder(t), newAnchorTestHolder(t)
	table, gate := observerGateFixture(t, listed)

	endorsedID, endorsedPub := canonicalTestNode(t)
	if err := table.onRecord(swarm.Record{NodeID: endorsedID, Body: endorsedPeerBody(t, endorsedID, endorsedPub, nil, &listed), PubKey: endorsedPub}); err != nil {
		t.Fatal(err)
	}
	if !gate(endorsedID, endorsedPub, gateTestSubject) {
		t.Fatal("a node endorsed by a listed anchor key is refused as an observer")
	}
	otherID, otherPub := canonicalTestNode(t)
	if gate(endorsedID, otherPub, gateTestSubject) {
		t.Fatal("an attestation whose key is not the endorsed node's passes")
	}

	strangerID, strangerPub := canonicalTestNode(t)
	if err := table.onRecord(swarm.Record{NodeID: strangerID, Body: endorsedPeerBody(t, strangerID, strangerPub, nil, &unlisted), PubKey: strangerPub}); err != nil {
		t.Fatal(err)
	}
	if gate(strangerID, strangerPub, gateTestSubject) {
		t.Fatal("a node endorsed by an anchor key the mesh document does not list passes")
	}

	// The endorsed node's tag copied onto another node's record.
	info, _ := table.PeerInfo(string(endorsedID))
	stolen := directory.AnchorEndorsementTag + "=" + info.Tags[directory.AnchorEndorsementTag]
	body, err := proto.Marshal(&swarmpb.PeerRecord{
		NodeId:       []byte(otherID),
		IssuedAt:     time.Now().UnixNano(),
		Capabilities: &swarmpb.Capabilities{Tags: []string{stolen}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := table.onRecord(swarm.Record{NodeID: otherID, Body: body, PubKey: otherPub}); err != nil {
		t.Fatal(err)
	}
	if gate(otherID, otherPub, gateTestSubject) {
		t.Fatal("another node's endorsement copied onto a record passes")
	}
}

// InitSwarm wires the endorsement gate from the runtime's AnchorConfig: the
// policy it asks admits a peer endorsed by a listed key as RoleTable receives
// it, and refuses a self-advertised anchor.
func TestInitSwarmAsksTheEndorsementPolicyBuiltFromAnchorConfig(t *testing.T) {
	holder := newAnchorTestHolder(t)
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
		cfg:      Config{Anchor: AnchorConfig{PublicKey: holder.pubHex()}},
	}
	si, err := rt.InitSwarm(nil)
	if err != nil {
		t.Fatalf("InitSwarm: %v", err)
	}
	t.Cleanup(func() { _ = si.Node.Stop() })
	if si.observers == nil {
		t.Fatal("InitSwarm built no observer policy")
	}

	endorsedID, endorsedPub := canonicalTestNode(t)
	advertisedID, advertisedPub := canonicalTestNode(t)
	for _, r := range []swarm.Record{
		{NodeID: endorsedID, Body: endorsedPeerBody(t, endorsedID, endorsedPub, nil, &holder), PubKey: endorsedPub},
		{NodeID: advertisedID, Body: endorsedPeerBody(t, advertisedID, advertisedPub, []string{"anchor"}, nil), PubKey: advertisedPub},
	} {
		if err := si.RoleTable.onRecord(r); err != nil {
			t.Fatal(err)
		}
	}
	ctx := context.Background()
	if err := si.observers.AuthorizeObserver(ctx, ports.Principal{NodeID: ports.NodeID(string(endorsedID)), PubKey: endorsedPub}, ""); err != nil {
		t.Fatalf("the wired policy refuses a node endorsed by the configured anchor key: %v", err)
	}
	if err := si.observers.AuthorizeObserver(ctx, ports.Principal{NodeID: ports.NodeID(string(advertisedID)), PubKey: advertisedPub}, ""); err == nil {
		t.Fatal("the wired policy admits a self-advertised anchor")
	}
}

// A holder's fleet.peer record carries its anchor key's endorsement of its own
// node ID, identity key and the record's IssuedAt; a node without an anchor
// private key publishes none.
func TestAHoldersPeerRecordCarriesItsAnchorEndorsement(t *testing.T) {
	holder := newAnchorTestHolder(t)
	publish := func(rt *Runtime) *swarmpb.PeerRecord {
		t.Helper()
		node := &capturingPublishNode{stubSwarmNode: &stubSwarmNode{}}
		NewPeerPublisher(node, rt, nil).publishOnce()
		var rec swarmpb.PeerRecord
		if err := proto.Unmarshal(node.body, &rec); err != nil {
			t.Fatalf("published bytes do not decode as a PeerRecord: %v", err)
		}
		return &rec
	}
	endorsement := func(rec *swarmpb.PeerRecord) (string, bool) {
		for _, tag := range rec.GetCapabilities().GetTags() {
			if v, ok := strings.CutPrefix(tag, directory.AnchorEndorsementTag+"="); ok {
				return v, true
			}
		}
		return "", false
	}

	rt := bridgeTestRuntime(t)
	aid, err := aether.NewNodeID(rt.identity.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	rt.identity.NodeID = aid
	rt.cfg.Anchor = AnchorConfig{PublicKey: holder.pubHex(), PrivateKey: hex.EncodeToString(holder.priv)}
	rec := publish(rt)
	v, ok := endorsement(rec)
	if !ok {
		t.Fatal("a holder's fleet.peer record carries no anchor endorsement")
	}
	e, err := directory.ParseAnchorEndorsement(v)
	if err != nil {
		t.Fatal(err)
	}
	policy := directory.NewPolicy(directory.PolicyConfig{AnchorKeys: []ed25519.PublicKey{holder.pub}})
	id := ports.NodeID(string(aid))
	policy.SetAnchorEndorsements(staticEndorsements{id: {NodeKey: rt.identity.PublicKey, IssuedAt: rec.IssuedAt, Endorsement: e}})
	if err := policy.AuthorizeObserver(context.Background(), ports.Principal{NodeID: id, PubKey: rt.identity.PublicKey}, ""); err != nil {
		t.Fatalf("the published endorsement does not admit its own node: %v", err)
	}
	policy.SetAnchorEndorsements(staticEndorsements{id: {NodeKey: rt.identity.PublicKey, IssuedAt: rec.IssuedAt + 1, Endorsement: e}})
	if err := policy.AuthorizeObserver(context.Background(), ports.Principal{NodeID: id, PubKey: rt.identity.PublicKey}, ""); err == nil {
		t.Fatal("the endorsement verifies for a record with another IssuedAt")
	}

	plain := bridgeTestRuntime(t)
	plain.cfg.Anchor = AnchorConfig{PublicKey: holder.pubHex()}
	if _, ok := endorsement(publish(plain)); ok {
		t.Fatal("a node holding no anchor private key publishes an anchor endorsement")
	}
}

type staticEndorsements map[ports.NodeID]directory.EndorsedRecord

func (s staticEndorsements) AnchorEndorsements() map[ports.NodeID]directory.EndorsedRecord { return s }

// PublicKey alone is a one-entry list, PublicKeys adds each holder's key once,
// and an undecodable entry is named (by setting, not value) beside the keys
// that decode.
func TestAnchorPublicKeysReadsPublicKeyAsAOneEntryList(t *testing.T) {
	a, b := newAnchorTestHolder(t), newAnchorTestHolder(t)
	keys, err := anchorPublicKeys(AnchorConfig{PublicKey: a.pubHex()})
	if err != nil || len(keys) != 1 || !keys[0].Equal(a.pub) {
		t.Fatalf("PublicKey alone = %d keys, %v; want the one key", len(keys), err)
	}
	keys, err = anchorPublicKeys(AnchorConfig{PublicKey: a.pubHex(), PublicKeys: []string{b.pubHex(), a.pubHex(), " "}})
	if err != nil || len(keys) != 2 || !keys[0].Equal(a.pub) || !keys[1].Equal(b.pub) {
		t.Fatalf("PublicKey plus PublicKeys = %d keys, %v; want PublicKey then the other key, once each", len(keys), err)
	}
	keys, err = anchorPublicKeys(AnchorConfig{PublicKey: "not-hex", PublicKeys: []string{b.pubHex()}})
	if err == nil || !strings.Contains(err.Error(), "PublicKey") || len(keys) != 1 {
		t.Fatalf("an undecodable PublicKey = %d keys, %v; want it named and the decodable key kept", len(keys), err)
	}
	if strings.Contains(err.Error(), b.pubHex()) {
		t.Fatal("the error carries a key value")
	}
}

// A snapshot signed by any listed holder's anchor key verifies; one signed by
// an unlisted key does not.
func TestASnapshotFromAnyListedHolderVerifies(t *testing.T) {
	a, b, unlisted := newAnchorTestHolder(t), newAnchorTestHolder(t), newAnchorTestHolder(t)
	snapshotBy := func(h anchorTestHolder) *anchor.Snapshot {
		t.Helper()
		signer, err := anchor.NewSigner("holder", hex.EncodeToString(h.priv))
		if err != nil {
			t.Fatal(err)
		}
		content := anchor.SnapshotContent{Timestamp: time.Now().Unix(), Sequence: 1, NodeID: "holder"}
		sig, err := signer.Sign(&content)
		if err != nil {
			t.Fatal(err)
		}
		return &anchor.Snapshot{Header: anchor.SnapshotHeader{Timestamp: content.Timestamp, Sequence: 1, NodeID: "holder", Signature: sig}}
	}
	listed := []ed25519.PublicKey{a.pub, b.pub}
	if err := verifyAnchorSnapshot(snapshotBy(b), listed); err != nil {
		t.Fatalf("a snapshot from the second listed holder: %v", err)
	}
	if err := verifyAnchorSnapshot(snapshotBy(unlisted), listed); err == nil {
		t.Fatal("a snapshot signed by an unlisted anchor key verifies")
	}
	if err := verifyAnchorSnapshot(snapshotBy(a), nil); err == nil {
		t.Fatal("a snapshot verifies against no listed key")
	}
}
