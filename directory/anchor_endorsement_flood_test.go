/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package directory

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	aether "github.com/ORBTR/aether"
	"github.com/bbmumford/loom/ports"
)

// countVerifications makes verifyAnchorEndorsement count its signature
// checks for the length of t.
func countVerifications(t *testing.T) *atomic.Int64 {
	t.Helper()
	var n atomic.Int64
	real := verifyAnchorEndorsement
	verifyAnchorEndorsement = func(e AnchorEndorsement, id ports.NodeID, nodeKey ed25519.PublicKey, issuedAt int64) bool {
		n.Add(1)
		return real(e, id, nodeKey, issuedAt)
	}
	t.Cleanup(func() { verifyAnchorEndorsement = real })
	return &n
}

// floodNodes is n canonical node identities sorted by NodeID.
func floodNodes(t *testing.T, n int) ([]ports.NodeID, []ed25519.PublicKey) {
	t.Helper()
	type node struct {
		id  ports.NodeID
		key ed25519.PublicKey
	}
	nodes := make([]node, n)
	for i := range nodes {
		pub, _, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatal(err)
		}
		nid, err := aether.NewNodeID(pub)
		if err != nil {
			t.Fatal(err)
		}
		nodes[i] = node{ports.NodeID(string(nid)), pub}
	}
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].id < nodes[j].id })
	ids, keys := make([]ports.NodeID, n), make([]ed25519.PublicKey, n)
	for i, nd := range nodes {
		ids[i], keys[i] = nd.id, nd.key
	}
	return ids, keys
}

// A flood of fleet.peer records whose tags name a listed anchor key with a
// signature that key never made, each fresher than the holder's and under a
// lower NodeID, so every one is a record the holder's admission must rule
// out, costs one signature check per record however many attestations are
// judged against it: the holder's machine stays admitted on every check, and
// the checks made stay at one per record.
func TestAFloodOfBogusEndorsementsIsVerifiedOncePerRecord(t *testing.T) {
	const bogus, checks = 300, 50
	pubA, privA := newTestAnchorKey(t)
	ids, keys := floodNodes(t, bogus+1)
	holderID, holderKey := ids[bogus], keys[bogus]
	issuedAt := time.Now().UnixNano()
	v, err := SignAnchorEndorsement(privA, holderID, holderKey, issuedAt)
	if err != nil {
		t.Fatal(err)
	}
	e, err := ParseAnchorEndorsement(v)
	if err != nil {
		t.Fatal(err)
	}
	src := endorsementMap{holderID: {NodeKey: holderKey, IssuedAt: issuedAt, Endorsement: e}}
	for i := 0; i < bogus; i++ {
		sig := make([]byte, ed25519.SignatureSize)
		if _, err := rand.Read(sig); err != nil {
			t.Fatal(err)
		}
		src[ids[i]] = EndorsedRecord{NodeKey: keys[i], IssuedAt: issuedAt + int64(i) + 1,
			Endorsement: AnchorEndorsement{AnchorKey: pubA, Signature: sig}}
	}
	p := NewPolicy(PolicyConfig{AnchorKeys: []ed25519.PublicKey{pubA}})
	p.SetAnchorEndorsements(src)

	verified := countVerifications(t)
	for i := 0; i < checks; i++ {
		if err := p.AuthorizeObserver(context.Background(), ports.Principal{NodeID: holderID, PubKey: holderKey}, ""); err != nil {
			t.Fatalf("check %d: the holder's machine is refused under a flood of bogus endorsements: %v", i, err)
		}
	}
	if got, want := verified.Load(), int64(bogus+1); got > want {
		t.Fatalf("%d attestations judged against %d records made %d signature checks; want at most %d, one per record", checks, bogus+1, got, want)
	}
}

// A peer churning bogus records (each generation replacing the last) grows
// the signature cache by no more than the records the source holds: after
// every check the cache holds at most twice their count plus the floor, and
// the holder's machine stays admitted throughout.
func TestTheSignatureCacheStaysBoundedByTheHeldRecords(t *testing.T) {
	const perGeneration, generations = 200, 5
	pubA, privA := newTestAnchorKey(t)
	holderID, holderKey, holderRec := endorsedTestNode(t, privA)
	p := NewPolicy(PolicyConfig{AnchorKeys: []ed25519.PublicKey{pubA}})
	for g := 0; g < generations; g++ {
		src := endorsementMap{holderID: holderRec}
		ids, keys := floodNodes(t, perGeneration)
		for i, id := range ids {
			sig := make([]byte, ed25519.SignatureSize)
			if _, err := rand.Read(sig); err != nil {
				t.Fatal(err)
			}
			src[id] = EndorsedRecord{NodeKey: keys[i], IssuedAt: holderRec.IssuedAt + int64(i) + 1,
				Endorsement: AnchorEndorsement{AnchorKey: pubA, Signature: sig}}
		}
		p.SetAnchorEndorsements(src)
		if err := p.AuthorizeObserver(context.Background(), ports.Principal{NodeID: holderID, PubKey: holderKey}, ""); err != nil {
			t.Fatalf("generation %d: the holder's machine is refused: %v", g, err)
		}
		p.verifiedMu.Lock()
		held := len(p.verified)
		p.verifiedMu.Unlock()
		if bound := 2*len(src) + 64; held > bound {
			t.Fatalf("generation %d: the signature cache holds %d entries for %d records; want at most %d", g, held, len(src), bound)
		}
	}
}
