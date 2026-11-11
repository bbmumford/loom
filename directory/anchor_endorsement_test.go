/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package directory

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	aether "github.com/ORBTR/aether"
	"github.com/bbmumford/loom/ports"
)

type endorsementMap map[ports.NodeID]EndorsedRecord

func (m endorsementMap) AnchorEndorsements() map[ports.NodeID]EndorsedRecord { return m }

// endorsedTestNode is a canonical node identity and the record a holder's
// anchor key endorsed for it.
func endorsedTestNode(t *testing.T, holder ed25519.PrivateKey) (ports.NodeID, ed25519.PublicKey, EndorsedRecord) {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	nid, err := aether.NewNodeID(pub)
	if err != nil {
		t.Fatal(err)
	}
	id := ports.NodeID(string(nid))
	issuedAt := time.Now().UnixNano()
	v, err := SignAnchorEndorsement(holder, id, pub, issuedAt)
	if err != nil {
		t.Fatal(err)
	}
	e, err := ParseAnchorEndorsement(v)
	if err != nil {
		t.Fatal(err)
	}
	return id, pub, EndorsedRecord{NodeKey: pub, IssuedAt: issuedAt, Endorsement: e}
}

func newTestAnchorKey(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return pub, priv
}

// reissuedAt is rec re-signed by holder at issuedAt, as node id's next
// fleet.peer record carries it.
func reissuedAt(t *testing.T, holder ed25519.PrivateKey, id ports.NodeID, rec EndorsedRecord, issuedAt int64) EndorsedRecord {
	t.Helper()
	v, err := SignAnchorEndorsement(holder, id, rec.NodeKey, issuedAt)
	if err != nil {
		t.Fatal(err)
	}
	e, err := ParseAnchorEndorsement(v)
	if err != nil {
		t.Fatal(err)
	}
	return EndorsedRecord{NodeKey: rec.NodeKey, IssuedAt: issuedAt, Endorsement: e}
}

// One node per anchor key is an observer: the node whose valid endorsement
// under that key is the freshest (the highest IssuedAt), and at an equal
// IssuedAt the lower NodeID. Once that node's record is gone the next one
// counts, a node under another listed key counts beside it, and a fresher
// record whose endorsement does not verify displaces nobody.
func TestAuthorizeObserverCountsOneNodePerAnchorKey(t *testing.T) {
	ctx := context.Background()
	pubA, privA := newTestAnchorKey(t)
	pubB, privB := newTestAnchorKey(t)
	idOld, keyOld, recOld := endorsedTestNode(t, privA)
	idNew, keyNew, recNew := endorsedTestNode(t, privA)
	issued := time.Now().UnixNano()
	recOld = reissuedAt(t, privA, idOld, recOld, issued-int64(time.Minute))
	recNew = reissuedAt(t, privA, idNew, recNew, issued)
	idB, keyB, recB := endorsedTestNode(t, privB)
	src := endorsementMap{idOld: recOld, idNew: recNew, idB: recB}
	p := NewPolicy(PolicyConfig{AnchorKeys: []ed25519.PublicKey{pubA, pubB}})
	p.SetAnchorEndorsements(src)

	if err := p.AuthorizeObserver(ctx, ports.Principal{NodeID: idNew, PubKey: keyNew}, ""); err != nil {
		t.Fatalf("the freshest node under anchor key A: %v", err)
	}
	if err := p.AuthorizeObserver(ctx, ports.Principal{NodeID: idOld, PubKey: keyOld}, ""); err == nil {
		t.Fatal("a second node under anchor key A is admitted, so one holder counts twice")
	}
	if err := p.AuthorizeObserver(ctx, ports.Principal{NodeID: idB, PubKey: keyB}, ""); err != nil {
		t.Fatalf("the node under anchor key B: %v", err)
	}

	// At an equal IssuedAt the lower NodeID counts, on every reader.
	lo, hi := idOld, idNew
	keyLo, keyHi := keyOld, keyNew
	if hi < lo {
		lo, hi, keyLo, keyHi = hi, lo, keyHi, keyLo
	}
	src[lo] = reissuedAt(t, privA, lo, src[lo], issued)
	src[hi] = reissuedAt(t, privA, hi, src[hi], issued)
	if err := p.AuthorizeObserver(ctx, ports.Principal{NodeID: lo, PubKey: keyLo}, ""); err != nil {
		t.Fatalf("the lower node at an equal IssuedAt: %v", err)
	}
	if err := p.AuthorizeObserver(ctx, ports.Principal{NodeID: hi, PubKey: keyHi}, ""); err == nil {
		t.Fatal("both nodes under anchor key A are admitted at an equal IssuedAt")
	}
	src[idOld], src[idNew] = recOld, recNew

	delete(src, idNew)
	if err := p.AuthorizeObserver(ctx, ports.Principal{NodeID: idOld, PubKey: keyOld}, ""); err != nil {
		t.Fatalf("the remaining node under anchor key A once the fresher one's record is gone: %v", err)
	}

	// A fresher node whose endorsement does not verify does not displace a valid one.
	broken := recNew
	broken.IssuedAt++
	src[idNew] = broken
	if err := p.AuthorizeObserver(ctx, ports.Principal{NodeID: idNew, PubKey: keyNew}, ""); err == nil {
		t.Fatal("an endorsement over another IssuedAt is admitted")
	}
	if err := p.AuthorizeObserver(ctx, ports.Principal{NodeID: idOld, PubKey: keyOld}, ""); err != nil {
		t.Fatalf("an invalid endorsement under the same key displaced the valid one: %v", err)
	}
}

// A revoked node key, an unlisted anchor key and a policy with no endorsement
// source all refuse; a configured observer NodeID is admitted beside the
// anchor keys, and the self-attestation refusal holds.
func TestAuthorizeObserverRefusesWithoutAListedValidEndorsement(t *testing.T) {
	ctx := context.Background()
	pubA, privA := newTestAnchorKey(t)
	_, privUnlisted := newTestAnchorKey(t)
	id, key, rec := endorsedTestNode(t, privA)
	idU, keyU, recU := endorsedTestNode(t, privUnlisted)

	if err := NewPolicy(PolicyConfig{AnchorKeys: []ed25519.PublicKey{pubA}}).AuthorizeObserver(ctx, ports.Principal{NodeID: id, PubKey: key}, ""); err == nil {
		t.Fatal("a policy with no endorsement source admits an observer")
	}
	p := NewPolicy(PolicyConfig{AnchorKeys: []ed25519.PublicKey{pubA}, Observers: []ports.NodeID{idU}})
	p.SetAnchorEndorsements(endorsementMap{id: rec, idU: recU})
	if err := p.AuthorizeObserver(ctx, ports.Principal{NodeID: id, PubKey: key}, id); err == nil {
		t.Fatal("a node attesting about itself is admitted")
	}
	if err := p.AuthorizeObserver(ctx, ports.Principal{NodeID: idU, PubKey: keyU}, ""); err != nil {
		t.Fatalf("a configured observer NodeID: %v", err)
	}
	q := NewPolicy(PolicyConfig{AnchorKeys: []ed25519.PublicKey{pubA}})
	q.SetAnchorEndorsements(endorsementMap{id: rec, idU: recU})
	if err := q.AuthorizeObserver(ctx, ports.Principal{NodeID: idU, PubKey: keyU}, ""); err == nil {
		t.Fatal("a node endorsed by an unlisted anchor key is admitted")
	}
	q.RevokeKey(key)
	if err := q.AuthorizeObserver(ctx, ports.Principal{NodeID: id, PubKey: key}, ""); err == nil {
		t.Fatal("a revoked node key is admitted")
	}
}

// Clone copies AnchorKeys without sharing their storage.
func TestCloneCopiesAnchorKeys(t *testing.T) {
	pubA, _ := newTestAnchorKey(t)
	cfg := PolicyConfig{AnchorKeys: []ed25519.PublicKey{append(ed25519.PublicKey(nil), pubA...)}}
	c := cfg.Clone()
	if len(c.AnchorKeys) != 1 || !c.AnchorKeys[0].Equal(pubA) {
		t.Fatal("Clone dropped AnchorKeys")
	}
	c.AnchorKeys[0][0] ^= 0xff
	if !cfg.AnchorKeys[0].Equal(pubA) {
		t.Fatal("Clone shares AnchorKeys storage with the original")
	}
}

// A tag value that does not decode is no endorsement.
func TestParseAnchorEndorsementRefusesMalformedValues(t *testing.T) {
	for _, v := range []string{"", "nodot", "zz.zz", "00.00"} {
		if _, err := ParseAnchorEndorsement(v); err == nil {
			t.Errorf("ParseAnchorEndorsement(%q) accepted a malformed value", v)
		}
	}
}
