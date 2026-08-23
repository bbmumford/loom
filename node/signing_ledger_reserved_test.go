/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"io"
	"strings"
	"testing"

	lad "github.com/bbmumford/ledger"
)

// recordingLedger captures what actually reaches the inner ledger.
type recordingLedger struct{ got []lad.Record }

func (r *recordingLedger) Append(_ context.Context, rec lad.Record) error {
	r.got = append(r.got, rec)
	return nil
}

func (r *recordingLedger) BatchAppend(_ context.Context, recs []lad.Record) error {
	r.got = append(r.got, recs...)
	return nil
}

func (r *recordingLedger) Head(context.Context) (lad.CausalWatermark, error) {
	return lad.CausalWatermark{}, nil
}

func (r *recordingLedger) Stream(context.Context, lad.CausalWatermark, []lad.Topic) (<-chan lad.Record, error) {
	ch := make(chan lad.Record)
	close(ch)
	return ch, nil
}

func (r *recordingLedger) Snapshot(context.Context) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("")), nil
}

func newTestSigningLedger(t *testing.T) (*signingLedger, *recordingLedger) {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	inner := &recordingLedger{}
	return newSigningLedger(inner, priv), inner
}

// The reserved topics must be REFUSED at the publish chokepoint.
//
// `keyops` and `quorum` are registered on the cache with merge funcs and a
// signed-topic ACL, and `signingLedger` will happily sign them — but the
// receiving cache's applyCore ends in `default: // ignore other topics for
// directory view`, so a record on either topic is accepted with no error and
// then retained by nothing. That is accept-and-discard.
//
// It is harmless while nobody publishes (measured: zero producers across 2,741
// .go files) and it fails OPEN the moment anyone does — a key REVOCATION would
// be signed, ACL-accepted, gossiped, and silently dropped by every receiver.
// The publish path refusing is the available gate: ladcache is a published
// dependency we cannot change, this wrapper is ours.
func TestReservedTopicsAreRefusedAtThePublishChokepoint(t *testing.T) {
	sl, inner := newTestSigningLedger(t)
	ctx := context.Background()

	for _, topic := range []lad.Topic{lad.TopicKeyOps, lad.TopicQuorum} {
		err := sl.Append(ctx, lad.Record{Topic: topic, NodeID: "n1", Body: []byte(`{}`)})
		if err == nil {
			t.Fatalf("Append to reserved topic %q succeeded — the record would be "+
				"signed and gossiped, and every receiver would discard it", topic)
		}
		if !errors.Is(err, ErrTopicReserved) {
			t.Fatalf("Append(%q) error = %v, want ErrTopicReserved", topic, err)
		}

		err = sl.BatchAppend(ctx, []lad.Record{{Topic: topic, NodeID: "n1", Body: []byte(`{}`)}})
		if err == nil {
			t.Fatalf("BatchAppend to reserved topic %q succeeded", topic)
		}
		if !errors.Is(err, ErrTopicReserved) {
			t.Fatalf("BatchAppend(%q) error = %v, want ErrTopicReserved", topic, err)
		}
	}

	if len(inner.got) != 0 {
		t.Fatalf("%d reserved-topic record(s) reached the inner ledger despite the "+
			"refusal — the gate reports an error but still publishes", len(inner.got))
	}
}

// The gate must be narrow. Every topic the cache actually retains has to keep
// publishing exactly as before — a refusal that caught member/role/reach would
// take the mesh down, and this test is what separates "reserved" from "broken".
func TestRetainedTopicsStillPublishAndAreSigned(t *testing.T) {
	sl, inner := newTestSigningLedger(t)
	ctx := context.Background()

	retained := []lad.Topic{lad.TopicMember, lad.TopicRole, lad.TopicReach, lad.TopicLatency}
	for _, topic := range retained {
		if err := sl.Append(ctx, lad.Record{Topic: topic, NodeID: "n1", Body: []byte(`{}`)}); err != nil {
			t.Fatalf("Append to retained topic %q was refused: %v", topic, err)
		}
	}
	if len(inner.got) != len(retained) {
		t.Fatalf("inner ledger received %d records, want %d", len(inner.got), len(retained))
	}
	// The signing behaviour this wrapper exists for must survive the new gate —
	// but only for the topics it actually covers. `latency` is deliberately NOT
	// a signed topic (isSignedTopic lists member/role/reach/keyops/quorum), so
	// asserting a signature on it would pin a behaviour the code does not have
	// and never claimed to.
	signed := 0
	for _, rec := range inner.got {
		if !isSignedTopic(rec.Topic) {
			if len(rec.Signature) != 0 {
				t.Fatalf("record on unsigned topic %q acquired a signature", rec.Topic)
			}
			continue
		}
		signed++
		if len(rec.Signature) == 0 || len(rec.AuthorPubKey) == 0 {
			t.Fatalf("record on %q reached the ledger unsigned", rec.Topic)
		}
		if !lad.VerifyRecord(rec) {
			t.Fatalf("record on %q has a signature that does not verify", rec.Topic)
		}
	}
	if signed == 0 {
		t.Fatal("no signed-topic record was checked — the signing assertion is vacuous")
	}
}
