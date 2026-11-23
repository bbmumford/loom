/*
 * Copyright (c) 2026 Brandon Miller-Mumford. Licensed under the MIT License; see LICENSE.
 */

package node

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"sync"
	"testing"
	"time"

	lad "github.com/bbmumford/ledger"
	"github.com/bbmumford/ledger/storage"
)

// ladLedgers are the two ledgers the runtime wraps in a signingLedger: the LAD
// file a host keeps (encrypted tursogo through openLADFile) and the memory
// ledger an edge keeps.
func ladLedgers(t *testing.T) map[string]lad.Ledger {
	t.Helper()
	db, err := openLADFile(context.Background(), t.TempDir(), testLADKey(t))
	if err != nil {
		t.Fatalf("openLADFile: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	file, err := storage.NewLocalDBLedger(db, storage.LedgerConfig{})
	if err != nil {
		t.Fatalf("NewLocalDBLedger: %v", err)
	}
	t.Cleanup(func() { _ = file.Close() })
	mem := storage.NewMemoryLedger(storage.MemoryLedgerConfig{})
	t.Cleanup(func() { _ = mem.Close() })
	return map[string]lad.Ledger{"local": file, "memory": mem}
}

// streamed reads n records from l's history.
func streamed(t *testing.T, l lad.Ledger, n int) []lad.Record {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ch, err := l.Stream(ctx, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	var got []lad.Record
	timeout := time.After(10 * time.Second)
	for len(got) < n {
		select {
		case rec := <-ch:
			got = append(got, rec)
		case <-timeout:
			t.Fatalf("the ledger streamed %d of %d records", len(got), n)
		}
	}
	return got
}

func newNodeKey(t *testing.T) ed25519.PrivateKey {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return priv
}

// A record the runtime publishes with no Seq is signed after the ledger numbers
// it, so the record the ledger stores and streams to the directory cache
// verifies against the number it carries. Appends and batches both.
func TestASignedTopicRecordVerifiesWithTheSeqTheLedgerGaveIt(t *testing.T) {
	for name, inner := range ladLedgers(t) {
		t.Run(name, func(t *testing.T) {
			sl := newSigningLedger(inner, newNodeKey(t))
			ctx := context.Background()
			body := json.RawMessage(`{"nodeId":"n1"}`)
			for _, topic := range []lad.Topic{lad.TopicMember, lad.TopicRole, lad.TopicReach} {
				if err := sl.Append(ctx, lad.Record{Topic: topic, NodeID: "n1", Body: body}); err != nil {
					t.Fatalf("Append %s: %v", topic, err)
				}
			}
			if err := sl.BatchAppend(ctx, []lad.Record{
				{Topic: lad.TopicMember, NodeID: "n2", Body: body},
				{Topic: lad.TopicLatency, NodeID: "n2", Body: body},
				{Topic: lad.TopicMember, NodeID: "n3", Body: body},
			}); err != nil {
				t.Fatalf("BatchAppend: %v", err)
			}
			for _, rec := range streamed(t, inner, 6) {
				if rec.Seq == 0 {
					t.Errorf("%s record for %s was stored with no Seq", rec.Topic, rec.NodeID)
				}
				if !isSignedTopic(rec.Topic) {
					if len(rec.Signature) != 0 {
						t.Errorf("record on unsigned topic %s acquired a signature", rec.Topic)
					}
					continue
				}
				if !lad.VerifyRecord(rec) {
					t.Errorf("%s record for %s, Seq %d, does not verify as stored", rec.Topic, rec.NodeID, rec.Seq)
				}
			}
		})
	}
}

// Appends from several goroutines at once each get their own Seq on a topic,
// and every one is stored and verifies.
func TestConcurrentSignedAppendsEachGetTheirOwnSeq(t *testing.T) {
	for name, inner := range ladLedgers(t) {
		t.Run(name, func(t *testing.T) {
			sl := newSigningLedger(inner, newNodeKey(t))
			const n = 24
			errs := make(chan error, n)
			var wg sync.WaitGroup
			for i := 0; i < n; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					errs <- sl.Append(context.Background(), lad.Record{Topic: lad.TopicRole, NodeID: "n1", Body: json.RawMessage(`{}`)})
				}()
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				if err != nil {
					t.Errorf("concurrent Append: %v", err)
				}
			}
			seen := map[uint64]bool{}
			for _, rec := range streamed(t, inner, n) {
				if seen[rec.Seq] {
					t.Errorf("Seq %d was given twice", rec.Seq)
				}
				seen[rec.Seq] = true
				if !lad.VerifyRecord(rec) {
					t.Errorf("Seq %d does not verify", rec.Seq)
				}
			}
		})
	}
}
