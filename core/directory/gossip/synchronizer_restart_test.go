/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package gossip

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"sync"
	"testing"
	"time"

	lad "github.com/bbmumford/ledger"
	ladcache "github.com/bbmumford/ledger/cache"
	"github.com/bbmumford/ledger/storage"
)

// scriptedStream is what one Stream call of scriptedLedger delivers, and the
// reason it reports at the end.
type scriptedStream struct {
	recs []lad.Record
	why  error
}

// scriptedLedger answers its n-th StreamWithEnd with streams[n] (the last one
// for every later call), recording the watermark each call asked from.
type scriptedLedger struct {
	mu      sync.Mutex
	streams []scriptedStream
	froms   []lad.CausalWatermark
}

func (l *scriptedLedger) StreamWithEnd(ctx context.Context, from lad.CausalWatermark, topics []lad.Topic) (<-chan lad.Record, *lad.StreamEnd, error) {
	l.mu.Lock()
	i := len(l.froms)
	l.froms = append(l.froms, from)
	l.mu.Unlock()
	if i >= len(l.streams) {
		i = len(l.streams) - 1
	}
	s := l.streams[i]
	out := make(chan lad.Record, len(s.recs))
	for _, rec := range s.recs {
		out <- rec
	}
	end := &lad.StreamEnd{}
	end.Set(s.why)
	close(out)
	return out, end, nil
}

func (l *scriptedLedger) Stream(ctx context.Context, from lad.CausalWatermark, topics []lad.Topic) (<-chan lad.Record, error) {
	out, _, err := l.StreamWithEnd(ctx, from, topics)
	return out, err
}
func (l *scriptedLedger) Head(context.Context) (lad.CausalWatermark, error) { return nil, nil }
func (l *scriptedLedger) Append(context.Context, lad.Record) error          { return nil }
func (l *scriptedLedger) BatchAppend(context.Context, []lad.Record) error   { return nil }
func (l *scriptedLedger) Snapshot(context.Context) (io.ReadCloser, error)   { return nil, nil }

func (l *scriptedLedger) calls() []lad.CausalWatermark {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]lad.CausalWatermark(nil), l.froms...)
}

// plainLedger hides StreamWithEnd, standing for a ledger that does not
// report why its stream ended.
type plainLedger struct{ lad.Ledger }

func syncMember(t *testing.T, nodeID string, seq uint64) lad.Record {
	t.Helper()
	at := time.Now()
	body, err := json.Marshal(lad.MemberRecord{Partition: "p", NodeID: nodeID, PubKey: []byte("k-" + nodeID), CreatedAt: at})
	if err != nil {
		t.Fatal(err)
	}
	return lad.Record{Topic: lad.TopicMember, Partition: "p", NodeID: nodeID, Seq: seq, Timestamp: at, Body: body}
}

func cacheHolds(t *testing.T, c *ladcache.DirectoryCache, nodeID string) bool {
	t.Helper()
	members, err := c.Members(context.Background(), "p")
	if err != nil {
		t.Fatal(err)
	}
	for _, m := range members {
		if m.NodeID == nodeID {
			return true
		}
	}
	return false
}

// A stream that ends by overflowing its queue is streamed again from the
// same watermark, the replay's repeat of an applied record is harmless, and
// the ledger's close ends Sync with nil.
func TestSyncStreamsAgainFromItsWatermarkAfterAnOverflow(t *testing.T) {
	m1, m2 := syncMember(t, "n1", 1), syncMember(t, "n2", 2)
	l := &scriptedLedger{streams: []scriptedStream{
		{recs: []lad.Record{m1}, why: lad.ErrStreamOverflow},
		{recs: []lad.Record{m1, m2}, why: lad.ErrLedgerClosed},
	}}
	c := ladcache.NewDirectoryCache()
	changes := 0
	c.OnMembershipChange = func() { changes++ }
	from := lad.CausalWatermark{lad.TopicMember: 0}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := NewSynchronizer(l, c).Sync(ctx, from, nil); err != nil {
		t.Fatalf("Sync = %v, want nil at the ledger's close", err)
	}

	calls := l.calls()
	if len(calls) != 2 {
		t.Fatalf("Sync streamed %d times, want 2 (once, then again after the overflow)", len(calls))
	}
	for i, got := range calls {
		if !reflect.DeepEqual(got, from) {
			t.Fatalf("stream %d asked from %v, want the watermark Sync was given, %v", i, got, from)
		}
	}
	if !cacheHolds(t, c, "n1") || !cacheHolds(t, c, "n2") {
		t.Fatal("the cache misses a record the restarted stream delivered")
	}
	if changes != 2 {
		t.Fatalf("membership changes = %d, want 2: the replayed n1 is not a second change", changes)
	}
}

// A ledger that does not say why its stream ended is streamed again too,
// until the caller's context ends Sync with its error.
func TestSyncStreamsAgainWhenTheLedgerReportsNoReason(t *testing.T) {
	l := &scriptedLedger{streams: []scriptedStream{{recs: nil, why: nil}}}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- NewSynchronizer(plainLedger{l}, ladcache.NewDirectoryCache()).Sync(ctx, nil, nil) }()

	deadline := time.Now().Add(5 * time.Second)
	for len(l.calls()) < 2 {
		if time.Now().After(deadline) {
			cancel()
			t.Fatalf("Sync streamed %d times in 5 s, want a restart", len(l.calls()))
		}
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Sync = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Sync did not return after its context ended")
	}
}

// Against the ledger's own MemoryLedger: records reach the cache, and the
// ledger's close ends Sync with nil rather than a restart loop.
func TestSyncEndsWhenAMemoryLedgerCloses(t *testing.T) {
	l := storage.NewMemoryLedger(storage.MemoryLedgerConfig{})
	c := ladcache.NewDirectoryCache()
	done := make(chan error, 1)
	go func() { done <- NewSynchronizer(l, c).Sync(context.Background(), nil, nil) }()

	if err := l.Append(context.Background(), syncMember(t, "n1", 1)); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for !cacheHolds(t, c, "n1") {
		if time.Now().After(deadline) {
			t.Fatal("the appended record never reached the cache")
		}
		time.Sleep(5 * time.Millisecond)
	}
	_ = l.Close()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Sync = %v after the ledger closed, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Sync kept running after the ledger closed")
	}
}
