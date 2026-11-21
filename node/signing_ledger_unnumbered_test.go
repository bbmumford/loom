/*
 * Copyright (c) 2026 ORBTR Pty Ltd. All Rights Reserved.
 */

package node

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"

	lad "github.com/bbmumford/ledger"
)

// plainLedger is a lad.Ledger that cannot number a record before it is signed:
// it keeps what Append and BatchAppend are handed.
type plainLedger struct{ got []lad.Record }

func (p *plainLedger) Append(_ context.Context, rec lad.Record) error {
	p.got = append(p.got, rec)
	return nil
}

func (p *plainLedger) BatchAppend(_ context.Context, recs []lad.Record) error {
	p.got = append(p.got, recs...)
	return nil
}

func (p *plainLedger) Head(context.Context) (lad.CausalWatermark, error) {
	return lad.CausalWatermark{}, nil
}

func (p *plainLedger) Stream(context.Context, lad.CausalWatermark, []lad.Topic) (<-chan lad.Record, error) {
	ch := make(chan lad.Record)
	close(ch)
	return ch, nil
}

func (p *plainLedger) Snapshot(context.Context) (io.ReadCloser, error) {
	return io.NopCloser(strings.NewReader("")), nil
}

// A signed-topic record with no Seq is refused over a ledger that would number
// it after its signature was made, and reaches no ledger; one that already
// carries its Seq is signed and appended.
func TestARecordWithNoSeqIsNotSignedForALedgerThatNumbersItLater(t *testing.T) {
	inner := &plainLedger{}
	sl := newSigningLedger(inner, newNodeKey(t))
	ctx := context.Background()
	if err := sl.Append(ctx, lad.Record{Topic: lad.TopicMember, NodeID: "n1", Body: json.RawMessage(`{}`)}); !errors.Is(err, errCannotNumberBeforeSigning) {
		t.Fatalf("Append with no Seq: %v, want errCannotNumberBeforeSigning", err)
	}
	if err := sl.BatchAppend(ctx, []lad.Record{{Topic: lad.TopicMember, NodeID: "n1", Seq: 4, Body: json.RawMessage(`{}`)}, {Topic: lad.TopicRole, NodeID: "n1", Body: json.RawMessage(`{}`)}}); !errors.Is(err, errCannotNumberBeforeSigning) {
		t.Fatalf("BatchAppend with an unnumbered record: %v, want errCannotNumberBeforeSigning", err)
	}
	if len(inner.got) != 0 {
		t.Fatalf("%d refused records reached the ledger", len(inner.got))
	}
	if err := sl.Append(ctx, lad.Record{Topic: lad.TopicMember, NodeID: "n1", Seq: 9, Body: json.RawMessage(`{}`)}); err != nil {
		t.Fatalf("Append with its Seq: %v", err)
	}
	if err := sl.BatchAppend(ctx, []lad.Record{{Topic: lad.TopicRole, NodeID: "n1", Seq: 10, Body: json.RawMessage(`{}`)}}); err != nil {
		t.Fatalf("BatchAppend with its Seq: %v", err)
	}
	if len(inner.got) != 2 {
		t.Fatalf("the ledger received %d records, want 2", len(inner.got))
	}
	for _, rec := range inner.got {
		if !lad.VerifyRecord(rec) {
			t.Errorf("Seq %d does not verify", rec.Seq)
		}
	}
}
