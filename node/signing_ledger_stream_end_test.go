/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"errors"
	"testing"
	"time"

	lad "github.com/bbmumford/ledger"
	"github.com/bbmumford/ledger/storage"
)

// gossip.Synchronizer.Sync is handed the signing wrapper, so the wrapper
// must report the inner ledger's stream ends, or Sync could not tell an
// overflow it must restart from the ledger's close.
func TestSigningLedgerReportsTheInnerLedgersStreamEnd(t *testing.T) {
	inner := storage.NewMemoryLedger(storage.MemoryLedgerConfig{})
	var l lad.Ledger = newSigningLedger(inner, nil)
	er, ok := l.(lad.EndReportingLedger)
	if !ok {
		t.Fatal("signingLedger does not report why its streams end")
	}
	out, end, err := er.StreamWithEnd(context.Background(), nil, nil)
	if err != nil {
		t.Fatalf("StreamWithEnd: %v", err)
	}
	_ = inner.Close()
	timeout := time.After(5 * time.Second)
	for open := true; open; {
		select {
		case _, open = <-out:
		case <-timeout:
			t.Fatal("the stream stayed open after the inner ledger closed")
		}
	}
	if why := end.Err(); !errors.Is(why, lad.ErrLedgerClosed) {
		t.Fatalf("the stream ended with %v, want ErrLedgerClosed", why)
	}
}
