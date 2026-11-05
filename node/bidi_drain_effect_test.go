/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"errors"
	"testing"

	"github.com/ORBTR/aether/rpc/pb"
)

// endedStream is a bidi stream whose peer has gone: every Receive fails.
type endedStream struct{ hopStream }

func (*endedStream) Receive(context.Context) ([]byte, error) {
	return nil, errors.New("read: connection reset")
}

// When the read loop exits, each call still waiting gets a failed response
// stamped ambiguous: its request was handed to the peer, which may have run
// it, so the origin's HWPCaller must not read it as a handler's rejection.
func TestADrainedBidiCallIsAnsweredAsAnAmbiguousEffect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan *pb.RPCResponse, 1)
	b := &BidiRPC{
		stream:    &endedStream{},
		pending:   map[string]chan *pb.RPCResponse{"wire-1": ch},
		done:      make(chan struct{}),
		ctx:       ctx,
		ctxCancel: cancel,
	}
	b.readLoop()

	resp := <-ch
	if resp.Success || resp.Metadata["refusal"] != "ambiguous" {
		t.Fatalf("the drained call got (success %v, metadata %v); want a failure stamped ambiguous",
			resp.Success, resp.Metadata)
	}
	if b.IsAlive() {
		t.Fatal("the bidi still reads as alive after its read loop exited")
	}
}
