/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/rpc/pb"
	"github.com/bbmumford/loom/node/handlers"
	"github.com/bbmumford/loom/ports"
)

// servePrincipalOverBidi delivers typedPrincipalRequest to a bidi built for a
// session bound to partition ("" builds one through NewBidiRPC, as a session
// naming none is), the way a peer's request frame reaches it, and returns the
// response the bidi sent back with the probe the request was addressed to.
func servePrincipalOverBidi(t *testing.T, partition string, v ports.AuthValidator) (*pb.RPCResponse, *typedPrincipalProbe) {
	t.Helper()
	registry := handlers.NewHandlerRegistry()
	registry.SetPresencePolicy(typedPrincipalScopePolicy())
	probe := &typedPrincipalProbe{}
	if err := registry.RegisterRPC(probe); err != nil {
		t.Fatal(err)
	}
	server := NewRPCServer(registry)
	server.SetAuthValidator(v)

	var mu sync.Mutex
	var sent [][]byte
	stream := &bidiStream{block: make(chan struct{}), onSend: func(msg []byte) error {
		mu.Lock()
		sent = append(sent, append([]byte(nil), msg...))
		mu.Unlock()
		return nil
	}}
	t.Cleanup(func() { close(stream.block) })
	var b *BidiRPC
	if partition == "" {
		b = NewBidiRPC(stream, aether.NodeID(testNodeIDB), server, "noise-udp", "same-origin")
	} else {
		b = NewPartitionedBidiRPC(stream, aether.NodeID(testNodeIDB), server, "noise-udp", "same-origin", partition)
	}

	payload, err := pb.MarshalRequest(typedPrincipalRequest())
	if err != nil {
		t.Fatal(err)
	}
	b.handleIncomingRequest(payload)

	mu.Lock()
	defer mu.Unlock()
	if len(sent) != 1 || len(sent[0]) < 2 || sent[0][0] != msgTypeResponse {
		t.Fatalf("the bidi sent %d frame(s), want one response frame", len(sent))
	}
	resp, err := pb.UnmarshalResponse(sent[0][1:])
	if err != nil {
		t.Fatalf("the response frame does not decode: %v", err)
	}
	return resp, probe
}

// A principal a peer sends over a session's bidi is judged against the
// transport partition that session's handshake bound, as one sent over an
// accepted RPC session is (TestRPCServerInstallsAMeshPrincipalOnlyThroughTheAdmitter).
//
// The first leg is the claim: on a bidi bound to the partition the request's
// pinned axis names, the admitter admits and the handler runs under the
// principal the peer sent. The second is the shape every bidi had before it
// carried a partition, and is still the shape for a session naming none: the
// principal is refused by the partition-bound-transport check before any
// admitter is asked. The third shows the partition the admitter judged is the
// bidi's own: the same request over a bidi bound to another partition is
// refused by the admitter's rule, not by the missing-partition check.
func TestABidiServesAPrincipalUnderItsSessionsTransportPartition(t *testing.T) {
	t.Run("a bidi bound to the request's partition admits it", func(t *testing.T) {
		resp, probe := servePrincipalOverBidi(t, "orbtr", typedPrincipalValidator{})
		if !resp.Success {
			t.Fatalf("a principal over a bidi bound to its partition was refused: %s", resp.Error)
		}
		if calls := atomic.LoadInt32(&probe.calls); calls != 1 {
			t.Fatalf("handler calls = %d, want 1", calls)
		}
		if got := probe.got.Identity().Partition(); got != "org-A" {
			t.Fatalf("the handler ran under partition %q, want the peer's org-A", got)
		}
	})

	t.Run("a bidi naming no partition refuses a principal", func(t *testing.T) {
		resp, probe := servePrincipalOverBidi(t, "", typedPrincipalValidator{})
		if resp.Success {
			t.Fatal("a principal over a bidi naming no transport partition reached dispatch")
		}
		if !strings.Contains(resp.Error, "requires a partition-bound transport") {
			t.Fatalf("refused by the wrong check: want the partition-bound-transport refusal, got %q", resp.Error)
		}
		if calls := atomic.LoadInt32(&probe.calls); calls != 0 {
			t.Fatalf("handler ran %d time(s); the refusal must precede dispatch", calls)
		}
	})

	t.Run("the admitter judges the bidi's own partition", func(t *testing.T) {
		resp, probe := servePrincipalOverBidi(t, "elsewhere", typedPrincipalValidator{})
		if resp.Success {
			t.Fatal("a principal pinned to orbtr was admitted over a bidi bound to another partition")
		}
		if !strings.Contains(resp.Error, "does not match the transport") {
			t.Fatalf("refused by the wrong check: want the admitter's transport rule, got %q", resp.Error)
		}
		if calls := atomic.LoadInt32(&probe.calls); calls != 0 {
			t.Fatalf("handler ran %d time(s); the refusal must precede dispatch", calls)
		}
	})
}
