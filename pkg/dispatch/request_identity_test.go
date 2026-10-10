/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package dispatch

import (
	"context"
	"sync"
	"testing"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/rpc/pb"
)

// timedOutBidiFinder has a bidi channel whose call hits its time cap while
// the caller's context is still live, which sends HWPCaller on to its
// dynamic-stream attempt. It records the id of each request it was handed.
type timedOutBidiFinder struct {
	mockFinder
	noRoutes bool

	mu  sync.Mutex
	ids []string
}

func (f *timedOutBidiFinder) FindRoutes(ctx context.Context, role, handler string) []aether.ProbeRoute {
	if f.noRoutes {
		return nil
	}
	return f.mockFinder.FindRoutes(ctx, role, handler)
}

func (f *timedOutBidiFinder) CallViaBidi(_ context.Context, _ string, req *pb.RPCRequest) (*pb.RPCResponse, bool, error) {
	f.mu.Lock()
	f.ids = append(f.ids, req.Id)
	f.mu.Unlock()
	return nil, true, context.DeadlineExceeded
}

// sentSession answers every request with a success and keeps, in the order
// they were sent, the ids of the requests its streams carried.
type sentSession struct {
	rpcMockSession

	mu  sync.Mutex
	ids []string
}

func (s *sentSession) OpenStream(_ context.Context, cfg aether.StreamConfig) (aether.Stream, error) {
	respData, err := pb.MarshalResponse(&pb.RPCResponse{Success: true})
	if err != nil {
		return nil, err
	}
	return &sentStream{rpcMockStream: rpcMockStream{id: cfg.StreamID, respData: respData}, sess: s}, nil
}

type sentStream struct {
	rpcMockStream
	sess *sentSession
}

func (st *sentStream) Send(_ context.Context, data []byte) error {
	req, err := pb.UnmarshalRequest(data)
	if err != nil {
		return err
	}
	st.sess.mu.Lock()
	st.sess.ids = append(st.sess.ids, req.Id)
	st.sess.mu.Unlock()
	return nil
}

// One call keeps one request id across its attempts: the bidi attempt that
// timed out and the dynamic-stream attempt that follows it carry the same id,
// on the probed-route path and on callOnce. The receiving server keys its
// dedup cache on the id, so two ids make one call two requests to it. A
// second call gets an id of its own.
func TestACallKeepsOneRequestIDAcrossItsAttempts(t *testing.T) {
	for _, tc := range []struct {
		name     string
		noRoutes bool
	}{
		{"probed routes", false},
		{"no routes", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sess := &sentSession{}
			defer forgetSession(sess)
			finder := &timedOutBidiFinder{mockFinder: mockFinder{session: sess}, noRoutes: tc.noRoutes}
			caller := NewHWPCaller(finder)
			defer caller.Close()

			for call := 1; call <= 2; call++ {
				if _, err := caller.Call(context.Background(), "svc", "svc.Op", nil); err != nil {
					t.Fatalf("call %d failed: %v; the dynamic stream answers", call, err)
				}
			}
			bidi, stream := finder.ids, sess.ids
			if len(bidi) != 2 || len(stream) != 2 {
				t.Fatalf("premise wrong: %d bidi attempt(s) and %d stream attempt(s) for 2 calls",
					len(bidi), len(stream))
			}
			for call := 0; call < 2; call++ {
				if bidi[call] == "" || bidi[call] != stream[call] {
					t.Errorf("call %d sent id %q over bidi and %q over its dynamic stream; one call "+
						"keeps one id", call+1, bidi[call], stream[call])
				}
			}
			if bidi[0] == bidi[1] {
				t.Errorf("two calls share request id %q", bidi[0])
			}
		})
	}
}

// The parallel probes of one call carry one request id, each over its own
// route, so the node they converge on can tell they are one request.
func TestTheProbesOfOneCallShareItsRequestID(t *testing.T) {
	first, second := &sentSession{}, &sentSession{}
	defer forgetSession(first)
	defer forgetSession(second)
	caller := NewHWPCaller(&multiRouteFinder{routes: []aether.ProbeRoute{
		{Session: first, NodeID: "node-a", TargetNodeID: "target"},
		{Session: second, NodeID: "node-b", TargetNodeID: "target"},
	}})
	defer caller.Close()

	if _, err := caller.Call(context.Background(), "svc", "svc.Op", nil); err != nil {
		t.Fatalf("Call failed: %v", err)
	}
	sent := func(s *sentSession) []string {
		s.mu.Lock()
		defer s.mu.Unlock()
		return append([]string(nil), s.ids...)
	}
	// Call returns on the first answer; the other probe still sends.
	eventually(t, "a probe never sent its request", func() bool {
		return len(sent(first)) == 1 && len(sent(second)) == 1
	})
	if a, b := sent(first)[0], sent(second)[0]; a == "" || a != b {
		t.Fatalf("the two probes of one call sent ids %q and %q", a, b)
	}
}
