/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package dispatch

import (
	"context"
	"errors"
	"testing"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/rpc/pb"
)

// recordingFinder wraps mockFinder to observe WHICH dispatch paths were taken.
//
// The counters are the point of this file. Asserting that CallNode returns
// ErrNoRouteToNode is not enough on its own — a targeted call that errored AND
// ALSO reached some other node would satisfy that assertion while shipping the
// exact defect the method exists to prevent. So these tests assert the negative:
// the role-resolution seams (FindSession / FindRoutes) are never touched.
type recordingFinder struct {
	mockFinder

	bidiCalls    int
	bidiNodeIDs  []string
	bidiResp     *pb.RPCResponse
	bidiOK       bool
	findSessions int
	findRoutes   int
}

func (f *recordingFinder) CallViaBidi(ctx context.Context, nodeID string, req *pb.RPCRequest) (*pb.RPCResponse, bool, error) {
	f.bidiCalls++
	f.bidiNodeIDs = append(f.bidiNodeIDs, nodeID)
	return f.bidiResp, f.bidiOK, nil
}

func (f *recordingFinder) FindSession(ctx context.Context, role, handler string) (aether.Session, error) {
	f.findSessions++
	return f.mockFinder.FindSession(ctx, role, handler)
}

func (f *recordingFinder) FindRoutes(ctx context.Context, role, handler string) []aether.ProbeRoute {
	f.findRoutes++
	return f.mockFinder.FindRoutes(ctx, role, handler)
}

// TestCallNode_UnreachableTargetFailsClosed is the acceptance check for the
// targeted-dispatch contract: when the named node has no usable bidi channel,
// CallViaBidi reports (nil, false, nil) — the same signal the role-dispatch
// probe loop reads as "take the untargeted arm" — and CallNode MUST convert it
// into an error rather than reaching anyone else.
func TestCallNode_UnreachableTargetFailsClosed(t *testing.T) {
	// A session IS available for role dispatch. If CallNode ever fell back,
	// it would succeed here — which is precisely what must not happen.
	sess := &rpcMockSession{resp: &pb.RPCResponse{Success: true, Payload: []byte("WRONG-NODE")}}
	finder := &recordingFinder{
		mockFinder: mockFinder{session: sess},
		bidiOK:     false, // no bidi to the target
	}
	caller := NewHWPCaller(finder)
	defer caller.Close()

	resp, err := caller.CallNode(context.Background(), "node-B", "platform", "platform.CheckHealth", nil)

	if err == nil {
		t.Fatalf("expected an error for an unreachable target, got payload %q", resp)
	}
	if !errors.Is(err, ErrNoRouteToNode) {
		t.Fatalf("expected ErrNoRouteToNode, got %v", err)
	}
	if resp != nil {
		t.Fatalf("expected no payload on a failed targeted call, got %q", resp)
	}

	// THE NEGATIVE — no fallback to role resolution.
	if finder.findSessions != 0 {
		t.Fatalf("targeted call fell back to FindSession (%d calls) — that is role dispatch, not targeting", finder.findSessions)
	}
	if finder.findRoutes != 0 {
		t.Fatalf("targeted call fell back to FindRoutes (%d calls) — that is role dispatch, not targeting", finder.findRoutes)
	}

	// And the target actually reached the transport, unaltered.
	if finder.bidiCalls != 1 {
		t.Fatalf("expected exactly 1 CallViaBidi attempt, got %d", finder.bidiCalls)
	}
	if len(finder.bidiNodeIDs) != 1 || finder.bidiNodeIDs[0] != "node-B" {
		t.Fatalf("expected the target nodeID to be passed through, got %v", finder.bidiNodeIDs)
	}
}

// TestCallNode_ReachableTargetSucceeds is the POSITIVE CONTROL. Without it the
// test above proves nothing: a CallNode that always errored — or that was never
// reached at all — would pass it just as happily.
func TestCallNode_ReachableTargetSucceeds(t *testing.T) {
	finder := &recordingFinder{
		bidiResp: &pb.RPCResponse{Success: true, Payload: []byte("pong")},
		bidiOK:   true,
	}
	caller := NewHWPCaller(finder)
	defer caller.Close()

	resp, err := caller.CallNode(context.Background(), "node-A", "platform", "platform.CheckHealth", nil)
	if err != nil {
		t.Fatalf("CallNode to a reachable target: %v", err)
	}
	if string(resp) != "pong" {
		t.Fatalf("expected pong, got %q", resp)
	}
	if finder.findSessions != 0 || finder.findRoutes != 0 {
		t.Fatalf("targeted success path must not consult role resolution (session=%d routes=%d)",
			finder.findSessions, finder.findRoutes)
	}
}

// TestCallNode_EmptyNodeIDFailsClosed — an absent target must not degrade into
// "any node will do".
func TestCallNode_EmptyNodeIDFailsClosed(t *testing.T) {
	sess := &rpcMockSession{resp: &pb.RPCResponse{Success: true, Payload: []byte("WRONG-NODE")}}
	finder := &recordingFinder{mockFinder: mockFinder{session: sess}}
	caller := NewHWPCaller(finder)
	defer caller.Close()

	_, err := caller.CallNode(context.Background(), "", "platform", "platform.CheckHealth", nil)
	if !errors.Is(err, ErrNoRouteToNode) {
		t.Fatalf("expected ErrNoRouteToNode for an empty nodeID, got %v", err)
	}
	if finder.bidiCalls != 0 || finder.findSessions != 0 || finder.findRoutes != 0 {
		t.Fatalf("empty nodeID must not dispatch anywhere (bidi=%d session=%d routes=%d)",
			finder.bidiCalls, finder.findSessions, finder.findRoutes)
	}
}

// TestHWPCaller_ImplementsTargetedCaller pins the optional-interface wiring.
// If HWPCaller ever stops satisfying TargetedCaller, pkg/rpc's type assertion
// silently starts returning ErrNoRouteToNode for every targeted call — a
// failure that is invisible at compile time and looks like a transport outage.
func TestHWPCaller_ImplementsTargetedCaller(t *testing.T) {
	var c Caller = NewHWPCaller(&recordingFinder{})
	if _, ok := c.(TargetedCaller); !ok {
		t.Fatal("HWPCaller must implement TargetedCaller")
	}
}
