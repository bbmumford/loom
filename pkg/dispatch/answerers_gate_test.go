/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package dispatch

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/rpc/pb"
)

// disagreeingFinder refuses every call through the AnswererChecker seam and
// records whether any path of the dispatcher reached it afterwards.
type disagreeingFinder struct {
	recordingFinder
	asked    []string
	nodeSess aether.Session
}

func (f *disagreeingFinder) CheckAnswerers(role, handler, nodeID string) error {
	f.asked = append(f.asked, nodeID)
	return &AnswerersDisagreeError{Handler: handler, Requirements: []string{"open: n2", "nexus: n1"}}
}

func (f *disagreeingFinder) MeshSessionForNode(string) (aether.Session, bool) {
	return f.nodeSess, f.nodeSess != nil
}

func wantRefusedUnsent(t *testing.T, err error, f *disagreeingFinder, nodeID string) {
	t.Helper()
	if !errors.Is(err, ErrAnswerersDisagree) {
		t.Fatalf("err = %v, want the typed refusal", err)
	}
	var typed *AnswerersDisagreeError
	if !errors.As(err, &typed) || len(typed.Requirements) != 2 {
		t.Fatalf("err = %v; want the groups kept on the typed refusal", err)
	}
	if !Unsent(err) {
		t.Fatalf("Unsent(%v) = false, want true: nothing was sent", err)
	}
	if f.findRoutes != 0 || f.findSessions != 0 || f.bidiCalls != 0 {
		t.Fatalf("routes %d, sessions %d, bidi %d: a refused call reached a path", f.findRoutes, f.findSessions, f.bidiCalls)
	}
	if len(f.asked) == 0 || f.asked[len(f.asked)-1] != nodeID {
		t.Fatalf("checker asked with %v, want last %q", f.asked, nodeID)
	}
}

// The check runs before any path for a routed call.
func TestARoutedCallToDisagreeingAdvertsIsRefusedBeforeAnyPath(t *testing.T) {
	f := &disagreeingFinder{}
	c := NewHWPCaller(f)
	defer c.Close()
	_, err := c.Call(context.Background(), "svc", "svc.Op", []byte("x"))
	wantRefusedUnsent(t, err, f, "")
}

// A cached answer came from a holder of the operation, so it is not served while
// the holders disagree, on either of the two paths that read the cache.
func TestACachedAnswerIsNotServedWhileAdvertsDisagree(t *testing.T) {
	f := &disagreeingFinder{}
	c := NewHWPCaller(f)
	defer c.Close()
	c.SetCacheableHandlers(map[string]time.Duration{"svc/svc.Op": time.Minute})
	c.StoreCachedResult("svc/svc.Op", []byte("x"), []byte("cached"), time.Minute)

	if out, err := c.Call(context.Background(), "svc", "svc.Op", []byte("x")); !errors.Is(err, ErrAnswerersDisagree) {
		t.Fatalf("Call = (%q, %v), want the refusal", out, err)
	}
	if out, err := c.CallCacheable(context.Background(), "svc", "svc.Op", []byte("x"), time.Minute); !errors.Is(err, ErrAnswerersDisagree) {
		t.Fatalf("CallCacheable = (%q, %v), want the refusal", out, err)
	}
}

// A targeted call is held to the same check and names its node to it.
func TestATargetedCallToDisagreeingAdvertsIsRefusedBeforeSending(t *testing.T) {
	f := &disagreeingFinder{}
	f.bidiOK = true
	f.bidiResp = &pb.RPCResponse{Success: true}
	c := NewHWPCaller(f)
	defer c.Close()
	_, err := c.CallNode(context.Background(), "n1", "svc", "svc.Op", []byte("x"))
	wantRefusedUnsent(t, err, f, "n1")
}

// A handler stream is judged as a single-shot call to one node is.
func TestAHandlerStreamToDisagreeingAdvertsIsRefusedBeforeOpening(t *testing.T) {
	sess := &rpcMockSession{resp: &pb.RPCResponse{Success: true}}
	f := &disagreeingFinder{nodeSess: sess}
	c := NewHWPCaller(f)
	defer c.Close()
	_, err := c.OpenNodeStream(context.Background(), "n1", "svc", "svc.Op")
	wantRefusedUnsent(t, err, f, "n1")
}

// A finder that does not judge adverts refuses nothing here, so dispatch keeps
// working on a process with no swarm.
func TestAFinderThatDoesNotJudgeAdvertsRefusesNothing(t *testing.T) {
	c := NewHWPCaller(&recordingFinder{})
	defer c.Close()
	if err := c.checkAnswerers("svc", "svc.Op", ""); err != nil {
		t.Fatalf("checkAnswerers = %v, want nil", err)
	}
}

// A relay's refusal reaches the origin typed, so the origin neither reads it as a
// handler rejection nor sends the call on another route as if the relay had failed.
func TestARefusalFromALaterHopKeepsItsType(t *testing.T) {
	refused := &pb.RPCResponse{Error: "the hop's sentence"}
	StampRefusal(refused, &AnswerersDisagreeError{Handler: "svc.Op", Requirements: []string{"open: n2", "nexus: n1"}})

	err := responseError("svc.Op", refused)
	if !errors.Is(err, ErrAnswerersDisagree) || errors.Is(err, ErrHandlerError) {
		t.Fatalf("responseError = %v, want the typed refusal and no handler error", err)
	}
	var typed *AnswerersDisagreeError
	if !errors.As(err, &typed) || len(typed.Requirements) != 2 {
		t.Fatalf("responseError = %v; want the groups rebuilt", err)
	}
	if err.Error() != "the hop's sentence" {
		t.Fatalf("Error() = %q, want the refusing hop's own text", err.Error())
	}
}
