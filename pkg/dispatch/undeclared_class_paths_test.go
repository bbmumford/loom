/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package dispatch

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/rpc/pb"
)

// countingLocal is a LocalDispatcher that hosts every handler and counts the
// dispatches it ran.
type countingLocal struct {
	ran atomic.Int32
}

func (l *countingLocal) HasHandler(string) bool { return true }

func (l *countingLocal) DispatchBytes(context.Context, string, []byte) ([]byte, error) {
	l.ran.Add(1)
	return []byte("ran"), nil
}

// requireUndeclared fails the test unless err is the typed refusal of the
// class "turbo" as one the gate does not declare.
func requireUndeclared(t *testing.T, err error) {
	t.Helper()
	if !errors.Is(err, ErrGradeTooLow) {
		t.Fatalf("returned %v for an undeclared class; it must be refused with ErrGradeTooLow", err)
	}
	var gte *GradeTooLowError
	if !errors.As(err, &gte) || !gte.UnknownClass || gte.Class != "turbo" {
		t.Fatalf("returned %#v; the refusal must be a *GradeTooLowError naming the class and marking it unknown", err)
	}
}

// CallCacheable is a second way into the result cache, used by callers that
// pass their own ttl, and it holds a call naming an undeclared class to the
// same refusal Call gives: a result cached for the same handler and payload
// is not served in its place, and nothing is sent.
func TestCallCacheableRefusesAnUndeclaredClassBeforeTheCache(t *testing.T) {
	undeclared := WithOpClass(context.Background(), "turbo")
	sess := newGradedSession(aether.ProtoNoise, "ran")
	defer forgetSession(sess)
	finder := &recordingFinder{mockFinder: mockFinder{session: sess}, bidiOK: true,
		bidiResp: &pb.RPCResponse{Success: true, Payload: []byte("ran")}}
	caller := NewHWPCaller(finder)
	defer caller.Close()
	caller.StoreCachedResult("svc/svc.Op", nil, []byte("cached"), time.Minute)

	out, err := caller.CallCacheable(undeclared, "svc", "svc.Op", nil, time.Minute)
	requireUndeclared(t, err)
	if out != nil {
		t.Errorf("CallCacheable returned payload %q with its refusal", out)
	}
	if finder.findRoutes != 0 || finder.findSessions != 0 || finder.bidiCalls != 0 || sess.sends.Load() != 0 {
		t.Errorf("the finder was asked for routes %d, sessions %d and bidi sends %d times, and %d request(s) "+
			"were sent; an undeclared class is refused before any path",
			finder.findRoutes, finder.findSessions, finder.bidiCalls, sess.sends.Load())
	}

	// The declared default still reads the cache.
	out, err = caller.CallCacheable(context.Background(), "svc", "svc.Op", nil, time.Minute)
	if err != nil || string(out) != "cached" {
		t.Fatalf("CallCacheable with no class named = (%q, %v), want the cached result", out, err)
	}
}

// A handler hosted in this process is not run for a call naming an undeclared
// class. TryLocalDispatch is the path rpc.Call, rpc.CallRaw, CallJSON and
// CallProto take before the mesh, so without the refusal here the same call
// would be refused across the mesh and run when the handler happens to be
// local.
func TestTryLocalDispatchRefusesAnUndeclaredClassBeforeTheHandler(t *testing.T) {
	prev := LocalDispatcherInstance()
	defer SetLocalDispatcher(prev)
	local := &countingLocal{}
	SetLocalDispatcher(local)

	out, handled, err := TryLocalDispatch(WithOpClass(context.Background(), "turbo"), "svc.Op", nil)
	if !handled {
		t.Fatal("handled = false for a local handler: the call would go on to the mesh instead of being refused here")
	}
	requireUndeclared(t, err)
	if out != nil {
		t.Errorf("TryLocalDispatch returned payload %q with its refusal", out)
	}
	if n := local.ran.Load(); n != 0 {
		t.Fatalf("the local handler ran %d time(s) for an undeclared class", n)
	}

	// The declared default, and a declared class, still run the handler.
	for _, ctx := range []context.Context{context.Background(), WithOpClass(context.Background(), "critical")} {
		out, handled, err = TryLocalDispatch(ctx, "svc.Op", nil)
		if !handled || err != nil || string(out) != "ran" {
			t.Fatalf("TryLocalDispatch for a declared class = (%q, %v, %v), want the handler's answer", out, handled, err)
		}
	}
	if n := local.ran.Load(); n != 2 {
		t.Fatalf("the local handler ran %d time(s) for two declared-class calls, want 2", n)
	}
}
