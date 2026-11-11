/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/ORBTR/aether"

	"github.com/bbmumford/loom/node/handlers"
	"github.com/bbmumford/loom/pkg/dispatch"
	"github.com/bbmumford/loom/pkg/rpc"
)

// refusingBefore is a middleware whose Before rejects every call with err, as
// an idempotency or in-flight guard refuses a duplicate event or a change of a
// record already in flight before the handler runs.
type refusingBefore struct{ err error }

func (*refusingBefore) Name() string { return "refusing-before" }
func (m *refusingBefore) Before(ctx context.Context, _ string, _ *handlers.RPCRequest) (context.Context, error) {
	return ctx, m.err
}
func (*refusingBefore) After(_ context.Context, _ string, resp *handlers.RPCResponse, err error) (*handlers.RPCResponse, error) {
	return resp, err
}

// guardedRegistry is the far node's registry: op exported from an
// rpc.Registry, as a domain host is served, behind mw.
func guardedRegistry(t *testing.T, op rpc.Handler, mw handlers.Middleware) *handlers.HandlerRegistry {
	t.Helper()
	source := rpc.NewRegistry()
	if err := source.Register(op); err != nil {
		t.Fatalf("Register: %v", err)
	}
	exported := handlers.NewHandlerRegistry()
	if err := source.ExportToHandlerRegistry(exported); err != nil {
		t.Fatalf("ExportToHandlerRegistry: %v", err)
	}
	exported.Use(mw)
	return exported
}

// A middleware Before hook on another node refusing a call with a conflict
// reaches the origin as rpc.ErrConflict, under the hook's own words, and stays
// the far node's answer (dispatch.ErrHandlerError); the origin's HTTP bridge
// proxying the operation answers it 409. A Before refusal that also refuses
// the caller's authority reaches it as the denial alone (403), and the
// handler never runs.
func TestAFarBeforeHooksConflictReachesTheOriginAsConflict(t *testing.T) {
	for _, tc := range []struct {
		name             string
		refusal          error
		conflict, denied bool
		status           int
		words            string
	}{
		{"an in-flight conflict", errStaleRevision, true, false, http.StatusConflict, "revision 3 is stale"},
		{"a conflict that also refuses the caller", errDeniedConflict, false, true, http.StatusForbidden, "holds no grant"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			op := deniedOp(rpc.ScopeNone, nil)
			caller := dispatch.NewHWPCaller(&oneHopFinder{session: &servedSession{
				hopSession: hopSession{remote: "far", proto: aether.ProtoQUIC},
				server:     NewRPCServer(guardedRegistry(t, op, &refusingBefore{err: tc.refusal})),
			}})
			t.Cleanup(caller.Close)

			_, err := caller.Call(context.Background(), "test", op.FQN(), nil)
			if got := errors.Is(err, rpc.ErrConflict); got != tc.conflict {
				t.Fatalf("Call returned %v: conflict %v, want %v", err, got, tc.conflict)
			}
			if got := errors.Is(err, rpc.ErrScopeDenied); got != tc.denied {
				t.Fatalf("Call returned %v: denied %v, want %v", err, got, tc.denied)
			}
			if !errors.Is(err, dispatch.ErrHandlerError) || !strings.Contains(err.Error(), tc.words) {
				t.Errorf("Call returned %v; want the far node's answer with its words %q", err, tc.words)
			}

			previous := dispatch.GetCaller()
			dispatch.SetCaller(caller)
			t.Cleanup(func() { dispatch.SetCaller(previous) })
			if status, body := postThroughBridge(t, proxyFor(t, op), op.FQN()); status != tc.status {
				t.Fatalf("the bridge answered %d %s; want %d", status, body, tc.status)
			}
		})
	}
}

// A Before hook's conflict on a handler this process hosts, reached through
// the node's local dispatcher, stays a conflict for DispatchBytes' caller and
// answers 409 at the bridge proxying it.
func TestALocalBeforeHooksConflictAnswers409(t *testing.T) {
	op := deniedOp(rpc.ScopeNone, nil)
	adapter := &localDispatcherAdapter{registry: guardedRegistry(t, op, &refusingBefore{err: errStaleRevision})}

	_, err := adapter.DispatchBytes(context.Background(), op.FQN(), nil)
	if !errors.Is(err, rpc.ErrConflict) || errors.Is(err, rpc.ErrScopeDenied) || !strings.Contains(err.Error(), "revision 3 is stale") {
		t.Fatalf("DispatchBytes returned %v; want the hook's conflict as rpc.ErrConflict", err)
	}

	previous := rpc.LocalDispatcherInstance()
	rpc.SetLocalDispatcher(adapter)
	t.Cleanup(func() { rpc.SetLocalDispatcher(previous) })
	if status, body := postThroughBridge(t, proxyFor(t, op), op.FQN()); status != http.StatusConflict {
		t.Fatalf("the bridge proxying a local host answered %d %s; want 409", status, body)
	}
}
