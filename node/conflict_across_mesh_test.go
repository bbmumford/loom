/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/ORBTR/aether"

	"github.com/bbmumford/loom/node/handlers"
	"github.com/bbmumford/loom/pkg/dispatch"
	"github.com/bbmumford/loom/pkg/rpc"
	rpchttp "github.com/bbmumford/loom/pkg/rpc/http"
)

// These tests carry a handler's conflict with the state it holds across one
// hop, beside the denial tests' refusal of the caller's authority: the far
// node runs a handler exported from an rpc.Registry, as a domain host is
// served, and the handler answers a write naming a stale revision with an
// error wrapping dispatch.ErrConflict. The conflict reaches the origin's
// caller as rpc.ErrConflict and not as rpc.ErrScopeDenied, and the origin's
// HTTP bridge proxying the operation answers it 409.

// errStaleRevision stands for a domain host's refusal of a write whose
// revision is older than the stored one, which the host wraps around
// dispatch.ErrConflict.
var errStaleRevision = fmt.Errorf("%w: host: role revision 3 is stale; the stored revision is 4", dispatch.ErrConflict)

// errDeniedConflict is a conflict that also refuses the caller's authority:
// the denial is what the caller learns.
var errDeniedConflict = fmt.Errorf("%w: %w", errHostRefused, errStaleRevision)

// A far handler's stale-revision conflict reaches the origin as
// rpc.ErrConflict, under the far handler's own words, stays the handler's
// answer (dispatch.ErrHandlerError), and is not a refusal of the caller's
// authority. A conflict that also refuses the caller reaches it as the
// refusal alone.
func TestAFarHandlersConflictReachesTheOriginAsConflictNotDenied(t *testing.T) {
	for _, tc := range []struct {
		name             string
		answer           error
		conflict, denied bool
		words            string
	}{
		{"a stale revision", errStaleRevision, true, false, "revision 3 is stale"},
		{"a conflict that also refuses the caller", errDeniedConflict, false, true, "holds no grant"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			op := deniedOp(rpc.ScopeNone, tc.answer)
			caller := farServing(t, op)
			_, err := caller.Call(context.Background(), "test", op.FQN(), nil)
			if err == nil {
				t.Fatal("the call succeeded; the far handler refused it")
			}
			if got := errors.Is(err, rpc.ErrConflict); got != tc.conflict {
				t.Fatalf("errors.Is(%v, rpc.ErrConflict) = %v, want %v", err, got, tc.conflict)
			}
			if got := errors.Is(err, rpc.ErrScopeDenied); got != tc.denied {
				t.Fatalf("errors.Is(%v, rpc.ErrScopeDenied) = %v, want %v", err, got, tc.denied)
			}
			if !errors.Is(err, dispatch.ErrHandlerError) {
				t.Errorf("the origin got %v, which is not the handler's answer", err)
			}
			if !strings.Contains(err.Error(), tc.words) {
				t.Errorf("the error text %q lost the far node's words %q", err, tc.words)
			}
		})
	}
}

// A far handler's conflict, proxied by the origin's HTTP bridge over the mesh,
// answers 409 with the handler's words; a conflict that also refuses the
// caller answers 403.
func TestAFarConflictProxiedByTheBridgeAnswers409(t *testing.T) {
	for _, tc := range []struct {
		name   string
		answer error
		status int
		words  string
	}{
		{"a stale revision", errStaleRevision, http.StatusConflict, "revision 3 is stale"},
		{"a conflict that also refuses the caller", errDeniedConflict, http.StatusForbidden, "holds no grant"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			op := deniedOp(rpc.ScopeNone, tc.answer)
			caller := farServing(t, op)
			previous := dispatch.GetCaller()
			dispatch.SetCaller(caller)
			t.Cleanup(func() { dispatch.SetCaller(previous) })

			status, body := postThroughBridge(t, proxyFor(t, op), op.FQN())
			if status != tc.status {
				t.Fatalf("the bridge answered %d %s; want %d", status, body, tc.status)
			}
			if !strings.Contains(body, tc.words) {
				t.Errorf("the body %q lost the far handler's words %q", body, tc.words)
			}
		})
	}
}

// A conflict made by a handler this process hosts, which rpc.CallRaw reaches
// through the node's local dispatcher instead of the mesh, stays a conflict
// for every caller of DispatchBytes and answers 409 at the bridge proxying it.
// The bridge running the handler's Func in its own process answers 409 too.
func TestALocalHostsConflictAnswers409(t *testing.T) {
	op := deniedOp(rpc.ScopeNone, errStaleRevision)
	source := rpc.NewRegistry()
	if err := source.Register(op); err != nil {
		t.Fatalf("Register: %v", err)
	}
	exported := handlers.NewHandlerRegistry()
	if err := source.ExportToHandlerRegistry(exported); err != nil {
		t.Fatalf("ExportToHandlerRegistry: %v", err)
	}
	adapter := &localDispatcherAdapter{registry: exported}

	_, err := adapter.DispatchBytes(context.Background(), op.FQN(), nil)
	if !errors.Is(err, rpc.ErrConflict) || errors.Is(err, rpc.ErrScopeDenied) || !strings.Contains(err.Error(), "revision 3 is stale") {
		t.Fatalf("DispatchBytes returned %v; want the host's conflict as rpc.ErrConflict", err)
	}

	previous := rpc.LocalDispatcherInstance()
	rpc.SetLocalDispatcher(adapter)
	t.Cleanup(func() { rpc.SetLocalDispatcher(previous) })
	if status, body := postThroughBridge(t, proxyFor(t, op), op.FQN()); status != http.StatusConflict {
		t.Fatalf("the bridge proxying a local host answered %d %s; want 409", status, body)
	}

	if status, body := postThroughBridge(t, rpchttp.NewBridge(source), op.FQN()); status != http.StatusConflict {
		t.Fatalf("the bridge running the Func answered %d %s; want 409", status, body)
	}
}

// A conflict a handler registered on the node's registry directly returns as
// its error, which the far registry's Dispatch hands back as an error and not
// as a failed response, reaches the origin as rpc.ErrConflict too.
func TestAFarHandlersReturnedConflictReachesTheOriginAsConflict(t *testing.T) {
	reg := handlers.NewHandlerRegistry()
	h := &erringHandler{err: errStaleRevision}
	if err := reg.RegisterRPC(h); err != nil {
		t.Fatalf("RegisterRPC: %v", err)
	}
	caller := dispatch.NewHWPCaller(&oneHopFinder{session: &servedSession{
		hopSession: hopSession{remote: "far", proto: aether.ProtoQUIC},
		server:     NewRPCServer(reg),
	}})
	t.Cleanup(caller.Close)

	_, err := caller.Call(context.Background(), "test", h.Name(), nil)
	if !errors.Is(err, rpc.ErrConflict) || errors.Is(err, rpc.ErrScopeDenied) || !strings.Contains(err.Error(), "revision 3 is stale") {
		t.Fatalf("the origin got %v; want the far handler's conflict as rpc.ErrConflict", err)
	}
}
