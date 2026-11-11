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

// restatingAfter passes its Before and answers a failed response handed to
// its After with a failed response of its own that names neither kind, as a
// middleware rewriting the refusal text for its callers does.
type restatingAfter struct{}

func (*restatingAfter) Name() string { return "restating-after" }
func (*restatingAfter) Before(ctx context.Context, _ string, _ *handlers.RPCRequest) (context.Context, error) {
	return ctx, nil
}
func (*restatingAfter) After(_ context.Context, _ string, resp *handlers.RPCResponse, err error) (*handlers.RPCResponse, error) {
	if resp == nil || resp.Success {
		return resp, err
	}
	return &handlers.RPCResponse{Success: false, Error: "restated: " + resp.Error}, err
}

// A middleware's Before conflict under an outer middleware whose After
// answers it with a failed response of its own reaches the origin of a call
// across the mesh as a conflict, with the outer response's words, and the
// bridge answers it 409; the node's local dispatcher returns it as a
// conflict too.
func TestABeforeConflictUnderAnOuterAfterReachesTheOriginAsConflict(t *testing.T) {
	op := deniedOp(rpc.ScopeNone, nil)
	reg := guardedRegistry(t, op, &restatingAfter{})
	reg.Use(&refusingBefore{err: errStaleRevision})
	caller := dispatch.NewHWPCaller(&oneHopFinder{session: &servedSession{
		hopSession: hopSession{remote: "far", proto: aether.ProtoQUIC},
		server:     NewRPCServer(reg),
	}})
	t.Cleanup(caller.Close)

	_, err := caller.Call(context.Background(), "test", op.FQN(), nil)
	if !errors.Is(err, rpc.ErrConflict) || errors.Is(err, rpc.ErrScopeDenied) {
		t.Fatalf("Call returned %v; want the far node's conflict", err)
	}
	if !strings.Contains(err.Error(), "restated: ") || !strings.Contains(err.Error(), "revision 3 is stale") {
		t.Errorf("Call returned %v; want the outer middleware's words over the hook's", err)
	}

	previous := dispatch.GetCaller()
	dispatch.SetCaller(caller)
	t.Cleanup(func() { dispatch.SetCaller(previous) })
	if status, body := postThroughBridge(t, proxyFor(t, op), op.FQN()); status != http.StatusConflict {
		t.Fatalf("the bridge answered %d %s; want 409", status, body)
	}

	_, err = (&localDispatcherAdapter{registry: reg}).DispatchBytes(context.Background(), op.FQN(), nil)
	if !errors.Is(err, rpc.ErrConflict) || errors.Is(err, rpc.ErrScopeDenied) {
		t.Fatalf("DispatchBytes returned %v; want the hook's conflict", err)
	}
}
