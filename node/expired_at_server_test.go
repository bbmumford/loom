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

	"github.com/ORBTR/aether/rpc/pb"
	"github.com/bbmumford/loom/pkg/dispatch"
	"github.com/bbmumford/loom/pkg/rpc"
)

// lateForwarder delivers a request to the next server after its absolute
// deadline has passed, as a hop that held it too long would.
type lateForwarder struct {
	next *RPCServer
}

func (f *lateForwarder) Forward(ctx context.Context, req *pb.RPCRequest) (*pb.RPCResponse, error) {
	req.Deadline = time.Now().Add(-time.Second).UnixNano()
	return f.next.handleRequest(ctx, req), nil
}

// A server that receives a request past its absolute deadline refuses it with
// the typed deadline refusal, stamped on the response like the forwarder's,
// so the origin's caller gets rpc.ErrRouteDeadline through the hop that
// relayed it. The refusal text alone reaches the origin as a handler
// rejection, which a caller cannot tell from the handler having run and
// failed.
func TestAServersExpiredRequestRefusalReachesTheOriginTyped(t *testing.T) {
	executor, _, runs := localRunServer(t)
	caller := originOver(t, &lateForwarder{next: executor})

	_, err := caller.Call(context.Background(), "test.floor", "test.floor.Run", nil)
	if !errors.Is(err, rpc.ErrRouteDeadline) {
		t.Fatalf("the origin got %v; a request the executor received past its deadline must come back as rpc.ErrRouteDeadline", err)
	}
	var rde *RouteDeadlineError
	if !errors.As(err, &rde) || rde.Remaining >= 0 || rde.Need != 0 {
		t.Fatalf("the origin got %#v; want a *RouteDeadlineError with a negative remaining time and no need", err)
	}
	if errors.Is(err, dispatch.ErrHandlerError) {
		t.Errorf("the origin reports the refusal as a handler error: %v", err)
	}
	if n := runs.Load(); n != 0 {
		t.Errorf("the handler ran %d time(s) for a request past its deadline", n)
	}
}
