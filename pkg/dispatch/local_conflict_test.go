/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package dispatch

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/bbmumford/loom/node/handlers"
	rpcscope "github.com/bbmumford/loom/pkg/rpc/scope"
)

// LocalCaller returns a local handler's conflict with its stored state as an
// error that matches ErrConflict and not scope.ErrDenied, as HWPCaller returns
// one a handler on another node answered.
func TestLocalCallerKeepsAHandlersConflict(t *testing.T) {
	reg := handlers.NewHandlerRegistry()
	h := &answeringHandler{resp: &handlers.RPCResponse{Error: errStaleRevision.Error(), Conflict: errStaleRevision}}
	if err := reg.RegisterRPC(h); err != nil {
		t.Fatalf("RegisterRPC: %v", err)
	}
	_, err := NewLocalCaller(reg).Call(context.Background(), "svc", h.Name(), nil)
	if !errors.Is(err, ErrConflict) || errors.Is(err, rpcscope.ErrDenied) {
		t.Fatalf("Call returned %v; want the handler's conflict, matching ErrConflict and not scope.ErrDenied", err)
	}
	if !strings.Contains(err.Error(), "revision 3 is stale") {
		t.Errorf("the error text %q lost the handler's words", err)
	}
}
