/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package handlers

import (
	"context"
	"errors"
	"fmt"
	"testing"

	rpcscope "github.com/bbmumford/loom/pkg/rpc/scope"
)

// A middleware's Before refusing a call with a conflict with the stored state
// is carried on the failed response as Conflict, under the response's own
// text, and not as Denied; a Before refusal that also refuses the caller's
// authority is carried as Denied alone, and any other rejection as neither.
// The handler does not run past any of them.
func TestABeforeHooksConflictIsCarriedAsConflict(t *testing.T) {
	stale := fmt.Errorf("%w: idempotency: event 7 is already in flight", rpcscope.ErrConflict)
	for _, tc := range []struct {
		name             string
		err              error
		conflict, denied bool
	}{
		{"a conflict", stale, true, false},
		{"a conflict that also refuses the caller", fmt.Errorf("%w: %w", rpcscope.ErrDenied, stale), false, true},
		{"another rejection", errors.New("rate limited"), false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reg := NewHandlerRegistry()
			reg.Use(&refusingMiddleware{err: tc.err})
			open := &resolvedRPCProbe{name: "conflict.by.middleware"}
			if err := reg.RegisterRPC(open); err != nil {
				t.Fatalf("RegisterRPC: %v", err)
			}
			resp, err := reg.Dispatch(context.Background(), &RPCRequest{Handler: open.name})
			if err != nil || resp == nil || resp.Success {
				t.Fatalf("Dispatch = (%+v, %v); want a failed response", resp, err)
			}
			if got := resp.Conflict != nil; got != tc.conflict {
				t.Fatalf("Conflict = %v; want set %v", resp.Conflict, tc.conflict)
			}
			if got := resp.Denied != nil; got != tc.denied {
				t.Fatalf("Denied = %v; want set %v", resp.Denied, tc.denied)
			}
			if tc.conflict && (!errors.Is(resp.Conflict, rpcscope.ErrConflict) || resp.Conflict.Error() != resp.Error) {
				t.Errorf("Conflict %v with text %q; want an ErrConflict whose text is %q", resp.Conflict, errString(resp.Conflict), resp.Error)
			}
			if n := open.ran.Load(); n != 0 {
				t.Errorf("the handler ran %d time(s) past its middleware's rejection", n)
			}
		})
	}
}
