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

// answeringMiddleware passes its Before and answers After with what answer
// returns over the response and error it is handed.
type answeringMiddleware struct {
	answer func(*RPCResponse, error) (*RPCResponse, error)
}

func (*answeringMiddleware) Name() string { return "answering" }
func (*answeringMiddleware) Before(ctx context.Context, _ string, _ *RPCRequest) (context.Context, error) {
	return ctx, nil
}
func (m *answeringMiddleware) After(_ context.Context, _ string, resp *RPCResponse, err error) (*RPCResponse, error) {
	return m.answer(resp, err)
}

// An outer middleware whose After answers a later middleware's Before refusal
// with a failed response of its own, which names neither kind, keeps the
// refusal's kind: a conflict stays a conflict and a refusal of the caller's
// authority stays Denied, each carried under the outer response's own text,
// read from the error the After chain ended with or, when that names no kind,
// from the Before's refusal. A kind the After set itself, a success it
// answered, and a rejection of neither kind are left as the After answered.
func TestAnOuterAfterAnsweringABeforeRefusalKeepsItsKind(t *testing.T) {
	stale := fmt.Errorf("%w: idempotency: event 7 is already in flight", rpcscope.ErrConflict)
	denied := fmt.Errorf("%w: holds no grant", rpcscope.ErrDenied)
	for _, tc := range []struct {
		name             string
		refusal          error
		answer           func(*RPCResponse, error) (*RPCResponse, error)
		success          bool
		conflict, denied bool
	}{
		{"a conflict, the After's error passed on", stale, func(r *RPCResponse, e error) (*RPCResponse, error) {
			return &RPCResponse{Success: false, Error: "masked: " + r.Error}, e
		}, false, true, false},
		{"a conflict, the After's error dropped", stale, func(r *RPCResponse, _ error) (*RPCResponse, error) {
			return &RPCResponse{Success: false, Error: "masked: " + r.Error}, nil
		}, false, true, false},
		{"a denial, the After's error dropped", denied, func(r *RPCResponse, _ error) (*RPCResponse, error) {
			return &RPCResponse{Success: false, Error: "masked: " + r.Error}, nil
		}, false, false, true},
		{"a conflict the After restated as a denial", stale, func(r *RPCResponse, e error) (*RPCResponse, error) {
			return &RPCResponse{Success: false, Error: "masked: " + r.Error}, fmt.Errorf("%w: %w", rpcscope.ErrDenied, e)
		}, false, false, true},
		{"a conflict the After answered as success", stale, func(*RPCResponse, error) (*RPCResponse, error) {
			return &RPCResponse{Success: true}, nil
		}, true, false, false},
		{"another rejection", errors.New("rate limited"), func(r *RPCResponse, _ error) (*RPCResponse, error) {
			return &RPCResponse{Success: false, Error: "masked: " + r.Error}, nil
		}, false, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reg := NewHandlerRegistry()
			reg.Use(&answeringMiddleware{answer: tc.answer})
			reg.Use(&refusingMiddleware{err: tc.refusal})
			open := &resolvedRPCProbe{name: "conflict.under.outer.after"}
			if err := reg.RegisterRPC(open); err != nil {
				t.Fatalf("RegisterRPC: %v", err)
			}
			resp, err := reg.Dispatch(context.Background(), &RPCRequest{Handler: open.name})
			if err != nil || resp == nil || resp.Success != tc.success {
				t.Fatalf("Dispatch = (%+v, %v); want success %v", resp, err, tc.success)
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
			if tc.denied && (!errors.Is(resp.Denied, rpcscope.ErrDenied) || resp.Denied.Error() != resp.Error) {
				t.Errorf("Denied %v with text %q; want an ErrDenied whose text is %q", resp.Denied, errString(resp.Denied), resp.Error)
			}
			if n := open.ran.Load(); n != 0 {
				t.Errorf("the handler ran %d time(s) past its middleware's rejection", n)
			}
		})
	}

	// A kind the outer After set itself is the one carried.
	reg := NewHandlerRegistry()
	reg.Use(&answeringMiddleware{answer: func(r *RPCResponse, _ error) (*RPCResponse, error) {
		return &RPCResponse{Success: false, Error: denied.Error(), Denied: denied}, nil
	}})
	reg.Use(&refusingMiddleware{err: stale})
	open := &resolvedRPCProbe{name: "conflict.under.outer.denial"}
	if err := reg.RegisterRPC(open); err != nil {
		t.Fatalf("RegisterRPC: %v", err)
	}
	resp, err := reg.Dispatch(context.Background(), &RPCRequest{Handler: open.name})
	if err != nil || resp == nil || resp.Denied != denied || resp.Conflict != nil {
		t.Fatalf("Dispatch = (%+v, %v); want the After's own Denied and no Conflict", resp, err)
	}
}
