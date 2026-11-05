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

// refusingMiddleware rejects every request in Before with err.
type refusingMiddleware struct{ err error }

func (*refusingMiddleware) Name() string { return "refusing" }
func (m *refusingMiddleware) Before(ctx context.Context, _ string, _ *RPCRequest) (context.Context, error) {
	return ctx, m.err
}
func (*refusingMiddleware) After(_ context.Context, _ string, resp *RPCResponse, err error) (*RPCResponse, error) {
	return resp, err
}

// A refusal of the caller's authority made by the registry's scope gate or by
// a middleware's Before is carried on the failed response as Denied, under the
// response's own text, so the node serving the call can answer it as a
// refusal; a middleware's other rejection carries no Denied.
func TestARegistrysRefusalOfTheCallerIsCarriedAsDenied(t *testing.T) {
	ctx := context.Background()

	gated := NewHandlerRegistry()
	probe := &scopeRefusalProbe{name: "denied.by.scope"}
	if err := gated.RegisterRPC(probe); err != nil {
		t.Fatalf("RegisterRPC: %v", err)
	}
	resp, err := gated.Dispatch(ctx, &RPCRequest{Handler: probe.name})
	if err != nil || resp == nil || resp.Success {
		t.Fatalf("Dispatch = (%+v, %v); want a failed response", resp, err)
	}
	if !errors.Is(resp.Denied, rpcscope.ErrDenied) || resp.Denied.Error() != resp.Error {
		t.Errorf("the scope gate's refusal carries Denied %v with text %q; want an ErrDenied whose text is %q",
			resp.Denied, errString(resp.Denied), resp.Error)
	}

	for _, tc := range []struct {
		name   string
		err    error
		denied bool
	}{
		{"a refusal of the caller", fmt.Errorf("%w: tenant gate", rpcscope.ErrDenied), true},
		{"another rejection", errors.New("rate limited"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reg := NewHandlerRegistry()
			reg.Use(&refusingMiddleware{err: tc.err})
			open := &resolvedRPCProbe{name: "denied.by.middleware"}
			if err := reg.RegisterRPC(open); err != nil {
				t.Fatalf("RegisterRPC: %v", err)
			}
			resp, err := reg.Dispatch(ctx, &RPCRequest{Handler: open.name})
			if err != nil || resp == nil || resp.Success {
				t.Fatalf("Dispatch = (%+v, %v); want a failed response", resp, err)
			}
			if got := resp.Denied != nil; got != tc.denied {
				t.Fatalf("Denied = %v; want set %v", resp.Denied, tc.denied)
			}
			if tc.denied && !errors.Is(resp.Denied, rpcscope.ErrDenied) {
				t.Errorf("Denied %v does not wrap ErrDenied", resp.Denied)
			}
			if n := open.ran.Load(); n != 0 {
				t.Errorf("the handler ran %d time(s) past its middleware's rejection", n)
			}
		})
	}
}
