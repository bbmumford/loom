/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package handlers

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	rpcscope "github.com/bbmumford/loom/pkg/rpc/scope"
	"github.com/bbmumford/loom/ports"
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

// refusingRPCAuth refuses every handler's execution auth with err.
type refusingRPCAuth struct{ err error }

func (a refusingRPCAuth) ValidateExecutionAuth(context.Context, ports.SecureHandler) error {
	return a.err
}
func (refusingRPCAuth) WithPartitionID(ctx context.Context, _ string) context.Context { return ctx }

// A refusal of the caller's execution auth, the validator's answer
// DispatchRPCWithAuth asks before the scope gate, is carried on the failed
// response as Denied under the response's own text and the validator's words,
// as the scope gate's and a middleware's refusals are, and the handler does
// not run. An admitted call carries no Denied.
func TestAnExecutionAuthRefusalIsCarriedAsDenied(t *testing.T) {
	reg := NewHandlerRegistry()
	probe := &resolvedRPCProbe{name: "denied.by.execution.auth"}
	if err := reg.RegisterRPC(probe); err != nil {
		t.Fatalf("RegisterRPC: %v", err)
	}
	refusal := errors.New("handler denied.by.execution.auth requires scope \"rpc.execute\"")
	resp, err := reg.DispatchRPCWithAuth(context.Background(), &RPCRequest{Handler: probe.name}, refusingRPCAuth{err: refusal})
	if err != nil || resp == nil || resp.Success {
		t.Fatalf("DispatchRPCWithAuth = (%+v, %v); want a failed response", resp, err)
	}
	if !errors.Is(resp.Denied, rpcscope.ErrDenied) || resp.Denied.Error() != resp.Error {
		t.Fatalf("the execution auth's refusal carries Denied %v with text %q; want an ErrDenied whose text is %q",
			resp.Denied, errString(resp.Denied), resp.Error)
	}
	if !errors.Is(resp.Denied, refusal) || !strings.Contains(resp.Error, "requires scope") {
		t.Errorf("Denied %v lost the validator's refusal %v", resp.Denied, refusal)
	}
	if n := probe.ran.Load(); n != 0 {
		t.Errorf("the handler ran %d time(s) past its execution auth's refusal", n)
	}

	admitted, err := reg.DispatchRPCWithAuth(context.Background(), &RPCRequest{Handler: probe.name}, &allowingRPCAuth{})
	if err != nil || admitted == nil || !admitted.Success || admitted.Denied != nil {
		t.Fatalf("an admitted call = (%+v, %v); want success with no Denied", admitted, err)
	}
}
