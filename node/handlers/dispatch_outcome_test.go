/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
 */

package handlers

import (
	"context"
	"strings"
	"testing"
)

// A handler that answers no response and no error has answered nothing a
// caller can hand on, so Dispatch returns a failure naming the handler in its
// place, before and after an After chain that answers nothing over it, and
// never no response and no error together.
func TestAHandlerAnsweringNothingIsAFailureNamingIt(t *testing.T) {
	nothing := func(*RPCResponse, error) (*RPCResponse, error) { return nil, nil }
	for _, tc := range []struct {
		name  string
		chain []func(*RPCResponse, error) (*RPCResponse, error)
	}{
		{"no middleware", nil},
		{"an After chain answering nothing", []func(*RPCResponse, error) (*RPCResponse, error){nothing, nothing}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reg := NewHandlerRegistry()
			for _, a := range tc.chain {
				reg.Use(&answeringMiddleware{answer: a})
			}
			h := &answeringHandler{resolvedRPCProbe: resolvedRPCProbe{name: "handler.answering.nothing"}, answer: func() (*RPCResponse, error) {
				return nil, nil
			}}
			if err := reg.RegisterRPC(h); err != nil {
				t.Fatalf("RegisterRPC: %v", err)
			}
			resp, err := reg.Dispatch(context.Background(), &RPCRequest{Handler: h.name})
			if resp != nil || err == nil {
				t.Fatalf("Dispatch = (%+v, %v); want no response and a failure", resp, err)
			}
			if kind, _ := answeredKind(resp, err); kind != "neither" {
				t.Errorf("Dispatch returned %v, read as %s; want a failure of neither kind", err, kind)
			}
			if !strings.Contains(err.Error(), h.name) {
				t.Errorf("Dispatch returned %q; want it to name the handler %q", err, h.name)
			}
		})
	}
}
