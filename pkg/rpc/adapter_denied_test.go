/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package rpc

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/bbmumford/loom/node/handlers"
)

// An exported handler's refusal of its caller is kept on the shim's failed
// response as Denied, under the response's text, so the node serving it over
// the mesh answers it as a refusal; a Func's other errors carry no Denied.
func TestTheShimCarriesAFuncsRefusalOfItsCallerAsDenied(t *testing.T) {
	for _, tc := range []struct {
		name   string
		answer error
		denied bool
	}{
		{"a refusal of the caller", fmt.Errorf("%w: host: the caller holds no grant", ErrScopeDenied), true},
		{"another error", errors.New("host: the record is not found"), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			shim := newRegistryShim(&Handler{
				Namespace: "test", Domain: "denial", Operation: "Op",
				Func: func(context.Context, proto.Message) (proto.Message, error) {
					return nil, tc.answer
				},
				Request:  (*structpb.Struct)(nil),
				Response: (*structpb.Struct)(nil),
				Scope:    ScopeNone,
			})
			resp, err := shim.ExecuteRPC(context.Background(), &handlers.RPCRequest{ID: "req-1"})
			if err != nil || resp == nil || resp.Success || resp.Error != tc.answer.Error() {
				t.Fatalf("ExecuteRPC = (%+v, %v); want a failed response with the Func's text", resp, err)
			}
			if got := resp.Denied != nil; got != tc.denied {
				t.Fatalf("Denied = %v; want set %v", resp.Denied, tc.denied)
			}
			if tc.denied && !errors.Is(resp.Denied, ErrScopeDenied) {
				t.Errorf("Denied %v does not wrap ErrScopeDenied", resp.Denied)
			}
		})
	}
}
