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

// An exported handler's conflict with its stored state is kept on the shim's
// failed response as Conflict and not as Denied, under the response's text, so
// the node serving it over the mesh answers it as a conflict; a conflict that
// also refuses the caller is kept as Denied alone, and a Func's other errors
// carry neither.
func TestTheShimCarriesAFuncsConflictAsConflict(t *testing.T) {
	stale := fmt.Errorf("%w: host: role revision 3 is stale", ErrConflict)
	denial := fmt.Errorf("%w: host: the caller holds no grant", ErrScopeDenied)
	for _, tc := range []struct {
		name             string
		answer           error
		conflict, denied bool
	}{
		{"a stale revision", stale, true, false},
		{"a conflict that also refuses the caller", fmt.Errorf("%w: %w", denial, stale), false, true},
		{"another error", errors.New("host: the record is not found"), false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			shim := newRegistryShim(&Handler{
				Namespace: "test", Domain: "conflict", Operation: "Op",
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
			if got := resp.Conflict != nil; got != tc.conflict {
				t.Fatalf("Conflict = %v; want set %v", resp.Conflict, tc.conflict)
			}
			if got := resp.Denied != nil; got != tc.denied {
				t.Fatalf("Denied = %v; want set %v", resp.Denied, tc.denied)
			}
			if tc.conflict && !errors.Is(resp.Conflict, ErrConflict) {
				t.Errorf("Conflict %v does not wrap ErrConflict", resp.Conflict)
			}
		})
	}
}
