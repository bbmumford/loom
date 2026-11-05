/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package dispatch

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/bbmumford/loom/node/handlers"
	rpcscope "github.com/bbmumford/loom/pkg/rpc/scope"
)

// answeringHandler is an RPC handler that answers every call with resp.
type answeringHandler struct{ resp *handlers.RPCResponse }

func (*answeringHandler) Name() string                { return "svc.denial.Op" }
func (*answeringHandler) Role() string                { return "svc" }
func (*answeringHandler) RequiresAuth() bool          { return false }
func (*answeringHandler) AllowedAuthTypes() []string  { return nil }
func (*answeringHandler) Scopes() []string            { return nil }
func (*answeringHandler) Scope() handlers.Scope       { return handlers.ScopeNone }
func (*answeringHandler) AllowedPartitions() []string { return nil }
func (h *answeringHandler) ExecuteRPC(context.Context, *handlers.RPCRequest) (*handlers.RPCResponse, error) {
	return h.resp, nil
}

// LocalCaller returns a local handler's refusal of its caller as an error that
// matches scope.ErrDenied, as HWPCaller returns one a handler on another node
// made, and its other failures as text.
func TestLocalCallerKeepsAHandlersRefusalOfItsCaller(t *testing.T) {
	denial := fmt.Errorf("%w: host: the caller holds no grant", rpcscope.ErrDenied)
	for _, tc := range []struct {
		name   string
		resp   *handlers.RPCResponse
		denied bool
		words  string
	}{
		{"a refusal of the caller", &handlers.RPCResponse{Error: denial.Error(), Denied: denial}, true, "holds no grant"},
		{"another failure", &handlers.RPCResponse{Error: "not found"}, false, "not found"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reg := handlers.NewHandlerRegistry()
			h := &answeringHandler{resp: tc.resp}
			if err := reg.RegisterRPC(h); err != nil {
				t.Fatalf("RegisterRPC: %v", err)
			}
			_, err := NewLocalCaller(reg).Call(context.Background(), "svc", h.Name(), nil)
			if err == nil {
				t.Fatal("the call succeeded; the handler failed it")
			}
			if got := errors.Is(err, rpcscope.ErrDenied); got != tc.denied {
				t.Fatalf("errors.Is(%v, scope.ErrDenied) = %v, want %v", err, got, tc.denied)
			}
			if !strings.Contains(err.Error(), tc.words) {
				t.Errorf("the error text %q lost the handler's words %q", err, tc.words)
			}
		})
	}
}
