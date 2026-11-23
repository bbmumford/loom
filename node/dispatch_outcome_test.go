/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
 */

package node

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/ORBTR/aether"

	"github.com/bbmumford/loom/compose"
	"github.com/bbmumford/loom/node/handlers"
	"github.com/bbmumford/loom/pkg/dispatch"
	"github.com/bbmumford/loom/pkg/rpc"
)

// outcomeHandler is an RPC handler registered on a handler registry directly
// that answers every call with what answer returns, as a handler building its
// own response does.
type outcomeHandler struct {
	answer func() (*handlers.RPCResponse, error)
}

func (*outcomeHandler) Name() string                { return "test.outcome.Op" }
func (*outcomeHandler) Role() string                { return "test" }
func (*outcomeHandler) RequiresAuth() bool          { return false }
func (*outcomeHandler) AllowedAuthTypes() []string  { return nil }
func (*outcomeHandler) Scopes() []string            { return nil }
func (*outcomeHandler) Scope() handlers.Scope       { return handlers.ScopeNone }
func (*outcomeHandler) AllowedPartitions() []string { return nil }
func (h *outcomeHandler) ExecuteRPC(context.Context, *handlers.RPCRequest) (*handlers.RPCResponse, error) {
	return h.answer()
}

// outcomeCases are the five answers a handler gives: a success, a refusal of
// the caller's authority, a conflict with the state it holds, a failure of
// neither kind, and nothing at all. kind is how every reader of the dispatch
// result hands it on, and words what it hands on: the payload of a success,
// otherwise words the failure carries.
func outcomeCases() []struct {
	name   string
	answer func() (*handlers.RPCResponse, error)
	kind   string
	words  string
} {
	stale := fmt.Errorf("%w: host: role revision 3 is stale", rpc.ErrConflict)
	denied := fmt.Errorf("%w: host: the caller holds no grant", rpc.ErrScopeDenied)
	return []struct {
		name   string
		answer func() (*handlers.RPCResponse, error)
		kind   string
		words  string
	}{
		{"a success", func() (*handlers.RPCResponse, error) {
			return &handlers.RPCResponse{Success: true, Payload: []byte("answered")}, nil
		}, "success", "answered"},
		{"a refusal of the caller", func() (*handlers.RPCResponse, error) {
			return &handlers.RPCResponse{Success: false, Error: denied.Error(), Denied: denied}, nil
		}, "denied", "holds no grant"},
		{"a conflict", func() (*handlers.RPCResponse, error) {
			return &handlers.RPCResponse{Success: false, Error: stale.Error(), Conflict: stale}, nil
		}, "conflict", "revision 3 is stale"},
		{"a failure of neither kind", func() (*handlers.RPCResponse, error) {
			return &handlers.RPCResponse{Success: false, Error: "the record is not found"}, nil
		}, "neither", "not found"},
		{"no response and no error", func() (*handlers.RPCResponse, error) {
			return nil, nil
		}, "neither", "test.outcome.Op"},
	}
}

// outcomeKind names the kind of failure err hands on.
func outcomeKind(err error) string {
	switch {
	case errors.Is(err, rpc.ErrScopeDenied):
		return "denied"
	case errors.Is(err, rpc.ErrConflict):
		return "conflict"
	}
	return "neither"
}

// callRecovering runs call and reports a panic it raised as stopped.
func callRecovering(call func() ([]byte, error)) (out []byte, err error, stopped any) {
	defer func() { stopped = recover() }()
	out, err = call()
	return out, err, nil
}

// Every caller of a handler registry reads one dispatch result alike: the far
// caller over the mesh (RPCServer.executeLocal answering the wire), the node's
// local dispatcher and dispatch.LocalCaller each hand on the payload of a
// success, and for a refusal of the caller, a conflict, a failure of neither
// kind and a handler that answered nothing, a failure of the same kind under
// the same words: the words the local dispatcher, which adds none of its own,
// hands on are in every caller's failure. None of them stops the process.
func TestEveryCallerReadsADispatchResultAlike(t *testing.T) {
	for _, tc := range outcomeCases() {
		t.Run(tc.name, func(t *testing.T) {
			reg := handlers.NewHandlerRegistry()
			h := &outcomeHandler{answer: tc.answer}
			if err := reg.RegisterRPC(h); err != nil {
				t.Fatalf("RegisterRPC: %v", err)
			}
			far := dispatch.NewHWPCaller(&oneHopFinder{session: &servedSession{
				hopSession: hopSession{remote: "far", proto: aether.ProtoQUIC},
				server:     NewRPCServer(reg),
			}})
			t.Cleanup(far.Close)
			ctx := context.Background()

			surfaces := []struct {
				name string
				call func() ([]byte, error)
			}{
				{"the local dispatcher", func() ([]byte, error) {
					return (&localDispatcherAdapter{registry: reg}).DispatchBytes(ctx, h.Name(), nil)
				}},
				{"the far caller", func() ([]byte, error) { return far.Call(ctx, "test", h.Name(), nil) }},
				{"dispatch.LocalCaller", func() ([]byte, error) {
					return dispatch.NewLocalCaller(reg).Call(ctx, "test", h.Name(), nil)
				}},
			}
			var own string
			for _, s := range surfaces {
				out, err, stopped := callRecovering(s.call)
				if stopped != nil {
					t.Errorf("%s stopped the process: %v", s.name, stopped)
					continue
				}
				if tc.kind == "success" {
					if err != nil || string(out) != tc.words {
						t.Errorf("%s = (%q, %v); want the payload %q", s.name, out, err, tc.words)
					}
					continue
				}
				if err == nil {
					t.Errorf("%s = (%q, nil); want a failure", s.name, out)
					continue
				}
				if got := outcomeKind(err); got != tc.kind {
					t.Errorf("%s returned %v, a failure of kind %s; want %s", s.name, err, got, tc.kind)
				}
				if !strings.Contains(err.Error(), tc.words) {
					t.Errorf("%s returned %q; want the words %q", s.name, err, tc.words)
				}
				if own == "" {
					own = err.Error()
				} else if !strings.Contains(err.Error(), own) {
					t.Errorf("%s returned %q; want the local dispatcher's words %q in it", s.name, err, own)
				}
			}
		})
	}
}

// A compose function served by an RPC handler reads the same dispatch result
// into its own kinds: a success is a result with the payload, each failure is
// a failure under its words, and a handler that answered nothing executed with
// nothing to return.
func TestComposeInvokeReadsAnRPCAnswerAsEveryCallerDoes(t *testing.T) {
	for _, tc := range outcomeCases() {
		t.Run(tc.name, func(t *testing.T) {
			reg := handlers.NewHandlerRegistry()
			h := &outcomeHandler{answer: tc.answer}
			if err := reg.RegisterRPC(h); err != nil {
				t.Fatalf("RegisterRPC: %v", err)
			}
			runtimeCtx, cancel := context.WithCancel(context.Background())
			t.Cleanup(cancel)
			rt := &Runtime{ctx: runtimeCtx, cancel: cancel, rpcServer: NewRPCServer(reg)}
			rt.rpcServer.SetAuthValidator(&composeAuthValidator{})

			got := rt.ComposeInvoke(context.Background(), compose.FunctionID(h.Name()), nil)
			switch {
			case tc.kind == "success":
				if got.Kind != compose.ResultSuccess || string(got.Payload) != tc.words {
					t.Fatalf("ComposeInvoke = %+v; want a result with the payload %q", got, tc.words)
				}
			case tc.name == "no response and no error":
				if got.Kind != compose.ResultNoResult {
					t.Fatalf("ComposeInvoke = %+v; want executed with nothing to return", got)
				}
			default:
				if got.Kind != compose.ResultFailure || !strings.Contains(got.Err, tc.words) {
					t.Fatalf("ComposeInvoke = %+v; want a failure carrying %q", got, tc.words)
				}
			}
		})
	}
}
