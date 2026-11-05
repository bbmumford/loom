/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ORBTR/aether"
	"github.com/go-chi/chi/v5"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/bbmumford/loom/node/handlers"
	"github.com/bbmumford/loom/pkg/dispatch"
	"github.com/bbmumford/loom/pkg/rpc"
	rpchttp "github.com/bbmumford/loom/pkg/rpc/http"
	rpcscope "github.com/bbmumford/loom/pkg/rpc/scope"
)

// These tests carry a handler's refusal of its caller's authority across one
// hop: the far node runs a handler exported from an rpc.Registry, as a domain
// host is served, and the handler, or the far registry's own scope gate,
// refuses with an error wrapping scope.ErrDenied. The refusal reaches the
// origin's caller as rpc.ErrScopeDenied, and the origin's HTTP bridge proxying
// the operation answers it 403, as it answers the same refusal made in its own
// process.

// errHostRefused stands for a domain host's refusal of the caller's authority,
// which the host wraps around scope.ErrDenied.
var errHostRefused = fmt.Errorf("%w: host: the caller holds no grant for this operation", rpcscope.ErrDenied)

// deniedOp is the operation the far node serves: it answers every call with
// answer, the error its Func returns.
func deniedOp(scope rpc.ScopeTag, answer error) rpc.Handler {
	return rpc.Handler{
		Namespace: "test", Domain: "denial", Operation: "Op",
		Func: func(context.Context, proto.Message) (proto.Message, error) {
			return nil, answer
		},
		Request:  (*structpb.Struct)(nil),
		Response: (*structpb.Struct)(nil),
		Scope:    scope,
	}
}

// farServing builds the origin's caller over one hop to a far node whose
// registry holds h, exported from an rpc.Registry.
func farServing(t *testing.T, h rpc.Handler) *dispatch.HWPCaller {
	t.Helper()
	source := rpc.NewRegistry()
	if err := source.Register(h); err != nil {
		t.Fatalf("Register: %v", err)
	}
	exported := handlers.NewHandlerRegistry()
	if err := source.ExportToHandlerRegistry(exported); err != nil {
		t.Fatalf("ExportToHandlerRegistry: %v", err)
	}
	caller := dispatch.NewHWPCaller(&oneHopFinder{session: &servedSession{
		hopSession: hopSession{remote: "far", proto: aether.ProtoQUIC},
		server:     NewRPCServer(exported),
	}})
	t.Cleanup(caller.Close)
	return caller
}

// The far handler's refusal of its caller reaches the origin as
// rpc.ErrScopeDenied, under the far handler's own words, and stays the
// handler's answer (dispatch.ErrHandlerError), so nothing at the origin reads
// it as a transport failure. A refusal made by the far registry's own scope
// gate reaches it the same way. A handler's other errors are not refusals.
func TestAFarHandlersRefusalOfItsCallerReachesTheOriginAsScopeDenied(t *testing.T) {
	for _, tc := range []struct {
		name   string
		op     rpc.Handler
		denied bool
		words  string
	}{
		{"the handler refuses", deniedOp(rpc.ScopeNone, errHostRefused), true, "holds no grant"},
		{"the far registry's scope gate refuses", deniedOp(rpc.ScopePartition, errors.New("unreached")), true, "requires a presence policy"},
		{"the handler fails otherwise", deniedOp(rpc.ScopeNone, errors.New("host: the record is not found")), false, "not found"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			caller := farServing(t, tc.op)
			_, err := caller.Call(context.Background(), "test", tc.op.FQN(), nil)
			if err == nil {
				t.Fatal("the call succeeded; the far node refused it")
			}
			if got := errors.Is(err, rpc.ErrScopeDenied); got != tc.denied {
				t.Fatalf("errors.Is(%v, rpc.ErrScopeDenied) = %v, want %v", err, got, tc.denied)
			}
			if !errors.Is(err, dispatch.ErrHandlerError) {
				t.Errorf("the origin got %v, which is not the handler's answer", err)
			}
			if !strings.Contains(err.Error(), tc.words) {
				t.Errorf("the error text %q lost the far node's words %q", err, tc.words)
			}
		})
	}
}

// postThroughBridge mounts bridge as an endpoint mounts its /rpc/{handler}
// proxy and posts an empty request to fqn, returning the status and body.
func postThroughBridge(t *testing.T, bridge *rpchttp.Bridge, fqn string) (int, string) {
	t.Helper()
	router := chi.NewRouter()
	router.Post("/rpc/{handler}", bridge.HandleRPC)
	server := httptest.NewServer(router)
	defer server.Close()
	resp, err := http.Post(server.URL+"/rpc/"+fqn, "application/json", bytes.NewReader([]byte("{}")))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

// proxyFor is the gateway's registry for op: op's declaration with no Func,
// so the bridge forwards the call over the mesh.
func proxyFor(t *testing.T, op rpc.Handler) *rpchttp.Bridge {
	t.Helper()
	reg := rpc.NewRegistry()
	if err := reg.RegisterProxy([]rpc.Handler{op}); err != nil {
		t.Fatalf("RegisterProxy: %v", err)
	}
	return rpchttp.NewBridge(reg)
}

// A far handler's refusal of its caller, proxied by the origin's HTTP bridge
// over the mesh, answers 403 with the handler's words; its other errors keep
// answering 400. A client of the bridge reads 403 alone as a refusal.
func TestAFarRefusalProxiedByTheBridgeAnswers403(t *testing.T) {
	for _, tc := range []struct {
		name   string
		answer error
		status int
		words  string
	}{
		{"a refusal of the caller", errHostRefused, http.StatusForbidden, "holds no grant"},
		{"another handler error", errors.New("host: the record is not found"), http.StatusBadRequest, "not found"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			op := deniedOp(rpc.ScopeNone, tc.answer)
			caller := farServing(t, op)
			previous := dispatch.GetCaller()
			dispatch.SetCaller(caller)
			t.Cleanup(func() { dispatch.SetCaller(previous) })

			status, body := postThroughBridge(t, proxyFor(t, op), op.FQN())
			if status != tc.status {
				t.Fatalf("the bridge answered %d %s; want %d", status, body, tc.status)
			}
			if !strings.Contains(body, tc.words) {
				t.Errorf("the body %q lost the far handler's words %q", body, tc.words)
			}
		})
	}
}

// A refusal made by a handler this process hosts, which rpc.CallRaw reaches
// through the node's local dispatcher instead of the mesh, answers 403 at the
// bridge proxying it too, and stays a refusal for every caller of
// DispatchBytes.
func TestALocalHostsRefusalProxiedByTheBridgeAnswers403(t *testing.T) {
	op := deniedOp(rpc.ScopeNone, errHostRefused)
	source := rpc.NewRegistry()
	if err := source.Register(op); err != nil {
		t.Fatalf("Register: %v", err)
	}
	exported := handlers.NewHandlerRegistry()
	if err := source.ExportToHandlerRegistry(exported); err != nil {
		t.Fatalf("ExportToHandlerRegistry: %v", err)
	}
	adapter := &localDispatcherAdapter{registry: exported}

	_, err := adapter.DispatchBytes(context.Background(), op.FQN(), nil)
	if !errors.Is(err, rpc.ErrScopeDenied) || !strings.Contains(err.Error(), "holds no grant") {
		t.Fatalf("DispatchBytes returned %v; want the host's refusal as rpc.ErrScopeDenied", err)
	}

	previous := rpc.LocalDispatcherInstance()
	rpc.SetLocalDispatcher(adapter)
	t.Cleanup(func() { rpc.SetLocalDispatcher(previous) })
	status, body := postThroughBridge(t, proxyFor(t, op), op.FQN())
	if status != http.StatusForbidden {
		t.Fatalf("the bridge answered %d %s; want 403", status, body)
	}
}

// erringHandler is an RPC handler registered on the node's registry directly
// that returns err, not a failed response, for every call.
type erringHandler struct{ err error }

func (*erringHandler) Name() string                { return "test.denial.Raw" }
func (*erringHandler) Role() string                { return "test" }
func (*erringHandler) RequiresAuth() bool          { return false }
func (*erringHandler) AllowedAuthTypes() []string  { return nil }
func (*erringHandler) Scopes() []string            { return nil }
func (*erringHandler) Scope() handlers.Scope       { return handlers.ScopeNone }
func (*erringHandler) AllowedPartitions() []string { return nil }
func (h *erringHandler) ExecuteRPC(context.Context, *handlers.RPCRequest) (*handlers.RPCResponse, error) {
	return nil, h.err
}

// A refusal a handler returns as its error, which the far registry's Dispatch
// hands back as an error and not as a failed response, reaches the origin as
// rpc.ErrScopeDenied too.
func TestAFarHandlersReturnedRefusalReachesTheOriginAsScopeDenied(t *testing.T) {
	reg := handlers.NewHandlerRegistry()
	h := &erringHandler{err: errHostRefused}
	if err := reg.RegisterRPC(h); err != nil {
		t.Fatalf("RegisterRPC: %v", err)
	}
	caller := dispatch.NewHWPCaller(&oneHopFinder{session: &servedSession{
		hopSession: hopSession{remote: "far", proto: aether.ProtoQUIC},
		server:     NewRPCServer(reg),
	}})
	t.Cleanup(caller.Close)

	_, err := caller.Call(context.Background(), "test", h.Name(), nil)
	if !errors.Is(err, rpc.ErrScopeDenied) || !strings.Contains(err.Error(), "holds no grant") {
		t.Fatalf("the origin got %v; want the far handler's refusal as rpc.ErrScopeDenied", err)
	}
}
