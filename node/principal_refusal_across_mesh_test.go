/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"bytes"
	"context"
	"errors"
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
	rpcscope "github.com/bbmumford/loom/pkg/rpc/scope"
	"github.com/bbmumford/loom/ports"
)

// These tests carry a call bearing a sealed principal across one hop to a real
// RPCServer, whose principal binding refuses it before any handler runs. The
// refusal reaches the origin's caller as rpc.ErrScopeDenied, and the origin's
// HTTP bridge proxying the operation answers it 403, as the far registry's own
// refusals of the caller do (denial_across_mesh_test.go).

// boundSession is a servedSession whose streams serve each request under the
// transport partition the session was accepted on, as the listener stamps it on
// a real session's context.
type boundSession struct {
	servedSession
	partition string
}

func (s *boundSession) OpenStream(_ context.Context, cfg aether.StreamConfig) (aether.Stream, error) {
	return &boundStream{
		servedStream: servedStream{hopStream: hopStream{id: cfg.StreamID}, server: s.server},
		partition:    s.partition,
	}, nil
}

type boundStream struct {
	servedStream
	partition string
}

func (st *boundStream) Send(ctx context.Context, data []byte) error {
	return st.servedStream.Send(handlers.WithTransportPartition(ctx, st.partition), data)
}

// answeringOp is the operation the far node serves: it declares no scope and
// answers every call it is reached with.
func answeringOp() rpc.Handler {
	return rpc.Handler{
		Namespace: "test", Domain: "principal", Operation: "Op",
		Func: func(context.Context, proto.Message) (proto.Message, error) {
			return &structpb.Struct{}, nil
		},
		Request:  (*structpb.Struct)(nil),
		Response: (*structpb.Struct)(nil),
		Scope:    rpc.ScopeNone,
	}
}

// farBinding builds the origin's caller over one hop to a far node serving op
// under validator, on a session bound to partition ("" for a session bound to
// none).
func farBinding(t *testing.T, op rpc.Handler, validator ports.AuthValidator, partition string) *dispatch.HWPCaller {
	t.Helper()
	source := rpc.NewRegistry()
	if err := source.Register(op); err != nil {
		t.Fatalf("Register: %v", err)
	}
	exported := handlers.NewHandlerRegistry()
	if err := source.ExportToHandlerRegistry(exported); err != nil {
		t.Fatalf("ExportToHandlerRegistry: %v", err)
	}
	far := NewRPCServer(exported)
	far.SetAuthValidator(validator)
	var session aether.Session = &servedSession{hopSession: hopSession{remote: "far", proto: aether.ProtoQUIC}, server: far}
	if partition != "" {
		session = &boundSession{
			servedSession: servedSession{hopSession: hopSession{remote: "far", proto: aether.ProtoQUIC}, server: far},
			partition:     partition,
		}
	}
	caller := dispatch.NewHWPCaller(&oneHopFinder{session: session})
	t.Cleanup(caller.Close)
	return caller
}

// sealedAs is a context carrying a principal of subject pinned to transport.
func sealedAs(t *testing.T, transport, subject string) context.Context {
	t.Helper()
	p, err := rpcscope.NewAuthenticatedPrincipal(rpcscope.NewScopeIdentity(transport, transport, subject), nil)
	if err != nil {
		t.Fatalf("principal: %v", err)
	}
	return rpcscope.WithAuthenticatedPrincipal(context.Background(), p)
}

// A principal the far node's binding refuses reaches the origin as
// rpc.ErrScopeDenied, under the far node's words, and stays the far node's
// answer (dispatch.ErrHandlerError), whichever of the binding's refusals made
// it: the consumer's admitter refusing the principal over its transport, a
// session bound to no partition, or a validator with no admitter. The first
// leg is the control: the same principal over the transport it is pinned to is
// admitted and answered.
func TestAFarNodesRefusalOfTheCallersPrincipalReachesTheOriginAsScopeDenied(t *testing.T) {
	op := answeringOp()
	t.Run("control: an admitted principal is answered", func(t *testing.T) {
		caller := farBinding(t, op, typedPrincipalValidator{}, "orbtr")
		if _, err := caller.Call(sealedAs(t, "orbtr", "svc"), "test", op.FQN(), nil); err != nil {
			t.Fatalf("the admitted principal's call: %v", err)
		}
	})
	for _, tc := range []struct {
		name      string
		validator ports.AuthValidator
		partition string
		words     string
	}{
		{"the admitter refuses it over the transport", typedPrincipalValidator{}, "p-other", "does not match the transport"},
		{"the session is bound to no partition", typedPrincipalValidator{}, "", "requires a partition-bound transport"},
		{"the validator has no admitter", noAdmitterValidator{}, "orbtr", "requires a consumer principal admitter"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			caller := farBinding(t, op, tc.validator, tc.partition)
			_, err := caller.Call(sealedAs(t, "orbtr", "svc"), "test", op.FQN(), nil)
			if err == nil {
				t.Fatal("the call succeeded; the far node's binding refused the principal")
			}
			if !errors.Is(err, rpc.ErrScopeDenied) {
				t.Fatalf("the origin got %v; a refused principal must reach it as rpc.ErrScopeDenied", err)
			}
			if !errors.Is(err, dispatch.ErrHandlerError) {
				t.Errorf("the origin got %v, which is not the far node's answer", err)
			}
			if !strings.Contains(err.Error(), "authenticated principal denied") || !strings.Contains(err.Error(), tc.words) {
				t.Errorf("the error text %q lost the far node's words %q", err, tc.words)
			}
		})
	}
}

// A principal the far node's binding refuses, on a call the origin's HTTP
// bridge proxies over the mesh, answers 403 with the far node's words: the
// bridge's caller sealed the principal, as an endpoint's identity chain seals
// the bearer it proved, and the far node refused it over the transport it
// arrived on.
func TestAFarRefusalOfTheCallersPrincipalProxiedByTheBridgeAnswers403(t *testing.T) {
	op := answeringOp()
	caller := farBinding(t, op, typedPrincipalValidator{}, "p-other")
	previous := dispatch.GetCaller()
	dispatch.SetCaller(caller)
	t.Cleanup(func() { dispatch.SetCaller(previous) })

	bridge := proxyFor(t, op)
	router := chi.NewRouter()
	router.Post("/rpc/{handler}", func(w http.ResponseWriter, r *http.Request) {
		sealed, _ := rpcscope.AuthenticatedPrincipalFromContext(sealedAs(t, "orbtr", "svc"))
		bridge.HandleRPC(w, r.WithContext(rpcscope.WithAuthenticatedPrincipal(r.Context(), sealed)))
	})
	server := httptest.NewServer(router)
	defer server.Close()
	resp, err := http.Post(server.URL+"/rpc/"+op.FQN(), "application/json", bytes.NewReader([]byte("{}")))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("the bridge answered %d %s; want 403", resp.StatusCode, body)
	}
	if !strings.Contains(string(body), "does not match the transport") {
		t.Errorf("the body %q lost the far node's words", body)
	}
}
