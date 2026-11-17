/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package rpchttp

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/go-chi/chi/v5"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/bbmumford/loom/pkg/dispatch"
	"github.com/bbmumford/loom/pkg/rpc"
)

// countingCaller is the process's mesh caller for one test: it counts every call
// and answers an empty Struct.
type countingCaller struct{ calls atomic.Int64 }

func (c *countingCaller) Call(context.Context, string, string, []byte) ([]byte, error) {
	c.calls.Add(1)
	return proto.Marshal(&structpb.Struct{})
}
func (*countingCaller) Close() {}

// meshOnlyRegistry holds three operations under ScopeNone: test.meshonly.Proxied, a
// mesh-only declaration with no Func (as cortex's inference declarations are
// registered by a proxy); test.meshonly.Local, a mesh-only declaration with a Func;
// and test.bridged.Proxied, an ordinary proxy, the control proving the harness
// dials. Each carries a middleware counting the requests it sees.
func meshOnlyRegistry(t *testing.T, middleware *atomic.Int64, funcs *atomic.Int64) *rpc.Registry {
	t.Helper()
	count := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			middleware.Add(1)
			next.ServeHTTP(w, r)
		})
	}
	op := func(domain, operation string, exposure rpc.Exposure, local bool) rpc.Handler {
		h := rpc.Handler{
			Namespace: "test", Domain: domain, Operation: operation,
			Request: (*structpb.Struct)(nil), Response: (*structpb.Struct)(nil),
			Scope: rpc.ScopeNone, Middleware: []rpc.Middleware{count}, Exposure: exposure,
		}
		if local {
			h.Func = func(_ context.Context, req proto.Message) (proto.Message, error) {
				funcs.Add(1)
				return req, nil
			}
		}
		return h
	}
	reg := rpc.NewRegistry()
	reg.SetPresencePolicy(refPolicy("orbtr"))
	for _, h := range []rpc.Handler{
		op("meshonly", "Proxied", rpc.ExposureMeshOnly, false),
		op("meshonly", "Local", rpc.ExposureMeshOnly, true),
		op("bridged", "Proxied", rpc.ExposureMeshAndHTTP, false),
	} {
		if err := reg.Register(h); err != nil {
			t.Fatalf("register %s: %v", h.FQN(), err)
		}
	}
	return reg
}

func installCountingCaller(t *testing.T) *countingCaller {
	t.Helper()
	previous := dispatch.GetCaller()
	caller := &countingCaller{}
	if err := dispatch.SetCaller(caller); err != nil {
		t.Fatalf("SetCaller: %v", err)
	}
	t.Cleanup(func() { _ = dispatch.SetCaller(previous) })
	return caller
}

// A handler declaring rpc.ExposureMeshOnly is reachable from no HTTP request: a
// router RegisterAll mounted answers its path 404, Bridge.HandleRPC behind a
// wildcard route answers 404 as it answers a name the registry does not hold, and
// neither runs its middleware, its Func or the mesh caller; the directory neither
// counts nor lists it. The ordinary proxy on the same registry reaches the caller
// through both mounts, so the harness can dial.
func TestAMeshOnlyOperationIsAnsweredAsUnknownByEveryHTTPMountAndNeverDialled(t *testing.T) {
	caller := installCountingCaller(t)
	var middleware, funcs atomic.Int64
	reg := meshOnlyRegistry(t, &middleware, &funcs)

	perRoute := chi.NewRouter()
	RegisterAll(reg, perRoute)
	wildcard := chi.NewRouter()
	wildcard.Post("/rpc/{handler}", NewBridge(reg).HandleRPC)

	post := func(router http.Handler, fqn string) int {
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/rpc/"+fqn, bytes.NewReader([]byte("{}"))))
		return rec.Code
	}
	for _, mount := range []struct {
		name   string
		router http.Handler
	}{{"RegisterAll", perRoute}, {"HandleRPC", wildcard}} {
		for _, fqn := range []string{"test.meshonly.Proxied", "test.meshonly.Local"} {
			if code := post(mount.router, fqn); code != http.StatusNotFound {
				t.Errorf("%s: POST /rpc/%s answered %d, want 404", mount.name, fqn, code)
			}
		}
		if code := post(mount.router, "test.bridged.Proxied"); code != http.StatusOK {
			t.Errorf("%s: the ordinary proxy answered %d, want 200 from the mesh caller", mount.name, code)
		}
	}
	if got := caller.calls.Load(); got != 2 {
		t.Errorf("the mesh caller was called %d times, want 2 (the ordinary proxy, once per mount)", got)
	}
	if got := middleware.Load(); got != 2 {
		t.Errorf("middleware ran %d times, want 2 (the ordinary proxy's)", got)
	}
	if got := funcs.Load(); got != 0 {
		t.Errorf("the mesh-only Func ran %d times over HTTP", got)
	}

	bridge := NewBridge(reg)
	bridge.SetDirectoryAuth(func(*http.Request) bool { return true })
	rec := httptest.NewRecorder()
	bridge.HandleDirectory()(rec, httptest.NewRequest(http.MethodGet, "/rpc/_directory", nil))
	var listing DirectoryResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &listing); err != nil {
		t.Fatalf("directory: %v: %s", err, rec.Body.String())
	}
	if listing.Count != 1 || len(listing.Entries) != 1 || listing.Entries[0].FQN != "test.bridged.Proxied" || listing.Roles["test.meshonly"] != 0 {
		t.Errorf("directory lists %+v, want test.bridged.Proxied alone", listing)
	}
}
