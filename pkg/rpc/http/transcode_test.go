/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
 */

package rpchttp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/typepb"

	"github.com/bbmumford/loom/pkg/rpc"
)

// echoRoutes is an operation echoing its request back, taking req's type,
// declaring routes and running mw in front.
func echoRoutes(op string, req proto.Message, mw []rpc.Middleware, routes ...rpc.HTTPRoute) rpc.Handler {
	return rpc.Handler{
		Namespace: "test", Domain: "routes", Operation: op,
		Func:    func(_ context.Context, in proto.Message) (proto.Message, error) { return in, nil },
		Request: req, Response: req, Scope: rpc.ScopeNone,
		Middleware: mw, Routes: routes,
	}
}

// mounted is a router RegisterAllWithBridge mounted hs on.
func mounted(t *testing.T, hs ...rpc.Handler) chi.Router {
	t.Helper()
	reg := rpc.NewRegistry()
	if err := reg.RegisterAll(hs); err != nil {
		t.Fatalf("RegisterAll: %v", err)
	}
	router := chi.NewRouter()
	RegisterAllWithBridge(NewBridge(reg), router)
	return router
}

// send serves one request and returns its status and decoded JSON body.
func send(router http.Handler, method, target, body string) (int, map[string]any) {
	req := httptest.NewRequest(method, target, strings.NewReader(body))
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	out := map[string]any{}
	_ = json.Unmarshal(rec.Body.Bytes(), &out)
	return rec.Code, out
}

func TestARouteFillsTheRequestFromItsPathAndQuery(t *testing.T) {
	router := mounted(t, echoRoutes("Get", (*typepb.Field)(nil), nil, rpc.HTTPRoute{Method: "GET", Pattern: "/api/fields/{name}"}))
	code, out := send(router, "GET", "/api/fields/alpha?number=7&packed=true&kind=TYPE_STRING&jsonName=a", "")
	if code != http.StatusOK {
		t.Fatalf("GET = %d %v, want 200", code, out)
	}
	want := map[string]any{"name": "alpha", "number": float64(7), "packed": true, "kind": "TYPE_STRING", "jsonName": "a"}
	for k, v := range want {
		if out[k] != v {
			t.Errorf("%s = %v, want %v (response %v)", k, out[k], v, out)
		}
	}
	if code, out := send(router, "GET", "/api/fields/beta?kind=9", ""); code != http.StatusOK || out["kind"] != "TYPE_STRING" {
		t.Errorf("an enum by number = %d %v, want TYPE_STRING", code, out)
	}
	if code, out := send(router, "GET", "/api/fields/a%2Fb", ""); code != http.StatusOK || out["name"] != "a/b" {
		t.Errorf("an escaped path value = %d %v, want name a/b", code, out)
	}
}

func TestARouteTakingTheBodyFillsItAndThePathWins(t *testing.T) {
	router := mounted(t, echoRoutes("Put", (*typepb.Field)(nil), nil, rpc.HTTPRoute{Method: "PUT", Pattern: "/api/fields/{name}", Body: "*"}))
	code, out := send(router, "PUT", "/api/fields/beta", `{"name":"ignored","number":3}`)
	if code != http.StatusOK || out["name"] != "beta" || out["number"] != float64(3) {
		t.Errorf("PUT = %d %v, want name beta from the path and number 3 from the body", code, out)
	}
	if code, _ := send(router, "PUT", "/api/fields/beta", ``); code != http.StatusOK {
		t.Errorf("an empty body = %d, want 200 with only the path's field", code)
	}
}

func TestARepeatedQueryParameterFillsAList(t *testing.T) {
	router := mounted(t, echoRoutes("Type", (*typepb.Type)(nil), nil, rpc.HTTPRoute{Method: "GET", Pattern: "/api/types/{name}"}))
	code, out := send(router, "GET", "/api/types/t?oneofs=a&oneofs=b", "")
	list, _ := out["oneofs"].([]any)
	if code != http.StatusOK || len(list) != 2 || list[0] != "a" || list[1] != "b" {
		t.Errorf("GET = %d %v, want oneofs [a b]", code, out)
	}
}

func TestARouteRefusesWhatItsFieldsCannotTake(t *testing.T) {
	router := mounted(t,
		echoRoutes("Get", (*typepb.Field)(nil), nil, rpc.HTTPRoute{Method: "GET", Pattern: "/api/fields/{name}"}),
		echoRoutes("Post", (*typepb.Field)(nil), nil, rpc.HTTPRoute{Method: "POST", Pattern: "/api/fields", Body: "*"}),
		echoRoutes("Delete", (*typepb.Field)(nil), nil, rpc.HTTPRoute{Method: "DELETE", Pattern: "/api/fields/{name}"}),
	)
	cases := []struct{ method, target, body, why string }{
		{"GET", "/api/fields/a?unknown=1", "", "names no field"},
		{"GET", "/api/fields/a?name.x=1", "", "not a singular message field"},
		{"GET", "/api/fields/a?options.name=x", "", "not a singular message field"},
		{"GET", "/api/fields/a?packed=maybe", "", "true or false"},
		{"GET", "/api/fields/a?number=1&number=2", "", "takes one value"},
		{"GET", "/api/fields/a?options=x", "", ""},
		{"DELETE", "/api/fields/a", `{"number":1}`, "takes no body"},
		{"POST", "/api/fields?number=1", `{}`, "reads no query string"},
		{"POST", "/api/fields", `[1]`, "JSON object"},
		{"POST", "/api/fields", `null`, "JSON object"},
		{"POST", "/api/fields", `{"bogus":1}`, "invalid request"},
	}
	for _, tc := range cases {
		req := httptest.NewRequest(tc.method, tc.target, strings.NewReader(tc.body))
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest || !strings.Contains(rec.Body.String(), tc.why) {
			t.Errorf("%s %s %s = %d %q, want 400 naming %q", tc.method, tc.target, tc.body, rec.Code, rec.Body.String(), tc.why)
		}
	}
}

func TestARouteRunsTheOperationsMiddleware(t *testing.T) {
	refuse := func(http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusTeapot) })
	}
	router := mounted(t, echoRoutes("Get", (*typepb.Field)(nil), []rpc.Middleware{refuse}, rpc.HTTPRoute{Method: "GET", Pattern: "/api/fields/{name}"}))
	if code, _ := send(router, "GET", "/api/fields/a", ""); code != http.StatusTeapot {
		t.Errorf("GET = %d, want the Middleware's 418", code)
	}
	if code, _ := send(router, "POST", "/rpc/test.routes.Get", `{}`); code != http.StatusTeapot {
		t.Errorf("/rpc = %d, want the same Middleware's 418", code)
	}
}

func TestAMeshOnlyOperationsRoutesAreMountedNowhere(t *testing.T) {
	h := echoRoutes("Get", (*typepb.Field)(nil), nil, rpc.HTTPRoute{Method: "GET", Pattern: "/api/fields/{name}"})
	h.Exposure = rpc.ExposureMeshOnly
	router := mounted(t, h)
	if code, _ := send(router, "GET", "/api/fields/a", ""); code != http.StatusNotFound {
		t.Errorf("GET = %d, want 404 for a mesh-only operation's route", code)
	}
}

func TestARouteTheRouterAlreadyServesPanics(t *testing.T) {
	reg := rpc.NewRegistry()
	if err := reg.Register(echoRoutes("Get", (*typepb.Field)(nil), nil, rpc.HTTPRoute{Method: "GET", Pattern: "/api/fields/{name}"})); err != nil {
		t.Fatalf("Register: %v", err)
	}
	router := chi.NewRouter()
	router.Get("/api/fields/{other}", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusAccepted) })
	defer func() {
		r := recover()
		if r == nil || !strings.Contains(r.(string), "already served") {
			t.Fatalf("recover = %v, want a panic naming the served route", r)
		}
		if code, _ := send(router, "GET", "/api/fields/a", ""); code != http.StatusAccepted {
			t.Errorf("the router's own route = %d after the refusal, want it untouched", code)
		}
	}()
	RegisterAllWithBridge(NewBridge(reg), router)
}

func TestAPathVariableReplacesTheBodysValueUnderEitherName(t *testing.T) {
	router := mounted(t, echoRoutes("Put", (*typepb.Field)(nil), nil, rpc.HTTPRoute{Method: "PUT", Pattern: "/api/fields/{json_name}", Body: "*"}))
	code, out := send(router, "PUT", "/api/fields/from-path", `{"json_name":"from-body","number":4}`)
	if code != http.StatusOK || out["jsonName"] != "from-path" || out["number"] != float64(4) {
		t.Errorf("PUT = %d %v, want jsonName from the path over the body's json_name, number 4", code, out)
	}
}

func TestADottedQueryParameterFillsAFieldOfAMessageField(t *testing.T) {
	router := mounted(t, echoRoutes("Type", (*typepb.Type)(nil), nil, rpc.HTTPRoute{Method: "GET", Pattern: "/api/types/{name}"}))
	code, out := send(router, "GET", "/api/types/t?sourceContext.fileName=a.proto&edition=e", "")
	inner, _ := out["sourceContext"].(map[string]any)
	if code != http.StatusOK || inner["fileName"] != "a.proto" || out["edition"] != "e" {
		t.Errorf("GET = %d %v, want sourceContext.fileName a.proto and edition e", code, out)
	}
	if code, out := send(router, "GET", "/api/types/t?source_context.file_name=b.proto", ""); code != http.StatusOK || out["sourceContext"].(map[string]any)["fileName"] != "b.proto" {
		t.Errorf("GET by proto names = %d %v, want sourceContext.fileName b.proto", code, out)
	}
	if code, _ := send(router, "GET", "/api/types/t?options.name=x", ""); code != http.StatusBadRequest {
		t.Errorf("a dotted name through a list of messages = %d, want 400", code)
	}
}
