/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
 */

package rpc

import (
	"context"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/typepb"
)

// routed is an operation taking typepb.Field (string name, int32 number, bool
// packed, enum kind, repeated message options) declaring routes.
func routed(op string, routes ...HTTPRoute) Handler {
	return Handler{
		Namespace: "test", Domain: "routes", Operation: op,
		Func:    func(_ context.Context, req proto.Message) (proto.Message, error) { return req, nil },
		Request: (*typepb.Field)(nil), Response: (*typepb.Field)(nil),
		Scope: ScopeNone, Routes: routes,
	}
}

func TestARouteNoServerCouldServeIsRefused(t *testing.T) {
	cases := []struct {
		route HTTPRoute
		why   string
	}{
		{HTTPRoute{"FETCH", "/api/fields", ""}, "method"},
		{HTTPRoute{"POST", "/api/fields", "field"}, "body"},
		{HTTPRoute{"GET", "/api/fields", "*"}, "carries no body"},
		{HTTPRoute{"GET", "api/fields", ""}, "absolute path"},
		{HTTPRoute{"POST", "/rpc/test.routes.Op", "*"}, "outside /rpc"},
		{HTTPRoute{"GET", "/rpc", ""}, "outside /rpc"},
		{HTTPRoute{"GET", "/api//fields", ""}, "empty segment"},
		{HTTPRoute{"GET", "/api/fields/", ""}, "empty segment"},
		{HTTPRoute{"GET", "/api/fields/{nope}", ""}, "names no field"},
		{HTTPRoute{"GET", "/api/{name}/{name}", ""}, "twice"},
		{HTTPRoute{"GET", "/api/fields/x{name}", ""}, "one {field}"},
		{HTTPRoute{"GET", "/api/fields/*", ""}, "one {field}"},
		{HTTPRoute{"GET", "/api/fields/{options}", ""}, "singular scalar"},
	}
	for _, tc := range cases {
		err := NewRegistry().Register(routed("Op", tc.route))
		if err == nil || !strings.Contains(err.Error(), tc.why) {
			t.Errorf("route %+v: Register = %v, want it refused for %q", tc.route, err, tc.why)
		}
	}
	twice := routed("Op", HTTPRoute{"GET", "/api/fields/{name}", ""}, HTTPRoute{"GET", "/api/fields/{number}", ""})
	if err := NewRegistry().Register(twice); err == nil || !strings.Contains(err.Error(), "twice") {
		t.Errorf("one route declared twice: Register = %v, want it refused", err)
	}
}

func TestAServableRouteIsStored(t *testing.T) {
	reg := NewRegistry()
	h := routed("Op",
		HTTPRoute{"GET", "/api/fields/{name}", ""},
		HTTPRoute{"POST", "/api/fields/{jsonName}", "*"},
		HTTPRoute{"HEAD", "/api/fields", ""})
	if err := reg.Register(h); err != nil {
		t.Fatalf("Register: %v", err)
	}
	got, _ := reg.Get(h.FQN())
	if len(got.Routes) != 3 {
		t.Errorf("stored %d routes, want 3", len(got.Routes))
	}
}

func TestARouteAnotherOperationIsServedOnIsRefused(t *testing.T) {
	reg := NewRegistry()
	if err := reg.Register(routed("First", HTTPRoute{"GET", "/api/fields/{name}", ""})); err != nil {
		t.Fatalf("first: %v", err)
	}
	err := reg.Register(routed("Second", HTTPRoute{"GET", "/api/fields/{number}", ""}))
	if err == nil || !strings.Contains(err.Error(), "test.routes.First") {
		t.Errorf("a route differing only in its variable's name: Register = %v, want it refused naming First", err)
	}
	if err := reg.Register(routed("Third", HTTPRoute{"DELETE", "/api/fields/{name}", ""})); err != nil {
		t.Errorf("the same path under another method: %v, want it stored", err)
	}
	batch := []Handler{
		routed("Fourth", HTTPRoute{"PUT", "/api/fields/{name}", "*"}),
		routed("Fifth", HTTPRoute{"PUT", "/api/fields/{name}", "*"}),
	}
	if err := reg.RegisterAll(batch); err == nil || !strings.Contains(err.Error(), "test.routes.Fourth") {
		t.Errorf("two members of one batch on one route: RegisterAll = %v, want it refused", err)
	}
	if _, ok := reg.Get("test.routes.Fourth"); ok {
		t.Error("a refused batch stored a member")
	}
	if err := reg.RegisterAll([]Handler{routed("Fourth", HTTPRoute{"PUT", "/api/fields/{name}", "*"})}); err != nil {
		t.Errorf("the route alone after the refused batch: %v, want it stored", err)
	}
}

func TestTheRegistryKeepsItsOwnCopyOfTheRoutes(t *testing.T) {
	reg := NewRegistry()
	h := routed("Op", HTTPRoute{"GET", "/api/fields/{name}", ""})
	if err := reg.Register(h); err != nil {
		t.Fatalf("Register: %v", err)
	}
	h.Routes[0].Pattern = "/api/elsewhere"
	got, _ := reg.Get(h.FQN())
	got.Routes[0].Method = "DELETE"
	again, _ := reg.Get(h.FQN())
	if again.Routes[0] != (HTTPRoute{"GET", "/api/fields/{name}", ""}) {
		t.Errorf("stored route = %+v, want the one registered", again.Routes[0])
	}
}

func TestADeclarationReaderMayWriteRoutesAndTheyAreChecked(t *testing.T) {
	useDeclarationReader(t, func(h *Handler) error {
		h.Routes = append(h.Routes, HTTPRoute{"GET", "/api/read/{" + h.Operation + "}", ""})
		return nil
	})
	reg := NewRegistry()
	if err := reg.Register(routed("name")); err != nil {
		t.Fatalf("a route the reader wrote naming a field: %v", err)
	}
	if got, _ := reg.Get("test.routes.name"); len(got.Routes) != 1 {
		t.Errorf("stored %d routes, want the reader's one", len(got.Routes))
	}
	if err := reg.Register(routed("unknown")); err == nil || !strings.Contains(err.Error(), "names no field") {
		t.Errorf("a route the reader wrote naming no field: %v, want it refused", err)
	}
}

func TestRouteVariablesAreThePatternsFieldsInOrder(t *testing.T) {
	got := RouteVariables("/api/{name}/x/{number}")
	if len(got) != 2 || got[0] != "name" || got[1] != "number" {
		t.Errorf("RouteVariables = %v, want [name number]", got)
	}
}
