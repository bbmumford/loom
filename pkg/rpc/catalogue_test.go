/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
 */

package rpc

import (
	"context"
	"slices"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

func catalogueOp(namespace, domain, operation string) Handler {
	return Handler{
		Namespace: namespace, Domain: domain, Operation: operation,
		Request: (*structpb.Struct)(nil), Response: (*structpb.Struct)(nil), Scope: ScopeNone,
	}
}

// A catalogue-only declaration puts its role on the registry's role list and
// nowhere else: Get, MetadataFor, All, ByRole and Dispatch do not see it, so no
// HTTP mount or dispatch reads it as a handler.
func TestACatalogueDeclarationIsARoleAndNoHandler(t *testing.T) {
	reg := NewRegistry()
	served := catalogueOp("test", "here", "Runs")
	served.Func = func(_ context.Context, req proto.Message) (proto.Message, error) { return req, nil }
	if err := reg.Register(served); err != nil {
		t.Fatal(err)
	}
	if err := reg.Catalogue().Declare([]Handler{catalogueOp("test", "elsewhere", "Runs"), catalogueOp("test", "elsewhere", "Lists")}); err != nil {
		t.Fatalf("Declare: %v", err)
	}
	if got := reg.Roles(); !slices.Equal(got, []string{"test.elsewhere", "test.here"}) {
		t.Errorf("Roles() = %v, want the served role and the listed one", got)
	}
	if got := reg.Catalogue().Roles(); !slices.Equal(got, []string{"test.elsewhere"}) {
		t.Errorf("Catalogue().Roles() = %v", got)
	}
	if !reg.Catalogue().Lists("test.elsewhere.Runs") || reg.Catalogue().Lists("test.here.Runs") {
		t.Error("Lists does not name exactly the listed operations")
	}
	if _, ok := reg.Get("test.elsewhere.Runs"); ok {
		t.Error("Get finds a listed operation")
	}
	if _, ok := reg.MetadataFor("test.elsewhere.Runs"); ok {
		t.Error("MetadataFor finds a listed operation")
	}
	if all := reg.All(); len(all) != 1 || all[0].FQN() != "test.here.Runs" {
		t.Errorf("All() = %d handlers, want the served one", len(all))
	}
	if got := reg.ByRole("test.elsewhere"); len(got) != 0 {
		t.Errorf("ByRole(test.elsewhere) = %d handlers", len(got))
	}
	if _, err := reg.Dispatch(context.Background(), "test.elsewhere.Runs", nil); err == nil {
		t.Error("Dispatch answered a listed operation")
	}
}

// Declare is all or nothing, and refuses an operation the registry serves, one
// already listed, one named twice in its batch and one with an empty part.
func TestCatalogueDeclareRefusesWhatItCannotList(t *testing.T) {
	reg := NewRegistry()
	served := catalogueOp("test", "here", "Runs")
	served.Func = func(_ context.Context, req proto.Message) (proto.Message, error) { return req, nil }
	if err := reg.Register(served); err != nil {
		t.Fatal(err)
	}
	if err := reg.Catalogue().Declare([]Handler{catalogueOp("test", "elsewhere", "Runs")}); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		name  string
		batch []Handler
		want  string
	}{
		{"an operation this registry serves", []Handler{catalogueOp("test", "other", "A"), catalogueOp("test", "here", "Runs")}, "serves it"},
		{"an operation already listed", []Handler{catalogueOp("test", "other", "A"), catalogueOp("test", "elsewhere", "Runs")}, "already listed"},
		{"an operation named twice", []Handler{catalogueOp("test", "other", "A"), catalogueOp("test", "other", "A")}, "twice"},
		{"an operation with no domain", []Handler{catalogueOp("test", "other", "A"), catalogueOp("test", "", "B")}, "names no"},
	} {
		err := reg.Catalogue().Declare(c.batch)
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: Declare = %v, want a refusal naming %q", c.name, err, c.want)
		}
		if reg.Catalogue().Lists("test.other.A") {
			t.Errorf("%s: a refused batch listed part of itself", c.name)
		}
	}
}
