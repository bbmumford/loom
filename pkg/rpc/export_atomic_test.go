/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package rpc

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/bbmumford/loom/node/handlers"
)

// heldName is a mesh handler registered on a handler registry by something other
// than an rpc.Registry export, under a name an export also carries.
type heldName struct{ name string }

func (h heldName) Name() string                { return h.name }
func (h heldName) Role() string                { return "test.export" }
func (h heldName) RequiresAuth() bool          { return false }
func (h heldName) AllowedAuthTypes() []string  { return nil }
func (h heldName) Scopes() []string            { return nil }
func (h heldName) Scope() handlers.Scope       { return handlers.ScopeNone }
func (h heldName) AllowedPartitions() []string { return nil }
func (h heldName) ExecuteRPC(context.Context, *handlers.RPCRequest) (*handlers.RPCResponse, error) {
	return &handlers.RPCResponse{Success: true}, nil
}

// exportFixture is a registry of n handlers, test.export.Op0 .. Op<n-1>, each with
// a Func.
func exportFixture(t *testing.T, n int) *Registry {
	t.Helper()
	reg := NewRegistry()
	for i := 0; i < n; i++ {
		if err := reg.Register(Handler{
			Namespace: "test", Domain: "export", Operation: fmt.Sprintf("Op%d", i),
			Func: func(context.Context, proto.Message) (proto.Message, error) {
				return &wrapperspb.StringValue{}, nil
			},
			Request:  (*wrapperspb.StringValue)(nil),
			Response: (*wrapperspb.StringValue)(nil),
			Scope:    ScopeNone,
		}); err != nil {
			t.Fatalf("Register: %v", err)
		}
	}
	return reg
}

// An export the handler registry refuses one name of leaves the handler registry
// as it was and names the refused operation. Repeated over fresh registries
// because the stored handlers are visited in map order.
func TestTheMeshExportIsAllOrNothing(t *testing.T) {
	for round := 0; round < 32; round++ {
		reg := exportFixture(t, 6)
		hr := handlers.NewHandlerRegistry()
		if err := hr.RegisterRPC(heldName{name: "test.export.Op3"}); err != nil {
			t.Fatalf("RegisterRPC: %v", err)
		}
		err := reg.ExportToHandlerRegistry(hr)
		if err == nil {
			t.Fatal("the export over a name the handler registry already holds was accepted")
		}
		if !strings.Contains(err.Error(), "test.export.Op3") {
			t.Errorf("the export's refusal %q does not name the operation held", err)
		}
		if n := hr.Count(); n != 1 {
			t.Fatalf("round %d: the refused export left %d handler(s) on the handler registry, want the 1 it held", round, n)
		}
	}
}

// Exporting a registry again into the same handler registry is a no-op, so the
// spine's export after an endpoint exported the same registry itself is accepted;
// another registry carrying one of the names is refused.
func TestTheMeshExportOfTheSameRegistryAgainIsANoOp(t *testing.T) {
	reg := exportFixture(t, 3)
	hr := handlers.NewHandlerRegistry()
	if err := reg.ExportToHandlerRegistry(hr); err != nil {
		t.Fatalf("first export: %v", err)
	}
	if err := reg.ExportToHandlerRegistry(hr); err != nil {
		t.Fatalf("the second export of the same registry was refused: %v", err)
	}
	if n := hr.Count(); n != 3 {
		t.Fatalf("the handler registry holds %d handlers after two exports, want 3", n)
	}
	if err := exportFixture(t, 1).ExportToHandlerRegistry(hr); err == nil {
		t.Fatal("another registry's export of test.export.Op0 was accepted beside this one's")
	}
}
