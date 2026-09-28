/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package rpc

import (
	"context"
	"errors"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/bbmumford/loom/node/handlers"
	rpcscope "github.com/bbmumford/loom/pkg/rpc/scope"
)

// TestRegistryShimImplementsTaskHandler pins the contract that the shim
// installed by ExportToHandlerRegistry satisfies handlers.TaskHandler as
// well as handlers.RPCHandler. Mesh execution.Executor type-asserts
// GetMeta result to TaskHandler before invoking; without this the mesh
// task path drops rpc.Registry-registered handlers with
// "handler does not support task execution".
func TestRegistryShimImplementsTaskHandler(t *testing.T) {
	shim := newRegistryShim(&Handler{
		Namespace: "hstles",
		Domain:    "test",
		Operation: "Echo",
		Func: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			return req, nil
		},
		Request:  (*wrapperspb.StringValue)(nil),
		Response: (*wrapperspb.StringValue)(nil),
		Scope:    ScopeNone,
	})

	if _, ok := interface{}(shim).(handlers.RPCHandler); !ok {
		t.Errorf("shim does not implement handlers.RPCHandler")
	}
	if _, ok := interface{}(shim).(handlers.TaskHandler); !ok {
		t.Errorf("shim does not implement handlers.TaskHandler")
	}
}

// TestRegistryShimForwardsTheRequirement: the shim that carries an
// rpc.Registry handler onto the mesh registry reports the handler's declared
// Requirement (the mesh registry judges it through its authorization policy),
// and reports nil for a handler that declares none.
func TestRegistryShimForwardsTheRequirement(t *testing.T) {
	capability := rpcscope.Capability("orbtr.ai:inference.dispatch")
	declaring := newRegistryShim(&Handler{
		Namespace: "orbtr.ai", Domain: "inference", Operation: "Dispatch",
		Request:     (*wrapperspb.StringValue)(nil),
		Response:    (*wrapperspb.StringValue)(nil),
		Scope:       ScopeAllowlisted,
		Requirement: &capability,
	})
	var meta handlers.HandlerMeta = declaring
	declarer, ok := meta.(handlers.RequirementDeclarer)
	if !ok {
		t.Fatal("registry shim does not implement handlers.RequirementDeclarer")
	}
	if got := declarer.Requirement(); got == nil || *got != capability {
		t.Fatalf("forwarded requirement = %v, want %v", got, capability)
	}

	plain := newRegistryShim(&Handler{
		Namespace: "orbtr.ai", Domain: "inference", Operation: "Status",
		Request:  (*wrapperspb.StringValue)(nil),
		Response: (*wrapperspb.StringValue)(nil),
		Scope:    ScopeNone,
	})
	if got := plain.Requirement(); got != nil {
		t.Fatalf("handler with no requirement forwarded %v, want nil", got)
	}
}

// TestRegistryShimExecuteRPCProtoPayload verifies the shim decodes
// proto-binary payload via proto.Unmarshal — the mesh RPC path's wire
// format. Uses wrapperspb.StringValue as a stable exemplar so the test
// is not tied to any domain proto.
func TestRegistryShimExecuteRPCProtoPayload(t *testing.T) {
	shim := newRegistryShim(&Handler{
		Namespace: "hstles",
		Domain:    "test",
		Operation: "Echo",
		Func: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			return req, nil
		},
		Request:  (*wrapperspb.StringValue)(nil),
		Response: (*wrapperspb.StringValue)(nil),
		Scope:    ScopeNone,
	})

	payload, err := proto.Marshal(&wrapperspb.StringValue{Value: "hello-proto"})
	if err != nil {
		t.Fatalf("proto.Marshal: %v", err)
	}

	resp, err := shim.ExecuteRPC(context.Background(), &handlers.RPCRequest{Payload: payload})
	if err != nil {
		t.Fatalf("ExecuteRPC: %v", err)
	}
	if !resp.Success {
		t.Fatalf("resp.Success=false err=%q", resp.Error)
	}
	var got wrapperspb.StringValue
	if err := proto.Unmarshal(resp.Payload, &got); err != nil {
		t.Fatalf("proto.Unmarshal response: %v", err)
	}
	if got.Value != "hello-proto" {
		t.Errorf("Value = %q, want hello-proto", got.Value)
	}
}

// TestRegistryShimExecuteRPCJSONPayload verifies the shim decodes
// protojson payload via protojson.Unmarshal — the mesh task path's wire
// format (Task.Payload is json.RawMessage so scanners marshal handler
// requests with protojson). This is the migration-critical branch: the
// pre-migration ad-hoc BaseHandler used encoding/json which silently
// dropped fields whose proto json tag was snake_case while the payload
// used camelCase; protojson round-trips lowerCamelCase correctly.
func TestRegistryShimExecuteRPCJSONPayload(t *testing.T) {
	shim := newRegistryShim(&Handler{
		Namespace: "hstles",
		Domain:    "test",
		Operation: "Echo",
		Func: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			return req, nil
		},
		Request:  (*wrapperspb.StringValue)(nil),
		Response: (*wrapperspb.StringValue)(nil),
		Scope:    ScopeNone,
	})

	payload, err := protojson.Marshal(&wrapperspb.StringValue{Value: "hello-json"})
	if err != nil {
		t.Fatalf("protojson.Marshal: %v", err)
	}

	resp, err := shim.ExecuteRPC(context.Background(), &handlers.RPCRequest{Payload: payload})
	if err != nil {
		t.Fatalf("ExecuteRPC: %v", err)
	}
	if !resp.Success {
		t.Fatalf("resp.Success=false err=%q payload=%q", resp.Error, string(payload))
	}
	var got wrapperspb.StringValue
	if err := proto.Unmarshal(resp.Payload, &got); err != nil {
		t.Fatalf("proto.Unmarshal response: %v", err)
	}
	if got.Value != "hello-json" {
		t.Errorf("Value = %q, want hello-json", got.Value)
	}
}

// TestRegistryShimExecuteTaskDelegates verifies the task-path wrapper
// hands the payload to ExecuteRPC and lifts the result into a
// handlers.TaskResult with TaskStatusCompleted. Uses a protojson payload
// to mirror the mesh task executor's actual wire encoding.
func TestRegistryShimExecuteTaskDelegates(t *testing.T) {
	shim := newRegistryShim(&Handler{
		Namespace: "hstles",
		Domain:    "test",
		Operation: "Echo",
		Func: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			return req, nil
		},
		Request:  (*wrapperspb.StringValue)(nil),
		Response: (*wrapperspb.StringValue)(nil),
		Scope:    ScopeNone,
	})

	payload, err := protojson.Marshal(&wrapperspb.StringValue{Value: "hello-task"})
	if err != nil {
		t.Fatalf("protojson.Marshal: %v", err)
	}

	result, err := shim.ExecuteTask(context.Background(), &handlers.Task{
		ID:      "task-1",
		Handler: "hstles.test.Echo",
		Payload: payload,
	})
	if err != nil {
		t.Fatalf("ExecuteTask: %v", err)
	}
	if result.Status != handlers.TaskStatusCompleted {
		t.Errorf("Status = %q, want %q; err=%q", result.Status, handlers.TaskStatusCompleted, result.Error)
	}
	if result.TaskID != "task-1" {
		t.Errorf("TaskID = %q, want task-1", result.TaskID)
	}
	var got wrapperspb.StringValue
	if err := proto.Unmarshal(result.Payload, &got); err != nil {
		t.Fatalf("proto.Unmarshal result payload: %v", err)
	}
	if got.Value != "hello-task" {
		t.Errorf("Value = %q, want hello-task", got.Value)
	}
}

// TestRegistryShimRefusesAHandlerWithNoFunc: a shim runs its handler while the
// handler carries a Func, and once the Func is cleared on the stored handler
// it answers ExecuteRPC with ErrProxyDispatchLocal and ExecuteTask with a
// failed result, without calling the nil Func.
func TestRegistryShimRefusesAHandlerWithNoFunc(t *testing.T) {
	h := &Handler{
		Namespace: "test",
		Domain:    "shim",
		Operation: "Echo",
		Func: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			return req, nil
		},
		Request:  (*wrapperspb.StringValue)(nil),
		Response: (*wrapperspb.StringValue)(nil),
		Scope:    ScopeNone,
	}
	shim := newRegistryShim(h)
	payload, err := proto.Marshal(&wrapperspb.StringValue{Value: "hello"})
	if err != nil {
		t.Fatalf("proto.Marshal: %v", err)
	}

	resp, err := shim.ExecuteRPC(context.Background(), &handlers.RPCRequest{Payload: payload})
	if err != nil || resp == nil || !resp.Success {
		t.Fatalf("ExecuteRPC with a Func = (%+v, %v), want success", resp, err)
	}

	h.Func = nil
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("the shim called a nil Func: %v", r)
		}
	}()
	resp, err = shim.ExecuteRPC(context.Background(), &handlers.RPCRequest{Payload: payload})
	if !errors.Is(err, ErrProxyDispatchLocal) {
		t.Errorf("ExecuteRPC with no Func: error = %v, want %v", err, ErrProxyDispatchLocal)
	}
	if resp != nil && resp.Success {
		t.Errorf("ExecuteRPC with no Func reported success")
	}
	result, err := shim.ExecuteTask(context.Background(), &handlers.Task{
		ID: "task-1", Handler: h.FQN(), Payload: payload,
	})
	if err != nil {
		t.Fatalf("ExecuteTask with no Func: %v", err)
	}
	if result.Status != handlers.TaskStatusFailed || !strings.Contains(result.Error, ErrProxyDispatchLocal.Error()) {
		t.Errorf("ExecuteTask with no Func = (%q, %q), want %q naming %v",
			result.Status, result.Error, handlers.TaskStatusFailed, ErrProxyDispatchLocal)
	}
}

// TestFirstNonWhitespace pins the payload-format detection heuristic
// used by unmarshalPayload — protojson payloads begin with '{' after
// optional leading whitespace; proto-binary payloads do not.
func TestFirstNonWhitespace(t *testing.T) {
	cases := []struct {
		in   []byte
		want byte
	}{
		{[]byte(`{"value":"x"}`), '{'},
		{[]byte("  \t\n{"), '{'},
		{[]byte("\x08\x01"), 0x08},
		{[]byte(""), 0},
		{[]byte("   \t\n"), 0},
	}
	for _, c := range cases {
		got := firstNonWhitespace(c.in)
		if got != c.want {
			t.Errorf("firstNonWhitespace(%q) = %#x, want %#x", c.in, got, c.want)
		}
	}
}
