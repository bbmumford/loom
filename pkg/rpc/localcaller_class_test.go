/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package rpc

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/bbmumford/loom/node/handlers"
	"github.com/bbmumford/loom/pkg/dispatch"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// A process that serves its calls through a LocalCaller, as an endpoint does
// during failover, refuses a call naming an operation class the grade gate
// does not declare before the handler runs, as the mesh caller does. Whether
// the handler is reached across the mesh or in this process must not decide
// whether an undeclared class runs.
func TestLocalCallerRefusesAnUndeclaredClassBeforeTheHandler(t *testing.T) {
	var runs atomic.Int32
	h := &Handler{
		Namespace: "test",
		Domain:    "local",
		Operation: "Echo",
		Func: func(ctx context.Context, req proto.Message) (proto.Message, error) {
			runs.Add(1)
			return req, nil
		},
		Request:  (*wrapperspb.StringValue)(nil),
		Response: (*wrapperspb.StringValue)(nil),
		Scope:    ScopeNone,
	}
	source := NewRegistry()
	if err := source.RegisterAll([]Handler{*h}); err != nil {
		t.Fatalf("RegisterAll: %v", err)
	}
	target := handlers.NewHandlerRegistry()
	if err := source.ExportToHandlerRegistry(target); err != nil {
		t.Fatalf("ExportToHandlerRegistry: %v", err)
	}
	caller := dispatch.NewLocalCaller(target)
	payload, err := proto.Marshal(&wrapperspb.StringValue{Value: "hello"})
	if err != nil {
		t.Fatalf("proto.Marshal: %v", err)
	}

	_, err = caller.Call(dispatch.WithOpClass(context.Background(), "turbo"), "test", h.FQN(), payload)
	var gte *dispatch.GradeTooLowError
	if !errors.Is(err, dispatch.ErrGradeTooLow) || !errors.As(err, &gte) || !gte.UnknownClass || gte.Class != "turbo" {
		t.Fatalf("LocalCaller.Call with an undeclared class returned %v; want the typed refusal of the class", err)
	}
	if n := runs.Load(); n != 0 {
		t.Fatalf("the handler ran %d time(s) for an undeclared class", n)
	}

	for _, ctx := range []context.Context{context.Background(), dispatch.WithOpClass(context.Background(), "critical")} {
		if _, err := caller.Call(ctx, "test", h.FQN(), payload); err != nil {
			t.Fatalf("LocalCaller.Call with a declared class: %v", err)
		}
	}
	if n := runs.Load(); n != 2 {
		t.Fatalf("the handler ran %d time(s) for two declared-class calls, want 2", n)
	}
}
