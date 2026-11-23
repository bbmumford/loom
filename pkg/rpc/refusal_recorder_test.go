/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
 */

package rpc

import (
	"context"
	"errors"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"

	"github.com/bbmumford/loom/pkg/rpc/scope"
)

// countingRecorder records each refusal it is handed and answers fail.
type countingRecorder struct {
	got  []scope.Refusal
	fail error
}

func (c *countingRecorder) RecordRefusal(_ context.Context, r scope.Refusal) error {
	c.got = append(c.got, r)
	return c.fail
}

func withinPartitionHandler(calls *int) Handler {
	return Handler{
		Namespace: "test",
		Domain:    "refusal",
		Operation: "Read",
		Scope:     ScopeWithinPartition,
		Request:   (*structpb.Struct)(nil),
		Response:  (*structpb.Struct)(nil),
		Func: func(_ context.Context, req proto.Message) (proto.Message, error) {
			*calls++
			return req, nil
		},
	}
}

// Dispatch hands the registered recorder one refusal per refused call, naming
// the operation, the caller and the scope tag it asks, and none for a call it
// admits; the refusal still returns ErrScopeDenied.
func TestDispatchRecordsEachRefusalThroughTheRegisteredRecorder(t *testing.T) {
	var calls int
	h := withinPartitionHandler(&calls)
	reg := NewRegistry()
	reg.SetPresencePolicy(refPolicy())
	rec := &countingRecorder{}
	reg.SetRefusalRecorder(rec)
	if err := reg.Register(h); err != nil {
		t.Fatalf("Register: %v", err)
	}

	caller := idAxes("orbtr", "", "user-1")
	refused := WithAuthenticatedScopeIdentity(context.Background(), caller)
	if _, err := reg.Dispatch(refused, h.FQN(), nil); !errors.Is(err, ErrScopeDenied) {
		t.Fatalf("Dispatch = %v, want ErrScopeDenied", err)
	}
	if len(rec.got) != 1 || rec.got[0].Operation != h.FQN() || rec.got[0].Scope != ScopeWithinPartition ||
		rec.got[0].Requirement != nil || rec.got[0].Caller.Subject != "user-1" {
		t.Fatalf("recorded %+v, want one refusal of %s for user-1", rec.got, h.FQN())
	}

	admitted := WithAuthenticatedScopeIdentity(context.Background(), idAxes("orbtr", "org-1", "user-1"))
	if _, err := reg.Dispatch(admitted, h.FQN(), nil); err != nil {
		t.Fatalf("admitted Dispatch: %v", err)
	}
	if len(rec.got) != 1 || calls != 1 {
		t.Fatalf("an admitted call recorded a refusal (%d) or did not run (%d)", len(rec.got), calls)
	}
}

// A refusal the recorder cannot record still returns as the denial, joined with
// ErrRefusalUnrecorded, and the handler does not run.
func TestARefusalTheRecorderCannotRecordStaysADenial(t *testing.T) {
	var calls int
	h := withinPartitionHandler(&calls)
	reg := NewRegistry()
	reg.SetPresencePolicy(refPolicy())
	reg.SetRefusalRecorder(&countingRecorder{fail: errors.New("audit host down")})
	if err := reg.Register(h); err != nil {
		t.Fatalf("Register: %v", err)
	}
	ctx := WithAuthenticatedScopeIdentity(context.Background(), idAxes("orbtr", "", "user-1"))
	_, err := reg.Dispatch(ctx, h.FQN(), nil)
	if !errors.Is(err, ErrRefusalUnrecorded) || !errors.Is(err, ErrScopeDenied) || calls != 0 {
		t.Fatalf("Dispatch = %v (calls %d), want the denial joined with ErrRefusalUnrecorded", err, calls)
	}

	var typedNil *countingRecorder
	reg.SetRefusalRecorder(typedNil)
	if _, err := reg.Dispatch(ctx, h.FQN(), nil); !errors.Is(err, ErrRefusalUnrecorded) {
		t.Fatalf("typed-nil recorder: Dispatch = %v, want ErrRefusalUnrecorded", err)
	}
}

// With no recorder registered the refusal returns as it always has.
func TestARegistryWithNoRecorderReturnsTheRefusal(t *testing.T) {
	var calls int
	h := withinPartitionHandler(&calls)
	reg := NewRegistry()
	reg.SetPresencePolicy(refPolicy())
	if err := reg.Register(h); err != nil {
		t.Fatalf("Register: %v", err)
	}
	ctx := WithAuthenticatedScopeIdentity(context.Background(), idAxes("orbtr", "", "user-1"))
	if _, err := reg.Dispatch(ctx, h.FQN(), nil); !errors.Is(err, ErrScopeDenied) || errors.Is(err, ErrRefusalUnrecorded) {
		t.Fatalf("Dispatch = %v, want ErrScopeDenied", err)
	}
}
