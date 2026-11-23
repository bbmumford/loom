/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
 */

package handlers

import (
	"context"
	"errors"
	"strings"
	"testing"

	rpcscope "github.com/bbmumford/loom/pkg/rpc/scope"
)

type meshRefusals struct {
	got  []rpcscope.Refusal
	fail error
}

func (m *meshRefusals) RecordRefusal(_ context.Context, r rpcscope.Refusal) error {
	m.got = append(m.got, r)
	return m.fail
}

// The mesh check point hands the registered recorder one refusal per refused
// call, naming the operation and the scope tag it asks, and the response stays
// a denial; a refusal the recorder cannot record is still answered as a
// denial, its error naming the record it could not make.
func TestTheMeshCheckPointRecordsEachRefusal(t *testing.T) {
	ctx := context.Background()
	reg := NewHandlerRegistry()
	rec := &meshRefusals{}
	reg.SetRefusalRecorder(rec)
	probe := &scopeRefusalProbe{name: "refused.and.recorded"}
	if err := reg.RegisterRPC(probe); err != nil {
		t.Fatalf("RegisterRPC: %v", err)
	}

	resp, err := reg.Dispatch(ctx, &RPCRequest{Handler: probe.name})
	if err != nil || resp == nil || resp.Success || !errors.Is(resp.Denied, rpcscope.ErrDenied) {
		t.Fatalf("Dispatch = (%+v, %v); want a denied response", resp, err)
	}
	if len(rec.got) != 1 || rec.got[0].Operation != probe.name || rec.got[0].Scope != ScopeWithinPartition || rec.got[0].Requirement != nil {
		t.Fatalf("recorded %+v; want one refusal of %s under its scope tag", rec.got, probe.name)
	}

	rec.fail = errors.New("audit host down")
	resp, err = reg.Dispatch(ctx, &RPCRequest{Handler: probe.name})
	if err != nil || resp == nil || resp.Success || resp.Denied == nil || !strings.Contains(resp.Error, rpcscope.ErrRefusalUnrecorded.Error()) {
		t.Fatalf("unrecorded Dispatch = (%+v, %v); want a failed response with its Denied", resp, err)
	}
	if probe.ran.Load() != 0 {
		t.Fatalf("the refused handler ran %d times", probe.ran.Load())
	}
}
