/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package rpchttp

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"google.golang.org/protobuf/types/known/structpb"

	"github.com/bbmumford/loom/pkg/rpc"
	"github.com/bbmumford/loom/pkg/rpc/scope"
)

type bridgeRefusals struct {
	got  []scope.Refusal
	fail error
}

func (b *bridgeRefusals) RecordRefusal(_ context.Context, r scope.Refusal) error {
	b.got = append(b.got, r)
	return b.fail
}

// The bridge's local and proxied calls each record their refusal
// through the registry's recorder and answer 403; a refusal the recorder
// cannot record answers 503, never 403.
func TestTheBridgeRecordsEveryRefusalAndAnswersAnUnrecordedOne503(t *testing.T) {
	bridge, h := newBridgeWithHandler(t, stubHandler(rpc.ScopeSubject))
	rec := &bridgeRefusals{}
	bridge.registry.SetRefusalRecorder(rec)

	var be *bridgeError
	_, err := bridge.executeLocal(newPostRequest(context.Background()), h, &structpb.Struct{})
	if !errors.As(err, &be) || be.status != http.StatusForbidden || len(rec.got) != 1 || rec.got[0].Operation != h.FQN() {
		t.Fatalf("local: %v, recorded %+v; want 403 and one refusal of %s", err, rec.got, h.FQN())
	}
	_, err = bridge.executeProxy(newPostRequest(context.Background()), h, &structpb.Struct{})
	if !errors.As(err, &be) || be.status != http.StatusForbidden || len(rec.got) != 2 {
		t.Fatalf("proxy: %v, recorded %d; want 403 and a second refusal", err, len(rec.got))
	}

	rec.fail = errors.New("audit host down")
	_, err = bridge.executeLocal(newPostRequest(context.Background()), h, &structpb.Struct{})
	if !errors.As(err, &be) || be.status != http.StatusServiceUnavailable || errors.Is(err, rpc.ErrScopeDenied) {
		t.Fatalf("unrecorded: %v; want 503 and no denial", err)
	}
	if got := classifyHandlerStatus(rpc.ErrRefusalUnrecorded); got != http.StatusServiceUnavailable {
		t.Fatalf("classifyHandlerStatus(ErrRefusalUnrecorded) = %d, want 503", got)
	}
}
