/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"errors"
	"testing"

	"github.com/bbmumford/loom/node/handlers"
	"github.com/bbmumford/loom/pkg/dispatch"
)

// The readers in this process hand on a handler's answer of nothing as
// handlers.ErrNoResponse, matchable, so a caller tells it from a failure the
// handler answered; the far caller receives its words on the wire.
func TestEveryLocalReaderHandsOnErrNoResponse(t *testing.T) {
	reg := handlers.NewHandlerRegistry()
	h := &outcomeHandler{answer: func() (*handlers.RPCResponse, error) { return nil, nil }}
	if err := reg.RegisterRPC(h); err != nil {
		t.Fatalf("RegisterRPC: %v", err)
	}
	ctx := context.Background()
	if _, err := (&localDispatcherAdapter{registry: reg}).DispatchBytes(ctx, h.Name(), nil); !errors.Is(err, handlers.ErrNoResponse) {
		t.Errorf("the local dispatcher returned %v; want handlers.ErrNoResponse", err)
	}
	if _, err := dispatch.NewLocalCaller(reg).Call(ctx, "test", h.Name(), nil); !errors.Is(err, handlers.ErrNoResponse) {
		t.Errorf("dispatch.LocalCaller returned %v; want handlers.ErrNoResponse", err)
	}
}
