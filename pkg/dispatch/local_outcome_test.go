/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
 */

package dispatch

import (
	"context"
	"errors"
	"testing"

	"github.com/bbmumford/loom/node/handlers"
	"github.com/bbmumford/loom/pkg/rpc/effect"
)

// The failure LocalCaller hands on for a handler that answered nothing is
// handlers.ErrNoResponse, the one reading of such a result.
func TestLocalCallerHandsOnErrNoResponse(t *testing.T) {
	reg := handlers.NewHandlerRegistry()
	h := &answeringHandler{}
	if err := reg.RegisterRPC(h); err != nil {
		t.Fatalf("RegisterRPC: %v", err)
	}
	if _, err := NewLocalCaller(reg).Call(context.Background(), "svc", h.Name(), nil); !errors.Is(err, handlers.ErrNoResponse) {
		t.Fatalf("Call returned %v; want handlers.ErrNoResponse", err)
	}
}

// The bidi answer of no response and no error is read as errNilBidiResponse
// where the call's path keeps the error it read: callOnce, with no routes, for
// an operation of either class.
func TestCallOnceReadsABidiAnswerOfNoResponseAsErrNilBidiResponse(t *testing.T) {
	for _, class := range []struct {
		name  string
		class effect.Class
	}{{"Unsafe", effect.Unsafe}, {"Idempotent", effect.Idempotent}} {
		t.Run(class.name, func(t *testing.T) {
			sess := answering("stream")
			defer forgetSession(sess)
			caller := NewHWPCaller(&bidiThenStreamFinder{
				mockFinder: mockFinder{session: sess, class: class.class},
				noRoutes:   true,
			})
			defer caller.Close()
			if _, err := caller.Call(context.Background(), "svc", "svc.Op", nil); !errors.Is(err, errNilBidiResponse) {
				t.Fatalf("Call returned %v; want errNilBidiResponse", err)
			}
		})
	}
}
