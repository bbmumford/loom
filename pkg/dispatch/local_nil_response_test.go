/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package dispatch

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/bbmumford/loom/node/handlers"
	"github.com/bbmumford/loom/pkg/rpc/effect"
	rpcscope "github.com/bbmumford/loom/pkg/rpc/scope"
)

// LocalCaller answers a call whose handler gave no response and no error as a
// failure of neither kind naming the operation, as RPCServer.executeLocal and
// node's local dispatcher answer the same call, and never reads the missing
// response.
func TestLocalCallerAnswersNoResponseAsAFailure(t *testing.T) {
	reg := handlers.NewHandlerRegistry()
	h := &answeringHandler{}
	if err := reg.RegisterRPC(h); err != nil {
		t.Fatalf("RegisterRPC: %v", err)
	}
	payload, err := NewLocalCaller(reg).Call(context.Background(), "svc", h.Name(), nil)
	if err == nil || payload != nil {
		t.Fatalf("Call = (%v, %v); want a failure", payload, err)
	}
	if errors.Is(err, ErrConflict) || errors.Is(err, rpcscope.ErrDenied) {
		t.Errorf("Call returned %v; want a failure of neither kind", err)
	}
	if !strings.Contains(err.Error(), "no response") || !strings.Contains(err.Error(), h.Name()) {
		t.Errorf("Call returned %q; want it to name the operation and the missing response", err)
	}
}

// A finder whose bidi channel handled a call (ok) and answered no response and
// no error has broken SessionFinder.CallViaBidi's contract, so HWPCaller reads
// that answer as a failed send on every path and never reads the missing
// response: an operation that is not Repeatable ends ambiguous, since the
// request may have run, and none is sent again over a dynamic stream.
func TestABidiAnswerOfNoResponseIsAFailedSendOnEveryPath(t *testing.T) {
	for _, path := range []struct {
		name     string
		noRoutes bool
	}{{"routed", false}, {"no routes", true}} {
		for _, class := range []struct {
			name      string
			class     effect.Class
			ambiguous bool
		}{
			{"Unsafe", effect.Unsafe, true},
			{"Idempotent", effect.Idempotent, false},
		} {
			t.Run(path.name+"/"+class.name, func(t *testing.T) {
				sess := answering("stream")
				defer forgetSession(sess)
				finder := &bidiThenStreamFinder{
					mockFinder: mockFinder{session: sess, class: class.class},
					noRoutes:   path.noRoutes,
				}
				caller := NewHWPCaller(finder)
				defer caller.Close()

				out, err := caller.Call(context.Background(), "svc", "svc.Op", nil)
				if err == nil || out != nil {
					t.Fatalf("Call = (%q, %v); want a failure", out, err)
				}
				if got := errors.Is(err, ErrAmbiguousEffect); got != class.ambiguous {
					t.Errorf("Call returned %v; ambiguous %v, want %v", err, got, class.ambiguous)
				}
				if !strings.Contains(err.Error(), "nil response") {
					t.Errorf("Call returned %q; want it to name the missing response", err)
				}
				if n := sess.sent.Load(); n != 0 {
					t.Errorf("the call was sent again over a dynamic stream %d time(s)", n)
				}
			})
		}
	}
}
