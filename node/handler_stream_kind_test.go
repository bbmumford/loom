/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
 */
package node

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/ORBTR/aether/rpc/pb"
	"github.com/bbmumford/loom/pkg/dispatch"
	rpcscope "github.com/bbmumford/loom/pkg/rpc/scope"
)

// A stream refused before its handler runs comes back to the opener as the typed
// refusal a refused call comes back as: a principal the node refuses, and a scope
// the handler's check refuses, are scope.ErrDenied (the kind serveHandlerStream
// stamps), and a handler the node does not hold is a handler rejection, not a
// denial.
func TestAHandlerStreamsRefusalReachesTheOpenerTyped(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	noPrincipal := func() *pb.RPCRequest {
		r := typedPrincipalRequest()
		r.Principal = nil
		return r
	}
	elsewhere := func() *pb.RPCRequest {
		r := typedPrincipalRequest()
		r.Handler = "orbtr.test.Elsewhere"
		return r
	}
	for _, c := range []struct {
		name    string
		server  func(*testing.T) *RPCServer
		request func() *pb.RPCRequest
		denied  bool
	}{
		{"an admitter refusing the principal", func(t *testing.T) *RPCServer {
			s, _ := streamingServer(t, refusingAdmitterValidator{})
			return s
		}, typedPrincipalRequest, true},
		{"no principal for a scoped handler", func(t *testing.T) *RPCServer {
			s, _ := streamingServer(t, typedPrincipalValidator{})
			return s
		}, noPrincipal, true},
		{"a handler this node does not hold", func(t *testing.T) *RPCServer {
			s, _ := streamingServer(t, typedPrincipalValidator{})
			return s
		}, elsewhere, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			_, err := dispatch.OpenHandlerStream(ctx, oneStreamSession{server: c.server(t), partition: "orbtr"}, c.request())
			if err == nil {
				t.Fatal("the stream opened")
			}
			if got := errors.Is(err, rpcscope.ErrDenied); got != c.denied {
				t.Fatalf("open: %v; errors.Is(err, scope.ErrDenied) = %v, want %v", err, got, c.denied)
			}
			if !c.denied && !errors.Is(err, dispatch.ErrHandlerError) {
				t.Fatalf("open: %v, want a handler rejection", err)
			}
		})
	}
}
