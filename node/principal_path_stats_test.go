/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"testing"

	"github.com/bbmumford/loom/node/handlers"
)

// Sealed and unsealed arrivals must be counted separately, because a
// deployment cannot otherwise tell "every caller is sealed" from "no one
// measured". Each leg asserts the OTHER counter stayed put: a single
// counter that incremented on every request would satisfy a one-sided
// assertion while distinguishing nothing.
func TestRPCServerCountsSealedAndUnsealedArrivalsSeparately(t *testing.T) {
	t.Run("a sealed principal counts as sealed, not unsealed", func(t *testing.T) {
		registry := handlers.NewHandlerRegistry()
		registry.SetPresencePolicy(typedPrincipalScopePolicy())
		if err := registry.RegisterRPC(&typedPrincipalProbe{}); err != nil {
			t.Fatal(err)
		}
		server := NewRPCServer(registry)
		server.SetAuthValidator(typedPrincipalValidator{})

		ctx := handlers.WithTransportPartition(context.Background(), "orbtr")
		if resp := server.handleRequest(ctx, typedPrincipalRequest()); !resp.Success {
			t.Fatalf("sealed request denied, so the counts below prove nothing: %s", resp.Error)
		}
		sealed, unsealed := server.PrincipalPathStats()
		if sealed != 1 || unsealed != 0 {
			t.Fatalf("sealed=%d unsealed=%d, want 1 and 0", sealed, unsealed)
		}
	})

	// A request without a principal is counted as unsealed and gets no
	// identity from the userId and scopes it carries in req.Context, so the
	// probe's declared scope denies it.
	t.Run("a request with no principal counts as unsealed and is denied", func(t *testing.T) {
		registry := handlers.NewHandlerRegistry()
		registry.SetPresencePolicy(typedPrincipalScopePolicy())
		if err := registry.RegisterRPC(&typedPrincipalProbe{}); err != nil {
			t.Fatal(err)
		}
		server := NewRPCServer(registry)
		server.SetAuthValidator(typedPrincipalValidator{})

		ctx := handlers.WithTransportPartition(context.Background(), "orbtr")
		req := typedPrincipalRequest()
		req.Principal = nil
		req.Context = map[string]string{"userId": "user-A", "scopes": "org.read"}
		if resp := server.handleRequest(ctx, req); resp.Success {
			t.Fatal("request with no principal was admitted on the userId and scopes in its request map")
		}

		sealed, unsealed := server.PrincipalPathStats()
		if sealed != 0 || unsealed != 1 {
			t.Fatalf("sealed=%d unsealed=%d, want 0 and 1", sealed, unsealed)
		}
	})
}
