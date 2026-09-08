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

// wireStampingValidator implements ports.ScopeStamper as well as the base
// validator, which is what makes the wire branch reachable: handleRequest only
// lifts req.Context userId/scopes when its validator satisfies that interface.
type wireStampingValidator struct{ customerPrincipalValidator }

func (wireStampingValidator) WithWireIdentity(
	ctx context.Context,
	_ string,
	_ []string,
) context.Context {
	return ctx
}

// The two identity paths must be counted separately, because a deployment
// cannot otherwise tell "no request reached the un-reconciled path" from "no
// one measured". Each leg asserts the OTHER counter stayed put: a single
// counter that incremented on every request would satisfy a one-sided
// assertion while distinguishing nothing.
func TestRPCServerCountsSealedAndWireIdentityPathsSeparately(t *testing.T) {
	t.Run("a sealed principal counts as sealed, not wire", func(t *testing.T) {
		registry := handlers.NewHandlerRegistry()
		registry.SetPresencePolicy(customerScopePolicy())
		if err := registry.RegisterRPC(&customerPrincipalProbe{}); err != nil {
			t.Fatal(err)
		}
		server := NewRPCServer(registry)
		server.SetAuthValidator(customerPrincipalValidator{})

		ctx := handlers.WithTransportPartition(context.Background(), "orbtr")
		if resp := server.handleRequest(ctx, customerPrincipalRequest()); !resp.Success {
			t.Fatalf("sealed request denied, so the counts below prove nothing: %s", resp.Error)
		}
		sealed, wire := server.PrincipalPathStats()
		if sealed != 1 || wire != 0 {
			t.Fatalf("sealed=%d wire=%d, want 1 and 0", sealed, wire)
		}
	})

	t.Run("a request with no principal counts as wire, not sealed", func(t *testing.T) {
		registry := handlers.NewHandlerRegistry()
		registry.SetPresencePolicy(customerScopePolicy())
		if err := registry.RegisterRPC(&customerPrincipalProbe{}); err != nil {
			t.Fatal(err)
		}
		server := NewRPCServer(registry)
		server.SetAuthValidator(wireStampingValidator{})

		ctx := handlers.WithTransportPartition(context.Background(), "orbtr")
		req := customerPrincipalRequest()
		req.Principal = nil
		req.Context = map[string]string{"userId": "user-A", "scopes": "org.read"}
		server.handleRequest(ctx, req)

		sealed, wire := server.PrincipalPathStats()
		if sealed != 0 || wire != 1 {
			t.Fatalf("sealed=%d wire=%d, want 0 and 1", sealed, wire)
		}
	})
}
