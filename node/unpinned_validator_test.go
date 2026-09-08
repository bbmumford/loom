/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ORBTR/aether/rpc/pb"
	"github.com/bbmumford/loom/node/handlers"
	rpcscope "github.com/bbmumford/loom/pkg/rpc/scope"
	"github.com/bbmumford/loom/ports"
)

// unpinnedValidator is a complete ports.AuthValidator that does NOT implement
// ports.TransportAxisPin. bindAuthenticatedPrincipal type-asserts for that
// interface, so this is the shape a consumer produces by simply not writing the
// method — the case the receive path must refuse rather than wave through.
type unpinnedValidator struct{}

func (unpinnedValidator) ValidateExecutionAuth(context.Context, ports.SecureHandler) error {
	return nil
}

func (unpinnedValidator) WithTenantID(ctx context.Context, _ string) context.Context { return ctx }

func (unpinnedValidator) WithAuthenticatedPrincipal(
	ctx context.Context,
	principal rpcscope.AuthenticatedPrincipal,
) context.Context {
	return rpcscope.WithAuthenticatedPrincipal(ctx, principal)
}

// emptyPinValidator DOES implement ports.TransportAxisPin but names no axis.
// It is a separate arm because satisfying the interface and naming an axis are
// separate failures: the type assertion succeeds here, so only the emptiness
// check stands between this validator and an unreconciled principal.
type emptyPinValidator struct{ unpinnedValidator }

func (emptyPinValidator) TransportPinnedAxis() string { return "" }

// A mesh principal must not be reconstructed when the consumer's validator
// names no axis to reconcile against the transport-established partition.
// Without an axis there is nothing to compare the wire identity to, so the
// cross-tenant forge gate has no input and the request must be refused.
//
// Each denial asserts the SPECIFIC error, because the un-pinned case is refused
// TWICE: an empty axis also matches no wire axis, leaving pinnedValue "" against
// a non-empty transport partition, so the pinned-axis-mismatch check below
// refuses it as well. A test asserting only "denied" therefore stays green with
// the pin check deleted — measured, not assumed: disabling it left this test
// passing until these assertions named the error.
//
// The first leg is a POSITIVE CONTROL on the same registry, request and
// transport partition: it proves the fixture admits a principal when the
// validator DOES pin, so the two denials below are attributable to the pin and
// not to an unrelated setup failure that would deny anything.
func TestRPCServerDeniesMeshPrincipalWithoutATransportAxisPin(t *testing.T) {
	serve := func(t *testing.T, v ports.AuthValidator) (*pb.RPCResponse, int32) {
		t.Helper()
		registry := handlers.NewHandlerRegistry()
		registry.SetPresencePolicy(customerScopePolicy())
		probe := &customerPrincipalProbe{}
		if err := registry.RegisterRPC(probe); err != nil {
			t.Fatal(err)
		}
		server := NewRPCServer(registry)
		server.SetAuthValidator(v)

		ctx := handlers.WithTransportPartition(context.Background(), "orbtr")
		resp := server.handleRequest(ctx, customerPrincipalRequest())
		return resp, atomic.LoadInt32(&probe.calls)
	}

	t.Run("positive control: a pinning validator admits", func(t *testing.T) {
		resp, calls := serve(t, customerPrincipalValidator{})
		if !resp.Success {
			t.Fatalf("control denied, so the denials below prove nothing about the pin: %s", resp.Error)
		}
		if calls != 1 {
			t.Fatalf("control handler calls = %d, want 1", calls)
		}
	})

	t.Run("validator without TransportAxisPin is refused", func(t *testing.T) {
		resp, calls := serve(t, unpinnedValidator{})
		if resp.Success {
			t.Fatal("a validator that implements no TransportAxisPin bound a mesh principal: the wire identity reached dispatch with nothing reconciled against the transport partition")
		}
		if !strings.Contains(resp.Error, "transport-axis pin") {
			t.Fatalf("refused by the wrong check: want the transport-axis-pin refusal, got %q", resp.Error)
		}
		if calls != 0 {
			t.Fatalf("handler ran %d time(s) before the pin check; the refusal must precede dispatch", calls)
		}
	})

	t.Run("validator naming an empty axis is refused", func(t *testing.T) {
		resp, calls := serve(t, emptyPinValidator{})
		if resp.Success {
			t.Fatal("a validator naming an empty pinned axis bound a mesh principal: satisfying ports.TransportAxisPin is not the same as naming an axis")
		}
		if !strings.Contains(resp.Error, "transport-axis pin") {
			t.Fatalf("refused by the wrong check: want the transport-axis-pin refusal, got %q", resp.Error)
		}
		if calls != 0 {
			t.Fatalf("handler ran %d time(s) before the pin check; the refusal must precede dispatch", calls)
		}
	})
}
