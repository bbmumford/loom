/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/ORBTR/aether/rpc/pb"
	"github.com/bbmumford/loom/node/handlers"
	rpcscope "github.com/bbmumford/loom/pkg/rpc/scope"
	"github.com/bbmumford/loom/ports"
)

// noAdmitterValidator is a complete ports.AuthValidator that does NOT implement
// ports.PrincipalAdmitter. bindAuthenticatedPrincipal type-asserts for that
// interface, so this is the shape a consumer produces by simply not writing the
// method — the case the receive path must refuse rather than wave through.
type noAdmitterValidator struct{}

func (noAdmitterValidator) ValidateExecutionAuth(context.Context, ports.SecureHandler) error {
	return nil
}

func (noAdmitterValidator) WithPartitionID(ctx context.Context, _ string) context.Context { return ctx }

func (noAdmitterValidator) WithAuthenticatedPrincipal(
	ctx context.Context,
	principal rpcscope.AuthenticatedPrincipal,
) context.Context {
	return rpcscope.WithAuthenticatedPrincipal(ctx, principal)
}

// refusingAdmitterValidator implements ports.PrincipalAdmitter and refuses
// everything, so the only thing between it and dispatch is the admitter's error.
type refusingAdmitterValidator struct{ noAdmitterValidator }

func (refusingAdmitterValidator) AdmitPrincipal(context.Context, string, rpcscope.AuthenticatedPrincipal) error {
	return errors.New("refused by the consumer")
}

// recordingAdmitterValidator admits everything and records what the RPC server
// handed it, so a test can assert the admitter judged the wire assertion.
type recordingAdmitterValidator struct {
	noAdmitterValidator
	transport *string
	partition *string
}

func (v recordingAdmitterValidator) AdmitPrincipal(
	_ context.Context,
	transportPartition string,
	principal rpcscope.AuthenticatedPrincipal,
) error {
	*v.transport = transportPartition
	*v.partition = principal.Identity().Partition()
	return nil
}

func servePrincipalRequest(t *testing.T, v ports.AuthValidator) (*pb.RPCResponse, *typedPrincipalProbe) {
	t.Helper()
	registry := handlers.NewHandlerRegistry()
	registry.SetPresencePolicy(typedPrincipalScopePolicy())
	probe := &typedPrincipalProbe{}
	if err := registry.RegisterRPC(probe); err != nil {
		t.Fatal(err)
	}
	server := NewRPCServer(registry)
	server.SetAuthValidator(v)

	ctx := handlers.WithTransportPartition(context.Background(), "orbtr")
	return server.handleRequest(ctx, typedPrincipalRequest()), probe
}

// A mesh principal is every field its sending peer asserted, so it is installed
// only once the consumer's admitter has accepted it over that transport. Each
// denial asserts the SPECIFIC error, because a refusal from an unrelated check
// would also leave the handler uncalled.
//
// The first leg is a POSITIVE CONTROL on the same registry, request and
// transport partition: it proves the fixture admits a principal when the
// validator's admitter accepts, so the two denials are attributable to the
// admitter and not to an unrelated setup failure that would deny anything.
func TestRPCServerInstallsAMeshPrincipalOnlyThroughTheAdmitter(t *testing.T) {
	t.Run("positive control: an accepting admitter admits", func(t *testing.T) {
		resp, probe := servePrincipalRequest(t, typedPrincipalValidator{})
		if !resp.Success {
			t.Fatalf("control denied, so the denials below prove nothing about the admitter: %s", resp.Error)
		}
		if calls := atomic.LoadInt32(&probe.calls); calls != 1 {
			t.Fatalf("control handler calls = %d, want 1", calls)
		}
	})

	t.Run("a validator without an admitter is refused", func(t *testing.T) {
		resp, probe := servePrincipalRequest(t, noAdmitterValidator{})
		if resp.Success {
			t.Fatal("a validator that implements no PrincipalAdmitter installed a mesh principal: a peer's assertion reached dispatch unjudged")
		}
		if !strings.Contains(resp.Error, "requires a consumer principal admitter") {
			t.Fatalf("refused by the wrong check: want the missing-admitter refusal, got %q", resp.Error)
		}
		if calls := atomic.LoadInt32(&probe.calls); calls != 0 {
			t.Fatalf("handler ran %d time(s); the refusal must precede dispatch", calls)
		}
	})

	t.Run("an admitter's refusal refuses the request", func(t *testing.T) {
		resp, probe := servePrincipalRequest(t, refusingAdmitterValidator{})
		if resp.Success {
			t.Fatal("an admitter refused the principal and the request still dispatched")
		}
		if !strings.Contains(resp.Error, "refused by the consumer") {
			t.Fatalf("refused by the wrong check: want the admitter's error, got %q", resp.Error)
		}
		if calls := atomic.LoadInt32(&probe.calls); calls != 0 {
			t.Fatalf("handler ran %d time(s); the refusal must precede dispatch", calls)
		}
	})

	t.Run("the admitter judges the transport partition and the wire partition", func(t *testing.T) {
		var transport, partition string
		resp, _ := servePrincipalRequest(t, recordingAdmitterValidator{transport: &transport, partition: &partition})
		if !resp.Success {
			t.Fatalf("recording admitter admitted, but the request failed: %s", resp.Error)
		}
		if transport != "orbtr" || partition != "org-A" {
			t.Fatalf("admitter saw transport %q and partition %q, want orbtr and org-A", transport, partition)
		}
	})
}
