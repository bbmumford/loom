/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"net"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/noise"
	"github.com/ORBTR/aether/rpc/pb"
	"github.com/bbmumford/loom/core/transport/manager"
	"github.com/bbmumford/loom/node/handlers"
	rpcscope "github.com/bbmumford/loom/pkg/rpc/scope"
)

// A node configured with no partition listens on one transport bound to none.
// Its listener passes "" as the partition of every session that names none, so
// no transport partition reaches the RPC path, and the transport is not keyed
// in transportMgr, whose keys are partitions.
func TestUnpartitionedVL1BindsNoTransportPartition(t *testing.T) {
	dir := t.TempDir()
	rt, err := Initialize(Config{
		ServiceName: "unbound-transport-test",
		DataDir:     dir,
		NetworkKeys: []string{"unbound-transport-test-key"},
		VL1:         VL1TransportConfig{Enabled: true, UDPPort: freeUDPPort(t)},
		LAD:         LADDirectoryConfig{Storage: "local", LocalDir: filepath.Join(dir, "lad"), BootstrapHosts: "127.0.0.1:9"},
	})
	if err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	t.Cleanup(func() { _ = rt.Shutdown() })

	if len(rt.listenPartitionIDs) != 1 || rt.listenPartitionIDs[0] != "" {
		t.Fatalf("listener partitions = %q, want [\"\"]: a named partition here is stamped on "+
			"every session the unpartitioned listener accepts", rt.listenPartitionIDs)
	}
	if ids := rt.transportMgr.ListPartitions(); len(ids) != 0 {
		t.Fatalf("transportMgr holds %v, want no partition: the unpartitioned transport is "+
			"reachable under a partition name", ids)
	}
	if rt.unpartitionedTransport == nil || len(rt.listenTransports) != 1 ||
		rt.listenTransports[0] != aether.Transport(rt.unpartitionedTransport) {
		t.Fatal("the unpartitioned transport is not the one listener Shutdown closes")
	}
}

// oneSessionListener hands out one session, then blocks until its context ends.
type oneSessionListener struct {
	session aether.Connection
	taken   atomic.Bool
}

func (l *oneSessionListener) Accept(ctx context.Context) (aether.Connection, error) {
	if l.taken.CompareAndSwap(false, true) {
		return l.session, nil
	}
	<-ctx.Done()
	return nil, ctx.Err()
}
func (l *oneSessionListener) Close() error   { return nil }
func (l *oneSessionListener) Addr() net.Addr { return &net.UDPAddr{} }

// transportPartitionProbe records what the RPC path bound for the session a
// request arrived on.
type transportPartitionProbe struct {
	transport atomic.Value // string
	pinned    atomic.Value // string
	axes      atomic.Int32
}

func (p *transportPartitionProbe) Name() string                { return "orbtr.test.TransportPartition" }
func (p *transportPartitionProbe) Role() string                { return "orbtr.test" }
func (p *transportPartitionProbe) RequiresAuth() bool          { return false }
func (p *transportPartitionProbe) AllowedAuthTypes() []string  { return nil }
func (p *transportPartitionProbe) Scopes() []string            { return nil }
func (p *transportPartitionProbe) Scope() handlers.Scope       { return handlers.ScopeNone }
func (p *transportPartitionProbe) AllowedPartitions() []string { return nil }
func (p *transportPartitionProbe) ExecuteRPC(ctx context.Context, _ *handlers.RPCRequest) (*handlers.RPCResponse, error) {
	identity := rpcscope.AuthenticatedIdentityFromContext(ctx)
	p.transport.Store(handlers.TransportPartitionFromContext(ctx))
	p.pinned.Store(identity.Scope(rpcscope.AxisTransportPinned))
	p.axes.Store(int32(len(identity.Scopes)))
	return &handlers.RPCResponse{Success: true}, nil
}

// HandleIncomingSessions serves a listener bound to no partition: a session on
// it that names none reaches its handler with no transport partition and no
// transport-pinned axis.
func TestHandleIncomingSessionsBindsNoTransportPartition(t *testing.T) {
	registry := handlers.NewHandlerRegistry()
	probe := &transportPartitionProbe{}
	if err := registry.RegisterRPC(probe); err != nil {
		t.Fatal(err)
	}
	server := NewRPCServer(registry)
	client, served := newSessionPair()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go server.HandleIncomingSessions(ctx, &oneSessionListener{session: served})

	sendRequest(t, ctx, client, &pb.RPCRequest{Id: "unbound-1", Handler: probe.Name()})
	if resp := recvResponse(t, ctx, client); !resp.Success {
		t.Fatalf("request refused: %s", resp.Error)
	}
	if got, _ := probe.transport.Load().(string); got != "" {
		t.Fatalf("transport partition = %q, want none: an unbound listener named a partition "+
			"that every reader then treats as bound", got)
	}
	if got, _ := probe.pinned.Load().(string); got != "" || probe.axes.Load() != 0 {
		t.Fatalf("identity pinned axis = %q with %d axes, want no axis on an unbound session",
			got, probe.axes.Load())
	}
}

// presenceTierProbe is a ScopePartition handler: validateScope admits a call
// only where the presence policy finds the axis that tier requires.
type presenceTierProbe struct{ calls atomic.Int32 }

func (p *presenceTierProbe) Name() string                { return "orbtr.test.PresenceTier" }
func (p *presenceTierProbe) Role() string                { return "orbtr.test" }
func (p *presenceTierProbe) RequiresAuth() bool          { return false }
func (p *presenceTierProbe) AllowedAuthTypes() []string  { return nil }
func (p *presenceTierProbe) Scopes() []string            { return nil }
func (p *presenceTierProbe) Scope() handlers.Scope       { return handlers.ScopePartition }
func (p *presenceTierProbe) AllowedPartitions() []string { return nil }
func (p *presenceTierProbe) ExecuteRPC(context.Context, *handlers.RPCRequest) (*handlers.RPCResponse, error) {
	p.calls.Add(1)
	return &handlers.RPCResponse{Success: true}, nil
}

// A session served by a listener bound to no partition reaches a presence-tier
// handler with no transport-pinned axis, so a policy requiring that axis refuses
// it with scope.ErrDenied and the handler never runs; the same request on a
// listener bound to a partition is admitted, which shows the refusal comes from
// the missing binding. A listener that bound its unbound sessions to a named
// partition would give them that axis, and the first row would be admitted.
func TestUnboundListenerSessionIsRefusedByAPresenceTier(t *testing.T) {
	for _, tc := range []struct {
		listenerPartition string
		wantAdmitted      bool
	}{
		{listenerPartition: "", wantAdmitted: false},
		{listenerPartition: "p-bound", wantAdmitted: true},
	} {
		t.Run("listener="+tc.listenerPartition, func(t *testing.T) {
			registry := handlers.NewHandlerRegistry()
			registry.SetPresencePolicy(typedPrincipalScopePolicy())
			probe := &presenceTierProbe{}
			if err := registry.RegisterRPC(probe); err != nil {
				t.Fatal(err)
			}
			server := NewRPCServer(registry)
			client, served := newSessionPair()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			go server.HandleIncomingSessionsForPartition(ctx, &oneSessionListener{session: served}, tc.listenerPartition)

			sendRequest(t, ctx, client, &pb.RPCRequest{Id: "presence-1", Handler: probe.Name()})
			resp := recvResponse(t, ctx, client)
			if tc.wantAdmitted {
				if !resp.Success || probe.calls.Load() != 1 {
					t.Fatalf("request on a listener bound to %q refused: %s", tc.listenerPartition, resp.Error)
				}
				return
			}
			if resp.Success || probe.calls.Load() != 0 {
				t.Fatalf("presence-tier handler ran on a session bound to no partition (success=%v, calls=%d)",
					resp.Success, probe.calls.Load())
			}
			if !strings.Contains(resp.Error, rpcscope.ErrDenied.Error()) {
				t.Fatalf("refusal = %q, want scope.ErrDenied (%q)", resp.Error, rpcscope.ErrDenied)
			}
		})
	}
}

// partitionRecordingValidator is typedPrincipalValidator that also records
// every partition handleRequest hands WithPartitionID.
type partitionRecordingValidator struct {
	typedPrincipalValidator
	partitions *[]string
}

func (v partitionRecordingValidator) WithPartitionID(ctx context.Context, partitionID string) context.Context {
	*v.partitions = append(*v.partitions, partitionID)
	return ctx
}

// A transport partition whose name is "default" is a partition: handleRequest
// hands it to the validator, and a mesh principal on it reaches the consumer
// admitter instead of the refusal a session bound to no partition gets.
func TestPartitionNamedDefaultIsBoundLikeAnyPartition(t *testing.T) {
	for _, tc := range []struct {
		transport      string
		wantPartitions []string
		wantAdmitted   bool
		wantErr        string
	}{
		{transport: "default", wantPartitions: []string{"default"}, wantAdmitted: true},
		{transport: "", wantPartitions: nil, wantErr: "partition-bound transport"},
	} {
		t.Run("transport="+tc.transport, func(t *testing.T) {
			registry := handlers.NewHandlerRegistry()
			registry.SetPresencePolicy(typedPrincipalScopePolicy())
			probe := &typedPrincipalProbe{}
			if err := registry.RegisterRPC(probe); err != nil {
				t.Fatal(err)
			}
			var seen []string
			server := NewRPCServer(registry)
			server.SetAuthValidator(partitionRecordingValidator{partitions: &seen})

			req := typedPrincipalRequest()
			req.Principal.Axes[0].Value = "default"
			resp := server.handleRequest(handlers.WithTransportPartition(context.Background(), tc.transport), req)

			if strings.Join(seen, ",") != strings.Join(tc.wantPartitions, ",") {
				t.Fatalf("WithPartitionID saw %q, want %q", seen, tc.wantPartitions)
			}
			if tc.wantAdmitted {
				if !resp.Success || atomic.LoadInt32(&probe.calls) != 1 {
					t.Fatalf("principal on partition %q refused: %s", tc.transport, resp.Error)
				}
				if got := probe.got.Identity().Scope(rpcscope.AxisTransportPinned); got != "default" {
					t.Fatalf("admitted principal pinned to %q, want default", got)
				}
				return
			}
			if resp.Success || !strings.Contains(resp.Error, tc.wantErr) {
				t.Fatalf("response = success:%v error:%q, want %q", resp.Success, resp.Error, tc.wantErr)
			}
		})
	}
}

// A shared transport is dialled for the partition on the dial context and no
// other. With none there the dial is refused rather than given a configured
// partition, and a partition the caller named is the one the transport is asked
// for even when another partition is configured first.
func TestDialNoiseUDPTakesNoConfiguredPartitionForASharedTransport(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	st, err := manager.NewSharedTransport(noise.NoiseTransportConfig{
		PrivateKey:  priv,
		NetworkKeys: []string{"shared-dial-test-key"},
		ListenAddr:  "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("NewSharedTransport: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if err := st.RegisterPartition("p-first", [][]byte{[]byte("psk-first")}); err != nil {
		t.Fatal(err)
	}
	rt := &Runtime{
		cfg: Config{Partitions: []PartitionTransportConfig{
			{PartitionID: "p-first", NetworkKeys: []string{"psk-first"}},
		}},
		transportMgr:       manager.New(),
		sharedTransport:    st,
		listenTransports:   []aether.Transport{st},
		listenPartitionIDs: []string{""},
	}
	target := aether.Target{NodeID: "peer-node", Address: "127.0.0.1:9"}

	_, err = rt.dialNoiseUDP(context.Background(), target)
	if err == nil || !strings.Contains(err.Error(), manager.ErrSharedNoPartitionInCtx.Error()) {
		t.Fatalf("dial with no partition = %v, want the shared transport's refusal (%v): "+
			"a configured partition was presented in its place", err, manager.ErrSharedNoPartitionInCtx)
	}

	_, err = rt.dialNoiseUDP(manager.WithPartitionID(context.Background(), "p-named"), target)
	if err == nil || !strings.Contains(err.Error(), manager.ErrSharedPartitionNotFound.Error()) ||
		!strings.Contains(err.Error(), "p-named") {
		t.Fatalf("dial for p-named = %v, want the shared transport asked for p-named: the "+
			"caller's partition was replaced", err)
	}
}
