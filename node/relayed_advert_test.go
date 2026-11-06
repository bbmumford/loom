/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bbmumford/loom/pkg/rpc"
	"github.com/bbmumford/loom/ports"
	"github.com/bbmumford/swarm"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// Three nodes join one mesh through one seed, as endpoints on the shared seed
// list do: the earlier joiner and the later joiner each hold a session to the
// seed and none to each other. The later joiner's advert reaches the earlier
// joiner through the seed within one gossip interval of the seed holding it,
// because the earlier joiner's routing and resolution-side checks read that
// advert from its own role table, and a node holding no session to the
// publisher has no other source for it. Which end of the later joiner's session
// opens the swarm stream follows the two node IDs (RegisterPeer), so each order
// is run: the end that waits for the stream holds no swarm peer when the session
// joins.
func TestALaterJoinersAdvertReachesAnEarlierJoinerThroughTheSeedWithinTheGossipInterval(t *testing.T) {
	if testing.Short() {
		t.Skip("starts three mesh runtimes")
	}
	for _, tc := range []struct {
		name      string
		lateOpens bool
	}{
		{"the later joiner opens the swarm stream", true},
		{"the seed opens the swarm stream", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			seedKey, lateKey := relayTestSeedPair(t, tc.lateOpens)
			seed := relayTestRuntime(t, "seed", seedKey)
			host, roots := seedJoinTestServer(t, seed)
			publishRelayTestOp(t, seed, "AtSeed")

			early := relayTestRuntime(t, "early", nil)
			publishRelayTestOp(t, early, "AtEarly")
			joinRelayTestSeed(t, early, host, roots)
			waitRelayTest(t, 30*time.Second, "the seed and the earlier joiner to hold each other's adverts", func() bool {
				return holdsRelayTestAdvert(seed, "AtEarly", early) && holdsRelayTestAdvert(early, "AtSeed", seed)
			})

			late := relayTestRuntime(t, "late", lateKey)
			if (string(late.NodeID()) < string(seed.NodeID())) != tc.lateOpens {
				t.Fatalf("the later joiner %s and the seed %s are not in the order this case runs", late.NodeID(), seed.NodeID())
			}
			publishRelayTestOp(t, late, "AtLate")
			joinRelayTestSeed(t, late, host, roots)
			waitRelayTest(t, 30*time.Second, "the seed to hold the later joiner's advert", func() bool {
				return holdsRelayTestAdvert(seed, "AtLate", late)
			})
			held := time.Now()

			interval := seed.cfg.GossipInterval
			deadline := held.Add(interval + time.Second)
			for !holdsRelayTestAdvert(early, "AtLate", late) {
				if time.Now().After(deadline) {
					t.Fatalf("the earlier joiner does not hold the later joiner's advert %v after the seed held it; "+
						"the gossip interval is %v", time.Since(held).Round(time.Millisecond), interval)
				}
				time.Sleep(100 * time.Millisecond)
			}
			t.Logf("the earlier joiner held the later joiner's advert %v after the seed did (gossip interval %v)",
				time.Since(held).Round(time.Millisecond), interval)
			if _, ok := early.ConnManager().MeshSessionForNode(string(late.NodeID())); ok {
				t.Fatalf("the earlier joiner holds a session to the later joiner, so the advert need not have come through the seed")
			}
		})
	}
}

// relayTestSeedPair returns identity seeds for the seed node and the later
// joiner whose node IDs order as the case needs: the later joiner's below the
// seed's when it opens the swarm stream, above it otherwise.
func relayTestSeedPair(t *testing.T, lateOpens bool) (seedKey, lateKey []byte) {
	t.Helper()
	for i := 0; i < 64; i++ {
		a, b := make([]byte, ed25519.SeedSize), make([]byte, ed25519.SeedSize)
		_, _ = rand.Read(a)
		_, _ = rand.Read(b)
		ia, err := identityFromSeed(a)
		if err != nil {
			t.Fatal(err)
		}
		ib, err := identityFromSeed(b)
		if err != nil {
			t.Fatal(err)
		}
		if (string(ib.NodeID) < string(ia.NodeID)) == lateOpens {
			return a, b
		}
	}
	t.Fatal("no identity pair in the needed order")
	return nil, nil
}

// relayTestRuntime starts a runtime as seedJoinTestRuntime does, on the identity
// key's node ID when key is given and on a fresh one otherwise.
func relayTestRuntime(t *testing.T, service string, key []byte) *Runtime {
	t.Helper()
	t.Setenv(EnvNodeIdentitySeed, "")
	if key != nil {
		t.Setenv(EnvNodeIdentitySeed, base64.StdEncoding.EncodeToString(key))
	}
	t.Setenv(EnvFlyMachineID, "")
	dir := t.TempDir()
	rt, err := Initialize(Config{
		ServiceName: service,
		DataDir:     dir,
		NetworkKeys: []string{seedJoinTestKey},
		Partitions:  []PartitionTransportConfig{{PartitionID: "orbtr", NetworkKeys: []string{seedJoinTestKey}}},
		VL1:         VL1TransportConfig{Enabled: true, UDPPort: freeUDPPort(t)},
		LAD:         LADDirectoryConfig{Storage: "local", LocalDir: filepath.Join(dir, "lad"), BootstrapHosts: "127.0.0.1:9"},
	})
	if err != nil {
		t.Fatalf("Initialize %s: %v", service, err)
	}
	t.Cleanup(func() { _ = rt.Shutdown() })
	return rt
}

// relayTestFQN is the operation each node of the relay test advertises.
func relayTestFQN(op string) string { return "orbtr.relaytest." + op }

// publishRelayTestOp registers orbtr.relaytest.<op>, which answers with op, on a
// registry of rt's own, exports it to the handler registry rt's RPC server runs
// from and publishes rt's peer record with it, as the endpoint spine does for
// what its features register.
func publishRelayTestOp(t *testing.T, rt *Runtime, op string) {
	t.Helper()
	reg := rpc.NewRegistry()
	if err := reg.Register(rpc.Handler{
		Namespace: "orbtr", Domain: "relaytest", Operation: op, Scope: rpc.ScopeNone,
		Request: (*wrapperspb.StringValue)(nil), Response: (*wrapperspb.StringValue)(nil),
		Func: func(context.Context, proto.Message) (proto.Message, error) { return wrapperspb.String(op), nil },
	}); err != nil {
		t.Fatal(err)
	}
	if err := reg.ExportToHandlerRegistry(rt.Registry()); err != nil {
		t.Fatalf("ExportToHandlerRegistry: %v", err)
	}
	if err := rt.PublishRPCHandlersToLAD(context.Background(), reg); err != nil {
		t.Fatalf("PublishRPCHandlersToLAD: %v", err)
	}
}

// joinRelayTestSeed joins rt to the seed at host and hands the joined stream to
// rt's DialAndAcceptMesh as the initiator, as the endpoint spine's seed dial does.
func joinRelayTestSeed(t *testing.T, rt *Runtime, host string, roots *x509.CertPool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	j, err := rt.JoinSeed(ctx, host, roots)
	if err != nil {
		t.Fatalf("JoinSeed: %v", err)
	}
	go rt.DialAndAcceptMesh(context.Background(), j.Conn, j.NodeID, "", ProtoTLS, host, j.ServiceName, "", dialBorrowsConnectingState)
}

// holdsRelayTestAdvert reports whether reader's live directory holds server's
// advert of orbtr.relaytest.<op>.
func holdsRelayTestAdvert(reader *Runtime, op string, server *Runtime) bool {
	dir := reader.LiveDirectory()
	if dir == nil {
		return false
	}
	adverts, err := dir.HandlersByName(context.Background(), "", relayTestFQN(op))
	if err != nil {
		return false
	}
	return slices.ContainsFunc(adverts, func(a ports.HandlerAdvert) bool { return string(a.NodeID) == string(server.NodeID()) })
}

func waitRelayTest(t *testing.T, within time.Duration, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %v waiting for %s", within, what)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// Each end of a session calls the attach callback once its swarm stream is in
// place, the claiming end included, whose stream binds after RegisterPeer has
// returned: a frame the callback sends reaches the other end.
func TestTheAttachCallbackRunsOnEachEndOnceItsSwarmStreamIsInPlace(t *testing.T) {
	sa, sb := reopenPair(t)
	ta := NewMeshSwarmTransport(swarm.NodeID("vl1_aaa"))
	tb := NewMeshSwarmTransport(swarm.NodeID("vl1_bbb"))
	t.Cleanup(ta.Stop)
	t.Cleanup(tb.Stop)
	var atA, atB atomic.Int64
	ta.OnReceive(func(swarm.NodeID, []byte) { atA.Add(1) })
	tb.OnReceive(func(swarm.NodeID, []byte) { atB.Add(1) })
	sent := make(chan error, 2)
	ta.onStreamAttached(func(id swarm.NodeID) { sent <- ta.Send(id, []byte("from the opener")) })
	tb.onStreamAttached(func(id swarm.NodeID) { sent <- tb.Send(id, []byte("from the claimer")) })

	ctx := context.Background()
	if err := tb.RegisterPeer(ctx, "vl1_aaa", sb); err != nil {
		t.Fatal(err)
	}
	if err := ta.RegisterPeer(ctx, "vl1_bbb", sa); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		select {
		case err := <-sent:
			if err != nil {
				t.Fatalf("a frame sent from the attach callback: %v", err)
			}
		case <-time.After(20 * time.Second):
			t.Fatalf("the attach callback ran on %d of the two ends", i)
		}
	}
	reopenWaitFor(t, "a frame sent from an attach callback did not arrive", func() bool {
		return atA.Load() == 1 && atB.Load() == 1
	})
}
