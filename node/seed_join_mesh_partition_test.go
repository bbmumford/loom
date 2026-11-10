/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	rpcscope "github.com/bbmumford/loom/pkg/rpc/scope"
	swarmpb "github.com/bbmumford/swarm/proto/pb"
	"google.golang.org/protobuf/proto"
)

// A node whose shared transport holds the mesh's partition "orbtr" and one
// more, as an infrastructure node's does once its partition-key source lists a
// partition, joins through a seed under the mesh's partition: the joined stream
// and the session on both ends are bound to "orbtr", and a principal-bearing
// call the joiner sends over that session reaches the seed's consumer admitter
// under "orbtr" and is admitted. Bound to no partition, the seed's RPC path
// refuses every mesh principal before the admitter is asked.
func TestAJoinerHoldingTheMeshPartitionAndAnotherJoinsUnderTheMeshPartition(t *testing.T) {
	seed := seedJoinTestRuntime(t, "seed", seedJoinTestKey, nil)
	probe := &typedPrincipalProbe{}
	seed.Registry().SetPresencePolicy(typedPrincipalScopePolicy())
	if err := seed.Registry().RegisterRPC(probe); err != nil {
		t.Fatal(err)
	}
	seed.RPCServer().SetAuthValidator(typedPrincipalValidator{})
	host, roots := seedJoinTestServer(t, seed)

	joiner := seedJoinTestRuntime(t, "joiner", seedJoinTestKey, nil)
	if err := joiner.RegisterPartition("customer-1", [][]byte{[]byte("customer-1-key")}); err != nil {
		t.Fatalf("RegisterPartition: %v", err)
	}
	if got := joiner.PartitionCount(); got != 2 {
		t.Fatalf("the joiner's shared transport holds %d partitions, want 2", got)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	j, err := joiner.JoinSeed(ctx, host, roots)
	if err != nil {
		t.Fatalf("JoinSeed: %v", err)
	}
	if got := transportPartitionOf(j.Conn); got != "orbtr" {
		t.Errorf("the joined stream is bound to %q, want %q", got, "orbtr")
	}
	go joiner.DialAndAcceptMesh(context.Background(), j.Conn, j.NodeID, "", ProtoTLS, host, j.ServiceName, "", dialBorrowsConnectingState)

	for {
		resp, handled, err := joiner.ConnManager().CallViaBidi(ctx, string(seed.NodeID()), typedPrincipalRequest())
		if handled {
			if err != nil {
				t.Errorf("the principal call over the joined session: %v", err)
			} else if !resp.Success || atomic.LoadInt32(&probe.calls) != 1 {
				t.Errorf("the principal call over the joined session was refused: %q (handler calls %d)",
					resp.Error, atomic.LoadInt32(&probe.calls))
			} else if pinned := probe.got.Identity().Scope(rpcscope.AxisTransportPinned); pinned != "orbtr" {
				t.Errorf("the admitted principal is pinned to %q, want %q", pinned, "orbtr")
			}
			break
		}
		if ctx.Err() != nil {
			t.Fatal("no bidi to the seed was registered on the joined session")
		}
		time.Sleep(100 * time.Millisecond)
	}

	seedSide := seed.ConnManager().sessionPartition(string(joiner.NodeID()))
	joinerSide := joiner.ConnManager().sessionPartition(string(seed.NodeID()))
	if seedSide != "orbtr" || joinerSide != "orbtr" {
		t.Errorf("session partitions: seed side %q, joiner side %q; want both %q", seedSide, joinerSide, "orbtr")
	}
}

// A node whose shared transport holds one partition and not the mesh's, as a
// node serving one partition's transport does, joins a seed holding the mesh's
// partition and that one under its own partition: naming a mesh partition it
// does not hold leaves its join to the partition it holds.
func TestAJoinerHoldingOnePartitionAndNotTheMeshsJoinsUnderItsOwn(t *testing.T) {
	const relayKey = "relay-partition-key"
	seed := seedJoinTestRuntime(t, "seed", seedJoinTestKey, nil)
	if err := seed.RegisterPartition("p-relay", [][]byte{[]byte(relayKey)}); err != nil {
		t.Fatalf("RegisterPartition: %v", err)
	}
	host, roots := seedJoinTestServer(t, seed)

	t.Setenv(EnvNodeIdentitySeed, "")
	t.Setenv(EnvFlyMachineID, "")
	dir := t.TempDir()
	joiner, err := Initialize(Config{
		ServiceName:   "joiner",
		DataDir:       dir,
		NetworkKeys:   []string{seedJoinTestKey},
		Partitions:    []PartitionTransportConfig{{PartitionID: "p-relay", NetworkKeys: []string{relayKey}}},
		MeshPartition: "orbtr",
		VL1:           VL1TransportConfig{Enabled: true, UDPPort: freeUDPPort(t)},
		LAD:           LADDirectoryConfig{Storage: "local", LocalDir: filepath.Join(dir, "lad"), BootstrapHosts: "127.0.0.1:9"},
	})
	if err != nil {
		t.Fatalf("Initialize joiner: %v", err)
	}
	t.Cleanup(func() { _ = joiner.Shutdown() })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	j, err := joiner.JoinSeed(ctx, host, roots)
	if err != nil {
		t.Fatalf("JoinSeed: %v", err)
	}
	if got := transportPartitionOf(j.Conn); got != "p-relay" {
		t.Fatalf("the joined stream is bound to %q, want %q", got, "p-relay")
	}
	go joiner.DialAndAcceptMesh(context.Background(), j.Conn, j.NodeID, "", ProtoTLS, host, j.ServiceName, "", dialBorrowsConnectingState)
	for {
		seedSide := seed.ConnManager().sessionPartition(string(joiner.NodeID()))
		joinerSide := joiner.ConnManager().sessionPartition(string(seed.NodeID()))
		if seedSide == "p-relay" && joinerSide == "p-relay" {
			return
		}
		if ctx.Err() != nil {
			t.Fatalf("session partitions: seed side %q, joiner side %q; want both %q", seedSide, joinerSide, "p-relay")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// The record a node publishes names cfg.MeshPartition while its shared
// transport holds it beside other partitions, and names none for several
// partitions when the mesh's is not among them; publishOnce writes exactly that
// onto the record bytes.
func TestPublishOnceAdvertisesTheMeshPartitionAmongSeveral(t *testing.T) {
	withMesh := dialPartitionRuntime(t, "customer-1", "orbtr")
	withMesh.identity = bridgeTestRuntime(t).identity
	withMesh.cfg.MeshPartition = "orbtr"
	if p, ok := withMesh.advertisedDialPartition(); !ok || p != "orbtr" {
		t.Fatalf("advertisedDialPartition = %q, %v; want orbtr, true", p, ok)
	}
	if p := withMesh.seedJoinPartition(); p != "orbtr" {
		t.Fatalf("seedJoinPartition = %q, want orbtr", p)
	}
	node := &capturingPublishNode{stubSwarmNode: &stubSwarmNode{}}
	NewPeerPublisher(node, withMesh, nil).publishOnce()
	var rec swarmpb.PeerRecord
	if err := proto.Unmarshal(node.body, &rec); err != nil {
		t.Fatalf("published bytes do not decode as a PeerRecord: %v", err)
	}
	var tags []string
	for _, tag := range rec.GetCapabilities().GetTags() {
		if strings.HasPrefix(tag, dialPartitionMetaKey+"=") {
			tags = append(tags, tag)
		}
	}
	if len(tags) != 1 || tags[0] != dialPartitionMetaKey+"=orbtr" {
		t.Fatalf("published dial partition tags = %q, want [%s=orbtr]", tags, dialPartitionMetaKey)
	}

	without := dialPartitionRuntime(t, "p-a", "p-b")
	without.cfg.MeshPartition = "orbtr"
	if p, ok := without.advertisedDialPartition(); ok {
		t.Fatalf("advertisedDialPartition = %q holding two partitions and not the mesh's, want none", p)
	}
	if p := without.seedJoinPartition(); p != "" {
		t.Fatalf("seedJoinPartition = %q holding two partitions and not the mesh's, want none", p)
	}
}
