/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/noise"
	"github.com/bbmumford/loom/core/transport/manager"
	"github.com/bbmumford/swarm"
	swarmpb "github.com/bbmumford/swarm/proto/pb"
	"google.golang.org/protobuf/proto"
)

// dialPartitionRuntime is a Runtime whose shared transport holds partitions,
// with a connection manager and a record table and nothing else, so
// dialPartitionFor can be asked about peers without a network.
func dialPartitionRuntime(t *testing.T, partitions ...string) *Runtime {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	st, err := manager.NewSharedTransport(noise.NoiseTransportConfig{
		PrivateKey:  priv,
		NetworkKeys: []string{"dial-partition-test-key"},
		ListenAddr:  "127.0.0.1:0",
	})
	if err != nil {
		t.Fatalf("NewSharedTransport: %v", err)
	}
	t.Cleanup(func() { _ = st.Close() })
	for _, p := range partitions {
		if err := st.RegisterPartition(aether.ScopeID(p), [][]byte{[]byte("psk-" + p)}); err != nil {
			t.Fatal(err)
		}
	}
	table, err := NewRoleTable(nil, "vl1_self")
	if err != nil {
		t.Fatal(err)
	}
	return &Runtime{
		transportMgr:       manager.New(),
		sharedTransport:    st,
		listenTransports:   []aether.Transport{st},
		listenPartitionIDs: []string{""},
		connMgr:            &ConnectionManager{peers: map[string]*peerConn{}},
		swarm:              &SwarmIntegration{RoleTable: table},
	}
}

// taggedPeerRecord is a fleet.peer record carrying tags, as RoleTable receives
// it from gossip.
func taggedPeerRecord(t *testing.T, nodeID string, tags ...string) swarm.Record {
	t.Helper()
	body, err := proto.Marshal(&swarmpb.PeerRecord{
		IssuedAt:     time.Now().UnixNano(),
		Capabilities: &swarmpb.Capabilities{Tags: tags},
	})
	if err != nil {
		t.Fatal(err)
	}
	return swarm.Record{NodeID: swarm.NodeID(nodeID), Body: body}
}

// dialPartitionFor answers from the target's last session, then from the
// partition its record names, and uses either only while the shared transport
// holds that partition's keys. A target known under neither, or only under a
// partition this node does not hold, gets "", and no configured partition is
// given in its place.
func TestDialPartitionForTakesTheSessionThenTheRecord(t *testing.T) {
	rt := dialPartitionRuntime(t, "p-a", "p-b")
	rt.connMgr.peers["vl1_session"] = &peerConn{nodeID: "vl1_session", partition: "p-a"}
	rt.connMgr.peers["vl1_both"] = &peerConn{nodeID: "vl1_both", partition: "p-a"}
	rt.connMgr.peers["vl1_stale_session"] = &peerConn{nodeID: "vl1_stale_session", partition: "p-removed"}
	for _, rec := range []swarm.Record{
		taggedPeerRecord(t, "vl1_record", dialPartitionMetaKey+"=p-b"),
		taggedPeerRecord(t, "vl1_both", dialPartitionMetaKey+"=p-b"),
		taggedPeerRecord(t, "vl1_stale_session", dialPartitionMetaKey+"=p-b"),
		taggedPeerRecord(t, "vl1_foreign", dialPartitionMetaKey+"=p-z"),
		taggedPeerRecord(t, "vl1_untagged", "region=test"),
	} {
		if err := rt.swarm.RoleTable.onRecord(rec); err != nil {
			t.Fatal(err)
		}
	}

	for _, tc := range []struct{ target, want, why string }{
		{"vl1_session", "p-a", "the partition its last session was bound to"},
		{"vl1_both", "p-a", "the session, which a handshake proved, over the record"},
		{"vl1_record", "p-b", "the partition its record names"},
		{"vl1_stale_session", "p-b", "the record, when the session's partition is no longer held"},
		{"vl1_foreign", "", "none: its record names a partition this node holds no keys for"},
		{"vl1_untagged", "", "none: its record names no partition"},
		{"vl1_unknown", "", "none: nothing names a partition for it"},
	} {
		if got := rt.dialPartitionFor(aether.NodeID(tc.target)); got != tc.want {
			t.Errorf("dialPartitionFor(%s) = %q, want %q (%s)", tc.target, got, tc.want, tc.why)
		}
	}
}

// A shared transport holding one partition is advertised under it, one holding
// several is advertised under none, and publishOnce writes exactly what
// advertisedDialPartition answers onto the record bytes.
func TestPublishOnceAdvertisesOnlyASoleDialPartition(t *testing.T) {
	published := func(rt *Runtime) []string {
		node := &capturingPublishNode{stubSwarmNode: &stubSwarmNode{}}
		NewPeerPublisher(node, rt, nil).publishOnce()
		var rec swarmpb.PeerRecord
		if err := proto.Unmarshal(node.body, &rec); err != nil {
			t.Fatalf("published bytes do not decode as a PeerRecord: %v", err)
		}
		var got []string
		for _, tag := range rec.GetCapabilities().GetTags() {
			if strings.HasPrefix(tag, dialPartitionMetaKey+"=") {
				got = append(got, tag)
			}
		}
		return got
	}

	sole := dialPartitionRuntime(t, "p-only")
	sole.identity = bridgeTestRuntime(t).identity
	if p, ok := sole.advertisedDialPartition(); !ok || p != "p-only" {
		t.Fatalf("advertisedDialPartition = %q, %v; want p-only, true", p, ok)
	}
	if got := published(sole); len(got) != 1 || got[0] != dialPartitionMetaKey+"=p-only" {
		t.Fatalf("published dial partition tags = %q, want [%s=p-only]", got, dialPartitionMetaKey)
	}

	several := dialPartitionRuntime(t, "p-a", "p-b")
	several.identity = bridgeTestRuntime(t).identity
	if p, ok := several.advertisedDialPartition(); ok {
		t.Fatalf("advertisedDialPartition = %q on a transport holding two partitions, want none", p)
	}
	if got := published(several); len(got) != 0 {
		t.Fatalf("published dial partition tags = %q with two partitions held, want none", got)
	}

	if p, ok := (&Runtime{}).advertisedDialPartition(); ok {
		t.Fatalf("advertisedDialPartition = %q with no shared transport, want none", p)
	}
}

// startSharedOnlyRuntime starts a runtime whose one VL1 transport is the shared
// transport, holding the one partition partition, the shape a composition root
// gives a node that serves one partition.
func startSharedOnlyRuntime(t *testing.T, partition string) (*Runtime, int) {
	t.Helper()
	dir := t.TempDir()
	port := freeUDPPort(t)
	rt, err := Initialize(Config{
		ServiceName: "dial-partition-test",
		DataDir:     dir,
		NetworkKeys: []string{"dial-partition-test-network-key"},
		Partitions: []PartitionTransportConfig{
			{PartitionID: partition, NetworkKeys: []string{"dial-partition-test-psk"}},
		},
		VL1: VL1TransportConfig{Enabled: true, UDPPort: port},
		LAD: LADDirectoryConfig{Storage: "local", LocalDir: filepath.Join(dir, "lad"), BootstrapHosts: "127.0.0.1:9"},
	})
	if err != nil {
		t.Fatalf("Initialize: %v", err)
	}
	t.Cleanup(func() { _ = rt.Shutdown() })
	if rt.sharedTransport == nil || rt.unpartitionedTransport != nil || len(rt.listenTransports) != 1 {
		t.Fatal("the runtime's only VL1 transport is not the shared transport")
	}
	return rt, port
}

// Two runtimes whose only VL1 transport is shared, holding the same partition,
// connect over noise-UDP. Before the target's record arrives the dial is
// refused; the dial after it takes the partition the record names; and the
// session it opens is bound to that partition on both ends, so each records it
// for its next dial to the other (TestDialPartitionForTakesTheSessionThenTheRecord
// pins that the session is read before any record). The target's dial back
// meets aether's simultaneous-dial rule (the existing inbound session wins),
// which is not what this test measures, so it asserts only that the shared
// transport did not refuse it for want of a partition.
func TestSharedOnlyRuntimesDialUnderTheTargetsPartition(t *testing.T) {
	const partition = "p-shared"
	a, _ := startSharedOnlyRuntime(t, partition)
	b, bPort := startSharedOnlyRuntime(t, partition)
	idA, idB := string(a.identity.NodeID), string(b.identity.NodeID)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	toB := aether.Target{NodeID: b.identity.NodeID, Address: fmt.Sprintf("127.0.0.1:%d", bPort)}

	if _, err := a.dialNoiseUDP(ctx, toB); !errors.Is(err, manager.ErrSharedNoPartitionInCtx) &&
		(err == nil || !strings.Contains(err.Error(), manager.ErrSharedNoPartitionInCtx.Error())) {
		t.Fatalf("dial with no record of the target = %v, want the shared transport's refusal (%v)",
			err, manager.ErrSharedNoPartitionInCtx)
	}

	node := &capturingPublishNode{stubSwarmNode: &stubSwarmNode{}}
	NewPeerPublisher(node, b, nil).publishOnce()
	if err := a.swarm.RoleTable.onRecord(swarm.Record{NodeID: swarm.NodeID(idB), Body: node.body}); err != nil {
		t.Fatal(err)
	}

	conn, err := a.dialNoiseUDP(ctx, toB)
	if err != nil {
		t.Fatalf("dial under the partition the target's record names: %v", err)
	}
	bs, ok := installableMeshConn(conn)
	if !ok {
		t.Fatalf("dial returned %T", conn)
	}
	if got := transportPartitionOf(bs.Conn); got != partition {
		t.Fatalf("dialled session bound to %q, want %q", got, partition)
	}
	go a.DialAndAcceptMesh(a.ctx, bs.Conn, idB, "", ProtoNoiseUDP, "", "", "", dialBorrowsConnectingState)

	waitMeshDB(t, 20*time.Second, "both ends to record the session's partition", func() bool {
		return a.connMgr.sessionPartition(idB) == partition && b.connMgr.sessionPartition(idA) == partition
	})

	if got := b.dialPartitionFor(a.identity.NodeID); got != partition {
		t.Fatalf("the target resolves the dialler's partition as %q, want %q from the session", got, partition)
	}
	back, err := b.dialNoiseUDP(ctx, aether.Target{NodeID: a.identity.NodeID, Address: fmt.Sprintf("127.0.0.1:%d", a.cfg.VL1.UDPPort)})
	if err != nil && strings.Contains(err.Error(), manager.ErrSharedNoPartitionInCtx.Error()) {
		t.Fatalf("dial back refused for want of a partition: %v", err)
	}
	if err == nil {
		_ = back.Close()
	}
}
