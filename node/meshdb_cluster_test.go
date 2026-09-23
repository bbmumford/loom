/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ORBTR/aether"
	"github.com/bbmumford/swarm"
	meshdbcfg "github.com/bbmumford/tursoraft/manager"
	"github.com/bbmumford/tursoraft/sqldtest"
)

func freeUDPPort(t *testing.T) int {
	t.Helper()
	c, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	return c.LocalAddr().(*net.UDPAddr).Port
}

// clusterMember is one runtime of the three-node test cluster and the
// directory it keeps its identity, LAD file, replica and Raft state in.
type clusterMember struct {
	dir  string
	port int
	rt   *Runtime
}

const clusterGroup, clusterDB = "control", "ctl"

func startClusterMember(t *testing.T, m *clusterMember, primary, key string) {
	t.Helper()
	m.port = freeUDPPort(t)
	cfg := Config{
		ServiceName: meshDBTestService,
		DataDir:     m.dir,
		NetworkKeys: []string{"meshdb-cluster-test-network-key"},
		VL1:         VL1TransportConfig{Enabled: true, UDPPort: m.port},
		LAD:         LADDirectoryConfig{Storage: "local", LocalDir: filepath.Join(m.dir, "lad"), BootstrapHosts: "127.0.0.1:9"},
		Databases: &DatabasesConfig{
			Enabled:   true,
			LocalDir:  filepath.Join(m.dir, "db"),
			VolumeDir: m.dir,
			Groups: []meshdbcfg.GroupConfig{{
				GroupName:     clusterGroup,
				DBNames:       []string{clusterDB},
				Databases:     []meshdbcfg.DatabaseConfig{{Name: clusterDB, URL: primary, AuthToken: "test"}},
				EncryptionKey: key,
				Raft:          &meshdbcfg.RaftSpec{VoterServices: []string{meshDBTestService}, VoterCount: 3, BootstrapExpect: 3},
			}},
		},
	}
	rt, err := Initialize(cfg)
	if err != nil {
		t.Fatalf("Initialize %s: %v", m.dir, err)
	}
	m.rt = rt
}

// connectMembers dials b's VL1 listener from a and runs the mesh session
// lifecycle on it, as the connection manager does after a dial, then waits
// for both transports to bind the session.
func connectMembers(t *testing.T, a, b *clusterMember) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	target := aether.Target{NodeID: b.rt.identity.NodeID, Address: fmt.Sprintf("127.0.0.1:%d", b.port)}
	conn, err := a.rt.dialNoiseUDP(ctx, target)
	if err != nil {
		t.Fatalf("dial %s: %v", b.dir, err)
	}
	bs, ok := installableMeshConn(conn)
	if !ok {
		t.Fatalf("dial returned %T", conn)
	}
	go a.rt.DialAndAcceptMesh(a.rt.ctx, bs.Conn, string(b.rt.identity.NodeID), "", ProtoNoiseUDP, "", "", "", dialBorrowsConnectingState)
	idA, idB := string(a.rt.identity.NodeID), string(b.rt.identity.NodeID)
	waitMeshDB(t, 20*time.Second, "both ends to bind the session", func() bool {
		return a.rt.meshDBTransport.hasLiveSession(idB) && b.rt.meshDBTransport.hasLiveSession(idA)
	})
}

func connectAll(t *testing.T, ms []*clusterMember) {
	t.Helper()
	for i := range ms {
		for j := i + 1; j < len(ms); j++ {
			connectMembers(t, ms[i], ms[j])
		}
	}
}

// agreedCluster waits until every member reports the same leader and cluster
// ID for the database, and returns them.
func agreedCluster(t *testing.T, ms []*clusterMember, within time.Duration) (leader, clusterID string) {
	t.Helper()
	var last string
	waitMeshDB(t, within, "one leader and one cluster ID on every member", func() bool {
		leader, clusterID = "", ""
		for i, m := range ms {
			h, err := m.rt.MeshDB().Health(clusterGroup, clusterDB)
			if err != nil || h.Leader == "" || h.ClusterID == "" {
				last = fmt.Sprintf("member %d: leader=%q cluster=%q err=%v", i, h.Leader, h.ClusterID, err)
				return false
			}
			if i == 0 {
				leader, clusterID = h.Leader, h.ClusterID
			} else if h.Leader != leader || h.ClusterID != clusterID {
				last = fmt.Sprintf("member %d sees leader=%s cluster=%s, member 0 %s %s", i, h.Leader, h.ClusterID, leader, clusterID)
				return false
			}
		}
		return true
	})
	if leader == "" {
		t.Fatalf("no agreement: %s", last)
	}
	return leader, clusterID
}

func shutdownMembers(t *testing.T, ms []*clusterMember) {
	t.Helper()
	var wg sync.WaitGroup
	for _, m := range ms {
		if m.rt == nil {
			continue
		}
		wg.Add(1)
		go func(m *clusterMember) {
			defer wg.Done()
			if err := m.rt.Shutdown(); err != nil {
				t.Errorf("Shutdown %s: %v", m.dir, err)
			}
			m.rt = nil
		}(m)
	}
	wg.Wait()
}

// Three runtimes on loopback VL1 form one tursoraft cluster over the mesh
// database transport, while swarm keeps working on its own stream, and a
// restart on the same directories rejoins the same cluster instead of
// bootstrapping a second one.
func TestMeshDBClusterOnLoopbackVL1(t *testing.T) {
	if testing.Short() {
		t.Skip("boots three runtimes and a sqld")
	}
	t.Setenv(EnvNodeIdentitySeed, "")
	t.Setenv(EnvFlyMachineID, "")
	primary := sqldtest.Start(t).URL()
	key := testGroupKey(t)
	ms := []*clusterMember{{dir: t.TempDir()}, {dir: t.TempDir()}, {dir: t.TempDir()}}
	t.Cleanup(func() { shutdownMembers(t, ms) })
	for _, m := range ms {
		startClusterMember(t, m, primary, key)
	}
	connectAll(t, ms)
	leader, clusterID := agreedCluster(t, ms, 90*time.Second)
	t.Logf("leader %s, cluster %s", truncID(leader), clusterID)

	// Swarm on its own stream: a record published by one member reaches the
	// other two alongside the Raft traffic on the mesh database stream.
	const topic = swarm.Topic("meshdb.clustertest")
	got := make(chan string, 4)
	for _, m := range ms[1:] {
		unsub, err := m.rt.swarm.Node.Subscribe(topic, func(r swarm.Record) error {
			if !r.Tombstone {
				got <- string(r.Body)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
		defer unsub()
	}
	if err := ms[0].rt.swarm.Node.Publish(topic, []byte("over stream 100")); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		select {
		case body := <-got:
			if body != "over stream 100" {
				t.Fatalf("swarm delivered %q", body)
			}
		case <-time.After(20 * time.Second):
			t.Fatalf("swarm delivered the record to %d of 2 peers", i)
		}
	}
	for _, m := range ms {
		for _, other := range ms {
			if m == other {
				continue
			}
			if !channelUp(m.rt.meshDBTransport, string(other.rt.identity.NodeID)) {
				t.Fatalf("no mesh database channel from %s to %s", m.dir, other.dir)
			}
		}
	}

	// A member that shuts down withdraws its slot while the swarm still
	// carries it: Runtime.Shutdown stops the manager before the swarm
	// transport, so the other members stop seeing it publish under the key.
	slot := meshdbcfg.MeshKey(clusterGroup, clusterDB)
	gone := string(ms[2].rt.identity.NodeID)
	shutdownMembers(t, ms[2:])
	waitMeshDB(t, 10*time.Second, "the stopped member's slot to be withdrawn", func() bool {
		return !ms[0].rt.meshDBTransport.dir.published(slot, gone) && !ms[1].rt.meshDBTransport.dir.published(slot, gone)
	})

	// Restart every member on its own directories.
	shutdownMembers(t, ms)
	for _, m := range ms {
		startClusterMember(t, m, primary, key)
	}
	connectAll(t, ms)
	_, again := agreedCluster(t, ms, 90*time.Second)
	if again != clusterID {
		t.Fatalf("cluster ID after restart = %s, want %s: a restart bootstrapped a second cluster", again, clusterID)
	}
}
