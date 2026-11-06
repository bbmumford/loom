/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"
)

// A join through the seed under the network key both nodes hold for the
// partition binds the session to that partition on both ends: JoinSeed's stream
// names it, and once DialAndAcceptMesh registers the session each side records
// it as the other's session partition, which is the value the RPC path judges a
// principal against.
func TestAJoinUnderTheMeshKeyBindsTheSessionOnBothEndsToThePartition(t *testing.T) {
	seed := seedJoinTestRuntime(t, "seed", seedJoinTestKey, nil)
	host, roots := seedJoinTestServer(t, seed)
	joiner := seedJoinTestRuntime(t, "joiner", seedJoinTestKey, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	j, err := joiner.JoinSeed(ctx, host, roots)
	if err != nil {
		t.Fatalf("JoinSeed: %v", err)
	}
	if j.NodeID != string(seed.NodeID()) {
		t.Fatalf("the join names seed %s, want %s", j.NodeID, seed.NodeID())
	}
	if got := transportPartitionOf(j.Conn); got != "orbtr" {
		t.Fatalf("the joined stream is bound to %q, want %q", got, "orbtr")
	}
	go joiner.DialAndAcceptMesh(context.Background(), j.Conn, j.NodeID, "", ProtoTLS, host, j.ServiceName, "", dialBorrowsConnectingState)

	deadline := time.Now().Add(20 * time.Second)
	for {
		seedSide := seed.ConnManager().sessionPartition(string(joiner.NodeID()))
		joinerSide := joiner.ConnManager().sessionPartition(string(seed.NodeID()))
		if seedSide == "orbtr" && joinerSide == "orbtr" {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("session partitions: seed side %q, joiner side %q; want both %q", seedSide, joinerSide, "orbtr")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// A joiner refuses a seed whose proof is not made under a key the joiner holds
// for the partition, before it sends its own proof, and the seed then holds
// nothing for it.
func TestAJoinerRefusesASeedOnAnotherNetworkKey(t *testing.T) {
	seed := seedJoinTestRuntime(t, "seed", seedJoinTestKey+"-another", nil)
	host, roots := seedJoinTestServer(t, seed)
	joiner := seedJoinTestRuntime(t, "joiner", seedJoinTestKey, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := joiner.JoinSeed(ctx, host, roots); !errors.Is(err, ErrSeedJoinRefused) {
		t.Fatalf("JoinSeed to a seed on another key: err %v, want ErrSeedJoinRefused", err)
	}
	time.Sleep(500 * time.Millisecond)
	if held := joinerState(seed, string(joiner.NodeID())); len(held) > 0 {
		t.Errorf("the seed holds %v for a joiner that refused it", held)
	}
}

// Two nodes holding no partition join under their unpartitioned keys, and the
// stream names no partition, as an unpartitioned noise session names none.
func TestAJoinUnderTheUnpartitionedKeysBindsNoPartition(t *testing.T) {
	start := func(service string) *Runtime {
		t.Setenv(EnvNodeIdentitySeed, "")
		t.Setenv(EnvFlyMachineID, "")
		dir := t.TempDir()
		rt, err := Initialize(Config{
			ServiceName: service,
			DataDir:     dir,
			NetworkKeys: []string{seedJoinTestKey},
			VL1:         VL1TransportConfig{Enabled: true, UDPPort: freeUDPPort(t)},
			LAD:         LADDirectoryConfig{Storage: "local", LocalDir: filepath.Join(dir, "lad"), BootstrapHosts: "127.0.0.1:9"},
		})
		if err != nil {
			t.Fatalf("Initialize %s: %v", service, err)
		}
		t.Cleanup(func() { _ = rt.Shutdown() })
		return rt
	}
	seed := start("seed")
	host, roots := seedJoinTestServer(t, seed)
	joiner := start("joiner")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	j, err := joiner.JoinSeed(ctx, host, roots)
	if err != nil {
		t.Fatalf("JoinSeed: %v", err)
	}
	defer j.Conn.Close()
	if got := transportPartitionOf(j.Conn); got != "" {
		t.Fatalf("the joined stream is bound to %q, want no partition", got)
	}
}
