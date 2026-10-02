/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package manager

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/noise"
)

// makeTestTransport creates a NoiseTransport with the given network keys and a short handshake timeout.
func makeTestTransport(t *testing.T, keys []string) (*noise.NoiseTransport, aether.NodeID) {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	pub := priv.Public().(ed25519.PublicKey)
	nodeID, err := aether.NewNodeID(pub)
	if err != nil {
		t.Fatal(err)
	}
	nt, err := noise.NewNoiseTransport(noise.NoiseTransportConfig{
		PrivateKey:       priv,
		LocalNode:        nodeID,
		NetworkKeys:      keys,
		ListenAddr:       "127.0.0.1:0",
		HandshakeTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	return nt, nodeID
}

// TestCrossPartitionDialRejection verifies that partition alpha's transport
// cannot complete a handshake with partition beta's listener when they use
// different PSKs.
func TestCrossPartitionDialRejection(t *testing.T) {
	tA, _ := makeTestTransport(t, []string{"psk-alpha"})
	tB, nodeB := makeTestTransport(t, []string{"psk-beta"})

	mgr := New()
	mgr.Register("alpha", tA)
	mgr.Register("beta", tB)
	defer mgr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Start the listener on partition beta's transport.
	listener, err := tB.Listen(ctx)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	serverAddr := listener.Addr().String()

	// Dial through TransportManager as partition alpha → targets beta's listener.
	// Alpha uses "psk-alpha" as prologue; beta's listener expects "psk-beta".
	// The handshake must fail because the prologues don't match.
	_, err = mgr.Dial(ctx, "alpha", aether.Target{
		Address: serverAddr,
		NodeID:  nodeB,
	})
	if err == nil {
		t.Fatal("expected handshake to fail due to PSK mismatch, but dial succeeded")
	}
	t.Logf("Cross-partition dial correctly rejected: %v", err)
}

// TestPreambleSpoofingRejection verifies that an attacker who names one
// partition in the preamble but holds another partition's PSK is rejected. The
// responder resolves the named partition's keys from the preamble, which do not
// match the attacker's CRC32 fingerprint (derived from the key it holds).
func TestPreambleSpoofingRejection(t *testing.T) {
	// Create shared transport with two partitions.
	_, sharedPriv, _ := ed25519.GenerateKey(rand.Reader)

	st, err := NewSharedTransport(noise.NoiseTransportConfig{
		PrivateKey:       sharedPriv,
		NetworkKeys:      []string{"shared-default"},
		ListenAddr:       "127.0.0.1:0",
		HandshakeTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("new shared: %v", err)
	}

	honestKey := []byte("psk-honest-tenant")
	victimKey := []byte("psk-victim-tenant")
	st.RegisterPartition("honest", [][]byte{honestKey})
	st.RegisterPartition("victim", [][]byte{victimKey})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	listener, err := st.Listen(ctx)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	serverAddr := listener.Addr().String()

	// Create an attacker transport that holds honest-partition's key.
	_, attackerPriv, _ := ed25519.GenerateKey(rand.Reader)
	attackerPub := attackerPriv.Public().(ed25519.PublicKey)
	attackerNodeID, _ := aether.NewNodeID(attackerPub)

	attacker, err := noise.NewNoiseTransport(noise.NoiseTransportConfig{
		PrivateKey:       attackerPriv,
		LocalNode:        attackerNodeID,
		NetworkKeys:      []string{string(honestKey)},
		HandshakeTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("attacker transport: %v", err)
	}

	// Spoof: claim to be "victim" in preamble but use honest-partition's key.
	// The responder will resolve "victim" → victimKey, but the CRC32 fingerprint
	// of honestKey won't match victimKey → handshake rejected at key resolution.
	spoofCtx := noise.WithDialOverride(ctx, "victim", [][]byte{honestKey})

	_, err = attacker.Dial(spoofCtx, aether.Target{
		Address: serverAddr,
		NodeID:  "", // unknown, we just want the handshake to fail
	})
	if err == nil {
		t.Fatal("expected preamble spoofing to be rejected, but dial succeeded")
	}
	t.Logf("Preamble spoofing correctly rejected: %v", err)
}

// TestInfrastructureServesAllPartitions verifies that a shared transport listener
// accepts handshakes from multiple partitions, each using their own PSK.
// Each partition uses a separate dialer transport with its own keypair (Noise
// handshakes require different static keys on each side).
func TestInfrastructureServesAllPartitions(t *testing.T) {
	// Server: shared transport that accepts multiple partitions
	_, serverPriv, _ := ed25519.GenerateKey(rand.Reader)
	serverPub := serverPriv.Public().(ed25519.PublicKey)
	serverNodeID, _ := aether.NewNodeID(serverPub)

	keyA := make([]byte, 32)
	keyB := make([]byte, 32)
	copy(keyA, "psk-tenant-A-padded-to-32-bytes!")
	copy(keyB, "psk-tenant-B-padded-to-32-bytes!")

	st, err := NewSharedTransport(noise.NoiseTransportConfig{
		PrivateKey:       serverPriv,
		LocalNode:        serverNodeID,
		NetworkKeys:      []string{string(keyA), string(keyB)},
		ListenAddr:       "127.0.0.1:0",
		HandshakeTimeout: 3 * time.Second,
	})
	if err != nil {
		t.Fatalf("new shared: %v", err)
	}
	st.RegisterPartition("tenant-a", [][]byte{keyA})
	st.RegisterPartition("tenant-b", [][]byte{keyB})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	listener, err := st.Listen(ctx)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()
	serverAddr := listener.Addr().String()

	// Dialers: separate transports with their OWN keypairs (different from server).
	// Each must call Listen() to bind a UDP socket (shared-socket design).
	dialerA, _ := makeTestTransport(t, []string{string(keyA)})
	dialerB, _ := makeTestTransport(t, []string{string(keyB)})

	lisA, err := dialerA.Listen(ctx)
	if err != nil {
		t.Fatalf("dialer-a listen: %v", err)
	}
	defer lisA.Close()

	lisB, err := dialerB.Listen(ctx)
	if err != nil {
		t.Fatalf("dialer-b listen: %v", err)
	}
	defer lisB.Close()

	mgr := New()
	mgr.Register("tenant-a", dialerA)
	mgr.Register("tenant-b", dialerB)
	defer mgr.Close()

	// Accept sessions in background (drain so handshakes complete).
	acceptDone := make(chan struct{})
	go func() {
		defer close(acceptDone)
		for i := 0; i < 2; i++ {
			sess, err := listener.Accept(ctx)
			if err != nil {
				return
			}
			sess.Close()
		}
	}()

	// The first partition dials the server; its network key resolves authentication.
	sessA, err := mgr.Dial(ctx, "tenant-a", aether.Target{
		Address: serverAddr,
		NodeID:  serverNodeID,
	})
	if err != nil {
		t.Fatalf("tenant-a dial: %v", err)
	}
	defer sessA.Close()

	// The second partition dials the server.
	sessB, err := mgr.Dial(ctx, "tenant-b", aether.Target{
		Address: serverAddr,
		NodeID:  serverNodeID,
	})
	if err != nil {
		t.Fatalf("tenant-b dial: %v", err)
	}
	defer sessB.Close()

	// Verify client-side PartitionSession wrappers carry correct partition IDs.
	if sessA.PartitionID() != "tenant-a" {
		t.Errorf("sessA.PartitionID() = %q, want %q", sessA.PartitionID(), "tenant-a")
	}
	if sessB.PartitionID() != "tenant-b" {
		t.Errorf("sessB.PartitionID() = %q, want %q", sessB.PartitionID(), "tenant-b")
	}

	// Wait for both accepts to complete.
	select {
	case <-acceptDone:
	case <-time.After(15 * time.Second):
		t.Fatal("timeout waiting for server accepts")
	}
	t.Log("Infrastructure served both partitions successfully")
}

// TestDedicatedTransportIsolation verifies that a dedicated transport rejects
// connections from clients using a different PSK.
func TestDedicatedTransportIsolation(t *testing.T) {
	orbtrTransport, orbtrNodeID := makeTestTransport(t, []string{"orbtr-secret-psk"})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	listener, err := orbtrTransport.Listen(ctx)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	serverAddr := listener.Addr().String()

	// External client with wrong PSK tries to connect.
	externalTransport, _ := makeTestTransport(t, []string{"wrong-external-psk"})

	_, err = externalTransport.Dial(ctx, aether.Target{
		Address: serverAddr,
		NodeID:  orbtrNodeID,
	})
	if err == nil {
		t.Fatal("expected connection with wrong PSK to be rejected, but dial succeeded")
	}
	t.Logf("Dedicated transport correctly rejected wrong PSK: %v", err)
}
