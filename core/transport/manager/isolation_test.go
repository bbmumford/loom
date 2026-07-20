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

// TestCrossTenantDialRejection verifies that Tenant A's transport cannot complete
// a handshake with Tenant B's listener when they use different PSKs.
func TestCrossTenantDialRejection(t *testing.T) {
	tA, _ := makeTestTransport(t, []string{"psk-alpha"})
	tB, nodeB := makeTestTransport(t, []string{"psk-beta"})

	mgr := New()
	mgr.Register("alpha", tA)
	mgr.Register("beta", tB)
	defer mgr.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Start listener on Tenant B's aether.
	listener, err := tB.Listen(ctx)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	serverAddr := listener.Addr().String()

	// Dial through TransportManager as Tenant A → targets Tenant B's listener.
	// Tenant A uses "psk-alpha" as prologue; Tenant B's listener expects "psk-beta".
	// The handshake must fail because the prologues don't match.
	_, err = mgr.Dial(ctx, "alpha", aether.Target{
		Address: serverAddr,
		NodeID:  nodeB,
	})
	if err == nil {
		t.Fatal("expected handshake to fail due to PSK mismatch, but dial succeeded")
	}
	t.Logf("Cross-tenant dial correctly rejected: %v", err)
}

// TestPreambleSpoofingRejection verifies that an attacker who claims to be
// Tenant B in the preamble but uses Tenant A's PSK is rejected. The responder
// resolves Tenant B's keys from the preamble, which won't match the attacker's
// CRC32 fingerprint (derived from Tenant A's key).
func TestPreambleSpoofingRejection(t *testing.T) {
	// Create shared transport with two tenants.
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
	st.RegisterTenant("honest", [][]byte{honestKey})
	st.RegisterTenant("victim", [][]byte{victimKey})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	listener, err := st.Listen(ctx)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()

	serverAddr := listener.Addr().String()

	// Create an attacker transport that has honest-tenant's key.
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

	// Spoof: claim to be "victim" in preamble but use honest-tenant's key.
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

// TestInfrastructureServesAllTenants verifies that a shared transport listener
// accepts handshakes from multiple tenants, each using their own PSK.
// Each tenant uses a separate dialer transport with its own keypair (Noise
// handshakes require different static keys on each side).
func TestInfrastructureServesAllTenants(t *testing.T) {
	// Server: shared transport that accepts multiple tenants
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
	st.RegisterTenant("tenant-a", [][]byte{keyA})
	st.RegisterTenant("tenant-b", [][]byte{keyB})

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

	// Tenant A dials server — network key match resolves authentication
	sessA, err := mgr.Dial(ctx, "tenant-a", aether.Target{
		Address: serverAddr,
		NodeID:  serverNodeID,
	})
	if err != nil {
		t.Fatalf("tenant-a dial: %v", err)
	}
	defer sessA.Close()

	// Tenant B dials server
	sessB, err := mgr.Dial(ctx, "tenant-b", aether.Target{
		Address: serverAddr,
		NodeID:  serverNodeID,
	})
	if err != nil {
		t.Fatalf("tenant-b dial: %v", err)
	}
	defer sessB.Close()

	// Verify client-side TenantSession wrappers carry correct tenant IDs.
	if sessA.TenantID() != "tenant-a" {
		t.Errorf("sessA.TenantID() = %q, want %q", sessA.TenantID(), "tenant-a")
	}
	if sessB.TenantID() != "tenant-b" {
		t.Errorf("sessB.TenantID() = %q, want %q", sessB.TenantID(), "tenant-b")
	}

	// Wait for both accepts to complete.
	select {
	case <-acceptDone:
	case <-time.After(15 * time.Second):
		t.Fatal("timeout waiting for server accepts")
	}
	t.Log("Infrastructure served both tenants successfully")
}

// TestDedicatedTransportIsolation verifies that ORBTR's dedicated transport
// rejects connections from clients using a different PSK.
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
