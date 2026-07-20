/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"io"
	"testing"
)

// TestIdentitySeedEnv_StableAcrossBoots is the regression guard for fleet-wide
// ghost minting.
//
// THE BUG: node identity was persisted only to <DataDir>/node-key.json, and the
// platform config defaults DataDir to "./data" — a RELATIVE path on the
// container's ephemeral layer (platform/configs/mesh/validators.go). Half the
// deployments mount no volume at all. So every deploy destroyed the seed,
// GenerateIdentity minted a NEW NodeID, and the previous identity was orphaned
// into the mesh roster as a ghost.
//
// Ghosts are not inert. A dead NodeID still resolves to a LIVE machine, which
// correctly refuses to answer to it: noise-UDP dials hang to msg2 timeout,
// WebSocket dials get 401/404 and fall back to TLS. Measured on the live fleet:
// ~100 disconnects/hour, 95 of them C->F, 782 tombstones against 11 real nodes.
// One ephemeral seed produced fleet-wide transport churn.
//
// MESH_IDENTITY_SEED makes identity a deployment property rather than a
// filesystem property: same seed => same NodeID, across restarts, deploys, and
// machine replacement (which a volume does NOT survive), with no mount needed.
func TestIdentitySeedEnv_StableAcrossBoots(t *testing.T) {
	seed := make([]byte, ed25519.SeedSize)
	if _, err := io.ReadFull(rand.Reader, seed); err != nil {
		t.Fatalf("seed: %v", err)
	}
	t.Setenv(EnvNodeIdentitySeed, base64.StdEncoding.EncodeToString(seed))

	ctx := context.Background()

	// Two independent "boots", each with a DIFFERENT throwaway DataDir — i.e.
	// the ephemeral-storage case that has been minting ghosts. The identity
	// must not depend on the directory at all.
	first, err := GenerateIdentity(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("boot 1: %v", err)
	}
	second, err := GenerateIdentity(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("boot 2: %v", err)
	}

	if first.NodeID != second.NodeID {
		t.Fatalf("NodeID changed across boots with the same seed: %s vs %s — "+
			"every deploy would orphan the previous identity into the roster as a ghost",
			first.NodeID.Short(), second.NodeID.Short())
	}
	if !first.PrivateKey.Equal(second.PrivateKey) {
		t.Fatal("private key changed across boots with the same seed")
	}

	// The seed must be authoritative even with NO DataDir — half the fleet has
	// no volume, and those endpoints must still hold a stable identity.
	noDir, err := GenerateIdentity(ctx, "")
	if err != nil {
		t.Fatalf("seed must not require a DataDir (half the fleet has no volume): %v", err)
	}
	if noDir.NodeID != first.NodeID {
		t.Fatalf("NodeID differs when DataDir is empty: %s vs %s", noDir.NodeID.Short(), first.NodeID.Short())
	}
}

// TestIdentitySeedEnv_DifferentSeedsDifferentNodes guards the inverse: the seed
// must actually determine identity, so two nodes seeded differently never
// collide on a NodeID.
func TestIdentitySeedEnv_DifferentSeedsDifferentNodes(t *testing.T) {
	mk := func(t *testing.T) *NodeIdentity {
		t.Helper()
		seed := make([]byte, ed25519.SeedSize)
		if _, err := io.ReadFull(rand.Reader, seed); err != nil {
			t.Fatalf("seed: %v", err)
		}
		t.Setenv(EnvNodeIdentitySeed, base64.StdEncoding.EncodeToString(seed))
		id, err := GenerateIdentity(context.Background(), t.TempDir())
		if err != nil {
			t.Fatalf("generate: %v", err)
		}
		return id
	}
	a := mk(t)
	b := mk(t)
	if a.NodeID == b.NodeID {
		t.Fatal("different seeds produced the same NodeID")
	}
}

// TestIdentitySeedEnv_BadSeedFailsClosed: a malformed or wrong-length seed must
// be a hard error, never a silent fallback to a random identity. A silent
// fallback is precisely the ghost-minting behaviour this env var exists to end
// — it would look healthy while rotating the NodeID on every boot.
func TestIdentitySeedEnv_BadSeedFailsClosed(t *testing.T) {
	for name, val := range map[string]string{
		"not-base64":   "!!!not base64!!!",
		"too-short":    base64.StdEncoding.EncodeToString([]byte("short")),
		"wrong-length": base64.StdEncoding.EncodeToString(make([]byte, 16)),
	} {
		t.Run(name, func(t *testing.T) {
			t.Setenv(EnvNodeIdentitySeed, val)
			_, err := GenerateIdentity(context.Background(), t.TempDir())
			if err == nil {
				t.Fatal("bad seed silently accepted — would rotate NodeID per boot and mint ghosts")
			}
			if !errors.Is(err, ErrNodeIdentityCorrupt) {
				t.Fatalf("want ErrNodeIdentityCorrupt, got %v", err)
			}
		})
	}
}
