/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"io"
	"testing"
)

func randSeed(t *testing.T) []byte {
	t.Helper()
	s := make([]byte, ed25519.SeedSize)
	if _, err := io.ReadFull(rand.Reader, s); err != nil {
		t.Fatalf("seed: %v", err)
	}
	return s
}

// TestGenerateIdentity_PerMachine_TwoMachinesOneSeed is the regression guard
// for the multi-machine identity collision.
//
// THE BUG: MESH_IDENTITY_SEED is an APP-level Fly secret, so both machines of
// app/relay/devices (2 machines each, syd + iad, distinct 6PN addresses) read
// the SAME seed and minted the SAME NodeID. The PeerRecord for that one
// identity then flapped between the two machines' addresses, and every
// noise-UDP session to it broke and re-handshook multiple times per second
// (connection-history: A→F→A on the multi-machine peers every 0.2-3s while
// single-machine nodes stayed put). Distinct per-machine identities end it.
func TestGenerateIdentity_PerMachine_TwoMachinesOneSeed(t *testing.T) {
	seed := randSeed(t)
	t.Setenv(EnvNodeIdentitySeed, base64.StdEncoding.EncodeToString(seed))
	ctx := context.Background()

	t.Setenv(EnvFlyMachineID, "7815611b6e4428") // app syd
	machineA, err := GenerateIdentity(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("machine A: %v", err)
	}

	t.Setenv(EnvFlyMachineID, "e82745df6d47d8") // app iad — same app secret
	machineB, err := GenerateIdentity(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("machine B: %v", err)
	}

	if machineA.NodeID == machineB.NodeID {
		t.Fatalf("two machines of one app sharing MESH_IDENTITY_SEED collided on NodeID %s — "+
			"the PeerRecord will flap between their 6PN addresses and noise-UDP will churn",
			machineA.NodeID.Short())
	}
	if machineA.PrivateKey.Equal(machineB.PrivateKey) {
		t.Fatal("two machines derived the same private key from one app seed")
	}
}

// TestGenerateIdentity_PerMachine_StableAcrossDeploys guards the property that
// keeps the fix from re-introducing ghost-minting: a machine's identity must
// depend ONLY on (seed, FLY_MACHINE_ID), both stable across in-place deploys,
// so redeploying the same machine yields the same NodeID. FLY_MACHINE_ID is
// deploy-stable on Fly (verified live: app machines created 2026-06-03 kept
// their IDs through deploys months later); this test locks the code half of
// that guarantee.
func TestGenerateIdentity_PerMachine_StableAcrossDeploys(t *testing.T) {
	seed := randSeed(t)
	t.Setenv(EnvNodeIdentitySeed, base64.StdEncoding.EncodeToString(seed))
	t.Setenv(EnvFlyMachineID, "185d9d6c249278")
	ctx := context.Background()

	first, err := GenerateIdentity(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("deploy 1: %v", err)
	}
	// A later deploy: same seed, same machine ID, different ephemeral DataDir.
	second, err := GenerateIdentity(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("deploy 2: %v", err)
	}
	if first.NodeID != second.NodeID {
		t.Fatalf("NodeID changed across deploys of one machine: %s vs %s — "+
			"each deploy would orphan a ghost", first.NodeID.Short(), second.NodeID.Short())
	}
	// And with no DataDir at all — half the fleet mounts no volume.
	noDir, err := GenerateIdentity(ctx, "")
	if err != nil {
		t.Fatalf("per-machine identity must not require a DataDir: %v", err)
	}
	if noDir.NodeID != first.NodeID {
		t.Fatalf("NodeID differs with empty DataDir: %s vs %s", noDir.NodeID.Short(), first.NodeID.Short())
	}
}

// TestGenerateIdentity_PerMachine_EmptyMachineIDFallback proves the derivation
// is a no-op off Fly: with FLY_MACHINE_ID unset, identity is exactly the raw
// seed identity, so local/non-Fly single-machine deployments are unchanged and
// there is no collision to break.
func TestGenerateIdentity_PerMachine_EmptyMachineIDFallback(t *testing.T) {
	seed := randSeed(t)
	t.Setenv(EnvNodeIdentitySeed, base64.StdEncoding.EncodeToString(seed))
	t.Setenv(EnvFlyMachineID, "") // explicitly absent
	ctx := context.Background()

	got, err := GenerateIdentity(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("generate: %v", err)
	}
	want, err := identityFromSeed(seed)
	if err != nil {
		t.Fatalf("reference: %v", err)
	}
	if got.NodeID != want.NodeID {
		t.Fatalf("empty FLY_MACHINE_ID must yield the raw-seed identity: %s vs %s",
			got.NodeID.Short(), want.NodeID.Short())
	}
}

// TestDerivePerMachineSeed_Properties covers the pure derivation directly:
// correct length, determinism, distinctness per machine, and — the security
// property — that the machine ID is only a salt: the same machine ID under two
// DIFFERENT root seeds yields two different derived seeds, so the derived key
// still depends on the secret (this is NOT the rev-073 public-only derivation).
func TestDerivePerMachineSeed_Properties(t *testing.T) {
	rootA := randSeed(t)
	rootB := randSeed(t)

	// Length: exactly one ed25519 seed, no expansion needed.
	if got := derivePerMachineSeed(rootA, "m1"); len(got) != ed25519.SeedSize {
		t.Fatalf("derived seed length = %d, want %d", len(got), ed25519.SeedSize)
	}

	// Determinism / deploy-stability.
	if !bytes.Equal(derivePerMachineSeed(rootA, "m1"), derivePerMachineSeed(rootA, "m1")) {
		t.Fatal("derivation is not deterministic for (seed, machineID)")
	}

	// Distinct per machine under one root seed.
	if bytes.Equal(derivePerMachineSeed(rootA, "m1"), derivePerMachineSeed(rootA, "m2")) {
		t.Fatal("two machine IDs under one seed derived the same seed")
	}

	// Secret-dependent: the machine ID is a salt, not the key. Same machine
	// ID under different secrets must not collide.
	if bytes.Equal(derivePerMachineSeed(rootA, "m1"), derivePerMachineSeed(rootB, "m1")) {
		t.Fatal("derived seed did not depend on the secret root seed — the machine ID must be a salt, not the key")
	}

	// Empty machine ID is a pass-through (single-machine / non-Fly).
	if !bytes.Equal(derivePerMachineSeed(rootA, ""), rootA) {
		t.Fatal("empty machine ID must return the root seed unchanged")
	}
	// A derived seed must never equal the root seed (would mean the machine
	// ID was ignored) — sanity that the salt actually mixes in.
	if bytes.Equal(derivePerMachineSeed(rootA, "m1"), rootA) {
		t.Fatal("non-empty machine ID did not change the seed")
	}
}
