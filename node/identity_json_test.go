/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestNodeIdentityJSONHidesSecrets is a fail-closed gate against regressions
// of finding rev-073: NodeIdentity must never serialise its ed25519 private
// key (or raw public key) through encoding/json. Any future struct-tag
// change that re-exposes a key field will fail this test.
func TestNodeIdentityJSONHidesSecrets(t *testing.T) {
	id, err := GenerateIdentity(context.Background(), t.TempDir())
	if err != nil {
		t.Fatalf("GenerateIdentity: %v", err)
	}

	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(id); err != nil {
		t.Fatalf("json.Encode: %v", err)
	}
	out := buf.String()

	if strings.Contains(out, "privateKey") {
		t.Fatalf("NodeIdentity JSON exposes privateKey field: %s", out)
	}
	if strings.Contains(out, "publicKey") {
		t.Fatalf("NodeIdentity JSON exposes publicKey field: %s", out)
	}

	// NodeID MUST remain exposed — it is the canonical public identifier
	// used by handlers and tests.
	if !strings.Contains(out, "nodeId") {
		t.Fatalf("NodeIdentity JSON missing nodeId field: %s", out)
	}

	// Defense-in-depth: byte-level scan to catch a future marshaller that
	// honours a custom MarshalJSON which leaks the raw seed.
	seed := id.PrivateKey.Seed()
	if bytes.Contains(buf.Bytes(), seed) {
		t.Fatalf("NodeIdentity JSON leaks ed25519 seed bytes")
	}
}

// TestNodeIdentityFieldAccessUnaffected confirms struct-tag changes did NOT
// break direct field access — the 30+ call sites that read identity.PrivateKey
// and identity.PublicKey directly (signing_ledger.go, swarm_integration.go,
// peer_connections.go, route_integration.go, lad_reach_bridge.go, holepunch.go,
// runtime.go) keep their unexported-by-JSON-but-exported-on-the-struct contract.
func TestNodeIdentityFieldAccessUnaffected(t *testing.T) {
	id, err := GenerateIdentity(context.Background(), t.TempDir())
	if err != nil {
		t.Fatalf("GenerateIdentity: %v", err)
	}

	// Direct field reads must still work: JSON tags only gate encoding/json,
	// not Go field access.
	if len(id.PublicKey) != ed25519.PublicKeySize {
		t.Fatalf("PublicKey field unreadable or wrong size: got %d want %d",
			len(id.PublicKey), ed25519.PublicKeySize)
	}
	if len(id.PrivateKey) != ed25519.PrivateKeySize {
		t.Fatalf("PrivateKey field unreadable or wrong size: got %d want %d",
			len(id.PrivateKey), ed25519.PrivateKeySize)
	}
	if id.NodeID == "" {
		t.Fatalf("NodeID field unreadable")
	}

	// Round-trip sign/verify proves the keypair is still functional via
	// direct field access, independent of any JSON layer.
	msg := []byte("rev-073 regression: keypair still functional")
	sig := ed25519.Sign(id.PrivateKey, msg)
	if !ed25519.Verify(id.PublicKey, msg, sig) {
		t.Fatalf("ed25519 sign/verify round-trip failed via direct field access")
	}
}

// TestNodeIdentityFailsClosedWithoutDataDir is the fail-closed gate for
// rev-073 fix #2: GenerateIdentity must refuse to boot when DataDir is
// empty. The previous hostname-derived scheme silently ran without any
// persistence, which is exactly what allowed the deterministic-derivation
// vulnerability to ship — random-and-lost-on-restart would rotate the
// NodeID under the mesh, so the boot must fail loudly instead.
func TestNodeIdentityFailsClosedWithoutDataDir(t *testing.T) {
	_, err := GenerateIdentity(context.Background(), "")
	if err == nil {
		t.Fatalf("GenerateIdentity(\"\") must return an error, got nil")
	}
	if !errors.Is(err, ErrNodeIdentityDataDirRequired) {
		t.Fatalf("GenerateIdentity(\"\") returned wrong sentinel: got %v want %v",
			err, ErrNodeIdentityDataDirRequired)
	}
}

// TestNodeIdentityRandomNotHostnameDerived is the direct kill-shot for the
// rev-073 root cause: two identities generated from independent DataDirs
// (i.e. independent first-runs on the same host) MUST produce different
// keypairs. Under the removed sha256("mesh-node-identity:"+hostname)
// scheme they were identical, which is what made the private key
// reconstructable from public information.
func TestNodeIdentityRandomNotHostnameDerived(t *testing.T) {
	ctx := context.Background()
	id1, err := GenerateIdentity(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("GenerateIdentity #1: %v", err)
	}
	id2, err := GenerateIdentity(ctx, t.TempDir())
	if err != nil {
		t.Fatalf("GenerateIdentity #2: %v", err)
	}

	// Two independent first-runs on the SAME host (same os.Hostname())
	// must diverge — proves the private key is no longer a pure function
	// of hostname.
	if id1.NodeID == id2.NodeID {
		t.Fatalf("two independent GenerateIdentity calls produced identical NodeID %q — hostname derivation may have crept back in",
			id1.NodeID)
	}
	if bytes.Equal(id1.PrivateKey.Seed(), id2.PrivateKey.Seed()) {
		t.Fatalf("two independent GenerateIdentity calls produced identical seeds — RNG is broken or hostname derivation regressed")
	}
}

// TestNodeIdentityPersistsAcrossCalls verifies that a mesh node keeps a
// stable NodeID across restarts by loading the persisted seed from
// <DataDir>/node-key.json rather than generating a new one every boot.
// Without this, restart would rotate the node's identity out from under
// every peer that had it cached.
func TestNodeIdentityPersistsAcrossCalls(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	id1, err := GenerateIdentity(ctx, dir)
	if err != nil {
		t.Fatalf("GenerateIdentity #1: %v", err)
	}
	id2, err := GenerateIdentity(ctx, dir)
	if err != nil {
		t.Fatalf("GenerateIdentity #2: %v", err)
	}
	if id1.NodeID != id2.NodeID {
		t.Fatalf("NodeID rotated across calls with same DataDir: %q vs %q",
			id1.NodeID, id2.NodeID)
	}
	if !bytes.Equal(id1.PrivateKey.Seed(), id2.PrivateKey.Seed()) {
		t.Fatalf("private key seed changed across calls with same DataDir")
	}

	// And the seed file must actually exist on disk.
	seedPath := filepath.Join(dir, "node-key.json")
	if _, err := os.Stat(seedPath); err != nil {
		t.Fatalf("expected node-key.json to be persisted at %s: %v", seedPath, err)
	}
}

// TestNodeIdentityCorruptFileFailsClosed confirms that a malformed
// on-disk seed file is not silently overwritten — an operator who
// corrupted the file must see a loud error, not a fresh key that would
// rotate the NodeID and split-brain the mesh.
func TestNodeIdentityCorruptFileFailsClosed(t *testing.T) {
	dir := t.TempDir()
	seedPath := filepath.Join(dir, "node-key.json")
	if err := os.WriteFile(seedPath, []byte("{not valid json"), 0o600); err != nil {
		t.Fatalf("write corrupt file: %v", err)
	}
	_, err := GenerateIdentity(context.Background(), dir)
	if err == nil {
		t.Fatalf("GenerateIdentity accepted corrupt file, expected error")
	}
	if !errors.Is(err, ErrNodeIdentityCorrupt) {
		t.Fatalf("GenerateIdentity returned wrong sentinel: got %v want %v",
			err, ErrNodeIdentityCorrupt)
	}
}
