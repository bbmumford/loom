/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"context"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	aether "github.com/ORBTR/aether"
)

// NodeIdentity holds the cryptographic identity of a mesh node.
//
// The PrivateKey field is intentionally excluded from JSON marshalling
// (`json:"-"`). No production call site marshals NodeIdentity, but the
// tag is load-bearing: if a future status/diagnostic handler ever embeds
// an identity struct in a response, the private key MUST NOT escape.
// PublicKey is likewise hidden by default — handlers that need to expose
// the node's signing pubkey must do so via an explicit DTO, not by
// marshalling NodeIdentity directly.
//
// Field access (signing_ledger.go, swarm_integration.go, peer_connections.go,
// route_integration.go, lad_reach_bridge.go, holepunch.go, runtime.go) is
// unaffected — JSON tags only gate encoding/json, not direct field reads.
type NodeIdentity struct {
	NodeID     aether.NodeID      `json:"nodeId"`
	PublicKey  ed25519.PublicKey  `json:"-"`
	PrivateKey ed25519.PrivateKey `json:"-"`
}

// nodeIdentityFile is the on-disk container for a persisted mesh-node
// identity. Only the ed25519 seed (32 bytes) is stored — PublicKey and
// NodeID are re-derived on load. When the deployment registers a
// platform SecretSealer, sealing of the seed is layered on top by the
// caller before writing; for now the file lives on a per-machine
// private volume with 0600 permissions.
type nodeIdentityFile struct {
	Version int    `json:"version"`
	Seed    string `json:"seed"` // base64(std) of ed25519.SeedSize (32) bytes
}

const (
	// EnvNodeIdentitySeed carries a base64(std) 32-byte ed25519 seed and is
	// the AUTHORITATIVE source of node identity when set. It exists because
	// DataDir persistence is not reliably available: the platform config
	// defaults DataDir to "./data" (a RELATIVE path on the container's
	// ephemeral layer), and only some deployments mount a volume at all. Every
	// deploy therefore minted a fresh NodeID and orphaned the previous one; the
	// dead identity lingered in the mesh roster as a ghost that peers kept
	// dialing forever. Those dials do not fail fast — a ghost ID usually
	// resolves to a LIVE machine, which correctly answers "not me": noise-UDP
	// gets msg2 timeout, WebSocket gets 401/404, and the peer falls back to TLS
	// and churns. One ephemeral seed produced fleet-wide transport churn.
	//
	// A seed is strictly better than a volume: it survives deploys AND machine
	// replacement (a volume does not), needs no mount, and is identical to
	// provision on every endpoint.
	//
	// SECURITY — this is deliberately NOT the rev-073 mistake. That code
	// derived the keypair from os.Hostname() via sha256, so anyone who knew the
	// hostname (DNS, TLS SNI, Fly metadata) could reconstruct the PRIVATE key.
	// This seed is operator-provisioned high-entropy material delivered as a
	// platform secret, never derived from public inputs:
	//
	//	fly secrets set MESH_IDENTITY_SEED=$(openssl rand -base64 32) -a <app>
	EnvNodeIdentitySeed = "MESH_IDENTITY_SEED"

	// EnvFlyMachineID is Fly's per-machine identifier. When present it is
	// mixed into the MESH_IDENTITY_SEED-derived key so that two machines of
	// the SAME app (which necessarily share the app-level MESH_IDENTITY_SEED
	// secret) get DISTINCT node identities instead of colliding on one.
	//
	// The collision it fixes: app/relay/devices run 2 machines in different
	// regions (syd + iad), each with its own 6PN address, but one shared
	// MESH_IDENTITY_SEED minted ONE NodeID for BOTH. The PeerRecord for that
	// single identity therefore flapped between the two machines' 6PN
	// addresses as each gossiped itself, and every noise-UDP session to the
	// identity broke and re-handshook multiple times per second (observed in
	// connection-history: A→F→A on vl1_fjll7snh/5hu565gy/6klfupzs every
	// 0.2-3s while single-machine nodes stayed stable). Distinct per-machine
	// identities give each machine a stable identity↔address↔session mapping.
	//
	// FLY_MACHINE_ID is stable across in-place (rolling) deploys — verified
	// live: app/relay/devices machines created 2026-06-03 kept the same IDs
	// through deploys months later — so mixing it in does NOT re-introduce
	// the per-deploy ghost-minting that MESH_IDENTITY_SEED exists to prevent.
	// It changes only on genuine machine replacement, where a new identity is
	// the correct outcome. When empty (local/non-Fly), the raw seed is used
	// unchanged: a single-machine context has no collision to break.
	EnvFlyMachineID = "FLY_MACHINE_ID"

	// perMachineSeedInfo domain-separates the HMAC that derives a
	// per-machine seed from the root MESH_IDENTITY_SEED. Versioned so a
	// future change to the derivation can co-exist without silently
	// rotating every node's identity a second time.
	perMachineSeedInfo = "mesh-node-identity/per-machine:v1|"

	// nodeIdentityFileName is the on-disk filename inside DataDir that
	// holds the persisted ed25519 seed. Matches the convention already
	// asserted in runtime_test.go TestIdentityFilePaths.
	nodeIdentityFileName = "node-key.json"

	// nodeIdentityFileVersion is the current on-disk format version. A
	// future sealed-envelope wrap will bump this so LoadOrGenerate can
	// distinguish seed-only from sealed-envelope files.
	nodeIdentityFileVersion = 1

	// nodeIdentityFilePerm restricts the seed file to owner read+write.
	// On POSIX this is enforced by the filesystem; on Windows the file
	// still lives inside DataDir (a per-machine private volume) so the
	// permission is best-effort defense-in-depth.
	nodeIdentityFilePerm = 0o600
)

// ErrNodeIdentityDataDirRequired is the fail-closed sentinel returned
// when GenerateIdentity is called without a DataDir. Mesh nodes must
// persist their identity so restart does not rotate the NodeID out
// from under the rest of the mesh — a boot with no DataDir is a
// configuration bug, not a soft-failure.
var ErrNodeIdentityDataDirRequired = errors.New("mesh: node identity requires non-empty DataDir")

// ErrNodeIdentityCorrupt is returned when the on-disk seed file exists
// but cannot be parsed or has an unexpected version / length. Operators
// see the exact reason via structured slog on the error path; the
// sentinel lets callers errors.Is when they need to distinguish
// corruption from a fresh-install codepath.
var ErrNodeIdentityCorrupt = errors.New("mesh: node identity file corrupt")

// GenerateIdentity loads a persisted ed25519 identity from
// <dataDir>/node-key.json or, on first run, generates a fresh random
// keypair via ed25519.GenerateKey(rand.Reader) and persists it.
//
// Finding rev-073: earlier versions derived the ed25519 keypair
// deterministically from os.Hostname() via sha256("mesh-node-identity:"
// + hostname). Same hostname always produced the same private key,
// which meant an attacker who knew the hostname (readable in DNS,
// TLS SNI, Fly machine metadata, ...) plus this open-source function
// could reconstruct the private key and forge mesh identity. That
// derivation is removed entirely — the seed now comes from crypto/rand.
//
// Callers must pass a non-empty dataDir. ctx is propagated to slog so
// error observability attributes to the correct request scope; the
// filesystem operations themselves are synchronous and short.
func GenerateIdentity(ctx context.Context, dataDir string) (*NodeIdentity, error) {
	// Seed env wins outright — it is stable across deploys AND machine
	// replacement, so it is the only source that actually keeps a NodeID
	// constant for the life of a deployment. Checked before the DataDir guard
	// so a seeded node needs no data directory at all.
	if raw := strings.TrimSpace(os.Getenv(EnvNodeIdentitySeed)); raw != "" {
		seed, err := base64.StdEncoding.DecodeString(raw)
		if err != nil {
			slog.ErrorContext(ctx, "mesh_node_identity_seed_decode_failed",
				slog.String("subsystem", "mesh.node.identity"),
				slog.String("reason", "seed_base64"),
				slog.String("env", EnvNodeIdentitySeed),
				slog.String("err", err.Error()),
			)
			return nil, fmt.Errorf("%w: %s base64: %v", ErrNodeIdentityCorrupt, EnvNodeIdentitySeed, err)
		}
		if len(seed) != ed25519.SeedSize {
			// Fail closed rather than pad/truncate: a wrong-length seed would
			// silently yield a DIFFERENT NodeID on every operator, which is the
			// exact ghost-minting failure this env var exists to end.
			slog.ErrorContext(ctx, "mesh_node_identity_seed_len_invalid",
				slog.String("subsystem", "mesh.node.identity"),
				slog.String("reason", "seed_len"),
				slog.String("env", EnvNodeIdentitySeed),
				slog.Int("len", len(seed)),
			)
			return nil, fmt.Errorf("%w: %s length %d != %d", ErrNodeIdentityCorrupt, EnvNodeIdentitySeed, len(seed), ed25519.SeedSize)
		}
		machineID := strings.TrimSpace(os.Getenv(EnvFlyMachineID))
		seed = derivePerMachineSeed(seed, machineID)
		id, err := identityFromSeed(seed)
		if err != nil {
			return nil, err
		}
		slog.InfoContext(ctx, "mesh_node_identity_ready",
			slog.String("subsystem", "mesh.node.identity"),
			slog.String("nodeId", id.NodeID.Short()),
			slog.String("source", "seed_env"),
			slog.Bool("perMachine", machineID != ""),
			slog.String("flyMachineId", machineID),
			slog.Bool("firstRun", false),
		)
		return id, nil
	}

	if dataDir == "" {
		slog.ErrorContext(ctx, "mesh_node_identity_datadir_missing",
			slog.String("subsystem", "mesh.node.identity"),
			slog.String("reason", "datadir_empty"),
		)
		return nil, ErrNodeIdentityDataDirRequired
	}

	path := filepath.Join(dataDir, nodeIdentityFileName)

	seed, loaded, err := loadIdentitySeed(ctx, path)
	if err != nil {
		return nil, err
	}
	if !loaded {
		if err := os.MkdirAll(dataDir, 0o700); err != nil {
			slog.ErrorContext(ctx, "mesh_node_identity_mkdir_failed",
				slog.String("subsystem", "mesh.node.identity"),
				slog.String("reason", "mkdir_error"),
				slog.String("dataDir", dataDir),
				slog.String("err", err.Error()),
			)
			return nil, fmt.Errorf("mesh: create data dir for node identity: %w", err)
		}
		seed = make([]byte, ed25519.SeedSize)
		if _, err := io.ReadFull(rand.Reader, seed); err != nil {
			slog.ErrorContext(ctx, "mesh_node_identity_rand_failed",
				slog.String("subsystem", "mesh.node.identity"),
				slog.String("reason", "rand_read"),
				slog.String("err", err.Error()),
			)
			return nil, fmt.Errorf("mesh: read random seed: %w", err)
		}
		if err := persistIdentitySeed(ctx, path, seed); err != nil {
			return nil, err
		}
	}

	if len(seed) != ed25519.SeedSize {
		slog.ErrorContext(ctx, "mesh_node_identity_seed_len_invalid",
			slog.String("subsystem", "mesh.node.identity"),
			slog.String("reason", "seed_len"),
			slog.Int("len", len(seed)),
		)
		return nil, fmt.Errorf("%w: seed length %d != %d", ErrNodeIdentityCorrupt, len(seed), ed25519.SeedSize)
	}

	id, err := identityFromSeed(seed)
	if err != nil {
		return nil, err
	}

	slog.InfoContext(ctx, "mesh_node_identity_ready",
		slog.String("subsystem", "mesh.node.identity"),
		slog.String("nodeId", id.NodeID.Short()),
		slog.String("source", "datadir_file"),
		slog.String("path", path),
		slog.Bool("firstRun", !loaded),
	)

	return id, nil
}

// identityFromSeed derives the ed25519 keypair and NodeID from a 32-byte seed.
// Shared by the seed-env and DataDir-file paths so both produce a byte-identical
// identity for the same seed — the property the whole ghost fix rests on.
// derivePerMachineSeed folds the per-machine FLY_MACHINE_ID into the
// shared root seed so co-located machines of one app derive distinct
// identities. See EnvFlyMachineID for the collision this fixes.
//
// The derivation is HMAC-SHA256(key=rootSeed, msg=info||machineID). HMAC
// with the secret rootSeed as the KEY is a PRF: the output is
// indistinguishable from random without knowledge of rootSeed, so the
// private key remains gated on the secret even though machineID is a
// low-entropy, semi-public value (Fly metadata). This is deliberately
// NOT the rev-073 mistake, which derived the whole key from a PURELY
// public input (os.Hostname) under a public hash — there the attacker
// needed no secret. Here machineID is only a salt; rootSeed is the secret.
//
// Output is exactly sha256.Size (32) = ed25519.SeedSize, so no expansion
// is required. When machineID is empty the raw seed is returned unchanged
// to preserve the pre-existing single-machine identity.
func derivePerMachineSeed(rootSeed []byte, machineID string) []byte {
	if machineID == "" {
		return rootSeed
	}
	mac := hmac.New(sha256.New, rootSeed)
	mac.Write([]byte(perMachineSeedInfo))
	mac.Write([]byte(machineID))
	return mac.Sum(nil) // 32 bytes == ed25519.SeedSize
}

func identityFromSeed(seed []byte) (*NodeIdentity, error) {
	privKey := ed25519.NewKeyFromSeed(seed)
	pubKey := privKey.Public().(ed25519.PublicKey)
	nodeID, err := aether.NewNodeID(pubKey)
	if err != nil {
		return nil, fmt.Errorf("mesh: derive node id: %w", err)
	}
	return &NodeIdentity{
		NodeID:     nodeID,
		PublicKey:  pubKey,
		PrivateKey: privKey,
	}, nil
}

// loadIdentitySeed reads and parses <dataDir>/node-key.json. It
// distinguishes three outcomes to the caller: (nil, false, nil) when
// the file does not exist (fresh install → generate), (seed, true, nil)
// when a valid seed was recovered, or (nil, false, err) for a malformed
// file (ErrNodeIdentityCorrupt) or an unexpected filesystem error.
func loadIdentitySeed(ctx context.Context, path string) ([]byte, bool, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, false, nil
		}
		slog.ErrorContext(ctx, "mesh_node_identity_read_failed",
			slog.String("subsystem", "mesh.node.identity"),
			slog.String("reason", "read_error"),
			slog.String("path", path),
			slog.String("err", err.Error()),
		)
		return nil, false, fmt.Errorf("mesh: read node identity: %w", err)
	}

	var f nodeIdentityFile
	if err := json.Unmarshal(data, &f); err != nil {
		slog.ErrorContext(ctx, "mesh_node_identity_parse_failed",
			slog.String("subsystem", "mesh.node.identity"),
			slog.String("reason", "json_parse"),
			slog.String("path", path),
			slog.String("err", err.Error()),
		)
		return nil, false, fmt.Errorf("%w: %v", ErrNodeIdentityCorrupt, err)
	}
	if f.Version != nodeIdentityFileVersion {
		slog.ErrorContext(ctx, "mesh_node_identity_version_unsupported",
			slog.String("subsystem", "mesh.node.identity"),
			slog.String("reason", "version_mismatch"),
			slog.String("path", path),
			slog.Int("version", f.Version),
		)
		return nil, false, fmt.Errorf("%w: unsupported version %d", ErrNodeIdentityCorrupt, f.Version)
	}
	seed, err := base64.StdEncoding.DecodeString(f.Seed)
	if err != nil {
		slog.ErrorContext(ctx, "mesh_node_identity_seed_decode_failed",
			slog.String("subsystem", "mesh.node.identity"),
			slog.String("reason", "seed_base64"),
			slog.String("path", path),
			slog.String("err", err.Error()),
		)
		return nil, false, fmt.Errorf("%w: seed base64: %v", ErrNodeIdentityCorrupt, err)
	}
	return seed, true, nil
}

// persistIdentitySeed writes the seed to <dataDir>/node-key.json with
// 0600 permissions. Uses a temp-file-then-rename pattern so a partial
// write cannot leave a truncated file that would fail later parsing.
func persistIdentitySeed(ctx context.Context, path string, seed []byte) error {
	f := nodeIdentityFile{
		Version: nodeIdentityFileVersion,
		Seed:    base64.StdEncoding.EncodeToString(seed),
	}
	body, err := json.Marshal(&f)
	if err != nil {
		slog.ErrorContext(ctx, "mesh_node_identity_marshal_failed",
			slog.String("subsystem", "mesh.node.identity"),
			slog.String("reason", "json_marshal"),
			slog.String("err", err.Error()),
		)
		return fmt.Errorf("mesh: marshal node identity: %w", err)
	}

	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".node-key.json.*")
	if err != nil {
		slog.ErrorContext(ctx, "mesh_node_identity_tempfile_failed",
			slog.String("subsystem", "mesh.node.identity"),
			slog.String("reason", "tempfile_create"),
			slog.String("dir", dir),
			slog.String("err", err.Error()),
		)
		return fmt.Errorf("mesh: create temp for node identity: %w", err)
	}
	tmpPath := tmp.Name()
	// If we return before rename, best-effort remove the tmp file.
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpPath)
		}
	}()

	if err := os.Chmod(tmpPath, nodeIdentityFilePerm); err != nil {
		_ = tmp.Close()
		slog.ErrorContext(ctx, "mesh_node_identity_chmod_failed",
			slog.String("subsystem", "mesh.node.identity"),
			slog.String("reason", "chmod"),
			slog.String("path", tmpPath),
			slog.String("err", err.Error()),
		)
		return fmt.Errorf("mesh: chmod node identity: %w", err)
	}
	if _, err := tmp.Write(body); err != nil {
		_ = tmp.Close()
		slog.ErrorContext(ctx, "mesh_node_identity_write_failed",
			slog.String("subsystem", "mesh.node.identity"),
			slog.String("reason", "write"),
			slog.String("path", tmpPath),
			slog.String("err", err.Error()),
		)
		return fmt.Errorf("mesh: write node identity: %w", err)
	}
	if err := tmp.Close(); err != nil {
		slog.ErrorContext(ctx, "mesh_node_identity_close_failed",
			slog.String("subsystem", "mesh.node.identity"),
			slog.String("reason", "close"),
			slog.String("path", tmpPath),
			slog.String("err", err.Error()),
		)
		return fmt.Errorf("mesh: close node identity: %w", err)
	}
	if err := os.Rename(tmpPath, path); err != nil {
		slog.ErrorContext(ctx, "mesh_node_identity_rename_failed",
			slog.String("subsystem", "mesh.node.identity"),
			slog.String("reason", "rename"),
			slog.String("from", tmpPath),
			slog.String("to", path),
			slog.String("err", err.Error()),
		)
		return fmt.Errorf("mesh: rename node identity: %w", err)
	}
	cleanup = false
	return nil
}

// Short returns a short string representation of the node identity
func (ni *NodeIdentity) Short() string {
	return ni.NodeID.Short()
}

// String returns the full node ID as a string
func (ni *NodeIdentity) String() string {
	return string(ni.NodeID)
}
