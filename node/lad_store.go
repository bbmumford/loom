/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"crypto/ed25519"
	"crypto/hkdf"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"

	"github.com/bbmumford/tursoraft/localdb"
)

// ladKeyInfo is the HKDF info string of the LAD file key. A change to it is a
// new key: every node's LAD file is then rebuilt from gossip.
const ladKeyInfo = "loom/lad/v1"

// ladFileName is the LAD file under LADDirectoryConfig.LocalDir.
const ladFileName = "lad.db"

// ladFileKey derives the key the LAD file is encrypted under:
// HKDF-SHA256 over the node identity's 32-byte ed25519 seed, with no salt
// (the seed is uniformly random) and info ladKeyInfo. The file is readable
// only by a process holding that identity, and a node needs no key
// configured for it.
func ladFileKey(priv ed25519.PrivateKey) ([]byte, error) {
	if len(priv) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("LAD key: node private key is %d bytes, want %d", len(priv), ed25519.PrivateKeySize)
	}
	return hkdf.Key(sha256.New, priv.Seed(), nil, ladKeyInfo, localdb.KeyLen)
}

// openLADFile opens dir/lad.db encrypted under key through tursoraft's
// localdb (tursogo, AES-256-GCM, foreign keys proven on every connection).
//
// LAD is a cache of gossip, so a file this node cannot use is deleted and
// rebuilt rather than kept: a plaintext file, which localdb refuses to open
// under a key, and a file that does not open under this key (one written
// under another node identity).
func openLADFile(ctx context.Context, dir string, key []byte) (*sql.DB, error) {
	if !filepath.IsAbs(dir) {
		return nil, fmt.Errorf("LAD local directory %q must be an absolute path", dir)
	}
	path := filepath.Join(dir, ladFileName)
	db, err := localdb.Open(ctx, path, key)
	if err == nil {
		return db, nil
	}
	if _, statErr := os.Stat(path); statErr != nil {
		return nil, fmt.Errorf("open LAD file: %w", err)
	}
	log.Printf("[NODE] LAD file %s does not open under this node's key (%v); deleting it, LAD is rebuilt from gossip", path, err)
	if err := removeLADFile(path); err != nil {
		return nil, err
	}
	db, err = localdb.Open(ctx, path, key)
	if err != nil {
		return nil, fmt.Errorf("open LAD file: %w", err)
	}
	return db, nil
}

// removeLADFile deletes the LAD file and its companion files.
func removeLADFile(path string) error {
	for _, p := range []string{path, path + "-wal", path + "-shm", path + "-journal"} {
		if err := os.Remove(p); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return fmt.Errorf("delete LAD file %s: %w", p, err)
		}
	}
	return nil
}
