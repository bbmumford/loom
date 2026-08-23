/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package storage

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
)

// DiskStore implements StoreBackend by persisting chunks on the local filesystem.
//
// Chunks are organized using a simple sharding scheme to avoid hot directories.
// By default two levels of hexadecimal sharding are used (e.g. <root>/aa/bb/<hash>).
//
// The store is safe for concurrent use and ensures idempotent writes: if a chunk
// already exists on disk, Put becomes a no-op.
type DiskStore struct {
	root       string
	shardDepth int
	dirPerm    fs.FileMode
	filePerm   fs.FileMode

	once sync.Once
	err  error
}

const (
	defaultShardDepth = 2
	maxShardDepth     = 4
	defaultDirPerm    = 0o755
	defaultFilePerm   = 0o644
)

// DiskStoreOption configures DiskStore construction.
type DiskStoreOption func(*DiskStore)

// WithShardDepth sets the number of two-character hex directory shards. Values
// below zero disable sharding, values greater than four are clamped.
func WithShardDepth(depth int) DiskStoreOption {
	return func(ds *DiskStore) {
		switch {
		case depth <= 0:
			ds.shardDepth = 0
		case depth > maxShardDepth:
			ds.shardDepth = maxShardDepth
		default:
			ds.shardDepth = depth
		}
	}
}

// WithDirectoryPerm sets the directory permission bits created by the store.
func WithDirectoryPerm(perm fs.FileMode) DiskStoreOption {
	return func(ds *DiskStore) {
		if perm != 0 {
			ds.dirPerm = perm
		}
	}
}

// WithFilePerm sets the file permission bits for stored chunks.
func WithFilePerm(perm fs.FileMode) DiskStoreOption {
	return func(ds *DiskStore) {
		if perm != 0 {
			ds.filePerm = perm
		}
	}
}

// NewDiskStore constructs a DiskStore rooted at the provided directory.
// The directory is created if it does not already exist.
func NewDiskStore(root string, opts ...DiskStoreOption) (*DiskStore, error) {
	if root == "" {
		return nil, fmt.Errorf("storage: disk store root cannot be empty")
	}

	absRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("storage: resolve disk root: %w", err)
	}

	store := &DiskStore{
		root:       absRoot,
		shardDepth: defaultShardDepth,
		dirPerm:    defaultDirPerm,
		filePerm:   defaultFilePerm,
	}

	for _, opt := range opts {
		opt(store)
	}

	if err := os.MkdirAll(store.root, store.dirPerm); err != nil {
		return nil, fmt.Errorf("storage: ensure disk root: %w", err)
	}

	return store, nil
}

// Put writes the provided chunk if it is not already present on disk.
func (ds *DiskStore) Put(hash Hash, data []byte) error {
	if err := ds.ensureRoot(); err != nil {
		return err
	}

	if computed := Digest(data); computed != hash {
		return fmt.Errorf("storage: chunk digest mismatch: expected %s, got %s", hash.Hex(), computed.Hex())
	}

	path := ds.pathFor(hash)

	if _, err := os.Stat(path); err == nil {
		// Already persisted; dedupe hit.
		return nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("storage: stat chunk: %w", err)
	}

	if err := os.MkdirAll(filepath.Dir(path), ds.dirPerm); err != nil {
		return fmt.Errorf("storage: ensure shard dir: %w", err)
	}

	// Write to a temp file in the same directory, fsync, then
	// atomically rename into the content-addressed path. Writing directly to
	// `path` (O_CREATE|O_EXCL) left a TRUNCATED file there on a crash between
	// create and Sync; the dedupe Stat above then never rewrote it and Get
	// served the corrupt bytes unverified. A rename is atomic, so a reader never
	// observes a partial chunk. (delta_persist.go uses the same pattern.)
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-chunk-*")
	if err != nil {
		return fmt.Errorf("storage: create temp chunk: %w", err)
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("storage: write chunk: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		_ = os.Remove(tmpName)
		return fmt.Errorf("storage: sync chunk: %w", err)
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("storage: close temp chunk: %w", err)
	}
	if err := os.Chmod(tmpName, ds.filePerm); err != nil {
		_ = os.Remove(tmpName)
		return fmt.Errorf("storage: chmod temp chunk: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		_ = os.Remove(tmpName)
		// A concurrent writer may have already placed the (identical,
		// content-addressed) chunk — treat an existing target as success.
		if _, statErr := os.Stat(path); statErr == nil {
			return nil
		}
		return fmt.Errorf("storage: rename chunk into place: %w", err)
	}
	return nil
}

// Get retrieves the chunk data for the supplied hash.
func (ds *DiskStore) Get(hash Hash) ([]byte, error) {
	if err := ds.ensureRoot(); err != nil {
		return nil, err
	}

	path := ds.pathFor(hash)
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, ErrChunkNotFound
		}
		return nil, fmt.Errorf("storage: read chunk: %w", err)
	}
	return data, nil
}

// Has reports whether the given chunk is present on disk.
func (ds *DiskStore) Has(hash Hash) bool {
	if err := ds.ensureRoot(); err != nil {
		return false
	}

	_, err := os.Stat(ds.pathFor(hash))
	return err == nil
}

// Evict removes a chunk from disk.
func (ds *DiskStore) Evict(hash Hash) error {
	if err := ds.ensureRoot(); err != nil {
		return err
	}

	if err := os.Remove(ds.pathFor(hash)); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("storage: remove chunk: %w", err)
	}
	return nil
}

// Stats returns basic information about the disk backend.
func (ds *DiskStore) Stats() map[string]any {
	return map[string]any{
		"provider":    "disk",
		"root":        ds.root,
		"shard_depth": ds.shardDepth,
	}
}

func (ds *DiskStore) ensureRoot() error {
	ds.once.Do(func() {
		if _, err := os.Stat(ds.root); err != nil {
			if errors.Is(err, fs.ErrNotExist) {
				ds.err = os.MkdirAll(ds.root, ds.dirPerm)
			} else {
				ds.err = err
			}
		}
	})
	return ds.err
}

func (ds *DiskStore) pathFor(hash Hash) string {
	hex := hash.Hex()
	if ds.shardDepth <= 0 {
		return filepath.Join(ds.root, hex)
	}

	var parts []string
	parts = append(parts, ds.root)
	for i := 0; i < ds.shardDepth && i*2+2 <= len(hex); i++ {
		parts = append(parts, hex[i*2:i*2+2])
	}
	parts = append(parts, hex)
	return filepath.Join(parts...)
}
