/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package storage

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDiskStorePutGet(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	store, err := NewDiskStore(dir)
	if err != nil {
		t.Fatalf("NewDiskStore: %v", err)
	}

	payload := []byte("hello mesh storage")
	hash := Digest(payload)

	if err := store.Put(hash, payload); err != nil {
		t.Fatalf("Put: %v", err)
	}

	// Deduped write should be a no-op.
	if err := store.Put(hash, payload); err != nil {
		t.Fatalf("second Put: %v", err)
	}

	data, err := store.Get(hash)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}

	if string(data) != string(payload) {
		t.Fatalf("unexpected data: got %q want %q", data, payload)
	}

	if !store.Has(hash) {
		t.Fatalf("Has returned false for stored chunk")
	}

	shardDepth := store.Stats()["shard_depth"].(int)
	if shardDepth != defaultShardDepth {
		t.Fatalf("unexpected shard depth: %d", shardDepth)
	}

	// Ensure file exists at expected path.
	expectPath := filepath.Join(dir, hash.Hex()[:2], hash.Hex()[2:4], hash.Hex())
	if _, err := os.Stat(expectPath); err != nil {
		t.Fatalf("chunk not written to expected path: %v", err)
	}
}

func TestDiskStoreEvictAndMissing(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	store, err := NewDiskStore(dir, WithShardDepth(1))
	if err != nil {
		t.Fatalf("NewDiskStore: %v", err)
	}

	hash := Digest([]byte("missing"))

	if store.Has(hash) {
		t.Fatalf("Has reported true for missing chunk")
	}

	if _, err := store.Get(hash); err != ErrChunkNotFound {
		t.Fatalf("Get missing: got %v want %v", err, ErrChunkNotFound)
	}

	if err := store.Evict(hash); err != nil {
		t.Fatalf("Evict missing returned error: %v", err)
	}

	payload := []byte("to remove")
	hash = Digest(payload)
	if err := store.Put(hash, payload); err != nil {
		t.Fatalf("Put: %v", err)
	}

	if err := store.Evict(hash); err != nil {
		t.Fatalf("Evict stored: %v", err)
	}

	if store.Has(hash) {
		t.Fatalf("Has true after Evict")
	}
}
