/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/bbmumford/whisper"
)

// jsonDeltaPersistence stores DeltaTracker watermarks as a single JSON
// file under <dataDir>/.mesh/watermarks.json. Atomic write (temp +
// rename) so a crash mid-save can never produce a half-written file
// that fails to parse on the next Load.
//
// Backing format is a flat array of whisper.PersistedPeerState — the
// schema lives in whisper to keep the persistence boundary one-sided
// (consumers don't dictate to whisper what it persists; whisper
// dictates the schema, consumers pick the storage).
//
// Mutex serialises Save against Load and against itself so a slow
// disk doesn't produce concurrent rewrites of the same target file.
// In practice Save runs on a 30-sec ticker so contention is rare.
type jsonDeltaPersistence struct {
	mu   sync.Mutex
	path string
}

// NewJSONDeltaPersistence returns a DeltaPersistence backed by a JSON
// file at <dataDir>/.mesh/watermarks.json. Creates the parent
// directory on first use; logs (but doesn't fail) if the directory
// can't be created — the consumer's runtime continues without
// persistence (degrades to today's behaviour where every restart
// starts cold).
func NewJSONDeltaPersistence(dataDir string) whisper.DeltaPersistence {
	dir := filepath.Join(dataDir, ".mesh")
	_ = os.MkdirAll(dir, 0o700)
	return &jsonDeltaPersistence{
		path: filepath.Join(dir, "watermarks.json"),
	}
}

// Save writes the snapshot atomically. Temp file + rename so a crash
// mid-write never leaves a half-encoded JSON that the next Load can't
// parse — instead the rename is the atomic commit point.
func (p *jsonDeltaPersistence) Save(states []whisper.PersistedPeerState) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	tmp, err := os.CreateTemp(filepath.Dir(p.path), ".watermarks-*.tmp")
	if err != nil {
		return fmt.Errorf("watermarks: create temp: %w", err)
	}
	tmpPath := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpPath) }

	if err := json.NewEncoder(tmp).Encode(states); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("watermarks: encode: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		cleanup()
		return fmt.Errorf("watermarks: sync temp: %w", err)
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return fmt.Errorf("watermarks: close temp: %w", err)
	}
	if err := os.Rename(tmpPath, p.path); err != nil {
		cleanup()
		return fmt.Errorf("watermarks: rename: %w", err)
	}
	return nil
}

// Load reads the persisted snapshot. Missing file returns (nil, nil)
// — the first-ever boot case. Corrupted or unreadable files return
// an error so the caller can log and continue with empty state.
func (p *jsonDeltaPersistence) Load() ([]whisper.PersistedPeerState, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	data, err := os.ReadFile(p.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("watermarks: read: %w", err)
	}
	var out []whisper.PersistedPeerState
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, fmt.Errorf("watermarks: decode: %w", err)
	}
	return out, nil
}
