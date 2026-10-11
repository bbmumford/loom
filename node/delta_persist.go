/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/bbmumford/loom/core/directory/gossip"
	"github.com/bbmumford/whisper"
)

// The declared bounds of the persisted delta watermarks, which
// persistDeltaWatermarks passes to the tracker.
//
// deltaWatermarkSaveInterval is how often the tracker writes its watermarks
// while the runtime runs; it writes once more when the runtime's context ends.
// A generation that dies without that last write resumes from watermarks at
// most one interval old, which only widens its first delta.
//
// deltaWatermarkMaxAge is the oldest persisted watermark the tracker loads.
// The gossip initiator loop sends a peer only the records stamped after its
// watermark, which is complete only while the peer still holds what it had
// then, and each runtime's directory cache drops records as they age (the
// eviction sweep Initialize starts with StartEviction). Past this bound the
// watermark is dropped and that peer's first exchange is a full sync.
const (
	deltaWatermarkSaveInterval = 30 * time.Second
	deltaWatermarkMaxAge       = 20 * time.Minute
)

// startDeltaTracker gives this runtime generation its own gossip delta
// tracker, which the gossip loops mesh_connection.go starts read and advance,
// so no other runtime in the process shares its watermarks. On a runtime with
// durable storage (persistsToDataDir) it persists them through
// NewJSONDeltaPersistence under the data directory: the persisted watermarks
// load now, so a peer whose watermark is still fresh gets a delta on its first
// exchange after a restart instead of a full snapshot. Without durable storage
// the watermarks live in memory only.
func (rt *Runtime) startDeltaTracker() {
	rt.deltaTracker = gossip.NewDeltaTracker()
	if !rt.persistsToDataDir() {
		return
	}
	rt.persistDeltaWatermarks(NewJSONDeltaPersistence(rt.cfg.DataDir))
	log.Printf("[NODE] gossip delta watermarks persisted under %s (save %v, max age %v)",
		filepath.Join(rt.cfg.DataDir, ".mesh"), deltaWatermarkSaveInterval, deltaWatermarkMaxAge)
}

// persistDeltaWatermarks attaches backend to this generation's tracker and
// makes the runtime's stop sequence wait for the tracker's last write. The
// tracker loads backend's watermarks now and saves through it on a goroutine
// AttachPersistence starts and returns no handle for, so the tracker is given
// a deltaWatermarkStore instead and a goroutine enrolled with Runtime.Go waits
// for that store's last write. Shutdown's rt.cancel then rt.wg.Wait therefore
// returns only once the watermarks are on disk and this generation writes
// nothing more under the data directory, where the next generation's
// Initialize loads them. It returns the store so a test can read its state.
//
// TODO: wait on the tracker's save loop itself once AttachPersistence returns
// a handle for it.
func (rt *Runtime) persistDeltaWatermarks(backend whisper.DeltaPersistence) *deltaWatermarkStore {
	store := &deltaWatermarkStore{backend: backend, ended: rt.ctx.Done(), last: make(chan struct{})}
	rt.deltaTracker.AttachPersistence(rt.ctx, store, deltaWatermarkSaveInterval, deltaWatermarkMaxAge)
	rt.Go("gossip.delta_watermark_save", store.waitLastSave)
	return store
}

// deltaWatermarkStore is the persistence a runtime's tracker saves through.
// It passes every save to backend until the runtime's context ends (ended).
// The first save after that is the tracker's last one for this generation:
// the store writes it, reports it done (waitLastSave) and writes nothing
// afterwards. The tracker's save loop runs one more save after it when a
// periodic tick and the context's end arrive together, and without this that
// write would land after Shutdown had returned, under a directory the next
// generation is already reading.
type deltaWatermarkStore struct {
	backend whisper.DeltaPersistence
	ended   <-chan struct{}

	mu     sync.Mutex
	closed bool
	last   chan struct{}
}

// Save writes the tracker's snapshot through backend, and nothing once the
// last save of this generation has been written.
func (s *deltaWatermarkStore) Save(states []whisper.PersistedPeerState) error {
	select {
	case <-s.ended:
	default:
		return s.backend.Save(states)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	defer close(s.last)
	return s.backend.Save(states)
}

// Load reads the persisted snapshot from backend.
func (s *deltaWatermarkStore) Load() ([]whisper.PersistedPeerState, error) {
	return s.backend.Load()
}

// waitLastSave blocks until the runtime's context has ended and the save the
// tracker makes on that end has returned. persistDeltaWatermarks runs it on a
// goroutine Shutdown waits for.
func (s *deltaWatermarkStore) waitLastSave() {
	<-s.ended
	<-s.last
}

// persistsToDataDir reports whether this runtime keeps state under its data
// directory across restarts: a data directory is set and LAD storage is
// neither disabled nor "memory". A node on memory storage (a bootstrap anchor
// with no mounted volume) has no durable filesystem, so writing there would
// churn the container's ephemeral layer without ever surviving a restart. The
// peer store and the delta watermarks both follow this rule.
func (rt *Runtime) persistsToDataDir() bool {
	return rt.cfg.DataDir != "" && rt.cfg.LAD.Storage != "" && rt.cfg.LAD.Storage != "memory"
}

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
// file at <dataDir>/.mesh/watermarks.json. It creates the parent
// directory and ignores an error doing so: Load then returns no states
// or an error, and whisper's AttachPersistence starts empty on either;
// each Save returns its create-temp error, which the tracker's save loop
// logs. The runtime keeps running without persisted watermarks, so every
// restart starts cold.
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
