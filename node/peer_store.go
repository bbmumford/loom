/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// peerStoreMaxEntries caps the in-memory peer store to prevent unbounded growth.
const peerStoreMaxEntries = 500

// PeerStoreConfig configures the file-based peer store.
type PeerStoreConfig struct {
	FilePath     string        // path to the peer list JSON file
	SaveInterval time.Duration // how often to write to disk (default: 5m)
	MaxAge       time.Duration // discard entries older than this on load (default: 24h)
}

// DefaultPeerStoreConfig returns sensible defaults.
func DefaultPeerStoreConfig(dataDir string) PeerStoreConfig {
	return PeerStoreConfig{
		FilePath:     filepath.Join(dataDir, "mesh-peers.json"),
		SaveInterval: 5 * time.Minute,
		MaxAge:       24 * time.Hour,
	}
}

// PersistedPeer is a single entry in the persisted peer list.
type PersistedPeer struct {
	NodeID    string    `json:"nodeId"`
	Addresses []string  `json:"addresses"`
	Region    string    `json:"region"`
	LastSeen  time.Time `json:"lastSeen"`
	Source    string    `json:"source"` // "pex", "lad", "manual"
}

// persistedPeerList is the JSON schema for the persisted file.
type persistedPeerList struct {
	Peers     []PersistedPeer `json:"peers"`
	UpdatedAt time.Time       `json:"updatedAt"`
	NodeID    string          `json:"nodeId"` // identity of the node that wrote this file
}

// FilePeerStore implements a file-based persisted peer list.
// Writes known peers to a JSON file periodically. On startup, loads the file
// and filters out entries older than MaxAge.
type FilePeerStore struct {
	mu     sync.Mutex
	config PeerStoreConfig
	nodeID string            // our own NodeID (excluded from saved list)
	peers  map[string]PersistedPeer // in-memory state keyed by NodeID
}

// NewFilePeerStore creates a file-based peer store.
func NewFilePeerStore(nodeID string, config PeerStoreConfig) *FilePeerStore {
	return &FilePeerStore{
		config: config,
		nodeID: nodeID,
		peers:  make(map[string]PersistedPeer),
	}
}

// Load reads the persisted peer list from disk and filters stale entries.
func (s *FilePeerStore) Load() ([]PersistedPeer, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, err := os.ReadFile(s.config.FilePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // no persisted list yet
		}
		return nil, err
	}

	var list persistedPeerList
	if err := json.Unmarshal(data, &list); err != nil {
		log.Printf("[PEER-STORE] Failed to parse %s: %v (will recreate)", s.config.FilePath, err)
		return nil, nil
	}

	// Filter stale entries and populate in-memory state
	cutoff := time.Now().Add(-s.config.MaxAge)
	var peers []PersistedPeer
	for _, p := range list.Peers {
		if p.LastSeen.Before(cutoff) {
			continue // stale, skip
		}
		if p.NodeID == s.nodeID {
			continue // skip ourselves
		}
		s.peers[p.NodeID] = p
		peers = append(peers, p)
	}

	staleCount := len(list.Peers) - len(peers)
	dbgPeers.Printf("Loaded %d peers from %s (%d stale entries filtered)",
		len(peers), s.config.FilePath, staleCount)
	return peers, nil
}

// Save writes the current peer list to disk atomically (write temp, rename).
func (s *FilePeerStore) Save() error {
	s.mu.Lock()
	entries := make([]PersistedPeer, 0, len(s.peers))
	for _, p := range s.peers {
		if p.NodeID == s.nodeID {
			continue
		}
		entries = append(entries, p)
	}
	nodeID := s.nodeID
	s.mu.Unlock()

	list := persistedPeerList{
		Peers:     entries,
		UpdatedAt: time.Now(),
		NodeID:    nodeID,
	}

	data, err := json.MarshalIndent(list, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal peer list: %w", err)
	}

	// Ensure directory exists
	dir := filepath.Dir(s.config.FilePath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create peer store directory: %w", err)
	}

	// Write to temp file then rename for atomic update
	tmpFile := s.config.FilePath + ".tmp"
	if err := os.WriteFile(tmpFile, data, 0o644); err != nil {
		return fmt.Errorf("write temp peer file: %w", err)
	}
	if err := os.Rename(tmpFile, s.config.FilePath); err != nil {
		os.Remove(tmpFile)
		return fmt.Errorf("rename peer file: %w", err)
	}

	dbgPeers.Printf("Saved %d peers to %s", len(entries), s.config.FilePath)
	return nil
}

// Add adds or updates a peer in the store.
func (s *FilePeerStore) Add(peer PersistedPeer) {
	if peer.NodeID == s.nodeID {
		return // never store ourselves
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	peer.LastSeen = time.Now()
	s.peers[peer.NodeID] = peer
	s.pruneIfNeeded()
}

// pruneIfNeeded evicts oldest peers by LastSeen when the store exceeds its cap.
// Must be called with mu held.
func (s *FilePeerStore) pruneIfNeeded() {
	if len(s.peers) <= peerStoreMaxEntries {
		return
	}
	evictCount := len(s.peers) - peerStoreMaxEntries
	for i := 0; i < evictCount; i++ {
		var oldestID string
		var oldestTime time.Time
		for id, p := range s.peers {
			if oldestID == "" || p.LastSeen.Before(oldestTime) {
				oldestID = id
				oldestTime = p.LastSeen
			}
		}
		delete(s.peers, oldestID)
	}
	dbgPeers.Printf("Pruned to %d entries (cap=%d, evicted=%d)",
		len(s.peers), peerStoreMaxEntries, evictCount)
}

// Remove removes a peer from the store.
func (s *FilePeerStore) Remove(nodeID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.peers, nodeID)
}

// All returns all currently stored peers.
func (s *FilePeerStore) All() []PersistedPeer {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]PersistedPeer, 0, len(s.peers))
	for _, p := range s.peers {
		result = append(result, p)
	}
	return result
}

// StartPeriodicSave begins a background goroutine that saves the peer list
// at the configured interval. Performs a final save on context cancellation.
func (s *FilePeerStore) StartPeriodicSave(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(s.config.SaveInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				// Final save on shutdown
				if err := s.Save(); err != nil {
					log.Printf("[PEER-STORE] Final save failed: %v", err)
				}
				return
			case <-ticker.C:
				if err := s.Save(); err != nil {
					log.Printf("[PEER-STORE] Periodic save failed: %v", err)
				}
			}
		}
	}()
}
