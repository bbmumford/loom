/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package storage

import (
	"context"
	"testing"
)

func TestConfigValidate(t *testing.T) {
	t.Parallel()

	cfg := Config{
		Store: StoreConfig{
			Disk: &DiskStoreConfig{Root: t.TempDir()},
		},
	}

	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate returned error: %v", err)
	}
}

func TestManagerBuildsNodeAndLeech(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	cfg := Config{Store: StoreConfig{Disk: &DiskStoreConfig{Root: dir, ShardDepth: 1}}}

	fetcher := stubFetcher{}
	mgr, err := NewManager(cfg, stubChain{}, WithManagerFetchers(fetcher))
	if err != nil {
		t.Fatalf("NewManager: %v", err)
	}

	if mgr.Store() == nil {
		t.Fatalf("Store is nil")
	}

	if len(mgr.Fetchers()) != 1 {
		t.Fatalf("expected 1 fetcher, got %d", len(mgr.Fetchers()))
	}

	var nodeFetcherCount int
	if _, err := mgr.NewNode(func(n *Node) { nodeFetcherCount = len(n.fetchers) }); err != nil {
		t.Fatalf("NewNode: %v", err)
	}
	if nodeFetcherCount != 1 {
		t.Fatalf("expected 1 fetcher on node, got %d", nodeFetcherCount)
	}

	var leechFetcherCount int
	if _, err := mgr.NewLeech(func(l *Leech) { leechFetcherCount = len(l.fetchers) }); err != nil {
		t.Fatalf("NewLeech: %v", err)
	}
	if leechFetcherCount != 1 {
		t.Fatalf("expected 1 fetcher on leech, got %d", leechFetcherCount)
	}
}

type stubFetcher struct{}

func (stubFetcher) Fetch(context.Context, Hash) ([]byte, error) {
	return nil, ErrChunkNotFound
}

type stubChain struct{}

func (stubChain) PublishManifest(context.Context, *Manifest) (ManifestID, error) {
	return ManifestID{}, nil
}
func (stubChain) ResolveName(context.Context, string) (ManifestID, error) { return ManifestID{}, nil }
func (stubChain) GetManifestHeader(context.Context, ManifestID) (*ManifestHeader, error) {
	return &ManifestHeader{}, nil
}
func (stubChain) LoadManifest(context.Context, ManifestID) (*Manifest, error) {
	return &Manifest{}, nil
}
func (stubChain) WatchUpdates(context.Context, string) (<-chan UpdateEvent, error) {
	ch := make(chan UpdateEvent)
	close(ch)
	return ch, nil
}
func (stubChain) AnnounceProvides(context.Context, string, Hash) error { return nil }
