/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
)

// Leech retrieves manifests and chunks, populating the local store as it goes.
type Leech struct {
	store    StoreBackend
	chain    ChainBackend
	fetchers []FetchBackend
}

// LeechOption configures a Leech instance.
type LeechOption func(*Leech)

// WithLeechFetchers appends fetch backends used during retrieval.
func WithLeechFetchers(fetchers ...FetchBackend) LeechOption {
	return func(l *Leech) {
		l.fetchers = append(l.fetchers, fetchers...)
	}
}

// NewLeech constructs a Leech.
func NewLeech(store StoreBackend, chain ChainBackend, opts ...LeechOption) (*Leech, error) {
	if store == nil {
		return nil, errors.New("storage: store backend is required")
	}
	if chain == nil {
		return nil, errors.New("storage: chain backend is required")
	}
	l := &Leech{store: store, chain: chain}
	for _, opt := range opts {
		opt(l)
	}
	return l, nil
}

// FetchByName resolves a manifest via name pointer and streams it to w.
func (l *Leech) FetchByName(ctx context.Context, name string, w io.Writer) (*Manifest, error) {
	manifestID, err := l.chain.ResolveName(ctx, name)
	if err != nil {
		return nil, err
	}
	return l.FetchByID(ctx, manifestID, w)
}

// FetchByID loads a manifest from the chain and then streams content to w.
func (l *Leech) FetchByID(ctx context.Context, id ManifestID, w io.Writer) (*Manifest, error) {
	manifest, err := l.chain.LoadManifest(ctx, id)
	if err != nil {
		return nil, err
	}
	// MESH-H04: verify the loaded manifest actually hashes to the requested id
	// before fetching any chunks. Content-addressing IS the store's integrity
	// guarantee — without this a compromised/buggy chain or cache could return a
	// manifest with attacker-chosen chunk hashes and the leech would faithfully
	// fetch and serve that content under the requested content-ID. Recomputing
	// the ID binds the chunk-hash list to `id`. (Publisher-signature
	// verification would add authenticity but needs a trusted-key anchor the
	// chain doesn't currently expose.)
	if manifest == nil {
		return nil, errors.New("storage: chain returned nil manifest")
	}
	computed, err := manifest.ComputeContentID()
	if err != nil {
		return nil, fmt.Errorf("storage: compute manifest content-ID: %w", err)
	}
	if !computed.Equal(id) {
		return nil, fmt.Errorf("storage: manifest content-ID mismatch: requested %s, got %s", id.Hex(), computed.Hex())
	}
	if err := l.FetchManifest(ctx, manifest, w); err != nil {
		return nil, err
	}
	return manifest, nil
}

// FetchManifest streams the provided manifest to w, hydrating missing chunks along the way.
func (l *Leech) FetchManifest(ctx context.Context, manifest *Manifest, w io.Writer) error {
	if manifest == nil {
		return errors.New("storage: manifest is nil")
	}
	if err := manifest.Validate(); err != nil {
		return err
	}
	for _, chunk := range manifest.Chunks {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		data, err := l.obtainChunk(ctx, chunk)
		if err != nil {
			return err
		}

		if w != nil {
			if _, err := w.Write(data); err != nil {
				return fmt.Errorf("storage: write chunk %d: %w", chunk.Index, err)
			}
		}
	}
	return nil
}

func (l *Leech) obtainChunk(ctx context.Context, chunk ChunkRef) ([]byte, error) {
	if l.store.Has(chunk.Hash) {
		data, err := l.store.Get(chunk.Hash)
		if err == nil && verifyChunk(chunk, data) == nil {
			return data, nil
		}
	}

	var lastErr error
	for _, fetcher := range l.fetchers {
		data, err := fetcher.Fetch(ctx, chunk.Hash)
		if err != nil {
			lastErr = err
			continue
		}
		if err := verifyChunk(chunk, data); err != nil {
			lastErr = err
			continue
		}
		if err := l.store.Put(chunk.Hash, data); err != nil {
			return nil, fmt.Errorf("storage: store chunk %d: %w", chunk.Index, err)
		}
		return data, nil
	}

	if lastErr == nil {
		lastErr = fmt.Errorf("%w: %s", ErrChunkNotFound, chunk.Hash.Hex())
	}
	return nil, lastErr
}

func verifyChunk(ref ChunkRef, data []byte) error {
	if uint32(len(data)) != ref.Size {
		return fmt.Errorf("storage: chunk %d size mismatch", ref.Index)
	}
	hash := Digest(data)
	if !hash.Equal(ref.Hash) {
		return fmt.Errorf("storage: chunk %d hash mismatch", ref.Index)
	}
	return nil
}
