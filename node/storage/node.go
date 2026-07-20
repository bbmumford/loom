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

const (
	defaultChunkSize = 4 * 1024 * 1024  // 4 MiB
	minChunkSize     = 64 * 1024        // 64 KiB
	maxChunkSize     = 64 * 1024 * 1024 // 64 MiB (safety guard)
)

// PublishOptions customise how content is chunked and published.
type PublishOptions struct {
	ChunkSize      int
	PrevManifestID *ManifestID
	Meta           map[string]string
}

// PublishResult contains the manifest and identifier returned by Publish.
type PublishResult struct {
	Manifest   *Manifest
	ManifestID ManifestID
}

// Node coordinates storing chunks and publishing manifests to the chain backend.
type Node struct {
	store    StoreBackend
	chain    ChainBackend
	fetchers []FetchBackend
	signer   Signer
}

// NodeOption configures node behaviour.
type NodeOption func(*Node)

// WithSigner sets a manifest signer.
func WithSigner(s Signer) NodeOption {
	return func(n *Node) {
		n.signer = s
	}
}

// WithFetchers appends fetch backends to the node (used for hedged fetch during publication validation).
func WithFetchers(fetchers ...FetchBackend) NodeOption {
	return func(n *Node) {
		n.fetchers = append(n.fetchers, fetchers...)
	}
}

// NewNode constructs a Node.
func NewNode(store StoreBackend, chain ChainBackend, opts ...NodeOption) (*Node, error) {
	if store == nil {
		return nil, errors.New("storage: store backend is required")
	}
	if chain == nil {
		return nil, errors.New("storage: chain backend is required")
	}
	n := &Node{store: store, chain: chain}
	for _, opt := range opts {
		opt(n)
	}
	return n, nil
}

// Publish chunks the reader, stores the data, builds a manifest, signs it (if configured) and publishes it to the chain backend.
func (n *Node) Publish(ctx context.Context, r io.Reader, opts PublishOptions) (*PublishResult, error) {
	if r == nil {
		return nil, errors.New("storage: reader is nil")
	}
	chunkSize := opts.ChunkSize
	if chunkSize <= 0 {
		chunkSize = defaultChunkSize
	}
	if chunkSize < minChunkSize {
		chunkSize = minChunkSize
	}
	if chunkSize > maxChunkSize {
		return nil, fmt.Errorf("storage: chunk size %d exceeds maximum %d", chunkSize, maxChunkSize)
	}

	manifest := &Manifest{
		Version:        1,
		ChunkSize:      uint32(chunkSize),
		PrevManifestID: opts.PrevManifestID,
		Meta:           cloneStringMap(opts.Meta),
	}

	chunkBuf := make([]byte, chunkSize)
	idx := 0
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		nRead, readErr := io.ReadFull(r, chunkBuf)
		// errors.Is so a wrapped EOF / ErrUnexpectedEOF from any reader in
		// the chain still terminates the loop cleanly. See
		// tools/sentinelequalcheck (audit-r2 D3) for the lint that enforces
		// this — the previous == comparison broke silently the moment any
		// intermediate reader returned `fmt.Errorf("...: %w", io.EOF)`.
		if errors.Is(readErr, io.EOF) {
			break
		}
		if errors.Is(readErr, io.ErrUnexpectedEOF) {
			if nRead == 0 {
				break
			}
			chunk := append([]byte(nil), chunkBuf[:nRead]...)
			if err := n.storeChunk(chunk, idx, manifest); err != nil {
				return nil, err
			}
			break
		}
		if readErr != nil {
			return nil, fmt.Errorf("storage: read chunk %d: %w", idx, readErr)
		}
		chunk := append([]byte(nil), chunkBuf[:nRead]...)
		if err := n.storeChunk(chunk, idx, manifest); err != nil {
			return nil, err
		}
		idx++
	}

	if len(manifest.Chunks) == 0 {
		return nil, errors.New("storage: no data read from reader")
	}

	id, err := manifest.ComputeContentID()
	if err != nil {
		return nil, err
	}

	if n.signer != nil {
		canonical, err := manifest.CanonicalBytes()
		if err != nil {
			return nil, err
		}
		sig, err := n.signer.Sign(canonical)
		if err != nil {
			return nil, fmt.Errorf("storage: manifest signing failed: %w", err)
		}
		manifest.WithSignature(sig, n.signer.PublicKey())
	}

	publishedID, err := n.chain.PublishManifest(ctx, manifest)
	if err != nil {
		return nil, err
	}
	if !publishedID.Equal(id) && publishedID != (ManifestID{}) {
		return nil, fmt.Errorf("%w: chain returned %s expected %s", ErrManifestMismatch, publishedID.Hex(), id.Hex())
	}

	return &PublishResult{Manifest: manifest, ManifestID: id}, nil
}

func (n *Node) storeChunk(data []byte, idx int, manifest *Manifest) error {
	hash := Digest(data)
	if !n.store.Has(hash) {
		if err := n.store.Put(hash, data); err != nil {
			return fmt.Errorf("storage: store chunk %d: %w", idx, err)
		}
	}
	manifest.Chunks = append(manifest.Chunks, ChunkRef{Index: idx, Hash: hash, Size: uint32(len(data))})
	return nil
}

func cloneStringMap(src map[string]string) map[string]string {
	if len(src) == 0 {
		return nil
	}
	cpy := make(map[string]string, len(src))
	for k, v := range src {
		cpy[k] = v
	}
	return cpy
}
