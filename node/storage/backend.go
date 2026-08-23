/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package storage

import (
	"context"
)

// StoreBackend defines the content-addressable storage backend responsible for persisting chunks locally.
type StoreBackend interface {
	Put(hash Hash, data []byte) error
	Get(hash Hash) ([]byte, error)
	Has(hash Hash) bool
	Evict(hash Hash) error
	Stats() map[string]any
}

// FetchBackend is used by leechers to retrieve chunks from remote peers or gateways.
type FetchBackend interface {
	Fetch(ctx context.Context, hash Hash) ([]byte, error)
}

// ManifestHeader contains lightweight manifest metadata returned by chain backends.
type ManifestHeader struct {
	ManifestID ManifestID
	Size       int64
	ChunkSize  uint32
	ChunkCount int
	Signature  []byte
}

// UpdateEvent represents a change notification from the chain layer (e.g. manifest pointer update).
type UpdateEvent struct {
	Pointer   string
	Manifest  ManifestID
	Timestamp int64
}

// ChainBackend exposes interactions with the blockchain/control plane.
type ChainBackend interface {
	PublishManifest(ctx context.Context, m *Manifest) (ManifestID, error)
	ResolveName(ctx context.Context, name string) (ManifestID, error)
	GetManifestHeader(ctx context.Context, id ManifestID) (*ManifestHeader, error)
	LoadManifest(ctx context.Context, id ManifestID) (*Manifest, error)
	WatchUpdates(ctx context.Context, pointer string) (<-chan UpdateEvent, error)
	AnnounceProvides(ctx context.Context, peerID string, root Hash) error
}

// Signer is responsible for signing manifest canonical bytes.
type Signer interface {
	Sign(data []byte) ([]byte, error)
	PublicKey() []byte
}

// SignerFunc adapts a function to the Signer interface (no public key exposure).
type SignerFunc func(data []byte) ([]byte, error)

// Sign implements Signer for SignerFunc.
func (sf SignerFunc) Sign(data []byte) ([]byte, error) { return sf(data) }

// PublicKey returns nil for SignerFunc; callers should inject a richer Signer where public key disclosure is required.
func (sf SignerFunc) PublicKey() []byte { return nil }
