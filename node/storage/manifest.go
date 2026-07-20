/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"sort"
)

// Manifest represents a content-addressed collection of chunks.
type Manifest struct {
	ContentID      ManifestID        `json:"contentID"`
	Version        uint64            `json:"version"`
	ChunkSize      uint32            `json:"chunkSize"`
	Chunks         []ChunkRef        `json:"chunks"`
	PrevManifestID *ManifestID       `json:"prevManifestID,omitempty"`
	Meta           map[string]string `json:"meta,omitempty"`
	Signature      []byte            `json:"signature,omitempty"`
	PublisherKey   []byte            `json:"publisherKey,omitempty"`
}

// metaEntry is used for deterministic JSON encoding.
type metaEntry struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type canonicalChunk struct {
	Index int    `json:"index"`
	Hash  string `json:"hash"`
	Size  uint32 `json:"size"`
}

type canonicalManifest struct {
	Version        uint64           `json:"version"`
	ChunkSize      uint32           `json:"chunkSize"`
	Chunks         []canonicalChunk `json:"chunks"`
	PrevManifestID string           `json:"prevManifestID,omitempty"`
	Meta           []metaEntry      `json:"meta,omitempty"`
}

// Validate performs static checks on the manifest contents.
func (m *Manifest) Validate() error {
	if m == nil {
		return errors.New("storage: manifest is nil")
	}
	if m.ChunkSize == 0 {
		return errors.New("storage: chunk size must be greater than zero")
	}
	for i, chunk := range m.Chunks {
		if chunk.Index != i {
			return fmt.Errorf("storage: chunk index mismatch at %d", i)
		}
		if chunk.Size == 0 {
			return fmt.Errorf("storage: chunk %d has zero size", i)
		}
	}
	return nil
}

// CanonicalBytes returns a deterministic JSON encoding used for hashing and signing.
func (m *Manifest) CanonicalBytes() ([]byte, error) {
	if err := m.Validate(); err != nil {
		return nil, err
	}
	canon := canonicalManifest{
		Version:   m.Version,
		ChunkSize: m.ChunkSize,
		Chunks:    make([]canonicalChunk, len(m.Chunks)),
	}
	for i, chunk := range m.Chunks {
		canon.Chunks[i] = canonicalChunk{
			Index: chunk.Index,
			Hash:  chunk.Hash.Hex(),
			Size:  chunk.Size,
		}
	}
	if m.PrevManifestID != nil {
		canon.PrevManifestID = m.PrevManifestID.Hex()
	}
	if len(m.Meta) > 0 {
		names := make([]string, 0, len(m.Meta))
		for k := range m.Meta {
			names = append(names, k)
		}
		sort.Strings(names)
		canon.Meta = make([]metaEntry, 0, len(names))
		for _, name := range names {
			canon.Meta = append(canon.Meta, metaEntry{Key: name, Value: m.Meta[name]})
		}
	}
	return json.Marshal(canon)
}

// ComputeContentID recalculates the manifest content ID from canonical bytes.
func (m *Manifest) ComputeContentID() (ManifestID, error) {
	bytes, err := m.CanonicalBytes()
	if err != nil {
		return ManifestID{}, err
	}
	id := Digest(bytes)
	m.ContentID = id
	return id, nil
}

// WithSignature updates the manifest with the provided signature and optional publisher key bytes.
func (m *Manifest) WithSignature(sig []byte, pubKey []byte) {
	m.Signature = sig
	m.PublisherKey = pubKey
}
