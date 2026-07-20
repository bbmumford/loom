/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package storage

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
)

// Hash represents a SHA-256 digest used for chunk and manifest identifiers.
type Hash [32]byte

// ManifestID is an alias for Hash to emphasise manifest identifiers.
type ManifestID = Hash

// ChunkRef stores metadata describing the chunk at a given index of a manifest.
type ChunkRef struct {
	Index int    `json:"index"`
	Hash  Hash   `json:"hash"`
	Size  uint32 `json:"size"`
}

// MustHash creates a Hash from a 32 byte slice, panicking if the size is invalid.
func MustHash(b []byte) Hash {
	var h Hash
	if len(b) != len(h) {
		panic("storage: invalid hash size")
	}
	copy(h[:], b)
	return h
}

// HashFromBytes converts a byte slice into a Hash.
func HashFromBytes(b []byte) (Hash, error) {
	var h Hash
	if len(b) != len(h) {
		return h, errors.New("storage: invalid hash length")
	}
	copy(h[:], b)
	return h, nil
}

// HashFromHex decodes a hex string into a Hash value.
func HashFromHex(s string) (Hash, error) {
	raw, err := hex.DecodeString(s)
	if err != nil {
		return Hash{}, err
	}
	return HashFromBytes(raw)
}

// Hex returns the lowercase hexadecimal representation of the hash.
func (h Hash) Hex() string {
	return hex.EncodeToString(h[:])
}

// Equal reports whether two hashes are identical.
func (h Hash) Equal(other Hash) bool {
	// MESH-H10: removed a dead `if &h == &other` branch — it compared the
	// addresses of two by-value parameter copies, which are always distinct, so
	// it was always false and misleading. Hash is a comparable array; == is the
	// correct check.
	return h == other
}

// Digest computes a SHA-256 digest for the provided data and returns it as a Hash.
func Digest(data []byte) Hash {
	return sha256.Sum256(data)
}
