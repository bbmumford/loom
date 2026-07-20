/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package storage

import "errors"

var (
	// ErrChunkNotFound is returned when a chunk cannot be located locally or via fetchers.
	ErrChunkNotFound = errors.New("storage: chunk not found")
	// ErrManifestMismatch is returned when the manifest identifier returned by the chain backend does not match the computed content ID.
	ErrManifestMismatch = errors.New("storage: manifest identifier mismatch")
)
