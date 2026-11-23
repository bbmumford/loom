/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
 */
// Package storage implements a content-addressed chunk storage layer with
// pluggable store, fetch, and chain backends for the loom mesh. The
// package provides helpers to chunk and publish data (`Node`), hydrate content
// on demand (`Leech`), and compose reusable infrastructure via `Manager` and
// `Config`. A disk-backed StoreBackend ships out of the box; additional
// providers (S3, in-memory, libp2p) can be registered by satisfying the
// interfaces defined in backend.go.
package storage
