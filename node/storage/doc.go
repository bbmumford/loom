/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
// Package storage implements a content-addressed chunk storage layer with
// pluggable store, fetch, and chain backends for the Orbtr mesh SDK. The
// package provides helpers to chunk and publish data (`Node`), hydrate content
// on demand (`Leech`), and compose reusable infrastructure via `Manager` and
// `Config`. A disk-backed StoreBackend ships out of the box; additional
// providers (S3, in-memory, libp2p) can be registered by satisfying the
// interfaces defined in backend.go.
package storage
