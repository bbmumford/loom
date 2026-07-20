/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package gossip

import (
	"sync/atomic"

	"github.com/bbmumford/whisper"
)

// Process-wide driver registry. The Runtime constructs the
// reconciliation and snapshot drivers once at boot (a single codec +
// store pair binds the entire node's gossip stack) and parks them
// here so every per-connection responder can hand the same drivers to
// its whisper.Engine via WithReconcile / WithSnapshot.
//
// Atomic.Value is used instead of a mutex because the registry is
// write-once-on-startup and read on every gossip responder spin-up —
// the tens-of-microseconds-per-spin-up cost matters more than the
// startup ergonomics of a mutex, especially under reconnect storms.
var (
	sharedReconcileDriver atomic.Value // *whisper.ReconcileDriver
	sharedSnapshotDriver  atomic.Value // *whisper.SnapshotDriver
)

// SetReconcileDriver registers the process-wide reconciliation
// driver. Pass nil to clear (used in tests). Idempotent — later calls
// override earlier ones.
func SetReconcileDriver(d *whisper.ReconcileDriver) {
	if d == nil {
		sharedReconcileDriver.Store((*whisper.ReconcileDriver)(nil))
		return
	}
	sharedReconcileDriver.Store(d)
}

// ReconcileDriver returns the registered driver or nil if none has
// been wired. Callers that pass the result to whisper.WithReconcile
// MUST guard nil — WithReconcile(nil) leaves the engine without a G4
// handler, which matches the no-driver-no-handler contract.
func ReconcileDriver() *whisper.ReconcileDriver {
	v := sharedReconcileDriver.Load()
	if v == nil {
		return nil
	}
	d, _ := v.(*whisper.ReconcileDriver)
	return d
}

// SetSnapshotDriver registers the process-wide G5 snapshot driver.
func SetSnapshotDriver(d *whisper.SnapshotDriver) {
	if d == nil {
		sharedSnapshotDriver.Store((*whisper.SnapshotDriver)(nil))
		return
	}
	sharedSnapshotDriver.Store(d)
}

// SnapshotDriver returns the registered driver or nil.
func SnapshotDriver() *whisper.SnapshotDriver {
	v := sharedSnapshotDriver.Load()
	if v == nil {
		return nil
	}
	d, _ := v.(*whisper.SnapshotDriver)
	return d
}
