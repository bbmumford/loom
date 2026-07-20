/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package dispatch

import (
	"sync"
	"sync/atomic"

	"github.com/ORBTR/aether"
)

// dynamicStreamIDFloor is the lowest dynamic stream ID. IDs 0-9 are
// reserved for well-known streams (gossip, RPC, keepalive, control,
// reconcile, BidiRPC). Allocators start at 10.
const dynamicStreamIDFloor = 10

// sessionStreamCounters is a session-pointer-keyed atomic counter pool.
// Every allocator (HWPCaller.openDynamicStream, StreamPool.Fill,
// StreamPool.Get fresh-open) calls nextDynamicStreamID(sess) which
// mints a unique ID FROM THE SAME COUNTER for a given session.
//
// Before this consolidation, HWPCaller.streamID (per-instance) and the
// package-global streamPoolCounter both minted IDs starting at 10 on
// the SAME session. The aether stream multiplexer treated collisions
// as distinct streams (one wins the OpenStream call, the other got
// rejected mid-flight), but the collision would surface as a phantom
// closed-stream error long after the bug — was the
// H-Dispatch-StreamIDCollision finding.
//
// Counters are keyed by aether.Session pointer; an entry is created
// lazily on first use and discarded when forgetSession is called
// (typically when evictIdle prunes a cachedSession). Worst case: a
// session with no entries gets a fresh counter, which still mints
// monotonically-increasing IDs from 10 — and since the session has
// never been used, no collision risk exists.
var sessionStreamCounters sync.Map // map[aether.Session]*atomic.Uint64

// nextDynamicStreamID allocates a unique-per-session dynamic stream ID.
// Safe for concurrent use across HWPCaller instances and StreamPool —
// the counter is per-session, not per-caller.
func nextDynamicStreamID(sess aether.Session) uint64 {
	v, ok := sessionStreamCounters.Load(sess)
	if !ok {
		ctr := new(atomic.Uint64)
		actual, loaded := sessionStreamCounters.LoadOrStore(sess, ctr)
		if loaded {
			ctr = actual.(*atomic.Uint64)
		}
		v = ctr
	}
	return v.(*atomic.Uint64).Add(1) + (dynamicStreamIDFloor - 1)
}

// forgetSession discards the session's counter entry. Call when a
// session is fully torn down (evictIdle, closeAndEvict, etc.) so a
// long-running process doesn't accumulate stale entries.
//
// Safe to call multiple times — sync.Map.Delete is a no-op for missing
// keys. After forgetSession, a subsequent nextDynamicStreamID call on
// the same session pointer would restart from dynamicStreamIDFloor —
// but by then the session is dead so no new dispatch should target it.
func forgetSession(sess aether.Session) {
	sessionStreamCounters.Delete(sess)
}
