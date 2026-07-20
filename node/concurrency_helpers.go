/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"context"
	"log"
	"runtime/debug"
	"time"
)

// safeGo runs fn in a goroutine with panic recovery. label identifies the
// goroutine in the recovery log so an unexpected panic in any spawned
// helper is debuggable without losing the rest of the call's progress.
//
// Use safeGo for every goroutine in mesh/node — bare `go fn()` lets a
// panic in one fan-out kill the entire process, defeating BidiRPC's own
// per-request recovery. The forwarder probe loop and HandleIncomingSessions
// are the documented exposures; safeGo closes them via a single helper so
// reviewers can grep for the pattern.
//
// Stack traces are captured at panic time and logged once; the goroutine
// terminates normally afterwards. The caller is responsible for any
// channel-send or state-update that needs to happen even on panic — the
// recovery doesn't rewind side effects.
func safeGo(label string, fn func()) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[mesh.safeGo] %s panic: %v\n%s", label, r, debug.Stack())
			}
		}()
		fn()
	}()
}

// safeGoCh is the channel-result variant of safeGo. On panic it pushes the
// supplied onPanic value to ch so the consumer doesn't deadlock waiting
// for a result that will never arrive — the helper closes the fan-out
// completion path even when the work explodes.
func safeGoCh[T any](label string, ch chan<- T, onPanic T, fn func()) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				log.Printf("[mesh.safeGo] %s panic: %v\n%s", label, r, debug.Stack())
				// Best-effort recover the consumer — non-blocking send so
				// a missing receiver doesn't hang the goroutine.
				select {
				case ch <- onPanic:
				default:
				}
			}
		}()
		fn()
	}()
}

// mergeCtxWithTimeout returns a context whose Done fires when EITHER
//
//   - the parent's Done channel closes (caller cancelled, deadline hit), OR
//   - the supplied timeout elapses,
//
// whichever happens first.
//
// Use this when the caller needs a hard time budget that MAY exceed the
// parent's deadline (e.g. extending a slow cross-region forward hop past
// the caller's per-RPC deadline) but MUST still cancel as soon as the
// parent cancels (so sibling-probe first-win cancellation propagates
// correctly).
//
// context.WithTimeout(parent, timeout) is the wrong choice here because
// it takes MIN(parent_deadline, now+timeout) — when parent's deadline is
// shorter than timeout, parent's deadline wins and the call gets less
// time than the caller intended. mergeCtxWithTimeout keeps the full
// timeout budget AND honours parent cancellation.
//
// Always defer the returned cancel — it cleans up the watcher goroutine
// even on the happy path.
func mergeCtxWithTimeout(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	timeoutCtx, timeoutCancel := context.WithTimeout(context.Background(), timeout)
	mergedCtx, mergedCancel := context.WithCancel(timeoutCtx)
	go func() {
		select {
		case <-parent.Done():
			mergedCancel()
		case <-mergedCtx.Done():
			// either timeout fired or caller invoked the returned cancel
		}
	}()
	return mergedCtx, func() {
		mergedCancel()
		timeoutCancel()
	}
}
