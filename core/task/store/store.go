/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
 */

// Package store holds the ports through which a job kind makes the task
// exchange durable: the Store that owns every admission, award, lease version
// and terminal outcome, the LeaseSigner of an award's lease, and the
// ReceiptVerifier of an executor's completion. gateway.Gateway drives each
// transition through them and rehydrates from Load at start. MemoryStore is
// the in-process store the gateway uses when none is given.
//
// The exchange knows no money: a job kind that holds funds for a task does so
// inside its own Store's transitions. Each transition below states the times
// the gateway hands it, so such a hold can follow the task exactly: Award and
// Renew carry the lease's ExpiresAt and its TakeoverUntil (the hold must last
// to the latter), KeepWaiting is called for every waiting task on each sweep
// (the hold of a waiting task is renewed there, or the wait ended), and
// Finish with OutcomeCompleted records the completion (the hold must stop
// expiring in that same transaction, so only a settle or a typed release ends
// it).
package store

import (
	"context"
	"errors"
	"time"

	"github.com/bbmumford/loom/core/task"
	"github.com/bbmumford/loom/ports"
)

// State is where an admitted task stands in the store. A leased task whose
// lease has expired is orphaned until its lease's TakeoverUntil: Load answers
// it as StateLeased and the gateway offers it again for a takeover.
type State string

const (
	StateWaiting State = "waiting"
	StateLeased  State = "leased"
	StateEnded   State = "ended"
)

// Outcome is a task's terminal outcome.
type Outcome string

const (
	// OutcomeCompleted: an OK result whose receipt the verifier admitted.
	OutcomeCompleted Outcome = "completed"
	// OutcomeFailed: the executor reported failure with no attempt left.
	OutcomeFailed Outcome = "failed"
	// OutcomeRejected: the executor refused the task's authority.
	OutcomeRejected Outcome = "rejected"
	// OutcomeReaped: the holder stopped renewing and no takeover won the task
	// before its lease's TakeoverUntil.
	OutcomeReaped Outcome = "reaped"
	// OutcomeAbandoned: the task waited past the gateway's MaxWait, or the
	// store ended its wait (KeepWaiting).
	OutcomeAbandoned Outcome = "abandoned"
	// OutcomeCancelled: the gateway withdrew the task before any outcome.
	OutcomeCancelled Outcome = "cancelled"
)

// The causes the gateway names on the outcomes it decides itself. A store's
// KeepWaiting names its own.
const (
	CauseTakeoverWindowElapsed = "takeover_window_elapsed"
	CauseMaxWaitElapsed        = "max_wait_elapsed"
	CauseAttemptsExhausted     = "attempts_exhausted"
	CauseExecutorRejected      = "executor_rejected"
	CauseAttemptFailed         = "attempt_failed"
	CauseLeaseExpired          = "lease_expired"
	CauseReceiptRefused        = "receipt_refused"
	CauseQueueFull             = "queue_full"
)

var (
	// ErrAdmitted refuses an admission of a task id the store holds unended.
	ErrAdmitted = errors.New("task store: task already admitted")
	// ErrUnknownTask answers a transition naming a task the store never admitted.
	ErrUnknownTask = errors.New("task store: unknown task")
	// ErrEnded answers a transition on a task whose outcome is recorded.
	ErrEnded = errors.New("task store: task already ended")
	// ErrStale answers a transition whose lease identity or state is not the
	// task's current one: the compare-and-set lost and nothing was written.
	ErrStale = errors.New("task store: stale lease or state")
)

// Record is one admitted task as the store holds it. Principal is the product
// authority the admission was bound to (nil for an unbound Enqueue); a store
// that persists across restarts rebuilds it from its own records. Lease is the
// current version while the task is leased.
type Record struct {
	Task       *task.Task
	Principal  ports.ExecutionPrincipal
	AdmittedAt time.Time
	State      State
	Lease      *task.Lease
}

// Award asks the store to lease a task to NodeID. Supersedes is nil for the
// first award of a waiting task and names the lapsed lease a takeover replaces,
// which must be the task's current version with At after its ExpiresAt and not
// after its TakeoverUntil.
type Award struct {
	TaskID        string
	NodeID        string
	Attempt       int
	At            time.Time
	ExpiresAt     time.Time
	TakeoverUntil time.Time
	Supersedes    *task.LeaseIdentity
}

// Renewal asks the store to extend the lease version Lease names, which must
// be the task's current one and unexpired at At.
type Renewal struct {
	Lease         task.LeaseIdentity
	At            time.Time
	ExpiresAt     time.Time
	TakeoverUntil time.Time
}

// Release ends the lease version Lease names without ending the task, which
// waits for its next award (a failed attempt the gateway retries, or a lease
// that expired with no takeover window).
type Release struct {
	TaskID string
	Lease  *task.LeaseIdentity
	At     time.Time
	Cause  string
}

// Terminal records a task's outcome. Lease, when set, must be the task's
// current version; nil ends a task that holds no lease. Receipt is the
// admitted receipt of an OutcomeCompleted.
type Terminal struct {
	TaskID  string
	Lease   *task.LeaseIdentity
	Outcome Outcome
	Cause   string
	Receipt []byte
	At      time.Time
}

// WaitVerdict is KeepWaiting's answer: Keep, or end the wait with Cause.
type WaitVerdict struct {
	Keep  bool
	Cause string
}

// Store owns the task exchange's durable state. Each method is one
// transaction the store owns: it commits entirely or writes nothing, and a
// lost compare-and-set answers ErrStale. The gateway calls it under its own
// lock, so a call sees the gateway state it was decided from.
type Store interface {
	// Admit records a waiting task.
	Admit(ctx context.Context, rec Record) error
	// Award commits the first lease version of an award, or of a takeover,
	// and has signer sign its canonical bytes inside the same transaction; a
	// signing failure writes nothing.
	Award(ctx context.Context, award Award, signer LeaseSigner) (*task.Lease, error)
	// Renew commits the next version of a current lease, signed as Award signs.
	Renew(ctx context.Context, renewal Renewal, signer LeaseSigner) (*task.Lease, error)
	// Release ends a lease and returns the task to waiting.
	Release(ctx context.Context, release Release) error
	// KeepWaiting is the sweep's call for a waiting task: keep it waiting
	// (renewing whatever the job kind holds for it) or end the wait.
	KeepWaiting(ctx context.Context, taskID string, at time.Time) (WaitVerdict, error)
	// Finish records the terminal outcome.
	Finish(ctx context.Context, terminal Terminal) error
	// Load answers every task not ended, for the gateway's start.
	Load(ctx context.Context) ([]Record, error)
}

// LeaseSigner signs a lease version's canonical bytes (task.LeaseTerms.Canonical)
// and names the key it signed with.
type LeaseSigner interface {
	SignLease(ctx context.Context, canonical []byte) (signature []byte, keyID string, err error)
}

// ReceiptVerifier admits an executor's signed completion against the lease
// version the result's fence names.
type ReceiptVerifier interface {
	VerifyReceipt(ctx context.Context, lease task.Lease, result task.Result) error
}
