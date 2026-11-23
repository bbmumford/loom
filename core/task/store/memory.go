/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
 */

package store

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/bbmumford/loom/core/task"
	"github.com/google/uuid"
)

// MemoryStore is a Store held in process memory: what the gateway uses when
// it is given none, and what a test of the exchange runs over. It keeps one
// lease id per task, raising the generation with every version, and never
// forgets an ended task, so a replayed admission of its id is refused. A nil
// signer leaves a version unsigned.
type MemoryStore struct {
	mu    sync.Mutex
	tasks map[string]*memoryRecord
	order []string
}

type memoryRecord struct {
	rec     Record
	leaseID string
	gen     uint64
	outcome Outcome
	cause   string
	receipt []byte
}

// NewMemoryStore returns an empty MemoryStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{tasks: map[string]*memoryRecord{}}
}

var _ Store = (*MemoryStore)(nil)

// Admit implements Store.
func (m *MemoryStore) Admit(_ context.Context, rec Record) error {
	if rec.Task == nil || rec.Task.ID == "" {
		return errors.New("task store: admit requires a task with an id")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.tasks[rec.Task.ID]; ok {
		return ErrAdmitted
	}
	rec.Task = rec.Task.Clone()
	rec.State = StateWaiting
	rec.Lease = nil
	m.tasks[rec.Task.ID] = &memoryRecord{rec: rec}
	m.order = append(m.order, rec.Task.ID)
	return nil
}

func (m *MemoryStore) liveLocked(taskID string) (*memoryRecord, error) {
	r, ok := m.tasks[taskID]
	if !ok {
		return nil, ErrUnknownTask
	}
	if r.rec.State == StateEnded {
		return nil, ErrEnded
	}
	return r, nil
}

func currentIs(r *memoryRecord, id *task.LeaseIdentity) bool {
	if r.rec.Lease == nil || id == nil {
		return r.rec.Lease == nil && id == nil
	}
	return r.rec.Lease.Identity() == *id
}

// sign builds and signs the next version of r's lease without changing r.
func (m *MemoryStore) sign(ctx context.Context, r *memoryRecord, leaseID, nodeID string, attempt int,
	at, expires, takeoverUntil time.Time, signer LeaseSigner) (*task.Lease, error) {
	terms := task.LeaseTerms{
		TaskID: r.rec.Task.ID, Partition: r.rec.Task.Partition, NodeID: nodeID, LeaseID: leaseID,
		Generation: r.gen + 1, Attempt: attempt, IssuedAt: at, ExpiresAt: expires, TakeoverUntil: takeoverUntil,
	}
	canonical, err := terms.Canonical()
	if err != nil {
		return nil, err
	}
	l := &task.Lease{Terms: terms, Canonical: canonical}
	if signer != nil {
		sig, keyID, err := signer.SignLease(ctx, canonical)
		if err != nil {
			return nil, fmt.Errorf("task store: sign lease: %w", err)
		}
		l.Signature, l.SignerKeyID = sig, keyID
	}
	return l, nil
}

// Award implements Store.
func (m *MemoryStore) Award(ctx context.Context, a Award, signer LeaseSigner) (*task.Lease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, err := m.liveLocked(a.TaskID)
	if err != nil {
		return nil, err
	}
	if !currentIs(r, a.Supersedes) {
		return nil, ErrStale
	}
	if a.Supersedes != nil {
		cur := r.rec.Lease.Terms
		if !a.At.After(cur.ExpiresAt) || a.At.After(cur.TakeoverUntil) {
			return nil, ErrStale
		}
	}
	leaseID := r.leaseID
	if leaseID == "" {
		leaseID = uuid.NewString()
	}
	l, err := m.sign(ctx, r, leaseID, a.NodeID, a.Attempt, a.At, a.ExpiresAt, a.TakeoverUntil, signer)
	if err != nil {
		return nil, err
	}
	r.leaseID, r.gen = leaseID, l.Terms.Generation
	r.rec.State, r.rec.Lease = StateLeased, l
	return l.Clone(), nil
}

// Renew implements Store.
func (m *MemoryStore) Renew(ctx context.Context, rn Renewal, signer LeaseSigner) (*task.Lease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.byLeaseLocked(rn.Lease.LeaseID)
	if !ok {
		return nil, ErrStale
	}
	if r.rec.State == StateEnded {
		return nil, ErrEnded
	}
	if !currentIs(r, &rn.Lease) || rn.At.After(r.rec.Lease.Terms.ExpiresAt) {
		return nil, ErrStale
	}
	cur := r.rec.Lease.Terms
	l, err := m.sign(ctx, r, cur.LeaseID, cur.NodeID, cur.Attempt, rn.At, rn.ExpiresAt, rn.TakeoverUntil, signer)
	if err != nil {
		return nil, err
	}
	r.gen = l.Terms.Generation
	r.rec.Lease = l
	return l.Clone(), nil
}

func (m *MemoryStore) byLeaseLocked(leaseID string) (*memoryRecord, bool) {
	if leaseID == "" {
		return nil, false
	}
	for _, r := range m.tasks {
		if r.leaseID == leaseID {
			return r, true
		}
	}
	return nil, false
}

// Release implements Store.
func (m *MemoryStore) Release(_ context.Context, rel Release) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, err := m.liveLocked(rel.TaskID)
	if err != nil {
		return err
	}
	if !currentIs(r, rel.Lease) {
		return ErrStale
	}
	r.rec.State, r.rec.Lease = StateWaiting, nil
	return nil
}

// KeepWaiting implements Store: a memory store holds nothing for a waiting
// task, so every live waiting task is kept.
func (m *MemoryStore) KeepWaiting(_ context.Context, taskID string, _ time.Time) (WaitVerdict, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, err := m.liveLocked(taskID)
	if err != nil {
		return WaitVerdict{}, err
	}
	if r.rec.State != StateWaiting {
		return WaitVerdict{}, ErrStale
	}
	return WaitVerdict{Keep: true}, nil
}

// Finish implements Store.
func (m *MemoryStore) Finish(_ context.Context, t Terminal) error {
	if t.Outcome == "" {
		return errors.New("task store: finish requires an outcome")
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	r, err := m.liveLocked(t.TaskID)
	if err != nil {
		return err
	}
	if !currentIs(r, t.Lease) {
		return ErrStale
	}
	r.rec.State = StateEnded
	r.outcome, r.cause = t.Outcome, t.Cause
	r.receipt = append([]byte(nil), t.Receipt...)
	return nil
}

// Load implements Store.
func (m *MemoryStore) Load(_ context.Context) ([]Record, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []Record
	for _, id := range m.order {
		r := m.tasks[id]
		if r.rec.State == StateEnded {
			continue
		}
		rec := r.rec
		rec.Task = r.rec.Task.Clone()
		rec.Lease = r.rec.Lease.Clone()
		out = append(out, rec)
	}
	return out, nil
}

// Outcome answers a task's recorded outcome and cause, and whether it ended.
func (m *MemoryStore) Outcome(taskID string) (Outcome, string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.tasks[taskID]
	if !ok || r.rec.State != StateEnded {
		return "", "", false
	}
	return r.outcome, r.cause, true
}

// Current answers a task's state and its current lease version.
func (m *MemoryStore) Current(taskID string) (State, *task.Lease, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.tasks[taskID]
	if !ok {
		return "", nil, false
	}
	return r.rec.State, r.rec.Lease.Clone(), true
}
