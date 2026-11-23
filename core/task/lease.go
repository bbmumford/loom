/*
 * Copyright (c) 2026 Brandon Miller-Mumford.
 * Licensed under the MIT License; see LICENSE.
 */
package task

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// LeaseIdentity names one signed version of a task's lease: a lease id the
// store mints per task, a generation that rises with every version the store
// commits for that lease (the award, each renewal, a takeover), and the hex
// SHA-256 of the version's canonical bytes. It is the fence an assignment,
// renewal or result carries, so a caller naming a superseded version loses the
// store's compare-and-set however it obtained the tuple.
type LeaseIdentity struct {
	LeaseID    string `json:"leaseId"`
	Generation uint64 `json:"generation"`
	Digest     string `json:"digest"`
}

// Valid reports whether every part of the identity is set.
func (i LeaseIdentity) Valid() bool {
	return i.LeaseID != "" && i.Generation > 0 && len(i.Digest) == 2*sha256.Size
}

// Fence is the identity as the one string Assignment.Fence, Renewal.Fence and
// Result.Fence carry: lease id, generation and digest joined by '/'. ParseFence
// reads it back.
func (i LeaseIdentity) Fence() string {
	return i.LeaseID + "/" + strconv.FormatUint(i.Generation, 10) + "/" + i.Digest
}

// ParseFence reads a fence written by LeaseIdentity.Fence and refuses any
// other shape.
func ParseFence(fence string) (LeaseIdentity, error) {
	parts := strings.Split(fence, "/")
	if len(parts) != 3 {
		return LeaseIdentity{}, fmt.Errorf("task: fence %q is not lease/generation/digest", fence)
	}
	gen, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return LeaseIdentity{}, fmt.Errorf("task: fence generation %q: %w", parts[1], err)
	}
	id := LeaseIdentity{LeaseID: parts[0], Generation: gen, Digest: parts[2]}
	if !id.Valid() {
		return LeaseIdentity{}, fmt.Errorf("task: fence %q is incomplete", fence)
	}
	return id, nil
}

// LeaseTerms are the facts one lease version binds, in the order its canonical
// bytes encode them. ExpiresAt is when the holder must have renewed;
// TakeoverUntil, at or after it, is when a task whose holder stopped renewing
// is ended if no takeover won it first.
type LeaseTerms struct {
	TaskID        string    `json:"taskId"`
	Partition     string    `json:"partition"`
	NodeID        string    `json:"nodeId"`
	LeaseID       string    `json:"leaseId"`
	Generation    uint64    `json:"generation"`
	Attempt       int       `json:"attempt"`
	IssuedAt      time.Time `json:"issuedAt"`
	ExpiresAt     time.Time `json:"expiresAt"`
	TakeoverUntil time.Time `json:"takeoverUntil"`
}

// canonicalTerms is LeaseTerms with its times as Unix milliseconds, so the
// bytes do not depend on a time's location or monotonic reading.
type canonicalTerms struct {
	TaskID        string `json:"taskId"`
	Partition     string `json:"partition"`
	NodeID        string `json:"nodeId"`
	LeaseID       string `json:"leaseId"`
	Generation    uint64 `json:"generation"`
	Attempt       int    `json:"attempt"`
	IssuedAt      int64  `json:"issuedAtUnixMillis"`
	ExpiresAt     int64  `json:"expiresAtUnixMillis"`
	TakeoverUntil int64  `json:"takeoverUntilUnixMillis"`
}

// Canonical returns the bytes a lease signer signs and LeaseIdentity.Digest
// hashes: the terms as JSON with fixed field order and millisecond times.
func (t LeaseTerms) Canonical() ([]byte, error) {
	if t.TaskID == "" || t.NodeID == "" || t.LeaseID == "" || t.Generation == 0 {
		return nil, errors.New("task: lease terms need a task, a node, a lease id and a generation")
	}
	if t.ExpiresAt.Before(t.IssuedAt) || t.TakeoverUntil.Before(t.ExpiresAt) {
		return nil, errors.New("task: lease terms must expire after issue and end their takeover window after expiry")
	}
	return json.Marshal(canonicalTerms{
		TaskID: t.TaskID, Partition: t.Partition, NodeID: t.NodeID, LeaseID: t.LeaseID,
		Generation: t.Generation, Attempt: t.Attempt, IssuedAt: t.IssuedAt.UnixMilli(),
		ExpiresAt: t.ExpiresAt.UnixMilli(), TakeoverUntil: t.TakeoverUntil.UnixMilli(),
	})
}

// LeaseDigest is the hex SHA-256 of canonical lease bytes, the Digest of the
// identity they name.
func LeaseDigest(canonical []byte) string {
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:])
}

// Lease is one signed lease version as the store committed it and an
// assignment or renewal answer carries it: its terms, their canonical bytes,
// and the signature over those bytes with the signer's key id. An executor
// checks the signature before running anything under it.
type Lease struct {
	Terms       LeaseTerms `json:"terms"`
	Canonical   []byte     `json:"canonical"`
	Signature   []byte     `json:"signature,omitempty"`
	SignerKeyID string     `json:"signerKeyId,omitempty"`
}

// Identity is the version's fence tuple.
func (l Lease) Identity() LeaseIdentity {
	return LeaseIdentity{LeaseID: l.Terms.LeaseID, Generation: l.Terms.Generation, Digest: LeaseDigest(l.Canonical)}
}

// Clone copies the lease's byte slices.
func (l *Lease) Clone() *Lease {
	if l == nil {
		return nil
	}
	c := *l
	c.Canonical = append([]byte(nil), l.Canonical...)
	if l.Signature != nil {
		c.Signature = append([]byte(nil), l.Signature...)
	}
	return &c
}

// Renewal is a lease holder's request to keep its lease: the fence of the
// version it holds. The gateway renews it through its store to a fresh
// expiry and takeover window and answers the new version on the assignment
// channel.
type Renewal struct {
	TaskID string    `json:"taskId"`
	NodeID string    `json:"nodeId"`
	Fence  string    `json:"fence"`
	At     time.Time `json:"at"`
}
