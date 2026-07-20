/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package task

import (
	"encoding/json"
	"time"

	"github.com/bbmumford/loom/core/task/artifacts"
)

// TaskStatus represents lifecycle stages for a task.
type TaskStatus string

const (
	TaskStatusQueued    TaskStatus = "queued"
	TaskStatusOffered   TaskStatus = "offered"
	TaskStatusAssigned  TaskStatus = "assigned"
	TaskStatusExecuting TaskStatus = "executing"
	TaskStatusCompleted TaskStatus = "completed"
	TaskStatusFailed    TaskStatus = "failed"
	TaskStatusCanceled  TaskStatus = "canceled"
)

// TaskSLO captures service-level objectives for latency, priority and reliability.
type TaskSLO struct {
	Latency     time.Duration `json:"latency"`
	Priority    uint8         `json:"priority"`
	Reliability float64       `json:"reliability"`
}

// Task encapsulates a unit of work that can be executed by a mesh executor.
type Task struct {
	ID          string               `json:"id"`
	TenantID    string               `json:"tenantId"`
	Role        string               `json:"role"`
	Type        string               `json:"type"`
	Tags        []string             `json:"tags,omitempty"`
	Payload     json.RawMessage      `json:"payload"`
	Artifacts   []artifacts.Artifact `json:"artifacts,omitempty"`
	Hint        []byte               `json:"hint,omitempty"`
	SLO         TaskSLO              `json:"slo"`
	Idempotency string               `json:"idempotency"`
	Attempt     int                  `json:"attempt"`
	Status      TaskStatus           `json:"status"`
	CreatedAt   time.Time            `json:"createdAt"`
	EnqueuedAt  time.Time            `json:"enqueuedAt"`
	OfferedAt   time.Time            `json:"offeredAt"`
}

// CloneForRetry duplicates the task for a retry attempt, bumping the attempt counter.
func (t *Task) CloneForRetry() *Task {
	if t == nil {
		return nil
	}
	clone := *t
	clone.Attempt = t.Attempt + 1
	clone.Status = TaskStatusQueued
	clone.EnqueuedAt = time.Time{}
	clone.OfferedAt = time.Time{}
	if t.Tags != nil {
		clone.Tags = append([]string(nil), t.Tags...)
	}
	if t.Payload != nil {
		clone.Payload = append([]byte(nil), t.Payload...)
	}
	if t.Hint != nil {
		clone.Hint = append([]byte(nil), t.Hint...)
	}
	if len(t.Artifacts) > 0 {
		dup := make([]artifacts.Artifact, len(t.Artifacts))
		for i, a := range t.Artifacts {
			dup[i] = a.Copy()
		}
		clone.Artifacts = dup
	}
	return &clone
}

// TaskOffer represents an offer broadcast from a gateway to potential executors.
type TaskOffer struct {
	Task       *Task     `json:"task"`
	IssuedAt   time.Time `json:"issuedAt"`
	ExpiresAt  time.Time `json:"expiresAt"`
	OfferToken string    `json:"offerToken"`
}

// Bid encapsulates an executor's interest in handling a task.
type Bid struct {
	TaskID        string            `json:"taskId"`
	NodeID        string            `json:"nodeId"`
	Score         float64           `json:"score"`
	ETA           time.Duration     `json:"eta"`
	CapacityScore float64           `json:"capacityScore"`
	RoleVersion   string            `json:"roleVersion"`
	Metadata      map[string]string `json:"metadata,omitempty"`
	ReceivedAt    time.Time         `json:"receivedAt"`
}

// ResultStatus indicates the outcome of a task execution.
type ResultStatus string

const (
	ResultStatusOK       ResultStatus = "OK"
	ResultStatusError    ResultStatus = "ERR"
	ResultStatusRejected ResultStatus = "REJECTED"
)

// Assignment contains the lease and fencing tokens required to execute a task.
type Assignment struct {
	Task          *Task         `json:"task"`
	TaskID        string        `json:"taskId"`
	Primary       string        `json:"primary"`
	Standby       []string      `json:"standby,omitempty"`
	Fence         string        `json:"fence"`
	LeaseDuration time.Duration `json:"leaseDuration"`
	Keys          []byte        `json:"keys,omitempty"`
	IssuedAt      time.Time     `json:"issuedAt"`
}

// Progress represents executor progress updates while a task is running.
type Progress struct {
	TaskID  string    `json:"taskId"`
	NodeID  string    `json:"nodeId"`
	Fence   string    `json:"fence"`
	Pct     uint8     `json:"pct"`
	Message string    `json:"message,omitempty"`
	At      time.Time `json:"at"`
}

// Result captures the terminal outcome of a task execution.
type Result struct {
	TaskID    string               `json:"taskId"`
	NodeID    string               `json:"nodeId"`
	Fence     string               `json:"fence"`
	Status    ResultStatus         `json:"status"`
	Output    []byte               `json:"output,omitempty"`
	Error     string               `json:"error,omitempty"`
	Artifacts []artifacts.Artifact `json:"artifacts,omitempty"`
	At        time.Time            `json:"at"`
}

// ResultReport is the payload sent from executors to gateways.
type ResultReport struct {
	Result Result         `json:"result"`
	Stats  map[string]any `json:"stats,omitempty"`
}
