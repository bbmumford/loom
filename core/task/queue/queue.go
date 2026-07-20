/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package queue

import (
	"container/heap"
	"math"
	"sync"
	"time"

	task "github.com/bbmumford/loom/core/task"
)

// ScoreWeights defines the weighting applied to task scheduling heuristics.
type ScoreWeights struct {
	Priority float64
	Deadline float64
	Age      float64
}

// QueuePolicy configures how queued tasks are prioritised.
type QueuePolicy struct {
	Weights         ScoreWeights
	DefaultLatency  time.Duration
	PriorityCeiling uint8
	Now             func() time.Time
}

// DefaultQueuePolicy returns a balanced queue policy matching the design doc heuristics.
func DefaultQueuePolicy() QueuePolicy {
	return QueuePolicy{
		Weights: ScoreWeights{
			Priority: 2.0,
			Deadline: 1.2,
			Age:      0.8,
		},
		DefaultLatency:  2 * time.Second,
		PriorityCeiling: 15,
		Now:             time.Now,
	}
}

// score computes a sortable score for a task using EDF + priority weighting.
func (p QueuePolicy) score(t *task.Task) float64 {
	now := time.Now
	if p.Now != nil {
		now = p.Now
	}
	latency := t.SLO.Latency
	if latency <= 0 {
		latency = p.DefaultLatency
	}
	deadline := t.CreatedAt.Add(latency)
	timeToDeadline := deadline.Sub(now())
	if timeToDeadline < 0 {
		timeToDeadline = 0
	}
	priority := float64(t.SLO.Priority)
	if t.SLO.Priority > p.PriorityCeiling {
		priority = float64(p.PriorityCeiling)
	}
	age := now().Sub(t.CreatedAt).Seconds()
	if age < 0 {
		age = 0
	}
	deadlineScore := 0.0
	if timeToDeadline > 0 {
		deadlineScore = 1 / timeToDeadline.Seconds()
	}
	return p.Weights.Priority*priority + p.Weights.Deadline*deadlineScore + p.Weights.Age*age
}

type queueItem struct {
	task     *task.Task
	score    float64
	enqueued time.Time
	index    int
}

type priorityQueue []*queueItem

func (pq priorityQueue) Len() int { return len(pq) }

func (pq priorityQueue) Less(i, j int) bool {
	// Higher score gets higher priority. If equal, earlier enqueue wins.
	if math.Abs(pq[i].score-pq[j].score) < 1e-6 {
		return pq[i].enqueued.Before(pq[j].enqueued)
	}
	return pq[i].score > pq[j].score
}

func (pq priorityQueue) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *priorityQueue) Push(x interface{}) {
	item := x.(*queueItem)
	item.index = len(*pq)
	*pq = append(*pq, item)
}

func (pq *priorityQueue) Pop() interface{} {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	*pq = old[0 : n-1]
	return item
}

// DefaultMaxQueueDepth caps the number of tasks a TaskQueue holds (MESH-H05) so
// producers plus timer-driven retries can't grow the heap without bound.
const DefaultMaxQueueDepth = 10000

// TaskQueue is a concurrency-safe priority queue for tasks.
type TaskQueue struct {
	policy   QueuePolicy
	mu       sync.Mutex
	pq       priorityQueue
	maxDepth int
	dropped  uint64 // tasks rejected because the queue was full
}

// NewTaskQueue constructs an empty queue using the provided policy.
func NewTaskQueue(policy QueuePolicy) *TaskQueue {
	return &TaskQueue{policy: policy, pq: make(priorityQueue, 0), maxDepth: DefaultMaxQueueDepth}
}

// Enqueue inserts a task and returns its computed score. MESH-H05: when the
// queue is at capacity the task is rejected (dropped) rather than growing the
// heap unbounded — losing a task under sustained overload is preferable to OOM.
func (q *TaskQueue) Enqueue(task *task.Task) float64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	score := q.policy.score(task)
	max := q.maxDepth
	if max <= 0 {
		max = DefaultMaxQueueDepth
	}
	if q.pq.Len() >= max {
		q.dropped++
		return score // rejected — queue full
	}
	item := &queueItem{task: task, score: score, enqueued: time.Now()}
	heap.Push(&q.pq, item)
	return score
}

// Dropped returns the number of tasks rejected because the queue was full.
func (q *TaskQueue) Dropped() uint64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.dropped
}

// Pop removes the highest priority task or returns nil if empty.
func (q *TaskQueue) Pop() *task.Task {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.pq.Len() == 0 {
		return nil
	}
	item := heap.Pop(&q.pq).(*queueItem)
	return item.task
}

// Len returns the number of tasks waiting in the queue.
func (q *TaskQueue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.pq.Len()
}
