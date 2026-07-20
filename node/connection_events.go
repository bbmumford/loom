/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"sync"
	"time"
)

// ConnectionEventType categorizes connection events.
type ConnectionEventType string

const (
	EventConnected    ConnectionEventType = "connected"
	EventDisconnected ConnectionEventType = "disconnected"
	EventUpgraded     ConnectionEventType = "upgraded"
	EventDowngraded   ConnectionEventType = "downgraded"
	EventDrainStarted ConnectionEventType = "drain_started"
	EventDrainComplete ConnectionEventType = "drain_complete"
)

// ConnectionEventLog is an append-only event log with time-based retention
// and a size cap. Events are stored chronologically (append-only guarantees
// sorted order). Thread-safe for concurrent reads and writes.
type ConnectionEventLog struct {
	mu      sync.RWMutex
	events  []ConnectionEvent
	maxAge  time.Duration // retain events for this duration (default: 4h)
	maxSize int           // cap total events (default: 2000)
}

// NewConnectionEventLog creates an event log with default retention policy:
// 4 hours max age, 2000 events max size.
func NewConnectionEventLog() *ConnectionEventLog {
	return &ConnectionEventLog{
		maxAge:  4 * time.Hour,
		maxSize: 2000,
	}
}

// Append adds an event to the log. Automatically prunes expired and excess
// events after insertion.
func (l *ConnectionEventLog) Append(event ConnectionEvent) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}
	// MESH-C09: keep events sorted by Timestamp. EventsSince's front-scan cutoff
	// and prune both assume sorted order, but callers pass their own timestamps
	// (e.g. "connected" is stamped at accept-start yet appended much later than
	// newer "disconnected"/"drained" events), so a bare append could leave the
	// slice out of order — dropping still-in-window events and pruning the wrong
	// ones. Insert at the right position via a backward scan; the log is
	// near-sorted so only the occasional late arrival moves more than one slot.
	i := len(l.events)
	for i > 0 && l.events[i-1].Timestamp.After(event.Timestamp) {
		i--
	}
	if i == len(l.events) {
		l.events = append(l.events, event)
	} else {
		l.events = append(l.events, ConnectionEvent{})
		copy(l.events[i+1:], l.events[i:])
		l.events[i] = event
	}
	l.prune()
}

// Events returns all events within the given time window (i.e., events
// whose timestamp is after time.Now().Add(-since)).
func (l *ConnectionEventLog) Events(since time.Duration) []ConnectionEvent {
	return l.EventsSince(time.Now().Add(-since))
}

// EventsSince returns all events after the given time.
func (l *ConnectionEventLog) EventsSince(t time.Time) []ConnectionEvent {
	l.mu.RLock()
	defer l.mu.RUnlock()

	// Linear scan from the front; events are sorted by time (append-only).
	start := 0
	for start < len(l.events) && l.events[start].Timestamp.Before(t) {
		start++
	}
	if start >= len(l.events) {
		return nil
	}

	result := make([]ConnectionEvent, len(l.events)-start)
	copy(result, l.events[start:])
	return result
}

// Recent returns the last n events.
func (l *ConnectionEventLog) Recent(n int) []ConnectionEvent {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if n > len(l.events) {
		n = len(l.events)
	}
	if n == 0 {
		return nil
	}

	result := make([]ConnectionEvent, n)
	copy(result, l.events[len(l.events)-n:])
	return result
}

// Count returns the total number of events currently in the log.
func (l *ConnectionEventLog) Count() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.events)
}

// Summary returns event counts grouped by reason for events after the given time.
func (l *ConnectionEventLog) Summary(since time.Time) map[string]int {
	events := l.EventsSince(since)
	counts := make(map[string]int)
	for _, e := range events {
		counts[e.Reason]++
	}
	return counts
}

// prune removes events older than maxAge and trims to maxSize.
// Must be called with l.mu held (write lock).
func (l *ConnectionEventLog) prune() {
	// Age-based eviction
	cutoff := time.Now().Add(-l.maxAge)
	start := 0
	for start < len(l.events) && l.events[start].Timestamp.Before(cutoff) {
		start++
	}
	if start > 0 {
		l.events = l.events[start:]
	}

	// Size-based eviction
	if len(l.events) > l.maxSize {
		l.events = l.events[len(l.events)-l.maxSize:]
	}
}
