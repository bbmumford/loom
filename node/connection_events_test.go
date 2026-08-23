/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"testing"
	"time"
)

// COVERAGE of the connection event log, all 0.0%: Append, Events,
// EventsSince, Count and prune.
//
// Censused first. This
// surface IS driven: Append from connection_scaling.go:331, EventsSince from
// peer_reputation.go:118 (it feeds reputation scoring), and Events from
// ORBTR io/endpoints/help.orbtr.io/monitoring_api.go:292 — a live consumer on
// the deployment surface.
//
// ⚠ NOT COVERED HERE, DELIBERATELY: Recent and Summary have ZERO non-test
// callers across all three roots. These are not tests that
// make an unwired surface look supported. Reported separately.

// eventAt builds an event with an explicit timestamp. Reason is the field
// consumers group by, so it doubles as the identity in assertions.
func eventAt(reason string, ts time.Time) ConnectionEvent {
	return ConnectionEvent{PeerNodeID: testNodeIDB, Reason: reason, Timestamp: ts}
}

func reasons(events []ConnectionEvent) []string {
	out := make([]string, len(events))
	for i, e := range events {
		out[i] = e.Reason
	}
	return out
}

// 🔴 MESH-C09, REPRODUCED.
//
// Callers stamp their own timestamps, and "connected" is stamped at
// accept-start yet appended much later than newer "disconnected"/"drained"
// events. A bare append would therefore leave the slice out of order — and
// BOTH EventsSince's front-scan cutoff AND prune assume sorted order.
//
// The failure is not cosmetic: with an out-of-order tail, EventsSince's scan
// stops at the first event that is not before the cutoff and returns
// everything after it — including events OUTSIDE the requested window.
func TestAppendKeepsTheLogSortedWhenALateEventArrivesOutOfOrder(t *testing.T) {
	base := time.Now().Add(-time.Hour)
	l := NewConnectionEventLog()

	l.Append(eventAt("newer", base.Add(10*time.Minute)))
	l.Append(eventAt("older", base.Add(5*time.Minute))) // the late arrival

	got := reasons(l.EventsSince(base))
	want := []string{"older", "newer"}
	if len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("log order = %v, want %v — a late-arriving event was appended "+
			"at the tail instead of inserted by timestamp", got, want)
	}

	// THE CONSEQUENCE, asserted separately from the ordering itself: a window
	// that excludes "older" must not return it.
	inWindow := reasons(l.EventsSince(base.Add(7 * time.Minute)))
	if len(inWindow) != 1 || inWindow[0] != "newer" {
		t.Fatalf("EventsSince(+7m) = %v, want [newer] — the front-scan cutoff "+
			"hit an out-of-order element and returned events from OUTSIDE the "+
			"requested window, which is what MESH-C09 fixed", inWindow)
	}
}

func TestAppendStampsAnUnsetTimestampAndCounts(t *testing.T) {
	l := NewConnectionEventLog()
	before := time.Now()

	l.Append(ConnectionEvent{PeerNodeID: testNodeIDB, Reason: "connected"}) // zero Timestamp

	if got := l.Count(); got != 1 {
		t.Fatalf("Count = %d after one Append, want 1", got)
	}
	got := l.EventsSince(before.Add(-time.Second))
	if len(got) != 1 {
		t.Fatalf("EventsSince returned %d events, want 1", len(got))
	}
	if got[0].Timestamp.IsZero() {
		t.Fatal("an event appended with no timestamp was stored with a ZERO " +
			"timestamp — it sorts before everything and prune will evict it on " +
			"the very next append")
	}
	if got[0].Timestamp.Before(before) {
		t.Fatalf("the stamped timestamp %v predates the Append call %v",
			got[0].Timestamp, before)
	}
}

// The window boundary is inclusive: the scan advances while
// `events[start].Timestamp.Before(t)`, so an event exactly AT t is returned.
// Consumers poll with the previous poll's timestamp, so an exclusive boundary
// here would silently drop an event on every poll.
func TestEventsSinceIncludesAnEventExactlyAtTheBoundary(t *testing.T) {
	at := time.Now().Add(-time.Minute)
	l := NewConnectionEventLog()
	l.Append(eventAt("exactly-at", at))
	l.Append(eventAt("after", at.Add(time.Second)))
	l.Append(eventAt("before", at.Add(-time.Second)))

	got := reasons(l.EventsSince(at))

	if len(got) != 2 || got[0] != "exactly-at" || got[1] != "after" {
		t.Fatalf("EventsSince(boundary) = %v, want [exactly-at after] — an "+
			"event landing exactly on a poll boundary is dropped, and pollers "+
			"that pass their previous timestamp lose one event per poll", got)
	}
}

// Events(d) is the duration-shaped wrapper the monitoring endpoint calls.
func TestEventsWindowsByDurationFromNow(t *testing.T) {
	l := NewConnectionEventLog()
	l.Append(eventAt("stale", time.Now().Add(-30*time.Minute)))
	l.Append(eventAt("fresh", time.Now().Add(-1*time.Minute)))

	got := reasons(l.Events(5 * time.Minute))

	if len(got) != 1 || got[0] != "fresh" {
		t.Fatalf("Events(5m) = %v, want [fresh] — the monitoring endpoint's "+
			"window is not being applied", got)
	}
}

// prune has two independent eviction axes and runs on every Append. Testing
// them separately matters: a fixture that trips both cannot tell which one
// did the work, and dropping either fails toward unbounded growth.
func TestPruneEvictsByAgeAndBySizeIndependently(t *testing.T) {
	t.Run("age", func(t *testing.T) {
		// maxSize deliberately huge so ONLY the age axis can act.
		l := &ConnectionEventLog{maxAge: 10 * time.Minute, maxSize: 100000}
		l.Append(eventAt("expired", time.Now().Add(-time.Hour)))
		l.Append(eventAt("live", time.Now()))

		if got := l.Count(); got != 1 {
			t.Fatalf("Count = %d, want 1 — an event older than maxAge survived, "+
				"so the log grows for the life of the process", got)
		}
		if r := reasons(l.EventsSince(time.Now().Add(-2 * time.Hour))); r[0] != "live" {
			t.Fatalf("the wrong event was evicted: %v remains", r)
		}
	})

	t.Run("size", func(t *testing.T) {
		// maxAge deliberately huge so ONLY the size axis can act.
		l := &ConnectionEventLog{maxAge: 24 * time.Hour, maxSize: 3}
		base := time.Now().Add(-time.Hour)
		for i := 0; i < 6; i++ {
			l.Append(eventAt(string(rune('a'+i)), base.Add(time.Duration(i)*time.Minute)))
		}

		if got := l.Count(); got != 3 {
			t.Fatalf("Count = %d, want maxSize 3 — the size cap is not enforced", got)
		}
		// The NEWEST are kept: an event log that discarded the newest would
		// report a stale picture forever.
		got := reasons(l.EventsSince(base))
		want := []string{"d", "e", "f"}
		for i := range want {
			if got[i] != want[i] {
				t.Fatalf("after the size cap the log holds %v, want the NEWEST "+
					"%v — eviction is dropping the wrong end", got, want)
			}
		}
	})
}

// EventsSince returns a copy. A caller that sorts or mutates the returned
// slice must not be able to corrupt the log's ordering invariant — which
// MESH-C09 (above) depends on.
func TestEventsSinceReturnsACopyTheCallerCannotCorrupt(t *testing.T) {
	base := time.Now().Add(-time.Hour)
	l := NewConnectionEventLog()
	l.Append(eventAt("first", base))
	l.Append(eventAt("second", base.Add(time.Minute)))

	got := l.EventsSince(base)
	if len(got) != 2 {
		t.Fatalf("premise wrong: got %d events, want 2", len(got))
	}
	got[0].Reason = "mutated-by-caller"

	if after := reasons(l.EventsSince(base)); after[0] != "first" {
		t.Fatalf("the log now reads %v — EventsSince handed out its backing "+
			"array, so any consumer that mutates or sorts the result silently "+
			"rewrites the shared log", after)
	}
}
