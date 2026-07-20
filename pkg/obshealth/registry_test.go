/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package health

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func newTestRegistry(t *testing.T) *Registry {
	t.Helper()
	return New(AllowedSubsystems())
}

// TestMark_UnknownSubsystem_FailsClosed exercises the bounded-namespace
// gate: arbitrary identifiers must be refused with ErrUnknownSubsystem
// so callers cannot grow the Registry unboundedly with per-event keys.
func TestMark_UnknownSubsystem_FailsClosed(t *testing.T) {
	r := newTestRegistry(t)
	bogus := SubsystemID("webhook.stripe.evt_xxx")

	err := r.Mark(bogus, time.Now(), errors.New("boom"))
	if !errors.Is(err, ErrUnknownSubsystem) {
		t.Fatalf("Mark(bogus): want ErrUnknownSubsystem, got %v", err)
	}
	if !r.IsHealthy() {
		t.Fatalf("registry should remain healthy after failed Mark")
	}
	if got := r.DegradedCount(); got != 0 {
		t.Fatalf("DegradedCount after failed Mark = %d, want 0", got)
	}
}

func TestClear_UnknownSubsystem_FailsClosed(t *testing.T) {
	r := newTestRegistry(t)
	bogus := SubsystemID("nonsense")

	err := r.Clear(bogus, time.Now())
	if !errors.Is(err, ErrUnknownSubsystem) {
		t.Fatalf("Clear(bogus): want ErrUnknownSubsystem, got %v", err)
	}
}

func TestMark_Allowed_RecordsDegraded(t *testing.T) {
	r := newTestRegistry(t)
	at := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)

	if err := r.Mark(SubsystemBillingWebhooks, at, errors.New("stripe 5xx")); err != nil {
		t.Fatalf("Mark err = %v", err)
	}
	snap := r.Snapshot()
	if len(snap.Entries) != 1 {
		t.Fatalf("snapshot entries = %d, want 1", len(snap.Entries))
	}
	got := snap.Entries[0]
	if got.Name != SubsystemBillingWebhooks {
		t.Fatalf("entry name = %s, want %s", got.Name, SubsystemBillingWebhooks)
	}
	if got.Error != "stripe 5xx" {
		t.Fatalf("entry error = %q, want stripe 5xx", got.Error)
	}
	if !got.Since.Equal(at) {
		t.Fatalf("entry since = %v, want %v", got.Since, at)
	}
	if r.IsHealthy() {
		t.Fatalf("IsHealthy = true, want false")
	}
}

func TestClear_HealthyRecovered(t *testing.T) {
	r := newTestRegistry(t)
	t0 := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	if err := r.Mark(SubsystemMeshRaft, t0, errors.New("no leader")); err != nil {
		t.Fatalf("Mark err = %v", err)
	}
	t1 := t0.Add(5 * time.Second)
	if err := r.Clear(SubsystemMeshRaft, t1); err != nil {
		t.Fatalf("Clear err = %v", err)
	}
	if !r.IsHealthy() {
		t.Fatalf("registry should be healthy after Clear")
	}
}

func TestSnapshot_DeterministicSort(t *testing.T) {
	r := newTestRegistry(t)
	at := time.Now()
	_ = r.Mark(SubsystemCommandWS, at, errors.New("a"))
	_ = r.Mark(SubsystemBillingWebhooks, at, errors.New("b"))
	_ = r.Mark(SubsystemMeshRaft, at, errors.New("c"))

	snap := r.Snapshot()
	if len(snap.Entries) != 3 {
		t.Fatalf("entries = %d, want 3", len(snap.Entries))
	}
	for i := 1; i < len(snap.Entries); i++ {
		if snap.Entries[i-1].Name >= snap.Entries[i].Name {
			t.Fatalf("entries not sorted: %v", snap.Entries)
		}
	}
}

// TestMark_MonotonicGate confirms that an out-of-order Mark (Linux
// clock skew or wall-clock jump) does NOT overwrite a newer entry.
func TestMark_MonotonicGate(t *testing.T) {
	r := newTestRegistry(t)
	t1 := time.Date(2026, 6, 15, 12, 0, 5, 0, time.UTC) // newer
	t0 := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC) // older

	if err := r.Mark(SubsystemMeshLedger, t1, errors.New("newer")); err != nil {
		t.Fatalf("Mark t1 err = %v", err)
	}
	// Out-of-order Mark with an OLDER timestamp.
	if err := r.Mark(SubsystemMeshLedger, t0, errors.New("older")); err != nil {
		t.Fatalf("Mark t0 err = %v", err)
	}
	snap := r.Snapshot()
	if len(snap.Entries) != 1 {
		t.Fatalf("entries = %d, want 1", len(snap.Entries))
	}
	if snap.Entries[0].Error != "newer" {
		t.Fatalf("entry error = %q, want newer (older event must lose)", snap.Entries[0].Error)
	}
}

func TestClear_MonotonicGate(t *testing.T) {
	r := newTestRegistry(t)
	t1 := time.Date(2026, 6, 15, 12, 0, 5, 0, time.UTC)
	t0 := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)

	if err := r.Mark(SubsystemMeshLedger, t1, errors.New("err")); err != nil {
		t.Fatalf("Mark err = %v", err)
	}
	// Older Clear must NOT recover a newer Mark.
	if err := r.Clear(SubsystemMeshLedger, t0); err != nil {
		t.Fatalf("Clear err = %v", err)
	}
	if r.IsHealthy() {
		t.Fatalf("out-of-order Clear must not flip healthy")
	}
}

func TestMark_ReMarkDoesNotEmitDuplicateEvent(t *testing.T) {
	r := newTestRegistry(t)
	var degraded, recovered int32
	r.AddListener(func(_ context.Context, ev Event) {
		switch ev.Kind {
		case EventDegraded:
			atomic.AddInt32(&degraded, 1)
		case EventRecovered:
			atomic.AddInt32(&recovered, 1)
		}
	})

	t0 := time.Date(2026, 6, 15, 12, 0, 0, 0, time.UTC)
	if err := r.Mark(SubsystemMeshTransport, t0, errors.New("e1")); err != nil {
		t.Fatalf("Mark err = %v", err)
	}
	if err := r.Mark(SubsystemMeshTransport, t0.Add(time.Second), errors.New("e2")); err != nil {
		t.Fatalf("re-Mark err = %v", err)
	}
	if got := atomic.LoadInt32(&degraded); got != 1 {
		t.Fatalf("degraded event count = %d, want 1 (re-Mark must not emit)", got)
	}

	// And the stored Error should be the most recent.
	snap := r.Snapshot()
	if snap.Entries[0].Error != "e2" {
		t.Fatalf("Mark must update error in place; got %q", snap.Entries[0].Error)
	}
	// Since must still reflect the FIRST degradation, not the re-mark.
	if !snap.Entries[0].Since.Equal(t0) {
		t.Fatalf("Since updated by re-Mark; want %v, got %v", t0, snap.Entries[0].Since)
	}
}

func TestListener_EmitsDegradedAndRecovered(t *testing.T) {
	r := newTestRegistry(t)
	type received struct {
		kind EventKind
		name SubsystemID
		err  error
	}
	var (
		mu  sync.Mutex
		got []received
	)
	r.AddListener(func(_ context.Context, ev Event) {
		mu.Lock()
		got = append(got, received{ev.Kind, ev.Name, ev.Err})
		mu.Unlock()
	})

	t0 := time.Now()
	if err := r.Mark(SubsystemMonitoringNotifier, t0, errors.New("smtp down")); err != nil {
		t.Fatalf("Mark err = %v", err)
	}
	if err := r.Clear(SubsystemMonitoringNotifier, t0.Add(time.Second)); err != nil {
		t.Fatalf("Clear err = %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 2 {
		t.Fatalf("event count = %d, want 2", len(got))
	}
	if got[0].kind != EventDegraded || got[0].name != SubsystemMonitoringNotifier {
		t.Fatalf("event[0] = %+v, want degraded/notifier", got[0])
	}
	if got[0].err == nil || got[0].err.Error() != "smtp down" {
		t.Fatalf("event[0].err = %v, want smtp down", got[0].err)
	}
	if got[1].kind != EventRecovered || got[1].name != SubsystemMonitoringNotifier {
		t.Fatalf("event[1] = %+v, want recovered/notifier", got[1])
	}
	if got[1].err != nil {
		t.Fatalf("recovered event must carry nil err, got %v", got[1].err)
	}
}

// TestListener_MayReenterRegistry confirms the documented contract
// that the lock is dropped before listeners run, so a listener may
// call Snapshot()/Mark()/Clear() without deadlock.
func TestListener_MayReenterRegistry(t *testing.T) {
	r := newTestRegistry(t)
	done := make(chan struct{}, 1)
	r.AddListener(func(_ context.Context, ev Event) {
		if ev.Kind == EventDegraded {
			// Re-enter the registry; if the lock were held, this
			// would deadlock under the race detector.
			_ = r.Snapshot()
		}
		done <- struct{}{}
	})

	if err := r.Mark(SubsystemUptimeRunner, time.Now(), errors.New("x")); err != nil {
		t.Fatalf("Mark err = %v", err)
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("listener did not complete — possible deadlock")
	}
}

func TestConcurrentMarkClear(t *testing.T) {
	r := newTestRegistry(t)
	const workers = 32
	const iterations = 200

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			subs := AllowedSubsystems()
			for j := 0; j < iterations; j++ {
				s := subs[(seed+j)%len(subs)]
				_ = r.Mark(s, time.Now(), errors.New("e"))
				_ = r.Clear(s, time.Now())
			}
		}(i)
	}
	wg.Wait()

	// Final state must be coherent — Snapshot must succeed even under
	// arbitrary interleavings.
	_ = r.Snapshot()
}

func TestAllowedSubsystems_NonEmpty(t *testing.T) {
	if len(AllowedSubsystems()) == 0 {
		t.Fatalf("AllowedSubsystems must return at least one identifier")
	}
}

// TestNew_EmptyAllowlist_IsFailClosed confirms that a Registry built
// with an empty allow-list refuses every Mark/Clear (it is the user's
// responsibility to declare their subsystems).
func TestNew_EmptyAllowlist_IsFailClosed(t *testing.T) {
	r := New(nil)
	err := r.Mark(SubsystemMeshRaft, time.Now(), errors.New("e"))
	if !errors.Is(err, ErrUnknownSubsystem) {
		t.Fatalf("empty allow-list must reject every Mark; got %v", err)
	}
}

// TestAllowedSubsystems_IncludesORBTRPolicy asserts every policy.*
// subsystem the ORBTR agent PolicyEngine reflects into the shared
// Registry (rev-054) is present in AllowedSubsystems(). If a new
// applyOrLog call site is added to PolicyEngine.PullAndApply, its
// mapped SubsystemID MUST be added to allowlist.go in the same commit
// or the Mark will fail-closed at runtime.
func TestAllowedSubsystems_IncludesORBTRPolicy(t *testing.T) {
	want := []SubsystemID{
		SubsystemPolicyLocalStore,
		SubsystemPolicyOSFirewall,
		SubsystemPolicyPacketFilter,
		SubsystemPolicyAddressGroups,
		SubsystemPolicyQuarantine,
		SubsystemPolicyEdgeSegment,
		SubsystemPolicyEdgeRole,
		SubsystemPolicyAck,
	}
	got := AllowedSubsystems()
	seen := make(map[SubsystemID]struct{}, len(got))
	for _, id := range got {
		seen[id] = struct{}{}
	}
	for _, id := range want {
		if _, ok := seen[id]; !ok {
			t.Errorf("AllowedSubsystems missing %q", id)
		}
	}
}
