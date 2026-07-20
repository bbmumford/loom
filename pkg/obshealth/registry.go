/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package health

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"sync"
	"time"
)

// SubsystemID is a typed alias over string. ONLY values returned by
// AllowedSubsystems() may be passed to Registry.Mark / Clear — the
// Registry enforces this at runtime, refusing unknown identifiers with
// ErrUnknownSubsystem (fail-closed). Per-event compound keys such as
// "webhook.stripe.evt_xxx" therefore cannot grow the map unboundedly.
type SubsystemID string

// Entry is the public, point-in-time view of one degraded subsystem.
// JSON tags match the ORBTR DegradedSubsystem shape so meshmon can
// deserialise either source with one struct.
type Entry struct {
	Name  SubsystemID `json:"name"`
	Error string      `json:"error"`
	Since time.Time   `json:"since"`
}

// Snapshot is the public read view returned to the monitoring API.
// Entries is sorted by Name for deterministic output.
type Snapshot struct {
	GeneratedAt time.Time `json:"generatedAt"`
	Entries     []Entry   `json:"entries"`
}

// EventKind discriminates degraded vs recovered transitions.
type EventKind int

const (
	// EventDegraded indicates a healthy→degraded transition.
	EventDegraded EventKind = iota + 1
	// EventRecovered indicates a degraded→healthy transition.
	EventRecovered
)

// Event describes a healthy/degraded transition. Err is nil on
// EventRecovered.
type Event struct {
	Kind EventKind
	Name SubsystemID
	At   time.Time
	Err  error
}

// EventListener is invoked synchronously when a subsystem transitions
// healthy→degraded or degraded→healthy. The Registry holds NO lock
// while calling listeners (drop-the-lock-then-emit) so listeners may
// re-enter the Registry safely. Listeners MUST NOT block for more than
// a few ms; long work belongs in a queued goroutine owned by the
// listener.
type EventListener func(ctx context.Context, ev Event)

// ErrUnknownSubsystem is returned by Mark / Clear when the caller
// passes an identifier that is not in AllowedSubsystems().
var ErrUnknownSubsystem = errors.New("health: unknown subsystem identifier")

// Option configures a Registry at construction time.
type Option func(*Registry)

// WithClock injects a deterministic clock for tests.
func WithClock(fn func() time.Time) Option {
	return func(r *Registry) {
		if fn != nil {
			r.clock = fn
		}
	}
}

// WithLogger injects a custom slog logger. Defaults to slog.Default().
func WithLogger(l *slog.Logger) Option {
	return func(r *Registry) {
		if l != nil {
			r.logger = l
		}
	}
}

// Registry is the platform-wide subsystem health sink. Safe for
// concurrent Mark/Clear from any goroutine. Zero-value not usable —
// callers must use New.
type Registry struct {
	mu        sync.RWMutex
	allowed   map[SubsystemID]struct{}
	entries   map[SubsystemID]registryEntry
	listeners []EventListener
	clock     func() time.Time
	logger    *slog.Logger
}

type registryEntry struct {
	err        error
	since      time.Time
	lastUpdate time.Time // monotonic gate
}

// New constructs a Registry restricted to the supplied allow-list.
// Typical usage passes AllowedSubsystems() as the allowed argument.
//
// Passing an empty allowed list is legal but yields a Registry that
// refuses every Mark / Clear; this is intentional fail-closed
// behaviour for callers that have not yet declared their subsystems.
func New(allowed []SubsystemID, opts ...Option) *Registry {
	r := &Registry{
		allowed: make(map[SubsystemID]struct{}, len(allowed)),
		entries: make(map[SubsystemID]registryEntry),
		clock:   time.Now,
		logger:  slog.Default(),
	}
	for _, id := range allowed {
		r.allowed[id] = struct{}{}
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// Mark records that name is currently degraded.
//
// Returns ErrUnknownSubsystem if name is not in the allow-list.
//
// Monotonic gate: every Mark stamps `at` as lastUpdate. If a Mark
// arrives with at < existing.lastUpdate (Linux clock skew or wall
// clock jump, or out-of-order goroutine scheduling) it is dropped.
// Callers therefore pass the time captured BEFORE doing the
// underlying work so the most recent real event wins.
//
// Listeners are invoked synchronously OUTSIDE the registry lock when
// a healthy→degraded transition is observed. Re-marking an already
// degraded subsystem with a new error updates the stored error in
// place and does NOT emit a new degraded event.
func (r *Registry) Mark(name SubsystemID, at time.Time, err error) error {
	if _, ok := r.allowed[name]; !ok {
		r.logger.Error("subsystem_unknown",
			slog.String("subsystem", string(name)),
			slog.String("op", "Mark"))
		return ErrUnknownSubsystem
	}

	if at.IsZero() {
		at = r.clock()
	}

	r.mu.Lock()
	prev, wasDegraded := r.entries[name]
	if wasDegraded && at.Before(prev.lastUpdate) {
		// Out-of-order event; drop.
		r.mu.Unlock()
		return nil
	}

	since := at
	if wasDegraded {
		since = prev.since
	}
	r.entries[name] = registryEntry{
		err:        err,
		since:      since,
		lastUpdate: at,
	}
	listeners := append([]EventListener(nil), r.listeners...)
	r.mu.Unlock()

	if !wasDegraded {
		r.logger.Error("subsystem_degraded",
			slog.String("subsystem", string(name)),
			slog.Time("at", at),
			slog.Any("err", err))
		ev := Event{Kind: EventDegraded, Name: name, At: at, Err: err}
		ctx := context.Background()
		for _, fn := range listeners {
			fn(ctx, ev)
		}
	}
	return nil
}

// Clear marks the subsystem healthy. Same monotonic gate as Mark — a
// Clear with at < existing.lastUpdate is dropped. Clearing an already
// healthy subsystem is a no-op (no event emitted).
func (r *Registry) Clear(name SubsystemID, at time.Time) error {
	if _, ok := r.allowed[name]; !ok {
		r.logger.Error("subsystem_unknown",
			slog.String("subsystem", string(name)),
			slog.String("op", "Clear"))
		return ErrUnknownSubsystem
	}

	if at.IsZero() {
		at = r.clock()
	}

	r.mu.Lock()
	prev, wasDegraded := r.entries[name]
	if !wasDegraded {
		r.mu.Unlock()
		return nil
	}
	if at.Before(prev.lastUpdate) {
		// Out-of-order Clear; drop.
		r.mu.Unlock()
		return nil
	}
	delete(r.entries, name)
	listeners := append([]EventListener(nil), r.listeners...)
	r.mu.Unlock()

	recoveryDuration := at.Sub(prev.since)
	r.logger.Info("subsystem_recovered",
		slog.String("subsystem", string(name)),
		slog.Int64("since_ms", recoveryDuration.Milliseconds()))
	ev := Event{Kind: EventRecovered, Name: name, At: at, Err: nil}
	ctx := context.Background()
	for _, fn := range listeners {
		fn(ctx, ev)
	}
	return nil
}

// Snapshot returns a deterministic, sorted point-in-time view.
// Callers MUST NOT mutate the returned slice.
func (r *Registry) Snapshot() Snapshot {
	r.mu.RLock()
	entries := make([]Entry, 0, len(r.entries))
	for name, e := range r.entries {
		msg := ""
		if e.err != nil {
			msg = e.err.Error()
		}
		entries = append(entries, Entry{
			Name:  name,
			Error: msg,
			Since: e.since,
		})
	}
	r.mu.RUnlock()

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name < entries[j].Name
	})

	return Snapshot{
		GeneratedAt: r.clock(),
		Entries:     entries,
	}
}

// AddListener registers an event listener. Listeners are called
// synchronously OUTSIDE the registry lock — they MUST NOT block for
// more than a few ms.
func (r *Registry) AddListener(fn EventListener) {
	if fn == nil {
		return
	}
	r.mu.Lock()
	r.listeners = append(r.listeners, fn)
	r.mu.Unlock()
}

// IsHealthy returns true iff zero subsystems are currently degraded.
func (r *Registry) IsHealthy() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.entries) == 0
}

// DegradedCount returns the number of subsystems currently degraded.
// Useful for /heartbeat-style status checks that want a single int.
func (r *Registry) DegradedCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.entries)
}
