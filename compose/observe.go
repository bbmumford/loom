/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package compose

import (
	"context"
	"time"
)

// The OBSERVABLE surface — the fourth composition primitive. It presents the mesh's live state
// through one worker over provider SEAMS the node implements, never importing the node packages
// directly, so compose stays a leaf (the /mesh/status DTO is decoupled the same way, to avoid an
// import cycle). The surface RELAYS what the providers give, VERBATIM: the Prometheus metric names +
// label sets and the /mesh/status JSON shape are an external wire contract (help.orbtr.io
// dashboards), so this layer carries opaque bytes and provider-named values rather than re-deriving
// or renaming anything.

// HealthSnapshot is one reading of the 4-layer HealthEvaluator, relayed as the provider reports it.
// Score follows the mesh_service_health scale (1.0/0.5/0.25/0.0); this layer does not reinterpret it.
type HealthSnapshot struct {
	Score      float64
	Subsystems map[string]float64 // provider-named subsystem → its score
}

// MetricFamily is one Prometheus family relayed verbatim — Name, label set, and value exactly as the
// exporter produces them. This layer never renames Name or a label key.
type MetricFamily struct {
	Name   string
	Labels map[string]string
	Value  float64
}

// HealthSource, MetricsSource, and StatusSource are the seams the node's HealthEvaluator, metrics
// export, and meshstatus DTO satisfy. StatusSource yields the /mesh/status body as raw JSON so its
// shape + CORS posture pass through untouched. Each is optional at the Observer.
type HealthSource interface {
	HealthSnapshot() HealthSnapshot
}
type MetricsSource interface {
	MetricFamilies() []MetricFamily
}
type StatusSource interface {
	MeshStatusJSON() []byte
}

// Observation is one composed reading across the wired sources. An unwired source contributes its
// zero value, so a partially-wired mesh still observes what it has.
type Observation struct {
	Health  HealthSnapshot
	Metrics []MetricFamily
	Status  []byte // raw /mesh/status JSON, verbatim
}

// Observer is the observability worker. It composes whichever of the three sources are wired into a
// single reading and can stream readings at an interval.
type Observer struct {
	health  HealthSource
	metrics MetricsSource
	status  StatusSource
}

// NewObserver wires the observability sources. Any may be nil — the corresponding part of an
// Observation is then its zero value, never a panic.
func NewObserver(h HealthSource, m MetricsSource, s StatusSource) *Observer {
	return &Observer{health: h, metrics: m, status: s}
}

// Observe returns one composed reading. It is cheap and side-effect-free — safe to call on every
// scrape — and copies the metric slice + status bytes so a caller cannot mutate provider-owned state
// through the returned Observation.
func (o *Observer) Observe() Observation {
	var obs Observation
	if o.health != nil {
		obs.Health = o.health.HealthSnapshot()
	}
	if o.metrics != nil {
		src := o.metrics.MetricFamilies()
		obs.Metrics = make([]MetricFamily, len(src))
		copy(obs.Metrics, src)
	}
	if o.status != nil {
		if raw := o.status.MeshStatusJSON(); raw != nil {
			obs.Status = append([]byte(nil), raw...)
		}
	}
	return obs
}

// Stream emits one reading immediately, then one every interval, until ctx is done; the channel is
// closed on return. It is the push form of Observe for a subscriber that wants live composition
// state rather than polling. A non-positive interval yields only the immediate reading, then closes.
func (o *Observer) Stream(ctx context.Context, interval time.Duration) <-chan Observation {
	out := make(chan Observation)
	go func() {
		defer close(out)
		// Immediate first reading so a subscriber sees state without waiting a full interval.
		select {
		case out <- o.Observe():
		case <-ctx.Done():
			return
		}
		if interval <= 0 {
			return
		}
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				select {
				case out <- o.Observe():
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}
