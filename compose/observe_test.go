/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package compose

import (
	"bytes"
	"context"
	"testing"
	"time"
)

type fakeHealth struct{ s HealthSnapshot }

func (f fakeHealth) HealthSnapshot() HealthSnapshot { return f.s }

type fakeMetrics struct{ m []MetricFamily }

func (f fakeMetrics) MetricFamilies() []MetricFamily { return f.m }

type fakeStatus struct{ j []byte }

func (f fakeStatus) MeshStatusJSON() []byte { return f.j }

func TestObserver_ComposesWiredSources(t *testing.T) {
	h := fakeHealth{HealthSnapshot{Score: 0.5, Subsystems: map[string]float64{"transport": 1.0}}}
	m := fakeMetrics{[]MetricFamily{
		{Name: "mesh_service_health", Labels: map[string]string{"role": "auth"}, Value: 1.0},
		{Name: "mesh_connections_total", Labels: map[string]string{"transport": "aether", "grade": "direct"}, Value: 7},
	}}
	s := fakeStatus{[]byte(`{"role":"auth","peers":3}`)}

	obs := NewObserver(h, m, s).Observe()

	if obs.Health.Score != 0.5 || obs.Health.Subsystems["transport"] != 1.0 {
		t.Fatalf("health not relayed: %+v", obs.Health)
	}
	// Wire contract: the metric names + labels must pass through unchanged.
	if len(obs.Metrics) != 2 || obs.Metrics[0].Name != "mesh_service_health" || obs.Metrics[1].Labels["grade"] != "direct" {
		t.Fatalf("metrics renamed or dropped: %+v", obs.Metrics)
	}
	// Wire contract: the /mesh/status body is relayed byte-for-byte.
	if !bytes.Equal(obs.Status, []byte(`{"role":"auth","peers":3}`)) {
		t.Fatalf("status body reshaped: %s", obs.Status)
	}
}

func TestObserver_NilSourcesAreZeroNotPanic(t *testing.T) {
	obs := NewObserver(nil, nil, nil).Observe()
	if obs.Health.Score != 0 || obs.Metrics != nil || obs.Status != nil {
		t.Fatalf("unwired observer must yield zero values: %+v", obs)
	}
}

func TestObserver_ObserveCopiesProviderState(t *testing.T) {
	src := []MetricFamily{{Name: "mesh_rpc_handler_total", Value: 1}}
	o := NewObserver(nil, fakeMetrics{src}, nil)
	obs := o.Observe()
	obs.Metrics[0].Name = "TAMPERED" // mutate the returned copy
	// The provider's own slice must be untouched — the copy protected it.
	if src[0].Name != "mesh_rpc_handler_total" {
		t.Fatal("Observe must copy metrics so a caller cannot mutate provider state")
	}
}

func TestObserver_StreamEmitsThenClosesOnCancel(t *testing.T) {
	o := NewObserver(fakeHealth{HealthSnapshot{Score: 1.0}}, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	ch := o.Stream(ctx, 10*time.Millisecond)

	// The immediate first reading arrives without waiting an interval.
	first, ok := <-ch
	if !ok || first.Health.Score != 1.0 {
		t.Fatalf("expected an immediate first reading, got ok=%v %+v", ok, first)
	}
	// Cancelling drains + closes the channel.
	cancel()
	deadline := time.After(time.Second)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return // closed — correct
			}
		case <-deadline:
			t.Fatal("Stream channel did not close after cancel")
		}
	}
}
