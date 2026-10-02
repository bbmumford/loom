/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"strings"
	"testing"
)

// MeshMetrics publishes the cross-origin dial funnel under cross_origin_*
// keys and the per-partition compose limit under
// compose_task_partition_in_flight_limit. Monitoring clients read these
// names, so each counter is loaded with a distinct value and read back by
// key: a key spelled with another stem, or two keys swapped, fails here.
func TestMeshMetricsNamesTheCrossOriginFunnelByOrigin(t *testing.T) {
	rt := &Runtime{}
	m := &ConnectionManager{rt: rt, peers: map[string]*peerConn{}, budget: DefaultConnectionBudget()}
	rt.connMgr = m

	m.crossOriginPreambleDialsAttempted.Add(1)
	m.crossOriginPreambleDialsSucceeded.Add(2)
	m.crossOriginPreambleDialsFailed.Add(3)
	m.crossOriginPickPathNoiseUDPIncluded.Add(4)
	m.crossOriginBestAddrPublicUDPCount.Add(5)
	m.crossOriginBestAddrCandidatesEmpty.Add(6)
	m.crossOriginBestAddrAllDead.Add(7)
	m.crossOriginBestAddrNonAnycastSelected.Add(8)
	m.crossOriginDialNoSuitableAddr.Add(9)

	metrics := rt.MeshMetrics()

	want := map[string]uint64{
		"cross_origin_preamble_dials_attempted":      1,
		"cross_origin_preamble_dials_succeeded":      2,
		"cross_origin_preamble_dials_failed":         3,
		"cross_origin_pickpath_noise_udp_included":   4,
		"cross_origin_bestaddr_public_udp_count":     5,
		"cross_origin_bestaddr_candidates_empty":     6,
		"cross_origin_bestaddr_all_dead":             7,
		"cross_origin_bestaddr_non_anycast_selected": 8,
		"cross_origin_dial_no_suitable_addr":         9,
		// No NoiseTransport is attached, so no forwarder counts a msg1.
		"cross_origin_msg1_received": 0,
	}
	for key, value := range want {
		got, ok := metrics[key]
		if !ok {
			t.Errorf("MeshMetrics has no %q key", key)
			continue
		}
		if got != value {
			t.Errorf("MeshMetrics[%q] = %#v, want %d", key, got, value)
		}
	}
	if _, ok := metrics["compose_task_partition_in_flight_limit"]; !ok {
		t.Error(`MeshMetrics has no "compose_task_partition_in_flight_limit" key`)
	}
	for key := range metrics {
		if strings.Contains(key, "cross_org") || strings.HasPrefix(key, "compose_task_owner") {
			t.Errorf("MeshMetrics emits %q: the funnel and compose limit keys are named by origin and partition", key)
		}
	}
}

// bidiScopeLabel's three values are the <scope> segment of every
// bidirpc_*_ms_<transport>_<scope> key, so each branch is pinned here.
func TestBidiScopeLabelNamesTheOriginAndRegionClass(t *testing.T) {
	cases := []struct {
		name  string
		peer  *peerConn
		local string
		want  string
	}{
		{"nil peer", nil, "syd", "same-origin-same-region"},
		{"another origin", &peerConn{crossOrigin: true, peerRegion: "syd"}, "syd", "cross-origin"},
		{"another origin and region", &peerConn{crossOrigin: true, peerRegion: "iad"}, "syd", "cross-origin"},
		{"same origin, another region", &peerConn{peerRegion: "iad"}, "syd", "same-origin-cross-region"},
		{"same origin and region", &peerConn{peerRegion: "syd"}, "syd", "same-origin-same-region"},
		{"same origin, region unknown", &peerConn{}, "syd", "same-origin-same-region"},
	}
	for _, tc := range cases {
		if got := bidiScopeLabel(tc.peer, tc.local); got != tc.want {
			t.Errorf("%s: bidiScopeLabel = %q, want %q", tc.name, got, tc.want)
		}
	}
}
