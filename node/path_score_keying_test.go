/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"testing"
	"time"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/quality"
)

// COVERAGE of RecordPathSuccess (mesh_services.go:82) and RecordPathFailure
// (:99), both at 0.0%.
//
// 🔴 WHY THESE TWO. They are the only writers into the AddressTracker that
// bestAddress reads (peer_connections.go:392 gates on Score(...).IsDead()), so
// they decide which transport+address the mesh will dial. A defect here does
// not surface as an error — it surfaces as traffic routed over a dead path, or
// a healthy path taken out of rotation.
//
// 🔑 THE PROPERTY WORTH TESTING IS THE KEY, NOT THE COUNT. Both functions key
// by node-layer proto.String(). Their doc comments say aether-layer keying
// "would silently merge unrelated paths' success/RTT histories" — and that
// collision is real, not hypothetical: ProtoTLS is the VL1-hijacked-HTTPS
// bootstrap and rides aether.ProtoWebSocket, so at the aether layer "tls" and
// "websocket" are one bucket. At the node layer they are "tls" and "websocket".
//
// The fixtures below are anti-correlated on purpose: the two protocols must
// DISAGREE about liveness, or the assertion cannot tell a shared bucket from a
// separate one.

func pathFixture() *ConnectionManager {
	return &ConnectionManager{
		rt:             &Runtime{ctx: context.Background()},
		addressTracker: quality.NewAddressTracker(),
	}
}

// 🔴 A DEAD TLS BOOTSTRAP MUST NOT CONDEMN THE WEBSOCKET PATH TO THE SAME
// ENDPOINT. These are the two protocols that collide at the aether layer, so
// this is the exact case the keying decision exists for.
//
// If the histories merged, one broken bootstrap would take out every transport
// to that peer+address — and because bestAddress skips dead addresses, the peer
// would become undialable over a path that never failed.
func TestADeadTLSPathLeavesTheWebSocketPathToTheSameAddressAlive(t *testing.T) {
	m := pathFixture()
	const peer, addr = "peer-a", "203.0.113.7:443"

	for i := 0; i < quality.AddressFailuresBeforeDead; i++ {
		m.RecordPathFailure(peer, ProtoTLS, addr)
	}

	tls, ok := m.addressTracker.Score(peer, ProtoTLS.String(), addr)
	if !ok {
		t.Fatalf("fixture wrong: no score recorded for %s after %d failures — the debit "+
			"never reached the tracker", ProtoTLS, quality.AddressFailuresBeforeDead)
	}
	if !tls.IsDead() {
		t.Fatalf("fixture wrong: %d failures did not kill the tls path, so this test "+
			"cannot distinguish merged buckets from separate ones",
			quality.AddressFailuresBeforeDead)
	}

	// The discriminating assertion: same peer, same address, different node
	// protocol — and the two share an aether protocol.
	if ws, ok := m.addressTracker.Score(peer, ProtoWebSocket.String(), addr); ok && ws.IsDead() {
		t.Error("killing the tls path also killed websocket at the same address — the " +
			"histories are keyed at the aether layer, where tls bootstrap rides " +
			"ProtoWebSocket, so one broken bootstrap makes the peer undialable over a " +
			"transport that never failed")
	}
}

// The mirror on the credit side: a success on one protocol must not credit
// another. Without this, only the debit half of the keying is measured — and
// the two halves are separate call sites that could drift apart.
func TestASuccessOnOneProtocolDoesNotCreditAnother(t *testing.T) {
	m := pathFixture()
	const peer, addr = "peer-a", "203.0.113.7:443"

	m.RecordPathSuccess(peer, ProtoQUIC, addr, sessionWithSamples(t, 40*time.Millisecond, 4))

	if _, ok := m.addressTracker.Score(peer, ProtoQUIC.String(), addr); !ok {
		t.Fatal("no score recorded for quic after a success — the credit never reached " +
			"the tracker")
	}
	if _, ok := m.addressTracker.Score(peer, ProtoWebSocket.String(), addr); ok {
		t.Error("a quic success created a websocket score at the same address — the " +
			"credit is landing in a shared bucket, so an RTT measured on one transport " +
			"is attributed to another")
	}
}

// The RTT must be the one the session actually measured. A path score that
// records the right key with the wrong latency still misroutes: bestAddress
// ranks on it.
func TestTheRecordedRTTIsTheSessionsOwnMeasurement(t *testing.T) {
	m := pathFixture()
	const peer, addr = "peer-a", "203.0.113.7:443"

	m.RecordPathSuccess(peer, ProtoQUIC, addr, sessionWithSamples(t, 40*time.Millisecond, 4))

	score, ok := m.addressTracker.Score(peer, ProtoQUIC.String(), addr)
	if !ok {
		t.Fatal("no score recorded")
	}
	if score.RTT <= 0 {
		t.Errorf("recorded RTT = %v, want the session's measurement — a zero RTT sorts "+
			"as the FASTEST path in any ranking that treats absent as best", score.RTT)
	}
}

// Both functions must tolerate a nil tracker. mesh_services.go:56 states
// callers "keep working with addressTracker == nil", so this is a supported
// configuration and not a defensive nicety — a panic here would take down the
// connect path on every deployment that never wired a tracker.
func TestPathScoringToleratesAnAbsentTracker(t *testing.T) {
	m := &ConnectionManager{rt: &Runtime{ctx: context.Background()}} // no tracker

	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("path scoring panicked with no tracker wired: %v — this is a "+
				"SUPPORTED configuration per mesh_services.go:56, so the connect path "+
				"dies on every deployment without one", r)
		}
	}()

	m.RecordPathFailure("peer-a", ProtoQUIC, "203.0.113.7:443")
	m.RecordPathSuccess("peer-a", ProtoQUIC, "203.0.113.7:443", sessionWithSamples(t, time.Millisecond, 1))
}

// 🔬 MEASURING A SUSPECTED HAZARD RATHER THAN ASSERTING IT.
//
// RecordPathSuccess does `session.Health.RTT` with no nil check, and
// health.Monitor.RTT takes m.mu without a nil-receiver guard. A nil Health is
// not hypothetical in this package: nilHealthSession exists in
// adaptive_timeouts_test.go precisely because of "the real race window between
// session construction and SetHealthMonitor", and mesh_connection.go:790
// nil-checks the very same opts.Session that :456 passes in unguarded.
//
// MEASURED: it panicked (nil pointer dereference), and  fixed it.
//
// 🔴 THE SECOND ASSERTION IS THE ONE THAT MATTERS. "Does not panic" would also
// be satisfied by an early return — and that fix would be worse than the bug
// it cures, silently. Crediting the success is what resets ConsecutiveFailures
// and clears DeadUntil; drop it and an address that recovers stays in its
// 30-minute cooldown forever, because the only thing that could revive it is
// the success that was thrown away. An unmeasured RTT is a reason to record no
// RTT, never a reason to record no SUCCESS.
func TestRecordPathSuccessSurvivesASessionWithNoHealthMonitor(t *testing.T) {
	for _, tc := range []struct {
		name    string
		session aether.Session
	}{
		{"health monitor not yet installed", &nilHealthSession{}},
		{"no session at all", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := pathFixture()
			const peer, addr = "peer-a", "203.0.113.7:443"

			// Kill the path first, so the credit has something observable to undo.
			for i := 0; i < quality.AddressFailuresBeforeDead; i++ {
				m.RecordPathFailure(peer, ProtoQUIC, addr)
			}
			if s, _ := m.addressTracker.Score(peer, ProtoQUIC.String(), addr); !s.IsDead() {
				t.Fatal("fixture wrong: the path is not dead, so a lost credit would be invisible")
			}

			func() {
				defer func() {
					if r := recover(); r != nil {
						t.Fatalf("RecordPathSuccess panicked (%s): %v — the connect path at "+
							"mesh_connection.go:456 passes opts.Session straight in, and :790 "+
							"nil-checks that same field, so the window is real", tc.name, r)
					}
				}()
				m.RecordPathSuccess(peer, ProtoQUIC, addr, tc.session)
			}()

			score, ok := m.addressTracker.Score(peer, ProtoQUIC.String(), addr)
			if !ok {
				t.Fatal("no score at all after a success")
			}
			if score.IsDead() {
				t.Error("the success was NOT credited — an absent RTT measurement was " +
					"treated as a reason to record nothing, so this address stays in its " +
					"30-minute dead cooldown with nothing left that could revive it")
			}
			if score.RTT != 0 {
				t.Errorf("recorded RTT = %v with no monitor to measure it — an invented "+
					"measurement, and a small one sorts as the FASTEST path", score.RTT)
			}
		})
	}
}
