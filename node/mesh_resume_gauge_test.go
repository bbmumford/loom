/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"net"
	"sync/atomic"
	"testing"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/noise"
	"github.com/bbmumford/loom/core/transport/manager"
)

// TestMeshResume_SharedTransportDropsTicketCapable pins the divergence that the
// mesh_resume_capable_transports census exists to expose.
//
// *noise.NoiseTransport implements aether.TicketCapable (IssueTicket +
// ResumeSession). loom wraps it in *manager.SharedTransport for multi-tenant
// ports, and that wrapper forwards Dial/Listen/Close but NOT the ticket methods
// — so the optional interface is silently dropped at the wrapper. Any caller
// that type-asserts TicketCapable on a shared transport gets false, even though
// SharedTransport.Dial delegates to inner.Dial and DOES resume internally.
//
// This is why runtime.go asserts on the transport interface value rather than
// unwrapping to the inner NoiseTransport first: unwrapping would make the census
// a compile-time tautology that always reports "capable".
//
// ⚠ IF THIS TEST FLIPS: someone added IssueTicket/ResumeSession forwarding to
// SharedTransport. That is a fine change — but update the census comment in
// runtime.go, because the "incapable" bucket will stop counting shared-transport
// tenants and the reading changes.
func TestMeshResume_SharedTransportDropsTicketCapable(t *testing.T) {
	// POSITIVE CONTROL FIRST. Without it, a typo in the interface name would
	// make both assertions report "not capable" and the test would pass while
	// measuring nothing.
	var inner any = (*noise.NoiseTransport)(nil)
	if _, ok := inner.(aether.TicketCapable); !ok {
		t.Fatal("POSITIVE CONTROL FAILED: *noise.NoiseTransport must satisfy " +
			"aether.TicketCapable. Either the interface moved/renamed or the " +
			"transport lost its ticket methods — until this passes, the negative " +
			"assertion below proves nothing")
	}

	var wrapper any = (*manager.SharedTransport)(nil)
	if _, ok := wrapper.(aether.TicketCapable); ok {
		t.Error("SharedTransport now satisfies aether.TicketCapable — the " +
			"capability is no longer dropped at the wrapper. Update the " +
			"mesh_resume_capable_transports census comment in runtime.go: the " +
			"incapable bucket no longer counts shared-transport tenants")
	}
}

// TestMeshResume_CapabilityCensusMeasuresReachability drives the census the way
// MeshMetrics does and asserts it splits a mixed transport set correctly.
//
// The census is the half of the resumption reading that can come out either way:
// if it reported zero capable transports, the "we mint tickets nobody can
// redeem" conclusion would NOT hold, and that is the outcome that refutes it.
func TestMeshResume_CapabilityCensusMeasuresReachability(t *testing.T) {
	transports := []aether.Transport{
		(*noise.NoiseTransport)(nil),    // capable
		(*manager.SharedTransport)(nil), // wrapper drops it
		(*noise.NoiseTransport)(nil),    // capable
	}

	var capable, incapable uint64
	for _, tr := range transports {
		if _, ok := tr.(aether.TicketCapable); ok {
			capable++
			continue
		}
		incapable++
	}

	if capable != 2 || incapable != 1 {
		t.Errorf("census split wrong: capable=%d incapable=%d, want 2/1 over a "+
			"set of 2 NoiseTransport + 1 SharedTransport", capable, incapable)
	}
}

// TestMeshResume_SetupMeshSessionCountsBothDirections pins that the gauge is
// actually REACHED by the mesh session path — the distinction that matters, since
// a counter nothing increments reads 0 by construction and looks like evidence.
//
// Asserts DELTAS rather than absolute values so the test is order-independent:
// these are package-level counters and other tests in this package may establish
// sessions too. An absolute assertion here would be a coin-flip under -shuffle.
func TestMeshResume_SetupMeshSessionCountsBothDirections(t *testing.T) {
	rt := &Runtime{identity: &NodeIdentity{}}

	beforeInit := atomic.LoadUint64(&meshInitiatorSessions)
	beforeResp := atomic.LoadUint64(&meshResponderSessions)

	// ⚠ ASYMMETRIC ON PURPOSE: 2 initiator, 1 responder.
	//
	// The obvious version of this test establishes one session in each direction
	// and asserts 1/1. That is MUTATION-BLIND to swapped branches: reversing the
	// if/else produces 1/1 as well, so the test passes while every initiator
	// session is counted as a responder and vice versa. Measured, not theorised
	// — the symmetric version of this test passed with the branches swapped.
	//
	// With 2/1, correct code gives (2,1) and swapped code gives (1,2). Symmetric
	// data cannot detect an asymmetric defect.
	//
	// net.Pipe is enough: adapter.NewSessionForProtocol is a pure constructor
	// (factory.go switches on protocol and wraps the conn), so no handshake or
	// I/O happens here.
	for _, initiator := range []bool{true, true, false} {
		c1, c2 := net.Pipe()
		defer c1.Close()
		defer c2.Close()

		sess, err := rt.SetupMeshSession(context.Background(), c1, "peer-node-id",
			ProtoNoiseUDP, initiator)
		if err != nil {
			t.Fatalf("SetupMeshSession(initiator=%v): %v", initiator, err)
		}
		if sess == nil {
			t.Fatalf("SetupMeshSession(initiator=%v) returned a nil session", initiator)
		}
	}

	gotInit := atomic.LoadUint64(&meshInitiatorSessions) - beforeInit
	gotResp := atomic.LoadUint64(&meshResponderSessions) - beforeResp

	if gotInit != 2 {
		t.Errorf("mesh_initiator_sessions delta = %d, want 2 — the gauge is not "+
			"reached by the initiator path, so a zero reading would be a "+
			"tautology rather than a measurement", gotInit)
	}
	if gotResp != 1 {
		t.Errorf("mesh_responder_sessions delta = %d, want 1", gotResp)
	}
	// Name the swap explicitly so the failure diagnoses itself rather than
	// leaving two off-by-one errors for the next reader to correlate.
	if gotInit == 1 && gotResp == 2 {
		t.Error("the initiator/responder branches are SWAPPED in " +
			"SetupMeshSession: 2 initiator + 1 responder sessions were " +
			"established and the counters report the mirror image")
	}
}

// TestMeshResume_CountersDoNotMoveWithoutASession is the NEGATIVE CONTROL that
// R-925 requires, and the test above is incomplete without it.
//
// The mutation proof and the delta assertions above establish that the gauge CAN
// see a session and CAN report a miscount. Neither shows that it stays SILENT on
// a known-clean case — and an instrument validated only on positives is how you
// ship one that flags everything. Concretely: if these counters were ever moved
// to a metrics-read path, or incremented from a constructor or a health tick,
// every assertion above would still pass while the numbers became meaningless as
// a session count.
//
// So: exercise the surrounding machinery WITHOUT establishing a mesh session and
// require both counters to be exactly unchanged.
func TestMeshResume_CountersDoNotMoveWithoutASession(t *testing.T) {
	rt := &Runtime{identity: &NodeIdentity{}}

	beforeInit := atomic.LoadUint64(&meshInitiatorSessions)
	beforeResp := atomic.LoadUint64(&meshResponderSessions)

	// Things that must NOT count as establishing a mesh session: reading the
	// session options, constructing a conn pair and closing it, and mapping a
	// protocol. None of these should touch the gauge.
	_ = rt.SessionOptions()
	rt.SetSessionOptions(rt.SessionOptions())
	_ = mapProtocol(ProtoNoiseUDP)
	c1, c2 := net.Pipe()
	c1.Close()
	c2.Close()

	if got := atomic.LoadUint64(&meshInitiatorSessions) - beforeInit; got != 0 {
		t.Errorf("NEGATIVE CONTROL FAILED: mesh_initiator_sessions moved by %d "+
			"without any session being established. The counter is being "+
			"incremented from somewhere other than SetupMeshSession, so it no "+
			"longer measures mesh session establishment", got)
	}
	if got := atomic.LoadUint64(&meshResponderSessions) - beforeResp; got != 0 {
		t.Errorf("NEGATIVE CONTROL FAILED: mesh_responder_sessions moved by %d "+
			"without any session being established", got)
	}
}
