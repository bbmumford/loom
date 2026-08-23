/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"context"
	"log"
	"time"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/quality"
)

// StartMeshServices initializes and starts all Aether background services.
// Called during ConnectionManager startup, after the manager is initialized.
//
// Background services:
//   - StreamGC: idle stream garbage collection (5min timeout)
//   - AdaptiveController: CPU-aware feature degradation
//
// Per-call services consumed elsewhere:
//   - AddressTracker: dial-side per-(peer, transport, addr) success/RTT
//     scoring used by ConnectionManager.bestAddress and on connect events
//   - Profile: used by adapters at session creation
//   - TransportClass: used by adapters for per-class defaults
//   - HeaderCompression: used by TCP/WS adapters on hot streams
//   - Multipath: integrated into ConnectionManager's session tracking
//   - PMTU: started per Noise-UDP session
//   - Migration: handled via HANDSHAKE frame dispatch
//   - 0-RTT Resume: checked at connection establishment
func StartMeshServices(ctx context.Context, connMgr *ConnectionManager) {
	// Session options are now per-session (set at session creation), not global.
	opts := aether.DefaultSessionOptions()
	log.Printf("[AETHER] Default session options: FEC=%v Comp=%v Enc=%v Sched=%v",
		opts.FEC, opts.Compression, opts.Encryption, opts.Scheduler)

	// Start stream garbage collector
	streamGC := aether.NewStreamGC(aether.DefaultStreamIdleTimeout, func(streamID uint64) {
		log.Printf("[AETHER] Stream GC: would reset idle stream %d", streamID)
		// In production, this calls the Connection's stream.Reset()
		// For now, just log — actual reset requires access to the Aether session
	})
	go streamGC.Start()

	// Start adaptive CPU controller. NewAdaptiveController is
	// now parameterless — the controller degrades LIVE registered sessions
	// rather than a by-value SessionOptions copy nothing read. Wiring session
	// Register/Unregister on open/close (so it actually sheds load) is a
	// tracked follow-up; instantiating + Starting it here is unchanged.
	adaptive := aether.NewAdaptiveController()
	go adaptive.Start()

	// Both services own a ticker goroutine that runs until stopped. The ctx
	// this function already accepts is the node's lifetime; without this the
	// parameter is unused and the two goroutines outlive every Shutdown, so a
	// Runtime torn down and rebuilt in one process accumulates them.
	go func() {
		<-ctx.Done()
		streamGC.Stop()
		adaptive.Stop()
		log.Printf("[AETHER] Services stopped: StreamGC, AdaptiveController")
	}()

	// Initialise the per-address tracker. Only created here so existing
	// tests and entry points that build a ConnectionManager without
	// calling StartMeshServices keep working with addressTracker == nil
	// (the consumers all guard against it).
	connMgr.addressTracker = quality.NewAddressTracker()

	// pprof activation has moved to each endpoint's own main.go (call
	// debug.StartPprofIfEnabled() once after config is loaded). Owning
	// it at the endpoint level keeps the Library free of process-global
	// side effects and lets each endpoint decide whether it wants the
	// debug port at all (e.g. low-memory thin apps may keep it off
	// permanently regardless of DEBUG value).

	log.Printf("[AETHER] Services started: StreamGC, AdaptiveController, AddressTracker")
}

// RecordPathSuccess credits a successful connect/exchange for the
// (peer, transport, address) triple. Reads SRTT off the freshly
// established session so the per-address RTT used by bestAddress
// matches the actual measured handshake-to-first-ack latency.
//
// Transport key uses the node-layer Protocol.String() ("noise-udp",
// "websocket", "tls", "grpc", "quic") rather than the aether-layer
// equivalent — aether maps several distinct dial paths into a single
// adapter (TLS bootstrap rides aether.ProtoWebSocket), so keying by
// aether.Protocol would silently merge unrelated paths' success/RTT
// histories. The dialer dispatches by node Protocol, so AddressTracker
// must too for bestAddress and ProtoIsDead to give the right answer.
func (m *ConnectionManager) RecordPathSuccess(nodeID string, proto Protocol, addr string, session aether.Session) {
	if m.addressTracker == nil {
		return
	}
	// 🔴 A SESSION CAN REACH HERE BEFORE ITS HEALTH MONITOR IS INSTALLED, and
	// health.Monitor.RTT takes m.mu with NO nil-receiver guard — so the
	// unguarded `session.Health().RTT()` this replaced was a nil dereference on
	// the connect path, not a theoretical one. mesh_connection.go:790
	// nil-checks the very same opts.Session that :456 passes in here unguarded,
	// and adaptive_timeouts_test.go's nilHealthSession exists for "the real
	// race window between session construction and SetHealthMonitor".
	//
	// 🔑 REPORTING ABSENCE AS 0 IS SAFE HERE ONLY BECAUSE OF A GUARD IN ANOTHER
	// MODULE: AddressTracker.RecordSuccess applies `if rtt > 0` before storing
	// (aether@v0.0.111 quality/address_tracker.go:91), so a 0 credits the
	// success — resetting ConsecutiveFailures and clearing DeadUntil, which is
	// the part that matters — without recording a 0ms path. If that guard ever
	// goes, this becomes "absent sorts as best": an unmeasured path would rank
	// as the fastest one available. The success must still be credited, so
	// skipping the call is not the alternative.
	var rtt time.Duration
	if session != nil {
		if h := session.Health(); h != nil {
			_, rtt = h.RTT()
		}
	}
	m.addressTracker.RecordSuccess(nodeID, proto.String(), addr, rtt)
	// [PATH-SCORE] every credit/debit is logged so an operator can see
	// directly which transport+address is gaining ground for a peer.
	log.Printf("[PATH-SCORE] success peer=%s proto=%s addr=%s rtt=%v",
		truncID(nodeID), proto, addr, rtt)
}

// RecordPathFailure debits a failed connect/exchange for an address.
// Crossing the consecutive-failure threshold inside AddressTracker
// triggers a cooldown that bestAddress and the dial path respect.
// Keys the entry by node-layer proto.String() — see RecordPathSuccess
// for why aether-layer keying would conflate distinct dial paths.
func (m *ConnectionManager) RecordPathFailure(nodeID string, proto Protocol, addr string) {
	if m.addressTracker == nil {
		return
	}
	m.addressTracker.RecordFailure(nodeID, proto.String(), addr)
	log.Printf("[PATH-SCORE] failure peer=%s proto=%s addr=%s",
		truncID(nodeID), proto, addr)
}
