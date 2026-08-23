/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"testing"
	"time"

	"github.com/ORBTR/aether"
)

// COVERAGE of the connection reporter: snapshotLiveSessions,
// ActiveConnections, ConnectionTo, ConnectedPeerCount — all 0.0%.
//
// Censused first with a NON-IGNORING grep: every one is driven, and
// two consumers are outside loom entirely —
// ORBTR io/endpoints/help.orbtr.io/monitoring_api.go calls both
// ActiveConnections and ConnectionTo. This is the surface the topology view
// and the monitoring endpoint read, so a wrong answer here is a wrong
// operational picture rather than a wrong routing decision.

// reporterSession is a probeSession with a settable RTT. probeSession.Metrics
// returns the zero value, and RTT is half of what the live-session override
// is for — embedding rather than editing the shared fixture keeps other
// tests unaffected.
type reporterSession struct {
	*probeSession
	rtt time.Duration
}

func (s *reporterSession) Metrics() aether.SessionMetrics {
	return aether.SessionMetrics{RTT: s.rtt}
}

// reporterFixture builds a manager holding one connected peer whose COLLAPSED
// fields (protocol, lastRTT) deliberately disagree with its live session.
// That disagreement is the entire subject of this file.
func reporterFixture(liveProto aether.Protocol, liveRTT time.Duration, closed bool) (*ConnectionManager, *peerConn) {
	p := &peerConn{
		nodeID:        testNodeIDB,
		state:         PeerConnected,
		protocol:      ProtoNoiseUDP, // collapsed field: the most recent INSTALL
		lastRTT:       500 * time.Millisecond,
		isMuxed:       true,
		peerRegion:    "syd",
		connCount:     2,
		lastConnected: time.Now(),
	}
	m := &ConnectionManager{peers: map[string]*peerConn{testNodeIDB: p}}
	sess := &reporterSession{probeSession: &probeSession{proto: liveProto}, rtt: liveRTT}
	sess.closed = closed
	m.meshSessions = map[string]aether.Session{testNodeIDB: sess}
	return m, p
}

// 🔴 THE DOCUMENTED PROPERTY, AND THE REASON IT EXISTS.
//
// peerConn.protocol/lastRTT are collapsed fields that reflect the most recent
// INSTALL. When a peer holds multiple concurrent transports they lag the data
// plane: noise-udp can sit in p.protocol while ws is actually carrying
// traffic. The topology view must report the data plane.
//
// Note this is asserted on THREE derived outputs, not one — Transport, Grade
// and RTT are computed separately downstream of the same override, and an
// implementation could plausibly fix one and miss another.
func TestActiveConnectionsReportsTheLiveSessionNotTheLastInstall(t *testing.T) {
	const liveRTT = 12 * time.Millisecond
	m, p := reporterFixture(aether.ProtoWebSocket, liveRTT, false)

	if p.protocol != ProtoNoiseUDP {
		t.Fatal("premise wrong: the collapsed field is not noise-udp")
	}
	if GradeForProtocol(ProtoNoiseUDP) == GradeForProtocol(ProtoWebSocket) {
		t.Fatal("premise wrong: the two transports share a grade, so the Grade " +
			"assertion below could not distinguish them")
	}

	conns := NewConnectionReporter(m).ActiveConnections()

	if len(conns) != 1 {
		t.Fatalf("ActiveConnections returned %d entries, want 1", len(conns))
	}
	ci := conns[0]
	if ci.Transport != ProtoWebSocket.String() {
		t.Fatalf("Transport = %q, want %q — the topology view is reporting the "+
			"most recent INSTALL instead of the transport actually carrying "+
			"traffic", ci.Transport, ProtoWebSocket.String())
	}
	if ci.Grade != GradeForProtocol(ProtoWebSocket) {
		t.Fatalf("Grade = %v, want %v — Grade is being derived from the "+
			"collapsed peer field rather than from the overridden transport, so "+
			"the view claims a path quality the data plane does not have",
			ci.Grade, GradeForProtocol(ProtoWebSocket))
	}
	if ci.RTT != liveRTT {
		t.Fatalf("RTT = %v, want the live session's %v — latency is reported "+
			"from a stale per-peer field", ci.RTT, liveRTT)
	}
}

// A closed session is not the data plane. Falling back to the collapsed
// fields is correct here; using a dead session's view would be worse than
// using a stale one.
func TestAClosedSessionDoesNotOverrideThePeerView(t *testing.T) {
	m, _ := reporterFixture(aether.ProtoWebSocket, 12*time.Millisecond, true)

	conns := NewConnectionReporter(m).ActiveConnections()

	if len(conns) != 1 {
		t.Fatalf("ActiveConnections returned %d entries, want 1 — a peer with a "+
			"CLOSED session is still a connected peer and must be reported",
			len(conns))
	}
	if conns[0].Transport != ProtoNoiseUDP.String() {
		t.Fatalf("Transport = %q, want the collapsed %q — a CLOSED session was "+
			"used as the live view", conns[0].Transport, ProtoNoiseUDP.String())
	}
}

// Only PeerConnected peers appear. A peer that is dialling or backing off is
// not a connection, and counting it inflates every topology and capacity
// figure that reads this.
func TestOnlyConnectedPeersAreReported(t *testing.T) {
	m, p := reporterFixture(aether.ProtoWebSocket, 0, false)
	r := NewConnectionReporter(m)
	if len(r.ActiveConnections()) != 1 || r.ConnectedPeerCount() != 1 {
		t.Fatal("premise wrong: the connected peer is not being reported")
	}

	for _, st := range []PeerState{PeerDiscovered, PeerConnecting, PeerReconnecting, PeerDisconnected} {
		p.state = st
		if got := r.ActiveConnections(); len(got) != 0 {
			t.Fatalf("state %v produced %d ActiveConnections, want 0", st, len(got))
		}
		if got := r.ConnectedPeerCount(); got != 0 {
			t.Fatalf("state %v produced ConnectedPeerCount %d, want 0", st, got)
		}
		if _, ok := r.ConnectionTo(testNodeIDB); ok {
			t.Fatalf("ConnectionTo reported a %v peer as a live connection", st)
		}
	}
}

// ConnectionTo is the single-peer path and duplicates ActiveConnections'
// logic rather than sharing it — so it needs its own assertions, not a
// spot-check. topology_router.go:77 routes on the Grade it returns.
func TestConnectionToMatchesTheActiveConnectionsViewAndMissesCleanly(t *testing.T) {
	const liveRTT = 7 * time.Millisecond
	m, _ := reporterFixture(aether.ProtoWebSocket, liveRTT, false)
	r := NewConnectionReporter(m)

	ci, ok := r.ConnectionTo(testNodeIDB)
	if !ok {
		t.Fatal("ConnectionTo did not find a connected peer")
	}
	if ci.Transport != ProtoWebSocket.String() || ci.Grade != GradeForProtocol(ProtoWebSocket) || ci.RTT != liveRTT {
		t.Fatalf("ConnectionTo view = {%s %v %v}, want {%s %v %v} — the "+
			"single-peer path does not apply the live-session override that "+
			"ActiveConnections does, so the router and the topology view "+
			"disagree about the same peer",
			ci.Transport, ci.Grade, ci.RTT,
			ProtoWebSocket.String(), GradeForProtocol(ProtoWebSocket), liveRTT)
	}

	// The two paths must agree, since consumers mix them freely.
	active := r.ActiveConnections()[0]
	if active.Transport != ci.Transport || active.Grade != ci.Grade || active.RTT != ci.RTT {
		t.Fatalf("ActiveConnections %v disagrees with ConnectionTo %v for the "+
			"same peer at the same instant", active, ci)
	}

	if _, ok := r.ConnectionTo("never-seen-node"); ok {
		t.Fatal("ConnectionTo returned ok for a peer that is not in the table")
	}
}

// Purpose is what the monitoring view labels "gossip" vs "gossip+dispatch".
// It is cosmetic, and it is also the field that misled dashboards into
// thinking dispatch was not wired (see the isMuxed note in
// AcceptMeshConnection).
func TestPurposeTracksIsMuxed(t *testing.T) {
	m, p := reporterFixture(aether.ProtoWebSocket, 0, false)
	r := NewConnectionReporter(m)

	if got := r.ActiveConnections()[0].Purpose; got != PurposeGossipDispatch {
		t.Fatalf("Purpose = %v for a muxed peer, want %v", got, PurposeGossipDispatch)
	}

	p.isMuxed = false
	if got := r.ActiveConnections()[0].Purpose; got != PurposeGossip {
		t.Fatalf("Purpose = %v for a non-muxed peer, want %v", got, PurposeGossip)
	}
}

// A nil meshSessions map is the pre-first-session state and must not panic —
// the reporter is called from monitoring endpoints that can hit a node at any
// point in its lifecycle.
func TestReporterToleratesNoSessionsAtAll(t *testing.T) {
	p := &peerConn{nodeID: testNodeIDB, state: PeerConnected, protocol: ProtoGRPC}
	m := &ConnectionManager{peers: map[string]*peerConn{testNodeIDB: p}} // meshSessions nil
	r := NewConnectionReporter(m)

	conns := r.ActiveConnections()
	if len(conns) != 1 {
		t.Fatalf("ActiveConnections returned %d with no sessions registered, "+
			"want 1 — a peer known to the table is still reportable", len(conns))
	}
	if conns[0].Transport != ProtoGRPC.String() {
		t.Fatalf("Transport = %q, want the collapsed %q", conns[0].Transport, ProtoGRPC.String())
	}
	if _, ok := r.ConnectionTo(testNodeIDB); !ok {
		t.Fatal("ConnectionTo failed with a nil session map")
	}
}
