/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/health"
	"github.com/bbmumford/swarm"
)

// Characterisation probe for the swarm stream-100 bind, written while @P's
// production capture is still outstanding.
//
// It asserts what RegisterPeer DOES today and approves neither outcome:
//
//   - the INITIATOR (lower NodeID) calls OpenStream(100) and attaches
//     immediately and unconditionally;
//   - the RESPONDER (higher NodeID) arms AcceptStreamByID(100) with a 30s
//     timeout in a goroutine and, on expiry, logs and returns — no retry, no
//     backoff, no teardown signal to the peer, and attachPeer is never reached.
//
// That asymmetry is the reason this file exists: one side ends up writing into
// a stream the other side has permanently stopped waiting for.
//
// ⚠ THIS IS NOT THE FLEET WEDGE, AND THE ORIGINAL VERSION OF THIS COMMENT
// CLAIMED OTHERWISE. The measured root cause of the production stream-100 wedge
// is upstream of this file and needs no race: aether's noise path never sets
// FlagSYN, so handleImplicitOpen — the designed recovery for a lost stream-OPEN
// — is unreachable there, and the OPEN frame itself is sent with no SeqNo, no
// send-window registration, and no retransmit anywhere. One lost UDP datagram
// therefore strands stream 100 permanently, with no race required.
//
// What this file still pins is real and still worth a regression guard: once the
// responder's accept fails for ANY reason, nothing ever retries it. The only
// re-arm in the system is a brand-new session, and the documented second chance
// (AcceptPeer) has zero non-test callers.
//
// Read a failure here as "the bind protocol changed", and check whether the
// change added a retry (good) or removed the accept (bad).
//
// 🛑 IF TestSwarmBind_ResponderAbandonsPermanently STARTS FAILING because the
// accept is now retried, that is the intended fix — delete the abandonment
// assertion and keep the attach-count one.

// probeSession is a minimal aether.Session. Every method not exercised by
// RegisterPeer returns a zero value; the two that matter are counted.
type probeSession struct {
	local, remote aether.NodeID
	opens         atomic.Int32
	acceptByID    atomic.Int32
	acceptErr     error // when non-nil, AcceptStreamByID fails after acceptDelay
	acceptDelay   time.Duration

	// proto selects the transport this session reports, which is what
	// grade.SessionGrade derives its Grade from. Zero value keeps the
	// original ProtoNoise behaviour so existing users are unaffected;
	// set it to drive registerMeshSession's grade comparisons.
	proto aether.Protocol
	// closed makes IsClosed() settable — the register path branches on it.
	closed bool
	// closeCalls counts Close(), so a test can assert that dedup did NOT
	// tear down an existing healthy session — the "Fix B" incident.
	closeCalls atomic.Int32
}

func (s *probeSession) OpenStream(ctx context.Context, cfg aether.StreamConfig) (aether.Stream, error) {
	s.opens.Add(1)
	return nil, context.Canceled // attachPeer is not reached; we only count the call
}

func (s *probeSession) AcceptStream(ctx context.Context) (aether.Stream, error) {
	return nil, context.Canceled
}

func (s *probeSession) AcceptStreamByID(ctx context.Context, streamID uint64) (aether.Stream, error) {
	s.acceptByID.Add(1)
	if s.acceptDelay > 0 {
		select {
		case <-time.After(s.acceptDelay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return nil, s.acceptErr
}

func (s *probeSession) LocalNodeID() aether.NodeID                                { return s.local }
func (s *probeSession) RemoteNodeID() aether.NodeID                               { return s.remote }
func (s *probeSession) LocalPeerID() aether.PeerID                                { return aether.PeerID{} }
func (s *probeSession) RemotePeerID() aether.PeerID                               { return aether.PeerID{} }
func (s *probeSession) Capabilities() aether.Capabilities                         { return 0 }
func (s *probeSession) Ping(context.Context) (time.Duration, error)               { return 0, nil }
func (s *probeSession) GoAway(context.Context, aether.GoAwayReason, string) error { return nil }
func (s *probeSession) Close() error                                              { s.closeCalls.Add(1); return nil }
func (s *probeSession) IsClosed() bool                                            { return s.closed }
func (s *probeSession) Health() *health.Monitor                                   { return nil }
func (s *probeSession) SessionKey() []byte                                        { return nil }
func (s *probeSession) ConnectionID() aether.ConnectionID                         { return aether.ConnectionID{} }
func (s *probeSession) CongestionWindow() int64                                   { return 0 }
func (s *probeSession) Protocol() aether.Protocol {
	if s.proto == aether.ProtoUnknown {
		return aether.ProtoNoise
	}
	return s.proto
}
func (s *probeSession) Metrics() aether.SessionMetrics { return aether.SessionMetrics{} }

// newProbeTransport builds a MeshSwarmTransport for a node with the given swarm
// NodeID and tears down its goroutines when the test ends.
func newProbeTransport(t *testing.T, localID string) *MeshSwarmTransport {
	t.Helper()
	tr := NewMeshSwarmTransport(swarm.NodeID(localID))
	t.Cleanup(tr.Stop)
	return tr
}

// probePeerCount reads the attached-peer count under the transport's own lock.
// Reading t.peers directly races the responder's accept goroutine, which is
// exactly the goroutine these tests are observing — and -race would be right to
// flag it.
func probePeerCount(tr *MeshSwarmTransport) int {
	tr.mu.RLock()
	defer tr.mu.RUnlock()
	return len(tr.peers)
}

// TestSwarmBind_RoleIsDecidedByNodeIDOrder pins the deterministic tie-break, and
// is the control that makes the abandonment test below interpretable: if the
// role split ever stops depending on NodeID order, the other test is measuring
// something else.
func TestSwarmBind_RoleIsDecidedByNodeIDOrder(t *testing.T) {
	// selfID < peerID  => this node is the INITIATOR and must call OpenStream.
	lo := &probeSession{local: "vl1_aaa", remote: "vl1_zzz"}
	tr := newProbeTransport(t, "vl1_aaa")
	_ = tr.RegisterPeer(context.Background(), swarm.NodeID("vl1_zzz"), lo)

	if got := lo.opens.Load(); got != 1 {
		t.Errorf("lower NodeID must be the INITIATOR and call OpenStream(100) once; got %d calls", got)
	}
	if got := lo.acceptByID.Load(); got != 0 {
		t.Errorf("the initiator must NOT arm AcceptStreamByID; got %d calls", got)
	}
}

// TestSwarmBind_ResponderAbandonsPermanently is the finding.
//
// The responder's accept is armed in a goroutine with a bounded context. When it
// fails, RegisterPeer's goroutine returns and NOTHING retries — so a peer whose
// OPEN(100) arrives late, or is parked past the waiter's arming, never binds for
// the life of the session.
func TestSwarmBind_ResponderAbandonsPermanently(t *testing.T) {
	// selfID > peerID => this node is the RESPONDER.
	hi := &probeSession{local: "vl1_zzz", remote: "vl1_aaa", acceptErr: context.DeadlineExceeded}
	tr := newProbeTransport(t, "vl1_zzz")

	if err := tr.RegisterPeer(context.Background(), swarm.NodeID("vl1_aaa"), hi); err != nil {
		t.Fatalf("RegisterPeer (responder) returned an error: %v", err)
	}
	if got := hi.opens.Load(); got != 0 {
		t.Errorf("the responder must NOT call OpenStream; got %d calls", got)
	}

	// The accept is armed asynchronously — wait for it to have been attempted.
	deadline := time.Now().Add(2 * time.Second)
	for hi.acceptByID.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if hi.acceptByID.Load() == 0 {
		t.Fatal("the responder never armed AcceptStreamByID(100) at all — the bind " +
			"protocol has changed and this probe's premise needs re-measuring")
	}

	// THE FINDING: give it well past any plausible retry interval and assert the
	// count never rises. One attempt, ever.
	time.Sleep(300 * time.Millisecond)
	if got := hi.acceptByID.Load(); got != 1 {
		t.Errorf("AcceptStreamByID was attempted %d times — a retry now exists. If that "+
			"is the intended fix, this assertion should be replaced by one that "+
			"bounds the retry; if it is accidental, it needs a backoff", got)
	}

	if got := probePeerCount(tr); got != 0 {
		t.Errorf("attachPeer must NOT have run after a failed accept (got %d peers) — "+
			"if it did, the responder is attaching a nil stream", got)
	}
}
