/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/relay"
)

// Covers SessionHealthMonitor, which runtime.go constructs and wires on every
// node — Start, Stop, Register and Unregister all have live callers there. Its
// job is to CLOSE sessions: a wrong verdict tears down a working peer, and a
// missed one leaks a dead session.
//
// `Touch` is the exception, and it has no callers anywhere. It is the only
// thing that moves a session's list position on ACTIVITY, so the list is
// ordered by registration rather than by use.

// healthSession is a probeSession that also reports health, so checkSession
// gets past its `aether.HealthReporter` type assertion. Every field is
// settable because each one selects a different eviction verdict.
type healthSession struct {
	nodeID       aether.NodeID
	closeCalls   atomic.Int32
	lastActivity time.Time
	alive        bool
	missedPings  int
	lastPong     time.Time
	isClosed     bool

	pingErr   error
	pingCalls atomic.Int32
}

// aether.Connection — everything except Close and RemoteNodeID is inert; the
// monitor only ever closes a session and asks who it belongs to.
func (s *healthSession) Send(context.Context, []byte) error      { return nil }
func (s *healthSession) Receive(context.Context) ([]byte, error) { return nil, nil }
func (s *healthSession) Close() error                            { s.closeCalls.Add(1); return nil }
func (s *healthSession) RemoteAddr() net.Addr                    { return nil }
func (s *healthSession) RemoteNodeID() aether.NodeID             { return s.nodeID }
func (s *healthSession) NetConn() net.Conn                       { return nil }
func (s *healthSession) Protocol() aether.Protocol               { return aether.ProtoWebSocket }
func (s *healthSession) OnClose(func())                          {}

var _ aether.Connection = (*healthSession)(nil)

// aether.HealthReporter + aether.Pingable — the two optional interfaces
// checkSession type-asserts.
func (s *healthSession) LastActivity() time.Time             { return s.lastActivity }
func (s *healthSession) RTT() (time.Duration, time.Duration) { return 0, 0 }
func (s *healthSession) IsAlive(time.Duration) bool          { return s.alive }
func (s *healthSession) MissedPings() int                    { return s.missedPings }
func (s *healthSession) LastPongReceived() time.Time         { return s.lastPong }
func (s *healthSession) IsClosed() bool                      { return s.isClosed }

func (s *healthSession) SendPing() (uint32, error) {
	s.pingCalls.Add(1)
	if s.pingErr != nil {
		return 0, s.pingErr
	}
	return 1, nil
}
func (s *healthSession) IncrementMissedPings() int { s.missedPings++; return s.missedPings }

// healthy returns a session that must never be evicted: recently active,
// alive, no missed pings, pong just received.
func healthy(id aether.NodeID) *healthSession {
	now := time.Now()
	return &healthSession{nodeID: id, lastActivity: now, alive: true, lastPong: now}
}

func monitorForTest(t *testing.T, cfg relay.HealthConfig) (*SessionHealthMonitor, *evictLog) {
	t.Helper()
	m := NewSessionHealthMonitor(cfg)
	ev := &evictLog{}
	m.SetEvictCallback(ev.record)
	return m, ev
}

type evictLog struct {
	mu   sync.Mutex
	seen []string // "<nodeID>|<reason>"
}

func (e *evictLog) record(nodeID aether.NodeID, reason string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.seen = append(e.seen, string(nodeID)+"|"+reason)
}

func (e *evictLog) all() []string {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]string(nil), e.seen...)
}

// waitFor polls until cond holds or the deadline passes. Evictions close the
// session and fire the callback from a goroutine, so the observation is
// necessarily asynchronous.
func waitForSession(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal(msg)
}

// ── Capacity eviction decides on measured activity ──────────────────────────

// Eviction reads the session's own LastActivity(), the same source checkSession
// trusts, so the monitor holds one definition of "recently active". It carries
// a "cannot say" discriminator so zero-valued reporters are not sorted as
// ancient.
//
// The list itself cannot supply that ordering. `Touch` is what would turn
// registration order into recency-of-use order and it has no callers, while
// `entry.lastUsed` is written in three places and read only by a debug log
// line. The sole movers of an element to the front are PushFront on a new
// session and Register's MoveToFront on a RE-registration, so list order is
// registration order — and taking Back() from it selects the
// OLDEST-ESTABLISHED session, usually the most stable one under capacity
// pressure.
//
// This test pins that real activity decides and the vestigial Touch/lastUsed
// path does not influence the outcome. MaxSessions defaults to 1000 via
// relay.DefaultHealthConfig, so the path is armed on every node and fires only
// under real session pressure.
//
// Touch and lastUsed are consequently unreachable but are deliberately left in
// place, so their removal is a decision made in the open rather than a silent
// deletion.
func TestEvictionFollowsMeasuredActivityNotTouchOrRegistration(t *testing.T) {
	// Touch moves the list element, but the list no longer decides. The session
	// with the OLDEST measured activity goes, whatever Touch did.
	m, ev := monitorForTest(t, relay.HealthConfig{MaxSessions: 2, PingInterval: time.Hour})

	first, second := staleAt("node-first", 2*time.Hour), staleAt("node-second", time.Minute)
	m.Register(first)
	m.Register(second)
	m.Touch("node-first") // front of the list — and irrelevant to the decision

	m.Register(healthy("node-third"))

	if got := evictedOne(t, ev); got != "node-first"+evictReason {
		t.Fatalf("evicted %q, want node-first — it is two hours idle by MEASURED "+
			"activity while node-second is one minute idle. If Touch's list "+
			"reordering still decides the victim, eviction is following "+
			"registration order again", got)
	}

	// The inverse direction: real activity with NO Touch must protect the
	// active session rather than condemn it.
	m2, ev2 := monitorForTest(t, relay.HealthConfig{MaxSessions: 2, PingInterval: time.Hour})
	a, b := staleAt("node-a", time.Hour), staleAt("node-b", time.Hour)
	m2.Register(a)
	m2.Register(b)
	// Data flowing on `a` — and nothing calls Touch, exactly as in production.
	a.lastActivity = time.Now()
	a.lastPong = time.Now()

	m2.Register(healthy("node-c"))

	if got := evictedOne(t, ev2); got != "node-b"+evictReason {
		t.Fatalf("evicted %q, want node-b — node-a is ACTIVE and was registered "+
			"first, so evicting it means the busiest session was closed because "+
			"the list is ordered by registration", got)
	}
}

// ── Registration and capacity ───────────────────────────────────────────────

func TestRegisteringTheSameNodeTwiceReplacesRatherThanDuplicates(t *testing.T) {
	m, ev := monitorForTest(t, relay.HealthConfig{MaxSessions: 2, PingInterval: time.Hour})

	m.Register(healthy("node-a"))
	m.Register(healthy("node-a")) // reconnect: same node, new session

	if got := m.ActiveCount(); got != 1 {
		t.Fatalf("ActiveCount = %d after re-registering one node, want 1 — "+
			"duplicate entries consume capacity slots and the second one can "+
			"never be unregistered by node ID", got)
	}
	// 🔑 ActiveCount reads the MAP; the capacity check reads the LIST. A
	// re-registration that appends instead of moving leaves an orphaned list
	// element the map cannot see, so ActiveCount alone cannot detect it — the
	// node still counts once in the map and twice against MaxSessions.
	m.mu.RLock()
	listLen := m.lruList.Len()
	m.mu.RUnlock()
	if listLen != m.ActiveCount() {
		t.Fatalf("LRU list holds %d elements but the map holds %d — a "+
			"re-registration orphaned a list element, and capacity is now "+
			"enforced against a count that includes sessions nothing can "+
			"reach or unregister", listLen, m.ActiveCount())
	}
	if got := ev.all(); len(got) != 0 {
		t.Fatalf("re-registration evicted something (%v) — a peer reconnecting "+
			"must not cost another peer its session", got)
	}
}

// Capacity is unbounded when MaxSessions is 0, and the guard is `> 0` rather
// than a truthiness test — an unset config must not mean "evict everything".
func TestZeroMaxSessionsMeansUnlimited(t *testing.T) {
	m, ev := monitorForTest(t, relay.HealthConfig{MaxSessions: 0, PingInterval: time.Hour})

	for i := 0; i < 8; i++ {
		m.Register(healthy(aether.NodeID("node-" + string(rune('a'+i)))))
	}
	if got := m.ActiveCount(); got != 8 {
		t.Fatalf("ActiveCount = %d with MaxSessions=0, want 8 — an unset limit "+
			"is being read as a limit of zero and every session is evicted", got)
	}
	if got := ev.all(); len(got) != 0 {
		t.Fatalf("evictions fired with no configured limit: %v", got)
	}
}

func TestUnregisterRemovesTheSessionWithoutEvicting(t *testing.T) {
	m, ev := monitorForTest(t, relay.HealthConfig{MaxSessions: 4, PingInterval: time.Hour})
	s := healthy("node-a")
	m.Register(s)

	m.Unregister("node-a")

	if got := m.ActiveCount(); got != 0 {
		t.Fatalf("ActiveCount = %d after Unregister, want 0", got)
	}
	// Any close would be launched in a goroutine, so give it a window to
	// appear rather than sampling once and racing it.
	time.Sleep(20 * time.Millisecond)
	if got := s.closeCalls.Load(); got != 0 {
		t.Fatalf("Unregister closed the session %d times — it is the "+
			"bookkeeping half of a teardown the caller has already done, and "+
			"closing again races the caller's own cleanup", got)
	}
	if got := ev.all(); len(got) != 0 {
		t.Fatalf("Unregister fired the evict callback (%v) — a deliberate "+
			"removal is not an eviction and would be reported as peer loss", got)
	}
	// Unregistering an unknown node is a no-op, not a panic.
	m.Unregister("never-registered")
}

// ── Health verdicts ─────────────────────────────────────────────────────────

// Each verdict isolated, so a failure names which rule broke. checkSession
// returns the eviction reason, or "" to keep the session.
func TestCheckSessionVerdicts(t *testing.T) {
	cfg := relay.HealthConfig{IdleTimeout: time.Minute, MaxMissedPings: 3, PingInterval: time.Minute}
	m := NewSessionHealthMonitor(cfg)

	t.Run("healthy session is kept", func(t *testing.T) {
		if got := m.checkSession(healthy("n")); got != "" {
			t.Fatalf("a healthy session was marked for eviction: %q", got)
		}
	})

	t.Run("closed session is evicted", func(t *testing.T) {
		s := healthy("n")
		s.isClosed = true
		if got := m.checkSession(s); got != "session closed" {
			t.Fatalf("verdict = %q, want %q", got, "session closed")
		}
	})

	// 🔴 MESH-G01, PINNED. IsAlive()==false means EITHER a genuine idle
	// timeout OR a transport that cannot report activity at all. A
	// BaseConnection over a raw net.Conn returns false + a ZERO LastActivity,
	// so the unconditional check evicted every healthy WebSocket/gRPC relay
	// session on each ping interval. The zero-LastActivity guard is what
	// distinguishes "idle" from "cannot say".
	t.Run("not alive but zero LastActivity is NOT an idle timeout", func(t *testing.T) {
		s := healthy("n")
		s.alive = false
		s.lastActivity = time.Time{} // transport cannot report activity
		if got := m.checkSession(s); got == "idle timeout exceeded" {
			t.Fatal("a session whose transport cannot report activity was " +
				"evicted as idle — this is MESH-G01, and it churned every " +
				"WebSocket and gRPC relay session once per ping interval")
		}
	})

	t.Run("not alive with real activity IS an idle timeout", func(t *testing.T) {
		s := healthy("n")
		s.alive = false
		s.lastActivity = time.Now().Add(-time.Hour) // it CAN report, and it is stale
		if got := m.checkSession(s); got != "idle timeout exceeded" {
			t.Fatalf("verdict = %q, want an idle timeout — a genuinely idle "+
				"session is now never reclaimed", got)
		}
	})

	t.Run("missed pings at the limit evict", func(t *testing.T) {
		s := healthy("n")
		s.missedPings = 3 // == MaxMissedPings
		if got := m.checkSession(s); got != "max missed pings exceeded" {
			t.Fatalf("verdict = %q at exactly MaxMissedPings, want eviction — "+
				"an off-by-one here keeps a dead session forever", got)
		}
	})

	t.Run("one below the limit does not evict", func(t *testing.T) {
		s := healthy("n")
		s.missedPings = 2
		if got := m.checkSession(s); got != "" {
			t.Fatalf("verdict = %q one ping below the limit, want kept", got)
		}
	})

	t.Run("a session that cannot report health at all is skipped", func(t *testing.T) {
		// plainSession implements aether.Connection and nothing else.
		if got := m.checkSession(&plainSession{}); got != "" {
			t.Fatalf("verdict = %q for a session with no health reporting — "+
				"absence of health data is not evidence of ill health", got)
		}
	})
}

// An overdue pong triggers a ping, and a ping that FAILS counts toward the
// missed-ping budget rather than being swallowed.
func TestOverduePongSendsAPingAndFailuresCountTowardTheBudget(t *testing.T) {
	cfg := relay.HealthConfig{IdleTimeout: time.Hour, MaxMissedPings: 3, PingInterval: time.Millisecond}
	m := NewSessionHealthMonitor(cfg)

	s := healthy("n")
	s.lastPong = time.Now().Add(-time.Hour) // long overdue
	if got := m.checkSession(s); got != "" {
		t.Fatalf("verdict = %q, want kept — one overdue pong is a reason to "+
			"ping, not to evict", got)
	}
	if s.pingCalls.Load() != 1 {
		t.Fatalf("SendPing called %d times for an overdue pong, want 1 — "+
			"liveness is never probed and the session dies silently",
			s.pingCalls.Load())
	}

	failing := healthy("n")
	failing.lastPong = time.Now().Add(-time.Hour)
	failing.pingErr = errors.New("transport down")
	failing.missedPings = 2 // one failure away from the limit
	if got := m.checkSession(failing); got != "ping send failures" {
		t.Fatalf("verdict = %q after a ping send failure at the limit, want "+
			"eviction — a transport that cannot even send is being kept", got)
	}
}

// ── Sweep and eviction ──────────────────────────────────────────────────────

// checkAllSessions is the sweep the monitor loop runs: it must evict the
// unhealthy and leave the healthy alone, in one pass.
func TestSweepEvictsOnlyTheUnhealthySession(t *testing.T) {
	m, ev := monitorForTest(t, relay.HealthConfig{
		IdleTimeout: time.Minute, MaxMissedPings: 3, PingInterval: time.Hour, MaxSessions: 8,
	})

	good := healthy("node-good")
	bad := healthy("node-bad")
	bad.isClosed = true
	m.Register(good)
	m.Register(bad)

	m.checkAllSessions()

	waitForSession(t, func() bool { return m.ActiveCount() == 1 },
		"the sweep did not remove the closed session — dead sessions accumulate "+
			"and consume capacity slots against MaxSessions")

	got := ev.all()
	if len(got) != 1 || got[0] != "node-bad|session closed" {
		t.Fatalf("evictions = %v, want exactly the closed session", got)
	}
	if good.closeCalls.Load() != 0 {
		t.Fatalf("the HEALTHY session was closed %d times during the sweep",
			good.closeCalls.Load())
	}
}

// evictSession must close the session and fire the callback exactly once, and
// a second eviction of the same node must be a silent no-op — the sweep and a
// capacity eviction can both target one node.
func TestEvictingTheSameSessionTwiceClosesItOnce(t *testing.T) {
	m, ev := monitorForTest(t, relay.HealthConfig{MaxSessions: 4, PingInterval: time.Hour})
	s := healthy("node-a")
	m.Register(s)

	m.evictSession("node-a", "first")
	m.evictSession("node-a", "second")

	if got := s.closeCalls.Load(); got != 1 {
		t.Fatalf("Close called %d times, want 1 — a double close races the "+
			"transport's own teardown", got)
	}
	if got := ev.all(); len(got) != 1 || got[0] != "node-a|first" {
		t.Fatalf("callbacks = %v, want exactly one for the first reason — a "+
			"second peer-loss notification for an already-gone peer triggers "+
			"another round of reconnect work", got)
	}
}

// Start/Stop must be safe and must actually run a sweep.
//
// Stop closes a channel, so without a sync.Once guard a second call panics with
// "close of closed channel". Runtime reaches Stop only inside shutdownOnce-
// guarded Shutdown, but Stop is exported and any other caller reaches it
// unguarded; the sibling LADSnapshotCache.Stop carries the same guard for the
// same reason.
func TestSessionHealthStopIsIdempotent(t *testing.T) {
	m, _ := monitorForTest(t, relay.HealthConfig{PingInterval: time.Hour})
	m.Start()
	m.Stop()
	m.Stop() // must not panic
	m.Stop()
}

func TestStartRunsSweepsAndStopHalts(t *testing.T) {
	m, ev := monitorForTest(t, relay.HealthConfig{
		PingInterval: 5 * time.Millisecond, IdleTimeout: time.Minute,
		MaxMissedPings: 3, MaxSessions: 4,
	})

	dead := healthy("node-dead")
	dead.isClosed = true
	m.Register(dead)

	m.Start()
	waitForSession(t, func() bool { return len(ev.all()) == 1 },
		"the monitor loop never swept — a closed session is never reclaimed, "+
			"and neither is any idle or unresponsive one")
	m.Stop()

	// After Stop the loop must not sweep again.
	m.Register(healthy("node-b"))
	before := len(ev.all())
	time.Sleep(30 * time.Millisecond)
	if got := len(ev.all()); got != before {
		t.Fatalf("evictions grew from %d to %d after Stop — the loop is still "+
			"running and will close sessions during shutdown", before, got)
	}
}

// plainSession implements aether.Connection and NOTHING else — no
// HealthReporter, no Pingable. It is the shape checkSession must skip rather
// than judge, and it must NOT embed healthSession: embedding would promote the
// health methods and the type assertion would succeed, making the test assert
// the opposite of what it claims.
type plainSession struct{ nodeID aether.NodeID }

func (s *plainSession) Send(context.Context, []byte) error      { return nil }
func (s *plainSession) Receive(context.Context) ([]byte, error) { return nil, nil }
func (s *plainSession) Close() error                            { return nil }
func (s *plainSession) RemoteAddr() net.Addr                    { return nil }
func (s *plainSession) RemoteNodeID() aether.NodeID             { return s.nodeID }
func (s *plainSession) NetConn() net.Conn                       { return nil }
func (s *plainSession) Protocol() aether.Protocol               { return aether.ProtoWebSocket }
func (s *plainSession) OnClose(func())                          {}

var _ aether.Connection = (*plainSession)(nil)

// The premise of the skip test, enforced at compile time in the only direction
// Go can express: if plainSession ever gains health reporting, this block stops
// compiling and the test stops being vacuous silently.
func init() {
	if _, ok := any(&plainSession{}).(aether.HealthReporter); ok {
		panic("plainSession must not implement aether.HealthReporter — the " +
			"skip-path test would assert nothing")
	}
}

// ── Capacity eviction: LastActivity, with the "cannot say" discriminator ────
//
// Eviction does not consult list order at all when any session can report its
// own activity.

const evictReason = "|LRU capacity eviction"

// staleAt returns a session that CAN report activity, last active `age` ago.
func staleAt(id aether.NodeID, age time.Duration) *healthSession {
	s := healthy(id)
	s.lastActivity = time.Now().Add(-age)
	return s
}

// cannotSay returns a session whose transport does not track activity — a
// WebSocket/gRPC relay over a raw net.Conn. Zero LastActivity means "cannot
// say", NOT "idle since 1970".
func cannotSay(id aether.NodeID) *healthSession {
	s := healthy(id)
	s.lastActivity = time.Time{}
	return s
}

// evictedOne waits for exactly one eviction and returns it. Evictions fire from
// a goroutine, so the observation is necessarily asynchronous.
func evictedOne(t *testing.T, ev *evictLog) string {
	t.Helper()
	waitForSession(t, func() bool { return len(ev.all()) >= 1 }, "no eviction fired")
	got := ev.all()
	if len(got) != 1 {
		t.Fatalf("expected exactly one eviction, got %v", got)
	}
	return got[0]
}

// 🔴 THE PROPERTY THE WHOLE CHANGE EXISTS FOR. A session that cannot report
// activity must NOT be evicted ahead of one that reports a genuinely stale
// timestamp. Sorting zero-as-oldest evicts every WS/gRPC relay first — the
// fallback transports MESH-G01's health guard was written to protect.
func TestACannotSaySessionIsNotEvictedAheadOfAGenuinelyStaleOne(t *testing.T) {
	m, ev := monitorForTest(t, relay.HealthConfig{MaxSessions: 2})

	m.Register(cannotSay("relay-ws"))           // registered FIRST => list back
	m.Register(staleAt("stale-udp", time.Hour)) // can report, and very stale
	m.Register(healthy("newcomer"))             // trips capacity eviction

	if got := evictedOne(t, ev); got != "stale-udp"+evictReason {
		t.Fatalf("evicted %q, want stale-udp — a session that CANNOT report "+
			"activity was treated as less-recently-used than one that reported an "+
			"hour of idleness. Zero LastActivity means 'cannot say', not 1970, and "+
			"this ordering churns exactly the WS/gRPC relays MESH-G01 protects",
			got)
	}
}

// Among sessions that can all report, the OLDEST goes — and registration order
// must not override that. This is what makes it a real LRU rather than a FIFO.
func TestTheOldestReportingSessionIsEvictedRegardlessOfRegistrationOrder(t *testing.T) {
	m, ev := monitorForTest(t, relay.HealthConfig{MaxSessions: 3})

	m.Register(staleAt("registered-first", time.Minute)) // oldest by REGISTRATION
	m.Register(staleAt("truly-oldest", 2*time.Hour))     // oldest by ACTIVITY
	m.Register(staleAt("recent", time.Second))
	m.Register(healthy("newcomer")) // trips eviction

	if got := evictedOne(t, ev); got != "truly-oldest"+evictReason {
		t.Fatalf("evicted %q, want truly-oldest — eviction is still following "+
			"registration order rather than measured activity, so the monitor is a "+
			"FIFO wearing an LRU's name", got)
	}
}

// When NOTHING can report activity, fall back to registration order — the old
// behaviour, which is the only sane tiebreak among equals.
func TestWithNoReportableActivityEvictionFallsBackToRegistrationOrder(t *testing.T) {
	m, ev := monitorForTest(t, relay.HealthConfig{MaxSessions: 2})

	m.Register(cannotSay("first"))
	m.Register(cannotSay("second"))
	m.Register(cannotSay("third")) // trips eviction

	if got := evictedOne(t, ev); got != "first"+evictReason {
		t.Fatalf("evicted %q, want first — with no session able to report "+
			"activity there is no basis but registration order, and the oldest "+
			"registration is the back of the list", got)
	}
}
