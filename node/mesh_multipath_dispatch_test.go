/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"testing"
	"time"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/multipath"
	"github.com/ORBTR/aether/quality"
)

// Covers GetMeshSession's MULTIPATH branch, the half where the dial decision
// is made.
//
// Structure under test:
//   1. PickByQuality(OpDefault) — return it if alive.
//   2. If that returned a CLOSED session or none, walk AllSessions() for ANY
//      open one. The code's own words: "preserves dispatch availability when
//      the score-driven primary briefly points at a zombie."
//   3. Only then fall through to the single-session dispatch map.
//
// 🔑 With no quality config attached, PickByQuality documents its own
// fallback to legacy primary selection — which makes a bare NewManager() a
// DETERMINISTIC fixture rather than one at the mercy of score inputs.

// withMultipath installs a manager for nodeID holding the given sessions, the
// first of which becomes the legacy primary.
func withMultipath(t *testing.T, m *ConnectionManager, nodeID string, sessions ...aether.Session) *multipath.Manager {
	t.Helper()
	mgr := multipath.NewManager()
	for i, s := range sessions {
		mgr.AddPath(s, aether.ProtoNoise, 100-i)
	}
	m.multipathMu.Lock()
	if m.multipathManagers == nil {
		m.multipathManagers = make(map[string]*multipath.Manager)
	}
	m.multipathManagers[nodeID] = mgr
	m.multipathMu.Unlock()
	return mgr
}

func TestMultipathQualityPickWinsWhenAlive(t *testing.T) {
	m := registerTestManager()
	primary, sibling := noiseSession(), wsSession()
	withMultipath(t, m, testNodeIDB, primary, sibling)

	// PREMISE: nothing is in the single-session map, so a hit can only come
	// from the multipath branch.
	m.dispatchMu.Lock()
	_, inMap := m.meshSessions[testNodeIDB]
	m.dispatchMu.Unlock()
	if inMap {
		t.Fatal("premise wrong: the peer is in the single-session map, so this " +
			"test would pass without the multipath branch running at all")
	}

	got, ok := m.GetMeshSession(testNodeIDB)
	if !ok {
		t.Fatal("no session returned although a live multipath path exists — " +
			"dispatch reports the peer unreachable while a healthy path is open")
	}
	if got != aether.Session(primary) {
		t.Fatalf("got the %v path, want the primary — the quality pick is not "+
			"selecting the primary path", got.Protocol())
	}
}

// 🔴 THE AVAILABILITY GUARANTEE, IN THE CODE'S OWN WORDS: when the
// score-driven pick points at a zombie, the all-paths walk must still find an
// open sibling.
//
// Without it, a peer with a perfectly healthy second path is reported
// unreachable because the FIRST one died — and nothing errors; RPCs simply
// stop being dispatched.
func TestMultipathFallsBackToAnyOpenPathWhenThePickIsAZombie(t *testing.T) {
	m := registerTestManager()
	zombie, alive := noiseSession(), wsSession()
	zombie.closed = true // the score-driven primary died
	withMultipath(t, m, testNodeIDB, zombie, alive)

	got, ok := m.GetMeshSession(testNodeIDB)
	if !ok {
		t.Fatal("dispatch reported the peer unreachable although an OPEN " +
			"multipath sibling exists — the zombie-primary fallback did not run, " +
			"and a healthy transport is being wasted with no error anywhere")
	}
	if got != aether.Session(alive) {
		t.Fatalf("got %v, want the OPEN sibling — dispatch selected a closed "+
			"session", got.Protocol())
	}
	if got.IsClosed() {
		t.Fatal("dispatch handed out a CLOSED session from the multipath walk")
	}
}

// When every multipath path is closed, the branch must not claim a session:
// control falls through to the single-session map, which then applies its own
// closed/absent guards.
func TestMultipathAllClosedFallsThroughToTheDispatchMap(t *testing.T) {
	m := registerTestManager()
	d1, d2 := noiseSession(), wsSession()
	d1.closed, d2.closed = true, true
	withMultipath(t, m, testNodeIDB, d1, d2)

	// Nothing in the single-session map either → the honest answer is false.
	if got, ok := m.GetMeshSession(testNodeIDB); ok || got != nil {
		t.Fatalf("got (%v, %v) with every path closed and no map entry, want "+
			"(nil, false) — dispatch would write to a dead transport", got, ok)
	}

	// Now give the map a LIVE entry: the fall-through must find it.
	live := wsSession()
	m.dispatchMu.Lock()
	m.meshSessions = map[string]aether.Session{testNodeIDB: live}
	m.dispatchMu.Unlock()

	got, ok := m.GetMeshSession(testNodeIDB)
	if !ok || got != aether.Session(live) {
		t.Fatalf("got (%v, %v) — with all multipath paths dead the lookup must "+
			"fall through to the single-session map, or a peer that still has a "+
			"classic session is reported unreachable", got, ok)
	}
}

// The op-specific lookup family: four variants of one shape, consulting
// multipath first and DELEGATING to GetMeshSession when the manager has
// nothing alive to offer. Each variant's documented fallback is driven below.

func TestGetMeshSessionForBytesFallsBackWhenThePickIsClosed(t *testing.T) {
	m := registerTestManager()
	zombie := noiseSession()
	zombie.closed = true
	withMultipath(t, m, testNodeIDB, zombie)

	// The single-session map holds a LIVE session; the frame-size pick is dead.
	live := wsSession()
	m.dispatchMu.Lock()
	m.meshSessions = map[string]aether.Session{testNodeIDB: live}
	m.dispatchMu.Unlock()

	got, ok := m.GetMeshSessionForBytes(testNodeIDB, 1200)
	if !ok || got != aether.Session(live) {
		t.Fatalf("got (%v, %v) — a CLOSED frame-size pick must fall back to "+
			"GetMeshSession, or a large frame is dispatched to a dead path "+
			"while a live one exists", got, ok)
	}
}

func TestGetMeshSessionForOpFallsBackWhenThePickIsClosed(t *testing.T) {
	m := registerTestManager()
	zombie := noiseSession()
	zombie.closed = true
	withMultipath(t, m, testNodeIDB, zombie)

	live := wsSession()
	m.dispatchMu.Lock()
	m.meshSessions = map[string]aether.Session{testNodeIDB: live}
	m.dispatchMu.Unlock()

	// OpDefault: with no quality config the manager uses legacy primary
	// selection, which here is the zombie — so the fallback must engage.
	got, ok := m.GetMeshSessionForOp(testNodeIDB, quality.OpDefault)
	if !ok || got != aether.Session(live) {
		t.Fatalf("got (%v, %v) — a CLOSED op-class pick must fall back, or "+
			"latency-sensitive and bulk traffic are both routed to a dead path",
			got, ok)
	}
}

// With NO multipath manager both variants must behave exactly like
// GetMeshSession — the single-path peer case, which is most peers.
func TestOpSpecificLookupsDelegateWhenNoMultipathManagerExists(t *testing.T) {
	m := registerTestManager()
	live := wsSession()
	m.dispatchMu.Lock()
	m.meshSessions = map[string]aether.Session{testNodeIDB: live}
	m.dispatchMu.Unlock()

	if _, ok := m.GetMultipathManager(testNodeIDB); ok {
		t.Fatal("premise wrong: this case needs NO multipath manager")
	}
	if got, ok := m.GetMeshSessionForBytes(testNodeIDB, 64); !ok || got != aether.Session(live) {
		t.Fatalf("ForBytes did not delegate: (%v, %v)", got, ok)
	}
	if got, ok := m.GetMeshSessionForOp(testNodeIDB, quality.OpDefault); !ok || got != aether.Session(live) {
		t.Fatalf("ForOp did not delegate: (%v, %v)", got, ok)
	}
}

// GetAllMeshSessions is used by REALTIME-class senders to duplicate a frame
// across paths. Its documented contract: nil for a single-path peer.
func TestGetAllMeshSessionsIsNilWithoutMultipathAndListsPathsWithIt(t *testing.T) {
	m := registerTestManager()
	if got := m.GetAllMeshSessions(testNodeIDB); got != nil {
		t.Fatalf("got %d sessions for a peer with no multipath manager, want nil "+
			"— a realtime sender would duplicate onto a path list it does not have",
			len(got))
	}
	a, b := noiseSession(), wsSession()
	withMultipath(t, m, testNodeIDB, a, b)
	if got := m.GetAllMeshSessions(testNodeIDB); len(got) != 2 {
		t.Fatalf("got %d sessions, want 2 — realtime duplication would cover "+
			"fewer paths than exist", len(got))
	}
}

// 🔑 GetAnyMeshSession picks the BEST-GRADE open session across ALL peers.
// Two properties, and the second is the fail-closed one.
func TestGetAnyMeshSessionPrefersTheHighestGradeAndSkipsClosed(t *testing.T) {
	m := registerTestManager()

	lowGrade := wsSession()     // Grade C
	highGrade := noiseSession() // Grade A
	m.dispatchMu.Lock()
	m.meshSessions = map[string]aether.Session{
		testNodeIDA: lowGrade,
		testNodeIDB: highGrade,
	}
	m.dispatchMu.Unlock()

	got, ok := m.GetAnyMeshSession()
	if !ok || got != aether.Session(highGrade) {
		t.Fatalf("got (%v, %v) — the highest-grade open session must win, or a "+
			"bootstrap picks a WebSocket while a noise-UDP path is open",
			got, ok)
	}

	// Close the best one: the lower grade must be selected rather than nothing.
	highGrade.closed = true
	got2, ok2 := m.GetAnyMeshSession()
	if !ok2 || got2 != aether.Session(lowGrade) {
		t.Fatalf("got (%v, %v) after closing the best path — a CLOSED session "+
			"is being preferred over a live lower-grade one", got2, ok2)
	}

	// Close everything: the honest answer is false, not a dead session.
	lowGrade.closed = true
	if got3, ok3 := m.GetAnyMeshSession(); ok3 || got3 != nil {
		t.Fatalf("got (%v, %v) with every session closed, want (nil, false)",
			got3, ok3)
	}
}

// The multipath registry's WRITE side, pairing with the read side above.
//
// 🛑 removeMultipathSession GUARDS A NAMED INCIDENT, quoted from its own
// comment: without stopping the manager before dropping it, "the goroutine
// survives and keeps calling DialFunc forever — every successive accept then
// creates a new manager with its own goroutine, and the orphaned ones keep
// dialing … The result is a runaway dial storm where every
// disconnect/reconnect cycle adds another orphan loop."
//
// ⇒ The observable guard is that the manager is DELETED when its last path
// goes. A manager left in the map is the orphan.

func TestAddMultipathSessionCreatesOneManagerPerPeer(t *testing.T) {
	m := registerTestManager()
	a, b := noiseSession(), wsSession()

	m.addMultipathSession(testNodeIDB, a, ProtoNoiseUDP)
	mgr1, ok := m.GetMultipathManager(testNodeIDB)
	if !ok || mgr1 == nil {
		t.Fatal("no manager after the first path was added")
	}

	m.addMultipathSession(testNodeIDB, b, ProtoWebSocket)
	mgr2, ok2 := m.GetMultipathManager(testNodeIDB)
	if !ok2 || mgr2 != mgr1 {
		t.Fatal("the second path created a SECOND manager for the same peer — " +
			"each manager runs its own probe/EnsureK goroutine, so a per-path " +
			"manager is a per-path goroutine leak")
	}
	if n := mgr1.PathCount(); n != 2 {
		t.Fatalf("PathCount = %d, want 2 — the second path did not join the "+
			"existing manager", n)
	}
}

func TestRemovingTheLastPathStopsAndDropsTheManager(t *testing.T) {
	m := registerTestManager()
	a, b := noiseSession(), wsSession()
	m.addMultipathSession(testNodeIDB, a, ProtoNoiseUDP)
	m.addMultipathSession(testNodeIDB, b, ProtoWebSocket)

	// PREMISE: two paths, one manager.
	mgr, _ := m.GetMultipathManager(testNodeIDB)
	if mgr == nil || mgr.PathCount() != 2 {
		t.Fatal("premise wrong: fixture does not hold two paths in one manager")
	}

	// Removing ONE path must keep the manager — the peer is still reachable.
	m.removeMultipathSession(testNodeIDB, a)
	if _, ok := m.GetMultipathManager(testNodeIDB); !ok {
		t.Fatal("removing one of two paths dropped the manager — the surviving " +
			"path loses its multipath home while its session is still live")
	}

	// Removing the LAST path must stop and DELETE it.
	m.removeMultipathSession(testNodeIDB, b)
	if _, ok := m.GetMultipathManager(testNodeIDB); ok {
		t.Fatal("the manager SURVIVED its last path — this is the documented " +
			"orphan: its EnsureK goroutine keeps calling DialFunc forever, and " +
			"every reconnect adds another one, producing a runaway dial storm")
	}
}

// The remove path is called from session cleanup, which runs for peers that
// may never have had a manager at all.
func TestRemoveMultipathSessionIsSafeWithoutAManager(t *testing.T) {
	m := registerTestManager()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("removing from a peer with no manager panicked: %v — the "+
				"cleanup path calls this unconditionally", r)
		}
	}()
	m.removeMultipathSession(testNodeIDB, wsSession()) // nil map
	m.addMultipathSession(testNodeIDA, noiseSession(), ProtoNoiseUDP)
	m.removeMultipathSession(testNodeIDB, wsSession()) // populated map, absent peer

	if _, ok := m.GetMultipathManager(testNodeIDA); !ok {
		t.Fatal("removing an UNKNOWN peer's path dropped a DIFFERENT peer's manager")
	}
}

// MeshSessionCount is a health/telemetry read: it must count only sessions
// that could actually carry traffic.
func TestMeshSessionCountCountsOnlyOpenSessions(t *testing.T) {
	m := registerTestManager()
	if got := m.MeshSessionCount(); got != 0 {
		t.Fatalf("count on a fresh manager = %d, want 0", got)
	}
	open1, open2, dead := wsSession(), noiseSession(), wsSession()
	dead.closed = true
	m.dispatchMu.Lock()
	m.meshSessions = map[string]aether.Session{"a": open1, "b": open2, "c": dead}
	m.dispatchMu.Unlock()

	if got := m.MeshSessionCount(); got != 2 {
		t.Fatalf("count = %d, want 2 — a CLOSED session is being counted as a "+
			"live peer, so health reporting overstates connectivity", got)
	}
}

// The multipath telemetry surfaces.

// RecordMultipathStats feeds the quality scores that drive PickByQuality.
// Its guard is that a peer with NO manager is a silent no-op — the stats
// path runs from gossip for every peer, most of which are single-path.
func TestRecordMultipathStatsIsANoOpWithoutAManager(t *testing.T) {
	m := registerTestManager()
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("recording stats for a peer with no multipath manager "+
				"panicked: %v — this runs from the gossip path for EVERY peer, "+
				"so it would take the node down on the first single-path peer", r)
		}
	}()
	m.RecordMultipathStats(testNodeIDB, wsSession(), 12*time.Millisecond, 0.01) // nil map
	m.addMultipathSession(testNodeIDA, noiseSession(), ProtoNoiseUDP)
	m.RecordMultipathStats(testNodeIDB, wsSession(), 12*time.Millisecond, 0.01) // absent peer

	// And with a real manager it must reach the path without error.
	sess := noiseSession()
	m.addMultipathSession(testNodeIDB, sess, ProtoNoiseUDP)
	m.RecordMultipathStats(testNodeIDB, sess, 5*time.Millisecond, 0.0)
	if _, ok := m.GetMultipathManager(testNodeIDB); !ok {
		t.Fatal("recording stats destroyed the manager")
	}
}

// SessionMetrics is the health/telemetry projection. Its one real rule is the
// same as MeshSessionCount's: a CLOSED session must not appear, or operators
// see connectivity that does not exist.
func TestSessionMetricsExcludesClosedSessions(t *testing.T) {
	m := registerTestManager()
	if got := m.SessionMetrics(); got != nil {
		t.Fatalf("metrics on a fresh manager = %v, want nil", got)
	}

	open, dead := noiseSession(), wsSession()
	dead.closed = true
	m.dispatchMu.Lock()
	m.meshSessions = map[string]aether.Session{testNodeIDA: open, testNodeIDB: dead}
	m.dispatchMu.Unlock()

	got := m.SessionMetrics()
	if len(got) != 1 {
		t.Fatalf("got %d metric rows, want 1 — a CLOSED session is being "+
			"reported as a live path and an operator reading this dashboard "+
			"sees connectivity that does not exist: %+v", len(got), got)
	}
	if got[0].NodeID != testNodeIDA {
		t.Fatalf("the surviving row is for %s, want the OPEN session's peer",
			got[0].NodeID)
	}
	if !got[0].IsAether || got[0].Protocol != "noise" {
		t.Fatalf("row shape wrong: IsAether=%v Protocol=%q", got[0].IsAether, got[0].Protocol)
	}
}
