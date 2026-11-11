/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package node

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/health"
	"github.com/ORBTR/aether/multipath"
	"github.com/ORBTR/aether/quality"
)

// keepaliveSession is the session runMeshKeepalive probes: answer decides whether the
// nth Ping (from 1) is answered, and an unanswered Ping fails at once, so a test spends
// the plan's Interval and RetryGap and none of its ProbeTimeout. Every other Session
// method is the nil embedded interface's and panics.
type keepaliveSession struct {
	aether.Session
	health *health.Monitor
	answer func(n int32) bool
	pings  atomic.Int32
}

func (s *keepaliveSession) Ping(context.Context) (time.Duration, error) {
	n := s.pings.Add(1)
	if s.answer != nil && s.answer(n) {
		return time.Millisecond, nil
	}
	return 0, context.DeadlineExceeded
}

func (s *keepaliveSession) Health() *health.Monitor   { return s.health }
func (s *keepaliveSession) IsClosed() bool            { return false }
func (s *keepaliveSession) Protocol() aether.Protocol { return aether.ProtoNoise }

// An answer after unanswered probes is recorded on the session's health monitor, so the
// ProbeLoss aether.KeepalivePlanFor reads into Strikes is the loss this loop measured,
// as aether.RunKeepalive records it.
func TestAMeshKeepaliveRecordsTheProbesLostBeforeAnAnswer(t *testing.T) {
	t.Parallel()
	s := &keepaliveSession{health: health.NewMonitor(0), answer: func(n int32) bool { return n >= 3 }}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go runMeshKeepalive(ctx, s, "vl1_keepalive_loss", nil)

	deadline := time.Now().Add(20 * time.Second)
	for s.health.ProbeLoss() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	got, pings := s.health.ProbeLoss(), s.pings.Load()
	cancel()
	if got == 0 {
		t.Fatalf("after %d probes, two of them unanswered, ProbeLoss is 0: the loss never reaches the plan", pings)
	}
	want := health.NewMonitor(0)
	want.RecordKeepaliveAnswer(2)
	if pings == 3 && got != want.ProbeLoss() {
		t.Fatalf("ProbeLoss = %v after two lost probes and an answer, want %v", got, want.ProbeLoss())
	}
}

// A silent session with no multipath manager is let go after the plan's Strikes probes
// in a row: Strikes is the one strike budget.
func TestAMeshKeepaliveLetsASilentSessionGoAfterThePlansStrikes(t *testing.T) {
	t.Parallel()
	s := &keepaliveSession{}
	strikes := aether.KeepalivePlanFor(s).Strikes
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if !runMeshKeepalive(ctx, s, "vl1_keepalive_strikes", nil) {
		t.Fatal("the keepalive ended without letting the silent session go")
	}
	if got := s.pings.Load(); got != int32(strikes) {
		t.Fatalf("the silent session was let go after %d probes, want the plan's Strikes, %d", got, strikes)
	}
}

// A session runs one keepalive: a second runMeshKeepalive on a session whose keepalive
// is running returns false at once and sends no probe.
func TestASecondMeshKeepaliveOnASessionProbesNothing(t *testing.T) {
	t.Parallel()
	s := &keepaliveSession{answer: func(int32) bool { return true }}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go runMeshKeepalive(ctx, s, "vl1_keepalive_one", nil)
	for wait := time.Now().Add(time.Second); time.Now().Before(wait); time.Sleep(5 * time.Millisecond) {
		if _, running := meshKeepalives.Load(s); running {
			break
		}
	}

	second, cancelSecond := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancelSecond()
	start := time.Now()
	letGo := runMeshKeepalive(second, s, "vl1_keepalive_one", nil)
	if took := time.Since(start); took > time.Second {
		t.Fatalf("a second keepalive ran on the session for %v", took)
	}
	if letGo {
		t.Fatal("the second keepalive let the session go")
	}
}

// aether.RunKeepalive is the keepalive of a session no node runtime runs one on, so no
// file of this package calls it: runMeshKeepalive is a mesh session's one keepalive.
func TestNoNodeFileRunsAetherRunKeepalive(t *testing.T) {
	files, err := filepath.Glob("*.go")
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	for _, name := range files {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		f, err := parser.ParseFile(fset, name, src, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(f, func(n ast.Node) bool {
			sel, ok := n.(*ast.SelectorExpr)
			if ok && sel.Sel.Name == "RunKeepalive" {
				if id, ok := sel.X.(*ast.Ident); ok && id.Name == "aether" {
					t.Errorf("%s calls aether.RunKeepalive", fset.Position(sel.Pos()))
				}
			}
			return true
		})
	}
}

// recordedPaths is a meshKeepalivePaths that plans from the session alone and records
// whether each probe it is handed was answered.
type recordedPaths struct {
	mu       sync.Mutex
	answered []bool
}

func (r *recordedPaths) KeepalivePlanFor(s aether.Session) quality.KeepalivePlan {
	return aether.KeepalivePlanFor(s)
}

func (r *recordedPaths) RecordKeepaliveProbe(_ aether.Session, _ time.Duration, err error) {
	r.mu.Lock()
	r.answered = append(r.answered, err == nil)
	r.mu.Unlock()
}

func (r *recordedPaths) results() []bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]bool(nil), r.answered...)
}

// The keepalive hands every probe's result to the peer's multipath manager, so the
// manager takes its path state from the session's one prober.
func TestAMeshKeepaliveHandsEveryProbeToThePeersPaths(t *testing.T) {
	t.Parallel()
	s := &keepaliveSession{health: health.NewMonitor(0), answer: func(n int32) bool { return n >= 2 }}
	rec := &recordedPaths{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go runMeshKeepalive(ctx, s, "vl1_keepalive_paths", func() meshKeepalivePaths { return rec })

	deadline := time.Now().Add(20 * time.Second)
	for len(rec.results()) < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	if got := rec.results(); len(got) < 2 || got[0] || !got[1] {
		t.Fatalf("the peer's paths were handed %v after %d probes, want an unanswered probe then an answered one", got, s.pings.Load())
	}
}

// seededHealth is a monitor whose estimator has measured eight 1 ms round trips, so a
// session's plan is past its warmup.
func seededHealth() *health.Monitor {
	h := health.NewMonitor(0)
	for seq := uint32(1); seq <= 8; seq++ {
		h.RecordPingSent(seq)
		h.RecordPongRecv(seq, time.Now().Add(-time.Millisecond))
	}
	return h
}

// A silent session whose peer holds a measured standby path is let go after the plan's
// with-standby Strikes, read from the peer's multipath manager, which is fewer than the
// Strikes of the same session alone.
func TestASilentMeshSessionBesideAMeasuredStandbyIsLetGoAtTheWithStandbyStrikes(t *testing.T) {
	t.Parallel()
	s := &keepaliveSession{health: seededHealth()}
	standby := &keepaliveSession{health: seededHealth(), answer: func(int32) bool { return true }}
	mgr := multipath.NewManager()
	mgr.AddPath(s, aether.ProtoNoise, 100)
	mgr.AddPath(standby, aether.ProtoNoise, 50)
	alone, with := aether.KeepalivePlanFor(s).Strikes, mgr.KeepalivePlanFor(s).Strikes
	if with >= alone {
		t.Fatalf("premise wrong: %d strikes beside the standby, %d alone", with, alone)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if !runMeshKeepalive(ctx, s, "vl1_keepalive_standby", func() meshKeepalivePaths { return mgr }) {
		t.Fatal("the keepalive ended without letting the silent session go")
	}
	if got := s.pings.Load(); got != int32(with) {
		t.Fatalf("the silent session beside a measured standby was let go after %d probes, want the with-standby Strikes, %d (alone: %d)", got, with, alone)
	}
}

// A peer's multipath manager takes its probe results from the sessions' keepalives:
// its probe rounds ping no path, so each session has one prober.
func TestAMeshPeersPathManagerPingsNoPath(t *testing.T) {
	t.Parallel()
	cm := &ConnectionManager{}
	primary := &keepaliveSession{health: health.NewMonitor(0), answer: func(int32) bool { return true }}
	standby := &keepaliveSession{health: health.NewMonitor(0), answer: func(int32) bool { return true }}
	cm.addMultipathSession("vl1_keepalive_manager", primary, ProtoNoiseUDP)
	cm.addMultipathSession("vl1_keepalive_manager", standby, ProtoNoiseUDP)
	mgr, ok := cm.GetMultipathManager("vl1_keepalive_manager")
	if !ok {
		t.Fatal("no multipath manager for the peer")
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		mgr.RunProbeLoop(ctx)
	}()
	time.Sleep(aether.KeepalivePlanFor(standby).Interval + 2*time.Second)
	cancel()
	<-done
	if p, s := primary.pings.Load(), standby.pings.Load(); p != 0 || s != 0 {
		t.Fatalf("the manager's probe rounds pinged the primary %d and the standby %d times, want none", p, s)
	}
}

// A standby going silent beside an answering primary, both sessions running their one
// keepalive with the peer's manager: the standby reads the answering primary as its own
// standby and is let go; AcceptMeshConnection's cleanup reports it (OnPathFailure), which
// leaves the primary as primary; the primary's plan then reads no standby (5 strikes),
// and the answering primary is never let go.
func TestAStandbyGoingSilentBesideAnAnsweringPrimaryIsLetGoAlone(t *testing.T) {
	t.Parallel()
	primary := &keepaliveSession{health: seededHealth(), answer: func(int32) bool { return true }}
	var silent atomic.Bool
	standby := &keepaliveSession{health: seededHealth(), answer: func(int32) bool { return !silent.Load() }}
	mgr := multipath.NewManager()
	mgr.AddPath(primary, aether.ProtoNoise, 100)
	mgr.AddPath(standby, aether.ProtoNoise, 50)
	paths := func() meshKeepalivePaths { return mgr }
	if got := mgr.KeepalivePlanFor(primary).Strikes; got != 2 {
		t.Fatalf("premise: the primary beside a measured, answering standby has %d strikes, want 2", got)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	primaryLetGo := make(chan bool, 1)
	go func() { primaryLetGo <- runMeshKeepalive(ctx, primary, "vl1_keepalive_answering_primary", paths) }()
	standbyLetGo := make(chan bool, 1)
	go func() { standbyLetGo <- runMeshKeepalive(ctx, standby, "vl1_keepalive_silent_standby", paths) }()

	deadline := time.Now().Add(30 * time.Second)
	for standby.pings.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	silent.Store(true)
	select {
	case v := <-standbyLetGo:
		if !v {
			t.Fatal("the standby's keepalive ended without letting it go")
		}
	case <-time.After(40 * time.Second):
		t.Fatal("the silent standby was never let go")
	}
	if mgr.OnPathFailure(standby) {
		t.Error("OnPathFailure for the standby reported a failover")
	}
	if p, _ := mgr.PrimarySession(); p != primary {
		t.Error("the standby's failure moved the primary")
	}
	if got := mgr.KeepalivePlanFor(primary).Strikes; got != 5 {
		t.Errorf("with its standby dead the primary's plan has %d strikes, want 5", got)
	}
	select {
	case <-primaryLetGo:
		t.Fatal("the answering primary was let go")
	case <-time.After(12 * time.Second):
	}
}

// The peer dies: the primary and its standby go silent at once, each reading the other
// as its standby. The with-standby floor holds only while the other path's latest probe
// was answered, so neither is let go after a single unanswered probe.
func TestAPrimaryAndItsStandbyGoingSilentTogetherAreEachLetGo(t *testing.T) {
	t.Parallel()
	var silent atomic.Bool
	answer := func(int32) bool { return !silent.Load() }
	primary := &keepaliveSession{health: seededHealth(), answer: answer}
	standby := &keepaliveSession{health: seededHealth(), answer: answer}
	mgr := multipath.NewManager()
	mgr.AddPath(primary, aether.ProtoNoise, 100)
	mgr.AddPath(standby, aether.ProtoNoise, 50)
	paths := func() meshKeepalivePaths { return mgr }
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	pDone := make(chan bool, 1)
	sDone := make(chan bool, 1)
	go func() { pDone <- runMeshKeepalive(ctx, primary, "vl1_keepalive_both_primary", paths) }()
	go func() { sDone <- runMeshKeepalive(ctx, standby, "vl1_keepalive_both_standby", paths) }()
	deadline := time.Now().Add(30 * time.Second)
	for (primary.pings.Load() < 1 || standby.pings.Load() < 1) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	silent.Store(true)
	p0, s0 := primary.pings.Load(), standby.pings.Load()
	for _, ch := range []chan bool{pDone, sDone} {
		select {
		case v := <-ch:
			if !v {
				t.Fatal("a keepalive ended without letting its session go")
			}
		case <-time.After(90 * time.Second):
			t.Fatal("a silent session was never let go")
		}
	}
	pu, su := primary.pings.Load()-p0, standby.pings.Load()-s0
	if pu < 2 || su < 2 {
		t.Errorf("a session was let go after a single unanswered probe (primary %d, standby %d)", pu, su)
	}
}

// standbyDiesWhilePrimarySilent runs a primary and a standby that both answer, each
// with its one keepalive and the peer's manager, then silences the primary; during the
// primary's dieAt-th unanswered probe, inside its Ping and so before its keepalive acts
// on the failure, the standby dies as AcceptMeshConnection's cleanup reports it
// (OnPathFailure). It returns how many unanswered probes the primary's keepalive took
// before letting it go.
func standbyDiesWhilePrimarySilent(t *testing.T, dieAt int32, primaryID, standbyID string) int32 {
	t.Helper()
	var silent atomic.Bool
	var silentFrom atomic.Int32
	var mgr *multipath.Manager
	var standby *keepaliveSession
	died := make(chan bool, 1)
	primary := &keepaliveSession{health: seededHealth()}
	primary.answer = func(n int32) bool {
		if !silent.Load() {
			return true
		}
		if silentFrom.Load() == 0 {
			silentFrom.Store(n)
		}
		if n-silentFrom.Load()+1 == dieAt {
			died <- mgr.OnPathFailure(standby)
		}
		return false
	}
	standby = &keepaliveSession{health: seededHealth(), answer: func(int32) bool { return true }}
	mgr = multipath.NewManager()
	mgr.AddPath(primary, aether.ProtoNoise, 100)
	mgr.AddPath(standby, aether.ProtoNoise, 50)
	paths := func() meshKeepalivePaths { return mgr }
	if got := mgr.KeepalivePlanFor(primary).Strikes; got != 2 {
		t.Fatalf("premise: the primary beside a measured, answering standby has %d strikes, want 2", got)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	sctx, scancel := context.WithCancel(ctx)
	defer scancel()
	go runMeshKeepalive(sctx, standby, standbyID, paths)
	letGo := make(chan bool, 1)
	go func() { letGo <- runMeshKeepalive(ctx, primary, primaryID, paths) }()

	deadline := time.Now().Add(40 * time.Second)
	for (primary.pings.Load() < 1 || standby.pings.Load() < 1) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	silent.Store(true)
	select {
	case failover := <-died:
		scancel() // the dead standby's keepalive ends with it
		if failover {
			t.Error("OnPathFailure for the dying standby reported a failover")
		}
	case <-time.After(60 * time.Second):
		t.Fatal("the primary never reached its unanswered probe")
	}
	if p, _ := mgr.PrimarySession(); p != primary {
		t.Error("the standby's death moved the primary")
	}
	if got := mgr.KeepalivePlanFor(primary).Strikes; got != 5 {
		t.Errorf("with its standby dead the primary's plan has %d strikes, want 5", got)
	}
	select {
	case v := <-letGo:
		if !v {
			t.Fatal("the primary's keepalive ended without letting it go")
		}
	case <-time.After(90 * time.Second):
		t.Fatal("the silent primary was never let go")
	}
	return primary.pings.Load() - silentFrom.Load() + 1
}

// The standby dies during the primary's first unanswered probe: from then on the
// primary has no standby, so it is let go at the no-standby Strikes (5).
func TestAStandbyDyingAtThePrimarysFirstSilentProbeLeavesTheNoStandbyStrikes(t *testing.T) {
	t.Parallel()
	if got := standbyDiesWhilePrimarySilent(t, 1, "vl1_keepalive_dies_first_p", "vl1_keepalive_dies_first_s"); got != 5 {
		t.Errorf("the primary was let go after %d unanswered probes, want 5 (no standby left)", got)
	}
}

// The standby dies during the primary's second unanswered probe, the last of the
// with-standby Strikes (2). When the keepalive decides on that probe the primary has no
// standby, so the no-standby Strikes (5) decide it: the plan is the one read after the
// probe failed, not before it was sent.
func TestAStandbyDyingAtThePrimarysLastWithStandbyProbeLeavesTheNoStandbyStrikes(t *testing.T) {
	t.Parallel()
	if got := standbyDiesWhilePrimarySilent(t, 2, "vl1_keepalive_dies_last_p", "vl1_keepalive_dies_last_s"); got != 5 {
		t.Errorf("the primary was let go after %d unanswered probes with its standby already dead, want 5", got)
	}
}
