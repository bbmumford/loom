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
