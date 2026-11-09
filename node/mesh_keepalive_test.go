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
	"sync/atomic"
	"testing"
	"time"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/health"
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
	go runMeshKeepalive(ctx, s, "vl1_keepalive_loss")

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

// A silent session is let go after the plan's Strikes probes in a row, whatever paths
// the peer holds beside it: Strikes is the one strike budget.
func TestAMeshKeepaliveLetsASilentSessionGoAfterThePlansStrikes(t *testing.T) {
	t.Parallel()
	s := &keepaliveSession{}
	strikes := aether.KeepalivePlanFor(s).Strikes
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if !runMeshKeepalive(ctx, s, "vl1_keepalive_strikes") {
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
	go runMeshKeepalive(ctx, s, "vl1_keepalive_one")
	for wait := time.Now().Add(time.Second); time.Now().Before(wait); time.Sleep(5 * time.Millisecond) {
		if _, running := meshKeepalives.Load(s); running {
			break
		}
	}

	second, cancelSecond := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancelSecond()
	start := time.Now()
	letGo := runMeshKeepalive(second, s, "vl1_keepalive_one")
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
