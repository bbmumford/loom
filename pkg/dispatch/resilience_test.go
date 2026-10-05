/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package dispatch

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ORBTR/aether"
	"github.com/ORBTR/aether/rpc/pb"
)

// These tests cover the resilient-call rules HWPCaller runs by: pooled
// streams are opened under a context that ends with their session and with
// the caller; a request id is collision-resistant across processes; a circuit
// is scoped to one capability, destination and session generation, counts
// availability failures only, and admits one leased half-open probe.

// ── Pool fill lifecycle ────────────────────────────────────────────────────

// blockingSession is an aether.Session whose OpenStream blocks until the
// context it was given ends, the way a real OpenStream blocks on a stalled
// peer. CloseSignal is the close channel aether.BaseSession exposes.
type blockingSession struct {
	rpcMockSession
	closeOnce sync.Once
	closed    chan struct{}
	inOpen    chan struct{}
	opens     atomic.Int32
}

func newBlockingSession() *blockingSession {
	return &blockingSession{closed: make(chan struct{}), inOpen: make(chan struct{}, 16)}
}

func (s *blockingSession) OpenStream(ctx context.Context, cfg aether.StreamConfig) (aether.Stream, error) {
	s.opens.Add(1)
	s.inOpen <- struct{}{}
	<-ctx.Done()
	return nil, ctx.Err()
}
func (s *blockingSession) CloseSignal() <-chan struct{} { return s.closed }
func (s *blockingSession) IsClosed() bool {
	select {
	case <-s.closed:
		return true
	default:
		return false
	}
}
func (s *blockingSession) Close() error {
	s.closeOnce.Do(func() { close(s.closed) })
	return nil
}

func waitDone(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatalf("%s is still blocked 3 s later; the pool fill is running under a "+
			"context that outlives it", what)
	}
}

// A pool fill blocked on a stalled OpenStream ends when its session closes.
func TestPoolFillStopsWhenItsSessionCloses(t *testing.T) {
	sess := newBlockingSession()
	pool := NewStreamPool(2)

	done := make(chan struct{})
	go func() {
		pool.Fill(context.Background(), sess)
		close(done)
	}()
	<-sess.inOpen

	sess.Close()
	waitDone(t, done, "Fill after the session closed")
}

// A pool refill blocked on a stalled OpenStream ends when the caller that owns
// the pool closes, and Close itself does not wait behind the refill.
func TestPoolRefillStopsWhenTheCallerCloses(t *testing.T) {
	sess := newBlockingSession()
	caller := NewHWPCaller(&mockFinder{})
	caller.mu.Lock()
	caller.sessions["stalled"] = &cachedSession{
		session:      sess,
		role:         "stalled",
		lastUsedNano: time.Now().UnixNano(),
		pool:         NewStreamPool(2),
	}
	caller.mu.Unlock()

	done := make(chan struct{})
	go func() {
		caller.refillPools()
		close(done)
	}()
	<-sess.inOpen

	closed := make(chan struct{})
	go func() {
		caller.Close()
		close(closed)
	}()
	waitDone(t, closed, "HWPCaller.Close")
	waitDone(t, done, "refillPools after the caller closed")
}

// ── Request identity ───────────────────────────────────────────────────────

const mintIDEnv = "LOOM_DISPATCH_MINT_REQUEST_ID"

// Two processes mint their first request id from the same starting state; the
// ids must still differ. A process-local counter starts at the same value in
// every process, so ids built from it alone collide across processes, and the
// receiving node's dedup cache keys on the id.
func TestTwoProcessesNeverMintEqualRequestIDs(t *testing.T) {
	if os.Getenv(mintIDEnv) == "1" {
		fmt.Println("REQUEST_ID=" + buildRPCRequest("role", "role.Op", nil, 0).Id)
		return
	}
	seen := map[string]int{}
	for i := 0; i < 2; i++ {
		cmd := exec.Command(os.Args[0], "-test.run=^TestTwoProcessesNeverMintEqualRequestIDs$", "-test.count=1")
		cmd.Env = append(os.Environ(), mintIDEnv+"=1")
		out, err := cmd.Output()
		if err != nil {
			t.Fatalf("child process %d failed: %v\n%s", i, err, out)
		}
		id := ""
		sc := bufio.NewScanner(strings.NewReader(string(out)))
		for sc.Scan() {
			if v, ok := strings.CutPrefix(sc.Text(), "REQUEST_ID="); ok {
				id = v
			}
		}
		if id == "" {
			t.Fatalf("child process %d printed no request id:\n%s", i, out)
		}
		if prev, dup := seen[id]; dup {
			t.Fatalf("processes %d and %d both minted request id %q as their first id", prev, i, id)
		}
		seen[id] = i
	}
}

// Within one process ids stay distinct, and the counter appears only as a
// suffix after the random part.
func TestRequestIDsCarryARandomPartBeforeTheCounter(t *testing.T) {
	a := buildRPCRequest("r", "r.A", nil, 0).Id
	b := buildRPCRequest("r", "r.B", nil, 0).Id
	if a == b {
		t.Fatalf("two requests in one process share id %q", a)
	}
	ra, _, okA := strings.Cut(a, "-")
	rb, _, okB := strings.Cut(b, "-")
	if !okA || !okB || len(ra) < 32 || ra == rb {
		t.Fatalf("ids %q and %q do not lead with distinct 128-bit random parts", a, b)
	}
}

// ── Circuit scope ──────────────────────────────────────────────────────────

// genSession is a failing rpcMockSession with a settable connection id, the
// session generation a circuit is scoped to.
type genSession struct {
	rpcMockSession
	gen aether.ConnectionID
}

func (s *genSession) ConnectionID() aether.ConnectionID { return s.gen }

// Two capabilities served by one node trip independently: five availability
// failures on one capability open its circuit and leave the other's closed,
// so a call to the second still reaches the session.
func TestTwoCapabilitiesToOneNodeTripIndependently(t *testing.T) {
	sess := &rpcMockSession{openErr: fmt.Errorf("conn dead")}
	caller := NewHWPCaller(&mockFinder{session: sess})
	defer caller.Close()

	for i := 0; i < 5; i++ {
		_, _ = caller.Call(context.Background(), "billing", "billing.Charge", nil)
	}
	before := atomic.LoadInt32(&sess.openCount)
	_, err := caller.Call(context.Background(), "billing", "billing.Charge", nil)
	if after := atomic.LoadInt32(&sess.openCount); after != before {
		t.Fatalf("premise wrong: the tripped capability still reached the session (%v)", err)
	}

	_, err = caller.Call(context.Background(), "billing", "billing.Refund", nil)
	if errors.Is(err, ErrCircuitOpen) || strings.Contains(fmt.Sprint(err), ErrCircuitOpen.Error()) {
		t.Fatalf("a second capability on the same node was refused by the first one's "+
			"circuit: %v", err)
	}
	if after := atomic.LoadInt32(&sess.openCount); after == before {
		t.Fatal("a call to a second capability on the same node never reached the session")
	}
}

// A new session generation to the same node gets its own circuit: a circuit
// opened on one generation does not refuse calls over its replacement.
func TestANewSessionGenerationGetsItsOwnCircuit(t *testing.T) {
	old := &genSession{rpcMockSession: rpcMockSession{openErr: fmt.Errorf("conn dead")}, gen: aether.ConnectionID{1}}
	finder := &multiRouteFinder{routes: []aether.ProbeRoute{{Session: old, NodeID: "n1", TargetNodeID: "n1"}}}
	caller := NewHWPCaller(finder)
	defer caller.Close()

	for i := 0; i < 5; i++ {
		_, _ = caller.Call(context.Background(), "svc", "svc.Op", nil)
	}
	fresh := &genSession{rpcMockSession: rpcMockSession{resp: &pb.RPCResponse{Success: true, Payload: []byte("ok")}}, gen: aether.ConnectionID{2}}
	finder.routes = []aether.ProbeRoute{{Session: fresh, NodeID: "n1", TargetNodeID: "n1"}}
	caller.invalidateRouteCache("svc", "svc.Op")

	resp, err := caller.Call(context.Background(), "svc", "svc.Op", nil)
	if err != nil || string(resp) != "ok" {
		t.Fatalf("a replacement session was refused by the previous generation's circuit: %v", err)
	}
}

// An open circuit's own refusals are not availability failures. Counting them
// re-arms the open window on every refused call, so a circuit under steady
// traffic never reaches half-open and the node stays cut off.
func TestAnOpenCircuitsOwnRefusalsDoNotKeepItOpen(t *testing.T) {
	sess := &rpcMockSession{openErr: fmt.Errorf("conn dead")}
	caller := NewHWPCaller(&mockFinder{session: sess})
	caller.breakerReset = 60 * time.Millisecond
	defer caller.Close()

	for i := 0; i < 5; i++ {
		_, _ = caller.Call(context.Background(), "svc", "svc.Op", nil)
	}
	// Calls every 10 ms for five reset periods: with no gap as long as the
	// reset timeout, only the circuit's own clock can admit a probe.
	before := atomic.LoadInt32(&sess.openCount)
	deadline := time.Now().Add(5 * caller.breakerReset)
	for time.Now().Before(deadline) {
		_, _ = caller.Call(context.Background(), "svc", "svc.Op", nil)
		time.Sleep(10 * time.Millisecond)
	}
	if atomic.LoadInt32(&sess.openCount) == before {
		t.Fatal("the circuit refused every call under steady traffic and never admitted a " +
			"half-open probe: its own refusals are being counted as failures")
	}
}

// ── Half-open lease ────────────────────────────────────────────────────────

func trip(cb *CircuitBreaker) {
	for i := 0; i < cb.failureThreshold; i++ {
		cb.RecordFailure()
	}
}

// Exactly one half-open probe is admitted at a time.
func TestOneHalfOpenProbeIsAdmittedAtATime(t *testing.T) {
	cb := NewCircuitBreaker(3, time.Minute, 20*time.Millisecond)
	trip(cb)
	time.Sleep(30 * time.Millisecond)

	var admitted atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if cb.Allow() {
				admitted.Add(1)
			}
		}()
	}
	wg.Wait()
	if n := admitted.Load(); n != 1 {
		t.Fatalf("%d half-open probes were admitted, want exactly 1", n)
	}
}

// A half-open probe that ends with no verdict, such as one the caller
// cancelled, releases its lease, and the next call becomes the probe.
func TestAReleasedHalfOpenLeaseAdmitsTheNextProbe(t *testing.T) {
	cb := NewCircuitBreaker(3, time.Minute, 20*time.Millisecond)
	trip(cb)
	time.Sleep(30 * time.Millisecond)
	if !cb.Allow() {
		t.Fatal("premise wrong: no half-open probe after the reset timeout")
	}
	cb.Release()
	if !cb.Allow() {
		t.Fatal("after the probe released its lease with no verdict, the next call was " +
			"refused; the circuit is stuck half-open")
	}
}

// A lease nobody reports on expires after the reset timeout, so a probe lost
// without a verdict cannot hold the circuit half-open for ever.
func TestAnUnreportedHalfOpenLeaseExpires(t *testing.T) {
	cb := NewCircuitBreaker(3, time.Minute, 20*time.Millisecond)
	trip(cb)
	time.Sleep(30 * time.Millisecond)
	if !cb.Allow() {
		t.Fatal("premise wrong: no half-open probe after the reset timeout")
	}
	if cb.Allow() {
		t.Fatal("a second probe was admitted while the first held the lease")
	}
	time.Sleep(30 * time.Millisecond)
	if !cb.Allow() {
		t.Fatal("the half-open lease never expired; a probe lost without a verdict holds " +
			"the circuit half-open for ever")
	}
}
