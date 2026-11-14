/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */
package gateway

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"sync"
	"testing"
	"time"

	task "github.com/bbmumford/loom/core/task"
	taskstore "github.com/bbmumford/loom/core/task/store"
)

type edSigner struct {
	priv ed25519.PrivateKey
	fail bool
}

func (s edSigner) SignLease(_ context.Context, canonical []byte) ([]byte, string, error) {
	if s.fail {
		return nil, "", errors.New("signer unavailable")
	}
	return ed25519.Sign(s.priv, canonical), "lease-key-1", nil
}

// receiptVerifier admits a receipt that is "done:" and the result's fence.
type receiptVerifier struct{}

func (receiptVerifier) VerifyReceipt(_ context.Context, lease task.Lease, result task.Result) error {
	if !bytes.Equal(result.Receipt, []byte("done:"+lease.Identity().Fence())) {
		return errors.New("receipt does not bind the lease")
	}
	return nil
}

// sweptStore is a MemoryStore whose KeepWaiting ends every wait with cause
// once end is set, counting the calls.
type sweptStore struct {
	*taskstore.MemoryStore
	mu    sync.Mutex
	calls int
	end   string
}

func (s *sweptStore) KeepWaiting(ctx context.Context, taskID string, at time.Time) (taskstore.WaitVerdict, error) {
	s.mu.Lock()
	s.calls++
	end := s.end
	s.mu.Unlock()
	if end != "" {
		return taskstore.WaitVerdict{Cause: end}, nil
	}
	return s.MemoryStore.KeepWaiting(ctx, taskID, at)
}

func newKey(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return pub, priv
}

func storeGateway(t *testing.T, st taskstore.Store, clock *awardClock, cfg GatewayConfig, opts ...GatewayOption) *Gateway {
	t.Helper()
	cfg.ID = "store-test"
	base := []GatewayOption{WithClock(clock), WithAwardRule(cheapestRule{}), WithStore(st)}
	g, err := NewGateway(cfg, append(base, opts...)...)
	if err != nil {
		t.Fatalf("NewGateway: %v", err)
	}
	t.Cleanup(g.Stop)
	return g
}

// awardTo opens the queued task's offer, takes it off the offers channel as a
// transport would, and has node win it.
func awardTo(t *testing.T, g *Gateway, taskID, node string) task.Assignment {
	t.Helper()
	g.produceOffers()
	select {
	case <-g.offers:
	default:
		t.Fatalf("no offer of %s was published", taskID)
	}
	g.handleBid(task.Bid{TaskID: taskID, NodeID: node, Metadata: map[string]string{"price": "1"}})
	select {
	case a := <-g.assignments:
		return a
	default:
		t.Fatalf("%s won no assignment of %s", node, taskID)
		return task.Assignment{}
	}
}

func outcomeOf(t *testing.T, st *taskstore.MemoryStore, taskID string) (taskstore.Outcome, string) {
	t.Helper()
	o, cause, ended := st.Outcome(taskID)
	if !ended {
		t.Fatalf("%s has no recorded outcome", taskID)
	}
	return o, cause
}

func TestAnAwardCommitsASignedLeaseWhoseIdentityIsTheFenceAndAnAdmittedReceiptCompletesIt(t *testing.T) {
	pub, priv := newKey(t)
	st := taskstore.NewMemoryStore()
	clock := &awardClock{now: time.Unix(1_800_000_000, 0)}
	g := storeGateway(t, st, clock, GatewayConfig{LeaseDuration: 10 * time.Second, TakeoverWindow: time.Minute},
		WithLeaseSigner(edSigner{priv: priv}), WithReceiptVerifier(receiptVerifier{}))
	if err := g.Enqueue(&task.Task{ID: "job-1", Partition: "p1"}); err != nil {
		t.Fatal(err)
	}
	a := awardTo(t, g, "job-1", "node-a")
	if a.Lease == nil || !ed25519.Verify(pub, a.Lease.Canonical, a.Lease.Signature) || a.Lease.SignerKeyID != "lease-key-1" {
		t.Fatalf("assignment lease %+v is not signed by the gateway's signer", a.Lease)
	}
	if a.Fence != a.Lease.Identity().Fence() || a.Lease.Terms.NodeID != "node-a" || a.Lease.Terms.Partition != "p1" {
		t.Fatalf("fence %q, terms %+v: want the lease identity as the fence, bound to node-a in p1", a.Fence, a.Lease.Terms)
	}
	if got := a.Lease.Terms.TakeoverUntil.Sub(a.Lease.Terms.ExpiresAt); got != time.Minute {
		t.Errorf("takeover window bound in the lease = %v, want 1m", got)
	}
	state, current, _ := st.Current("job-1")
	if state != taskstore.StateLeased || current.Identity() != a.Lease.Identity() {
		t.Fatalf("store holds %s %+v, want the awarded lease", state, current)
	}

	stale := a.Lease.Identity()
	stale.Generation++
	g.handleResult(task.Result{TaskID: "job-1", NodeID: "node-a", Fence: stale.Fence(), Status: task.ResultStatusOK})
	g.handleResult(task.Result{TaskID: "job-1", NodeID: "node-b", Fence: a.Fence, Status: task.ResultStatusOK,
		Receipt: []byte("done:" + a.Fence)})
	if _, cur, _ := st.Current("job-1"); cur == nil || cur.Identity() != a.Lease.Identity() {
		t.Fatal("a stale fence or another node's result moved the lease")
	}
	if _, _, done := st.Outcome("job-1"); done {
		t.Fatal("a stale fence or another node's result ended the task")
	}

	g.handleResult(task.Result{TaskID: "job-1", NodeID: "node-a", Fence: a.Fence, Status: task.ResultStatusOK,
		Receipt: []byte("done:" + a.Fence)})
	if o, _ := outcomeOf(t, st, "job-1"); o != taskstore.OutcomeCompleted {
		t.Fatalf("outcome = %s, want completed", o)
	}
}

func TestARefusedReceiptFailsTheAttemptAndNeverCompletesTheTask(t *testing.T) {
	_, priv := newKey(t)
	st := taskstore.NewMemoryStore()
	clock := &awardClock{now: time.Unix(1_800_000_000, 0)}
	g := storeGateway(t, st, clock, GatewayConfig{LeaseDuration: 10 * time.Second, MaxAttempts: 1},
		WithLeaseSigner(edSigner{priv: priv}), WithReceiptVerifier(receiptVerifier{}))
	if err := g.Enqueue(&task.Task{ID: "job-r"}); err != nil {
		t.Fatal(err)
	}
	a := awardTo(t, g, "job-r", "node-a")
	g.handleResult(task.Result{TaskID: "job-r", NodeID: "node-a", Fence: a.Fence, Status: task.ResultStatusOK,
		Receipt: []byte("forged")})
	if state, lease, _ := st.Current("job-r"); state != taskstore.StateWaiting || lease != nil {
		t.Fatalf("after a refused receipt the store holds %s %+v, want the lease released and the task waiting", state, lease)
	}
	if _, _, done := st.Outcome("job-r"); done {
		t.Fatal("a refused receipt ended the task")
	}
}

func TestTheHolderRenewsItsLeaseAndASupersededVersionRenewsNothing(t *testing.T) {
	_, priv := newKey(t)
	st := taskstore.NewMemoryStore()
	clock := &awardClock{now: time.Unix(1_800_000_000, 0)}
	g := storeGateway(t, st, clock, GatewayConfig{LeaseDuration: 10 * time.Second, TakeoverWindow: time.Minute},
		WithLeaseSigner(edSigner{priv: priv}))
	if err := g.Enqueue(&task.Task{ID: "job-2"}); err != nil {
		t.Fatal(err)
	}
	first := awardTo(t, g, "job-2", "node-a")
	clock.advance(5 * time.Second)
	g.handleRenewal(task.Renewal{TaskID: "job-2", NodeID: "node-b", Fence: first.Fence})
	g.handleRenewal(task.Renewal{TaskID: "job-2", NodeID: "node-a", Fence: first.Fence})
	var renewed task.Assignment
	select {
	case renewed = <-g.assignments:
	default:
		t.Fatal("the holder's renewal answered no new version")
	}
	if renewed.Lease.Terms.LeaseID != first.Lease.Terms.LeaseID || renewed.Lease.Terms.Generation != 2 ||
		!renewed.Lease.Terms.ExpiresAt.Equal(clock.Now().Add(10*time.Second)) {
		t.Fatalf("renewed terms %+v, want generation 2 of the same lease expiring 10s from now", renewed.Lease.Terms)
	}
	g.handleRenewal(task.Renewal{TaskID: "job-2", NodeID: "node-a", Fence: first.Fence})
	if _, cur, _ := st.Current("job-2"); cur.Terms.Generation != 2 {
		t.Fatalf("a superseded version renewed the lease to generation %d", cur.Terms.Generation)
	}
	g.mu.Lock()
	deadline := g.inFlight["job-2"].deadline
	g.mu.Unlock()
	if !deadline.Equal(renewed.Lease.Terms.ExpiresAt) {
		t.Errorf("in-flight deadline %v, want the renewed expiry %v", deadline, renewed.Lease.Terms.ExpiresAt)
	}
}

func TestAnUnrenewedLeaseIsOfferedForATakeoverOnTheSameLease(t *testing.T) {
	st := taskstore.NewMemoryStore()
	clock := &awardClock{now: time.Unix(1_800_000_000, 0)}
	g := storeGateway(t, st, clock, GatewayConfig{LeaseDuration: 10 * time.Second, TakeoverWindow: 30 * time.Second})
	if err := g.Enqueue(&task.Task{ID: "job-3"}); err != nil {
		t.Fatal(err)
	}
	first := awardTo(t, g, "job-3", "node-a")
	clock.advance(11 * time.Second)
	g.expireLeases()
	if g.queue.Len() != 1 {
		t.Fatalf("queue holds %d tasks after the lease lapsed, want the task offered again", g.queue.Len())
	}
	second := awardTo(t, g, "job-3", "node-b")
	if second.Lease.Terms.LeaseID != first.Lease.Terms.LeaseID || second.Lease.Terms.Generation != 2 ||
		second.Lease.Terms.NodeID != "node-b" || second.Lease.Terms.Attempt != first.Lease.Terms.Attempt {
		t.Fatalf("takeover terms %+v, want node-b on generation 2 of the same lease and attempt", second.Lease.Terms)
	}
	g.handleResult(task.Result{TaskID: "job-3", NodeID: "node-a", Fence: first.Fence, Status: task.ResultStatusOK})
	if _, _, done := st.Outcome("job-3"); done {
		t.Fatal("the superseded holder's result ended the task")
	}
}

func TestATakeoverWindowWithNoWinnerReapsTheTaskWithItsCause(t *testing.T) {
	st := taskstore.NewMemoryStore()
	clock := &awardClock{now: time.Unix(1_800_000_000, 0)}
	g := storeGateway(t, st, clock, GatewayConfig{LeaseDuration: 10 * time.Second, TakeoverWindow: 30 * time.Second})
	if err := g.Enqueue(&task.Task{ID: "job-4"}); err != nil {
		t.Fatal(err)
	}
	awardTo(t, g, "job-4", "node-a")
	clock.advance(11 * time.Second)
	g.expireLeases()
	clock.advance(30 * time.Second)
	g.expireLeases()
	if o, cause := outcomeOf(t, st, "job-4"); o != taskstore.OutcomeReaped || cause != taskstore.CauseTakeoverWindowElapsed {
		t.Fatalf("outcome %s (%s), want reaped by the elapsed takeover window", o, cause)
	}
	g.produceOffers()
	select {
	case offer := <-g.offers:
		t.Fatalf("a reaped task was offered again: %+v", offer)
	default:
	}
}

func TestAWaitingTaskEndsAtItsMaximumWaitOrWhenTheStoreEndsItsWait(t *testing.T) {
	clock := &awardClock{now: time.Unix(1_800_000_000, 0)}
	mem := taskstore.NewMemoryStore()
	g := storeGateway(t, mem, clock, GatewayConfig{MaxWait: time.Minute})
	if err := g.Enqueue(&task.Task{ID: "job-5"}); err != nil {
		t.Fatal(err)
	}
	g.produceOffers()
	clock.advance(61 * time.Second)
	g.sweepWaiting()
	if o, cause := outcomeOf(t, mem, "job-5"); o != taskstore.OutcomeAbandoned || cause != taskstore.CauseMaxWaitElapsed {
		t.Fatalf("outcome %s (%s), want abandoned at the maximum wait", o, cause)
	}

	swept := &sweptStore{MemoryStore: taskstore.NewMemoryStore()}
	h := storeGateway(t, swept, clock, GatewayConfig{})
	if err := h.Enqueue(&task.Task{ID: "job-6"}); err != nil {
		t.Fatal(err)
	}
	h.produceOffers()
	h.sweepWaiting()
	if swept.calls != 1 {
		t.Fatalf("KeepWaiting called %d times for a task with an open offer, want 1", swept.calls)
	}
	swept.end = "pools_cannot_cover"
	h.sweepWaiting()
	if o, cause := outcomeOf(t, swept.MemoryStore, "job-6"); o != taskstore.OutcomeAbandoned || cause != "pools_cannot_cover" {
		t.Fatalf("outcome %s (%s), want abandoned with the store's cause", o, cause)
	}
}

func TestASigningFailureCommitsNoAwardAndOffersTheTaskAgain(t *testing.T) {
	_, priv := newKey(t)
	st := taskstore.NewMemoryStore()
	clock := &awardClock{now: time.Unix(1_800_000_000, 0)}
	g := storeGateway(t, st, clock, GatewayConfig{}, WithLeaseSigner(edSigner{priv: priv, fail: true}))
	if err := g.Enqueue(&task.Task{ID: "job-7"}); err != nil {
		t.Fatal(err)
	}
	g.produceOffers()
	g.handleBid(task.Bid{TaskID: "job-7", NodeID: "node-a", Metadata: map[string]string{"price": "1"}})
	select {
	case a := <-g.assignments:
		t.Fatalf("an unsigned award was assigned: %+v", a)
	default:
	}
	if state, lease, _ := st.Current("job-7"); state != taskstore.StateWaiting || lease != nil {
		t.Fatalf("store holds %s %+v after a signing failure, want the task waiting with no lease", state, lease)
	}
	if g.queue.Len() != 1 {
		t.Fatalf("queue holds %d, want the task offered again", g.queue.Len())
	}
}

func TestStartRehydratesFromTheStoreWithoutReawardingALiveLease(t *testing.T) {
	ctx := context.Background()
	st := taskstore.NewMemoryStore()
	t0 := time.Unix(1_800_000_000, 0)
	admit := func(id string) {
		if err := st.Admit(ctx, taskstore.Record{Task: &task.Task{ID: id}, AdmittedAt: t0}); err != nil {
			t.Fatal(err)
		}
	}
	award := func(id string, at time.Time) *task.Lease {
		l, err := st.Award(ctx, taskstore.Award{TaskID: id, NodeID: "node-a", At: at, ExpiresAt: at.Add(10 * time.Second),
			TakeoverUntil: at.Add(40 * time.Second)}, nil)
		if err != nil {
			t.Fatal(err)
		}
		return l
	}
	now := t0.Add(time.Hour)
	admit("waiting")
	admit("live")
	live := award("live", now.Add(-5*time.Second))
	admit("orphaned")
	orphaned := award("orphaned", now.Add(-20*time.Second))
	admit("lapsed")
	award("lapsed", now.Add(-time.Minute))

	g := storeGateway(t, st, &awardClock{now: now}, GatewayConfig{TakeoverWindow: 30 * time.Second})
	g.mu.Lock()
	err := g.rehydrateLocked(ctx)
	g.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	g.mu.Lock()
	held, inFlight := g.inFlight["live"]
	g.mu.Unlock()
	if !inFlight || held.lease.Identity() != live.Identity() || !held.deadline.Equal(live.Terms.ExpiresAt) {
		t.Fatal("the unexpired lease was not held in flight to its expiry")
	}
	queued := map[string]*task.Task{}
	for tk := g.queue.Pop(); tk != nil; tk = g.queue.Pop() {
		queued[tk.ID] = tk
	}
	if len(queued) != 2 || queued["waiting"] == nil || queued["orphaned"] == nil {
		t.Fatalf("queued %v, want the waiting task and the orphaned one", queued)
	}
	g.mu.Lock()
	sup := g.admissionsByTask[queued["orphaned"]].supersedes
	g.mu.Unlock()
	if sup == nil || *sup != orphaned.Identity() {
		t.Fatal("the orphaned task was not offered as a takeover of its lapsed lease")
	}
	if o, cause := outcomeOf(t, st, "lapsed"); o != taskstore.OutcomeReaped || cause != taskstore.CauseTakeoverWindowElapsed {
		t.Fatalf("lapsed outcome %s (%s), want reaped", o, cause)
	}
}
