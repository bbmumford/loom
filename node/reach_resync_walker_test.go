/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"testing"
	"time"

	lad "github.com/bbmumford/ledger"
)

// COVERAGE of runReachResyncWalker itself — the loop that DRIVES
// resyncStalePeerAddresses. Measured at 0.0% and named as the remaining
// entry condition for the peer.addresses migration.
//
// 🛑 WHY A BACKSTOP'S LOOP IS WORTH ITS OWN TEST. resyncStalePeerAddresses
// being covered says what one sweep does; it says NOTHING about whether
// sweeps keep happening. A `return` misplaced in the tick arm would make
// this walker run exactly once per process and then stop forever, and
// because it is a BACKSTOP the symptom is not an error — it is the
// intermittent reappearance of the "(unresolved)" peer bug this file
// exists to prevent, with no signal pointing back here.
//
// That property cannot be observed on the production 2-minute period,
// which is why the period is now a parameter (see reach_resync.go).

func walkerTestManager(t *testing.T, cands map[string][]DialCandidate) *ConnectionManager {
	t.Helper()
	if cands == nil {
		cands = map[string][]DialCandidate{}
	}
	return &ConnectionManager{
		peers: map[string]*peerConn{},
		rt: &Runtime{
			ctx:   context.Background(),
			swarm: &SwarmIntegration{AddressTable: &AddressTable{byNode: cands}},
		},
	}
}

// stalePeer is a connected peer holding only a WS address — no "udp"
// entry, which is exactly the staleness condition the sweep looks for.
func stalePeer(nodeID string) *peerConn {
	return &peerConn{
		nodeID: nodeID, state: PeerConnected,
		addresses: []lad.ReachAddress{
			{Host: "203.0.113.7", Port: 443, Proto: "ws", Scope: "public"},
		},
	}
}

func (m *ConnectionManager) hasUDPFor(nodeID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	p := m.peers[nodeID]
	if p == nil {
		return false
	}
	for _, a := range p.addresses {
		if a.Proto == "udp" {
			return true
		}
	}
	return false
}

// waitFor polls cond until it holds or the deadline passes. Returns
// whether it held — callers assert, so a timeout is a test failure with
// a message rather than a hang.
func waitFor(cond func() bool, within time.Duration) bool {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(time.Millisecond)
	}
	return cond()
}

// 🔑 THE LOAD-BEARING ONE: the walker must keep sweeping, not stop after
// its first tick.
//
// Proving a SECOND sweep needs an observable that only a second sweep
// can produce, and the obvious fixture cannot give one: after peer A is
// merged it is no longer stale, so every later sweep is a legitimate
// no-op and "nothing changed" is indistinguishable from "the loop died".
//
// So the table gains peer B's candidates only AFTER peer A's merge has
// been observed. B can only be merged by a sweep that ran later than the
// one that merged A.
func TestReachResyncWalkerKeepsSweepingAfterTheFirstTick(t *testing.T) {
	table := map[string][]DialCandidate{
		testNodeIDA: {{
			NodeID: testNodeIDA, Transport: "noise-udp",
			Host: "fdaa:0:1234:a7b::2", Port: 41641, Scope: "private",
		}},
	}
	m := walkerTestManager(t, table)
	m.peers[testNodeIDA] = stalePeer(testNodeIDA)
	m.peers[testNodeIDB] = stalePeer(testNodeIDB)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.runReachResyncWalker(ctx, time.Millisecond)

	if !waitFor(func() bool { return m.hasUDPFor(testNodeIDA) }, 2*time.Second) {
		t.Fatal("peer A never gained a UDP candidate — the walker's tick arm " +
			"never reached resyncStalePeerAddresses at all")
	}
	// Premise: B is still unmerged, so what follows can only come from a
	// LATER sweep. Without this the test could pass on a single sweep.
	if m.hasUDPFor(testNodeIDB) {
		t.Fatal("premise wrong: peer B was merged before its candidates were " +
			"published, so a second sweep is not what is being measured")
	}

	at := m.rt.swarm.AddressTable
	at.mu.Lock()
	at.byNode[testNodeIDB] = []DialCandidate{{
		NodeID: testNodeIDB, Transport: "noise-udp",
		Host: "fdaa:0:1234:9ef::9", Port: 41641, Scope: "private",
	}}
	at.mu.Unlock()

	if !waitFor(func() bool { return m.hasUDPFor(testNodeIDB) }, 2*time.Second) {
		t.Fatal("peer B was never merged although its candidates were published " +
			"after the first sweep — the walker stopped sweeping after one tick, " +
			"so the reach-resync backstop is a one-shot and any peer whose record " +
			"arrives late stays at (unresolved) forever")
	}
}

// The walker must return when its context is cancelled, or every
// ConnectionManager.Start leaks a goroutine that outlives the node it
// belongs to and keeps mutating peer state after Stop.
func TestReachResyncWalkerStopsOnContextCancel(t *testing.T) {
	m := walkerTestManager(t, nil)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		m.runReachResyncWalker(ctx, time.Millisecond)
		close(done)
	}()

	// Let it tick at least once so cancellation is exercised against a
	// RUNNING loop rather than one that never entered its select.
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the walker did not return after its context was cancelled — " +
			"ConnectionManager.Start leaks this goroutine on every Stop")
	}
}

// An already-cancelled context must not produce a sweep.
//
// ⚠ SCOPE, STATED HONESTLY BECAUSE I FIRST WROTE IT WIDER THAN IT IS.
// This pins that the walker RETURNS and that no merge is observed. It
// does NOT pin resyncStalePeerAddresses's ctx.Err() guard: with a live
// ticker at 1ms and ctx already cancelled, only ctx.Done() is ready at
// the select, so the tick arm is not entered and the guard is never
// reached. MEASURED — deleting the guard leaves this test green.
//
// The guard is pinned by TestResyncFailsClosedWithoutItsDependencies,
// which calls the sweep directly on a cancelled context with a table
// that WOULD otherwise merge. That is the test to keep honest; this one
// covers the walker's return, not the sweep's fail-closed behaviour.
func TestReachResyncWalkerDoesNotSweepOnACancelledContext(t *testing.T) {
	m := walkerTestManager(t, map[string][]DialCandidate{
		testNodeIDA: {{
			NodeID: testNodeIDA, Transport: "noise-udp",
			Host: "fdaa:0:1234:a7b::2", Port: 41641, Scope: "private",
		}},
	})
	m.peers[testNodeIDA] = stalePeer(testNodeIDA)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan struct{})
	go func() {
		m.runReachResyncWalker(ctx, time.Millisecond)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the walker did not return on an already-cancelled context")
	}

	// Give any erroneously-scheduled sweep room to land before asserting.
	time.Sleep(20 * time.Millisecond)
	if m.hasUDPFor(testNodeIDA) {
		t.Fatal("a cancelled context still produced a merge — the tick arm won " +
			"the select race and resyncStalePeerAddresses's ctx.Err() guard did " +
			"not hold it")
	}
}

// A non-positive period must fall back to the constant, not panic.
// time.NewTicker panics on <= 0, and this runs in its own goroutine
// where that panic is unrecovered and kills the node — so the failure
// mode of getting this wrong is a process crash, not a bad period.
func TestReachResyncWalkerRejectsNonPositivePeriods(t *testing.T) {
	for _, every := range []time.Duration{0, -time.Second} {
		m := walkerTestManager(t, nil)
		ctx, cancel := context.WithCancel(context.Background())

		panicked := make(chan any, 1)
		done := make(chan struct{})
		go func() {
			defer close(done)
			defer func() {
				if r := recover(); r != nil {
					panicked <- r
				}
			}()
			m.runReachResyncWalker(ctx, every)
		}()

		// The fallback period is 2 minutes, so no sweep can occur here;
		// cancellation is the only way this returns, which also confirms
		// it built a valid ticker rather than dying.
		time.Sleep(10 * time.Millisecond)
		cancel()

		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("every=%v: walker did not return", every)
		}
		select {
		case r := <-panicked:
			t.Fatalf("every=%v: walker panicked (%v) — a non-positive period "+
				"reaches time.NewTicker and takes the node down with it", every, r)
		default:
		}
	}
}
