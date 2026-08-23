/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"errors"
	"testing"
	"time"

	swarm "github.com/bbmumford/swarm"

	"github.com/bbmumford/loom/secrets"
)

// COVERAGE of StartTakeover (:266), which was 0.0%.
//
// This file measures resolveChainPosition's CONSUMER, not the function.
// Pinning resolveChainPosition directly leaves its caller unasserted, and
// StartTakeover is the only production caller (:295) — it WRITES THE RESULT
// BACK INTO cfg, which publishClaim then stamps on the wire (:527). A fix
// verified at the function and unverified at its consumer is the
// "a value SET is not a value DELIVERED" shape — so this file walks the change
// from resolveChainPosition to the config the engine actually runs with.
//
// cfg.Entitled is supplied here only because StartTakeover requires it to be
// non-nil. Nothing in this file asserts anything about entitlement semantics,
// opens an envelope, or constructs secret material, and the entitlement gate
// itself is deliberately not exercised.

// takeoverFixture builds the smallest Runtime StartTakeover will accept, with a
// cancellable context so the engine's ticker goroutine can be drained.
func takeoverFixture(t *testing.T) (*Runtime, context.CancelFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	rt := &Runtime{
		ctx:   ctx,
		swarm: &SwarmIntegration{Node: &stubSwarmNode{}},
	}
	t.Cleanup(func() {
		cancel()
		rt.wg.Wait() // drain e.run before the test ends
	})
	return rt, cancel
}

func minimalCfg() TakeoverConfig {
	return TakeoverConfig{
		Roles:    []string{"auth"},
		Entitled: func(string) error { return nil },
	}
}

// 🔴 THE CONSUMER OF the FIX. An unconfigured chain must reach the engine
// as an UNDECLARED rung, so publishClaim stamps an absent rung and every peer
// sorts this node last. Before the correction, StartTakeover stored rung 1 —
// the globally most-preferred class — for a node that had configured nothing.
func TestStartTakeoverStoresAnUndeclaredRungForAnUnconfiguredChain(t *testing.T) {
	rt, _ := takeoverFixture(t)

	e, err := rt.StartTakeover(minimalCfg(), secrets.NewOpener(nil, nil, func() error { return nil }))
	if err != nil {
		t.Fatalf("StartTakeover: %v", err)
	}

	if e.cfg.ChainRung != 0 {
		t.Errorf("engine stored ChainRung=%d for a node that configured no chain, want 0 "+
			"(undeclared). A concrete rung here is published by publishClaim and ranked by "+
			"every peer; storing 1 would make an unconfigured node the most preferred "+
			"class in the fleet", e.cfg.ChainRung)
	}
	if e.cfg.ChainDepth != 1 {
		t.Errorf("engine stored ChainDepth=%d, want 1 (flat single-rung chain)", e.cfg.ChainDepth)
	}
}

// 🔬 THIS TEST EXISTS BECAUSE A MUTANT SURVIVED. Discarding the
// resolved rung — `_, cfg.ChainDepth = resolveChainPosition(...)` — passed every
// other test in this file, because their inputs are ones resolveChainPosition
// leaves ALONE (0→0, 3→3). With input equal to expected output, the write-back
// is unobservable and the assertion proves only that the field was not
// corrupted.
//
// A NEGATIVE declaration is the discriminating input: resolveChainPosition maps
// it to 0, so the stored value differs from the supplied one, and only a real
// write-back can produce it. Without this case the engine could publish a
// negative rung — which sorts BEFORE 0 as a plain int and would make a garbage
// declaration the most preferred class in the fleet.
func TestStartTakeoverWritesTheResolvedRungBackIntoTheRunningConfig(t *testing.T) {
	rt, _ := takeoverFixture(t)
	cfg := minimalCfg()
	cfg.ChainRung = -3 // a negative declaration; resolveChainPosition maps it to 0

	e, err := rt.StartTakeover(cfg, secrets.NewOpener(nil, nil, func() error { return nil }))
	if err != nil {
		t.Fatalf("StartTakeover: %v", err)
	}

	if e.cfg.ChainRung != 0 {
		t.Errorf("engine stored ChainRung=%d for a supplied value of -3, want 0. The "+
			"resolved position was not written back, so publishClaim would stamp a "+
			"negative rung on the wire — and a negative int sorts ahead of every declared "+
			"class at the comparator", e.cfg.ChainRung)
	}
}

// A DECLARED class must survive into the engine unchanged, whatever the local
// depth — the cross-node comparability property (the design 🔑 note). This is the
// consumer-side twin of
// TestTwoNodesDeclaringTheSameClassPublishTheSameRungWhateverTheirPrivateDepth.
func TestStartTakeoverPreservesADeclaredClassRegardlessOfLocalDepth(t *testing.T) {
	for _, depth := range []int{0, 1, 2, 9} {
		rt, _ := takeoverFixture(t)
		cfg := minimalCfg()
		cfg.ChainRung, cfg.ChainDepth = 3, depth

		e, err := rt.StartTakeover(cfg, secrets.NewOpener(nil, nil, func() error { return nil }))
		if err != nil {
			t.Fatalf("depth %d: StartTakeover: %v", depth, err)
		}
		if e.cfg.ChainRung != 3 {
			t.Errorf("depth %d: engine stored ChainRung=%d, want 3 — the published class "+
				"changed because of a private local value no peer can see", depth, e.cfg.ChainRung)
		}
	}
}

// The fail-closed prologue. Each guard is the only thing standing between a
// misconfigured caller and a running takeover engine.
func TestStartTakeoverRefusesAnIncompleteConfiguration(t *testing.T) {
	goodOpener := secrets.NewOpener(nil, nil, func() error { return nil })

	t.Run("swarm not initialised", func(t *testing.T) {
		rt := &Runtime{ctx: context.Background()}
		if _, err := rt.StartTakeover(minimalCfg(), goodOpener); err == nil {
			t.Error("started with no swarm — the engine subscribes to swarm topics, so it " +
				"would run with nothing delivering claims or envelopes")
		}
	})

	t.Run("nil opener", func(t *testing.T) {
		rt, _ := takeoverFixture(t)
		if _, err := rt.StartTakeover(minimalCfg(), nil); err == nil {
			t.Error("started with a nil opener — every envelope open would panic at the " +
				"moment this node wins a role")
		}
	})

	t.Run("no roles to guard", func(t *testing.T) {
		rt, _ := takeoverFixture(t)
		cfg := minimalCfg()
		cfg.Roles = nil
		if _, err := rt.StartTakeover(cfg, goodOpener); err == nil {
			t.Error("started with no roles — a ticker running forever over an empty set")
		}
	})
}

// The timing and replica defaults must be positive after Start, or the engine
// either never sweeps (zero interval) or treats every role as covered by zero
// holders.
func TestStartTakeoverAppliesPositiveDefaults(t *testing.T) {
	rt, _ := takeoverFixture(t)

	e, err := rt.StartTakeover(minimalCfg(), secrets.NewOpener(nil, nil, func() error { return nil }))
	if err != nil {
		t.Fatalf("StartTakeover: %v", err)
	}

	for _, tc := range []struct {
		name string
		got  time.Duration
	}{
		{"CorroborationWindow", e.cfg.CorroborationWindow},
		{"ClaimSettle", e.cfg.ClaimSettle},
		{"CheckInterval", e.cfg.CheckInterval},
	} {
		if tc.got <= 0 {
			t.Errorf("%s defaulted to %v — a non-positive interval means the engine never "+
				"sweeps, so a missing role is never noticed", tc.name, tc.got)
		}
	}
	if e.cfg.MinReplicas <= 0 {
		t.Errorf("MinReplicas defaulted to %d — zero holders would count as covered, so "+
			"takeover never arms", e.cfg.MinReplicas)
	}
	if e.cfg.MaxWinners <= 0 {
		t.Errorf("MaxWinners defaulted to %d — no claimant could ever win", e.cfg.MaxWinners)
	}
}

// flakySubscriber fails the Nth Subscribe call and counts how many of the
// unsubscribes it handed out were actually run.
type flakySubscriber struct {
	*stubSwarmNode
	failOn    int // 1-based call index that returns an error
	calls     int
	unsubsRun int
}

func (s *flakySubscriber) Subscribe(topic swarm.Topic, sub swarm.Subscriber) (swarm.Unsubscribe, error) {
	s.calls++
	if s.calls == s.failOn {
		return nil, errTestSubscribeRefused
	}
	return func() { s.unsubsRun++ }, nil
}

var errTestSubscribeRefused = errors.New("swarm subscribe refused")

// 🔴 THE PARTIAL-FAILURE UNWIND ACTUALLY FIRES. StartTakeover calls stopSubs on
// both subscribe-error paths (:315, :325). That the call EXISTS is not evidence
// that it releases anything — a fallback that exists is not one that fires, and
// the unwind sits inside the branch it is meant to protect, so nothing else in
// the suite could ever have reached it.
//
// Leaking a subscription would leave a dead engine still receiving claim
// records and mutating maps nobody reads — and StartTakeover returns an error,
// so no caller holds a handle that could ever stop it.
//
// 🔬 BOTH FAILURE INDICES, BECAUSE A MUTANT SURVIVED. StartTakeover
// subscribes twice per role — secrets then claims — with a SEPARATE, textually
// identical unwind after each. A fixture that only ever fails on an odd call
// exercises the secrets unwind and never the claims one, so deleting the
// claims unwind changed nothing and the suite stayed green. Third time this
// session that two structurally identical guards let one hide behind the
// other (see mutant I3 in claim_ingest_test.go and E3 in
// takeover_evaluate_test.go); the fix is the same each time — pick fixtures
// that reach each guard SEPARATELY rather than one that happens to cover both.
func TestAFailedSubscribeUnwindsTheSubscriptionsAlreadyEstablished(t *testing.T) {
	for _, tc := range []struct {
		name       string
		failOn     int
		wantUnsubs int
	}{
		// role-a secrets(1) ok, role-a claims(2) ok, role-b secrets(3) FAILS.
		{"failure on a secrets subscribe", 3, 2},
		// role-a secrets(1) ok, role-a claims(2) FAILS. Reaches the OTHER unwind.
		{"failure on a claims subscribe", 2, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			node := &flakySubscriber{stubSwarmNode: &stubSwarmNode{}, failOn: tc.failOn}
			rt := &Runtime{ctx: context.Background(), swarm: &SwarmIntegration{Node: node}}
			cfg := minimalCfg()
			cfg.Roles = []string{"role-a", "role-b"}

			e, err := rt.StartTakeover(cfg, secrets.NewOpener(nil, nil, func() error { return nil }))

			if err == nil {
				t.Fatal("StartTakeover succeeded despite a refused subscription")
			}
			if e != nil {
				t.Error("an engine was returned alongside the error — a caller that checks " +
					"the engine rather than the error would run a half-subscribed engine")
			}
			if node.unsubsRun != tc.wantUnsubs {
				t.Errorf("ran %d unsubscribes after failing on subscribe #%d, want %d — the "+
					"subscriptions established before the failure leaked, and since "+
					"StartTakeover returns an error nobody holds a handle that could ever "+
					"release them", node.unsubsRun, tc.failOn, tc.wantUnsubs)
			}
			if rt.Takeover() != nil {
				t.Error("rt.takeover was set despite the failure — Takeover hands out an " +
					"engine that was never started")
			}
		})
	}
}

// The engine must be reachable through Takeover after a successful start,
// and nil before one — that accessor is how callers reach the running engine.
func TestTakeoverAccessorReportsTheRunningEngine(t *testing.T) {
	rt, _ := takeoverFixture(t)

	if got := rt.Takeover(); got != nil {
		t.Errorf("Takeover = %v before any start, want nil", got)
	}

	e, err := rt.StartTakeover(minimalCfg(), secrets.NewOpener(nil, nil, func() error { return nil }))
	if err != nil {
		t.Fatalf("StartTakeover: %v", err)
	}
	if rt.Takeover() != e {
		t.Error("Takeover does not return the engine StartTakeover created — a caller " +
			"cannot reach the running engine")
	}
}
