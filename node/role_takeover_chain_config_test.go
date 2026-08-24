/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"crypto/ed25519"
	"testing"

	aether "github.com/ORBTR/aether"

	"github.com/bbmumford/loom/ports"
)

// armAllowPolicy admits every trust decision. armRoleTakeover only needs a
// non-nil Policy to arm and never invokes a decision during arming (the
// per-role entitlement gate runs later, at envelope-open time), so a
// permissive stub is enough to reach the engine and read its resolved config.
type armAllowPolicy struct{}

func (armAllowPolicy) VerifyNodeKey(ports.NodeID, ed25519.PublicKey) error { return nil }
func (armAllowPolicy) AuthorizePublish(context.Context, ports.Principal, ports.Topic) error {
	return nil
}
func (armAllowPolicy) AuthorizeRead(context.Context, ports.Principal, ports.Topic) error { return nil }
func (armAllowPolicy) AuthorizeRoleEntitlement(context.Context, ports.Principal, string) error {
	return nil
}
func (armAllowPolicy) AuthorizeObserver(context.Context, ports.Principal, ports.NodeID) error {
	return nil
}
func (armAllowPolicy) AuthorizeSecretRecipient(context.Context, ports.Principal, string) error {
	return nil
}
func (armAllowPolicy) KeyState(ed25519.PublicKey) ports.KeyStatus { return ports.KeyStatusActive }

// armFixture builds the smallest Runtime armRoleTakeover will act on: a live
// context, a swarm node for the engine to subscribe against, and an identity
// with a keypair. The takeover engine's run loop is drained via wg.Wait on the
// cancelled context.
func armFixture(t *testing.T, tc *RoleTakeoverConfig) *Runtime {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate identity key: %v", err)
	}
	rt := &Runtime{
		ctx:      ctx,
		swarm:    &SwarmIntegration{Node: &stubSwarmNode{}},
		identity: &NodeIdentity{NodeID: aether.NodeID("arm-test-node"), PublicKey: pub, PrivateKey: priv},
	}
	rt.cfg.RoleTakeover = tc
	t.Cleanup(func() {
		cancel()
		rt.wg.Wait()
	})
	return rt
}

// A nil RoleTakeover is the default, and it must leave the engine unstarted —
// a default node adds no takeover behaviour at all.
func TestArmRoleTakeoverIsANoOpWhenUnconfigured(t *testing.T) {
	rt := armFixture(t, nil)

	rt.armRoleTakeover()

	if rt.Takeover() != nil {
		t.Error("armRoleTakeover started an engine for a nil RoleTakeover config — the " +
			"default must be off, with zero writers and no subscriptions")
	}
}

// The failover-chain position and the winner cap only affect behaviour if they
// reach the running engine. Before they were threaded, a consumer could set
// them on RoleTakeoverConfig and the engine would run flat, so the chain order
// (node.hstles.com → node.orbtr.io → thin-apps) could not be expressed at all.
func TestArmRoleTakeoverThreadsTheFailoverChainIntoTheEngine(t *testing.T) {
	rt := armFixture(t, &RoleTakeoverConfig{
		Roles:       []string{"auth"},
		MinReplicas: 2,
		MaxWinners:  3,
		ChainRung:   2,
		ChainDepth:  4,
		Policy:      armAllowPolicy{},
	})

	rt.armRoleTakeover()

	e := rt.Takeover()
	if e == nil {
		t.Fatal("engine was not armed for a fully configured RoleTakeover")
	}
	if e.cfg.ChainRung != 2 {
		t.Errorf("engine ChainRung=%d, want 2 — the configured failover-chain class did not "+
			"reach the ranker, so publishClaim would stamp the wrong rung on the wire", e.cfg.ChainRung)
	}
	if e.cfg.ChainDepth != 4 {
		t.Errorf("engine ChainDepth=%d, want 4 — the local depth bound did not thread through", e.cfg.ChainDepth)
	}
	if e.cfg.MaxWinners != 3 {
		t.Errorf("engine MaxWinners=%d, want 3 — the configured winner cap did not reach the "+
			"ranker, so more than N nodes could activate the role", e.cfg.MaxWinners)
	}
	if e.cfg.MinReplicas != 2 {
		t.Errorf("engine MinReplicas=%d, want 2", e.cfg.MinReplicas)
	}
}

// Zero chain fields must reproduce the flat, single-rung behaviour a node had
// before the chain was configurable: an undeclared rung (0, sorts last), a
// flat depth (1), and a winner cap that defaults to the replica floor.
func TestArmRoleTakeoverZeroChainReproducesFlatBehaviour(t *testing.T) {
	rt := armFixture(t, &RoleTakeoverConfig{
		Roles:       []string{"auth"},
		MinReplicas: 1,
		Policy:      armAllowPolicy{},
	})

	rt.armRoleTakeover()

	e := rt.Takeover()
	if e == nil {
		t.Fatal("engine was not armed for a minimal RoleTakeover")
	}
	if e.cfg.ChainRung != 0 {
		t.Errorf("engine ChainRung=%d, want 0 (undeclared, sorts last) for an unconfigured chain", e.cfg.ChainRung)
	}
	if e.cfg.ChainDepth != 1 {
		t.Errorf("engine ChainDepth=%d, want 1 (flat single-rung chain)", e.cfg.ChainDepth)
	}
	if e.cfg.MaxWinners != 1 {
		t.Errorf("engine MaxWinners=%d, want 1 (defaulted to MinReplicas)", e.cfg.MaxWinners)
	}
}

// A nil Policy is fail-closed: the engine must not arm, because activation is
// gated on Policy and an armed engine with no entitlement authority could open
// a sealed bundle for a role it was never entitled to.
func TestArmRoleTakeoverWithNilPolicyFailsClosed(t *testing.T) {
	rt := armFixture(t, &RoleTakeoverConfig{
		Roles:     []string{"auth"},
		ChainRung: 1,
		Policy:    nil,
	})

	rt.armRoleTakeover()

	if rt.Takeover() != nil {
		t.Error("armed the takeover engine with a nil Policy — fail-closed requires no engine")
	}
}
