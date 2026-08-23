/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"errors"
	"testing"

	"github.com/bbmumford/loom/node/handlers"
	"github.com/bbmumford/loom/ports"
	"github.com/bbmumford/loom/secrets"
)

// Covers role_activation.go.
//
// ActivateRole's third parameter is a `secrets.ConfigBundle` — a DECRYPTED
// role-config bundle — and nothing in this file touches that surface. Every
// test passes an EMPTY bundle, which skips the config loop entirely, so no
// secret material is constructed, read, or asserted on.
//
// That costs no coverage of what this file is for: the properties under test
// are ORDERING and UNWIND, not payload handling, and the activation sequence,
// the failure unwind, and the role-set round-trip are all exercised with an
// empty bundle.
//
// What is consequently NOT asserted anywhere here, stated so a green run does
// not imply otherwise: the `for name, payload := range bundle` loop, and
// activationEnv.RegisterConfig's precedence rule that a live config wins over
// a gossiped bundle. That precedence is a real correctness property and this
// file leaves it uncovered.

// ─── fixture ─────────────────────────────────────────────────────────────

// activationFixture builds the smallest Runtime that ActivateRole can run
// against: a real HandlerRegistry (exact scoped registration ownership is what
// we are testing, so faking the registry would test the fake) and a real
// manager.
//
// 🔬 MEASURED, and it is why rpcServer is not optional here: ActivateRole opens
// a registry-owned causal registration scope, and rt.Registry() returns nil
// when rt.rpcServer is nil.
//
// That is NOT a live defect: runtime.go:557-562 assigns rt.rpcServer and
// rt.roleActivation three lines apart, unconditionally, in one constructor
// block — and ActivateRole dereferences rt.roleActivation first, so a Runtime
// without the manager panics before it ever reaches Registry(). Whether that
// discharge covers this path matters as much as the discharge itself:
// the guarantee comes from a co-assignment in another file, not from a guard here,
// and step 4 of this very function DOES guard `rt.rpcRegistry != nil`. The
// defensiveness is asymmetric within one function; recorded, not "fixed", because
// changing it would be a production edit this slice did not claim.
// ⚠ rt.ctx is DELIBERATELY NOT context.Background(). Mutation caught this: with
// rt.ctx == context.Background(), a mutant replacing env.Go's `fn(rt.ctx)` with
// `fn(context.Background())` SURVIVED — the two are the same value, so no
// assertion could tell them apart. A fixture whose values coincide cannot
// distinguish the mechanisms that read them.
func activationFixture(roles ...string) *Runtime {
	registry := handlers.NewHandlerRegistry()
	rt := &Runtime{
		ctx:       context.WithValue(context.Background(), runtimeCtxMarker{}, "runtime"),
		rpcServer: NewRPCServer(registry),
	}
	rt.cfg.Roles = append([]string(nil), roles...)
	rt.roleActivation = newRoleActivationManager(rt)
	return rt
}

// runtimeCtxMarker makes the fixture's runtime context distinguishable from
// context.Background() by identity.
type runtimeCtxMarker struct{}

// scriptedActivator registers a fixed set of handler names, then optionally
// fails — the shape that exercises the partial-registration unwind.
type scriptedActivator struct {
	role         string
	registers    []string
	activateErr  error
	deactivateEr error
	rt           *Runtime
	activateRuns int
	deactivRuns  int
}

func (a *scriptedActivator) Role() string { return a.role }

func (a *scriptedActivator) Activate(ctx context.Context, env ports.ActivationEnv) error {
	a.activateRuns++
	for _, name := range a.registers {
		if _, err := a.rt.Registry().RegisterRPCScoped(
			env.RegistrationScope(),
			&rpcOnlyHandler{name: name, role: a.role, scope: handlers.TenantScopeNone},
		); err != nil {
			return err
		}
	}
	return a.activateErr
}

func (a *scriptedActivator) Deactivate(ctx context.Context) error {
	a.deactivRuns++
	return a.deactivateEr
}

var _ ports.RoleActivator = (*scriptedActivator)(nil)

// ─── registration ────────────────────────────────────────────────────────

// The guard is fail-closed and it is the only thing standing between a
// misconfigured activator and a role that can never be torn down: an activator
// registered under "" would be unreachable by ActivateRole (which looks up by
// role name) yet still occupy the map.
func TestRegisterRoleActivatorRejectsNilAndEmptyRole(t *testing.T) {
	rt := activationFixture()

	if err := rt.RegisterRoleActivator(nil); err == nil {
		t.Error("a nil activator was accepted")
	}
	if err := rt.RegisterRoleActivator(&scriptedActivator{role: "", rt: rt}); err == nil {
		t.Error("an activator with an empty role name was accepted")
	}
	if got := rt.ActiveTakeoverRoles(); len(got) != 0 {
		t.Errorf("a rejected registration left state behind: %v", got)
	}
}

// "Last registration per role wins (config-reload friendly)" is a documented
// promise, and a reload that silently kept the FIRST activator would run stale
// service code after an operator reloaded config.
func TestLastRegistrationPerRoleWins(t *testing.T) {
	rt := activationFixture()
	first := &scriptedActivator{role: "auth", rt: rt}
	second := &scriptedActivator{role: "auth", rt: rt}

	if err := rt.RegisterRoleActivator(first); err != nil {
		t.Fatalf("first registration: %v", err)
	}
	if err := rt.RegisterRoleActivator(second); err != nil {
		t.Fatalf("second registration: %v", err)
	}
	if err := rt.ActivateRole(context.Background(), "auth", secrets.ConfigBundle{}); err != nil {
		t.Fatalf("ActivateRole: %v", err)
	}

	if first.activateRuns != 0 {
		t.Error("the SUPERSEDED activator ran — a config reload would start stale services")
	}
	if second.activateRuns != 1 {
		t.Errorf("the current activator ran %d times, want 1", second.activateRuns)
	}
}

// 🔑 "CAN THIS ROLE BE ACTIVATED" AND "IS THIS ROLE ACTIVE" ARE DIFFERENT
// QUESTIONS OVER DIFFERENT MAPS (activators vs active), and they are easy to
// conflate because both read as "does this node do auth?". A caller that used
// HasRoleActivator to decide whether to CLAIM a role would be right; one that
// used it to decide whether the role is already SERVED would double-activate.
func TestHasRoleActivatorAndActiveTakeoverRolesAnswerDifferentQuestions(t *testing.T) {
	rt := activationFixture()
	if err := rt.RegisterRoleActivator(&scriptedActivator{role: "auth", rt: rt}); err != nil {
		t.Fatalf("register: %v", err)
	}

	if !rt.HasRoleActivator("auth") {
		t.Error("HasRoleActivator(auth) is false after registering an auth activator")
	}
	if got := rt.ActiveTakeoverRoles(); len(got) != 0 {
		t.Errorf("ActiveTakeoverRoles = %v before any activation — registering is not activating", got)
	}
	if rt.HasRoleActivator("billing") {
		t.Error("HasRoleActivator(billing) is true with no billing activator")
	}

	if err := rt.ActivateRole(context.Background(), "auth", secrets.ConfigBundle{}); err != nil {
		t.Fatalf("ActivateRole: %v", err)
	}
	if got := rt.ActiveTakeoverRoles(); len(got) != 1 || got[0] != "auth" {
		t.Errorf("ActiveTakeoverRoles = %v after activation, want [auth]", got)
	}
}

// ─── the unwind ──────────────────────────────────────────────────────────

// 🔴 THE FAIL-CLOSED PROPERTY THIS FILE EXISTS FOR. The comment on the unwind
// branch states it: "a half-activated role must not leave dispatchable handlers
// behind." A handler left registered by a FAILED activation is dispatchable
// while the service backing it was never started — the caller gets a handler
// that cannot serve, and the role is not in the enabled set, so nothing else
// would ever clean it up.
func TestAFailedActivationLeavesNoHandlerRegistered(t *testing.T) {
	rt := activationFixture("system")
	boom := errors.New("activator exploded after registering two handlers")
	act := &scriptedActivator{
		role:        "auth",
		registers:   []string{"orbtr.io.auth.A", "orbtr.io.auth.B"},
		activateErr: boom,
		rt:          rt,
	}
	if err := rt.RegisterRoleActivator(act); err != nil {
		t.Fatalf("register: %v", err)
	}

	err := rt.ActivateRole(context.Background(), "auth", secrets.ConfigBundle{})
	if !errors.Is(err, boom) {
		t.Fatalf("ActivateRole error = %v, want it to wrap %v", err, boom)
	}

	for _, name := range act.registers {
		if _, ok := rt.Registry().GetMeta(name); ok {
			t.Errorf("handler %q survived a FAILED activation — it is dispatchable with no "+
				"service behind it, and nothing else will remove it", name)
		}
	}
	if got := rt.ActiveTakeoverRoles(); len(got) != 0 {
		t.Errorf("a failed activation marked the role active: %v", got)
	}
	// The role must not have been advertised: step 3 (SetEnabledRoles) and
	// step 5 (SetRoles) are both AFTER the error return.
	if rt.Registry().IsRoleEnabled("auth") {
		t.Error("a failed activation enabled dispatch for the role")
	}
	if got := rt.cfg.Roles; len(got) != 1 || got[0] != "system" {
		t.Errorf("cfg.Roles = %v after a failed activation, want the original [system]", got)
	}
}

// Idempotence is documented per role ("Idempotent per role"), and the takeover
// path can re-fire on repeated claim evaluations — a second activation that ran
// the activator again would double-start the role's services.
func TestActivatingAnAlreadyActiveRoleIsANoOp(t *testing.T) {
	rt := activationFixture("system")
	act := &scriptedActivator{role: "auth", registers: []string{"orbtr.io.auth.A"}, rt: rt}
	if err := rt.RegisterRoleActivator(act); err != nil {
		t.Fatalf("register: %v", err)
	}

	for i := 0; i < 3; i++ {
		if err := rt.ActivateRole(context.Background(), "auth", secrets.ConfigBundle{}); err != nil {
			t.Fatalf("ActivateRole #%d: %v", i+1, err)
		}
	}

	if act.activateRuns != 1 {
		t.Errorf("the activator ran %d times for 3 ActivateRole calls — the role's services "+
			"are being started more than once", act.activateRuns)
	}
	if got := rt.cfg.Roles; len(got) != 2 {
		t.Errorf("cfg.Roles = %v — repeated activation appended the role more than once "+
			"(appendUniqueString's duplicate guard is the only thing preventing this)", got)
	}
}

// Reaches appendUniqueString's duplicate guard directly. The idempotence guard
// above returns early on every RE-activation, so no test that goes through
// ActivateRole ever presents the append path with a role already in the set —
// neutering that guard fails nothing without this test.
//
// The scenario that DOES reach it is ordinary and shipping: a node BOOTED with
// "auth" already in cfg.Roles (a statically configured thick node) that also
// registers an auth activator and later wins a takeover claim for it. m.active
// is false — it was never runtime-activated — so ActivateRole proceeds, and
// appendUniqueString is handed a list that already contains the role.
//
// Without the guard the node advertises "auth" twice, and every subsequent
// activate/deactivate cycle adds another copy: removeString strips them all at
// once, so the duplication is invisible until the role set is read.
func TestActivatingARoleTheNodeAlreadyAdvertisesDoesNotDuplicateIt(t *testing.T) {
	rt := activationFixture("system", "auth")
	act := &scriptedActivator{role: "auth", registers: []string{"orbtr.io.auth.A"}, rt: rt}
	if err := rt.RegisterRoleActivator(act); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := rt.ActivateRole(context.Background(), "auth", secrets.ConfigBundle{}); err != nil {
		t.Fatalf("ActivateRole: %v", err)
	}

	seen := 0
	for _, r := range rt.cfg.Roles {
		if r == "auth" {
			seen++
		}
	}
	if seen != 1 {
		t.Errorf("cfg.Roles = %v — %q appears %d times after activating a role the node "+
			"already advertised; the node publishes a duplicated role set",
			rt.cfg.Roles, "auth", seen)
	}

	// And the round trip must still land exactly on the boot configuration.
	if err := rt.DeactivateRole(context.Background(), "auth"); err != nil {
		t.Fatalf("DeactivateRole: %v", err)
	}
	if got := rt.cfg.Roles; len(got) != 1 || got[0] != "system" {
		t.Errorf("cfg.Roles = %v after deactivating a BOOT-configured role, want [system] — "+
			"note this drops a statically configured role, which is the documented behaviour "+
			"of removeString but worth pinning: teardown does not distinguish a runtime-added "+
			"role from a boot-configured one", got)
	}
}

func TestActivateRoleFailsWhenNoActivatorIsRegistered(t *testing.T) {
	rt := activationFixture("system")

	if err := rt.ActivateRole(context.Background(), "auth", secrets.ConfigBundle{}); err == nil {
		t.Fatal("ActivateRole succeeded for a role with no registered activator")
	}
	if got := rt.cfg.Roles; len(got) != 1 || got[0] != "system" {
		t.Errorf("cfg.Roles = %v, want the original [system] untouched", got)
	}
}

// ─── the round trip ──────────────────────────────────────────────────────

// The agreement test: appendUniqueString
// and removeString are two independent functions that must be exact inverses on
// the role set, and NOTHING in the code forces them to agree. Activate-then-
// deactivate must leave the node advertising precisely what it advertised
// before — a residue means the node advertises a role it no longer serves
// (claims land on it and fail), and an over-removal means it stops advertising
// a role it still serves.
func TestActivateThenDeactivateRestoresTheExactOriginalRoleSet(t *testing.T) {
	rt := activationFixture("system", "relay")
	act := &scriptedActivator{
		role:      "auth",
		registers: []string{"orbtr.io.auth.A", "orbtr.io.auth.B"},
		rt:        rt,
	}
	if err := rt.RegisterRoleActivator(act); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := rt.ActivateRole(context.Background(), "auth", secrets.ConfigBundle{}); err != nil {
		t.Fatalf("ActivateRole: %v", err)
	}
	if got := len(rt.cfg.Roles); got != 3 {
		t.Fatalf("cfg.Roles has %d entries after activation, want 3", got)
	}

	if err := rt.DeactivateRole(context.Background(), "auth"); err != nil {
		t.Fatalf("DeactivateRole: %v", err)
	}

	if got := rt.cfg.Roles; len(got) != 2 || got[0] != "system" || got[1] != "relay" {
		t.Errorf("cfg.Roles = %v after the round trip, want the original [system relay] — "+
			"appendUniqueString and removeString are not exact inverses", got)
	}
	if rt.Registry().IsRoleEnabled("auth") {
		t.Error("dispatch is still enabled for a deactivated role")
	}
	// EXACTLY the handlers the activation registered, and only those.
	for _, name := range act.registers {
		if _, ok := rt.Registry().GetMeta(name); ok {
			t.Errorf("handler %q survived deactivation", name)
		}
	}
	if act.deactivRuns != 1 {
		t.Errorf("the activator's Deactivate ran %d times, want 1", act.deactivRuns)
	}
}

// Deactivating a role that was never active must not run the activator's
// teardown: Deactivate is documented as releasing resources, and calling it on
// services that were not started is the mirror of the double-start above.
func TestDeactivatingAnInactiveRoleIsANoOp(t *testing.T) {
	rt := activationFixture("system")
	act := &scriptedActivator{role: "auth", rt: rt}
	if err := rt.RegisterRoleActivator(act); err != nil {
		t.Fatalf("register: %v", err)
	}

	if err := rt.DeactivateRole(context.Background(), "auth"); err != nil {
		t.Fatalf("DeactivateRole on an inactive role returned %v, want nil", err)
	}
	if act.deactivRuns != 0 {
		t.Error("Deactivate ran for a role that was never activated")
	}
	if got := rt.cfg.Roles; len(got) != 1 || got[0] != "system" {
		t.Errorf("cfg.Roles = %v, want [system] untouched", got)
	}
}

// A teardown that fails keeps the role recorded ACTIVE and its handlers
// registered so a later teardown can retry. An ordinary error does NOT certify
// that resources remain usable: generation-bound task admission stays closed
// unless the activator returns the explicit transactional still-active
// disposition (covered in role_activation_generation_test.go).
func TestAFailedDeactivationLeavesTheRoleActive(t *testing.T) {
	rt := activationFixture("system")
	boom := errors.New("teardown could not drain")
	act := &scriptedActivator{
		role:         "auth",
		registers:    []string{"orbtr.io.auth.A"},
		deactivateEr: boom,
		rt:           rt,
	}
	if err := rt.RegisterRoleActivator(act); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := rt.ActivateRole(context.Background(), "auth", secrets.ConfigBundle{}); err != nil {
		t.Fatalf("ActivateRole: %v", err)
	}

	if err := rt.DeactivateRole(context.Background(), "auth"); !errors.Is(err, boom) {
		t.Fatalf("DeactivateRole error = %v, want it to wrap %v", err, boom)
	}

	if got := rt.ActiveTakeoverRoles(); len(got) != 1 || got[0] != "auth" {
		t.Errorf("ActiveTakeoverRoles = %v after a FAILED teardown — the role was dropped "+
			"while its services are still running", got)
	}
	if _, ok := rt.Registry().GetMeta("orbtr.io.auth.A"); !ok {
		t.Error("a failed teardown unregistered the handler anyway — the service is running " +
			"with no way to reach it")
	}
	if got := rt.cfg.Roles; len(got) != 2 {
		t.Errorf("cfg.Roles = %v after a failed teardown — the role was un-advertised while "+
			"still serving", got)
	}
	rt.roleActivation.mu.Lock()
	generation := rt.roleActivation.generations["auth"]
	accepting := generation != nil && generation.accepting
	rt.roleActivation.mu.Unlock()
	if accepting {
		t.Error("unknown/possibly-partial teardown failure reopened task admission")
	}
}

// ─── activationEnv ───────────────────────────────────────────────────────

// The H7 closure-capture rule, enforced by construction: env.Go must bind the
// RUNTIME's context, not the caller's activation context, so a role goroutine
// outlives the activation attempt and is drained by Shutdown. An activator that
// captured its `ctx` parameter would see its goroutine cancelled the moment
// activation returned.
func TestActivationEnvGoBindsTheRuntimeContextNotTheCallersContext(t *testing.T) {
	rt := activationFixture()
	env := activationEnv{rt: rt}

	if env.Context() != rt.ctx {
		t.Error("env.Context() is not the runtime context")
	}

	activationCtx, cancelActivation := context.WithCancel(context.Background())
	got := make(chan context.Context, 1)
	env.Go("role-worker", func(ctx context.Context) { got <- ctx })
	cancelActivation() // the activation attempt ends...

	handed := <-got
	rt.wg.Wait()

	if handed != rt.ctx {
		t.Error("env.Go handed the goroutine a context that is not rt.ctx")
	}
	if handed.Err() != nil {
		t.Error("the role goroutine's context was cancelled when the activation attempt " +
			"ended — long-running role work would die with the claim evaluation that started it")
	}
	_ = activationCtx
}
