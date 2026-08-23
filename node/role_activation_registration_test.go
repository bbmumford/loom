/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"testing"
	"time"

	"github.com/bbmumford/loom/node/handlers"
	"github.com/bbmumford/loom/ports"
	"github.com/bbmumford/loom/secrets"
)

type registrationScopeActivator struct {
	rt      *Runtime
	role    string
	owned   string
	entered chan struct{}
	proceed <-chan struct{}
	scope   chan ports.RegistrationScope
}

type registrationScopeAdmission struct{}

func (registrationScopeAdmission) Acquire() (func(), bool) {
	return func() {}, true
}

func (a *registrationScopeActivator) Role() string { return a.role }

func (a *registrationScopeActivator) Activate(
	ctx context.Context,
	env ports.ActivationEnv,
) error {
	if a.owned != "" {
		if _, err := a.rt.Registry().RegisterRPCScoped(
			env.RegistrationScope(),
			&rpcOnlyHandler{
				name:  a.owned,
				role:  a.role,
				scope: handlers.TenantScopeNone,
			},
		); err != nil {
			return err
		}
	}
	if a.scope != nil {
		a.scope <- env.RegistrationScope()
	}
	if a.entered != nil {
		close(a.entered)
	}
	if a.proceed != nil {
		select {
		case <-a.proceed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (*registrationScopeActivator) Deactivate(context.Context) error { return nil }

func TestRoleActivationDoesNotCaptureUnrelatedTemporalRegistration(t *testing.T) {
	rt := activationFixture("system")
	entered := make(chan struct{})
	proceed := make(chan struct{})
	const ownedName = "orbtr.io.auth.Owned"
	const unrelatedName = "orbtr.io.auth.Unrelated"
	a := &registrationScopeActivator{
		rt:      rt,
		role:    "auth",
		owned:   ownedName,
		entered: entered,
		proceed: proceed,
	}
	if err := rt.RegisterRoleActivator(a); err != nil {
		t.Fatalf("RegisterRoleActivator: %v", err)
	}

	activated := make(chan error, 1)
	go func() {
		activated <- rt.ActivateRole(
			context.Background(),
			"auth",
			secrets.ConfigBundle{},
		)
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("activator did not enter")
	}

	unrelated := &rpcOnlyHandler{
		name:  unrelatedName,
		role:  "auth",
		scope: handlers.TenantScopeNone,
	}
	if err := rt.Registry().RegisterRPC(unrelated); err != nil {
		t.Fatalf("unrelated global RegisterRPC: %v", err)
	}
	close(proceed)
	if err := <-activated; err != nil {
		t.Fatalf("ActivateRole: %v", err)
	}
	if err := rt.DeactivateRole(context.Background(), "auth"); err != nil {
		t.Fatalf("DeactivateRole: %v", err)
	}

	if _, ok := rt.Registry().GetMeta(ownedName); ok {
		t.Fatal("exact scoped registration survived teardown")
	}
	got, ok := rt.Registry().GetMeta(unrelatedName)
	if !ok || got != unrelated {
		t.Fatal("unrelated temporal registration was attributed to the activation")
	}
}

func TestActiveRoleScopeOwnsLateRegistrationAndRejectsItAfterSeal(t *testing.T) {
	rt := activationFixture("system")
	scopeCh := make(chan ports.RegistrationScope, 1)
	a := &registrationScopeActivator{
		rt:    rt,
		role:  "auth",
		owned: "orbtr.io.auth.Initial",
		scope: scopeCh,
	}
	if err := rt.RegisterRoleActivator(a); err != nil {
		t.Fatalf("RegisterRoleActivator: %v", err)
	}
	if err := rt.ActivateRole(
		context.Background(),
		"auth",
		secrets.ConfigBundle{},
	); err != nil {
		t.Fatalf("ActivateRole: %v", err)
	}
	scope := <-scopeCh

	const lateName = "orbtr.io.auth.Late"
	if _, err := rt.Registry().RegisterRPCScoped(
		scope,
		&rpcOnlyHandler{
			name:  lateName,
			role:  "auth",
			scope: handlers.TenantScopeNone,
		},
	); err != nil {
		t.Fatalf("late scoped registration: %v", err)
	}

	rt.roleActivation.mu.Lock()
	generation := rt.roleActivation.generations["auth"]
	rt.roleActivation.mu.Unlock()
	if _, ok := rt.Registry().SealRegistrationScope(generation.scope); !ok {
		t.Fatal("SealRegistrationScope failed")
	}
	if _, err := rt.Registry().RegisterRPCScoped(
		scope,
		&rpcOnlyHandler{
			name:  "orbtr.io.auth.AfterSeal",
			role:  "auth",
			scope: handlers.TenantScopeNone,
		},
	); err == nil {
		t.Fatal("sealed activation scope accepted a late registration")
	}
	if !rt.Registry().ReopenRegistrationScope(generation.scope) {
		t.Fatal("ReopenRegistrationScope failed")
	}

	if err := rt.DeactivateRole(context.Background(), "auth"); err != nil {
		t.Fatalf("DeactivateRole: %v", err)
	}
	if _, ok := rt.Registry().GetMeta(lateName); ok {
		t.Fatal("late causally scoped registration survived teardown")
	}
}

func TestRoleTeardownCannotRemoveSameNameReplacement(t *testing.T) {
	rt := activationFixture("system")
	const name = "orbtr.io.auth.Replaceable"
	a := &registrationScopeActivator{rt: rt, role: "auth", owned: name}
	if err := rt.RegisterRoleActivator(a); err != nil {
		t.Fatalf("RegisterRoleActivator: %v", err)
	}
	if err := rt.ActivateRole(
		context.Background(),
		"auth",
		secrets.ConfigBundle{},
	); err != nil {
		t.Fatalf("ActivateRole: %v", err)
	}
	if !rt.Registry().Unregister(name) {
		t.Fatal("remove owned registration failed")
	}
	replacement := &rpcOnlyHandler{
		name:  name,
		role:  "auth",
		scope: handlers.TenantScopeNone,
	}
	if err := rt.Registry().RegisterRPC(replacement); err != nil {
		t.Fatalf("register same-name replacement: %v", err)
	}

	if err := rt.DeactivateRole(context.Background(), "auth"); err != nil {
		t.Fatalf("DeactivateRole: %v", err)
	}
	got, ok := rt.Registry().GetMeta(name)
	if !ok || got != replacement {
		t.Fatal("exact-handle teardown removed a same-name replacement")
	}
}

func TestRegistrationScopeRejectsWrongRoleWithoutMutation(t *testing.T) {
	reg := handlers.NewHandlerRegistry()
	scope, err := reg.OpenRegistrationScope(
		"auth",
		registrationScopeAdmission{},
	)
	if err != nil {
		t.Fatalf("OpenRegistrationScope: %v", err)
	}
	if _, err := reg.RegisterRPCScoped(
		scope,
		&rpcOnlyHandler{
			name:  "orbtr.io.billing.WrongRole",
			role:  "billing",
			scope: handlers.TenantScopeNone,
		},
	); err == nil {
		t.Fatal("auth registration scope admitted a billing handler")
	}
	if reg.Count() != 0 {
		t.Fatalf("wrong-role registration mutated registry: count=%d", reg.Count())
	}
}

var _ ports.RoleActivator = (*registrationScopeActivator)(nil)
