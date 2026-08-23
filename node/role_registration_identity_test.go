/*
 * Copyright (c) 2026 HSTLES / ORBTR Pty Ltd. All Rights Reserved.
 * Queries: licensing@hstles.com
 */

package node

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bbmumford/loom/node/handlers"
	"github.com/bbmumford/loom/ports"
	"github.com/bbmumford/loom/secrets"
)

type identityRoleActivator struct {
	role         string
	activate     func(ports.ActivationEnv) error
	deactivate   func() error
	activateRuns atomic.Int32
	deactivRuns  atomic.Int32
}

type blockingRoleRPCHandler struct {
	name    string
	role    string
	entered chan struct{}
	release <-chan struct{}
}

func (h *blockingRoleRPCHandler) Name() string                      { return h.name }
func (h *blockingRoleRPCHandler) Role() string                      { return h.role }
func (h *blockingRoleRPCHandler) RequiresAuth() bool                { return false }
func (h *blockingRoleRPCHandler) AllowedAuthTypes() []string        { return nil }
func (h *blockingRoleRPCHandler) Scopes() []string                  { return nil }
func (h *blockingRoleRPCHandler) TenantScope() handlers.TenantScope { return handlers.TenantScopeNone }
func (h *blockingRoleRPCHandler) AllowedTenants() []string          { return nil }
func (h *blockingRoleRPCHandler) ExecuteRPC(
	ctx context.Context,
	req *handlers.RPCRequest,
) (*handlers.RPCResponse, error) {
	close(h.entered)
	select {
	case <-h.release:
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	return &handlers.RPCResponse{ID: req.ID, Success: true}, nil
}

func (a *identityRoleActivator) Role() string { return a.role }

func (a *identityRoleActivator) Activate(
	_ context.Context,
	env ports.ActivationEnv,
) error {
	a.activateRuns.Add(1)
	if a.activate != nil {
		return a.activate(env)
	}
	return nil
}

func (a *identityRoleActivator) Deactivate(context.Context) error {
	a.deactivRuns.Add(1)
	if a.deactivate != nil {
		return a.deactivate()
	}
	return nil
}

func awaitRoleResult(t *testing.T, ch <-chan error, op string) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(2 * time.Second):
		t.Fatalf("%s did not finish", op)
		return nil
	}
}

func TestRoleActivationDoesNotOwnUnrelatedConcurrentRegistration(t *testing.T) {
	rt := activationFixture("system")
	entered := make(chan struct{})
	proceed := make(chan struct{})
	ownedName := "orbtr.io.auth.Owned"
	unrelatedName := "orbtr.io.system.Unrelated"
	a := &identityRoleActivator{
		role: "auth",
		activate: func(env ports.ActivationEnv) error {
			close(entered)
			<-proceed
			_, err := rt.Registry().RegisterRPCScoped(
				env.RegistrationScope(),
				&rpcOnlyHandler{
					name:  ownedName,
					role:  "auth",
					scope: handlers.TenantScopeNone,
				},
			)
			return err
		},
	}
	if err := rt.RegisterRoleActivator(a); err != nil {
		t.Fatalf("RegisterRoleActivator: %v", err)
	}
	activated := make(chan error, 1)
	go func() {
		activated <- rt.ActivateRole(context.Background(), "auth", secrets.ConfigBundle{})
	}()
	<-entered
	if err := rt.Registry().RegisterRPC(&rpcOnlyHandler{
		name:  unrelatedName,
		role:  "system",
		scope: handlers.TenantScopeNone,
	}); err != nil {
		t.Fatalf("unrelated RegisterRPC: %v", err)
	}
	close(proceed)
	if err := awaitRoleResult(t, activated, "ActivateRole"); err != nil {
		t.Fatalf("ActivateRole: %v", err)
	}
	if err := rt.DeactivateRole(context.Background(), "auth"); err != nil {
		t.Fatalf("DeactivateRole: %v", err)
	}
	if _, ok := rt.Registry().GetMeta(ownedName); ok {
		t.Fatal("owned registration survived role teardown")
	}
	if _, ok := rt.Registry().GetMeta(unrelatedName); !ok {
		t.Fatal("unrelated concurrent registration was attributed to the role")
	}
}

func TestScopedRoleHandlerRemainsInvisibleUntilExactActivationSucceeds(t *testing.T) {
	rt := activationFixture("system")
	const name = "orbtr.io.auth.Pending"
	registered := make(chan struct{})
	finish := make(chan struct{})
	a := &identityRoleActivator{
		role: "auth",
		activate: func(env ports.ActivationEnv) error {
			if _, err := rt.Registry().RegisterRPCScoped(
				env.RegistrationScope(),
				&rpcOnlyHandler{
					name:  name,
					role:  "auth",
					scope: handlers.TenantScopeNone,
				},
			); err != nil {
				return err
			}
			close(registered)
			<-finish
			return nil
		},
	}
	if err := rt.RegisterRoleActivator(a); err != nil {
		t.Fatalf("RegisterRoleActivator: %v", err)
	}
	activated := make(chan error, 1)
	go func() {
		activated <- rt.ActivateRole(context.Background(), "auth", secrets.ConfigBundle{})
	}()
	<-registered

	if _, ok := rt.Registry().Resolve(name); ok {
		t.Fatal("pending activation handler was resolvable before Activate returned")
	}
	if (&localDispatcherAdapter{registry: rt.Registry()}).HasHandler(name) {
		t.Fatal("pending activation handler suppressed the remote RPC fallback")
	}
	if err := rt.Registry().RegisterRPC(&rpcOnlyHandler{
		name:  name,
		role:  "auth",
		scope: handlers.TenantScopeNone,
	}); err == nil || !strings.Contains(err.Error(), "reserved") {
		t.Fatalf("pending same-name registration = %v, want reservation error", err)
	}

	close(finish)
	if err := awaitRoleResult(t, activated, "ActivateRole"); err != nil {
		t.Fatalf("ActivateRole: %v", err)
	}
	if !(&localDispatcherAdapter{registry: rt.Registry()}).HasHandler(name) {
		t.Fatal("successful activation did not publish the scoped RPC handler")
	}
	if err := rt.DeactivateRole(context.Background(), "auth"); err != nil {
		t.Fatalf("DeactivateRole: %v", err)
	}
}

func TestFailedRoleActivationReleasesUnpublishedRegistration(t *testing.T) {
	rt := activationFixture("system")
	const name = "orbtr.io.auth.Aborted"
	wantErr := errors.New("activation rejected")
	a := &identityRoleActivator{
		role: "auth",
		activate: func(env ports.ActivationEnv) error {
			if _, err := rt.Registry().RegisterRPCScoped(
				env.RegistrationScope(),
				&rpcOnlyHandler{
					name:  name,
					role:  "auth",
					scope: handlers.TenantScopeNone,
				},
			); err != nil {
				return err
			}
			return wantErr
		},
	}
	if err := rt.RegisterRoleActivator(a); err != nil {
		t.Fatalf("RegisterRoleActivator: %v", err)
	}
	if err := rt.ActivateRole(
		context.Background(),
		"auth",
		secrets.ConfigBundle{},
	); err == nil || !errors.Is(err, wantErr) {
		t.Fatalf("ActivateRole error = %v, want %v", err, wantErr)
	}
	if _, ok := rt.Registry().Resolve(name); ok {
		t.Fatal("failed activation left a staged handler visible")
	}
	if err := rt.Registry().RegisterRPC(&rpcOnlyHandler{
		name:  name,
		role:  "auth",
		scope: handlers.TenantScopeNone,
	}); err != nil {
		t.Fatalf("failed activation stranded the name reservation: %v", err)
	}
}

func TestFailedScopedRegistrationPreservesPreexistingLiveCapability(t *testing.T) {
	rt := activationFixture("system")
	const name = "orbtr.io.auth.Live"
	live := &rpcOnlyHandler{
		name:  name,
		role:  "auth",
		scope: handlers.TenantScopeNone,
	}
	if err := rt.Registry().RegisterRPC(live); err != nil {
		t.Fatalf("RegisterRPC(live): %v", err)
	}
	a := &identityRoleActivator{
		role: "auth",
		activate: func(env ports.ActivationEnv) error {
			_, err := rt.Registry().RegisterRPCScoped(
				env.RegistrationScope(),
				&rpcOnlyHandler{
					name:  name,
					role:  "auth",
					scope: handlers.TenantScopeNone,
				},
			)
			return err
		},
	}
	if err := rt.RegisterRoleActivator(a); err != nil {
		t.Fatalf("RegisterRoleActivator: %v", err)
	}
	if err := rt.ActivateRole(
		context.Background(),
		"auth",
		secrets.ConfigBundle{},
	); err == nil || !strings.Contains(err.Error(), "already registered") {
		t.Fatalf("same-name ActivateRole error = %v, want live-conflict error", err)
	}
	if got, ok := rt.Registry().GetMeta(name); !ok || got != live {
		t.Fatalf("preexisting live handler = (%p, %v), want %p", got, ok, live)
	}
	if !(&localDispatcherAdapter{registry: rt.Registry()}).HasHandler(name) {
		t.Fatal("failed activation suppressed the preexisting local capability")
	}
}

func TestSimultaneousActiveRoleScopesPreserveLateOwnershipAndTeardownIsolation(t *testing.T) {
	rt := activationFixture("system")
	var authScope ports.RegistrationScope
	var billingScope ports.RegistrationScope
	const (
		authInitial    = "orbtr.io.auth.Initial"
		authLate       = "orbtr.io.auth.LateConcurrent"
		billingInitial = "orbtr.io.billing.Initial"
		billingLate    = "orbtr.io.billing.LateConcurrent"
		billingLater   = "orbtr.io.billing.LaterConcurrent"
	)
	authActivator := &identityRoleActivator{
		role: "auth",
		activate: func(env ports.ActivationEnv) error {
			authScope = env.RegistrationScope()
			_, err := rt.Registry().RegisterRPCScoped(
				authScope,
				&rpcOnlyHandler{
					name:  authInitial,
					role:  "auth",
					scope: handlers.TenantScopeNone,
				},
			)
			return err
		},
	}
	billingActivator := &identityRoleActivator{
		role: "billing",
		activate: func(env ports.ActivationEnv) error {
			billingScope = env.RegistrationScope()
			_, err := rt.Registry().RegisterRPCScoped(
				billingScope,
				&rpcOnlyHandler{
					name:  billingInitial,
					role:  "billing",
					scope: handlers.TenantScopeNone,
				},
			)
			return err
		},
	}
	if err := rt.RegisterRoleActivator(authActivator); err != nil {
		t.Fatalf("RegisterRoleActivator(auth): %v", err)
	}
	if err := rt.RegisterRoleActivator(billingActivator); err != nil {
		t.Fatalf("RegisterRoleActivator(billing): %v", err)
	}
	if err := rt.ActivateRole(
		context.Background(),
		"auth",
		secrets.ConfigBundle{},
	); err != nil {
		t.Fatalf("ActivateRole(auth): %v", err)
	}
	if err := rt.ActivateRole(
		context.Background(),
		"billing",
		secrets.ConfigBundle{},
	); err != nil {
		t.Fatalf("ActivateRole(billing): %v", err)
	}
	if authScope.Token() == billingScope.Token() {
		t.Fatal("simultaneous active role scopes share one token identity")
	}
	if _, err := rt.Registry().RegisterRPCScoped(
		authScope,
		&rpcOnlyHandler{
			name:  authLate,
			role:  "auth",
			scope: handlers.TenantScopeNone,
		},
	); err != nil {
		t.Fatalf("late auth registration: %v", err)
	}
	if _, err := rt.Registry().RegisterRPCScoped(
		billingScope,
		&rpcOnlyHandler{
			name:  billingLate,
			role:  "billing",
			scope: handlers.TenantScopeNone,
		},
	); err != nil {
		t.Fatalf("late billing registration: %v", err)
	}

	if err := rt.DeactivateRole(context.Background(), "auth"); err != nil {
		t.Fatalf("DeactivateRole(auth): %v", err)
	}
	for _, name := range []string{authInitial, authLate} {
		if _, ok := rt.Registry().GetMeta(name); ok {
			t.Fatalf("auth-owned registration %q survived auth teardown", name)
		}
	}
	for _, name := range []string{billingInitial, billingLate} {
		if _, ok := rt.Registry().GetMeta(name); !ok {
			t.Fatalf("billing-owned registration %q removed by auth teardown", name)
		}
	}
	if _, err := rt.Registry().RegisterRPCScoped(
		authScope,
		&rpcOnlyHandler{
			name:  "orbtr.io.auth.AfterClose",
			role:  "auth",
			scope: handlers.TenantScopeNone,
		},
	); err == nil {
		t.Fatal("closed auth scope accepted a later registration")
	}
	if _, err := rt.Registry().RegisterRPCScoped(
		billingScope,
		&rpcOnlyHandler{
			name:  billingLater,
			role:  "billing",
			scope: handlers.TenantScopeNone,
		},
	); err != nil {
		t.Fatalf("billing scope was closed by auth teardown: %v", err)
	}
	if err := rt.DeactivateRole(context.Background(), "billing"); err != nil {
		t.Fatalf("DeactivateRole(billing): %v", err)
	}
	for _, name := range []string{billingInitial, billingLate, billingLater} {
		if _, ok := rt.Registry().GetMeta(name); ok {
			t.Fatalf("billing-owned registration %q survived billing teardown", name)
		}
	}
}

func TestRoleTeardownPreservesSameNameReplacement(t *testing.T) {
	rt := activationFixture("system")
	const name = "orbtr.io.auth.Replaceable"
	handleCh := make(chan handlers.RegistrationHandle, 1)
	a := &identityRoleActivator{
		role: "auth",
		activate: func(env ports.ActivationEnv) error {
			handle, err := rt.Registry().RegisterRPCScoped(
				env.RegistrationScope(),
				&rpcOnlyHandler{name: name, role: "auth", scope: handlers.TenantScopeNone},
			)
			if err == nil {
				handleCh <- handle
			}
			return err
		},
	}
	if err := rt.RegisterRoleActivator(a); err != nil {
		t.Fatalf("RegisterRoleActivator: %v", err)
	}
	if err := rt.ActivateRole(context.Background(), "auth", secrets.ConfigBundle{}); err != nil {
		t.Fatalf("ActivateRole: %v", err)
	}
	if !rt.Registry().UnregisterExact(<-handleCh) {
		t.Fatal("could not remove exact owned registration")
	}
	replacement := &rpcOnlyHandler{
		name:  name,
		role:  "auth",
		scope: handlers.TenantScopeNone,
	}
	if err := rt.Registry().RegisterRPC(replacement); err != nil {
		t.Fatalf("RegisterRPC(replacement): %v", err)
	}
	if err := rt.DeactivateRole(context.Background(), "auth"); err != nil {
		t.Fatalf("DeactivateRole: %v", err)
	}
	if got, ok := rt.Registry().GetMeta(name); !ok || got != replacement {
		t.Fatalf("same-name neighbour = (%p, %v), want retained replacement", got, ok)
	}
}

func TestPendingRoleDoesNotLeasePreexistingSameRoleHandler(t *testing.T) {
	rt := activationFixture("system")
	const name = "orbtr.io.auth.Preexisting"
	preexisting := &rpcOnlyHandler{
		name:  name,
		role:  "auth",
		scope: handlers.TenantScopeNone,
	}
	if err := rt.Registry().RegisterRPC(preexisting); err != nil {
		t.Fatalf("RegisterRPC(preexisting): %v", err)
	}
	entered := make(chan struct{})
	proceed := make(chan struct{})
	a := &identityRoleActivator{
		role: "auth",
		activate: func(ports.ActivationEnv) error {
			close(entered)
			<-proceed
			return nil
		},
	}
	if err := rt.RegisterRoleActivator(a); err != nil {
		t.Fatalf("RegisterRoleActivator: %v", err)
	}
	activated := make(chan error, 1)
	go func() {
		activated <- rt.ActivateRole(context.Background(), "auth", secrets.ConfigBundle{})
	}()
	<-entered
	if _, ok := rt.Registry().Resolve(name); !ok {
		t.Fatal("preexisting handler disappeared during activation")
	}
	response, err := rt.Registry().Dispatch(
		context.Background(),
		&handlers.RPCRequest{ID: "preexisting", Handler: name},
	)
	if err != nil || response == nil || !response.Success {
		t.Fatalf("preexisting dispatch = (%+v, %v), want success without pending-generation lease", response, err)
	}
	close(proceed)
	if err := awaitRoleResult(t, activated, "ActivateRole"); err != nil {
		t.Fatalf("ActivateRole: %v", err)
	}
	if err := rt.DeactivateRole(context.Background(), "auth"); err != nil {
		t.Fatalf("DeactivateRole: %v", err)
	}
	if _, ok := rt.Registry().GetMeta(name); !ok {
		t.Fatal("preexisting same-role handler was removed by teardown")
	}
}

func TestLateScopedRegistrationIsOwnedAndRemoved(t *testing.T) {
	rt := activationFixture("system")
	const name = "orbtr.io.auth.Late"
	releaseLate := make(chan struct{})
	registered := make(chan error, 1)
	a := &identityRoleActivator{
		role: "auth",
		activate: func(env ports.ActivationEnv) error {
			env.Go("late-role-registration", func(context.Context) {
				<-releaseLate
				_, err := rt.Registry().RegisterRPCScoped(
					env.RegistrationScope(),
					&rpcOnlyHandler{name: name, role: "auth", scope: handlers.TenantScopeNone},
				)
				registered <- err
			})
			return nil
		},
	}
	if err := rt.RegisterRoleActivator(a); err != nil {
		t.Fatalf("RegisterRoleActivator: %v", err)
	}
	if err := rt.ActivateRole(context.Background(), "auth", secrets.ConfigBundle{}); err != nil {
		t.Fatalf("ActivateRole: %v", err)
	}
	close(releaseLate)
	if err := awaitRoleResult(t, registered, "late registration"); err != nil {
		t.Fatalf("late scoped registration: %v", err)
	}
	if _, ok := rt.Registry().GetMeta(name); !ok {
		t.Fatal("late scoped registration did not become visible")
	}
	if err := rt.DeactivateRole(context.Background(), "auth"); err != nil {
		t.Fatalf("DeactivateRole: %v", err)
	}
	if _, ok := rt.Registry().GetMeta(name); ok {
		t.Fatal("late scoped registration survived exact teardown")
	}
	rt.wg.Wait()
}

func TestRegistrationAfterScopeSealFailsClosed(t *testing.T) {
	rt := activationFixture("system")
	const name = "orbtr.io.auth.TooLate"
	releaseLate := make(chan struct{})
	registered := make(chan error, 1)
	deactivateEntered := make(chan struct{})
	finishDeactivate := make(chan struct{})
	a := &identityRoleActivator{
		role: "auth",
		activate: func(env ports.ActivationEnv) error {
			env.Go("sealed-role-registration", func(context.Context) {
				<-releaseLate
				_, err := rt.Registry().RegisterRPCScoped(
					env.RegistrationScope(),
					&rpcOnlyHandler{name: name, role: "auth", scope: handlers.TenantScopeNone},
				)
				registered <- err
			})
			return nil
		},
		deactivate: func() error {
			close(deactivateEntered)
			<-finishDeactivate
			return nil
		},
	}
	if err := rt.RegisterRoleActivator(a); err != nil {
		t.Fatalf("RegisterRoleActivator: %v", err)
	}
	if err := rt.ActivateRole(context.Background(), "auth", secrets.ConfigBundle{}); err != nil {
		t.Fatalf("ActivateRole: %v", err)
	}
	deactivated := make(chan error, 1)
	go func() {
		deactivated <- rt.DeactivateRole(context.Background(), "auth")
	}()
	<-deactivateEntered
	close(releaseLate)
	err := awaitRoleResult(t, registered, "sealed late registration")
	if err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("sealed late registration = %v, want closed-scope error", err)
	}
	close(finishDeactivate)
	if err := awaitRoleResult(t, deactivated, "DeactivateRole"); err != nil {
		t.Fatalf("DeactivateRole: %v", err)
	}
	if _, ok := rt.Registry().GetMeta(name); ok {
		t.Fatal("post-seal registration leaked into registry")
	}
	rt.wg.Wait()
}

func TestDeactivateUsesExactActivatorThatCreatedGeneration(t *testing.T) {
	rt := activationFixture("system")
	first := &identityRoleActivator{role: "auth"}
	second := &identityRoleActivator{role: "auth"}
	if err := rt.RegisterRoleActivator(first); err != nil {
		t.Fatalf("RegisterRoleActivator(first): %v", err)
	}
	if err := rt.ActivateRole(context.Background(), "auth", secrets.ConfigBundle{}); err != nil {
		t.Fatalf("ActivateRole(first): %v", err)
	}
	if err := rt.RegisterRoleActivator(second); err != nil {
		t.Fatalf("RegisterRoleActivator(second): %v", err)
	}
	if err := rt.DeactivateRole(context.Background(), "auth"); err != nil {
		t.Fatalf("DeactivateRole: %v", err)
	}
	if got := first.deactivRuns.Load(); got != 1 {
		t.Fatalf("creating activator Deactivate calls = %d, want 1", got)
	}
	if got := second.deactivRuns.Load(); got != 0 {
		t.Fatalf("replacement activator Deactivate calls = %d, want 0", got)
	}
}

func TestRoleDeactivationDrainsScopedRPCBeforeActivatorTeardown(t *testing.T) {
	rt := activationFixture("system")
	releaseRPC := make(chan struct{})
	rpcEntered := make(chan struct{})
	deactivateEntered := make(chan struct{})
	const name = "orbtr.io.auth.BlockingRPC"
	a := &identityRoleActivator{
		role: "auth",
		activate: func(env ports.ActivationEnv) error {
			_, err := rt.Registry().RegisterRPCScoped(
				env.RegistrationScope(),
				&blockingRoleRPCHandler{
					name:    name,
					role:    "auth",
					entered: rpcEntered,
					release: releaseRPC,
				},
			)
			return err
		},
		deactivate: func() error {
			close(deactivateEntered)
			return nil
		},
	}
	if err := rt.RegisterRoleActivator(a); err != nil {
		t.Fatalf("RegisterRoleActivator: %v", err)
	}
	if err := rt.ActivateRole(context.Background(), "auth", secrets.ConfigBundle{}); err != nil {
		t.Fatalf("ActivateRole: %v", err)
	}

	rpcDone := make(chan error, 1)
	go func() {
		response, err := rt.Registry().Dispatch(
			context.Background(),
			&handlers.RPCRequest{ID: "blocking", Handler: name},
		)
		if err == nil && (response == nil || !response.Success) {
			err = errors.New("blocking RPC returned unsuccessful response")
		}
		rpcDone <- err
	}()
	<-rpcEntered

	deactivateDone := make(chan error, 1)
	go func() {
		deactivateDone <- rt.DeactivateRole(context.Background(), "auth")
	}()
	waitForRoleAdmissionClosed(t, rt, "auth")
	select {
	case <-deactivateEntered:
		t.Fatal("activator teardown began before scoped RPC drained")
	default:
	}

	close(releaseRPC)
	if err := awaitRoleResult(t, rpcDone, "RPC dispatch"); err != nil {
		t.Fatalf("RPC dispatch: %v", err)
	}
	if err := awaitRoleResult(t, deactivateDone, "DeactivateRole"); err != nil {
		t.Fatalf("DeactivateRole: %v", err)
	}
	select {
	case <-deactivateEntered:
	default:
		t.Fatal("activator teardown did not run after RPC drained")
	}
}
